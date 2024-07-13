#pragma once

#include <ostream>
#include <random>
#include <remus/logging/logging.h>
#include <remus/rdma/memory_pool.h>
#include <remus/rdma/rdma.h>

#include <dcache/cache_store.h>
#include <dcache/cached_ptr.h>

#include "../../dcache/test/faux_mempool.h"

#include "spscq.h"
#include "ebr.h"

#include "../../common.h"
#include <optional>
#include <thread>

using namespace remus::rdma;

typedef CountingPool capability;

enum OpType {
    UNLINK,
    RAISE,
};

template <class K, class V>
struct UpdateOperation {
  K key;
  int height;
  OpType type;
};

/// Is a task pool for update operations
template <class K, class V>
class UpdaterThread {
    std::thread t;

    using Operation = UpdateOperation<K, V>;
    using Queue = ConcurrentQueue<Operation>;

    /// Frequency is the number of milliseconds to sleep for on timeout
    UpdaterThread(capability* pool, Queue* operations[], int thread_count){
        t = std::thread([&](){
            while(true){
                for(int i = 0; i < thread_count; i++){
                    Queue* q = operations[i];
                    if (!q->empty()){
                        Operation o = q->pop();
                        
                    }
                }
            }
        });
    }

    ~UpdaterThread(){
        t.join();
    }
};

template <class T>
inline bool is_deleted(rdma_ptr<T> obj){
    return obj.address() & 0x1;
}

template <class T>
inline rdma_ptr<T> marked_del(rdma_ptr<T> obj){
    return rdma_ptr<T>(obj.raw() | 0x1);
}

template <class T>
inline rdma_ptr<T> unmarked_del(rdma_ptr<T> obj){
    return rdma_ptr<T>(obj.raw() & ~0x1);
}

/// SIZE is DEGREE * 2
template <class K, class V, int MAX_HEIGHT, K SENTINEL> class RdmaSkipList {
private:
    Peer self_;
    CacheDepth::CacheDepth cache_depth_;

    /// Configuration information
    struct alignas(64) Node {
        K key;
        V value;
        rdma_ptr<Node> next[MAX_HEIGHT];

        Node(K key, V value) : key(key), value(value) {
            for(int i = 0; i < MAX_HEIGHT; i++){
                next[i] = nullptr;
            }
        }
    };

    typedef rdma_ptr<Node> nodeptr;

    struct Task {
        rdma_ptr<nodeptr> ptr;
        nodeptr to_be;
        nodeptr org;

        Task() {
            ptr = nullptr;
        }
        Task(nodeptr org, rdma_ptr<nodeptr> ptr, nodeptr to_be) : org(org), ptr(ptr), to_be(to_be) {}
    };

    template <typename T> inline bool is_local(rdma_ptr<T> ptr) {
        return ptr.id() == self_.id;
    }

    nodeptr root;
    RemoteCacheImpl<capability>* cache;

    // Got from synchrobench (p=0.5)
    int get_rand_level() {
        static uint32_t y = 2463534242UL;
        y^=(y<<13);
        y^=(y>>17);
        y^=(y<<5);
        uint32_t temp = y;
        uint32_t level = 0;
        while (((temp >>= 1) & 1) != 0) {
            ++level;
        }
        /* 0 <= level < MAX_HEIGHT */
        if (level >= MAX_HEIGHT) {
            return (int) MAX_HEIGHT - 1;
        } else {
            return (int) level;
        }
    }
  
    // preallocated memory for RDMA operations (avoiding frequent allocations)
    // rdma_ptr<T> temp_lock;
    EBRObjectPool<Node, 100>* ebr;

public:
    RdmaSkipList(Peer& self, CacheDepth::CacheDepth depth, RemoteCacheImpl<capability>* cache, capability* pool, int thread_count, vector<Peer> peers) 
    : self_(std::move(self)), cache_depth_(depth), cache(cache) {
        REMUS_INFO("SENTINEL is MIN? {}", SENTINEL);
        ebr = new EBRObjectPool<Node, 100>(pool, thread_count);
        // todo: uncomment ebr->Init((rdma_capability*) pool, self.id, peers);
    }

    /// Register the thread --> needed for EBR
    void RegisterThread(){
        ebr->RegisterThread();
    }

    /// Free all the resources associated with the data structure
    void destroy(capability* pool) {
        delete ebr;
    }

    /// @brief Create a fresh iht
    /// @param pool the capability to init the IHT with
    /// @return the iht root pointer
    rdma_ptr<anon_ptr> InitAsFirst(capability* pool){
        this->root = pool->Allocate<Node>();
        this->root->key = SENTINEL;
        for(int i = 0; i < MAX_HEIGHT; i++)
            this->root->next[i] = nullptr;
        this->root = mark_ptr(this->root);
        return static_cast<rdma_ptr<anon_ptr>>(this->root);
    }

    /// @brief Initialize an IHT from the pointer of another IHT
    /// @param root_ptr the root pointer of the other iht from InitAsFirst();
    void InitFromPointer(rdma_ptr<anon_ptr> root_ptr){
        this->root = static_cast<nodeptr>(mark_ptr(root_ptr));
    }

    /// @brief Gets a value at the key.
    /// @param pool the capability providing one-sided RDMA
    /// @param key the key to search on
    /// @return an optional containing the value, if the key exists
    std::optional<V> contains(capability* pool, K key) {
        int height = MAX_HEIGHT - 1;
        // first node is a sentinel, it will always be linked in the data structure
        CachedObject<Node> next_curr;
        CachedObject<Node> curr = cache->Read<Node>(root);
        while(height != -1){
            if (unmarked_del(curr->next[height]) == nullptr){
                height--;
            } else {
                next_curr = cache->Read<Node>(unmarked_del(curr->next[height]));
                if (key == next_curr->key) {
                    ebr->match_version();
                    if (is_deleted(next_curr->next[0]))
                        return std::nullopt;
                    return make_optional(next_curr->value);
                } else if (key > next_curr->key){
                    // go to the next node
                    curr = std::move(next_curr);
                } else {
                    // Key is greater than the next, so we need to descend in the level
                    height--;
                }
            }
        }
        ebr->match_version();
        return std::nullopt;
    }

    private: inline rdma_ptr<uint64_t> get_level_ptr(nodeptr node, int level) {                
        return rdma_ptr<uint64_t>(node.id(), node.address() + sizeof(K) + sizeof(V) + (level * sizeof(nodeptr)));
    } public:

    /// Search for a node where it's result->key is <= key
    /// Will unlink nodes only at the data level leaving them indexable
    private: CachedObject<Node> find(capability* pool, K key){
        // first node is a sentinel, it will always be linked in the data structure
        CachedObject<Node> next_curr;
        CachedObject<Node> curr = cache->Read<Node>(root); // root is never deleted, let's say curr is never a deleted node
        for(int height = MAX_HEIGHT - 1; height != 0; height--){
            // iterate on this level until we find the last node that is not deleted and <= the key
            while(true){
                if (curr->next[height] == nullptr) break; // if next is the END, descend a level
                next_curr = cache->Read<Node>(curr->next[height]);
                while(key >= next_curr->key && next_curr->next[height] != nullptr && is_deleted(next_curr->next[0])){ // while the next is less and deleted
                    next_curr = cache->Read<Node>(unmarked_del(next_curr->next[height]));
                }
                if (key < next_curr->key) break;
                if (is_deleted(next_curr->next[0])) break; // couldn't find a non-deleted node with next as less, descend a level
                curr = std::move(next_curr); // safely transfer to the next one
            }
        } // we've traversed all the way down to the root level
        while(true){
            if (curr->next[0] == nullptr) return curr; // if next is the END, return curr
            next_curr = cache->Read<Node>(curr->next[0]);
            if (is_deleted(next_curr->next[0])){
                // if the next is a deleted node, we need to physically delete
                rdma_ptr<uint64_t> dest = get_level_ptr(curr.remote_origin(), 0);
                uint64_t old = pool->CompareAndSwap<uint64_t>(dest, curr->next[0].raw(), unmarked_del(next_curr->next[0]).raw());
                if (old == curr->next[0].raw()){ // if our CAS was successful, invalidate the object we modified
                    cache->Invalidate(curr.remote_origin());
                    // todo: add a task to the queue for unlinking next_curr
                } else {
                    // the removal failed (either an insert or delete occured), just retry the operation by refreshing curr
                    curr = cache->Read(curr.remote_origin());
                    // the removal failed because curr was deleted... hard-restart
                    if (is_deleted(curr->next[0]))
                        return find(pool, key);
                }
            } else if (key >= next_curr->key) {
                curr = std::move(next_curr); // safely transfer to the next one
            } else {
                return curr;
            }
        }
    } public:

    /// @brief Insert a key and value into the iht. Result will become the value
    /// at the key if already present.
    /// @param pool the capability providing one-sided RDMA
    /// @param key the key to insert
    /// @param value the value to associate with the key
    /// @return an empty optional if the insert was successful. Otherwise it's the value at the key.
    std::optional<V> insert(capability* pool, K key, V value) {
        retry:
        CachedObject<Node> curr = find(pool, key);
        if (curr->key == key) {
            ebr->match_version();
            return make_optional(curr->value);
        }
        // first node is a sentinel, it will always be linked in the data structure
        nodeptr new_node = ebr->allocate(); // allocates a node
        *new_node = Node(key, value);
        new_node->next[0] = curr->next[0];

        // if the next is a deleted node, we need to physically delete
        rdma_ptr<uint64_t> dest = get_level_ptr(curr.remote_origin(), 0);
        uint64_t old = pool->CompareAndSwap<uint64_t>(dest, curr->next[0].raw(), new_node.raw());
        if (old == curr->next[0].raw()){ // if our CAS was successful, invalidate the object we modified
            cache->Invalidate(curr.remote_origin());
            ebr->match_version();
            return std::nullopt;
            // todo: add a task to the queue for raising the level
        } else {
            // the insert failed (either an insert or delete occured), retry the operation
            goto retry;
            // todo: fix leaked memory
        }
    }

    /// @brief Will remove a value at the key. Will stored the previous value in
    /// result.
    /// @param pool the capability providing one-sided RDMA
    /// @param key the key to remove at
    /// @return an optional containing the old value if the remove was successful. Otherwise an empty optional.
    std::optional<V> remove(capability* pool, K key) {
        retry:
        CachedObject<Node> curr = find(pool, key);
        if (curr->key != key) {
            std::cout << "!!!" << curr->key << std::endl;
            ebr->match_version();
            return std::nullopt;
        }

        // if the next is a deleted node, we need to physically delete
        rdma_ptr<uint64_t> dest = get_level_ptr(curr.remote_origin(), 0);
        uint64_t old = pool->CompareAndSwap<uint64_t>(dest, curr->next[0].raw(), marked_del(curr->next[0]).raw()); // mark for deletion
        if (old == unmarked_del(curr->next[0]).raw()){ // if our CAS was successful, invalidate the object we modified
            cache->Invalidate(curr.remote_origin());
            ebr->match_version();
            return make_optional(curr->value);
            // todo: add a task to the queue for raising the level
        } else {
            // the insert failed (either an insert or delete occured), retry the operation
            goto retry;
        }
        // todo: deallocate old node
    }

    /// @brief Populate only works when we have numerical keys. Will add data
    /// @param pool the capability providing one-sided RDMA
    /// @param op_count the number of values to insert. Recommended in total to do key_range / 2
    /// @param key_lb the lower bound for the key range
    /// @param key_ub the upper bound for the key range
    /// @param value the value to associate with each key. Currently, we have
    /// asserts for result to be equal to the key. Best to set value equal to key!
    int populate(capability* pool, int op_count, K key_lb, K key_ub, std::function<K(V)> value) {
        // Populate only works when we have numerical keys
        K key_range = key_ub - key_lb;
        // Create a random operation generator that is
        // - evenly distributed among the key range
        int success_count = 0;
        std::uniform_real_distribution<double> dist = std::uniform_real_distribution<double>(0.0, 1.0);
        std::default_random_engine gen(std::chrono::system_clock::now().time_since_epoch().count() * self_.id);
        while (success_count != op_count) {
            int k = (dist(gen) * key_range) + key_lb;
            if (insert(pool, k, value(k)) == std::nullopt) success_count++;
            // Wait some time before doing next insert...
            std::this_thread::sleep_for(std::chrono::nanoseconds(10));
        }
        return success_count;
    }

    int calc_height(Node& n){
        for(int i = MAX_HEIGHT - 1; i >= 0; i--){
            if (n.next[i] != nullptr) return i + 1;
        }
        return 0;
    }

    /// Single threaded, local print
    void debug(){
        for(int height = MAX_HEIGHT - 1; height != 0; height--){
            Node curr = *root;
            std::cout << height << " SENT -> ";
            while(curr.next[height] != nullptr){
                curr = *curr.next[height];
                std::cout << curr.key << " -> ";
            }
            std::cout << "END" << std::endl;
        }

        Node curr = *root;
        std::cout << 0 << " SENT -> ";
        while(unmarked_del(curr.next[0]) != nullptr){
            curr = *unmarked_del(curr.next[0]);
            if (is_deleted(curr.next[0])) std::cout << "DELETED(" << curr.key << ") -> ";
            else std::cout << curr.key << " -> ";
        }
        std::cout << "END" << std::endl;
    }

    /// No concurrent or thread safe (if everyone is readonly, then its fine). Counts the number of elements in the IHT
    int count(capability* pool){
        // Get leftmost leaf by wrapping SENTINEL
        int count = 0;
        Node curr = *cache->Read<Node>(root);
        while(unmarked_del(curr.next[0]) != nullptr){
            curr = *cache->Read<Node>(unmarked_del(curr.next[0]));
            if (!is_deleted(curr.next[0]))
                count++;
        }
        return count;
    }
};
