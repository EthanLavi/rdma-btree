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

template <class K>
struct UpdateOperation {
  K key;
  int height;
  OpType type;
};

/// Configuration information
template <class K, int MAX_HEIGHT>
struct alignas(64) node {
    K key;
    uint64_t value;
    rdma_ptr<node> next[MAX_HEIGHT];

    node(K key, uint64_t value) : key(key), value(value) {
        for(int i = 0; i < MAX_HEIGHT; i++){
            next[i] = nullptr;
        }
    }
};

/// Is a task pool for update operations
template <class K, int MAX_HEIGHT>
class UpdaterThread {
    typedef node<K, MAX_HEIGHT> Node;
    std::thread t;
    rdma_ptr<Node> root;

    using Operation = UpdateOperation<K>;
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

/// SIZE is DEGREE * 2
template <class K, int MAX_HEIGHT, K MINKEY, uint64_t DELETE_SENTINEL, uint64_t UNLINK_SENTINEL> class RdmaSkipList {
private:
    Peer self_;
    CacheDepth::CacheDepth cache_depth_;
    
    typedef node<K, MAX_HEIGHT> Node;
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
        REMUS_INFO("SENTINEL is MIN? {}", MINKEY);
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
        this->root->key = MINKEY;
        this->root->value = 0;
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

    private: inline rdma_ptr<uint64_t> get_value_ptr(nodeptr node) {
        Node* tmp = (Node*) 0x0;
        return rdma_ptr<uint64_t>(node.id(), node.address() + (uint64_t) &tmp->value);
    } public:

    private: inline rdma_ptr<uint64_t> get_level_ptr(nodeptr node, int level) {          
        Node* tmp = (Node*) 0x0;
        return rdma_ptr<uint64_t>(node.id(), node.address() + (uint64_t) &tmp->next[level]);
    } public:

    /// Search for a node where it's result->key is <= key
    /// Will unlink nodes only at the data level leaving them indexable
    private: CachedObject<Node> find(capability* pool, K key){
        // first node is a sentinel, it will always be linked in the data structure
        CachedObject<Node> curr = cache->Read<Node>(root); // root is never deleted, let's say curr is never a deleted node
        CachedObject<Node> next_curr;
        for(int height = MAX_HEIGHT - 1; height != -1; height--){
            // iterate on this level until we find the last node that <= the key
            while(true){
                if (curr->next[height] == nullptr) break; // if next is the END, descend a level
                next_curr = cache->Read<Node>(curr->next[height]);
                if (next_curr->key <= key) curr = std::move(next_curr); // next_curr is eligible, continue with it
                else break; // otherwise descend a level since next_curr is past the limit
            }
        }
        return curr;
    } public:

    /// @brief Gets a value at the key.
    /// @param pool the capability providing one-sided RDMA
    /// @param key the key to search on
    /// @return an optional containing the value, if the key exists
    std::optional<uint64_t> contains(capability* pool, K key) {
        CachedObject<Node> node = find(pool, key);
        ebr->match_version();
        if (key == node->key && node->value != DELETE_SENTINEL && node->value != UNLINK_SENTINEL) {
            return make_optional(node->value);
        }
        return std::nullopt;
    }

    /// @brief Insert a key and value into the iht. Result will become the value
    /// at the key if already present.
    /// @param pool the capability providing one-sided RDMA
    /// @param key the key to insert
    /// @param value the value to associate with the key
    /// @return an empty optional if the insert was successful. Otherwise it's the value at the key.
    std::optional<uint64_t> insert(capability* pool, K key, uint64_t value) {
        retry:
        CachedObject<Node> curr = find(pool, key);
        if (curr->key == key) {
            if (curr->value == UNLINK_SENTINEL) goto retry; // it is being unlinked, retry
            if (curr->value == DELETE_SENTINEL){
                rdma_ptr<uint64_t> curr_remote_value = get_value_ptr(curr.remote_origin());
                if (pool->CompareAndSwap<uint64_t>(curr_remote_value, DELETE_SENTINEL, value) == DELETE_SENTINEL){
                    // cas succeeded, reinstantiated the node
                    cache->Invalidate(curr.remote_origin());
                    ebr->match_version();
                    return std::nullopt;
                } else {
                    // cas failed, either someone else inserted or is being unlinked
                    goto retry;
                }
            } else {
                // kv already exists
                ebr->match_version();
                return make_optional(curr->value);
            }
        }

        // allocates a node
        nodeptr new_node_ptr = ebr->allocate();
        if (pool->is_local(new_node_ptr)){
            *new_node_ptr = Node(key, value);
            new_node_ptr->next[0] = curr->next[0];
        } else {
            Node new_node = Node(key, value);
            new_node.next[0] = curr->next[0];
            pool->Write<Node>(new_node_ptr, new_node);
        }

        // if the next is a deleted node, we need to physically delete
        // todo: couldn't I CAS the ptr after unlinking?! --> unlinking procedure needs two CAS to succeed, one on the value, one on the ptr
        rdma_ptr<uint64_t> dest = get_level_ptr(curr.remote_origin(), 0);
        uint64_t old = pool->CompareAndSwap<uint64_t>(dest, curr->next[0].raw(), new_node_ptr.raw());
        if (old == curr->next[0].raw()){ // if our CAS was successful, invalidate the object we modified
            cache->Invalidate(curr.remote_origin());
            ebr->match_version();
            return std::nullopt;
            // todo: add a task to the queue for raising the level
        } else {
            // the insert failed (either an insert or delete occured), retry the operation
            ebr->requeue(new_node_ptr); // recycle the data
            goto retry;
        }
    }

    /// @brief Will remove a value at the key. Will stored the previous value in
    /// result.
    /// @param pool the capability providing one-sided RDMA
    /// @param key the key to remove at
    /// @return an optional containing the old value if the remove was successful. Otherwise an empty optional.
    std::optional<uint64_t> remove(capability* pool, K key) {
        retry:
        CachedObject<Node> curr = find(pool, key);
        if (curr->key != key) {
            // Couldn't find the key, return
            ebr->match_version();
            return std::nullopt;
        }

        // if the next is a deleted node, we need to physically delete
        rdma_ptr<uint64_t> dest = get_value_ptr(curr.remote_origin());
        uint64_t old = pool->CompareAndSwap<uint64_t>(dest, curr->value, DELETE_SENTINEL); // mark for deletion
        if (old == curr->value){ // if our CAS was successful, invalidate the object we modified
            cache->Invalidate(curr.remote_origin());
            ebr->match_version();
            return make_optional(curr->value);
            // todo: add a task to the queue for raising the level
        } else {
            // the remove failed (a different delete occured), return nullopt
            ebr->match_version();
            return std::nullopt;
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
    int populate(capability* pool, int op_count, K key_lb, K key_ub, std::function<K(uint64_t)> value) {
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
        while(curr.next[0] != nullptr){
            curr = *curr.next[0];
            if (curr.value == DELETE_SENTINEL) std::cout << "DELETED(" << curr.key << ") -> ";
            else if (curr.value == UNLINK_SENTINEL) std::cout << "UNLINKED(" << curr.key << ") -> ";
            else std::cout << curr.key << " -> ";
        }
        std::cout << "END" << std::endl;
    }

    /// No concurrent or thread safe (if everyone is readonly, then its fine). Counts the number of elements in the IHT
    int count(capability* pool){
        // Get leftmost leaf by wrapping SENTINEL
        int count = 0;
        Node curr = *cache->Read<Node>(root);
        while(curr.next[0] != nullptr){
            curr = *cache->Read<Node>(curr.next[0]);
            if (curr.value != DELETE_SENTINEL && curr.value != UNLINK_SENTINEL)
                count++;
        }
        return count;
    }
};
