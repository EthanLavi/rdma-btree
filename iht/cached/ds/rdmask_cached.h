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

/// Configuration information
template <class K, int MAX_HEIGHT>
struct alignas(64) node {
private:
    /// Got from synchrobench (p=0.5)
    int get_rand_level() {
        // todo: initialize it with a seed?
        static uint32_t y = 2463534242UL;
        y^=(y<<13);
        y^=(y>>17);
        y^=(y<<5);
        uint32_t temp = y;
        uint32_t level = 1;
        while (((temp >>= 1) & 1) != 0) {
            ++level;
        }
        /* 0 <= level < MAX_HEIGHT */
        if (level > MAX_HEIGHT) {
            return (int) MAX_HEIGHT;
        } else {
            return (int) level;
        }
    }

public:
    uint64_t value;
    K key;
    int height;
    rdma_ptr<node> next[MAX_HEIGHT];

    node(K key, uint64_t value) : key(key), value(value) {
        height = get_rand_level();
        for(int i = 0; i < MAX_HEIGHT; i++){
            next[i] = nullptr;
        }
    }
};

template <class T>
inline bool is_marked_del(rdma_ptr<T> ptr){
    return ptr.raw() & 0x1;
}

template <class T>
inline rdma_ptr<T> marked_del(rdma_ptr<T> ptr){
    return rdma_ptr<T>(ptr.raw() | 0x1);
}

/// A ptr without the marking (ptr "sans" marking)
template <class T>
inline rdma_ptr<T> sans(rdma_ptr<T> ptr){
    return rdma_ptr<T>(ptr.raw() & ~0x1);
}

/// SIZE is DEGREE * 2
template <class K, int MAX_HEIGHT, K MINKEY, uint64_t DELETE_SENTINEL, uint64_t UNLINK_SENTINEL> class RdmaSkipList {
private:
    Peer self_;
    CacheDepth::CacheDepth cache_depth_;

    typedef node<K, MAX_HEIGHT> Node;
    typedef rdma_ptr<Node> nodeptr;

    template <typename T> inline bool is_local(rdma_ptr<T> ptr) {
        return ptr.id() == self_.id;
    }

    nodeptr root;
    RemoteCacheImpl<capability>* cache;
  
    // preallocated memory for RDMA operations (avoiding frequent allocations)
    rdma_ptr<Node> tmp_node;
    // shared EBRObjectPool has thread_local internals so its all thread safe
    EBRObjectPool<Node, 100>* ebr;

    inline rdma_ptr<uint64_t> get_value_ptr(nodeptr node) {
        Node* tmp = (Node*) 0x0;
        return rdma_ptr<uint64_t>(node.id(), node.address() + (uint64_t) &tmp->value);
    }

    inline rdma_ptr<uint64_t> get_level_ptr(nodeptr node, int level) {          
        Node* tmp = (Node*) 0x0;
        return rdma_ptr<uint64_t>(node.id(), node.address() + (uint64_t) &tmp->next[level]);
    }

    /// Calculate the height of a node by how many levels can reach the current
    /// Returns 0 if completely unlinked
    int height(CachedObject<Node>& node, rdma_ptr<Node> nexts[MAX_HEIGHT]){
        for(int i = 0; i < MAX_HEIGHT; i++){
            if (nexts[i].raw() != node.remote_origin().raw()) return i;
        }
        return MAX_HEIGHT;
    }

    /// Unlink a node (lower height to 0)
    /// Return true if we can deallocate the node
    bool unlink_node(capability* pool, CachedObject<Node>& node, rdma_ptr<Node> prevs[MAX_HEIGHT], rdma_ptr<Node> nexts[MAX_HEIGHT]){
        int h = height(node, nexts);
        for(int i = h - 1; i != -1; i--){
            rdma_ptr<uint64_t> node_ptr = get_level_ptr(node.remote_origin(), i);
            if (!is_marked_del(node->next[i])){
                if (pool->CompareAndSwap<uint64_t>(node_ptr, node->next[i].raw(), marked_del(node->next[i]).raw()) == node->next[i].raw()){
                    cache->Invalidate<Node>(node.remote_origin());
                } else {
                    return false; // failed on the level, retry on the next go around
                }
            }
            rdma_ptr<uint64_t> prev_ptr = get_level_ptr(prevs[i], i);
            if (pool->CompareAndSwap<uint64_t>(prev_ptr, node.remote_origin().raw(), sans(node->next[i]).raw()) == node.remote_origin().raw()){
                // Unlink was successful
                cache->Invalidate<Node>(prevs[i]);
                nexts[i] = sans(node->next[i]); // update next to be accurate
            } else {
                // failed on the level, retry on the next go-around
                return false;
            }
        }
        return true;
    }

    /// Returns new height
    /// Raise a node to a random new height
    int raise_node(capability* pool, CachedObject<Node>& node, rdma_ptr<Node> prevs[MAX_HEIGHT], rdma_ptr<Node> nexts[MAX_HEIGHT]){
        int desired_height = node->height;
        for(int i = height(node, nexts); i < desired_height; i++){
            rdma_ptr<uint64_t> node_ptr = get_level_ptr(node.remote_origin(), i);
            uint64_t old_ptr_curr = pool->CompareAndSwap<uint64_t>(node_ptr, node->next[i].raw(), nexts[i].raw());
            if (old_ptr_curr == node->next[i].raw()) {
                // successful cas, invalidate the node
                cache->Invalidate<Node>(node.remote_origin());
            } else {
                // couldn't set curr, only raised to height i
                return i;
            }
            rdma_ptr<uint64_t> prev_ptr = get_level_ptr(prevs[i], i);
            uint64_t old_ptr_prev = pool->CompareAndSwap<uint64_t>(prev_ptr, nexts[i].raw(), node.remote_origin().raw());
            if (old_ptr_prev == nexts[i].raw()) {
                // insert on this level was successful
                cache->Invalidate<Node>(prevs[i]);
                nexts[i] = node.remote_origin(); // update nexts to the correct value
            } else {
                // couldn't set prev, only raised to height i
                return i;
            }
        }
        return desired_height;
    }

public:
    RdmaSkipList(Peer& self, CacheDepth::CacheDepth depth, RemoteCacheImpl<capability>* cache, capability* pool, int thread_count, vector<Peer> peers, EBRObjectPool<Node, 100>* ebr) 
    : self_(std::move(self)), cache_depth_(depth), cache(cache), ebr(ebr) {
        REMUS_INFO("SENTINEL is MIN? {}", MINKEY);
        tmp_node = pool->Allocate<Node>();
        // todo: uncomment ebr->Init((rdma_capability*) pool, self.id, peers);
    }

    void helper_thread(std::atomic<bool>* do_cont, capability* pool, EBRObjectPool<Node, 100>* ebr_helper, vector<LimboLists<Node>*> qs){
        rdma_ptr<Node> prevs[MAX_HEIGHT];
        rdma_ptr<Node> nexts[MAX_HEIGHT];
        CachedObject<Node> curr;
        int i = 0;
        while(do_cont->load()){ // endlessly traverse and maintain the index
            curr = cache->Read<Node>(root);
            // init prevs and nexts
            for(int i = 0; i < MAX_HEIGHT; i++){
                prevs[i] = curr.remote_origin(); // root
                nexts[i] = curr->next[i]; // root->next
            }
            while(sans(curr->next[0]) != nullptr){
                curr = cache->Read<Node>(sans(curr->next[0]));
                int curr_height = height(curr, nexts);
                if (curr->value == DELETE_SENTINEL){
                    rdma_ptr<uint64_t> ptr = get_value_ptr(curr.remote_origin());
                    if (pool->CompareAndSwap<uint64_t>(ptr, DELETE_SENTINEL, UNLINK_SENTINEL) == DELETE_SENTINEL){
                        if (unlink_node(pool, curr, prevs, nexts)){
                            // if unlink was successful, deallocate
                            LimboLists<Node>* q = qs.at(i);
                            q->free_lists[2].load()->push(curr.remote_origin());
                            // cycle the index
                            i++;
                            i %= qs.size();
                        }
                        curr = cache->Read<Node>(curr.remote_origin()); // jump a node back to refresh nexts and prevs
                        curr_height = height(curr, nexts);
                    }
                } else if (curr->value != UNLINK_SENTINEL){
                    // Have a value, check if it needs to be raised
                    if (curr->height != curr_height){
                        // The intended height isn't equal to the true height of curr
                        raise_node(pool, curr, prevs, nexts);
                        curr = cache->Read<Node>(curr.remote_origin()); // jump a node back to refresh nexts and prevs
                        curr_height = height(curr, nexts);
                    }
                } // else do nothing, someone else unlinking

                // Then update prevs and next (prev was curr up to its height, next is the next node of the curr)
                for(int i = 0; i < curr_height; i++){
                    prevs[i] = curr.remote_origin();
                    nexts[i] = curr->next[i];
                }
            }
            ebr_helper->match_version(true); // indicate done with epoch
        }
    }

    /// Free all the resources associated with the data structure
    void destroy(capability* pool, bool delete_as_first = true) {
        pool->Deallocate<Node>(tmp_node);
        if (delete_as_first) pool->Deallocate<Node>(root);
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
        this->root = this->root;
        return static_cast<rdma_ptr<anon_ptr>>(this->root);
    }

    /// @brief Initialize an IHT from the pointer of another IHT
    /// @param root_ptr the root pointer of the other iht from InitAsFirst();
    void InitFromPointer(capability* pool, rdma_ptr<anon_ptr> root_ptr){
        this->root = static_cast<nodeptr>(root_ptr);
    }

    /// Search for a node where it's result->key is <= key
    /// Will unlink nodes only at the data level leaving them indexable
    private: CachedObject<Node> find(capability* pool, K key){
        // first node is a sentinel, it will always be linked in the data structure
        CachedObject<Node> curr = cache->Read<Node>(root); // root is never deleted, let's say curr is never a deleted node
        CachedObject<Node> next_curr;
        for(int height = MAX_HEIGHT - 1; height != -1; height--){
            // iterate on this level until we find the last node that <= the key
            while(true){
                if (sans(curr->next[height]) == nullptr) break; // if next is the END, descend a level
                next_curr = cache->Read<Node>(sans(curr->next[height]));
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
        while(true){
            CachedObject<Node> curr = find(pool, key);
            if (curr->key == key) {
                if (curr->value == UNLINK_SENTINEL) continue; // it is being unlinked, retry
                if (curr->value == DELETE_SENTINEL){
                    rdma_ptr<uint64_t> curr_remote_value = get_value_ptr(curr.remote_origin());
                    uint64_t old_value = pool->CompareAndSwap<uint64_t>(curr_remote_value, DELETE_SENTINEL, value);
                    if (old_value == DELETE_SENTINEL){
                        // cas succeeded, reinstantiated the node
                        cache->Invalidate(curr.remote_origin());
                        ebr->match_version();
                        return std::nullopt;
                    } else if (old_value == UNLINK_SENTINEL) {
                        // cas failed, either someone else inserted or is being unlinked
                        continue;
                    } else {
                        // Someone else re-inserted instead of us
                        ebr->match_version();
                        return make_optional(old_value);
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
                pool->Write<Node>(new_node_ptr, new_node, tmp_node);
            }

            // if the next is a deleted node, we need to physically delete
            rdma_ptr<uint64_t> dest = get_level_ptr(curr.remote_origin(), 0);
            // will fail if the ptr is marked for unlinking
            uint64_t old = pool->CompareAndSwap<uint64_t>(dest, sans(curr->next[0]).raw(), new_node_ptr.raw());
            if (old == curr->next[0].raw()){ // if our CAS was successful, invalidate the object we modified
                cache->Invalidate(curr.remote_origin());
                ebr->match_version();
                return std::nullopt;
            } else {
                // the insert failed (either an insert or unlink occured), retry the operation
                ebr->requeue(new_node_ptr); // recycle the data
                continue;
            }
        }
    }

    /// @brief Will remove a value at the key. Will stored the previous value in
    /// result.
    /// @param pool the capability providing one-sided RDMA
    /// @param key the key to remove at
    /// @return an optional containing the old value if the remove was successful. Otherwise an empty optional.
    std::optional<uint64_t> remove(capability* pool, K key) {
        CachedObject<Node> curr = find(pool, key);
        if (curr->key != key) {
            // Couldn't find the key, return
            ebr->match_version();
            return std::nullopt;
        }
        if (curr->value == DELETE_SENTINEL || curr->value == UNLINK_SENTINEL) {
            // already removed
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
        } else {
            // the remove failed (a different delete occured), return nullopt
            ebr->match_version();
            return std::nullopt;
        }
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
            int counter = 0;
            Node curr = *root;
            std::cout << height << " SENT -> ";
            while(sans(curr.next[height]) != nullptr){
                curr = *sans(curr.next[height]);
                std::cout << curr.key << " -> ";
                counter++;
            }
            std::cout << "END{" << counter << "}" << std::endl;
        }

        int counter = 0;
        Node curr = *root;
        std::cout << 0 << " SENT -> ";
        while(sans(curr.next[0]) != nullptr){
            std::cout << sans(curr.next[0]) << " ";
            curr = *sans(curr.next[0]);
            if (curr.value == DELETE_SENTINEL) std::cout << "DELETED(" << curr.key << ") -> ";
            else if (curr.value == UNLINK_SENTINEL) std::cout << "UNLINKED(" << curr.key << ") -> ";
            else std::cout << curr.key << " -> ";
            counter++;
        }
        std::cout << "END{" << counter << "}" << std::endl;
    }

    /// A simple check to determine if we caused a corruption of our list (used in single-threaded, local debugging)
    bool is_infinite(){
        for(int height = MAX_HEIGHT - 1; height != -1; height--){
            Node curr = *root;
            K last_key = curr.key;
            while(sans(curr.next[height]) != nullptr){
                curr = *sans(curr.next[height]);
                if (curr.key == last_key) return true;
                last_key = curr.key;
            }
        }
        return false;
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
