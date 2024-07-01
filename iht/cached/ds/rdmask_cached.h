#pragma once

#include <ostream>
#include <random>
#include <remus/logging/logging.h>
#include <remus/rdma/memory_pool.h>
#include <remus/rdma/rdma.h>

#include <dcache/cache_store.h>
#include <dcache/cached_ptr.h>

#include "../../dcache/test/faux_mempool.h"

#include "../../common.h"
#include <optional>

using namespace remus::rdma;

/// SIZE is DEGREE * 2
typedef CountingPool capability;
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

    template <typename T> inline bool is_local(rdma_ptr<T> ptr) {
        return ptr.id() == self_.id;
    }

    rdma_ptr<Node> root;
    RemoteCacheImpl<capability>* cache;

    // Got from synchrobench
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

public:
    RdmaSkipList(Peer& self, CacheDepth::CacheDepth depth, RemoteCacheImpl<capability>* cache, capability* pool) 
    : self_(std::move(self)), cache_depth_(depth), cache(cache) {
        REMUS_INFO("SENTINEL is MIN? {}", SENTINEL);
    }

    /// Free all the resources associated with the data structure
    void destroy(capability* pool) {
        
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
        this->root = static_cast<rdma_ptr<Node>>(mark_ptr(root_ptr));
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
            if (curr->next[height] == nullptr){
                height--;
            } else {
                next_curr = cache->Read<Node>(curr->next[height]);
                if (key == next_curr->key) {
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
        return std::nullopt;
    }

    /// @brief Insert a key and value into the iht. Result will become the value
    /// at the key if already present.
    /// @param pool the capability providing one-sided RDMA
    /// @param key the key to insert
    /// @param value the value to associate with the key
    /// @return an empty optional if the insert was successful. Otherwise it's the value at the key.
    std::optional<V> insert(capability* pool, K key, V value) {
        int height = MAX_HEIGHT - 1;
        // first node is a sentinel, it will always be linked in the data structure
        CachedObject<Node> next_curr;
        CachedObject<Node> curr = cache->Read<Node>(root);
        rdma_ptr<Node> new_node = pool->Allocate<Node>();
        *new_node = Node(key, value);
        while(height != -1){
            if (curr->next[height] == nullptr){
                new_node->next[height] = nullptr;
                height--;
            } else {
                next_curr = cache->Read<Node>(curr->next[height]);
                if (key == next_curr->key) {
                    return make_optional(next_curr->value);
                } else if (key > next_curr->key){
                    // go to the next node
                    curr = std::move(next_curr);
                } else {
                    // Key is greater than the next, so we need to descend in the level and save the ptr
                    new_node->next[height] = curr->next[height];
                    height--;
                }
            }
        }
        // curr is now the node previous to the new node
        Node new_curr = *curr;
        int level = get_rand_level();
        for(int i = 0; i < MAX_HEIGHT; i++){
            if (i <= level){
                new_curr.next[i] = new_node;
            } else {
                new_node->next[i] = nullptr;
            }
        }
        cache->Write(curr.remote_origin(), new_curr); // write the ptrs
        return std::nullopt;
    }

    /// @brief Will remove a value at the key. Will stored the previous value in
    /// result.
    /// @param pool the capability providing one-sided RDMA
    /// @param key the key to remove at
    /// @return an optional containing the old value if the remove was successful. Otherwise an empty optional.
    std::optional<V> remove(capability* pool, K key) {
        int height = MAX_HEIGHT - 1;
        // first node is a sentinel, it will always be linked in the data structure
        CachedObject<Node> next_curr;
        CachedObject<Node> curr = cache->Read<Node>(root);
        V result;
        while(height != -1){
            if (curr->next[height] == nullptr){
                height--;
            } else {
                next_curr = cache->Read<Node>(curr->next[height]);
                if (key == next_curr->key) {
                    result = next_curr->value; // set result

                    // Update the pointers
                    Node n = *curr;
                    while(n.next[height].raw() == next_curr.remote_origin().raw()){
                        n.next[height] = next_curr->next[height]; // swing over next_curr
                        height--;
                    }
                    cache->Write(curr.remote_origin(), n);
                    height--; // decrease height and continue
                } else if (key > next_curr->key){
                    // go to the next node
                    curr = std::move(next_curr);
                } else {
                    return std::nullopt;
                }
            }
        }
        return make_optional(result);
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
        Node curr = *root;
        std::cout << "SENT" << " [" << calc_height(curr) << "] -> ";
        while(curr.next[0] != nullptr){
            curr = *curr.next[0];
            std::cout << curr.key << " [" << calc_height(curr) << "] -> ";
        }
        std::cout << "END" << std::endl;
    }

    /// No concurrent or thread safe (if everyone is readonly, then its fine). Counts the number of elements in the IHT
    int count(capability* pool){
        // Get leftmost leaf by wrapping SENTINEL
        int count = 0;
        Node curr = *cache->Read<Node>(root);
        count++;
        while(curr.next[0] != nullptr){
            curr = *cache->Read<Node>(curr.next[0]);
            count++;
        }
        return count;
    }
};
