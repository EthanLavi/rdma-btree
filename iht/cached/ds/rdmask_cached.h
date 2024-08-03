#pragma once

#include <climits>
#include <ostream>
#include <random>
#include <remus/logging/logging.h>
#include <remus/rdma/memory_pool.h>
#include <remus/rdma/rdma.h>

#include <dcache/cache_store.h>
#include <dcache/cached_ptr.h>

#include "ebr.h"

#include "../../common.h"
#include <optional>
#include <thread>
#include <random>

using namespace remus::rdma;

/* 
todo

Run btree and skiplist on RDMA since they underwent significant changes!
1) Optimize write behaviors
2) Caching
3) Prealloc?
*/

/// Random seeds with around half of the bits set. 
/// Output space is at least 2x the number of threads to prevent overlaps
static uint32_t seed(){
    std::random_device dev;
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> dist(0, 39);
    uint32_t arr[40] = {2463534242, 2318261108, 2689770536, 4186217900, 2639618674, 4135555716, 1896594581, 4177280317, 2478510474, 3602047420, 3882214040, 154032322, 2797879757, 4165739712, 4105014704, 1752638874, 2838708547, 2531708157, 530608692, 2974239974, 4069141101, 2010153904, 1329470636, 2088033866, 2866862742, 2185350033, 3082432825, 2932971466, 1348648012, 3457442513, 3905963781, 3877125244, 1453965676, 83881019, 1280748377, 3148969384, 3231393822, 470576835, 3388582210, 2740827379};
    return arr[dist(rng)];
}

/// Configuration information
template <class K, int MAX_HEIGHT>
struct alignas(64) node {
private:
    /// Got from synchrobench (p=0.5)
    int get_rand_level() {
        static thread_local uint32_t y = seed();
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
    int height; // [0, MAX HEIGHT - 1]
    uint64_t link_level; // [0, MAX HEIGHT] (a lock for helper threads to raise a node)
    rdma_ptr<node> next[MAX_HEIGHT];

    node(K key, uint64_t value) : key(key), value(value) {
        height = get_rand_level();
        link_level = 1;
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
template <class K, int MAX_HEIGHT, K MINKEY, uint64_t DELETE_SENTINEL, uint64_t UNLINK_SENTINEL, class capability> class RdmaSkipList {
private:
    using RemoteCache = RemoteCacheImpl<capability>;

    Peer self_;
    CacheDepth::CacheDepth cache_depth_;

    typedef node<K, MAX_HEIGHT> Node;
    typedef rdma_ptr<Node> nodeptr;

    template <typename T> inline bool is_local(rdma_ptr<T> ptr) {
        return ptr.id() == self_.id;
    }

    nodeptr root;
    RemoteCache* cache;
  
    // preallocated memory for RDMA operations (avoiding frequent allocations)
    rdma_ptr<Node> tmp_node;
    // shared EBRObjectPool has thread_local internals so its all thread safe
    EBRObjectPool<Node, 100, capability>* ebr;

    inline rdma_ptr<uint64_t> get_value_ptr(nodeptr node) {
        Node* tmp = (Node*) 0x0;
        return rdma_ptr<uint64_t>(unmark_ptr(node).id(), unmark_ptr(node).address() + (uint64_t) &tmp->value);
    }

    inline rdma_ptr<uint64_t> get_link_height_ptr(nodeptr node) {
        Node* tmp = (Node*) 0x0;
        return rdma_ptr<uint64_t>(unmark_ptr(node).id(), unmark_ptr(node).address() + (uint64_t) &tmp->link_level);
    }

    inline rdma_ptr<uint64_t> get_level_ptr(nodeptr node, int level) {          
        Node* tmp = (Node*) 0x0;
        return rdma_ptr<uint64_t>(unmark_ptr(node).id(), unmark_ptr(node).address() + (uint64_t) &tmp->next[level]);
    }

    CachedObject<Node> fill(K key, rdma_ptr<Node> preds[MAX_HEIGHT], rdma_ptr<Node> succs[MAX_HEIGHT], bool found[MAX_HEIGHT], K* prev_key = nullptr){
        // first node is a sentinel, it will always be linked in the data structure
        CachedObject<Node> curr = cache->template Read<Node>(root); // root is never deleted, let's say curr is never a deleted node
        CachedObject<Node> next_curr;
        K previous_key = MINKEY;
        for(int height = MAX_HEIGHT - 1; height != -1; height--){
            // iterate on this level until we find the last node that <= the key
            while(true){
                if (sans(curr->next[height]) == nullptr) {
                    preds[height] = curr.remote_origin();
                    previous_key = curr->key;
                    succs[height] = nullptr;
                    found[height] = false;
                    // if next is the END, descend a level
                    break;
                } 
                next_curr = cache->template Read<Node>(sans(curr->next[height]));
                if (next_curr->key < key) {
                    curr = std::move(next_curr);
                    continue; // move right
                } else if (next_curr->key == key){
                    preds[height] = curr.remote_origin();
                    previous_key = curr->key;
                    succs[height] = next_curr->next[height];
                    found[height] = true;
                } else {
                    preds[height] = curr.remote_origin();
                    previous_key = curr->key;
                    succs[height] = next_curr.remote_origin();
                    found[height] = false;
                }
                // descend a level
                break;
            }
        }
        if (prev_key != nullptr) *prev_key = previous_key;
        return next_curr;
    }

    /// Try to physically unlink a node that we know exists and we have the responsibility of unlinking
    void unlink_node(capability* pool, K key){
        rdma_ptr<Node> preds[MAX_HEIGHT];
        rdma_ptr<Node> succs[MAX_HEIGHT];
        bool found[MAX_HEIGHT];
        CachedObject<Node> node = fill(key, preds, succs, found);
        bool had_update = false;
        for(int height = MAX_HEIGHT - 1; height != -1; height--){
            if (!found[height]) continue;
            // if not marked, try to mark
            if (!is_marked_del(succs[height])) {
                rdma_ptr<uint64_t> level_ptr = get_level_ptr(node.remote_origin(), height);
                // mark for deletion to prevent inserts after and also delete issues?
                uint64_t old_ptr = pool->template CompareAndSwap<uint64_t>(level_ptr, succs[height].raw(), marked_del(succs[height]).raw());
                if (old_ptr != succs[height].raw()){
                    // cas failed, retry; also if we had an update, invalidate
                    if (had_update) cache->Invalidate(succs[height]);
                    return unlink_node(pool, key); // retry
                }
                had_update = true;
            }
            
            // physically unlink
            rdma_ptr<uint64_t> level_ptr = get_level_ptr(preds[height], height);
            // only remove the level if prev is not marked
            uint64_t old_ptr = pool->template CompareAndSwap<uint64_t>(level_ptr, sans(node.remote_origin()).raw(), sans(succs[height]).raw());
            if (old_ptr != node.remote_origin().raw()){
                if (had_update) cache->Invalidate(node.remote_origin()); // invalidate the node
                return unlink_node(pool, key); // retry
            } else
                cache->Invalidate(preds[height]);
        }
        if (had_update) cache->Invalidate(node.remote_origin()); // invalidate the node
    }

    /// Try to raise a node to the level
    void raise_node(capability* pool, K key, int goal_height){
        rdma_ptr<Node> preds[MAX_HEIGHT];
        rdma_ptr<Node> succs[MAX_HEIGHT];
        bool found[MAX_HEIGHT];
        CachedObject<Node> node = fill(key, preds, succs, found);
        bool had_update = false;
        for(int height = 0; height < goal_height; height++){
            if (found[height]) continue; // if reachable from a height, continue

            rdma_ptr<uint64_t> level_ptr = get_level_ptr(node.remote_origin(), height);
            uint64_t old_ptr = pool->template CompareAndSwap<uint64_t>(level_ptr, node->next[height].raw(), succs[height].raw());
            if (old_ptr != node->next[height].raw()){
                if (had_update) cache->Invalidate(node.remote_origin()); // invalidate the raising node
                return raise_node(pool, key, goal_height); // retry b/c cas failed
            }
            had_update = true;

            // Update previous node
            level_ptr = get_level_ptr(preds[height], height);
            old_ptr = pool->template CompareAndSwap<uint64_t>(level_ptr, sans(succs[height]).raw(), node.remote_origin().raw());
            if (old_ptr != sans(succs[height]).raw()){
                if (had_update) cache->Invalidate(node.remote_origin()); // invalidate the raising node
                return raise_node(pool, key, goal_height); // retry
            }
            cache->Invalidate(preds[height]);
        }
        if (had_update) cache->Invalidate(node.remote_origin());
    }

public:
    RdmaSkipList(Peer& self, CacheDepth::CacheDepth depth, RemoteCache* cache, capability* pool, vector<Peer> peers, EBRObjectPool<Node, 100, capability>* ebr) 
    : self_(std::move(self)), cache_depth_(depth), cache(cache), ebr(ebr) {
        REMUS_INFO("SENTINEL is MIN? {}", MINKEY);
        tmp_node = pool->template Allocate<Node>();
    }

    void helper_thread(std::atomic<bool>* do_cont, capability* pool, EBRObjectPool<Node, 100, capability>* ebr_helper, vector<LimboLists<Node>*> qs){
        CachedObject<Node> curr;
        int i = 0;
        while(do_cont->load()){ // endlessly traverse and maintain the index
            curr = cache->template Read<Node>(root);
            while(sans(curr->next[0]) != nullptr && do_cont->load()){
                curr = cache->template Read<Node>(sans(curr->next[0]));
                if (curr->value == DELETE_SENTINEL && curr->link_level == curr->height){
                    rdma_ptr<uint64_t> ptr = get_value_ptr(curr.remote_origin());
                    uint64_t last = pool->template CompareAndSwap<uint64_t>(ptr, DELETE_SENTINEL, UNLINK_SENTINEL);
                    if (last != DELETE_SENTINEL) continue; // something changed, gonna skip this node
                    cache->Invalidate(curr.remote_origin());
                    unlink_node(pool, curr->key); // physically unlink (has guarantee of unlink)

                    // deallocate unlinked node
                    LimboLists<Node>* q = qs.at(i);
                    q->free_lists[2].load()->push(curr.remote_origin());
                    
                    // cycle the index
                    i++;
                    i %= qs.size();
                } else if (curr->value == UNLINK_SENTINEL){
                    continue; // someone else is unlinking
                } else {
                    // Hasn't been raised yet and isn't in the process or raising (Test-Test-And-Set)
                    if (curr->link_level != curr->height && curr->link_level != ULONG_MAX){
                        // Raise the node
                        int old_height = pool->template CompareAndSwap<uint64_t>(get_link_height_ptr(curr.remote_origin()), curr->link_level, ULONG_MAX);
                        if (old_height == curr->link_level){
                            raise_node(pool, curr->key, curr->height);
                            old_height = pool->template CompareAndSwap<uint64_t>(get_link_height_ptr(curr.remote_origin()), ULONG_MAX, curr->height);
                            REMUS_ASSERT_DEBUG(old_height == ULONG_MAX, "I should be the only one to leave the critical section of raising");
                        }
                    }
                }
            }
            ebr_helper->match_version(pool, true); // indicate done with epoch
        }
    }

    /// Free all the resources associated with the data structure
    void destroy(capability* pool, bool delete_as_first = true) {
        pool->template Deallocate<Node>(tmp_node);
        if (delete_as_first) pool->template Deallocate<Node>(root);
    }

    /// @brief Create a fresh iht
    /// @param pool the capability to init the IHT with
    /// @return the iht root pointer
    rdma_ptr<anon_ptr> InitAsFirst(capability* pool){
        this->root = pool->template Allocate<Node>();
        this->root->key = MINKEY;
        this->root->value = 0;
        for(int i = 0; i < MAX_HEIGHT; i++)
            this->root->next[i] = nullptr;
        this->root = this->root;
        return static_cast<rdma_ptr<anon_ptr>>(this->root);
    }

    /// @brief Initialize an IHT from the pointer of another IHT
    /// @param root_ptr the root pointer of the other iht from InitAsFirst();
    void InitFromPointer(rdma_ptr<anon_ptr> root_ptr){
        this->root = static_cast<nodeptr>(root_ptr);
    }

    private: void nonblock_unlink_node(capability* pool, K key){
        rdma_ptr<Node> preds[MAX_HEIGHT];
        rdma_ptr<Node> succs[MAX_HEIGHT];
        bool found[MAX_HEIGHT];
        K prev_key;
        CachedObject<Node> node = fill(key, preds, succs, found, &prev_key);
        if (!found[1] && found[0] && node->value == UNLINK_SENTINEL) {
            // physically unlink
            rdma_ptr<uint64_t> level_ptr = get_level_ptr(preds[0], 0);
            // only remove the level if prev is not marked
            uint64_t old_ptr = pool->template CompareAndSwap<uint64_t>(level_ptr, sans(node.remote_origin()).raw(), sans(succs[0]).raw());
            if (old_ptr == sans(node.remote_origin()).raw()) {
                cache->Invalidate(preds[0]);
            } else if (old_ptr == marked_del(node.remote_origin()).raw()) {
                // unlink failed because previous was not deleted yet
                nonblock_unlink_node(pool, prev_key);
            }
        }
    } public:

    /// Search for a node where it's result->key is <= key
    /// Will unlink nodes only at the data level leaving them indexable
    private: CachedObject<Node> find(capability* pool, K key, bool is_insert){
        // first node is a sentinel, it will always be linked in the data structure
        CachedObject<Node> curr = cache->template Read<Node>(root); // root is never deleted, let's say curr is never a deleted node
        CachedObject<Node> next_curr;
        for(int height = MAX_HEIGHT - 1; height != -1; height--){
            // iterate on this level until we find the last node that <= the key
            K last_key = MINKEY;
            while(true){
                if (last_key >= curr->key && last_key != MINKEY){
                    REMUS_ERROR("Infinite loop detected height={} prev={} curr={}", height, last_key, curr->key);
                    abort();
                }
                last_key = curr->key;

                if (curr->key == key) return curr; // stop early if we find the right key
                if (sans(curr->next[height]) == nullptr) break; // if next is the END, descend a level
                next_curr = cache->template Read<Node>(sans(curr->next[height]));
                if (is_insert && height == 0 && is_marked_del(curr->next[height]) && next_curr->key >= key){
                    // we found a node that we are inserting directly after. Lets help unlink
                    nonblock_unlink_node(pool, curr->key);
                    return find(pool, key, is_insert); // recursively retry
                }
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
        CachedObject<Node> node = find(pool, key, false);
        ebr->match_version(pool);
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
            CachedObject<Node> curr = find(pool, key, true);
            if (curr->key == key) {
                if (curr->value == UNLINK_SENTINEL) continue; // it is being unlinked, retry
                if (curr->value == DELETE_SENTINEL){
                    rdma_ptr<uint64_t> curr_remote_value = get_value_ptr(curr.remote_origin());
                    uint64_t old_value = pool->template CompareAndSwap<uint64_t>(curr_remote_value, DELETE_SENTINEL, value);
                    if (old_value == DELETE_SENTINEL){
                        // cas succeeded, reinstantiated the node
                        cache->Invalidate(curr.remote_origin());
                        ebr->match_version(pool);
                        return std::nullopt;
                    } else if (old_value == UNLINK_SENTINEL) {
                        // cas failed, either someone else inserted or is being unlinked
                        continue;
                    } else {
                        // Someone else re-inserted instead of us
                        ebr->match_version(pool);
                        return make_optional(old_value);
                    }
                } else {
                    // kv already exists
                    ebr->match_version(pool);
                    return make_optional(curr->value);
                }
            }

            // allocates a node
            nodeptr new_node_ptr = ebr->allocate(pool);
            if (pool->is_local(new_node_ptr)){
                *new_node_ptr = Node(key, value);
                new_node_ptr->next[0] = curr->next[0];
            } else {
                Node new_node = Node(key, value);
                new_node.next[0] = curr->next[0];
                pool->template Write<Node>(new_node_ptr, new_node, tmp_node);
            }

            // if the next is a deleted node, we need to physically delete
            rdma_ptr<uint64_t> dest = get_level_ptr(curr.remote_origin(), 0);
            // will fail if the ptr is marked for unlinking
            uint64_t old = pool->template CompareAndSwap<uint64_t>(dest, sans(curr->next[0]).raw(), new_node_ptr.raw());
            if (old == curr->next[0].raw()){ // if our CAS was successful, invalidate the object we modified
                cache->Invalidate(curr.remote_origin());
                ebr->match_version(pool);
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
        CachedObject<Node> curr = find(pool, key, false);
        if (curr->key != key) {
            // Couldn't find the key, return
            ebr->match_version(pool);
            return std::nullopt;
        }
        if (curr->value == DELETE_SENTINEL || curr->value == UNLINK_SENTINEL) {
            // already removed
            ebr->match_version(pool);
            return std::nullopt;
        }

        // if the next is a deleted node, we need to physically delete
        rdma_ptr<uint64_t> dest = get_value_ptr(curr.remote_origin());
        uint64_t old = pool->template CompareAndSwap<uint64_t>(dest, curr->value, DELETE_SENTINEL); // mark for deletion
        if (old == curr->value){ // if our CAS was successful, invalidate the object we modified
            cache->Invalidate(curr.remote_origin());
            ebr->match_version(pool);
            return make_optional(curr->value);
        } else {
            // the remove failed (a different delete occured), return nullopt
            ebr->match_version(pool);
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
            curr = *sans(curr.next[0]);
            if (curr.value == DELETE_SENTINEL) std::cout << "DELETED(" << curr.key << ") -> ";
            else if (curr.value == UNLINK_SENTINEL) std::cout << "UNLINKED(" << curr.key << ") -> ";
            else std::cout << curr.key << " -> ";
            counter++;
        }
        std::cout << "END{" << counter << "}" << std::endl;
    }

    /// No concurrent or thread safe (if everyone is readonly, then its fine). Counts the number of elements in the IHT
    int count(capability* pool){
        // Get leftmost leaf by wrapping SENTINEL
        int count = 0;
        Node curr = *cache->template Read<Node>(root);
        while(curr.next[0] != nullptr){
            curr = *cache->template Read<Node>(curr.next[0]);
            if (curr.value != DELETE_SENTINEL && curr.value != UNLINK_SENTINEL)
                count++;
        }
        return count;
    }
};
