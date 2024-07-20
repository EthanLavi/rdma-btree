#pragma once

#include <atomic>
#include <climits>
#include <cstdint>
#include <ostream>
#include <random>
#include <remus/logging/logging.h>
#include <remus/rdma/memory_pool.h>
#include <remus/rdma/rdma.h>

#include <dcache/cache_store.h>
#include <dcache/cached_ptr.h>

// #include "../../dcache/test/faux_mempool.h"

#include "../../common.h"
#include <optional>
#include <remus/rdma/rdma_ptr.h>

using namespace remus::rdma;

typedef int32_t K;
static const K SENTINEL = INT_MAX;

static const bool ADDR = false;

template <class V, int DEGREE, class capability> class RdmaBPTree {
  /// SIZE is DEGREE * 2
  // typedef CountingPool capability;
  // typedef rdma_capability_thread capability;

private:
  static const int SIZE = (DEGREE * 2) + 1;
  static const uint64_t LOCK_BIT = (uint64_t) 1 << 63;
  Peer self_;
  CacheDepth::CacheDepth cache_depth_;

  static const int VLINE_SIZE = 56 / sizeof(V);
  static const int HLINE_SIZE = 48 / sizeof(V);
  #define KLINE_SIZE 14
  #define PLINE_SIZE 7

  /// A hybrid struct for the next ptr
  struct hline {
    long version;
    V values[HLINE_SIZE];
    rdma_ptr<hline> next;
  };

  /// A line with keys
  struct kline {
      long version;
      K keys[KLINE_SIZE];
  };

  /// A line with values
  struct vline {
      long version;
      V values[VLINE_SIZE];
  };

  /// A line with ptrs
  struct pline {
      long version;
      rdma_ptr<pline> ptrs[PLINE_SIZE];
  };

  struct BNode;

  /// Configuration information
  struct alignas(64) BRoot {
    uint64_t lock;
    int height;
    rdma_ptr<BNode> start;

    void reset_lock(){
      lock &= ~LOCK_BIT;
    }

    void set_start_and_inc(rdma_ptr<BNode> new_start){
      start = new_start;
      height++;
    }
  };

  struct alignas(64) BNode {
  private:
    const static int KLINES = (SIZE + KLINE_SIZE) / KLINE_SIZE;
    const static int PLINES = (SIZE + 1 + PLINE_SIZE) / PLINE_SIZE;
    kline key_lines[KLINES];
    pline ptr_lines[PLINES];

  public:
    BNode(){
      for(int i = 0; i < KLINES; i++){
        key_lines[i].version = 0;
      }
      for(int i = 0; i < PLINES; i++){
        ptr_lines[i].version = 0;
      }
      for(int i = 0; i < SIZE; i++){
        set_key(i, SENTINEL);
        set_ptr(i, nullptr);
      }
      set_ptr(SIZE, nullptr);
    }

    /// Checks if the version is valid
    bool is_valid() const {
      long base = key_lines[0].version;
      for(int i = 0; i < KLINES; i++){
        if (key_lines[i].version != base) return false;
      }
      for(int i = 0; i < PLINES; i++){
        if (ptr_lines[i].version != base) return false;
      }
      return true;
    }

    /// Get version of the node without the lock bit
    long version() const {
      return key_lines[0].version & ~LOCK_BIT;
    }

    /// unchecked increment version (also unlocks)
    void increment_version(){
      key_lines[0].version = (key_lines[0].version & ~LOCK_BIT) + 1;
      for(int i = 1; i < KLINES; i++) key_lines[i].version++;
      for(int i = 0; i < PLINES; i++) ptr_lines[i].version++;
    }

    K key_at(int index) const {
      return key_lines[index / KLINE_SIZE].keys[index % KLINE_SIZE];
    }

    void set_key(int index, K key) {
      key_lines[index / KLINE_SIZE].keys[index % KLINE_SIZE] = key;
    }

    rdma_ptr<BNode> ptr_at(int index) const {
      return static_cast<rdma_ptr<BNode>>(ptr_lines[index / PLINE_SIZE].ptrs[index % PLINE_SIZE]);
    }

    void set_ptr(int index, rdma_ptr<BNode> ptr){
      ptr_lines[index / PLINE_SIZE].ptrs[index % PLINE_SIZE] = static_cast<rdma_ptr<pline>>(ptr);
    }
  };

  struct alignas(64) BLeaf {
  private:
    const static int KLINES = (SIZE + KLINE_SIZE) / KLINE_SIZE;
    const static int VLINES = (SIZE + VLINE_SIZE - HLINE_SIZE) / VLINE_SIZE;
    kline key_lines[KLINES];
    vline value_lines[VLINES];
    hline last_line;
  public:
    BLeaf(){
      for(int i = 0; i < KLINES; i++){
        key_lines[i].version = 0;
      }
      for(int i = 0; i < VLINES; i++){
        value_lines[i].version = 0;
      }
      last_line.version = 0;
      for(int i = 0; i < SIZE; i++){
        set_key(i, SENTINEL);
        set_value(i, 0); // set just for debugging
      }
      set_next(nullptr);
    }

    /// Checks if the version is valid
    bool is_valid() const {
      long base = last_line.version;
      for(int i = 0; i < KLINES; i++){
        if (key_lines[i].version != base) return false;
      }
      for(int i = 0; i < VLINES; i++){
        if (value_lines[i].version != base) return false;
      }
      return true;
    }

    /// Get version of the node without the lock bit
    long version() const {
      return key_lines[0].version & ~LOCK_BIT;
    }

    /// unchecked increment version (also unlocks)
    void increment_version(){
      key_lines[0].version = (key_lines[0].version & ~LOCK_BIT) + 1;
      for(int i = 1; i < KLINES; i++) key_lines[i].version++;
      for(int i = 0; i < VLINES; i++) value_lines[i].version++;
      last_line.version++;
    }

    /// Unsafe function, is not coherent with RDMA... can be used before the bleaf is linked!
    void lock(){
      key_lines[0].version = (key_lines[0].version | LOCK_BIT);
    }

    /// Local write to unlock the node
    void unlock(){
      key_lines[0].version = (key_lines[0].version & ~LOCK_BIT);
    }

    /// Get the next ptr for traversal
    const rdma_ptr<BLeaf> get_next() const {
      return static_cast<rdma_ptr<BLeaf>>(last_line.next);
    }
    /// Set the next ptr
    void set_next(rdma_ptr<BLeaf> next_leaf){
      last_line.next = static_cast<rdma_ptr<hline>>(next_leaf);
    }

    K key_at(int index) const {
      return key_lines[index / KLINE_SIZE].keys[index % KLINE_SIZE];
    }

    void set_key(int index, K key) {
      key_lines[index / KLINE_SIZE].keys[index % KLINE_SIZE] = key;
    }

    V value_at(int index) const {
      if ((index / VLINE_SIZE) >= VLINES) return last_line.values[index % VLINE_SIZE];
      else return value_lines[index / VLINE_SIZE].values[index % VLINE_SIZE];
    }

    void set_value(int index, V value) {
      if ((index / VLINE_SIZE) >= VLINES) last_line.values[index % VLINE_SIZE] = value;
      else value_lines[index / VLINE_SIZE].values[index % VLINE_SIZE] = value;
    }
  };

  typedef rdma_ptr<BLeaf> bleaf_ptr;
  typedef rdma_ptr<BNode> bnode_ptr;

  template <typename T> inline bool is_local(rdma_ptr<T> ptr) {
    return ptr.id() == self_.id;
  }

  rdma_ptr<BRoot> root;
  RemoteCacheImpl<capability>* cache;
  
  // preallocated memory for RDMA operations (avoiding frequent allocations)
  rdma_ptr<uint64_t> temp_lock;

  /// returns true if we can acquire the version of the node
  template <class ptr_t>
  bool try_acquire(capability* pool, rdma_ptr<ptr_t> node, long version){
    // swap the first 8 bytes (the lock) from unlocked to locked
    uint64_t v = pool->template CompareAndSwap<uint64_t>(static_cast<rdma_ptr<uint64_t>>(unmark_ptr(node)), version, version | LOCK_BIT);
    if (v == (version & ~LOCK_BIT)) return true; // I think the lock bit will never be set in this scenario since we'd detect a broken read primarily
    return false;
  }

  template <class ptr_t>
  void release(capability* pool, rdma_ptr<ptr_t> node, long version){
    // todo: optimize using write behaviors and caching
    pool->template Write<uint64_t>(static_cast<rdma_ptr<uint64_t>>(unmark_ptr(node)), version, temp_lock);
  }

  template <class ptr_t>
  inline CachedObject<ptr_t> reliable_read(rdma_ptr<ptr_t> node){
    CachedObject<ptr_t> obj = cache->template Read<ptr_t>(node);
    while(!obj->is_valid()){
      obj = cache->template Read<ptr_t>(node);
    }
    return obj;
  }

  /// Allocate a middle layer node
  static bnode_ptr allocate_bnode(capability* pool){
    REMUS_ASSERT(56 % sizeof(V) == 0, "V must be a multiple of 2,4,8,14,28,56");
    bnode_ptr bnode = pool->template Allocate<BNode>();
    *bnode = BNode();
    return bnode;
  }

  /// Allocate a leaf node with sentinel values and nullptr for next
  static bleaf_ptr allocate_bleaf(capability* pool){
    bleaf_ptr bleaf = pool->template Allocate<BLeaf>();
    *bleaf = BLeaf();
    return bleaf;
  }

  /// Return the first index where the provided key is less than the element at that index 
  /// (-1 if the end element)
  public:
  template <class SRC>
  int search_node(const SRC* origin, K key) {
    if (key > origin->key_at(SIZE - 1)) return -1;
    int low = 0;
    int high = SIZE - 1;
    int mid;
    while(low != high){
      mid = (low + high) / 2;
      if (key == origin->key_at(mid))
        return mid;
      else if (key < origin->key_at(mid))
        high = mid;
      else
        low = mid + 1;
    }
    return low; // low = mid = high (but mid hasn't been updated)
    /* reference for equivalence
    for(int i = 0; i < SIZE; i++) if (key <= origin->key_at(i)) return i;
    */
  } private:

  template <class SRC>
  int search_node(const CachedObject<SRC>& origin, K key) {
    return search_node(origin.get(), key);
  }

  /// Returns the Key to go to the parent
  template <class OG, class TO>
  [[nodiscard("Must use removed key")]]
  inline K split_keys(OG keys_full, TO keys_empty, bool no_persist){
    K ret = keys_full->key_at(DEGREE);
    for(int i = 0; i < SIZE; i++){
      if (i < DEGREE){
        keys_empty->set_key(i, keys_full->key_at(DEGREE + i + 1));
        keys_full->set_key(DEGREE + i + 1, SENTINEL);
      } else {
        // otherwise initialize sentinels
        keys_empty->set_key(i, SENTINEL);
      }
    }
    if (no_persist){
      keys_full->set_key(DEGREE, SENTINEL);
    }
    return ret;
  }

  inline void split_values(BLeaf* values_full, BLeaf* values_empty){
    for(int i = 0; i < SIZE; i++){
      if (i < DEGREE){
        values_empty->set_value(i, values_full->value_at(DEGREE + i + 1));
        values_full->set_value(DEGREE + i + 1, SENTINEL);
      } else {
        // otherwise initialize sentinels
        values_empty->set_value(i, SENTINEL);
      }
    }
  }

  inline void split_ptrs(BNode* ptrs_full, BNode* ptrs_empty){
    for(int i = 0; i <= SIZE; i++){
      if (i <= DEGREE){
        ptrs_empty->set_ptr(i, ptrs_full->ptr_at(DEGREE + i + 1));
        ptrs_full->set_ptr(DEGREE + i + 1, nullptr);
      } else {
        // otherwise initialize sentinels
        ptrs_empty->set_ptr(i, nullptr);
      }
    }
  }

  /// Shift everything from index up leaving index open in keys and index + 1 open in ptr
  inline void shift_up(BNode* bnode, int index){
    for(int i = SIZE - 1; i > index; i--){
      bnode->set_key(i, bnode->key_at(i - 1));
    }
    for(int i = SIZE; i > index + 1; i--){
      bnode->set_ptr(i, bnode->ptr_at(i - 1));
    }
  }

  // todo; mark ptr
  /// At root
  void split_node(capability* pool, CachedObject<BRoot>& parent_p, CachedObject<BNode>& node_p){
    BRoot parent = *parent_p;
    BNode node = *node_p;

    // Full node so split
    bnode_ptr new_parent = allocate_bnode(pool);
    parent.set_start_and_inc(new_parent);

    bnode_ptr new_neighbor = allocate_bnode(pool);
    K to_parent = split_keys<BNode*, bnode_ptr>(&node, new_neighbor, true);
    split_ptrs(&node, (BNode*) new_neighbor);

    new_parent->set_key(0, to_parent);
    new_parent->set_ptr(0, node_p.remote_origin());
    new_parent->set_ptr(1, new_neighbor);

    parent.reset_lock();
    node.increment_version(); // increment version before writing

    cache->template Write<BRoot>(parent_p.remote_origin(), parent);      
    cache->template Write<BNode>(node_p.remote_origin(), node);
  }

  /// Move
  void split_node(capability* pool, CachedObject<BRoot>& parent_p, CachedObject<BLeaf>& node_p){
    BRoot parent = *parent_p;
    BLeaf node = *node_p;

    // Full node so split
    bnode_ptr new_parent = allocate_bnode(pool);
    parent.set_start_and_inc(new_parent);

    bleaf_ptr new_neighbor = allocate_bleaf(pool);
    
    K to_parent = split_keys<BLeaf*, bleaf_ptr>(&node, new_neighbor, false);
    split_values(&node, (BLeaf*) new_neighbor);
    node.set_next(new_neighbor);

    new_parent->set_key(0, to_parent);
    new_parent->set_ptr(0, static_cast<bnode_ptr>(node_p.remote_origin()));
    new_parent->set_ptr(1, static_cast<bnode_ptr>(new_neighbor));

    parent.reset_lock();
    node.increment_version();

    cache->template Write<BRoot>(parent_p.remote_origin(), parent);
    cache->template Write<BLeaf>(node_p.remote_origin(), node);
  }

  /// Not at root
  void split_node(capability* pool, CachedObject<BNode>& parent_p, CachedObject<BNode>& node_p){
    BNode parent = *parent_p;
    BNode node = *node_p;
    
    // Full node so split
    bnode_ptr new_neighbor = allocate_bnode(pool);
    K to_parent = split_keys<BNode*, bnode_ptr>(&node, new_neighbor, true);
    split_ptrs(&node, (BNode*) new_neighbor);

    int bucket = search_node<BNode>(parent_p, to_parent);
    REMUS_ASSERT(bucket != -1, "Implies a full parent");

    shift_up(&parent, bucket);
    parent.set_key(bucket, to_parent);
    parent.set_ptr(bucket + 1, new_neighbor);

    parent.increment_version(); // increment version before writing
    node.increment_version();

    cache->template Write<BNode>(parent_p.remote_origin(), parent);
    cache->template Write<BNode>(node_p.remote_origin(), node);
  }

  /// At leaf. Try to split the leaf into two and move a key to the parent
  BLeaf split_node(capability* pool, CachedObject<BNode>& parent_p, CachedObject<BLeaf>& node_p){
    BNode parent = *parent_p;
    BLeaf node = *node_p;

    // Full node so split
    bleaf_ptr new_neighbor = allocate_bleaf(pool);
    K to_parent = split_keys<BLeaf*, bleaf_ptr>(&node, new_neighbor, false);
    split_values(&node, (BLeaf*) new_neighbor);
    new_neighbor->set_next(node.get_next());
    node.set_next(new_neighbor);
    new_neighbor->lock(); // start locked

    int bucket = search_node<BNode>(parent_p, to_parent);
    REMUS_ASSERT(bucket != -1, "Implies a full parent");

    shift_up(&parent, bucket);
    parent.set_key(bucket, to_parent);
    parent.set_ptr(bucket + 1, static_cast<bnode_ptr>(new_neighbor));

    parent.increment_version();
    node.increment_version();

    // todo: prealloc?
    cache->template Write<BNode>(parent_p.remote_origin(), parent);
    // leave the leaf unmodified and locked for further use
    return node;
  }

  inline bnode_ptr read_level(BNode* curr, int bucket){
    bnode_ptr next_level = curr->ptr_at(SIZE);
    if (bucket != -1) next_level = curr->ptr_at(bucket);
    REMUS_ASSERT(next_level != nullptr, "Accessing SENTINEL's ptr");
    return next_level;
  }

  // compiling with optimizations turned on breaks correctness
  CachedObject<BLeaf> traverse(capability* pool, K key, bool modifiable, function<void(BLeaf*, int)> effect, int it_counter = 0){
    REMUS_ASSERT(it_counter < 500, "Infinite recursion detected");
  
    // read root first
    CachedObject<BRoot> curr_root = cache->template Read<BRoot>(root);
    int height = curr_root->height;
    bnode_ptr next_level = curr_root->start;
    REMUS_ASSERT(next_level != nullptr, "Accessing SENTINEL's ptr");

    if (height == 0){
      CachedObject<BLeaf> next_leaf = reliable_read<BLeaf>(static_cast<bleaf_ptr>(next_level));
      if (modifiable){
        // failed to get locks, retraverse
        if (!try_acquire<BRoot>(pool, curr_root.remote_origin(), 0)) return traverse(pool, key, modifiable, effect);
        if (!try_acquire<BLeaf>(pool, next_leaf.remote_origin(), next_leaf->version())) {
          release<BRoot>(pool, curr_root.remote_origin(), 0);
          return traverse(pool, key, modifiable, effect);
        }
        if (next_leaf->key_at(SIZE - 1) != SENTINEL){
          split_node(pool, curr_root, next_leaf);
          // Just restart
          return traverse(pool, key, modifiable, effect);
        } else {
          // Acquire both locks if room to spare in leaf, then write back
          BLeaf leaf_updated = *next_leaf;
          effect(&leaf_updated, search_node<BLeaf>(&leaf_updated, key));
          leaf_updated.increment_version();
          release<BRoot>(pool, curr_root.remote_origin(), 0);
          cache->template Write<BLeaf>(next_leaf.remote_origin(), leaf_updated);
        }
      }

      // Made it to the leaf
      return next_leaf;
    }

    // Traverse bnode until bleaf
    CachedObject<BNode> curr = reliable_read<BNode>(next_level);
    CachedObject<BNode> parent;
    // if split, we updated the root and we need to retraverse
    if (modifiable && curr->key_at(SIZE - 1) != SENTINEL){
      if (!try_acquire<BRoot>(pool, curr_root.remote_origin(), 0)) return traverse(pool, key, modifiable, effect);
      if (!try_acquire<BNode>(pool, curr.remote_origin(), curr->version())) {
        release<BRoot>(pool, curr_root.remote_origin(), 0);
        return traverse(pool, key, modifiable, effect);
      }
      split_node(pool, curr_root, curr);
      return traverse(pool, key, modifiable, effect);
    }

    int bucket = search_node<BNode>(curr, key);
    while(height != 1){
      // Get the next level ptr
      next_level = read_level((BNode*) curr.get(), bucket);

      // Read it as a BNode
      parent = std::move(curr);
      curr = reliable_read(next_level);
      if (modifiable && curr->key_at(SIZE - 1) != SENTINEL) {
        if (try_acquire<BNode>(pool, parent.remote_origin(), parent->version())) {
          if (try_acquire<BNode>(pool, curr.remote_origin(), curr->version())) {
            // can acquire parent and current, so split
            split_node(pool, parent, curr);
          } else {
            // cannot acquire child so release parent and try again
            release<BNode>(pool, parent.remote_origin(), parent->version());
          }
        }
        // re-read the parent and continue
        curr = reliable_read(parent.remote_origin());
        bucket = search_node<BNode>(curr, key);
        continue;
      }
      bucket = search_node<BNode>(curr, key);
      height--;
    }

    // Get the next level ptr
    next_level = read_level((BNode*) curr.get(), bucket);

    // Read it as a BLeaf
    bleaf_ptr next_leaf = static_cast<bleaf_ptr>(next_level);
    CachedObject<BLeaf> leaf = reliable_read<BLeaf>(next_leaf);
    bucket = search_node<BLeaf>(leaf, key);
    if (leaf->key_at(bucket) == SENTINEL){
      // our key is greater than all the keys in the leaf, check if we need to retry
      next_leaf = leaf->get_next();
      // find the next key, if it's greater or equal, we need to retry
      // todo: if the next node is empty, remove it?
      while(true){
        if (next_leaf == nullptr) break;
        CachedObject<BLeaf> leaf_after = reliable_read<BLeaf>(next_leaf);
        if (leaf_after->key_at(0) != SENTINEL && key >= leaf_after->key_at(0)) return traverse(pool, key, modifiable, effect, it_counter + 1);
        else if (leaf_after->key_at(0) != SENTINEL && key < leaf_after->key_at(0)) break;
        else next_leaf = leaf_after->get_next();
      }
    }
    // At this point, we are sure of the leaf node

    // Handle updates to the leaf
    if (modifiable){
      // acquire parent and current leaf
      if (try_acquire<BNode>(pool, curr.remote_origin(), curr->version())){
        if (try_acquire<BLeaf>(pool, leaf.remote_origin(), leaf->version())){
          if (leaf->key_at(SIZE - 1) != SENTINEL){
            // should split
            BLeaf leaf_updated = split_node(pool, curr, leaf);
            next_leaf = leaf_updated.get_next(); // next_leaf is local and readonly since we left it locked
            bucket = search_node<BLeaf>(&leaf_updated, key);
            if (bucket == -1 || leaf_updated.key_at(bucket) == SENTINEL) {
              // key goes into next
              cache->template Write<BLeaf>(leaf.remote_origin(), leaf_updated); // write and unlock the prev
              effect(next_leaf.get(), search_node<BLeaf>(next_leaf.get(), key)); // modify the next
              atomic_thread_fence(memory_order::seq_cst);
              next_leaf->unlock();
              return leaf;
            } else {
              // key goes into current, unlock next
              next_leaf->unlock();
              effect(&leaf_updated, bucket);
              cache->template Write<BLeaf>(leaf.remote_origin(), leaf_updated);
              return leaf;
            }
          } else {
            // modifiable so lock, update, write back
            BLeaf leaf_updated = *leaf;
            effect(&leaf_updated, search_node<BLeaf>(&leaf_updated, key));
            leaf_updated.increment_version();
            cache->template Write<BLeaf>(leaf.remote_origin(), leaf_updated);
            release<BNode>(pool, curr.remote_origin(), curr->version()); // release parent
            return leaf;
          }
        } else {
          // release parent and try again
          release<BNode>(pool, curr.remote_origin(), curr->version());
        }
      } // else nothing was acquired, try again
    } else {
      // not modifiable and no split needed
      return leaf;
    }
    return traverse(pool, key, modifiable, effect, it_counter + 1); // retry by retraversing
  }

public:
  RdmaBPTree(Peer& self, CacheDepth::CacheDepth depth, RemoteCacheImpl<capability>* cache, capability* pool, int print_info) 
  : self_(std::move(self)), cache_depth_(depth), cache(cache) {
    if (print_info){
      REMUS_INFO("Sentinel = {}", SENTINEL);
      REMUS_INFO("Struct Memory (BNode): {} % 64 = {}", sizeof(BNode), sizeof(BNode) % 64);
      REMUS_INFO("Struct Memory (BLeaf): {} % 64 = {}", sizeof(BLeaf), sizeof(BLeaf) % 64);
    }
    temp_lock = pool->template Allocate<uint64_t>(8); // todo: undo 8
  };

  /// Free all the resources associated with the IHT
  void destroy(capability* pool) {
    pool->template Deallocate<uint64_t>(temp_lock, 8);
  }

  /// @brief Create a fresh iht
  /// @param pool the capability to init the IHT with
  /// @return the iht root pointer
  rdma_ptr<anon_ptr> InitAsFirst(capability* pool){
    rdma_ptr<BRoot> broot = pool->template Allocate<BRoot>();
    broot->height = 0; // set height to 1
    broot->lock = 0; // initialize lock
    this->root = broot;
    this->root->start = static_cast<bnode_ptr>(allocate_bleaf(pool));
    // if (cache_depth_ >= 1)
    //     this->root = mark_ptr(this->root);
    return static_cast<rdma_ptr<anon_ptr>>(this->root);
  }

  /// @brief Initialize an IHT from the pointer of another IHT
  /// @param root_ptr the root pointer of the other iht from InitAsFirst();
  void InitFromPointer(rdma_ptr<anon_ptr> root_ptr){
      // if (cache_depth_ >= 1)
      //   root_ptr = mark_ptr(root_ptr);
    this->root = static_cast<rdma_ptr<BRoot>>(root_ptr);
  }

  /// @brief Gets a value at the key.
  /// @param pool the capability providing one-sided RDMA
  /// @param key the key to search on
  /// @return an optional containing the value, if the key exists
  std::optional<V> contains(capability* pool, K key) {
    CachedObject<BLeaf> leaf = traverse(pool, key, false, function([=](BLeaf*, int){}));
    int bucket = search_node<BLeaf>(leaf, key);
    if (leaf->key_at(bucket) == key) return make_optional(leaf->value_at(bucket));
    return nullopt;
  }

  /// @brief Insert a key and value into the iht. Result will become the value
  /// at the key if already present.
  /// @param pool the capability providing one-sided RDMA
  /// @param key the key to insert
  /// @param value the value to associate with the key
  /// @return an empty optional if the insert was successful. Otherwise it's the value at the key.
  std::optional<V> insert(capability* pool, K key, V value) {
    optional<V> prev_value = nullopt;
    traverse(pool, key, true, function([&](BLeaf* new_leaf, int bucket){
      if (new_leaf->key_at(bucket) != key) { // N.B. this is an inverted condition. Action upon the key is absent
        REMUS_ASSERT(new_leaf->key_at(SIZE - 1) == SENTINEL, "Splitting required!");
        // shift up to make room for the KV (if possible)
        for(int i = SIZE - 1; i > bucket; i--) new_leaf->set_value(i, new_leaf->value_at(i - 1));
        for(int i = SIZE - 1; i > bucket; i--) new_leaf->set_key(i, new_leaf->key_at(i - 1));
        new_leaf->set_key(bucket, key);
        new_leaf->set_value(bucket, value);
      } else {
        prev_value = optional(new_leaf->value_at(bucket));
      }
    }));
    return prev_value; // found a match
  }

  /// @brief Will remove a value at the key. Will stored the previous value in
  /// result.
  /// @param pool the capability providing one-sided RDMA
  /// @param key the key to remove at
  /// @return an optional containing the old value if the remove was successful. Otherwise an empty optional.
  std::optional<V> remove(capability* pool, K key) {
    optional<V> prev_value = nullopt;
    traverse(pool, key, true, function([&](BLeaf* new_leaf, int bucket){
      if (new_leaf->key_at(bucket) == key) {
        prev_value = optional(new_leaf->value_at(bucket));
        // shift down to overwrite the key
        for(int i = bucket + 1; i < SIZE; i++){
          new_leaf->set_key(i - 1, new_leaf->key_at(i));
          new_leaf->set_value(i - 1, new_leaf->value_at(i));
        }
      }
    }));
    return prev_value;
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

  void debug(bleaf_ptr node, int indent){
    BLeaf leaf = *node;
    for(int i = 0; i < indent; i++){
      std::cout << "\t";
    }

    if (ADDR)
      std::cout << "Leaf<next=" << std::hex << leaf.get_next().address() << std::dec << "> ";
    else
      std::cout << "Leaf<> ";
    for(int i = 0; i < SIZE; i++){
      if (leaf.key_at(i) == SENTINEL)
        std::cout << "(SENT) ";
      else
        std::cout << "(" << leaf.key_at(i) << ", " << leaf.value_at(i) << ") ";
    }
    std::cout << std::endl;
  }

  void debug(int height, bnode_ptr node, int indent, int index){
    if (height == 0){
      debug(static_cast<bleaf_ptr>(node), indent + 1);
      return;
    }
    BNode n = *node;
    for(int i = 0; i < indent; i++){
      std::cout << "\t";
    }
    std::cout << (n.is_valid() ? "valid " : "invalid ") << "(" << n.version() << ", " << height << ", " << index << ")";
    std::cout << " => ";
    if (ADDR)
      std::cout << std::hex << n.ptr_at(0).address() << std::dec;
    for(int i = 0; i < SIZE; i++){
      if (ADDR){
        if (n.key_at(i) == SENTINEL)
          std::cout << " (SENT) " << std::hex << n.ptr_at(i+1).address() << std::dec;
        else
          std::cout << " (" << n.key_at(i) << ") " << std::hex << n.ptr_at(i+1).address() << std::dec;
      } else {
            if (n.key_at(i) == SENTINEL)
              std::cout << " (SENT) ";
            else
              std::cout << " (" << n.key_at(i) << ") ";
      }
    }
    std::cout << std::endl;
    for(int i = 0; i <= SIZE; i++){
      if (n.ptr_at(i) != nullptr)
        debug(height - 1, n.ptr_at(i), indent + 1, i + 1);
    }
  }

  string is_valid(int height, bnode_ptr node, K lower, K upper){
    if (height == 0){
      BLeaf l = *static_cast<bleaf_ptr>(node);
      for(int i = 0; i < SIZE; i++){
        if (l.key_at(i) == SENTINEL) continue;
        if (l.key_at(i) <= lower || l.key_at(i) > upper) return "key are not in bounds, bleaf; " + to_string(l.key_at(i));
      }
      for(int i = 1; i < SIZE; i++){
        if (l.key_at(i) < l.key_at(i - 1)) return "keys are not sorted in bleaf; [i]=" + to_string(l.key_at(i)) + " [i-1]=" + to_string(l.key_at(i - 1));
        if (l.key_at(i) == l.key_at(i - 1) && l.key_at(i) != SENTINEL) return "duplicate keys in bnode; " + to_string(l.key_at(i));
      }
      return "";
    } else {
      BNode n = *node;
      for(int i = 0; i < SIZE; i++){
        if (n.key_at(i) == SENTINEL) continue;
        if (n.key_at(i) <= lower || n.key_at(i) >= upper) return "keys are not in bounds, bnode; " + to_string(n.key_at(i));
      }
      for(int i = 1; i < SIZE; i++){
        if (n.key_at(i) < n.key_at(i - 1)) return "keys are not sorted in bnode; [i]=" + to_string(n.key_at(i)) + " [i-1]=" + to_string(n.key_at(i - 1));
        if (n.key_at(i) == n.key_at(i - 1) && n.key_at(i) != SENTINEL) return "duplicate keys in bnode; " + to_string(n.key_at(i));
      }

      // Handle children
      for(int i = 0; i <= SIZE; i++){
        if (n.ptr_at(i) == nullptr) continue;
        K low_key = i == 0 ? lower : n.key_at(i - 1);
        K high_key = i == SIZE ? upper : n.key_at(i);
        if (high_key == SENTINEL) high_key = upper; // make sure we are using upper
        string reason = is_valid(height - 1, n.ptr_at(i), low_key, high_key);
        if (reason != "") return reason;
      }
      return "";
    }
  }

  string valid(){
    BRoot r = *root;
    string reason = is_valid(r.height, r.start, INT_MIN, INT_MAX);
    if (reason == "") reason = "yes";
    return reason;
  }

  /// Single threaded print
  void debug(){
    BRoot r = *root;
    std::cout << "Root(height=" << r.height << ")" << std::endl;
    debug(r.height, r.start, 0, 0);
    std::cout << std::endl;
  }

  /// No concurrent or thread safe (if everyone is readonly, then its fine). Counts the number of elements in the IHT
  int count(capability* pool){
    // Get leftmost leaf by wrapping SENTINEL
    CachedObject<BLeaf> curr = traverse(pool, 0 - SENTINEL - 1, false, function([=](BLeaf*, int){}));
    int count = 0;
    while(true){
      // Add to count
      for(int i = 0; i < SIZE; i++){
        if (curr->key_at(i) != SENTINEL) count++;
      }
      if (curr->get_next() == nullptr) break;
      curr = cache->template Read<BLeaf>(curr->get_next());
    }
    return count;
  }
};
