#pragma once

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
#include "ebr.h"
#include "../sherman/sherman_cache.h"
#include "../sherman/sherman_root.h"

#include "../../common.h"
#include <optional>
#include <remus/rdma/rdma_ptr.h>

using namespace remus::rdma;

typedef int32_t K;
#define SENTINEL INT_MAX
#define PRINT_ADDR false

template <class V, int DEGREE, class capability> class ShermanBPTree {
  /// SIZE is DEGREE * 2
  // typedef CountingPool capability;
  // typedef rdma_capability_thread capability;

private:
  static const int SIZE = (DEGREE * 2) + 1;
  static const uint64_t LOCK_BIT = (uint64_t) 1 << 63;
  static std::atomic<bool> is_leader_gen;
  bool is_leader = false;

  static const int VLINE_SIZE = 56 / sizeof(V);
  static const int HLINE_SIZE = 32 / sizeof(V);
  #define KLINE_SIZE 14
  #define PLINE_SIZE 7

  /// A hybrid struct for the next ptr
  struct hline {
    long version;
    V values[HLINE_SIZE];
    K key_low; // lower bound (cannot be eq)
    K key_high; // upper bound (can be eq)
    rdma_ptr<hline> next;
    long is_deleted;
  };

  struct fency_keys {
    long version;
    long height;
    K key_low;
    K key_high;
    long is_deleted;
    char padding[(64 - sizeof(K) - sizeof(K) - sizeof(long) - sizeof(long) - sizeof(long))];
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

public:
  struct BNode;

  /// Configuration information
  struct alignas(64) BRoot {
    uint64_t lock;
    int height;
    rdma_ptr<BNode> start;

    long version() const {
      return lock & ~LOCK_BIT;
    }

    void increment_version(){
      lock = (lock & ~LOCK_BIT) + 1;
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
    fency_keys fency_key;

  public:
    BNode(){
      fency_key.is_deleted = 0xDEADDEADDEADDEAD;
      fency_key.version = 0;
      fency_key.key_low = INT_MIN; // neither INT_MIN nor INT_MAX are valid keys!
      fency_key.key_high = SENTINEL;
      fency_key.height = -1;
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

    void mark_deleted(){
      fency_key.is_deleted = 0;
    }

    bool is_deleted() const {
      return fency_key.is_deleted != 0xDEADDEADDEADDEAD;
    }

    /// Checks if the version is valid
    bool is_valid(bool ignore_lock = false) const {
      if (is_deleted()) return false;
      long base = key_lines[0].version;
      if (ignore_lock) base = base & ~LOCK_BIT;
      for(int i = 0; i < KLINES; i++){
        if (key_lines[i].version != base) return false;
      }
      for(int i = 0; i < PLINES; i++){
        if (ptr_lines[i].version != base) return false;
      }
      if (fency_key.version != base) return false;
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
      fency_key.version++;
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

    /// Test if a key is in the range
    bool key_in_range(K key) const {
      return fency_key.key_low < key && key <= fency_key.key_high && !is_deleted();
    }

    bool key_in_range(K key, int height) const {
      return fency_key.key_low < key && key <= fency_key.key_high && height == fency_key.height && !is_deleted();
    }

    /// Get the lower bound for the leaf (exclusive)
    K key_low() const {
      return fency_key.key_low;
    }

    /// Get the upper bound for the leaf (inclusive)
    K key_high() const {
      return fency_key.key_high;
    }

    /// Set the range of the leaf
    void set_range(K key_low, K key_high){
      fency_key.key_low = key_low;
      fency_key.key_high = key_high;
    }

    long get_level(){
      return fency_key.height;
    }

    void set_level(long level){
      fency_key.height = level;
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
      last_line.key_low = INT_MIN; // neither INT_MIN nor INT_MAX are valid keys!
      last_line.key_high = SENTINEL;
      last_line.is_deleted = 0xDEADDEADDEADDEAD;
      for(int i = 0; i < SIZE; i++){
        set_key(i, SENTINEL);
        set_value(i, 0); // set just for debugging
      }
      set_next(nullptr);
    }

    bool is_deleted() const {
      return last_line.is_deleted != 0xDEADDEADDEADDEAD;
    }

    /// Checks if the version is valid
    bool is_valid(bool ignore_lock = false) const {
      if (is_deleted()) return false;
      long base = key_lines[0].version;
      if (ignore_lock) base = base & ~LOCK_BIT;
      for(int i = 1; i < KLINES; i++){
        if (key_lines[i].version != base) return false;
      }
      for(int i = 0; i < VLINES; i++){
        if (value_lines[i].version != base) return false;
      }
      if (last_line.version != base) return false;
      return true;
    }

    /// Test if a key is in the range
    bool key_in_range(K key) const {
      return last_line.key_low < key && key <= last_line.key_high && !is_deleted();
    }

    // mark as deleted
    void mark_deleted(){
      last_line.is_deleted = 0;
    }

    /// Get the lower bound for the leaf (exclusive)
    K key_low() const {
      return last_line.key_low;
    }

    /// Get the upper bound for the leaf (inclusive)
    K key_high() const {
      return last_line.key_high;
    }

    /// Set the range of the leaf
    void set_range(K key_low, K key_high){
      last_line.key_low = key_low;
      last_line.key_high = key_high;
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

private:

  typedef rdma_ptr<BLeaf> bleaf_ptr;
  typedef rdma_ptr<BNode> bnode_ptr;
  using EBRLeaf = EBRObjectPool<BLeaf, 100, capability>;
  using EBRNode = EBRObjectPoolAccompany<BNode, BLeaf, 100, capability>;
  using Cache = RemoteCacheImpl<capability>;
  using Index = IndexCache<BNode, DEGREE, K>;
  
  Peer self_;
  rdma_ptr<BRoot> root;
  Cache* cache;
  Index* index;
  ShermanRoot<K>* backup_index;
  EBRLeaf* ebr_leaf;
  EBRNode* ebr_node;

  template <typename T> inline bool is_local(rdma_ptr<T> ptr) {
    return ptr.id() == self_.id;
  }
  
  // preallocated memory for RDMA operations (avoiding frequent allocations)
  rdma_ptr<uint64_t> temp_lock;
  rdma_ptr<BRoot> prealloc_root_r;
  rdma_ptr<BRoot> prealloc_root_w;
  rdma_ptr<BNode> prealloc_node_w;
  rdma_ptr<BLeaf> prealloc_leaf_r1;
  rdma_ptr<BLeaf> prealloc_leaf_r2;
  rdma_ptr<BLeaf> prealloc_leaf_w;

  /// returns true if we can acquire the version of the node
  template <class ptr_t>
  bool try_acquire(capability* pool, rdma_ptr<ptr_t> node, long version){
    // swap the first 8 bytes (the lock) from unlocked to locked
    uint64_t v = pool->template CompareAndSwap<uint64_t>(static_cast<rdma_ptr<uint64_t>>(node), version & ~LOCK_BIT, version | LOCK_BIT);
    if (v == (version & ~LOCK_BIT)) return true; // I think the lock bit will never be set in this scenario since we'd detect a broken read primarily
    return false;
  }

  template <class ptr_t>
  void release(capability* pool, rdma_ptr<ptr_t> node, long version){
    pool->template Write<uint64_t>(static_cast<rdma_ptr<uint64_t>>(node), version, temp_lock, internal::RDMAWriteBehavior::RDMAWriteWithNoAck);
  }

  enum ReadBehavior {
    IGNORE_LOCK = 0,
    LOOK_FOR_SPLIT_MERGE = 1,
    WILL_NEED_ACQUIRE = 2,
  };

  template <class ptr_t>
  inline CachedObject<ptr_t> reliable_read(rdma_ptr<ptr_t> node, ReadBehavior behavior, rdma_ptr<ptr_t> prealloc = nullptr){
    CachedObject<ptr_t> obj = cache->template Read<ptr_t>(node, prealloc);
    if (behavior == IGNORE_LOCK){
      while(!obj->is_valid(true)){
        if (obj->is_deleted()) return obj;
        obj = cache->template Read<ptr_t>(node, prealloc);
      }
    } else if (behavior == LOOK_FOR_SPLIT_MERGE) {
      bool should_ignore_lock = (obj->key_at(SIZE - 1) != SENTINEL) || (obj->key_at(0) == SENTINEL);
      while(!obj->is_valid(should_ignore_lock)){
        if (obj->is_deleted()) return obj;
        obj = cache->template Read<ptr_t>(node, prealloc);
        should_ignore_lock = (obj->key_at(SIZE - 1) != SENTINEL) || (obj->key_at(0) == SENTINEL);
      }
    } else { // WILL_NEED_ACQUIRE
      while(!obj->is_valid(false)){
        if (obj->is_deleted()) return obj;
        obj = cache->template Read<ptr_t>(node, prealloc);
      }
    }
    return obj;
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

  /// Shift everything from index down, overwriting the key at index and the ptr at index + 1
  /// Returns the key it overwrote
  inline K shift_down(BNode* bnode, int index){
    K original_key = bnode->key_at(index);
    for(int i = index; i < SIZE - 1; i++){
      bnode->set_key(i, bnode->key_at(i + 1));
    }
    bnode->set_key(SIZE - 1, SENTINEL); // reset last key slot
    for(int i = index + 1; i < SIZE; i++){
      bnode->set_ptr(i, bnode->ptr_at(i + 1));
    }
    bnode->set_ptr(SIZE, nullptr);
    return original_key;
  }

  /// At root, splitting node_p into two with a new parent
  void split_node(capability* pool, CachedObject<BRoot>& parent_p, CachedObject<BNode>& node_p){
    BRoot parent = *parent_p;
    BNode node = *node_p;

    // Full node so split
    bnode_ptr new_parent = ebr_node->allocate(pool);
    parent.set_start_and_inc(new_parent);

    bnode_ptr new_neighbor = ebr_node->allocate(pool);
    K to_parent;
    if (pool->template is_local(new_neighbor)){
      *new_neighbor = BNode();
      to_parent = split_keys<BNode*, bnode_ptr>(&node, new_neighbor, true);
      split_ptrs(&node, (BNode*) new_neighbor);
      new_neighbor->set_range(to_parent, node.key_high());
      new_neighbor->set_level(node.get_level());
    } else {
      BNode new_neighbor_local = BNode();
      to_parent = split_keys<BNode*, BNode*>(&node, &new_neighbor_local, true);
      split_ptrs(&node, &new_neighbor_local);
      cache->template Write<BNode>(new_neighbor, new_neighbor_local, prealloc_node_w);
      new_neighbor_local.set_range(to_parent, node.key_high());
      new_neighbor_local.set_level(node.get_level());
    }
    node.set_range(node.key_low(), to_parent);

    if (pool->template is_local(new_parent)){
      *new_parent = BNode();
      new_parent->set_level(parent.height);
      new_parent->set_key(0, to_parent);
      new_parent->set_ptr(0, node_p.remote_origin());
      new_parent->set_ptr(1, new_neighbor);
    } else {
      BNode new_parent_local = BNode();
      new_parent_local.set_level(parent.height);
      new_parent_local.set_key(0, to_parent);
      new_parent_local.set_ptr(0, node_p.remote_origin());
      new_parent_local.set_ptr(1, new_neighbor);
      cache->template Write<BNode>(new_parent, new_parent_local, prealloc_node_w);
    }

    parent.increment_version();
    node.increment_version(); // increment version before writing
    cache->template Write<BRoot>(parent_p.remote_origin(), parent, prealloc_root_w);      
    cache->template Write<BNode>(node_p.remote_origin(), node, prealloc_node_w);
  }

  /// Move
  void split_node(capability* pool, CachedObject<BRoot>& parent_p, CachedObject<BLeaf>& node_p){
    BRoot parent = *parent_p;
    BLeaf node = *node_p;

    // Full node so split
    bnode_ptr new_parent = ebr_node->allocate(pool);
    parent.set_start_and_inc(new_parent);

    bleaf_ptr new_neighbor = ebr_leaf->allocate(pool);
    K to_parent;
    K key_low = node.key_low(), key_high = node.key_high();
    if (pool->template is_local(new_neighbor)){
      *new_neighbor = BLeaf();
      to_parent = split_keys<BLeaf*, bleaf_ptr>(&node, new_neighbor, false);
      split_values(&node, (BLeaf*) new_neighbor);
      new_neighbor->set_range(to_parent, key_high);
    } else {
      BLeaf new_leaf = BLeaf();
      to_parent = split_keys<BLeaf*, BLeaf*>(&node, &new_leaf, false);
      split_values(&node, &new_leaf);
      new_leaf.set_range(to_parent, key_high);
      cache->template Write<BLeaf>(new_neighbor, new_leaf, prealloc_leaf_w);
    }
    node.set_next(new_neighbor);
    node.set_range(key_low, to_parent);

    if (pool->template is_local(new_parent)){
      *new_parent = BNode();
      new_parent->set_level(parent.height);
      new_parent->set_key(0, to_parent);
      new_parent->set_ptr(0, static_cast<bnode_ptr>(node_p.remote_origin()));
      new_parent->set_ptr(1, static_cast<bnode_ptr>(new_neighbor));
    } else {
      BNode new_parent_local = BNode();
      new_parent_local.set_level(parent.height);
      new_parent_local.set_key(0, to_parent);
      new_parent_local.set_ptr(0, static_cast<bnode_ptr>(node_p.remote_origin()));
      new_parent_local.set_ptr(1, static_cast<bnode_ptr>(new_neighbor));
      cache->template Write<BNode>(new_parent, new_parent_local, prealloc_node_w);
    }
  
    parent.increment_version();
    node.increment_version();
    cache->template Write<BRoot>(parent_p.remote_origin(), parent, prealloc_root_w);
    cache->template Write<BLeaf>(node_p.remote_origin(), node, prealloc_leaf_w);
  }

  /// Not at root
  void split_node(capability* pool, CachedObject<BNode>& parent_p, CachedObject<BNode>& node_p){
    BNode parent = *parent_p;
    BNode node = *node_p;
    
    // Full node so split
    bnode_ptr new_neighbor = ebr_node->allocate(pool);
    K to_parent;
    if (pool->template is_local(new_neighbor)){
      *new_neighbor = BNode();
      to_parent = split_keys<BNode*, bnode_ptr>(&node, new_neighbor, true);
      split_ptrs(&node, (BNode*) new_neighbor);
      new_neighbor->set_range(to_parent, node.key_high());
      new_neighbor->set_level(node.get_level());
    } else {
      BNode new_neighbor_local = BNode();
      to_parent = split_keys<BNode*, BNode*>(&node, &new_neighbor_local, true);
      split_ptrs(&node, &new_neighbor_local);
      cache->template Write<BNode>(new_neighbor, new_neighbor_local, prealloc_node_w);
      new_neighbor_local.set_range(to_parent, node.key_high());
      new_neighbor_local.set_level(node.get_level());
    }
    node.set_range(node.key_low(), to_parent);

    int bucket = search_node<BNode>(parent_p, to_parent);
    REMUS_ASSERT_DEBUG(bucket != -1, "Implies a full parent");

    shift_up(&parent, bucket);
    parent.set_key(bucket, to_parent);
    parent.set_ptr(bucket + 1, new_neighbor);

    parent.increment_version(); // increment version before writing
    node.increment_version();
    // unmark parent and node in conjuction with splitting
    cache->template Write<BNode>(parent_p.remote_origin(), parent, prealloc_node_w);
    cache->template Write<BNode>(node_p.remote_origin(), node, prealloc_node_w);
  }

  /// At leaf. Try to split the leaf into two and move a key to the parent
  BLeaf split_node(capability* pool, CachedObject<BNode>& parent_p, CachedObject<BLeaf>& node_p){
    BNode parent = *parent_p;
    BLeaf node = *node_p;

    // Full node so split
    bleaf_ptr new_neighbor = ebr_leaf->allocate(pool);
    K to_parent;
    K key_low = node.key_low(), key_high = node.key_high();
    
    if (pool->template is_local(new_neighbor)){
      *new_neighbor = BLeaf();
      to_parent = split_keys<BLeaf*, bleaf_ptr>(&node, new_neighbor, false);
      split_values(&node, (BLeaf*) new_neighbor);
      new_neighbor->set_next(node.get_next());
      new_neighbor->set_range(to_parent, key_high);
      new_neighbor->lock(); // start locked
    } else {
      BLeaf new_leaf = BLeaf();
      to_parent = split_keys<BLeaf*, BLeaf*>(&node, &new_leaf, false);
      split_values(&node, &new_leaf);
      new_leaf.set_next(node.get_next());
      new_leaf.set_range(to_parent, key_high);
      new_leaf.lock(); // start locked
      cache->template Write<BLeaf>(new_neighbor, new_leaf, prealloc_leaf_w);
    }
    node.set_next(new_neighbor);
    node.set_range(key_low, to_parent);

    int bucket = search_node<BNode>(parent_p, to_parent);
    REMUS_ASSERT(bucket != -1, "Implies a full parent");

    shift_up(&parent, bucket);
    parent.set_key(bucket, to_parent);
    parent.set_ptr(bucket + 1, static_cast<bnode_ptr>(new_neighbor));
    parent.increment_version();
    node.increment_version();

    cache->template Write<BNode>(parent_p.remote_origin(), parent, prealloc_node_w);
    // leave the leaf unmodified and locked for further use
    return node;
  }

  /// Combine two neighboring bnodes in either order. Also modify the parent
  /// Returns true if deallocating one instead of two
  bool merge_node(BNode* node_one, int bucket_one, BNode* node_two, int bucket_two, BNode* parent){
    REMUS_ASSERT_DEBUG(node_one->key_at(1) == SENTINEL && node_two->key_at(SIZE - 1) == SENTINEL, "Space is assured");
    bool one_gone = bucket_one != 0;
    if (bucket_one != 0){
      REMUS_ASSERT_DEBUG(bucket_two == bucket_one - 1, "Sanity check");
      std::swap(node_one, node_two);
      std::swap(bucket_one, bucket_two);
    }
    // then (node_one <- node_two) and node_two is removed
    K lower_key = shift_down(parent, bucket_one);
    int size_one = 0;
    while(node_one->key_at(size_one) != SENTINEL) size_one++;
    node_one->set_key(size_one, lower_key);
    node_one->set_ptr(size_one + 1, node_two->ptr_at(0)); // get the merged ptr
    size_one++;
    node_one->set_range(node_one->key_low(), node_two->key_high());

    for(int i = 0; i < SIZE; i++){
      if (node_two->key_at(i) == SENTINEL) break;
      node_one->set_key(size_one, node_two->key_at(i));
      node_one->set_ptr(size_one + 1, node_two->ptr_at(i + 1));
      size_one++;
    }
    node_two->mark_deleted();
    return one_gone;
  }

  /// Combine two neighboring leafs in either order. Also modify the parent
  /// Returns true if deallocating one instead of two
  bool merge_leaf(BLeaf* leaf_one, int bucket_one, BLeaf* leaf_two, int bucket_two, BNode* parent){
    REMUS_ASSERT_DEBUG(leaf_one->key_at(1) == SENTINEL && leaf_two->key_at(SIZE - 1) == SENTINEL, "Space is assured");
    bool one_gone = bucket_one != 0;
    if (bucket_one != 0){
      REMUS_ASSERT_DEBUG(bucket_two == bucket_one - 1, "Sanity check");
      std::swap(leaf_one, leaf_two);
      std::swap(bucket_one, bucket_two);
    }
    // then (leaf_one <- leaf_two) and leaf_two is removed
    leaf_one->set_next(leaf_two->get_next());
    leaf_one->set_range(leaf_one->key_low(), leaf_two->key_high());
    int size_one = 0;
    for(int i = 0; i < SIZE; i++){
      if (leaf_two->key_at(i) == SENTINEL) break;
      while(leaf_one->key_at(size_one) != SENTINEL) size_one++;
      leaf_one->set_key(size_one, leaf_two->key_at(i));
      leaf_one->set_value(size_one, leaf_two->value_at(i));
    }
    shift_down(parent, bucket_one);
    leaf_two->mark_deleted();
    return one_gone;
  }

  inline bnode_ptr read_level(BNode* curr, int bucket){
    bnode_ptr next_level = curr->ptr_at(SIZE);
    if (bucket != -1) next_level = curr->ptr_at(bucket);
    REMUS_ASSERT_DEBUG(next_level != nullptr, "Accessing SENTINEL's ptr");
    return next_level;
  }

  CachedObject<BLeaf> traverse(capability* pool, K key, bool modifiable, function<void(BLeaf*, int)> effect, int it_counter = 0){
    REMUS_ASSERT(it_counter < 1000, "Too many retries! Infinite recursion detected?");

    rdma_ptr<BNode> tmp;
    const CacheEntry<BNode>* entry = index->search_from_cache(key, &tmp, is_leader);
    if (entry != nullptr){
      bleaf_ptr l = static_cast<bleaf_ptr>(tmp);
      CachedObject<BLeaf> leaf = reliable_read<BLeaf>(l, modifiable ? WILL_NEED_ACQUIRE : IGNORE_LOCK, prealloc_leaf_r1);
      // leaf has room and is in range
      if (leaf->key_in_range(key)){ // guard against deleted and in range
        if (modifiable){
          // do insert/remove procedure
          if (leaf->key_at(SIZE - 1) == SENTINEL){ // check enough space for ops
            if (try_acquire<BLeaf>(pool, leaf.remote_origin(), leaf->version())){ // acquire the leaf
              // modifiable so lock, update, write back
              BLeaf leaf_updated = *leaf;
              effect(&leaf_updated, search_node<BLeaf>(&leaf_updated, key));
              leaf_updated.increment_version();
              cache->template Write<BLeaf>(leaf.remote_origin(), leaf_updated, prealloc_leaf_w, internal::RDMAWriteBehavior::RDMAWriteWithNoAck);
              return leaf;
            } else {
              // retry if we cannot acquire lock
              return traverse(pool, key, modifiable, effect);
            }
          } // else -> not enough space, need full traversal
        } else {
          // do contains procedure
          return leaf;
        }
      } else {
        // failed traversal...
        index->invalidate(entry);
      }
    }

    int height;
    bnode_ptr next_level;

    rdma_ptr<anon_ptr> ccptr = backup_index->find_ptr(key);
    CachedObject<BNode> curr;
    CachedObject<BNode> parent;
    bool just_read_root = false;
    if (ccptr != nullptr){
      bnode_ptr next_node = static_cast<bnode_ptr>(ccptr);
      curr = reliable_read<BNode>(next_node, modifiable ? LOOK_FOR_SPLIT_MERGE : IGNORE_LOCK);
      BNode n = *curr;
      if(!n.key_in_range(key)){ // guard against deleted
        backup_index->invalidate(key);
        goto was_bad;
      } else {
        height = n.get_level();
        if (curr->key_at(SIZE - 1) != SENTINEL){ // full parent, gonna need root node for split
          goto was_bad;
        }
      }
    } else {
      was_bad:
      // read root first
      CachedObject<BRoot> curr_root = cache->template Read<BRoot>(root, prealloc_root_r);
      height = curr_root->height;
      next_level = curr_root->start;
      REMUS_ASSERT_DEBUG(next_level != nullptr, "Accessing SENTINEL's ptr");

      if (height == 0){
        CachedObject<BLeaf> next_leaf = reliable_read<BLeaf>(static_cast<bleaf_ptr>(next_level), modifiable ? WILL_NEED_ACQUIRE : IGNORE_LOCK, prealloc_leaf_r1);
        if (modifiable){
          // failed to get locks, retraverse
          if (!try_acquire<BRoot>(pool, curr_root.remote_origin(), curr_root->version())) return traverse(pool, key, modifiable, effect);
          if (!try_acquire<BLeaf>(pool, next_leaf.remote_origin(), next_leaf->version())) {
            release<BRoot>(pool, curr_root.remote_origin(), curr_root->version());
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
            release<BRoot>(pool, curr_root.remote_origin(), curr_root->version());
            cache->template Write<BLeaf>(next_leaf.remote_origin(), leaf_updated, prealloc_leaf_w);
          }
        }

        // Made it to the leaf
        return next_leaf;
      } // if statement returns

      // Traverse bnode until bleaf
      curr = reliable_read<BNode>(next_level, modifiable ? LOOK_FOR_SPLIT_MERGE : IGNORE_LOCK);
      // if split, we updated the root and we need to retraverse
      if (modifiable && curr->key_at(SIZE - 1) != SENTINEL){
        if (!try_acquire<BRoot>(pool, curr_root.remote_origin(), curr_root->version())) return traverse(pool, key, modifiable, effect);
        if (!try_acquire<BNode>(pool, curr.remote_origin(), curr->version())) {
          release<BRoot>(pool, curr_root.remote_origin(), curr_root->version());
          return traverse(pool, key, modifiable, effect);
        }
        split_node(pool, curr_root, curr);
        return traverse(pool, key, modifiable, effect);
      } else if (modifiable && curr->key_at(0) == SENTINEL){
        // Current is empty, remove a level
        if (try_acquire<BRoot>(pool, curr_root.remote_origin(), curr_root->version())){
          if (try_acquire<BNode>(pool, curr.remote_origin(), curr->version())){
            // Update the root
            BRoot new_root = *curr_root;
            new_root.height--;
            new_root.start = curr->ptr_at(0);
            new_root.increment_version();

            cache->template Write<BRoot>(curr_root.remote_origin(), new_root, prealloc_root_w);
            BNode new_curr = *curr;
            new_curr.mark_deleted();
            new_curr.increment_version();
            cache->template Write<BNode>(curr.remote_origin(), new_curr);
            ebr_node->deallocate(curr.remote_origin()); // deallocate the unlinked node
          } else {
            release<BRoot>(pool, curr_root.remote_origin(), curr_root->version()); // continue traversing, we failed to lower a level
          }
        }
        return traverse(pool, key, modifiable, effect); // retraverse
      }

      just_read_root = true;  
    }

    int bucket = search_node<BNode>(curr, key);
    while(height != 1){
      // Get the next level ptr
      next_level = read_level((BNode*) curr.get(), bucket);

      // Read it as a BNode
      parent = std::move(curr);
      curr = reliable_read<BNode>(next_level, modifiable ? LOOK_FOR_SPLIT_MERGE : IGNORE_LOCK);
      if (just_read_root){
        backup_index->put_into_cache(curr->key_low(), curr->key_high(), static_cast<rdma_ptr<anon_ptr>>(next_level));
      }
      just_read_root = false;

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
        curr = reliable_read<BNode>(parent.remote_origin(), modifiable ? LOOK_FOR_SPLIT_MERGE : IGNORE_LOCK);
        bucket = search_node<BNode>(curr, key);
        continue;
      } else if (modifiable && curr->key_at(0) == SENTINEL && parent->key_at(0) != SENTINEL){
        // Empty node with a neighbor, try to remove it
        int bucket_neighbor;
        rdma_ptr<BNode> merge_node_ptr;
        if (bucket == 0) {
          merge_node_ptr = read_level((BNode*) parent.get(), 1);
          bucket_neighbor = 1;
        } else {
          merge_node_ptr = read_level((BNode*) parent.get(), bucket - 1);
          bucket_neighbor = bucket - 1;
        }
        CachedObject<BNode> merging_node = reliable_read<BNode>(merge_node_ptr, WILL_NEED_ACQUIRE);
        if (merging_node->key_at(SIZE - 1) == SENTINEL){ // there is room in neighbor for the ptr
          if (try_acquire<BNode>(pool, parent.remote_origin(), parent->version())) {
            if (try_acquire<BNode>(pool, curr.remote_origin(), curr->version())) {
              if (try_acquire<BNode>(pool, merging_node.remote_origin(), merging_node->version())){
                // cannot acquire child so release parent and self and give up
                BNode empty_node = *curr;
                BNode merged_node = *merging_node;
                BNode parent_of_merge = *parent;
                if (merge_node(&empty_node, bucket, &merged_node, bucket_neighbor, &parent_of_merge)){
                  ebr_node->deallocate(curr.remote_origin());
                } else {
                  ebr_node->deallocate(merging_node.remote_origin());
                }
                empty_node.increment_version();
                merged_node.increment_version();
                parent_of_merge.increment_version();
                cache->template Write<BNode>(curr.remote_origin(), empty_node, prealloc_node_w);
                cache->template Write<BNode>(merging_node.remote_origin(), merged_node, prealloc_node_w);
                cache->template Write<BNode>(parent.remote_origin(), parent_of_merge, prealloc_node_w);
                // re-read the parent and continue
                curr = reliable_read<BNode>(parent.remote_origin(), LOOK_FOR_SPLIT_MERGE);
                bucket = search_node<BNode>(curr, key);
                continue;
              } else {
                // cannot acquire child so release parent and self and give up
                release<BNode>(pool, parent.remote_origin(), parent->version());
                release<BNode>(pool, curr.remote_origin(), curr->version());
              }
            } else {
              // cannot acquire child so release parent and try again
              release<BNode>(pool, parent.remote_origin(), parent->version());
            }
          }
        }
        // give-up b/c failed somewhere
      }
      bucket = search_node<BNode>(curr, key);
      height--;
    }

    // Get the next level ptr
    next_level = read_level((BNode*) curr.get(), bucket);
    bleaf_ptr next_leaf = static_cast<bleaf_ptr>(next_level);
    CachedObject<BLeaf> leaf = reliable_read<BLeaf>(next_leaf, modifiable ? WILL_NEED_ACQUIRE : IGNORE_LOCK, prealloc_leaf_r1);
    if (!leaf->key_in_range(key)){
      // key is out of the range, we traversed wrong
      return traverse(pool, key, modifiable, effect, it_counter + 1);
    }
    // add to cache on traversal
    index->add_to_cache((const BNode*) curr.get());

    // Handle updates to the leaf
    if (modifiable){
      // acquire parent and current leaf
      if (try_acquire<BLeaf>(pool, leaf.remote_origin(), leaf->version())){
        if (leaf->key_at(SIZE - 1) == SENTINEL && (leaf->key_at(0) != SENTINEL || curr->key_at(0) == SENTINEL)){
          // modifiable so lock, update, write back
          BLeaf leaf_updated = *leaf;
          effect(&leaf_updated, search_node<BLeaf>(&leaf_updated, key));
          leaf_updated.increment_version();
          cache->template Write<BLeaf>(leaf.remote_origin(), leaf_updated, prealloc_leaf_w, internal::RDMAWriteBehavior::RDMAWriteWithNoAck);
          return leaf;
        } else {
          if (try_acquire<BNode>(pool, curr.remote_origin(), curr->version())){
            if (leaf->key_at(SIZE - 1) != SENTINEL){
              // should split
              BLeaf leaf_updated = split_node(pool, curr, leaf); // todo: are we sure we can unlock parent before writing?
              next_leaf = leaf_updated.get_next(); // next_leaf is readonly since we left it locked
              bucket = search_node<BLeaf>(&leaf_updated, key);
              if (!leaf_updated.key_in_range(key)) {
                // write and unlock the prev
                cache->template Write<BLeaf>(leaf.remote_origin(), leaf_updated, prealloc_leaf_w);
                // key goes into next
                CachedObject<BLeaf> next_leaf_local_const = reliable_read<BLeaf>(next_leaf, IGNORE_LOCK, prealloc_leaf_r2);
                BLeaf next_leaf_local = *next_leaf_local_const;
                effect(&next_leaf_local, search_node<BLeaf>(next_leaf_local_const, key)); // modify the next
                next_leaf_local.increment_version();
                cache->template Write<BLeaf>(next_leaf, next_leaf_local, prealloc_leaf_w, internal::RDMAWriteBehavior::RDMAWriteWithNoAck);
                return leaf;
              } else {
                // key goes into current, unlock next
                release<BLeaf>(pool, next_leaf, 0);
                effect(&leaf_updated, bucket);
                cache->template Write<BLeaf>(leaf.remote_origin(), leaf_updated, prealloc_leaf_w, internal::RDMAWriteBehavior::RDMAWriteWithNoAck);
                return leaf;
              }
            } else if (leaf->key_at(0) == SENTINEL && curr->key_at(0) != SENTINEL){
              // Empty node with a neighbor, try to remove it
              int bucket_neighbor;
              rdma_ptr<BLeaf> merge_leaf_ptr;
              if (bucket == 0) {
                merge_leaf_ptr = static_cast<bleaf_ptr>(read_level((BNode*) curr.get(), 1));
                bucket_neighbor = 1;
              } else {
                merge_leaf_ptr = static_cast<bleaf_ptr>(read_level((BNode*) curr.get(), bucket - 1));
                bucket_neighbor = bucket - 1;
              }

              CachedObject<BLeaf> merging_leaf;
              bool do_merge = true;
              while(true){ // aggressively acquire the node since we only compete with simple insert/remove
                merging_leaf = reliable_read<BLeaf>(merge_leaf_ptr, WILL_NEED_ACQUIRE, prealloc_leaf_r2);
                if (merging_leaf->key_at(SIZE - 1) != SENTINEL) {
                  do_merge = false; // give up because merging will cause inserts to fail!
                  break;
                }
                // ? if we add linearizable range queries, we prob need to re-traverse instead to prevent deadlock!
                if (try_acquire(pool, merging_leaf.remote_origin(), merging_leaf->version())) break;
              }

              if (!do_merge){
                // lock, update, write back
                BLeaf leaf_updated = *leaf;
                effect(&leaf_updated, search_node<BLeaf>(&leaf_updated, key));
                leaf_updated.increment_version();
                cache->template Write<BLeaf>(leaf.remote_origin(), leaf_updated, prealloc_leaf_w, internal::RDMAWriteBehavior::RDMAWriteWithNoAck);
                release<BNode>(pool, curr.remote_origin(), curr->version()); // release parent
                return leaf;
              }
              BLeaf empty_leaf = *leaf;
              effect(&empty_leaf, 0);
              BLeaf merged_leaf = *merging_leaf;
              BNode parent_of_merge = *curr;
              if (merge_leaf(&empty_leaf, bucket, &merged_leaf, bucket_neighbor, &parent_of_merge)){
                ebr_leaf->deallocate(leaf.remote_origin());
                empty_leaf.increment_version();
                merged_leaf.increment_version();
                parent_of_merge.increment_version();
                cache->template Write<BLeaf>(merging_leaf.remote_origin(), merged_leaf, prealloc_leaf_w);
                cache->template Write<BLeaf>(leaf.remote_origin(), empty_leaf, prealloc_leaf_w, internal::RDMAWriteBehavior::RDMAWriteWithNoAck);
                cache->template Write<BNode>(curr.remote_origin(), parent_of_merge, prealloc_node_w, internal::RDMAWriteBehavior::RDMAWriteWithNoAck);
              } else {
                ebr_leaf->deallocate(merging_leaf.remote_origin());
                empty_leaf.increment_version();
                merged_leaf.increment_version();
                parent_of_merge.increment_version();
                cache->template Write<BLeaf>(leaf.remote_origin(), empty_leaf, prealloc_leaf_w);
                cache->template Write<BLeaf>(merging_leaf.remote_origin(), merged_leaf, prealloc_leaf_w, internal::RDMAWriteBehavior::RDMAWriteWithNoAck);
                cache->template Write<BNode>(curr.remote_origin(), parent_of_merge, prealloc_node_w, internal::RDMAWriteBehavior::RDMAWriteWithNoAck);
              }
              return leaf;
            } else {
              REMUS_ASSERT(false, "Unreachable");
            }
          } else {
            // failed to acquire leaf, release
            release<BLeaf>(pool, leaf.remote_origin(), leaf->version());
          }
        }
      } // else nothing was acquired, try again
    } else {
      // not modifiable and no split needed
      return leaf;
    }
    return traverse(pool, key, modifiable, effect, it_counter + 1); // retry by retraversing
  }

public:
  ShermanBPTree(Peer& self, Cache* cache, Index* index, capability* pool, EBRLeaf* leaf, EBRNode* node, bool print_info = false) 
  : self_(std::move(self)), cache(cache), index(index), ebr_leaf(leaf), ebr_node(node) {
    if (print_info){
      REMUS_INFO("Sentinel = {}", SENTINEL);
      REMUS_INFO("Struct Memory (BNode): {} % 64 = {}", sizeof(BNode), sizeof(BNode) % 64);
      REMUS_INFO("Struct Memory (BLeaf): {} % 64 = {}", sizeof(BLeaf), sizeof(BLeaf) % 64);
    }

    this->is_leader = is_leader_gen.exchange(false);
    REMUS_INFO("Stored is_leader={}", is_leader);
    
    temp_lock = pool->template Allocate<uint64_t>(8); // allocates 8 to ensure size of object=64
    // prealloc some memory
    prealloc_root_r = pool->template Allocate<BRoot>();
    prealloc_root_w = pool->template Allocate<BRoot>();
    prealloc_node_w = pool->template Allocate<BNode>();
    prealloc_leaf_r1 = pool->template Allocate<BLeaf>();
    prealloc_leaf_r2 = pool->template Allocate<BLeaf>();
    prealloc_leaf_w = pool->template Allocate<BLeaf>();

    backup_index = new ShermanRoot<K>(SIZE + 1);
  };

  /// Free all the resources associated with the IHT
  void destroy(capability* pool) {
    pool->template Deallocate<uint64_t>(temp_lock, 8);
    pool->template Deallocate<BRoot>(prealloc_root_r);
    pool->template Deallocate<BRoot>(prealloc_root_w);
    pool->template Deallocate<BNode>(prealloc_node_w);
    pool->template Deallocate<BLeaf>(prealloc_leaf_r1);
    pool->template Deallocate<BLeaf>(prealloc_leaf_r2);
    pool->template Deallocate<BLeaf>(prealloc_leaf_w);

    if (pool->template is_local(root)){
      BRoot tmp_root = *root;
      pool->template Deallocate<BRoot>(root);
      if (tmp_root.height == 0 && pool->template is_local(tmp_root.start)){
         pool->template Deallocate<BLeaf>(static_cast<bleaf_ptr>(tmp_root.start));
      }
    }

    delete backup_index;
  }

  /// @brief Create a fresh iht
  /// @param pool the capability to init the IHT with
  /// @return the iht root pointer
  rdma_ptr<anon_ptr> InitAsFirst(capability* pool){
    REMUS_ASSERT(56 % sizeof(V) == 0, "V must be a multiple of 2,4,8,14,28,56");
    rdma_ptr<BRoot> broot = pool->template Allocate<BRoot>();
    broot->height = 0; // set height to 1
    broot->lock = 0; // initialize lock
    this->root = broot;
    bleaf_ptr bleaf = pool->template Allocate<BLeaf>();
    *bleaf = BLeaf();
    this->root->start = static_cast<bnode_ptr>(bleaf);
    return static_cast<rdma_ptr<anon_ptr>>(this->root);
  }

  /// @brief Initialize an IHT from the pointer of another IHT
  /// @param root_ptr the root pointer of the other iht from InitAsFirst();
  void InitFromPointer(rdma_ptr<anon_ptr> root_ptr){
    this->root = static_cast<rdma_ptr<BRoot>>(root_ptr);
  }

  /// @brief Gets a value at the key.
  /// @param pool the capability providing one-sided RDMA
  /// @param key the key to search on
  /// @return an optional containing the value, if the key exists
  std::optional<V> contains(capability* pool, K key) {
    CachedObject<BLeaf> leaf = traverse(pool, key, false, function([=](BLeaf*, int){}));
    int bucket = search_node<BLeaf>(leaf, key);
    ebr_leaf->match_version(pool);
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
        REMUS_ASSERT_DEBUG(new_leaf->key_at(SIZE - 1) == SENTINEL, "Splitting failed! Effect doesn't accept full node");
        // shift up to make room for the KV (if possible)
        for(int i = SIZE - 1; i > bucket; i--) new_leaf->set_value(i, new_leaf->value_at(i - 1));
        for(int i = SIZE - 1; i > bucket; i--) new_leaf->set_key(i, new_leaf->key_at(i - 1));
        new_leaf->set_key(bucket, key);
        new_leaf->set_value(bucket, value);
      } else {
        prev_value = optional(new_leaf->value_at(bucket));
      }
    }));
    ebr_leaf->match_version(pool);
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
        // remove the old
        new_leaf->set_key(SIZE - 1, SENTINEL);
      }
    }));
    ebr_leaf->match_version(pool);
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

    if (PRINT_ADDR)
      std::cout << "Leaf<next=" << std::hex << leaf.get_next().address() << std::dec << "> ";
    else
      std::cout << "Leaf<low=" << leaf.key_low() << "  high=" << leaf.key_high() << "> ";
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
    std::cout << (n.is_valid() ? "" : "invalid ") << "(" << n.version() << ", " << height << ", " << index << ")";
    std::cout << " => ";
    std::cout << "Node<low=" << n.key_low() << "  high=" << n.key_high() << "  height=" << n.get_level() << "> ";
    if (PRINT_ADDR)
      std::cout << std::hex << n.ptr_at(0).address() << std::dec;
    for(int i = 0; i < SIZE; i++){
      if (PRINT_ADDR){
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

  /// Single threaded print
  void debug(){
    BRoot r = *root;
    std::cout << "Root(height=" << r.height << ")" << std::endl;
    debug(r.height, r.start, 0, 0);
    std::cout << std::endl;
  }

  void check_bounds(BNode* bnode, int height){
    if (height != 1) {
      for(int i = 0; i < SIZE + 1; i++){
        rdma_ptr<BNode> next = bnode->ptr_at(i);
        if (next != nullptr){
          check_bounds((BNode*) next, height - 1);
        }
      }
    } else {
      K last_upper_bound = INT_MIN;
      for(int i = 0; i < SIZE + 1; i++){
        BLeaf* leaf = (BLeaf*) bnode->ptr_at(i).raw();
        if (leaf != nullptr){
          if (last_upper_bound != leaf->key_low() && last_upper_bound != INT_MIN){
            REMUS_ERROR("Error at " + std::to_string(last_upper_bound));
            abort();
          }
          last_upper_bound = leaf->key_high();
        }
      }
    }
  }

  void check_bounds(){
    BRoot r = *root;
    if (r.height != 0){
      check_bounds((BNode*) r.start, r.height);
    }
  }

  string is_valid(int height, bnode_ptr node, K lower, K upper){
    if (height == 0){
      BLeaf l = *static_cast<bleaf_ptr>(node);
      if (l.get_next() != nullptr && l.key_high() != l.get_next()->key_low()) return "Key high is not next key low";
      if (l.get_next() == nullptr && l.key_high() != SENTINEL) return "Key high is not SENTINEL";
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

  /// No concurrent or thread safe (if everyone is readonly, then its fine). Counts the number of elements in the IHT
  int count(capability* pool){
    int height = cache->template Read<BRoot>(root)->height;
    REMUS_INFO("Final height = {}", height);

    // Get leftmost leaf by wrapping SENTINEL
    CachedObject<BLeaf> curr = traverse(pool, INT_MIN + 1, false, function([=](BLeaf*, int){}));
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

template <class V, int DEGREE, class capability> 
std::atomic<bool> ShermanBPTree<V, DEGREE, capability>::is_leader_gen = true;