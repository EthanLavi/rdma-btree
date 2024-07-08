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

#include "../../dcache/test/faux_mempool.h"

#include "../../common.h"
#include <optional>
#include <remus/rdma/rdma_ptr.h>

using namespace remus::rdma;

/// SIZE is DEGREE * 2
typedef CountingPool capability;
typedef int32_t K;
static const K SENTINEL = INT_MAX;

template <class V, int DEGREE> class RdmaBPTree {
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

  /// A line with keys
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

  /// returns true if we can acquire the version of the bnode
  bool try_acquire(capability* pool, bnode_ptr node, long version){
    // swap the first 8 bytes (the lock) from unlocked to locked
    uint64_t v = pool->CompareAndSwap<uint64_t>(static_cast<rdma_ptr<uint64_t>>(node), version, version | LOCK_BIT);
    if (v == version) return true;
    return false;
  }

  void release(capability* pool, bnode_ptr node, long version){
    pool->Write<uint64_t>(static_cast<rdma_ptr<uint64_t>>(node), version, temp_lock);
  }

  /// Allocate a middle layer node
  static bnode_ptr allocate_bnode(capability* pool){
    REMUS_ASSERT(56 % sizeof(V) == 0, "V must be a multiple of 2,4,8,14,28,56");
    bnode_ptr bnode = pool->Allocate<BNode>();
    *bnode = BNode();
    return bnode;
  }

  /// Allocate a leaf node with sentinel values and nullptr for next
  static bleaf_ptr allocate_bleaf(capability* pool){
    bleaf_ptr bleaf = pool->Allocate<BLeaf>();
    *bleaf = BLeaf();
    return bleaf;
  }

  /// Return the first index where the provided key is less than the element at that index 
  /// (-1 if the end element)
  template <class SRC>
  int search_node(const CachedObject<SRC>& origin, K key) {
    // todo: binary search
    for(int i = 0; i < SIZE; i++){
      if (key <= origin->key_at(i)){
        return i;
      }
    }
    return -1;
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

    node.increment_version(); // increment version before writing

    cache->Write<BRoot>(parent_p.remote_origin(), parent);      
    cache->Write<BNode>(node_p.remote_origin(), node);
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

    cache->Write<BRoot>(parent_p.remote_origin(), parent);
    cache->Write<BLeaf>(node_p.remote_origin(), node);
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

    cache->Write<BNode>(parent_p.remote_origin(), parent);
    cache->Write<BNode>(node_p.remote_origin(), node);
  }

  /// At leaf. Try to split the leaf into two and move a key to the parent
  void split_node(capability* pool, CachedObject<BNode>& parent_p, CachedObject<BLeaf>& node_p){
    BNode parent = *parent_p;
    BLeaf node = *node_p;

    // Full node so split
    bleaf_ptr new_neighbor = allocate_bleaf(pool);
    K to_parent = split_keys<BLeaf*, bleaf_ptr>(&node, new_neighbor, false);
    split_values(&node, (BLeaf*) new_neighbor);
    new_neighbor->set_next(node.get_next());
    node.set_next(new_neighbor);

    int bucket = search_node<BNode>(parent_p, to_parent);
    REMUS_ASSERT(bucket != -1, "Implies a full parent");

    shift_up(&parent, bucket);
    parent.set_key(bucket, to_parent);
    parent.set_ptr(bucket + 1, static_cast<bnode_ptr>(new_neighbor));

    parent.increment_version();

    cache->Write<BNode>(parent_p.remote_origin(), parent);
    cache->Write<BLeaf>(node_p.remote_origin(), node);
  }

  inline bnode_ptr read_level(BNode* curr, int bucket){
    bnode_ptr next_level = curr->ptr_at(SIZE);
    if (bucket != -1) next_level = curr->ptr_at(bucket);
    REMUS_ASSERT(next_level != nullptr, "Accessing SENTINEL's ptr");
    return next_level;
  }

  CachedObject<BLeaf> traverse(capability* pool, K key, bool do_split){
    // read root first
    CachedObject<BRoot> curr_root = cache->Read<BRoot>(root);
    int height = curr_root->height;
    bnode_ptr next_level = curr_root->start;
    REMUS_ASSERT(next_level != nullptr, "Accessing SENTINEL's ptr");

    if (height == 0){
      CachedObject<BLeaf> next_leaf = cache->Read<BLeaf>(static_cast<bleaf_ptr>(next_level));
      if (do_split && next_leaf->key_at(SIZE - 1) != SENTINEL){
        // failed to get locks, retraverse
        // if (!try_acquire(pool, curr_root, 0)) return traverse(pool, key, do_split);
        // if (!try_acquire(pool, next_leaf, 0)) return traverse(pool, key, do_split);
        split_node(pool, curr_root, next_leaf);
        // Just restart
        return traverse(pool, key, do_split);
      }

      // Made it to the leaf
      return next_leaf;
    }

    // Traverse bnode until bleaf
    CachedObject<BNode> curr = cache->Read<BNode>(next_level);
    while (!curr->is_valid()){
      curr = cache->Read<BNode>(next_level);
    }
    CachedObject<BNode> parent;
    // if split, we updated the root and we need to retraverse
    if (do_split && curr->key_at(SIZE - 1) != SENTINEL){
      // if (!try_acquire(pool, curr_root, 0)) return traverse(pool, key, do_split);
      if (!try_acquire(pool, curr.remote_origin(), curr->version())) return traverse(pool, key, do_split);
      split_node(pool, curr_root, curr);
      return traverse(pool, key, do_split); // todo: if we split, there should be no need to re-read
    } 

    int bucket = search_node<BNode>(curr, key);
    while(height != 1){
      // Get the next level ptr
      next_level = read_level((BNode*) curr.get(), bucket);

      // Read it as a BNode
      parent = std::move(curr);
      curr = cache->Read<BNode>(next_level);
      while (!curr->is_valid()){
        curr = cache->Read<BNode>(next_level);
      }
      if (do_split && curr->key_at(SIZE - 1) != SENTINEL) {
        if (try_acquire(pool, parent.remote_origin(), parent->version())) {
          if (try_acquire(pool, curr.remote_origin(), curr->version())) {
            // can acquire parent and current, so split
            split_node(pool, parent, curr);
          } else {
            // cannot acquire child so release parent and try again
            release(pool, parent.remote_origin(), parent->version());
          }
        }
        // re-read the parent and continue
        curr = cache->Read<BNode>(parent.remote_origin());
        while (!curr->is_valid()){
          curr = cache->Read<BNode>(next_level);
        }
        bucket = search_node<BNode>(curr, key);
        continue;
      }
      bucket = search_node<BNode>(curr, key);
      height--;
    }

    // Get the next level ptr
    next_level = read_level((BNode*) curr.get(), bucket);

    // Read it as a BLeaf
    auto next_leaf = static_cast<bleaf_ptr>(next_level);
    CachedObject<BLeaf> leaf = cache->Read<BLeaf>(next_leaf);
    // todo: acquire leaf here? todo: how do we retry?
    if (do_split && leaf->key_at(SIZE - 1) != SENTINEL) {
      if (try_acquire(pool, curr.remote_origin(), curr->version())){
        split_node(pool, curr, leaf);
      } else {
        REMUS_ERROR("UNIMPLEMENTED");
      }

      // Refresh if we caused a split
      curr = cache->Read<BNode>(curr.remote_origin());
      while (!curr->is_valid()){
        curr = cache->Read<BNode>(next_level);
      }
      bucket = search_node<BNode>(curr, key);
      auto next_leaf = static_cast<bleaf_ptr>(read_level((BNode*) curr.get(), bucket));
      leaf = cache->Read<BLeaf>(next_leaf);
    }
    return leaf;
  }

public:
  RdmaBPTree(Peer& self, CacheDepth::CacheDepth depth, RemoteCacheImpl<capability>* cache, capability* pool) 
  : self_(std::move(self)), cache_depth_(depth), cache(cache) {
    REMUS_INFO("Sentinel = {}", SENTINEL);
    REMUS_INFO("Struct Memory (BNode): {} % 64 = {}", sizeof(BNode), sizeof(BNode) % 64);
    REMUS_INFO("Struct Memory (BLeaf): {} % 64 = {}", sizeof(BLeaf), sizeof(BLeaf) % 64);
    temp_lock = pool->Allocate<uint64_t>(8); // todo: undo 8
  };

  /// Free all the resources associated with the IHT
  void destroy(capability* pool) {
    pool->Deallocate<uint64_t>(temp_lock, 8);
  }

  /// @brief Create a fresh iht
  /// @param pool the capability to init the IHT with
  /// @return the iht root pointer
  rdma_ptr<anon_ptr> InitAsFirst(capability* pool){
    rdma_ptr<BRoot> broot = pool->Allocate<BRoot>();
    broot->height = 0; // set height to 1
    this->root = broot;
    this->root->start = static_cast<bnode_ptr>(allocate_bleaf(pool));
    if (cache_depth_ >= 1)
        this->root = mark_ptr(this->root);
    return static_cast<rdma_ptr<anon_ptr>>(this->root);
  }

  /// @brief Initialize an IHT from the pointer of another IHT
  /// @param root_ptr the root pointer of the other iht from InitAsFirst();
  void InitFromPointer(rdma_ptr<anon_ptr> root_ptr){
      if (cache_depth_ >= 1)
        root_ptr = mark_ptr(root_ptr);
    this->root = static_cast<rdma_ptr<BRoot>>(root_ptr);
  }

  /// @brief Gets a value at the key.
  /// @param pool the capability providing one-sided RDMA
  /// @param key the key to search on
  /// @return an optional containing the value, if the key exists
  std::optional<V> contains(capability* pool, K key) {
    CachedObject<BLeaf> leaf = traverse(pool, key, false);
    int bucket = search_node<BLeaf>(leaf, key);
    while (bucket == -1){ // traverse to the right until we find the correct node
      leaf = cache->Read<BLeaf>(leaf->get_next());
      bucket = search_node<BLeaf>(leaf, key);
    }
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
    CachedObject<BLeaf> leaf = traverse(pool, key, true);
    int bucket = search_node<BLeaf>(leaf, key);
    while (bucket == -1){ // traverse to the right until we find the correct node
      leaf = cache->Read<BLeaf>(leaf->get_next());
      bucket = search_node(leaf, key);
    }
    V prev_value = leaf->value_at(bucket);
    if (leaf->key_at(bucket) != key) { // N.B. this is an inverted condition. Action upon the key is absent
      BLeaf new_leaf = *leaf;
      REMUS_ASSERT(new_leaf.key_at(SIZE - 1) == SENTINEL, "Splitting required!");
      // shift up to make room for the KV (if possible)
      for(int i = SIZE - 1; i > bucket; i--) new_leaf.set_value(i, new_leaf.value_at(i - 1));
      for(int i = SIZE - 1; i > bucket; i--) new_leaf.set_key(i, new_leaf.key_at(i - 1));
      new_leaf.set_key(bucket, key);
      new_leaf.set_value(bucket, value);
      cache->Write<BLeaf>(leaf.remote_origin(), new_leaf);
      return nullopt;
    }
    return make_optional(prev_value); // found a match
  }

  /// @brief Will remove a value at the key. Will stored the previous value in
  /// result.
  /// @param pool the capability providing one-sided RDMA
  /// @param key the key to remove at
  /// @return an optional containing the old value if the remove was successful. Otherwise an empty optional.
  std::optional<V> remove(capability* pool, K key) {
    CachedObject<BLeaf> leaf = traverse(pool, key, true);
    int bucket = search_node<BLeaf>(leaf, key);
    while (bucket == -1){ // traverse to the right until we find the correct node
      leaf = cache->Read<BLeaf>(leaf->next_leaf);
      bucket = search_node(leaf, key);
    }
    if (leaf->keys[bucket] == key) {
      BLeaf new_leaf = *leaf;
      V prev_val = new_leaf.values[bucket];
      // shift down to overwrite the key
      for(int i = bucket + 1; i < SIZE; i++){
        new_leaf.keys[i - 1] = new_leaf.keys[i];
        new_leaf.values[i - 1] = new_leaf.values[i];
      }
      cache->Write<BLeaf>(leaf.remote_origin(), new_leaf);
      return make_optional(prev_val);
    }
    return nullopt;
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
    std::cout << "Leaf<next=" << (leaf.get_next() != nullptr) << "> ";
    for(int i = 0; i < SIZE; i++){
      if (leaf.key_at(i) == SENTINEL)
        std::cout << "(SENT, " << leaf.value_at(i) << ") ";
      else
        std::cout << "(" << leaf.key_at(i) << ", " << leaf.value_at(i) << ") ";
    }
    std::cout << std::endl;
  }

  void debug(int height, bnode_ptr node, int indent){
    if (height == 0){
      debug(static_cast<bleaf_ptr>(node), indent + 1);
      return;
    }
    BNode n = *node;
    for(int i = 0; i < indent; i++){
      std::cout << "\t";
    }
    std::cout << (n.is_valid() ? "valid " : "invalid ") << n.version();
    std::cout << " => ";
    std::cout << (n.ptr_at(0) != nullptr);
    for(int i = 0; i < SIZE; i++){
      if (n.key_at(i) == SENTINEL)
        std::cout << " (SENT) " << (n.ptr_at(i+1) != nullptr);
      else
        std::cout << " (" << n.key_at(i) << ") " << (n.ptr_at(i+1) != nullptr);
    }
    std::cout << std::endl;
    for(int i = 0; i <= SIZE; i++){
      if (n.ptr_at(i) != nullptr)
        debug(height - 1, n.ptr_at(i), indent + 1);
    }
  }

  /// Single threaded print
  void debug(){
    BRoot r = *root;
    std::cout << "Root(height=" << r.height << ")" << std::endl;
    debug(r.height, r.start, 0);
    std::cout << std::endl;
  }

  /// No concurrent or thread safe (if everyone is readonly, then its fine). Counts the number of elements in the IHT
  int count(capability* pool){
    // Get leftmost leaf by wrapping SENTINEL
    CachedObject<BLeaf> curr = traverse(pool, 0 - SENTINEL - 1, false);
    int count = 0;
    while(true){
      // Add to count
      for(int i = 0; i < SIZE; i++){
        if (curr->key_at(i) != SENTINEL) count++;
      }
      if (curr->get_next() == nullptr) break;
      curr = cache->Read<BLeaf>(curr->get_next());
    }
    return count;
  }
};
