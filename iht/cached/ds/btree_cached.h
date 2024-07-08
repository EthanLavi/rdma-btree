#pragma once

#include <cstdint>
#include <ostream>
#include <random>
#include <remus/logging/logging.h>
#include <remus/rdma/memory_pool.h>
#include <remus/rdma/rdma.h>

#include <dcache/cache_store.h>
#include <dcache/cached_ptr.h>

#include "../../dcache/test/faux_mempool.h"

#include "btree_helper.h"
#include "../../common.h"
#include <optional>

using namespace remus::rdma;

/// SIZE is DEGREE * 2
typedef CountingPool capability;
template <class V, int DEGREE> class RdmaBPTree {
private:
  static const int SIZE = (DEGREE * 2) + 1;
  static const uint64_t LOCK_BIT = (uint64_t) 1 << 63;
  Peer self_;
  CacheDepth::CacheDepth cache_depth_;

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

  // struct alignas(64) BNode {
  //   uint64_t lock;
  //   K keys[SIZE];
  //   rdma_ptr<BNode> ptrs[SIZE+1];
  // };

  struct alignas(64) BLeaf {
    K keys[SIZE];
    V values[SIZE];
    rdma_ptr<BLeaf> next_leaf;

    K key_at(int index) const {
      return keys[index];
    }

    void set_key(int index, K key) {
      keys[index] = key;
    }

    int search_node(K key) const {
      // todo: binary search
      for(int i = 0; i < SIZE; i++){
        if (key <= keys[i]){
          return i;
        }
      }
      return -1;
    }
  };

  typedef rdma_ptr<BLeaf> bleaf_ptr;

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

    /// Return the first index where the provided key is less than the element at that index 
    /// (-1 if the end element)
    int search_node(K key) const {
      // todo: binary search
      for(int i = 0; i < SIZE; i++){
        if (key <= key_at(i)){
          return i;
        }
      }
      return -1;
    }
  };

  typedef rdma_ptr<BNode> bnode_ptr;

  /// Binary count the number of nodes
  // static int size_node(K* keys){
  //   // Return edge conditions
  //   if (keys[0] == SENTINEL) return 0;
  //   if (keys[SIZE - 1] != SENTINEL) return SIZE;
  //   int lower = 0;
  //   int upper = SIZE - 1;
  //   while (true){
  //     int mid = (lower + upper) / 2;
  //     if (mid == SIZE - 1 && keys[mid] != SENTINEL) return SIZE; // guard edge case
  //     else if (keys[mid] != SENTINEL && keys[mid + 1] == SENTINEL){
  //       return mid + 1;
  //     } else if (keys[mid] == SENTINEL){
  //       upper = mid;
  //     } else {
  //       lower = mid;
  //     }
  //   }
  // }


  template <typename T> inline bool is_local(rdma_ptr<T> ptr) {
    return ptr.id() == self_.id;
  }

  rdma_ptr<BRoot> root;
  RemoteCacheImpl<capability>* cache;
  
  // preallocated memory for RDMA operations (avoiding frequent allocations)
  rdma_ptr<uint64_t> temp_lock;

  /// returns true if we can acquire the version of the bnode
  bool acquire(capability* pool, bnode_ptr node, long version){
    // swap the first 8 bytes (the lock) from unlocked to locked
    uint64_t v = pool->CompareAndSwap<uint64_t>(node, version, version | LOCK_BIT);
    if (v == version) return true;
    return false;
  }

  /// Allocate a middle layer node
  static bnode_ptr allocate_bnode(capability* pool){
    REMUS_ASSERT(56 % sizeof(K) == 0, "K must be a multiple of 2,4,8,14,28,56");
    bnode_ptr bnode = pool->Allocate<BNode>();
    *bnode = BNode();
    return bnode;
  }

  /// Allocate a leaf node with sentinel values and nullptr for next
  static bleaf_ptr allocate_bleaf(capability* pool){
    bleaf_ptr bleaf = pool->Allocate<BLeaf>();
    for(int i = 0; i < SIZE; i++){
      bleaf->keys[i] = SENTINEL;
    }
    bleaf->next_leaf = nullptr;
    return bleaf;
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

  inline void split_values(V* values_full, V* values_empty){
    for(int i = 0; i < SIZE; i++){
      if (i < DEGREE){
        values_empty[i] = values_full[DEGREE + i + 1];
        values_full[DEGREE + i + 1] = SENTINEL;
      } else {
        // otherwise initialize sentinels
        values_empty[i] = SENTINEL;
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
    split_values(node.values, new_neighbor->values);
    node.next_leaf = new_neighbor;

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

    int bucket = parent_p->search_node(to_parent);
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
    split_values(node.values, new_neighbor->values);
    new_neighbor->next_leaf = node.next_leaf;
    node.next_leaf = new_neighbor;

    int bucket = parent_p->search_node(to_parent);
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
      if (do_split && next_leaf->keys[SIZE - 1] != SENTINEL){
        // failed to get locks, retraverse
        // if (!acquire(pool, curr_root, 0)) return traverse(pool, key, do_split);
        // if (!acquire(pool, next_leaf, 0)) return traverse(pool, key, do_split);
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
      // if (!acquire(pool, curr_root, 0)) return traverse(pool, key, do_split);
      // if (!acquire(pool, curr, curr->version())) return traverse(pool, key, do_split);
      split_node(pool, curr_root, curr);
      return traverse(pool, key, do_split); // todo: if we split, there should be no need to re-read
    } 

    int bucket = curr->search_node(key);
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
        split_node(pool, parent, curr);
        // re-read the parent and continue
        curr = cache->Read<BNode>(parent.remote_origin());
        while (!curr->is_valid()){
          curr = cache->Read<BNode>(next_level);
        }
        bucket = curr->search_node(key);
        continue;
      }
      bucket = curr->search_node(key);
      height--;
    }

    // Get the next level ptr
    next_level = read_level((BNode*) curr.get(), bucket);

    // Read it as a BLeaf
    auto next_leaf = static_cast<bleaf_ptr>(next_level);
    CachedObject<BLeaf> leaf = cache->Read<BLeaf>(next_leaf);
    if (do_split && leaf->keys[SIZE - 1] != SENTINEL) {
      split_node(pool, curr, leaf);
      // Refresh if we caused a split
      curr = cache->Read<BNode>(curr.remote_origin());
      while (!curr->is_valid()){
        curr = cache->Read<BNode>(next_level);
      }
      bucket = curr->search_node(key);
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
    int bucket = leaf->search_node(key);
    if (bucket == -1) return nullopt;
    if (leaf->keys[bucket] == key) return make_optional(leaf->values[bucket]);
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
    int bucket = leaf->search_node(key);
    if (bucket == -1) {
      // todo: traverse to the next node? Issue with descent?
      REMUS_FATAL("Unimplemented");
    }
    V prev_value = leaf->values[bucket];
    if (leaf->keys[bucket] != key) { // N.B. this is an inverted condition. Action upon the key is absent
      BLeaf new_leaf = *leaf;
      REMUS_ASSERT(new_leaf.keys[SIZE - 1] == SENTINEL, "Splitting required!");
      // shift up to make room for the KV (if possible)
      for(int i = SIZE - 1; i > bucket; i--){
        new_leaf.keys[i] = new_leaf.keys[i - 1];
        new_leaf.values[i] = new_leaf.values[i - 1];
      }
      new_leaf.keys[bucket] = key;
      new_leaf.values[bucket] = value;
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
    int bucket = leaf->search_node(key);
    if (bucket == -1) return nullopt;
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
    std::cout << "Leaf<next=" << (leaf.next_leaf != nullptr) << "> ";
    for(int i = 0; i < SIZE; i++){
      if (leaf.keys[i] == SENTINEL)
        std::cout << "(SENT, " << leaf.values[i] << ") ";
      else
        std::cout << "(" << leaf.keys[i] << ", " << leaf.values[i] << ") ";
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
    std::cout << " => ";
    std::cout << (n.ptrs[0] != nullptr);
    for(int i = 0; i < SIZE; i++){
      if (n.keys[i] == SENTINEL)
        std::cout << " (SENT) " << (n.ptrs[i+1] != nullptr);
      else
        std::cout << " (" << n.keys[i] << ") " << (n.ptrs[i+1] != nullptr);
    }
    std::cout << std::endl;
    for(int i = 0; i <= SIZE; i++){
      if (n.ptrs[i] != nullptr)
        debug(height - 1, n.ptrs[i], indent + 1);
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
        if (curr->keys[i] != SENTINEL) count++;
      }
      if (curr->next_leaf == nullptr) break;
      curr = cache->Read<BLeaf>(curr->next_leaf);
    }
    return count;
  }
};
