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
template <class K, class V, int DEGREE, K SENTINEL> class RdmaBPTree {
private:
  static const int SIZE = (DEGREE * 2) + 1;
  Peer self_;
  CacheDepth::CacheDepth cache_depth_;

  struct BNode;

  /// Return the first index where the provided key is less than the element at that index 
  /// (-1 if the end element)
  static int search_node(K key, const K* keys){
    // todo: binary search
    for(int i = 0; i < SIZE; i++){
      if (key <= keys[i]){ // todo: "<"?
        return i;
      }
    }
    return -1;
  }

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

  /// Configuration information
  struct alignas(64) BRoot {
    int height;
    rdma_ptr<BNode> start;

    void set_start_and_inc(rdma_ptr<BNode> new_start){
      start = new_start;
      height++;
    }
  };

  struct alignas(64) BNode {
    K keys[SIZE];
    rdma_ptr<BNode> ptrs[SIZE+1];
  };

  struct alignas(64) BLeaf {
    K keys[SIZE];
    V values[SIZE];
    rdma_ptr<BLeaf> next_leaf;
  };

  template <typename T> inline bool is_local(rdma_ptr<T> ptr) {
    return ptr.id() == self_.id;
  }

  rdma_ptr<BRoot> root;
  RemoteCacheImpl<capability>* cache;
  
  // preallocated memory for RDMA operations (avoiding frequent allocations)
  // rdma_ptr<T> temp_lock;

  /// Allocate a middle layer node
  static rdma_ptr<BNode> allocate_bnode(capability* pool){
    rdma_ptr<BNode> bnode = pool->Allocate<BNode>();
    for(int i = 0; i < SIZE; i++){
      bnode->keys[i] = SENTINEL;
      bnode->ptrs[i] = nullptr;
    }
    bnode->ptrs[SIZE] = nullptr;
    return bnode;
  }

  /// Allocate a leaf node with sentinel values and nullptr for next
  static rdma_ptr<BLeaf> allocate_bleaf(capability* pool){
    rdma_ptr<BLeaf> bleaf = pool->Allocate<BLeaf>();
    for(int i = 0; i < SIZE; i++){
      bleaf->keys[i] = SENTINEL;
    }
    bleaf->next_leaf = nullptr;
    return bleaf;
  }

  /// Returns the Key to go to the parent
  [[nodiscard("Must use removed key")]]
  inline K split_keys(K* keys_full, K* keys_empty, bool no_persist){
    K ret = keys_full[DEGREE];
    for(int i = 0; i < SIZE; i++){
      if (i < DEGREE){
        keys_empty[i] = keys_full[DEGREE + i + 1];
        keys_full[DEGREE + i + 1] = SENTINEL;
      } else {
        // otherwise initialize sentinels
        keys_empty[i] = SENTINEL;
      }
    }
    if (no_persist){
      keys_full[DEGREE] = SENTINEL;
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

  inline void split_ptrs(rdma_ptr<BNode>* ptrs_full, rdma_ptr<BNode>* ptrs_empty){
    for(int i = 0; i <= SIZE; i++){
      if (i <= DEGREE){
        ptrs_empty[i] = ptrs_full[DEGREE + i + 1];
        ptrs_full[DEGREE + i + 1] = nullptr;
      } else {
        // otherwise initialize sentinels
        ptrs_empty[i] = nullptr;
      }
    }
  }

  // todo; mark ptr
  /// At root
  bool split_node(capability* pool, CachedObject<BRoot>& parent_p, CachedObject<BNode>& node_p){
    if (node_p->keys[SIZE - 1] != SENTINEL) {
      REMUS_INFO("Splitting top node");
      BRoot parent = *parent_p;
      BNode node = *node_p;

      // Full node so split
      rdma_ptr<BNode> new_parent = allocate_bnode(pool);
      parent.set_start_and_inc(new_parent);

      rdma_ptr<BNode> new_neighbor = allocate_bnode(pool);
      K to_parent = split_keys(node.keys, new_neighbor->keys, true);
      split_ptrs(node.ptrs, new_neighbor->ptrs);

      new_parent->keys[0] = to_parent;
      new_parent->ptrs[0] = node_p.remote_origin();
      new_parent->ptrs[1] = new_neighbor;

      cache->Write<BRoot>(parent_p.remote_origin(), parent);
      cache->Write<BNode>(node_p.remote_origin(), node);
      return true;
    }
    return false;
  }

  /// Move
  bool split_node(capability* pool, CachedObject<BRoot>& parent_p, CachedObject<BLeaf>& node_p){
    if (node_p->keys[SIZE - 1] != SENTINEL) {
      REMUS_INFO("Splitting top node (leaf-version)");
      BRoot parent = *parent_p;
      BLeaf node = *node_p;

      // Full node so split
      rdma_ptr<BNode> new_parent = allocate_bnode(pool);
      parent.set_start_and_inc(new_parent);

      rdma_ptr<BLeaf> new_neighbor = allocate_bleaf(pool);
      K to_parent = split_keys(node.keys, new_neighbor->keys, false);
      split_values(node.values, new_neighbor->values);
      node.next_leaf = new_neighbor;

      new_parent->keys[0] = to_parent;
      new_parent->ptrs[0] = static_cast<rdma_ptr<BNode>>(node_p.remote_origin());
      new_parent->ptrs[1] = static_cast<rdma_ptr<BNode>>(new_neighbor);

      cache->Write<BRoot>(parent_p.remote_origin(), parent);
      cache->Write<BLeaf>(node_p.remote_origin(), node);
      return true;
    }
    return false;
  }

  /// Not at root
  bool split_node(capability* pool, CachedObject<BNode>& parent_p, CachedObject<BNode>& node_p){
   if (node_p->keys[SIZE - 1] != SENTINEL) {
      REMUS_INFO("Splitting middle node");
      BNode parent = *parent_p;
      BNode node = *node_p;
      
      // Full node so split
      rdma_ptr<BNode> new_neighbor = allocate_bnode(pool);
      K to_parent = split_keys(node.keys, new_neighbor->keys, true);
      split_ptrs(node.ptrs, new_neighbor->ptrs);

      int bucket = search_node(to_parent, parent_p->keys);
      REMUS_ASSERT(bucket != -1, "Implies a full parent");

      // todo: shift the parent
      parent.keys[bucket] = to_parent;
      parent.ptrs[bucket + 1] = new_neighbor;

      cache->Write<BNode>(parent_p.remote_origin(), parent);
      cache->Write<BNode>(node_p.remote_origin(), node);
      return true;
    }
    return false;
  }

  /// At leaf. Try to split the leaf into two and move a key to the parent
  bool split_node(capability* pool, CachedObject<BNode>& parent_p, CachedObject<BLeaf>& node_p){
    if (node_p->keys[SIZE - 1] != SENTINEL) {
      REMUS_INFO("Splitting lower node");
      BNode parent = *parent_p;
      BLeaf node = *node_p;

      // Full node so split
      rdma_ptr<BLeaf> new_neighbor = allocate_bleaf(pool);
      K to_parent = split_keys(node.keys, new_neighbor->keys, false);
      split_values(node.values, new_neighbor->values);
      node.next_leaf = new_neighbor;

      int bucket = search_node(to_parent, parent_p->keys);
      REMUS_ASSERT(bucket != -1, "Implies a full parent");

      // todo: shift up
      parent.keys[bucket] = to_parent;
      parent.ptrs[bucket + 1] = static_cast<rdma_ptr<BNode>>(new_neighbor);

      cache->Write<BNode>(parent_p.remote_origin(), parent);
      cache->Write<BLeaf>(node_p.remote_origin(), node);
      return true;
    }
    return false;
  }

  inline rdma_ptr<BNode> read_level(BNode* curr, int bucket){
    rdma_ptr<BNode> next_level = curr->ptrs[SIZE];
    if (bucket != -1) next_level = curr->ptrs[bucket];
    REMUS_ASSERT(next_level != nullptr, "Accessing SENTINEL's ptr");
    return next_level;
  }

  CachedObject<BLeaf> traverse(capability* pool, K key){
    // read root first
    CachedObject<BRoot> curr_root = cache->Read<BRoot>(root);
    int height = curr_root->height;
    rdma_ptr<BNode> next_level = curr_root->start;
    REMUS_ASSERT(next_level != nullptr, "Accessing SENTINEL's ptr");

    if (height == 0){
      CachedObject<BLeaf> next_leaf = cache->Read<BLeaf>(static_cast<rdma_ptr<BLeaf>>(next_level));
      if (split_node(pool, curr_root, next_leaf)){
        // Just restart
        return traverse(pool, key);
      } else {
        // Made it to the leaf
        return next_leaf;
      }
    }

    // Traverse bnode until bleaf
    CachedObject<BNode> curr = cache->Read<BNode>(next_level);
    CachedObject<BNode> parent;
    // if split, we updated the root and we need to retraverse
    if (split_node(pool, curr_root, curr)) traverse(pool, key); // todo: if we split, there should be no need to re-read

    int bucket = search_node(key, curr->keys);
    while(height != 1){
      // Get the next level ptr
      next_level = read_level((BNode*) curr.get(), bucket);

      // Read it as a BNode
      parent = std::move(curr);
      curr = cache->Read<BNode>(next_level);
      if (split_node(pool, parent, curr)) {
        // re-read the parent and continue
        curr = cache->Read<BNode>(parent.remote_origin());
        bucket = search_node(key, curr->keys);
        continue;
      }
      bucket = search_node(key, curr->keys);
      height--;
    }

    // Get the next level ptr
    next_level = read_level((BNode*) curr.get(), bucket);

    // Read it as a BLeaf
    auto next_leaf = static_cast<rdma_ptr<BLeaf>>(next_level);
    CachedObject<BLeaf> leaf = cache->Read<BLeaf>(next_leaf);
    if (split_node(pool, curr, leaf)) {
      // Refresh if we caused a split
      curr = cache->Read<BNode>(curr.remote_origin());
      bucket = search_node(key, curr->keys);
      auto next_leaf = static_cast<rdma_ptr<BLeaf>>(read_level((BNode*) curr.get(), bucket));
      leaf = cache->Read<BLeaf>(next_leaf);
    }
    return leaf;
  }

public:
  RdmaBPTree(Peer& self, CacheDepth::CacheDepth depth, RemoteCacheImpl<capability>* cache, capability* pool) 
  : self_(std::move(self)), cache_depth_(depth), cache(cache) {
    REMUS_INFO("Sentinel = {}", SENTINEL);
  };

  /// Free all the resources associated with the IHT
  void destroy(capability* pool) {
    
  }

  /// @brief Create a fresh iht
  /// @param pool the capability to init the IHT with
  /// @return the iht root pointer
  rdma_ptr<anon_ptr> InitAsFirst(capability* pool){
    rdma_ptr<BRoot> broot = pool->Allocate<BRoot>();
    broot->height = 0; // set height to 1
    this->root = broot;
    this->root->start = static_cast<rdma_ptr<BNode>>(allocate_bleaf(pool));
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
    CachedObject<BLeaf> leaf = traverse(pool, key);
    int bucket = search_node(key, leaf->keys);
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
    CachedObject<BLeaf> leaf = traverse(pool, key);
    int bucket = search_node(key, leaf->keys);
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
    CachedObject<BLeaf> leaf = traverse(pool, key);
    int bucket = search_node(key, leaf->keys);
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

  void debug(rdma_ptr<BLeaf> node, int indent){
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

  void debug(int height, rdma_ptr<BNode> node, int indent){
    if (height == 0){
      debug(static_cast<rdma_ptr<BLeaf>>(node), indent + 1);
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
    CachedObject<BLeaf> curr = traverse(pool, SENTINEL + 1);
    int count = 0;
    while(curr->next_leaf != nullptr){
      // Add to count
      for(int i = 0; i < SIZE; i++){
        if (curr.keys[i] != SENTINEL) count++;
      }
      curr = cache->Read<BLeaf>(curr->next_leaf);
    }
    return count;
  }
};
