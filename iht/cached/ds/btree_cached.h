#pragma once

#include <random>
#include <remus/logging/logging.h>
#include <remus/rdma/memory_pool.h>
#include <remus/rdma/rdma.h>

#include <dcache/cache_store.h>
#include <dcache/cached_ptr.h>

#include "../../common.h"
#include <optional>

using namespace remus::rdma;

/// SIZE is DEGREE * 2
template <class K, class V, int SIZE> class RdmaBPTree {
private:
  Peer self_;
  CacheDepth::CacheDepth cache_depth_;

  struct BNode;

  struct KV {
    K key;
    V value;
  };

  struct pair_t {
    rdma_ptr<BNode> next;
    KV kv;
  };

  struct alignas(64) BRoot {
    int height;
    pair_t pairs[SIZE];
    rdma_ptr<BNode> next;
  };

  struct alignas(64) BNode {
    pair_t pairs[SIZE];
    rdma_ptr<BNode> next;
  };

  struct alignas(64) BLeaf {
    KV kvs[SIZE];
    rdma_ptr<BLeaf> next_leaf;
  };

  template <typename T> inline bool is_local(rdma_ptr<T> ptr) {
    return ptr.id() == self_.id;
  }

  rdma_ptr<BRoot> root;
  RemoteCache* cache;
  
  // preallocated memory for RDMA operations (avoiding frequent allocations)
  // rdma_ptr<T> temp_lock;
  // N.B. I don't bother creating preallocated PLists since we're hoping to cache them anyways :)

public:
  RdmaBPTree(Peer& self, CacheDepth::CacheDepth depth, RemoteCache* cache, rdma_capability_thread* pool) 
  : self_(std::move(self)), cache_depth_(depth), cache(cache) {};

  /// Free all the resources associated with the IHT
  void destroy(rdma_capability_thread* pool) {
    
  }

  /// @brief Create a fresh iht
  /// @param pool the capability to init the IHT with
  /// @return the iht root pointer
  rdma_ptr<anon_ptr> InitAsFirst(rdma_capability_thread* pool){
    rdma_ptr<BRoot> broot = pool->Allocate<BRoot>();
    this->root = broot;
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
  std::optional<V> contains(rdma_capability_thread* pool, K key) {
    CachedObject<BRoot> root_read = cache->Read<BRoot>(root);
    // root_read->
    return nullopt;
  }

  /// @brief Insert a key and value into the iht. Result will become the value
  /// at the key if already present.
  /// @param pool the capability providing one-sided RDMA
  /// @param key the key to insert
  /// @param value the value to associate with the key
  /// @return an empty optional if the insert was successful. Otherwise it's the value at the key.
  std::optional<V> insert(rdma_capability_thread* pool, K key, V value) {
    return nullopt;
  }

  /// @brief Will remove a value at the key. Will stored the previous value in
  /// result.
  /// @param pool the capability providing one-sided RDMA
  /// @param key the key to remove at
  /// @return an optional containing the old value if the remove was successful. Otherwise an empty optional.
  std::optional<V> remove(rdma_capability_thread* pool, K key) {
    return nullopt;
  }

  /// @brief Populate only works when we have numerical keys. Will add data
  /// @param pool the capability providing one-sided RDMA
  /// @param op_count the number of values to insert. Recommended in total to do key_range / 2
  /// @param key_lb the lower bound for the key range
  /// @param key_ub the upper bound for the key range
  /// @param value the value to associate with each key. Currently, we have
  /// asserts for result to be equal to the key. Best to set value equal to key!
  int populate(rdma_capability_thread* pool, int op_count, K key_lb, K key_ub, std::function<K(V)> value) {
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

  /// No concurrent or thread safe (if everyone is readonly, then its fine). Counts the number of elements in the IHT
  int count(rdma_capability_thread* pool){
    return 0;
  }
};
