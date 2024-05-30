#pragma once

#include <random>
#include <remus/rdma/memory_pool.h>
#include <remus/rdma/rdma.h>

#include "../common.h"
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <stdexcept>

#include <dcache/cache_store.h>
#include <dcache/mark_ptr.h>

using namespace remus::rdma;

template <class K, class V, int PLIST_SIZE> class RDMALinearProbingMap {
private:
  /// Global lock state
  const uint64_t LOCKED = 0, UNLOCKED = 1;
  const K EMPTY = 0, TOMBSTONE = 1;

  typedef uint64_t lock_type;
  typedef rdma_ptr<lock_type> remote_lock;
  
  struct alignas(64) pair_t {
    K key;
    V val;

    pair_t(K key, V val) : key(key), val(val) {}
  };

  // PointerList stores EList pointers and assorted locks
  struct alignas(64) Map {
    lock_type lock;
    pair_t buckets[PLIST_SIZE]; // Pointer lock pairs
  };

  typedef rdma_ptr<Map> root_ptr;
  typedef rdma_ptr<pair_t> bucket_ptr;

  template <typename T> inline bool is_null(rdma_ptr<T> ptr) {
    return ptr == nullptr;
  }

  // Get the address of the start of the array
  inline bucket_ptr get_bucket_ptr(root_ptr arr_start){
      uint64_t new_addy = arr_start.address();
      new_addy += sizeof(lock_type);
      return bucket_ptr(arr_start.id(), new_addy);
  }

  inline remote_lock get_lock_ptr(root_ptr arr_start){
      return static_cast<remote_lock>(arr_start);
  }

  /// @brief Initialize the plist with values.
  /// @param p the plist pointer to init
  /// @param depth the depth of p, needed for PLIST_SIZE == base_size * (2 **
  /// (depth - 1)) pow(2, depth)
  inline void InitPList(root_ptr p) {
    Map* m = to_address(p);
    m->lock = UNLOCKED;
    for (size_t i = 0; i < PLIST_SIZE; i++){
        pair_t* pair = &m->buckets[i];
        pair->key = 0;
        pair->val = 0;
    }
  }

  root_ptr root; // Start of plist

  /// Acquire a lock on the bucket. Will prevent others from modifying it
  void acquire(rdma_capability* pool, remote_lock lock) {
    // Spin while trying to acquire the lock
    while (true) {
      // Can this be a CAS on an address within a PList?
      lock_type v = pool->CompareAndSwap<lock_type>(lock, UNLOCKED, LOCKED);

      // If we can switch from unlock to lock status
      if (v == UNLOCKED) break;
    }
  }

  /// @brief Unlock a lock ==> the reverse of acquire
  /// @param lock the lock to unlock
  /// @param unlock_status what should the end lock status be.
  inline void unlock(rdma_capability* pool, remote_lock lock) {
    pool->Write<lock_type>(lock, UNLOCKED, temp_lock, internal::RDMAWriteWithNoAck);
  }

  /// @brief Hashing function to decide bucket size
  /// @param key the key to hash
  /// @param level the level in the iht
  /// @param count the number of buckets to hash into
  inline uint64_t level_hash(const K &key) {
    std::hash<K> to_num;
    size_t prehash = to_num(key);
    // mix13 implementation
    prehash ^= (prehash >> 33);
    prehash *= 0xff51afd7ed558ccd;
    prehash ^= (prehash >> 33);
    prehash *= 0xc4ceb9fe1a85ec53;
    prehash ^= (prehash >> 33);
    return prehash % (PLIST_SIZE);
  }
  
  // preallocated memory for RDMA operations (avoiding frequent allocations)
  remote_lock temp_lock;
  RemoteCache* cache;

public:
  RDMALinearProbingMap(Peer& self, CacheDepth::CacheDepth, RemoteCache* cache,  rdma_capability* pool) : cache(cache) {
    temp_lock = pool->Allocate<lock_type>();
  };

  /// Free all the resources associated with the IHT
  void destroy(rdma_capability* pool) {
    pool->Deallocate(temp_lock);
  }

  /// @brief Create a fresh iht
  /// @param pool the capability to init the IHT with
  /// @return the iht root pointer
  rdma_ptr<anon_ptr> InitAsFirst(rdma_capability* pool){
      root_ptr iht_root = pool->Allocate<Map>();
      InitPList(iht_root);
      this->root = iht_root;
      return static_cast<rdma_ptr<anon_ptr>>(iht_root);
  }

  /// @brief Initialize an IHT from the pointer of another IHT
  /// @param root the root pointer of the other iht from InitAsFirst();
  void InitFromPointer(rdma_ptr<anon_ptr> root){
      this->root = static_cast<root_ptr>(root);
  }

  /// @brief Gets a value at the key.
  /// @param pool the capability providing one-sided RDMA
  /// @param key the key to search on
  /// @return an optional containing the value, if the key exists
  std::optional<V> contains(rdma_capability* pool, K key) {
    auto global_lock = get_lock_ptr(root);
    acquire(pool, global_lock);
    bucket_ptr b = get_bucket_ptr(root);
    int start = level_hash(key);
    for(int i = 0; i < PLIST_SIZE; i++){
        int bucket = (start + i) % PLIST_SIZE;
        // linear probe forward
        CachedObject<pair_t> blocal = cache->Read<pair_t>(mark_ptr(b[bucket]));
        const pair_t* kv = to_address(blocal);
        if (kv->key == key){
            unlock(pool, global_lock);
            return std::optional(kv->val);
        } else if (kv->key == EMPTY){
            unlock(pool, global_lock);
            return std::nullopt;
        }
    }
    unlock(pool, global_lock);
    return std::nullopt;
  }

  /// @brief Insert a key and value into the iht. Result will become the value at the key if already present.
  /// @param pool the capability providing one-sided RDMA
  /// @param key the key to insert
  /// @param value the value to associate with the key
  /// @return an empty optional if the insert was successful. Otherwise it's the value at the key.
  std::optional<V> insert(rdma_capability* pool, K key, V value) {
    auto global_lock = get_lock_ptr(root);
    acquire(pool, global_lock);
    bucket_ptr b = get_bucket_ptr(root);
    int start = level_hash(key);
    bucket_ptr tomb = nullptr;
    for(int i = 0; i < PLIST_SIZE; i++){
        int bucket = (start + i) % PLIST_SIZE;
        // linear probe forward
        bucket_ptr bucket_remote = mark_ptr(b[bucket]);
        CachedObject<pair_t> blocal = cache->Read<pair_t>(bucket_remote);
        const pair_t* kv = to_address(blocal);
        if (kv->key == key){
          unlock(pool, global_lock);
          return std::optional(kv->val);
        } else if (kv->key == TOMBSTONE){
          // Save the first tombstone to be the save location
          if (tomb == nullptr)
            tomb = bucket_remote;
        } else if (kv->key == EMPTY){
          // Put the kv at the earlier tomb if one was found
          if (tomb != nullptr)
            bucket_remote = tomb;
          cache->Write<pair_t>(bucket_remote, pair_t(key, value));
          unlock(pool, global_lock);
          return std::nullopt;
        }
    }
    unlock(pool, global_lock);
    throw std::runtime_error("Size of keyspace exceeds map's valid range");
  }

  /// @brief Will remove a value at the key. Will stored the previous value in result.
  /// @param pool the capability providing one-sided RDMA
  /// @param key the key to remove at
  /// @return an optional containing the old value if the remove was successful. Otherwise an empty optional.
  std::optional<V> remove(rdma_capability* pool, K key) {
    auto global_lock = get_lock_ptr(root);
    acquire(pool, global_lock);
    bucket_ptr b = get_bucket_ptr(root);
    int start = level_hash(key);
    for(int i = 0; i < PLIST_SIZE; i++){
        int bucket = (start + i) % PLIST_SIZE;
        // linear probe forward
        bucket_ptr bucket_remote = mark_ptr(b[bucket]);
        CachedObject<pair_t> blocal = cache->Read<pair_t>(bucket_remote);
        const pair_t* kv = to_address(blocal);
        if (kv->key == key){
            cache->Write(bucket_remote, pair_t(TOMBSTONE, 0));
            unlock(pool, global_lock);
            return std::optional(kv->val);
        }
        if (kv->key == EMPTY){
            unlock(pool, global_lock);
            return std::nullopt;
        }
    }
    unlock(pool, global_lock);
    return std::nullopt;
  }

  /// Count the number of elements in the array
  /// @param pool the capability providing one-sided RDMA
  /// @return the number of elements in the map
  int count(rdma_capability* pool){
    auto global_lock = get_lock_ptr(root);
    acquire(pool, global_lock);
    bucket_ptr b = get_bucket_ptr(root);
    int total = 0;
    for(int bucket = 0; bucket < PLIST_SIZE; bucket++){
        // linear probe forward
        bucket_ptr bucket_remote = mark_ptr(b[bucket]);
        CachedObject<pair_t> blocal = cache->Read<pair_t>(bucket_remote);
        const pair_t* kv = to_address(blocal);
        if (kv->key != EMPTY && kv->key != TOMBSTONE)
            total++;
    }
    unlock(pool, global_lock);
    return total;
  }

  /// @brief Populate only works when we have numerical keys. Will add data
  /// @param pool the capability providing one-sided RDMA
  /// @param op_count the number of values to insert. Recommended in total to do key_range / 2
  /// @param key_lb the lower bound for the key range
  /// @param key_ub the upper bound for the key range
  /// @param value the value to associate with each key. Currently, we have
  /// asserts for result to be equal to the key. Best to set value equal to key!
  int populate(rdma_capability* pool, int op_count, K key_lb, K key_ub, std::function<K(V)> value) {
    // Populate only works when we have numerical keys
    K key_range = key_ub - key_lb;
    // todo: Under-populating because of insert collisions?
    // Create a random operation generator that is
    // - evenly distributed among the key range
    int success_count = 0;
    std::uniform_real_distribution<double> dist = std::uniform_real_distribution<double>(0.0, 1.0);
    std::default_random_engine gen(std::chrono::system_clock::now().time_since_epoch().count());
    while (success_count != op_count) {
      int k = (dist(gen) * key_range) + key_lb;
      auto res = insert(pool, k, value(k));
      if (res == std::nullopt) success_count++;
      // Wait some time before doing next insert...
      std::this_thread::sleep_for(std::chrono::nanoseconds(10));
    }
    return success_count;
  }
};
