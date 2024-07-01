#include <atomic>
#include <random>
#include <remus/logging/logging.h>
#include <remus/rdma/memory_pool.h>
#include <remus/rdma/rdma.h>

#include <dcache/cache_store.h>
#include <dcache/cached_ptr.h>

#include "../../iht/common.h"
#include "dcache/mark_ptr.h"
#include "faux_mempool.h"
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>

template <class K, class V, int ELIST_SIZE, int PLIST_SIZE> class RdmaIHT {
private:
  Peer self_;
  CacheDepth::CacheDepth cache_depth_;

  /// State of a bucket
  /// E_LOCKED - The bucket is in use by a thread. The bucket points to an EList
  /// E_UNLOCKED - The bucket is free for manipulation. The bucket baseptr points to an EList
  /// P_UNLOCKED - The bucket will always be free for manipulation because it points to a PList. It is "calcified"
  const uint64_t E_LOCKED = 1, E_UNLOCKED = 2, P_UNLOCKED = 3;

  // "Super class" for the elist and plist structs
  struct Base {};
  typedef uint64_t lock_type;
  typedef rdma_ptr<Base> remote_baseptr;
  typedef rdma_ptr<lock_type> remote_lock;
  
  struct pair_t {
    K key;
    V val;
  };

  // ElementList stores a bunch of K/V pairs. IHT employs a "separate
  // chaining"-like approach. Rather than storing via a linked list (with easy
  // append), it uses a fixed size array
  struct alignas(64) EList : Base {
    size_t count = 0;         // The number of live elements in the Elist
    pair_t pairs[ELIST_SIZE]; // A list of pairs to store (stored as remote
                              // pointer to start of the contigous memory block)

    // Insert into elist a deconstructed pair
    void elist_insert(const K key, const V val) {
      pairs[count] = {key, val};
      count++;
    }

    // Insert into elist a pair
    void elist_insert(const pair_t pair) {
      pairs[count] = pair;
      count++;
    }

    EList() {
      this->count = 0; // ensure count is 0
    }
  };

  // [mfs]  This probably needs to be aligned
  // [esl]  I'm confused, this is only used as part of the plist, which is aligned...
  /// A PList bucket. It is a pointer-lock pair
  /// @param base is a pointer to the next elist/plist. 
  /// @param lock is the data that represents the state of the bucket. See lock_type.
  struct plist_pair_t {
    remote_baseptr base; // Pointer to base, the super class of Elist or Plist
    lock_type lock; // A lock to represent if the base is open or not
  };

  // PointerList stores EList pointers and assorted locks
  struct alignas(64) PList : Base {
    plist_pair_t buckets[PLIST_SIZE]; // Pointer lock pairs
  };

  typedef rdma_ptr<PList> remote_plist;
  typedef rdma_ptr<EList> remote_elist;

  template <typename T> inline bool is_local(rdma_ptr<T> ptr) {
    return ptr.id() == self_.id;
  }

  // Get the address of the lock at bucket (index)
  remote_lock get_lock(remote_plist arr_start, int index){
      uint64_t new_addy = arr_start.address();
      new_addy += (sizeof(plist_pair_t) * index) + 8;
      return unmark_ptr(remote_lock(arr_start.id(), new_addy));
  }

  // Get the address of the baseptr at bucket (index)
  rdma_ptr<remote_baseptr> get_baseptr(remote_plist arr_start, int index){
      uint64_t new_addy = arr_start.address();
      new_addy += sizeof(plist_pair_t) * index;
      return unmark_ptr(rdma_ptr<remote_baseptr>(arr_start.id(), new_addy));
  }

  /// @brief Initialize the plist with values.
  /// @param p the plist pointer to init
  /// @param depth the depth of p, needed for PLIST_SIZE == base_size * (2 **
  /// (depth - 1)) pow(2, depth)
  inline void InitPList(CountingPool* pool, remote_plist p, int mult_modder) {
    assert(sizeof(plist_pair_t) == 16); // Assert I did my math right...
    // memset(std::to_address(p), 0, 2 * sizeof(PList));
    for (size_t i = 0; i < PLIST_SIZE * mult_modder; i++){
      remote_elist e_new = pool->Allocate<EList>();
      objects.push_back(DeallocTask(static_cast<rdma_ptr<Object>>(e_new), sizeof(EList), nullptr));
      e_new->count = 0;
      p->buckets[i] = { static_cast<remote_baseptr>(e_new), E_UNLOCKED };
    }
  }

  remote_plist root; // Start of plist

  /// Acquire a lock on the bucket. Will prevent others from modifying it
  bool acquire(CountingPool* pool, remote_lock lock) {
    // Spin while trying to acquire the lock
    while (true) {
      // Can this be a CAS on an address within a PList?
      lock_type v = pool->CompareAndSwap<lock_type>(lock, E_UNLOCKED, E_LOCKED);

      // Permanent unlock
      if (v == P_UNLOCKED) { return false; }
      // If we can switch from unlock to lock status
      if (v == E_UNLOCKED) { return true; }
      // Check to make sure we aren't locking garbage
      REMUS_ASSERT(v == E_LOCKED, "Normal lock state");
    }
  }

  /// @brief Unlock a lock ==> the reverse of acquire
  /// @param lock the lock to unlock
  /// @param unlock_status what should the end lock status be.
  inline void unlock(CountingPool* pool, remote_lock lock, uint64_t unlock_status) {
    pool->Write<lock_type>(lock, unlock_status, temp_lock, internal::RDMAWriteWithNoAck);
  }

  /// @brief Change the baseptr for a given bucket to point to a different EList or a different PList
  /// @param list_start the start of the plist (bucket list)
  /// @param bucket the bucket to manipulate
  /// @param baseptr the new pointer that bucket should have
  inline void change_bucket_pointer(CountingPool* pool, remote_plist list_start,
                                    uint64_t bucket, remote_baseptr baseptr) {
    rdma_ptr<remote_baseptr> bucket_ptr = get_baseptr(list_start, bucket);
    if (!is_local(bucket_ptr)) {
      pool->Write<remote_baseptr>(bucket_ptr, baseptr, temp_ptr);
    } else {
      *bucket_ptr = baseptr;
    }
  }

  /// @brief Hashing function to decide bucket size
  /// @param key the key to hash
  /// @param level the level in the iht
  /// @param count the number of buckets to hash into
  inline uint64_t level_hash(const K &key, size_t level, size_t count) {
    std::hash<K> to_num;
    size_t prehash = to_num(key) ^ level;
    // mix13 implementation, maintains divisibility so we still have to subtract 1 from the bucket count
    prehash ^= (prehash >> 33);
    prehash *= 0xff51afd7ed558ccd;
    prehash ^= (prehash >> 33);
    prehash *= 0xc4ceb9fe1a85ec53;
    prehash ^= (prehash >> 33);

    // 1) pre_hash will first map the type K to size_t
    //    then pre_hash will help distribute non-uniform inputs evenly by applying a finalizer
    // 2) We use count-1 to ensure the bucket count is co-prime with the other plist bucket counts
    //    B/C of the property: A key maps to a suboptimal set of values when modding by 2A given "k mod A = Y" (where Y becomes the parent bucket)
    //    This happens because the hashing function maintains divisibility.
    return prehash % (count - 1);
  }

  /// Rehash function - will add more capacity
  /// @param pool The memory pool capability
  /// @param parent The P-List whose bucket needs rehashing
  /// @param pcount The number of elements in `parent`
  /// @param pdepth The depth of `parent`
  /// @param pidx   The index in `parent` of the bucket to rehash
  remote_plist rehash(CountingPool* pool, rdma_ptr<PList> parent, size_t pcount, size_t pdepth, size_t pidx) {
    pcount = pcount * 2;
    // how much bigger than original size we are
    int plist_size_factor = (pcount / PLIST_SIZE);

    // 2 ^ (depth) ==> in other words (depth:factor). 0:1, 1:2, 2:4, 3:8, 4:16, 5:32.
    remote_plist new_p = pool->Allocate<PList>(plist_size_factor);
    objects.push_back(DeallocTask(static_cast<rdma_ptr<Object>>(new_p), sizeof(PList) * plist_size_factor, nullptr));
    InitPList(pool, new_p, plist_size_factor);

    // hash everything from the full elist into it
    remote_elist parent_bucket = static_cast<remote_elist>(parent->buckets[pidx].base);
    remote_elist source = is_local(parent_bucket)
                              ? parent_bucket
                              : pool->Read<EList>(parent_bucket);

    // insert everything from the elist we rehashed into the plist
    for (size_t i = 0; i < source->count; i++) {
      uint64_t b = level_hash(source->pairs[i].key, pdepth + 1, pcount);
      remote_elist dest = static_cast<remote_elist>(new_p->buckets[b].base);
      dest->elist_insert(source->pairs[i]);
    }
    // Deallocate the old elist
    // TODO replace for remote deallocation
    // pool->Deallocate<EList>(source);
    // todo: freed by destroy since it is part of the data structure! (and hence is added as a dealloc task)
    return new_p;
  }

  RemoteCacheImpl<CountingPool>* cache;
  
  // preallocated memory for RDMA operations (avoiding frequent allocations)
  remote_lock temp_lock;
  rdma_ptr<remote_baseptr> temp_ptr;
  remote_elist temp_elist;
  // N.B. I don't bother creating preallocated PLists since we're hoping to cache them anyways :)
public:
  RdmaIHT(Peer& self, CacheDepth::CacheDepth depth, RemoteCacheImpl<CountingPool>* cache, CountingPool* pool) 
  : self_(std::move(self)), cache_depth_(depth), cache(cache) {
    // I want to make sure we are choosing PLIST_SIZE and ELIST_SIZE to best use the space (b/c of alignment)
    if ((PLIST_SIZE * sizeof(plist_pair_t)) % 64 != 0) {
      // PList must use all its space to obey the space requirements
      REMUS_FATAL("PList buckets must be continous. Therefore sizeof(PList) must be a multiple of 64. Try a multiple of 4");
    } else {
      REMUS_DEBUG("PList Level 1 takes up {} bytes", PLIST_SIZE * sizeof(plist_pair_t));
      assert(sizeof(PList) == PLIST_SIZE * sizeof(plist_pair_t));
    }
    auto size = ((ELIST_SIZE * sizeof(pair_t)) + sizeof(size_t));
    if (size % 64 < 60 && size % 64 != 0) {
      REMUS_WARN("Suboptimal ELIST_SIZE b/c EList aligned to 64 bytes");
    }

    // Allocate landing spots for the datastructure traversal
    temp_lock = pool->Allocate<lock_type>();
    temp_ptr = pool->Allocate<remote_baseptr>();
    temp_elist = pool->Allocate<EList>();
  };

  /// Free all the resources associated with the IHT
  std::vector<DeallocTask> objects;
  void destroy(CountingPool* pool) {
    // Have to deallocate "8" of them to account for alignment
    // But faux memory pool doesnt have the issue
    pool->Deallocate<lock_type>(temp_lock);
    pool->Deallocate<remote_baseptr>(temp_ptr);
    pool->Deallocate<EList>(temp_elist);
    for(int i = 0; i < objects.size(); i++){
      pool->Deallocate<Object>(objects[i].local_ptr, objects[i].size);
    }
  }

  /// @brief Create a fresh iht
  /// @param pool the capability to init the IHT with
  /// @return the iht root pointer
  rdma_ptr<anon_ptr> InitAsFirst(CountingPool* pool){
      remote_plist iht_root = pool->Allocate<PList>();
      objects.push_back(DeallocTask(static_cast<rdma_ptr<Object>>(iht_root), sizeof(PList), nullptr));
      InitPList(pool, iht_root, 1);
      this->root = iht_root;
      if (cache_depth_ >= 1)
        this->root = mark_ptr(this->root);
      return static_cast<rdma_ptr<anon_ptr>>(iht_root);
  }

  /// @brief Initialize an IHT from the pointer of another IHT
  /// @param root_ptr the root pointer of the other iht from InitAsFirst();
  void InitFromPointer(rdma_ptr<anon_ptr> root_ptr){
      if (cache_depth_ >= 1)
        root_ptr = mark_ptr(root_ptr);
      this->root = static_cast<remote_plist>(root_ptr);
  }

  /// @brief Gets a value at the key.
  /// @param pool the capability providing one-sided RDMA
  /// @param key the key to search on
  /// @return an optional containing the value, if the key exists
  std::optional<V> contains(CountingPool* pool, K key) {
    // Define some constants
    size_t depth = 1;
    size_t count = PLIST_SIZE;
    remote_plist parent_ptr = root;

    // start at root
    CachedObject<PList> curr = cache->Read<PList>(root);
    while (true) {
      uint64_t bucket = level_hash(key, depth, count);
      // Normal descent
      if (curr->buckets[bucket].lock == P_UNLOCKED){
        auto bucket_base = static_cast<remote_plist>(curr->buckets[bucket].base);
        curr = cache->ExtendedRead<PList>(bucket_base, 1 << depth);
        parent_ptr = bucket_base;
        depth++;
        count *= 2;
        continue;
      }

      // If we have a sizeof the elist 64, we can enable the lock-free GET
      if (sizeof(EList) != 64){
        // Erroneous descent into EList (Think we are at an EList, but it turns out its a PList)
        if (!acquire(pool, get_lock(parent_ptr, bucket))){
          // We must re-fetch the PList to ensure freshness of our pointers (1 << depth-1 to adjust size of read with customized ExtendedRead)
          curr = cache->ExtendedRead<PList>(parent_ptr, 1 << (depth - 1));
          continue;
        }
      }

      // We locked an elist, we can read the baseptr and progress
      remote_elist bucket_base = static_cast<remote_elist>(curr->buckets[bucket].base);
      // Past this point we have recursed to an elist
      remote_elist e = pool->Read<EList>(bucket_base, temp_elist);
      // Get elist and linear search
      for (size_t i = 0; i < e->count; i++) {
        // Linear search to determine if elist already contains the key 
        pair_t kv = e->pairs[i];
        if (kv.key == key) {
          // If lock free get, no need to unlock
          if (sizeof(EList) != 64)
            unlock(pool, get_lock(parent_ptr, bucket), E_UNLOCKED);
          return std::make_optional<V>(kv.val);
        }
      }
      // If lock free get, no need to unlock
      if (sizeof(EList) != 64)
        unlock(pool, get_lock(parent_ptr, bucket), E_UNLOCKED);
      return std::nullopt;
    }
  }

  /// @brief Insert a key and value into the iht. Result will become the value
  /// at the key if already present.
  /// @param pool the capability providing one-sided RDMA
  /// @param key the key to insert
  /// @param value the value to associate with the key
  /// @return an empty optional if the insert was successful. Otherwise it's the value at the key.
  std::optional<V> insert(CountingPool* pool, K key, V value) {
    // Define some constants
    size_t depth = 1;
    size_t count = PLIST_SIZE;
    remote_plist parent_ptr = root;

    // start at root
    CachedObject<PList> curr = cache->Read<PList>(root);
    while (true) {
      uint64_t bucket = level_hash(key, depth, count);
      // Normal descent
      if (curr->buckets[bucket].lock == P_UNLOCKED){
        auto bucket_base = static_cast<remote_plist>(curr->buckets[bucket].base);
        curr = cache->ExtendedRead<PList>(bucket_base, 1 << depth);
        parent_ptr = bucket_base;
        depth++;
        count *= 2;
        continue;
      }

      // Erroneous descent into EList (Think we are at an EList, but it turns out its a PList)
      if (!acquire(pool, get_lock(parent_ptr, bucket))){
        // We must re-fetch the PList to ensure freshness of our pointers (1 << depth-1 to adjust size of read with customized ExtendedRead)
        curr = cache->ExtendedRead<PList>(parent_ptr, 1 << (depth - 1));
        continue;
      }

      // We locked an elist, we can read the baseptr and progress
      remote_elist bucket_base = static_cast<remote_elist>(curr->buckets[bucket].base);
      remote_elist e = is_local(bucket_base) ? bucket_base : pool->Read<EList>(bucket_base, temp_elist);

      // We have recursed to an non-empty elist
      for (size_t i = 0; i < e->count; i++) {
        // Linear search to determine if elist already contains the key
        pair_t kv = e->pairs[i];
        if (kv.key == key) {
          std::optional<V> result = std::make_optional<V>(kv.val);
          unlock(pool, get_lock(parent_ptr, bucket), E_UNLOCKED);
          return result;
        }
      }

      // Check for enough insertion room
      if (e->count < ELIST_SIZE) {
        // insert, unlock, return
        e->elist_insert(key, value);
        // If we are modifying a local copy, we need to write to the remote at the end
        if (!is_local(bucket_base)) pool->Write<EList>(bucket_base, *e);
        unlock(pool, get_lock(parent_ptr, bucket), E_UNLOCKED);
        return std::nullopt;
      }

      // Need more room so rehash into plist and perma-unlock
      remote_plist p = rehash(pool, curr.get(), count, depth, bucket);
      if (cache_depth_ > depth)
        p = mark_ptr(p);

      // modify the bucket's pointer, keeping local curr updated with remote curr
      // todo: remove these lines if they can be removed (we want our cached object to have read-only semantics)
      // curr->buckets[bucket].base = static_cast<remote_baseptr>(p);
      // curr->buckets[bucket].lock = P_UNLOCKED;
      change_bucket_pointer(pool, parent_ptr, bucket, static_cast<remote_baseptr>(p));
      unlock(pool, get_lock(parent_ptr, bucket), P_UNLOCKED);
      // Prevent invalidate occuring before unlock
      std::atomic_thread_fence(std::memory_order_seq_cst);
      // have to invalidate a line associated with the object at parent_ptr
      // todo: async processing of unlock to ensure ordering (unlock -> invalidate)
      cache->Invalidate(parent_ptr);

      // we need to refresh our copy as well :)
      curr = cache->ExtendedRead<PList>(parent_ptr, 1 << (depth - 1)); // todo: check this
    }
  }

  /// @brief Will remove a value at the key. Will stored the previous value in
  /// result.
  /// @param pool the capability providing one-sided RDMA
  /// @param key the key to remove at
  /// @return an optional containing the old value if the remove was successful. Otherwise an empty optional.
  std::optional<V> remove(CountingPool* pool, K key) {
    // Define some constants
    size_t depth = 1;
    size_t count = PLIST_SIZE;
    remote_plist parent_ptr = root;

    // start at root
    CachedObject<PList> curr = cache->Read<PList>(root);

    while (true) {
      uint64_t bucket = level_hash(key, depth, count);
      // Normal descent
      if (curr->buckets[bucket].lock == P_UNLOCKED){
        auto bucket_base = static_cast<remote_plist>(curr->buckets[bucket].base);
        curr = cache->ExtendedRead<PList>(bucket_base, 1 << depth);
        parent_ptr = bucket_base;
        depth++;
        count *= 2;
        continue;
      }

      // Erroneous descent into EList (Think we are at an EList, but it turns out its a PList)
      if (!acquire(pool, get_lock(parent_ptr, bucket))){
        // We must re-fetch the PList to ensure freshness of our pointers (1 << depth-1 to adjust size of read with customized ExtendedRead)
        curr = cache->ExtendedRead<PList>(parent_ptr, 1 << (depth - 1));
        continue;
      }

      // We locked an elist, we can read the baseptr and progress
      remote_elist bucket_base = static_cast<remote_elist>(curr->buckets[bucket].base);
      // Past this point we have recursed to an elist
      remote_elist e = is_local(bucket_base) ? bucket_base : pool->Read<EList>(bucket_base, temp_elist);

      // Get elist and linear search
      for (size_t i = 0; i < e->count; i++) {
        // Linear search to determine if elist already contains the value
        pair_t kv = e->pairs[i];
        if (kv.key == key) {
          std::optional<V> result = std::make_optional<V>(kv.val); // saving the previous value at key
          // Edge swap if count != (0 or 1)
          if (e->count > 1) {
            e->pairs[i] = e->pairs[e->count - 1];
          }
          e->count -= 1;
          // If we are modifying the local copy, we need to write to the remote
          if (!is_local(bucket_base)) pool->Write<EList>(bucket_base, *e);
          unlock(pool, get_lock(parent_ptr, bucket), E_UNLOCKED);
          return result;
        }
      }
      unlock(pool, get_lock(parent_ptr, bucket), E_UNLOCKED);
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
  int populate(CountingPool* pool, int op_count, K key_lb, K key_ub, std::function<K(V)> value) {
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

  /// No concurrent or thread safe. Counts the number of elements in the IHT
  int count(CountingPool* pool){
    return count_plist(pool, root, 1);
  }

private:
  int count_plist(CountingPool* pool, remote_plist p, int size){
    int count = 0;
    remote_elist tmp = pool->Allocate<EList>();
    // unmarked because we don't want to read incorrect lock states :) (and we don't synchronize them)
    // I use the cache because I like the CachedObject since it automatically frees data
    CachedObject<PList> plocal_ = cache->ExtendedRead<PList>(unmark_ptr(p), size);
    PList* plocal = to_address(plocal_.get());
    for(int i = 0; i < (size * PLIST_SIZE); i++){
      plist_pair_t pair = plocal->buckets[i];
      if (pair.base == nullptr) continue;
      if (pair.lock == E_UNLOCKED){
        remote_elist elocal_ = pool->Read<EList>(static_cast<remote_elist>(pair.base), tmp);
        EList* elocal = to_address(elocal_);
        count += elocal->count;
      } else if (pair.lock == E_LOCKED) {
        REMUS_ERROR("Counting function should only be used on an IHT snapshot");
      } else {
        count += count_plist(pool, static_cast<remote_plist>(pair.base), size * 2);
      }
    }
    pool->Deallocate<EList>(tmp);
    return count;
  }
};
