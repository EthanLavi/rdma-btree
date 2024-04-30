#pragma once

#include <random>
#include <remus/rdma/rdma.h>

#include "common.h"
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>

using namespace remus::rdma;

/// Something that is both a normal PList and a RDMA Plist (in RDMA accessible memory)
template <class T>
class CachedRDMA {
private:
  rdma_ptr<T> plist;
  T* plist_cached;
  bool is_cached;
  int size;
  bool do_dealloc = true;
  // Function to relieve the cached plist of the responsibility to deallocate
  void dump(){
    do_dealloc = false;
  }

public:
  CachedRDMA() {
    plist = nullptr;
    plist_cached = nullptr;
    is_cached = true;
    size = -1;
  }

  CachedRDMA(rdma_ptr<T> ptr, int depth){
    plist_cached = nullptr;
    plist = ptr;
    is_cached = false;
    size = 1 << depth;
  }

  CachedRDMA(T* ptr){
    plist = nullptr;
    plist_cached = ptr;
    is_cached = true;
    size = -1;
  }

  // Can be accessed as either a 
  T* operator->() const {
    if (is_cached) return plist_cached;
    return std::to_address(plist);
  }

  // Will deallocate itself
  // todo: RAII
  inline void deallocate(std::shared_ptr<rdma_capability> pool) {
    if (!is_cached) pool->Deallocate(plist, size);
  }
};

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
  typedef CachedRDMA<PList> CachedPList;

  template <typename T> inline bool is_local(rdma_ptr<T> ptr) {
    return ptr.id() == self_.id;
  }

  template <typename T> inline bool is_null(rdma_ptr<T> ptr) {
    return ptr == nullptr;
  }

  // Get the address of the lock at bucket (index)
  remote_lock get_lock(remote_plist arr_start, int index){
      uint64_t new_addy = arr_start.address();
      new_addy += (sizeof(plist_pair_t) * index) + 8;
      return remote_lock(arr_start.id(), new_addy);
  }

  // Get the address of the baseptr at bucket (index)
  rdma_ptr<remote_baseptr> get_baseptr(remote_plist arr_start, int index){
      uint64_t new_addy = arr_start.address();
      new_addy += sizeof(plist_pair_t) * index;
      return rdma_ptr<remote_baseptr>(arr_start.id(), new_addy);
  }

  /// @brief Initialize the plist with values.
  /// @param p the plist pointer to init
  /// @param depth the depth of p, needed for PLIST_SIZE == base_size * (2 **
  /// (depth - 1)) pow(2, depth)
  inline void InitPList(remote_plist p, int mult_modder) {
    assert(sizeof(plist_pair_t) == 16); // Assert I did my math right...
    // memset(std::to_address(p), 0, 2 * sizeof(PList));
    for (size_t i = 0; i < PLIST_SIZE * mult_modder; i++){
      p->buckets[i] = {
        nullptr, E_UNLOCKED
      };
    }
  }

  remote_plist root; // Start of plist

  /// Acquire a lock on the bucket. Will prevent others from modifying it
  bool acquire(std::shared_ptr<rdma_capability> pool, remote_lock lock) {
    // Spin while trying to acquire the lock
    while (true) {
      // Can this be a CAS on an address within a PList?
      lock_type v = pool->CompareAndSwap<lock_type>(lock, E_UNLOCKED, E_LOCKED);

      // Permanent unlock
      if (v == P_UNLOCKED) { return false; }
      // If we can switch from unlock to lock status
      if (v == E_UNLOCKED) { return true; }
    }
  }

  /// @brief Unlock a lock ==> the reverse of acquire
  /// @param lock the lock to unlock
  /// @param unlock_status what should the end lock status be.
  inline void unlock(std::shared_ptr<rdma_capability> pool, remote_lock lock, uint64_t unlock_status) {
    pool->Write<lock_type>(lock, unlock_status, temp_lock, internal::RDMAWriteWithAck);
  }

  /// @brief Change the baseptr for a given bucket to point to a different EList or a different PList
  /// @param list_start the start of the plist (bucket list)
  /// @param bucket the bucket to manipulate
  /// @param baseptr the new pointer that bucket should have
  //
  // [mfs] I don't really understand this
  // [esl] Is supposed to be a short-hand for manipulating the EList/PList pointer within a bucket.
  //       I tried to update my documentation to make that more clear
  inline void change_bucket_pointer(std::shared_ptr<rdma_capability> pool, remote_plist list_start,
                                    uint64_t bucket, remote_baseptr baseptr) {
    rdma_ptr<remote_baseptr> bucket_ptr = get_baseptr(list_start, bucket);
    // [mfs] Can this address manipulation be hidden?
    // [esl] todo: I think Rome needs to support for the [] operator in the remote ptr...
    //             Otherwise I am forced to manually calculate the pointer of a bucket
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
  remote_plist rehash(std::shared_ptr<rdma_capability> pool, CachedPList parent, size_t pcount, size_t pdepth,
                      size_t pidx) {
    // pow(2, pdepth);
    pcount = pcount * 2;
    // how much bigger than original size we are
    int plist_size_factor = (pcount / PLIST_SIZE);

    // 2 ^ (depth) ==> in other words (depth:factor). 0:1, 1:2, 2:4, 3:8, 4:16,
    // 5:32.
    remote_plist new_p = pool->Allocate<PList>(plist_size_factor);
    InitPList(new_p, plist_size_factor);

    // hash everything from the full elist into it
    remote_elist parent_bucket = static_cast<remote_elist>(parent->buckets[pidx].base);
    remote_elist source = is_local(parent_bucket)
                              ? parent_bucket
                              : pool->Read<EList>(parent_bucket);
    
    // insert everything from the elist we rehashed into the plist
    for (size_t i = 0; i < source->count; i++) {
      uint64_t b = level_hash(source->pairs[i].key, pdepth + 1, pcount);
      if (is_null(new_p->buckets[b].base)) {
        remote_elist e = pool->Allocate<EList>();
        new_p->buckets[b].base = static_cast<remote_baseptr>(e);
        new_p->buckets[b].lock = E_UNLOCKED;
      }
      remote_elist dest = static_cast<remote_elist>(new_p->buckets[b].base);
      dest->elist_insert(source->pairs[i]);
    }
    // Deallocate the old elist
    // TODO replace for a remote deallocation at some point
    pool->Deallocate<EList>(source);
    return new_p;
  }

  // Cache for three layers
  // todo: replace for std::array?
  // make three different depths of PList caches
  PList** layer_pointers[3]; 
  bool* is_cached_pointers[3];
  PList* layer_1[1]; 
  bool is_l1_cached[1] = {false};
  PList* layer_2[PLIST_SIZE];
  bool is_l2_cached[PLIST_SIZE];
  PList* layer_3[PLIST_SIZE * PLIST_SIZE * 2];
  bool is_l3_cached[PLIST_SIZE * PLIST_SIZE * 2];
  
  // preallocated memory for RDMA operations (avoiding frequent allocations)
  remote_lock temp_lock;
  rdma_ptr<remote_baseptr> temp_ptr;
  remote_elist temp_elist;
  // N.B. I don't bother creating preallocated PLists since we're hoping to cache them anyways :)

  /// @brief Try to fetch the cached value for a remote pointer
  /// If it is cached, then it returns a non null
  PList* __attribute__((optimize("O2"))) fetch_cache(int* bucket_path) {
    // Bucket path:
    // [-1 -1 -1] -- root level (depth 1)
    // [n -1 -1] -- depth 2
    // [n1 n2 -1] -- depth 3
    int sizes[3] = {1, PLIST_SIZE, 2 * PLIST_SIZE * PLIST_SIZE};
    int bucket_of_cache = 0;
    bool* is_cached_list;
    PList** caches_list;
    int count = PLIST_SIZE;

    for(int i = 0; i < 8; i++){
      // Calculate the cache list and the bucket in such list
      is_cached_list = this->is_cached_pointers[i];
      caches_list = this->layer_pointers[i];
      if (i == cache_depth_) return nullptr;
      if (bucket_path[i] == -1) break;
      bucket_of_cache = (bucket_of_cache * sizes[i]) + bucket_path[i];
      count *= 2;
    }
    if (is_cached_list[bucket_of_cache])
      return caches_list[bucket_of_cache];
    else 
      return nullptr;
  }

  /// @brief Check if a PList is fully calcified, if so cache it
  /// @return if it was cached
  bool try_cache(remote_plist root_ptr, int* bucket_path) {
    // Bucket path:
    // [-1 -1 -1] -- root level (depth 1)
    // [n -1 -1] -- depth 2
    // [n1 n2 -1] -- depth 3
    // [n1 n2 n3] -- don't cache
    const int sizes[3] = {1, PLIST_SIZE, 2 * PLIST_SIZE * PLIST_SIZE};
    int bucket_of_cache = 0;
    bool* is_cached_list;
    PList** caches_list;
    int count = PLIST_SIZE;
    for(int i = 0; i < 8; i++){
      // Calculate the cache list and the bucket in such list
      is_cached_list = this->is_cached_pointers[i];
      caches_list = this->layer_pointers[i];
      if (i == cache_depth_) return false; // todo: max_depth of caching is 2
      if (bucket_path[i] == -1) break;
      bucket_of_cache = (bucket_of_cache * sizes[i]) + bucket_path[i];
      count *= 2;
    }
    // If cached, return false
    if (is_cached_list[bucket_of_cache]) return false;

    // Check if the pointer is cache-able
    for (int i = 0; i < count - 1; i++){
      if (root_ptr->buckets[i].lock != P_UNLOCKED)
        return false;
    }
    // If we made it here, we have a calcified plist, and thus we save it
    PList* cached_plist = (PList*) malloc(sizeof(plist_pair_t) * count);
    for(int i = 0; i < count; i++){
      cached_plist->buckets[i] = root_ptr->buckets[i];
    }
    caches_list[bucket_of_cache] = cached_plist;
    is_cached_list[bucket_of_cache] = true;
    return true;
  }

  struct descent_context {
    /// the elist (accessible elist, may be equal to bucket_base)
    remote_elist e; 
    /// the ptr to the original elist
    remote_elist bucket_base;
    /// The depth in the tree
    size_t depth;
    /// the number of buckets in the level
    size_t count;
    /// The bucket we are hased at
    uint64_t bucket;
    /// The ptr to the original current plist
    remote_plist parent_ptr;
    /// An accessible version of the current plist
    CachedPList curr;
  };

  /// Descend to a elist and do an action on descent_context. Used to implement all functions
  void do_with(std::shared_ptr<rdma_capability> pool, K key, std::function<bool(descent_context*)> apply){
    // a context object with important variables
    descent_context ctx;
    // Define some constants
    ctx.depth = 1;
    ctx.count = PLIST_SIZE;
    ctx.parent_ptr = root;
    int bucket_path[8] = { -1, -1, -1, -1, -1, -1, -1, -1 };

    // start at root
    PList* cache = fetch_cache(bucket_path);

    // todo: entropy of caching?
    // make curr cached or not
    if (cache == nullptr){
      remote_plist root_red = pool->Read<PList>(root);
      ctx.curr = CachedPList(root_red, 0);
      if (try_cache(root_red, bucket_path)){
        REMUS_TRACE("Cached at depth:{} key:{} (ROOT)", ctx.depth, key);
      }
    } else {
      // Go down with the plist as cached
      ctx.curr = CachedPList(cache);
    }
    
    while (true) {
      ctx.bucket = level_hash(key, ctx.depth, ctx.count);
      bucket_path[ctx.depth - 1] = (int) ctx.bucket;
      // Normal descent
      if (ctx.curr->buckets[ctx.bucket].lock == P_UNLOCKED){
        auto bucket_base = static_cast<remote_plist>(ctx.curr->buckets[ctx.bucket].base);
        ctx.curr.deallocate(pool);
        PList* cache = fetch_cache(bucket_path);
        if (cache == nullptr){
          remote_plist curr_red = pool->ExtendedRead<PList>(bucket_base, 1 << ctx.depth);
          ctx.curr = CachedPList(curr_red, ctx.depth);
          if (try_cache(curr_red, bucket_path)){
            REMUS_TRACE("Cached at depth:{} key:{} bucket:{}", ctx.depth + 1, key, ctx.bucket);
          }
        } else {
          ctx.curr = CachedPList(cache);
        }
        ctx.parent_ptr = bucket_base;
        ctx.depth++;
        ctx.count *= 2;
        continue;
      }

      // Erroneous descent into EList (Think we are at an EList, but it turns out its a PList)
      if (!acquire(pool, get_lock(ctx.parent_ptr, ctx.bucket))){
          // We must re-fetch the PList to ensure freshness of our pointers (1 << depth-1 to adjust size of read with customized ExtendedRead)
          ctx.curr.deallocate(pool);
          ctx.curr = CachedPList(pool->ExtendedRead<PList>(ctx.parent_ptr, 1 << (ctx.depth - 1)), ctx.depth - 1);
          continue;
      }

      // We locked an elist, we can read the baseptr and progress
      ctx.bucket_base = static_cast<remote_elist>(ctx.curr->buckets[ctx.bucket].base);
      // Past this point we have recursed to an elist
      ctx.e = is_local(ctx.bucket_base) || is_null(ctx.bucket_base) ? ctx.bucket_base : pool->Read<EList>(ctx.bucket_base, temp_elist);
      // apply function to the elist
      if (apply(&ctx)){
        unlock(pool, get_lock(ctx.parent_ptr, ctx.bucket), E_UNLOCKED);
        // deallocate plist that brought us to the elist & exit
        ctx.curr.deallocate(pool);
        break;
      } else {
        unlock(pool, get_lock(ctx.parent_ptr, ctx.bucket), P_UNLOCKED);
        continue;
      }
    }
  }

public:
  RdmaIHT(Peer& self, CacheDepth::CacheDepth cache_depth, std::shared_ptr<rdma_capability> pool) : self_(std::move(self)), cache_depth_(cache_depth) {
    this->layer_pointers[0] = this->layer_1;
    this->layer_pointers[1] = this->layer_2;
    this->layer_pointers[2] = this->layer_3;
    for(int i = 0; i < PLIST_SIZE; i++){
      this->is_l2_cached[i] = false;
    }
    for(int i = 0; i < PLIST_SIZE * PLIST_SIZE * 2; i++){
      this->is_l3_cached[i] = false;
    }
    this->is_cached_pointers[0] = this->is_l1_cached;
    this->is_cached_pointers[1] = this->is_l2_cached;
    this->is_cached_pointers[2] = this->is_l3_cached;
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
  void destroy(std::shared_ptr<rdma_capability> pool) {
    // Have to deallocate "8" of them to account for alignment
    // [esl] This "deallocate 8" is a hack to get around a rome memory leak. (must fix rome to fix this)
    pool->Deallocate<lock_type>(temp_lock, 8);
    pool->Deallocate<remote_baseptr>(temp_ptr, 8);
    pool->Deallocate<EList>(temp_elist);

    // if l1 is cached, free it
    if (is_l1_cached[0]) free(layer_1[0]);
    // if l2 is cached, free it
    for(int i = 0; i < PLIST_SIZE; i++){
      if (is_l2_cached[i]) free(layer_2[i]);
    }
    // if l3 is cached, free it
    for(int i = 0; i < PLIST_SIZE * PLIST_SIZE * 2; i++){
      if (is_l3_cached[i]) free(layer_3[i]);
    }
  }

  /// @brief Create a fresh iht
  /// @param pool the capability to init the IHT with
  /// @return the iht root pointer
  rdma_ptr<anon_ptr> InitAsFirst(std::shared_ptr<rdma_capability> pool){
      remote_plist iht_root = pool->Allocate<PList>();
      InitPList(iht_root, 1);
      this->root = iht_root;
      return static_cast<rdma_ptr<anon_ptr>>(iht_root);
  }

  /// @brief Initialize an IHT from the pointer of another IHT
  /// @param root_ptr the root pointer of the other iht from InitAsFirst();
  void InitFromPointer(rdma_ptr<anon_ptr> root_ptr){
      this->root = static_cast<remote_plist>(root_ptr);
  }

  /// @brief Gets a value at the key.
  /// @param pool the capability providing one-sided RDMA
  /// @param key the key to search on
  /// @return an optional containing the value, if the key exists
  std::optional<V> contains(std::shared_ptr<rdma_capability> pool, K key) {
    std::optional<V> result = std::nullopt;
    do_with(pool, key, [&](descent_context* ctx){
      if (is_null(ctx->e)) return true;

      // Get elist and linear search
      for (size_t i = 0; i < ctx->e->count; i++) {
        // Linear search to determine if elist already contains the key 
        pair_t kv = ctx->e->pairs[i];
        if (kv.key == key) {
          result = std::make_optional<V>(kv.val);
          return true;
        }
      }
      return true;
    });
    return result;
  }

  /// @brief Insert a key and value into the iht. Result will become the value
  /// at the key if already present.
  /// @param pool the capability providing one-sided RDMA
  /// @param key the key to insert
  /// @param value the value to associate with the key
  /// @return an empty optional if the insert was successful. Otherwise it's the value at the key.
  std::optional<V> insert(std::shared_ptr<rdma_capability> pool, K key, V value) {
    std::optional<V> result = std::nullopt;
    do_with(pool, key, [&](descent_context* ctx){
      // Past this point we have recursed to an elist
      if (is_null(ctx->e)) {
        // If we are we need to allocate memory for our elist
        remote_elist e_new = pool->Allocate<EList>();
        e_new->count = 0;
        e_new->elist_insert(key, value);
        remote_baseptr e_base = static_cast<remote_baseptr>(e_new);
        // modify the parent's bucket's pointer and unlock
        change_bucket_pointer(pool, ctx->parent_ptr, ctx->bucket, e_base);
        // successful insert
        return true;
      }

      // We have recursed to an non-empty elist
      for (size_t i = 0; i < ctx->e->count; i++) {
        // Linear search to determine if elist already contains the key
        pair_t kv = ctx->e->pairs[i];
        if (kv.key == key) {
          result = std::make_optional<V>(kv.val);
          return true;
        }
      }

      // Check for enough insertion room
      if (ctx->e->count < ELIST_SIZE) {
        // insert, unlock, return
        ctx->e->elist_insert(key, value);
        // If we are modifying a local copy, we need to write to the remote at the end
        if (!is_local(ctx->bucket_base)) pool->Write<EList>(static_cast<remote_elist>(ctx->bucket_base), *ctx->e);
        return true;
      }

      // Need more room so rehash into plist and perma-unlock
      // todo? Make it possible to prevent the loop by inserting with the current key/value in rehash function call
      remote_plist p = rehash(pool, ctx->curr, ctx->count, ctx->depth, ctx->bucket);

      // modify the bucket's pointer, keeping local curr updated with remote curr
      ctx->curr->buckets[ctx->bucket].base = static_cast<remote_baseptr>(p);
      change_bucket_pointer(pool, ctx->parent_ptr, ctx->bucket, static_cast<remote_baseptr>(p));
      ctx->curr->buckets[ctx->bucket].lock = P_UNLOCKED;
      return false;
    });
    return result;
  }

  /// @brief Will remove a value at the key. Will stored the previous value in
  /// result.
  /// @param pool the capability providing one-sided RDMA
  /// @param key the key to remove at
  /// @return an optional containing the old value if the remove was successful. Otherwise an empty optional.
  std::optional<V> remove(std::shared_ptr<rdma_capability> pool, K key) {
    std::optional<V> result = std::nullopt;
    do_with(pool, key, [&](descent_context* ctx){
      // If elist is null just return and unlock
      if (is_null(ctx->e)) return true;

      // Get elist and linear search
      for (size_t i = 0; i < ctx->e->count; i++) {
        // Linear search to determine if elist already contains the value
        pair_t kv = ctx->e->pairs[i];
        if (kv.key == key) {
          result = std::make_optional<V>(kv.val); // saving the previous value at key
          // Edge swap if count != (0 or 1)
          if (ctx->e->count > 1) {
            ctx->e->pairs[i] = ctx->e->pairs[ctx->e->count - 1];
          }
          ctx->e->count -= 1;
          // If we are modifying the local copy, we need to write to the remote
          if (!is_local(ctx->bucket_base)) pool->Write<EList>(static_cast<remote_elist>(ctx->bucket_base), *ctx->e);
        }
      }
      return true;
    });
    return result;
  }

  /// @brief Populate only works when we have numerical keys. Will add data
  /// @param pool the capability providing one-sided RDMA
  /// @param op_count the number of values to insert. Recommended in total to do
  /// key_range / 2
  /// @param key_lb the lower bound for the key range
  /// @param key_ub the upper bound for the key range
  /// @param value the value to associate with each key. Currently, we have
  /// asserts for result to be equal to the key. Best to set value equal to key!
  void populate(std::shared_ptr<rdma_capability> pool, int op_count, K key_lb, K key_ub, std::function<K(V)> value) {
    // Populate only works when we have numerical keys
    K key_range = key_ub - key_lb;
    // todo: Under-populating because of insert collisions?
    // Create a random operation generator that is
    // - evenly distributed among the key range
    std::uniform_real_distribution<double> dist = std::uniform_real_distribution<double>(0.0, 1.0);
    std::default_random_engine gen(std::chrono::system_clock::now().time_since_epoch().count());
    for (int c = 0; c < op_count; c++) {
      int k = (dist(gen) * key_range) + key_lb;
      insert(pool, k, value(k));
      // Wait some time before doing next insert...
      std::this_thread::sleep_for(std::chrono::nanoseconds(10));
    }
  }

  /// @brief debug print
  /// N.B. Will naively iterate through the PList (without acquiring locks or doing RDMA requests)
  void print(remote_plist start, int count, int indent){
    std::string out = "";
    for(int k = 0; k < indent; k++){
      out = out + "    ";
    }
    if (indent == 3){
      return;
    }
    for(int i = 0; i < count; i++){
      plist_pair_t t = start->buckets[i];
      if (t.lock == P_UNLOCKED){
        REMUS_INFO("{}Bucket: {} with {}", out, i, count * 2);
        this->print(static_cast<remote_plist>(t.base), count * 2, indent + 1);
      } else if (t.lock == E_UNLOCKED) {
        if (t.base == nullptr){
          REMUS_INFO("{}Bucket: {} is Empty", out, i);
          continue;
        }
        EList e = *static_cast<remote_elist>(t.base);
        for(int j = 0; j < e.count; j++){
          pair_t p = e.pairs[j];
          REMUS_INFO("{}Bucket: {} has Key: {} Value:{}", out, i, p.key, p.val);
        }
      } else if (t.lock == E_LOCKED){
        REMUS_INFO("{}Locked bucket {}", out, t.lock);
      } else {
        REMUS_INFO("{}Weird lock val of {}", out, t.lock);
      }
    }
  }
};