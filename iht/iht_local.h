#pragma once

#include <remus/logging/logging.h>

#include <atomic>
#include <functional>
#include <optional>

/// iht_umap is a relatively straightforward lock-based implementation of the
/// interlocked hash table.  There are two significant simplifications:
template <class K, class V, int ELIST_SIZE, int PLIST_SIZE>
class iht_carumap {
  /// States for IHT's spin-lock.  These get associated (1:1) with the pointers
  /// in a P-List.  Once a pointer goes from referencing an E-List to
  /// referencing a PList, it won't go back, so its lock should no longer be
  /// acquired.  The lock's value also indicates the type of the object
  /// references by the associated pointer.
  /* E_LOCKED and E_UNLOCKED are the toggle states. P_UNLOCKED is forever unlocked because its pointerlist now */
  enum lock_state_t { E_LOCKED, E_UNLOCKED, P_UNLOCKED };

  /// Common parent for EList and PList types.
  ///
  /// NB: In golang, the lock would go in Base.
  struct Base {};

  /// EList (ElementList) stores a bunch of K/V pairs
  ///
  /// NB: We construct with a factory, so the pairs can be a C-style variable
  ///     length array field.
  struct EList : Base {
    /// The key/value pair.  We don't structure split, so that we can have the
    /// array as a field.
    struct pair_t {
      K key; // A key
      V val; // A value
    };

    // The number of live elements in this EList
    size_t count;   

    // The K/V pairs stored in this EList
    pair_t pairs[ELIST_SIZE]; 

  private:
    /// Force construction via the make_elist factory
    EList() = default;

  public:
    /// Construct a EList that can hold up to `size` elements
    static EList *make(size_t size) {
      // Create an element list of size
      EList *e = (EList *)malloc(sizeof(EList) + size * sizeof(pair_t));
      // Set count to 0 and return it
      e->count = 0;
      return e;
    }

    /// Insert into an EList, without checking if there is enough room
    void unchecked_insert(const K &key, const V &val) {
      /// Put it in the next available slot.
      /// EX: Why?
      pairs[count].key = key;
      pairs[count].val = val;
      ++count;
    }
  };

  /// PList (PointerList) stores a bunch of pointers and their associated locks
  ///
  /// NB: We construct with a factory, so the pairs can be a C-style variable
  ///     length array field.  This means that `depth` and `count` can't be
  ///     const, but that's OK.
  struct PList : Base {
    /// A pointer/lock pair
    struct pair_t {
      Base *base;                     // A pointer to a P-List or E-List
      std::atomic<lock_state_t> lock; // A lock (also expresses type of `base`)
    };

    // The pointer/lock pairs stored in this P-List
    pair_t buckets[PLIST_SIZE];
  private:
    /// Force construction via the make_plist factory
    PList() = default;

  public:
    /// Construct a PList at depth `depth` that can hold up to `size` elements
    static PList *make(size_t size) {
      // Create a pointer list of size
      PList *p = (PList *) malloc(sizeof(PList) * (size / PLIST_SIZE));
      // Iterate through each bucket and unlock + set with nullptr
      for (size_t i = 0; i < size; ++i) {
        p->buckets[i].lock = E_UNLOCKED;
        p->buckets[i].base = nullptr;
      }
      // Return created list
      return p;
    }
  };

  PList *root;             // The root P-List

  /// Acquire a lock.  If this returns false, it means the pointer being locked
  /// has become a PointerList, and can't be locked anymore
  static bool acquire(std::atomic<lock_state_t> &lock) {
    while (true) {
      // Spin while trying to aquire lock
      auto v = lock.load();
      // If unlocked and can be set to lock, return true
      if (v == E_UNLOCKED && lock.compare_exchange_weak(v, E_LOCKED))
        return true;
      // If "forever" unlocked, just return false
      if (v == P_UNLOCKED)
        return false;
    }
  }

  /// For the time being, we re-hash a key at each level, xor-ing in the level
  /// so that keys are unlikely to collide repeatedly.
  ///
  /// [TODO] This shouldn't be too expensive, but we can probably do better.
  inline uint64_t level_hash(const K &key, size_t level, size_t count) {
    std::hash<K> to_num;
    size_t prehash = to_num(key) ^ level;
    // mix13 implementation
    prehash ^= (prehash >> 33);
    prehash *= 0xff51afd7ed558ccd;
    prehash ^= (prehash >> 33);
    prehash *= 0xc4ceb9fe1a85ec53;
    prehash ^= (prehash >> 33);

    // 1) pre_hash will first map the type K to size_t
    //    then pre_hash will help distribute non-uniform inputs evenly by applying a finalizer
    return prehash % count;
  }

  /// Given a P-List where the `index`th bucket is a full E-List, create a new
  /// P-List that is twice the size of `parent` and hash the full E-List's
  /// elements into it.  This only takes O(1) time.
  ///
  /// NB: we assume that parent.buckets[pidx].lock is held by the caller
  ///
  /// @param parent The P-List whose bucket needs rehashing
  /// @param pcount The number of elements in `parent`
  /// @param pdepth The depth of `parent`
  /// @param pidx   The index in `parent` of the bucket to rehash
  PList *rehash(PList *parent, size_t pcount, size_t pdepth, size_t pidx) {
    // Make a new P-List that is twice as big, with all locks set to E_UNLOCKED
    PList* p = PList::make(pcount * 2);

    // We are at the lowest level P-list
    // Grab the EList (use static cast because we are sure its an ELIST)
    EList* source = static_cast<EList *>(parent->buckets[pidx].base);
    for (size_t i = 0; i < source->count; ++i) {
      // Hash to find the bucket
      uint64_t b = level_hash(source->pairs[i].key, pdepth + 1, pcount);
      // If we have a nullptr, make an Elist (might already be created by other nodes)
      if (p->buckets[b].base == nullptr)
        p->buckets[b].base = EList::make(ELIST_SIZE);
      // Get the destination bucket of the plist.
      EList *dest = static_cast<EList *>(p->buckets[b].base);
      // Insert it into the destination
      dest->unchecked_insert(source->pairs[i].key, source->pairs[i].val);
    }

    // The caller locked the pointer to the E-List, so we can reclaim the E-List
    delete source;
    return p;
  }

public:
  /// Construct an IHT by configuring the constants and building the root P-List
  ///
  /// @param e_size The size of E-Lists
  /// @param p_size The size of P-Lists
  iht_carumap()
      : root(PList::make(PLIST_SIZE)) {}

  /// Search for a key in the map.  If found, return `true` and set the ref
  /// parameter `val` to the associated value.  Otherwise return `false`.
  ///
  /// @param key The key to search for
  /// @param val The value (pass-by-ref) that was found
  bool get(const K &key, V &val) {
    PList* curr = root; // Start at the root P-List
    size_t depth = 1, count = PLIST_SIZE;
    while (true) {
      // Repeat until traversing down
      u_int64_t bucket = level_hash(key, depth + 1, count);
      // If we can't lock, then it's a P-List, so traverse down
      if (!acquire(curr->buckets[bucket].lock)) {
        curr = static_cast<PList *>(curr->buckets[bucket].base);
        ++depth;
        count *= 2;
        continue;
      }
      // If it's null, unlock and fail
      if (curr->buckets[bucket].base == nullptr) {
        curr->buckets[bucket].lock = E_UNLOCKED;
        return false;
      }
      // If it's not null, do a linear search of the keys
      EList* e = static_cast<EList *>(curr->buckets[bucket].base);
      for (size_t i = 0; i < e->count; ++i) {
        // If we have a matching key
        if (e->pairs[i].key == key) {
          // Get the value (setting to the reference)
          val = e->pairs[i].val;
          // Unlock and return true
          curr->buckets[bucket].lock = E_UNLOCKED;
          return true;
        }
      }
      // Not found
      curr->buckets[bucket].lock = E_UNLOCKED;
      return false;
    }
  }

  /// Search for a key in the map.  If found, remove it and its associated value
  /// and return `true`.  Otherwise return `false`.
  ///
  /// @param key The key to search for
  /// @param value Will be set to the removed value
  bool remove(const K &key, V& val) {
    PList* curr = root; // Start at the root P-List
    size_t depth = 1, count = PLIST_SIZE;
    while (true) {
      u_int64_t bucket = level_hash(key, depth + 1, count);
      // If we can't lock, then it's a P-List, so traverse down
      if (!acquire(curr->buckets[bucket].lock)) {
        curr = static_cast<PList *>(curr->buckets[bucket].base);
        ++depth;
        count *= 2;
        continue;
      }
      // If it's null, unlock and fail
      if (curr->buckets[bucket].base == nullptr) {
        curr->buckets[bucket].lock = E_UNLOCKED;
        return false;
      }
      // If it's not null, do a linear search of the keys
      EList* e = static_cast<EList *>(curr->buckets[bucket].base);
      for (size_t i = 0; i < e->count; ++i) {
        // Iterate through values
        if (e->pairs[i].key == key) {
          // remove the K/V pair by overwriting, but only if there's >1 key (swaps with last element)
          val = e->pairs[i].val;
          if (e->count > 1)
            e->pairs[i] = e->pairs[e->count - 1];
          e->count -= 1;
          curr->buckets[bucket].lock = E_UNLOCKED;
          return true;
        }
      }
      // Not found
      curr->buckets[bucket].lock = E_UNLOCKED;
      return false;
    }
  }

  /// Insert a new key/value pair into the map, but only if the key is not
  /// already present.  Return `nullopt` if a mapping was added, `old value` otherwise.
  ///
  /// @param key The key to try to insert
  /// @param val The value to try to insert
  std::optional<V> insert(const K key, const V val) {
    PList* curr = root; // Start at the root P-List
    size_t depth = 1, count = PLIST_SIZE;
    while (true) {
      u_int64_t bucket = level_hash(key, depth + 1, count);
      // If we can't lock, then it's a P-List, so traverse down
      if (!acquire(curr->buckets[bucket].lock)) {
        // Get next p-list
        curr = static_cast<PList *>(curr->buckets[bucket].base);
        ++depth;
        count *= 2;
        continue;
      }
      // If it's null, make a new E-List, insert, and we're done
      if (curr->buckets[bucket].base == nullptr) {
        EList* e = EList::make(ELIST_SIZE);
        e->unchecked_insert(key, val);
        curr->buckets[bucket].base = e;
        curr->buckets[bucket].lock = E_UNLOCKED;
        return std::nullopt;
      }
      // If It's not null, do a linear search of the keys, return false if found
      EList* e = static_cast<EList *>(curr->buckets[bucket].base);
      for (size_t i = 0; i < e->count; ++i) {
        if (e->pairs[i].key == key) {
          V val = e->pairs[i].val;
          curr->buckets[bucket].lock = E_UNLOCKED;
          return std::make_optional(val);
        }
      }
      // Not found: insert if room
      if (e->count < ELIST_SIZE) {
        e->unchecked_insert(key, val);
        curr->buckets[bucket].lock = E_UNLOCKED;
        return std::nullopt;
      }
      // Otherwise expand and keep traversing, because pathological hash
      // collisions are always possible.
      curr->buckets[bucket].base = rehash(curr, count, depth, bucket);
      curr->buckets[bucket].lock = P_UNLOCKED;
    }
  }
};