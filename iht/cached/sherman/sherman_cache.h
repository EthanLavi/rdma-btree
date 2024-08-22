#pragma once

#include <cstdint>
#include <remus/rdma/rdma.h>
// #include "random.h"
#include "skiplist_api.h"
// #include "slice.h"
#include "timer.h"
#include "WRLock.h"
#include "lockfree_sk.h"

using namespace remus::rdma;

// Defintions from sherman
namespace define {
constexpr uint64_t MB = 1024ull * 1024;
constexpr uint64_t GB = 1024ull * MB;
constexpr uint16_t kCacheLineSize = 64;

// for remote allocate
constexpr uint64_t kChunkSize = 32 * MB;

// for store root pointer
constexpr uint64_t kRootPointerStoreOffest = kChunkSize / 2;
static_assert(kRootPointerStoreOffest % sizeof(uint64_t) == 0, "XX");

// lock on-chip memory
constexpr uint64_t kLockStartAddr = 0;
constexpr uint64_t kLockChipMemSize = 256 * 1024;

// number of locks
// we do not use 16-bit locks, since 64-bit locks can provide enough concurrency.
// if you want to use 16-bit locks, call *cas_dm_mask*
constexpr uint64_t kNumOfLock = kLockChipMemSize / sizeof(uint64_t);

// level of tree
constexpr uint64_t kMaxLevelOfTree = 7;

constexpr uint16_t kMaxCoro = 8;
constexpr int64_t kPerCoroRdmaBuf = 128 * 1024;

constexpr uint8_t kMaxHandOverTime = 8;

constexpr int kIndexCacheSize = 1000; // MB
} // namespace define

static inline unsigned long long asm_rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}

inline void compiler_barrier() { asm volatile("" ::: "memory"); }

template <class T, int DEGREE, typename Key>
class IndexCache {
private:
  // using CacheSkipList = SkipList<T>;
  using CacheSkipList = LockFreeSkiplist;
  static const int SIZE = (DEGREE * 2) + 1;
  uint64_t cache_size; // MB;
  std::atomic<int64_t> skiplist_room_entries;
  std::atomic<int64_t> skiplist_success_adds;
  int64_t all_page_cnt;
  LocalEBR<T, 1000>* local_ebr;

  WRLock free_lock;

  // SkipList
  CacheSkipList *skiplist;

  void evict_one(){
    uint64_t freq1, freq2;
    auto e1 = get_a_random_entry(freq1);
    auto e2 = get_a_random_entry(freq2);

    if (freq1 < freq2) {
      invalidate(e1);
    } else {
      invalidate(e2);
    }
  }
public:
  IndexCache(int cache_size, int thread_count) : cache_size(cache_size) {
    uint64_t memory_size = define::MB * cache_size;
    skiplist = new CacheSkipList(thread_count);
    all_page_cnt = memory_size / sizeof(T);
    REMUS_INFO("!CHECKME! {} {} {}", memory_size, sizeof(T), all_page_cnt);
    skiplist_room_entries.store(all_page_cnt);
    skiplist_success_adds.store(0);
    local_ebr = new LocalEBR<T, 1000>(thread_count);
  }

  ~IndexCache(){
    delete skiplist;
  }

  bool add_to_cache(const T* page){
    T* data = (T*) malloc(sizeof(T));
    *data = *page;
    if (add_entry(page->key_low(), page->key_high(), data)) {
      skiplist_success_adds.fetch_add(1);
      auto v = skiplist_room_entries.fetch_sub(1);
      if (v <= 0) {
        evict_one();
      }
      local_ebr->match_version();
      return true;
    }
    local_ebr->match_version();
    return false;
  }

  bool search_from_cache(const Key &k, rdma_ptr<T>* addr, bool is_leader = false){
    int accesses = -1;
    T* page = find_entry(k, &accesses);
    if (accesses == -1 || page == nullptr) {
      local_ebr->match_version();
      return false;
    }
    if (!page->key_in_range(k)) {
      local_ebr->match_version();
      return false;
    }

    bool find = false;
    for (int i = 0; i < SIZE; ++i) {
      if (k <= page->key_at(i)) {
        find = true;
        *addr = page->ptr_at(i);
        break;
      }
    }
    if (!find) {
      *addr = page->ptr_at(SIZE);
    }
    local_ebr->match_version();
    return true;
  }

  bool add_entry(const Key &from, const Key &to, T* data){
    return skiplist->insert(from, to, data);
  }

  T* get_a_random_entry(uint64_t &freq){
    uint32_t seed = asm_rdtsc();
  retry:
    auto k = rand_r(&seed) % (1000ull * define::MB);
    int accesses = -1;
    T* page = find_entry(k, &accesses);
    if (accesses == -1 || page == nullptr) goto retry;
  
    freq = accesses;
    return page;
  }

  /// Return data or nullptr
  T* find_entry(const Key &k, int* accesses){
    void* data = skiplist->get(k, accesses);
    return (T*) data;
  }

  // T* find_entry(const Key &low, const Key &high, int* accesses){
  //   void* data = skiplist->get(low, accesses);
  //   return (T*) data;
  // }

  bool invalidate(const Key& key) {
    void* result = skiplist->remove(key, key);
    if (result != nullptr) {
      skiplist_room_entries.fetch_add(1);
      local_ebr->deallocate((T*) result);
    }
    return result;
  }

  bool invalidate(T* data) {
    void* result = skiplist->remove(data->key_low(), data->key_high());
    if (result != nullptr) {
      skiplist_room_entries.fetch_add(1);
      local_ebr->deallocate((T*) result);
    }
    return result;
  }

  void statistics(){
    long number_of_nodes = skiplist->count();
    int per_node = sizeof(skiplist_node) + sizeof(T) + sizeof(int) * 3;
    REMUS_INFO("!CHECKME! sizeof(for each entry) {}", per_node);
    printf("[skiplist adds: %ld]  [skiplist load: %ld] [count: %ld] [size: %ld KB]\n", skiplist_success_adds.load(), 
      all_page_cnt - skiplist_room_entries.load(), number_of_nodes, number_of_nodes * per_node / 1000);
  }

  void bench(){
    Timer t;
    t.begin();
    const int loop = 100000;
    for (int i = 0; i < loop; ++i) {
      uint64_t r = rand() % (5 * define::MB);
      find_entry(r, nullptr);
    }
    t.end_print(loop);
  }
};