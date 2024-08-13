#pragma once

#include <cstdint>
#include <queue>
#include <remus/rdma/rdma.h>
// #include "random.h"
#include "skiplist_api.h"
// #include "slice.h"
#include "timer.h"
#include "WRLock.h"

using namespace remus::rdma;

template <class T>
struct WithFreq {
  const T data;
  mutable uint64_t index_cache_freq;
};

template <class T>
struct CacheEntry {
    uint32_t from;
    uint32_t to;
    mutable WithFreq<T>* data;
};

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
  using CacheSkipList = SkipList<CacheEntry<T>*>;
  static const int SIZE = (DEGREE * 2) + 1;
  uint64_t cache_size; // MB;
  std::atomic<int64_t> free_page_cnt;
  std::atomic<int64_t> skiplist_node_cnt;
  int64_t all_page_cnt;

  std::queue<std::pair<WithFreq<T>*, uint64_t>> delay_free_list;
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
  IndexCache(int cache_size) : cache_size(cache_size) {
    uint64_t memory_size = define::MB * cache_size;
    skiplist = new CacheSkipList();
    all_page_cnt = memory_size / sizeof(T);
    free_page_cnt.store(all_page_cnt);
    skiplist_node_cnt.store(0);
  }

  ~IndexCache(){
    delete skiplist;
  }

  bool add_to_cache(T* page){
    CacheEntry<T>* new_page = (CacheEntry<T>*) malloc(sizeof(CacheEntry<T>));
    WithFreq<T>* payload = (WithFreq<T>*) malloc(sizeof(WithFreq<T>));
    memcpy(payload, page, sizeof(T));
    payload->index_cache_freq = 0;
    new_page->data = payload;

    if (this->add_entry(page->key_low(), page->key_high(), new_page)) {
      skiplist_node_cnt.fetch_add(1);
      auto v = free_page_cnt.fetch_add(-1);
      if (v <= 0) {
        evict_one();
      }

      return true;
    } else { // conflicted but matches the keyrange. can cas to new page
      auto e = this->find_entry(page->key_low(), page->key_high());
      if (e && e->from == page->key_low() && e->to == page->key_high()) {
        WithFreq<T>* ptr = e->data;
        if (ptr == nullptr && __sync_bool_compare_and_swap(&(e->data), 0ull, new_page)) {
          auto v = free_page_cnt.fetch_add(-1);
          if (v <= 0) {
            evict_one();
          }
          return true;
        }
      }

      free(new_page);
      return false;
    }
    return false;
  }

  const CacheEntry<T> *search_from_cache(const Key &k, rdma_ptr<T>* addr,
                                      bool is_leader = false){
    // notice: please ensure the thread 0 can make progress
    if (is_leader && !delay_free_list.empty()) { // try to free a page in the delay-free-list
      std::pair<WithFreq<T>*, uint64_t> p = delay_free_list.front();
      if (asm_rdtsc() - p.second > 3000ull * 10) {
        free(p.first);
        free_page_cnt.fetch_add(1);

        free_lock.wLock();
        delay_free_list.pop();
        free_lock.wUnlock();
      }
    }

    const CacheEntry<T>* entry = find_entry(k);
    WithFreq<T>* page = entry ? entry->data : nullptr;

    if (page != nullptr && entry->from <= k && entry->to >= k) {
      page->index_cache_freq++;
      bool find = false;
      for (int i = 0; i < SIZE; ++i) {
        if (k <= page->data.key_at(i)) {
          find = true;
          *addr = page->data.ptr_at(i);
          break;
        }
      }
      if (!find) {
        *addr = page->data.ptr_at(SIZE);
      }

      compiler_barrier();
      if (entry->data != nullptr) { // check if it is freed.
        return entry;
      }
    }

    return nullptr;
  }

  bool add_entry(const Key &from, const Key &to, CacheEntry<T>* ptr){
    ptr->from = from;
    ptr->to = to;
    return skiplist->insert(from, to, ptr);
  }

  const CacheEntry<T>* find_entry(const Key &k){
    CacheEntry<T>* tmp;
    bool result = skiplist->get(k, &tmp);
    return result ? tmp : nullptr;
  }

  const CacheEntry<T>* find_entry(const Key &low, const Key &high){
    CacheEntry<T>* tmp;
    bool result = skiplist->get(low, &tmp);
    return result ? tmp : nullptr;
  }

  bool invalidate(const CacheEntry<T> *entry) {
    WithFreq<T>* ptr = entry->data;

    if (ptr == nullptr) {
      return false;
    }

    // delete
    if (__sync_bool_compare_and_swap(&(entry->data), ptr, 0)) {
      free_lock.wLock();
      delay_free_list.push(std::make_pair(ptr, asm_rdtsc()));
      free_lock.wUnlock();
      CacheEntry<T>* tmp;
      skiplist->remove(entry->from, entry->to, &tmp);
      // todo: memory leak of cache entry
      return true;
    }

    return false;
  }

  const CacheEntry<T> *get_a_random_entry(uint64_t &freq){
    uint32_t seed = asm_rdtsc();
    rdma_ptr<T> tmp_addr;
  retry:
    auto k = rand_r(&seed) % (1000ull * define::MB);
    const CacheEntry<T>* e = this->search_from_cache(k, &tmp_addr);
    if (!e) {
      goto retry;
    }
    WithFreq<T>* ptr = e->data;
    if (ptr == nullptr) {
      goto retry;
    }

    freq = ptr->index_cache_freq;
    if (e->data != ptr) {
      goto retry;
    }
    return e;
  }

  void statistics(){
    printf("[skiplist node: %ld]  [page cache: %ld]\n", skiplist_node_cnt.load(),
         all_page_cnt - free_page_cnt.load());
  }

  void bench(){
    Timer t;
    t.begin();
    const int loop = 100000;
    for (int i = 0; i < loop; ++i) {
      uint64_t r = rand() % (5 * define::MB);
      this->find_entry(r);
    }
    t.end_print(loop);
  }
};