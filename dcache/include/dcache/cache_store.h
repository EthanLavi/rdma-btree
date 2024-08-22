#pragma once

#include <atomic>
#include <cstdint>
#include <remus/logging/logging.h>
#include <remus/rdma/memory_pool.h>
#include <remus/rdma/rdma.h>
#include <remus/rdma/rdma_ptr.h>
#include <vector>

#include "object_pool.h"
#include "cached_ptr.h"
#include "mark_ptr.h"
#include "metrics.h"
// #include "../test/faux_mempool.h"

using namespace remus::rdma;

#define USE_RW_LOCK true // use rw lock instead of normal mutex
// #define EXPERIMENTAL true // (only used if USE_RW_LOCK is true, invalidate locally by acquiring shared-lock instead of exclusive-lock)
#define ASYNC_INVALIDATE true // async invalidate other cache lines
#define PRIORITY true

#ifdef USE_RW_LOCK
#include <shared_mutex>
#else
#include <mutex>
#endif

typedef std::atomic<int> ref_t;

class Object {};

struct DeallocTask {
    rdma_ptr<Object> local_ptr;
    int size;
    ref_t* ref_counter;

    DeallocTask() : local_ptr(nullptr), size(0), ref_counter(nullptr) {}
    DeallocTask(rdma_ptr<Object> ptr, int size, ref_t* counter) : local_ptr(ptr), size(size), ref_counter(counter) {}
};

inline ref_t* ref_generator(){
    ref_t* ref = new ref_t(0);
    return ref;
}

inline DeallocTask task_generator(){
    return DeallocTask();
}

thread_local static ObjectPool<ref_t*> reference_pool = ObjectPool<ref_t*>(std::function<ref_t*()>(ref_generator));
thread_local static ObjectPool<DeallocTask> dealloc_pool = ObjectPool<DeallocTask>(std::function<DeallocTask()>(task_generator));

// todo: fix memory issue (all of it is in rdma accessible memory when only the addresses need to be)? Better for cache?
// todo: maybe not aligning will be faster?
struct alignas(64) CacheLine {
    uint64_t address;
    #ifdef USE_RW_LOCK
    std::shared_mutex* mu;
    #else
    std::mutex* mu;
    #endif
    int priority;
    rdma_ptr<Object> local_ptr;
    int size;
    std::atomic<int>* ref_counter;
};

static_assert(offsetof(CacheLine, address) == 0);

static CacheMetrics history[10];
static int size = 0;

template <typename Pool = rdma_capability_thread>
class RemoteCacheImpl {
private:
    rdma_ptr<CacheLine> origin_address;
    vector<rdma_ptr<CacheLine>> remote_caches;
    CacheLine* lines;
    int number_of_lines;

    std::mutex init_lock;
    vector<rdma_ptr<uint64_t>> prealloc_cas_result;
    uint16_t self_id;
    int op_count;
    int check_freq;

    template <typename T>
    uint64_t hash(rdma_ptr<T> ptr){
        uint64_t ids_n = remote_caches.size() + 1;
        uint64_t offset = ((double) number_of_lines / ids_n) * ptr.id();
        uint64_t hashed = ptr.address() / 64;

        // mix13
        hashed ^= (hashed >> 33);
        hashed *= 0xff51afd7ed558ccd;
        hashed ^= (hashed >> 33);
        hashed *= 0xc4ceb9fe1a85ec53;
        hashed ^= (hashed >> 33);

        // we know information about the addresses, can we get closer to ideal
        return (hashed + offset) % number_of_lines;
    }

    // Attempt to free some elements
    void try_free_some(){
        while(!dealloc_pool.empty()){
            DeallocTask t = dealloc_pool.fetch();
            if (t.ref_counter->load() != 0) {
                dealloc_pool.release(t); // add it back for later
                return;
            }
            pool->template Deallocate<Object>(t.local_ptr, t.size);
            reference_pool.release(t.ref_counter);
        }
    }

    // Handle a local_ptr no longer as an item in the cache
    void handle_free(rdma_ptr<Object> ptr, int size, ref_t* reference_counter){
        reference_counter->fetch_sub(1);
        // Check if reference counter is 0
        if(reference_counter->load() == 0){
            // Then deallocate immediately
            if (ptr != nullptr)
                pool->template Deallocate<Object>(ptr, size);
            reference_pool.release(reference_counter);
        } else {
            // Send it to the pool to release if a real deallocation
            if (reference_counter != nullptr && ptr != nullptr)
                dealloc_pool.release(DeallocTask(ptr, size, reference_counter));
            else if (reference_counter != nullptr)
                reference_pool.release(reference_counter);
        }
    }

    int calculate_bytes(){
        int count = 0;
        for(int i = 0; i < number_of_lines; i++){
            if (lines[i].address != 0) {
                count += lines[i].size;
            }
        }
        return count;
    }

    template <class T>
    void invalidate(CacheLine* l, rdma_ptr<T> ptr){
        // Invalidate locally
        #ifdef EXPERIMENTAL
        l->mu->lock_shared();
        #else
        l->mu->lock();
        #endif
        if ((l->address & ~mask) == ptr.raw()){
            // todo?
            l->address = l->address | mask;
        }
        #ifdef EXPERIMENTAL
        l->mu->unlock_shared();
        #else
        l->mu->unlock();
        #endif

        // Invalidate the other caches
        uint64_t ids[remote_caches.size()];
        for(int i = 0; i < remote_caches.size(); i++){
            // CAS the remote cache's address to have the mask
            rdma_ptr<uint64_t> cache_line = static_cast<rdma_ptr<uint64_t>>(remote_caches[i][hash(ptr)]);
            #ifdef ASYNC_INVALIDATE
            // batched compare and swap
            rdma_ptr<uint64_t> cas_result = prealloc_cas_result[i];
            pool->template CompareAndSwapAsync(cache_line, cas_result, ptr.raw(), ptr.raw() | mask);
            ids[i] = cache_line.id();
            #else
            // sequential compare and swap
            uint64_t old_value = pool->template CompareAndSwap<uint64_t>(cache_line, ptr.raw(), ptr.raw() | mask);
            if (old_value == ptr.raw()) metrics.successful_invalidations++;
            #endif
            metrics.remote_cas++;
        }
        #ifdef ASYNC_INVALIDATE
        int count = remote_caches.size();
        for(int i = 0; i < remote_caches.size(); i++){
            pool->template Await(ids[i], count - i - 1);
            if (*prealloc_cas_result.at(i) == ptr.raw()) metrics.successful_invalidations++;
        }
        #endif
    }
public:
    /// Metrics for the thread across all caches
    thread_local static CacheMetrics metrics;
    thread_local static Pool* pool;
    thread_local static bool is_leader;

    /// Construct a remote cache object for RDMA
    /// - initializer: The pool to initialize with 
    /// - self_id: The id of the node the cache is running on. 
    /// - number_of_lines: The initial number of lines in the cache. This can change dynamically
    /// - check_freq: How often the master thread should check it's metrics to determine if a dynamic change is warranted
    RemoteCacheImpl(Pool* intializer, uint16_t self_id, int number_of_lines = 2000, int freq = 1000) : self_id(self_id), check_freq(freq) {
        static_assert(sizeof(Object) == 1, "Precondition");
        this->number_of_lines = number_of_lines;
        origin_address = intializer->template Allocate<CacheLine>(number_of_lines);
        REMUS_INFO("CacheLine start: {}, CacheLine end: {}", origin_address, origin_address + number_of_lines);
        lines = (CacheLine*) origin_address.address();
        for(int i = 0; i < number_of_lines; i++){
            lines[i].address = 0;
            lines[i].priority = INT_MAX; // want to always replace this empty line
            lines[i].local_ptr = nullptr;
            lines[i].ref_counter = reference_pool.fetch();
            lines[i].ref_counter->store(0);
            #ifdef USE_RW_LOCK
            lines[i].mu = new std::shared_mutex();
            #else
            lines[i].mu = new std::mutex();
            #endif
        }
        op_count = 0;
        reset_metrics();
    }

    /// This thread becomes the master thread 
    /// - responsible for dynamically changing the cache size
    /// - and for mutating the hash function
    /// - must be a part of the workload
    void claim_master(){
        REMUS_INFO("I claimed master!!!");
        is_leader = true;
    }

    /// Ideally, the remote cache is deconstructed in a higher scope so that no pending reference counters still refer to any elements
    ~RemoteCacheImpl(){
        for(int i = 0; i < number_of_lines; i++){
            // Deallocate forcefully, even if we have references to the object
            if (lines[i].ref_counter != nullptr){
                int c = lines[i].ref_counter->load();
                REMUS_ASSERT(c <= 1, "RemoteCache deconstructor called before CachedObjects left scope {}", c);
            }
            if (lines[i].local_ptr != nullptr)
                pool->template Deallocate<Object>(lines[i].local_ptr, lines[i].size);
            delete lines[i].ref_counter; // delete the ptr to the atomic int
        }
        pool->Deallocate(origin_address, number_of_lines);

        for(int i = 0; i < prealloc_cas_result.size(); i++){
            pool->template Deallocate<uint64_t>(prealloc_cas_result.at(i), 8);
        }
    }

    /// Get the root of the constructed cache
    uint64_t root(){
        return origin_address.raw();
    }

    /// Initialize the cache with the roots of the other caches
    void init(vector<uint64_t> peer_roots, int expected_length){
        init_lock.lock();
        for(int i = 0; i < peer_roots.size(); i++){
            rdma_ptr<CacheLine> p = rdma_ptr<CacheLine>(peer_roots[i]);
            // don't mess with local cache
            if (p.raw() == root()) continue;
            if (pool->is_local(p)) continue;

            // avoid duplicates
            bool is_dupl = false;
            for(int i = 0; i < remote_caches.size(); i++){
                if (remote_caches[i] == p) is_dupl = true;
            }
            if (!is_dupl){
                remote_caches.push_back(p);
                prealloc_cas_result.push_back(pool->template Allocate<uint64_t>());
            }
        }
        init_lock.unlock();
        // +1 to include themselves
        REMUS_INFO("Number of peers in CacheClique {}", remote_caches.size() + 1);
        if (remote_caches.size() != expected_length){
            REMUS_ERROR("Incorrect # of remote caches");
            abort();
        }
    }

    /// Deallocate all limbo lists
    /// This must be called on every thread that uses the RemoteCache to free it's thread_local data
    /// Will do so forcefully, so no CachedObjects can be in scope (will terminate in an effort to prevent use-after-free bugs)
    void free_all_tmp_objects(){
        while(!dealloc_pool.empty()){
            DeallocTask t = dealloc_pool.fetch();
            REMUS_ASSERT(t.ref_counter->load() <= 1, "free_all_tmp_objects called before CachedObjects left scope {}", t.ref_counter->load());
            if (t.local_ptr == nullptr) continue;
            pool->template Deallocate<Object>(t.local_ptr, t.size);
            delete t.ref_counter;
        }
    }

    /// Not thread safe. Use aside from operations
    int count_empty_lines(){
        int count = 0;
        for(int i = 0; i < number_of_lines; i++){
            if (lines[i].address == 0) count++;
        }
        metrics.empty_lines = count;
        return count;
    }

    /// Technically not thread safe b/c count empty lines...
    void print_metrics(std::string indication = ""){
        int empty_lines = count_empty_lines();
        int size_of_cache = calculate_bytes();
        REMUS_INFO("{}{}", indication, metrics.as_string());
        REMUS_INFO("Cache ({} lines) consumes {} KB", number_of_lines - empty_lines, (double) size_of_cache / 1000.0);
    }

    /// Resets the thread-local metrics
    void reset_metrics(){
        metrics = CacheMetrics();
    }

    /// Read data in. Lower priority is prioritized (root is 0 priority!)
    template <typename T>
    inline CachedObject<T> Read(rdma_ptr<T> ptr, rdma_ptr<T> prealloc = nullptr, int priority = 0){
        return ExtendedRead(ptr, 1, prealloc, priority);
    }

    template <typename T>
    CachedObject<T> ExtendedRead(rdma_ptr<T> ptr, int size, rdma_ptr<T> prealloc = nullptr, int priority = 0){
        REMUS_ASSERT_DEBUG(ptr != nullptr, "Cant read nullptr");
        // Periodically call try_free_some to cleanup limbo lists
        try_free_some();
    
        // todo: do i need to mark the cache line as volatile?
        // todo: implement priorities
        // todo: resetting the address is not coherent with remote CAS?
        rdma_ptr<T> result;
        ref_t* reference_counter;
        if (is_marked(ptr)){
            // Get cache line and lock
            ptr = unmark_ptr(ptr);
            CacheLine* l = &lines[hash(ptr)];
            #ifdef USE_RW_LOCK
            l->mu->lock_shared();
            bool acquired_rw_lock = false;
            #else
            l->mu->lock();
            #endif
            if ((l->address & ~mask) == ptr.raw()){
                if (l->address & mask){
                    #ifdef USE_RW_LOCK
                    l->mu->unlock_shared();
                    l->mu->lock();
                    acquired_rw_lock = true;
                    #endif
                    // -- Cache miss (coherence) -- //
                    // clear the invalid bit before reading. Linearizes the read
                    //      ensure any writes that happen before this are noticed in the read
                    //      ensure any writes that happen after this are recorded in the bit
                    l->address = l->address & ~mask;
                    atomic_thread_fence(std::memory_order_seq_cst);

                    // Read the new object into the local ptr
                    rdma_ptr<T> data = pool->template ExtendedRead<T>(ptr, size);
                    handle_free(l->local_ptr, l->size, l->ref_counter); // free the old data
                    l->local_ptr = static_cast<rdma_ptr<Object>>(data);
                    l->priority = priority;
                    REMUS_ASSERT(l->size == size * sizeof(T), "Sizes are equal when accessing objects");
                    l->ref_counter = reference_pool.fetch();
                    l->ref_counter->store(1);

                    // set result
                    result = static_cast<rdma_ptr<T>>(l->local_ptr); // set result while we are at it
                    reference_counter = l->ref_counter; // setup the reference counter

                    // Increment metrics
                    metrics.remote_reads++;
                    metrics.coherence_misses++;
                } else {
                    // -- Cache hit -- //
                    result = static_cast<rdma_ptr<T>>(l->local_ptr);
                    reference_counter = l->ref_counter;
                    REMUS_ASSERT(l->size == size * sizeof(T), "Size of read is equal to count");
                    metrics.hits++;
                }
            } else {
                #ifdef PRIORITY
                if (l->priority < priority){
                    // -- Cache miss (priority) -- //
                    // if old priority is less than new priority
                    metrics.priority_misses++;
                    #ifdef USE_RW_LOCK
                    l->mu->unlock_shared();
                    #else
                    l->mu->unlock();
                    #endif
                    goto unmarked_execution;
                }
                #endif
                #ifdef USE_RW_LOCK
                l->mu->unlock_shared();
                l->mu->lock();
                acquired_rw_lock = true;
                #endif
                // -- Cache miss (compulsory or conflict) -- //
                uint64_t old_address = l->address;
                // Overwrite the address 
                // todo: is it possible that this address change is not messed up?
                // l->address = ptr.raw();
                rdma_ptr<uint64_t> cache_line = rdma_ptr<uint64_t>(self_id, (uint64_t) l);
                pool->template AtomicSwap<uint64_t>(cache_line, ptr.raw(), l->address);
                atomic_thread_fence(std::memory_order_seq_cst);

                // Then read the data and update the cache line
                rdma_ptr<T> data = pool->template ExtendedRead<T>(ptr, size);
                handle_free(l->local_ptr, l->size, l->ref_counter); // free the old data
                l->local_ptr = static_cast<rdma_ptr<Object>>(data);
                l->size = size * sizeof(T);
                l->priority = priority;
                l->ref_counter = reference_pool.fetch();
                l->ref_counter->store(1);

                // Set result
                result = static_cast<rdma_ptr<T>>(l->local_ptr);
                reference_counter = l->ref_counter;

                // Increment metrics
                metrics.remote_reads++;
                if (old_address != 0)
                    metrics.conflict_misses++;
                else
                    metrics.cold_misses++;
            }
            reference_counter->fetch_add(1); // increment ref count before releasing cache line and causing other issues
            // Unlock mutex on cache line
            #ifdef USE_RW_LOCK
            if (acquired_rw_lock)
                l->mu->unlock();
            else
                l->mu->unlock_shared();
            #else
            l->mu->unlock();
            #endif
            // remark the ptr for the remote origin
            return CachedObject<T>(mark_ptr(ptr), result, reference_counter);
        }

        unmarked_execution:
        // -- No cache -- //
        // Setup the result
        result = pool->template ExtendedRead<T>(ptr, size, prealloc);

        // Increment metrics
        metrics.remote_reads++;
        metrics.allocation++;
        return CachedObject<T>(ptr, result, [=](){
            if (result != prealloc){ // don't accidentally deallocate prealloc
                pool->template Deallocate<T>(result, size);
            }
        });
    }

    template <typename T>
    void Write(rdma_ptr<T> ptr, const T& val, rdma_ptr<T> prealloc = nullptr, internal::RDMAWriteBehavior write_behavior = internal::RDMAWriteWithAck){
        if (is_marked(ptr)){
            // Get cache line and lock it
            ptr = unmark_ptr(ptr);
            CacheLine* l = &lines[hash(ptr)];

            // write to the value in the owner
            pool->Write(ptr, val, prealloc, write_behavior);
            metrics.remote_writes++;

            // Invalidate
            invalidate(l, ptr);
        } else {
            // write normally
            pool->Write(ptr, val, prealloc, write_behavior);
            metrics.remote_writes++;
        }
    }

    /// Semantics for invalidating an object
    /// Useful if we need multiple writes
    template <typename T>
    void Invalidate(rdma_ptr<T> ptr){
        if (!is_marked(ptr)) {
            return; // if the ptr is not marked, don't invalidate the object
        }
        // Get the cache line
        ptr = unmark_ptr(ptr);
        CacheLine* l = &lines[hash(ptr)];

        // Invalidate
        invalidate(l, ptr);
    }
};

typedef RemoteCacheImpl<> RemoteCache;
template<> inline thread_local CacheMetrics RemoteCache::metrics = CacheMetrics();
template<> inline thread_local rdma_capability_thread* RemoteCache::pool = nullptr;

template<class T> inline thread_local bool RemoteCacheImpl<T>::is_leader = false;