#pragma once

#include <atomic>
#include <cstdint>
#include <remus/logging/logging.h>
#include <remus/rdma/memory_pool.h>
#include <remus/rdma/rdma.h>
#include <remus/rdma/rdma_ptr.h>
#include <shared_mutex>
#include <vector>

#include "object_pool.h"
#include "cached_ptr.h"
#include "mark_ptr.h"
#include "metrics.h"

using namespace remus::rdma;

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
struct CacheLine {
    uint64_t address;
    // int priority;
    std::mutex* mu;
    rdma_ptr<Object> local_ptr;
    int size;
    std::atomic<int>* ref_counter;
};

template <typename Pool = rdma_capability_thread>
class RemoteCacheImpl {
private:
    rdma_ptr<CacheLine> origin_address;
    vector<rdma_ptr<CacheLine>> remote_caches;
    CacheLine* lines;
    int number_of_lines;

    template <typename T>
    uint64_t hash(rdma_ptr<T> ptr){
        // auto hashed = ptr.id() ^ ptr.address();
        // auto hashed = ptr.address() / 8;
        auto hashed = ptr.address() / 64;

        // mix13
        // hashed ^= (hashed >> 33);
        // hashed *= 0xff51afd7ed558ccd;
        // hashed ^= (hashed >> 33);
        // hashed *= 0xc4ceb9fe1a85ec53;
        // hashed ^= (hashed >> 33);
        // todo: maybe come up with a better hash function for addresses
        // todo: maybe self-tuning cache function?
        // we know information about the addresses, can we seed our hash function?
        return hashed;
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
public:
    /// Metrics for the thread across all caches
    thread_local static CacheMetrics metrics;
    thread_local static Pool* pool;

    RemoteCacheImpl(Pool* intializer, int number_of_lines = 2000) {
        REMUS_ASSERT(sizeof(Object) == 1, "Precondition");
        this->number_of_lines = number_of_lines;
        origin_address = intializer->template Allocate<CacheLine>(number_of_lines);
        REMUS_INFO("CacheLine start: {}, CacheLine end: {}", origin_address, origin_address + number_of_lines);
        lines = (CacheLine*) origin_address.address();
        for(int i = 0; i < number_of_lines; i++){
            lines[i].address = 0;
            // lines[i].priority = 0;
            lines[i].local_ptr = nullptr;
            lines[i].ref_counter = reference_pool.fetch();
            lines[i].ref_counter->store(0);
            lines[i].mu = new std::mutex();
        }
    }

    /// Ideally, the remote cache is deconstructed in a higher scope so that no pending reference counters still refer to any elements
    ~RemoteCacheImpl(){
        for(int i = 0; i < number_of_lines; i++){
            // Deallocate forcefully, even if we have references to the object
            if (lines[i].ref_counter != nullptr)
                REMUS_ASSERT(lines[i].ref_counter->load() == 0, "RemoteCache deconstructor called before CachedObjects left scope");
            if (lines[i].local_ptr != nullptr)
                pool->template Deallocate<Object>(lines[i].local_ptr, lines[i].size);
            delete lines[i].ref_counter; // delete the ptr to the atomic int
        }
        pool->Deallocate(origin_address, number_of_lines);
    }

    /// Get the root of the constructed cache
    uint64_t root(){
        return origin_address.raw();
    }

    /// Initialize the cache with the roots of the other caches
    void init(vector<uint64_t> peer_roots){
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
            if (!is_dupl)
                remote_caches.push_back(p);
        }
        // 1 to include themselves
        REMUS_INFO("Number of peers in CacheClique {}", remote_caches.size() + 1);
    }

    /// Deallocate all limbo lists
    /// This must be called on every thread that uses the RemoteCache to free it's thread_local data
    /// Will do so forcefully, so no CachedObjects can be in scope (will terminate in an effort to prevent use-after-free bugs)
    void free_all_tmp_objects(){
        while(!dealloc_pool.empty()){
            DeallocTask t = dealloc_pool.fetch();
            REMUS_ASSERT(t.ref_counter->load() == 0, "free_all_tmp_objects called before CachedObjects left scope");
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

    /// Technically not thread safe
    void print_metrics(){
        count_empty_lines();
        REMUS_INFO("{}", metrics.as_string());
    }

    /// Technically not thread safe
    void reset_metrics(){
        metrics = CacheMetrics();
    }

    template <typename T>
    inline CachedObject<T> Read(rdma_ptr<T> ptr){
        return ExtendedRead(ptr, 1);
    }

    template <typename T>
    CachedObject<T> ExtendedRead(rdma_ptr<T> ptr, int size){
        // Periodically call try_free_some to cleanup limbo lists
        try_free_some();
    
        // todo: do i need to mark the cache line as volatile?
        // todo: implement priorities
        // todo: resetting the address is not coherent with remote CAS?
        // todo: what happens if I extendedRead the same pointer with different size
        rdma_ptr<T> result;
        ref_t* reference_counter;
        if (is_marked(ptr)){
            // Get cache line and lock
            ptr = unmark_ptr(ptr);
            CacheLine* l = &lines[hash(ptr) % number_of_lines];
            l->mu->lock(); // todo: upgrade lock on conditional write?
            if ((l->address & ~mask) == ptr.raw()){
                if (l->address & mask){
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
                // -- Cache miss (compulsory or conflict) -- //
                // Overwrite the address
                l->address = ptr.raw();
                atomic_thread_fence(std::memory_order_seq_cst);

                // Then read the data and update the cache line
                rdma_ptr<T> data = pool->template ExtendedRead<T>(ptr, size);
                handle_free(l->local_ptr, l->size, l->ref_counter); // free the old data
                l->local_ptr = static_cast<rdma_ptr<Object>>(data);
                l->size = size * sizeof(T);
                l->ref_counter = reference_pool.fetch();
                l->ref_counter->store(1);

                // Set result
                result = static_cast<rdma_ptr<T>>(l->local_ptr);
                reference_counter = l->ref_counter;

                // Increment metrics
                metrics.remote_reads++;
                metrics.conflict_misses++;
            }
            // Unlock mutex on cache line
            l->mu->unlock();
            reference_counter->fetch_add(1);
            return CachedObject<T>(result, reference_counter);
        } else {
            // -- No cache -- //
            // Setup the result
            result = pool->template ExtendedRead<T>(ptr, size);

            // Increment metrics
            metrics.remote_reads++;
            metrics.allocation++;
            return CachedObject<T>(result, [=](){
                pool->template Deallocate<T>(result, size);
            });
        }
    }

    template <typename T>
    void Write(CachedObject<T>& ptr, const T& val, rdma_ptr<T> prealloc = nullptr, internal::RDMAWriteBehavior write_behavior = internal::RDMAWriteWithAck){
        return Write(ptr.get(), val, prealloc, write_behavior);
    }

    template <typename T>
    void Write(rdma_ptr<T> ptr, const T& val, rdma_ptr<T> prealloc = nullptr, internal::RDMAWriteBehavior write_behavior = internal::RDMAWriteWithAck){
        if (is_marked(ptr)){
            // Get cache line and lock it
            ptr = unmark_ptr(ptr);
            CacheLine* l = &lines[hash(ptr) % number_of_lines];

            // write to the value in the owner
            pool->Write(ptr, val, prealloc, write_behavior);
            metrics.remote_writes++;

            // Invalidate locally
            l->mu->lock();
            if ((l->address & ~mask) == ptr.raw()){
                l->address = l->address | mask;
            }
            l->mu->unlock();

            // Invalidate the other caches
            for(int i = 0; i < remote_caches.size(); i++){
                // CAS the remote cache's address to have the mask
                rdma_ptr<uint64_t> cache_line = static_cast<rdma_ptr<uint64_t>>(remote_caches[i][hash(ptr) % number_of_lines]);
                pool->template CompareAndSwap<uint64_t>(cache_line, ptr.raw(), ptr.raw() | mask);
                metrics.remote_cas++;
                // todo: batch compare and swap
            }

            // todo: downgrade rw lock to read lock after we make the local switch, thus allowing reads to pass through but blocking writes
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
        // Get the cache line
        ptr = unmark_ptr(ptr);
        CacheLine* l = &lines[hash(ptr) % number_of_lines];

        // Invalidate my own line
        l->mu->lock();
        if ((l->address & ~mask) == ptr.raw()){
            l->address = l->address | mask;
        }
        l->mu->unlock();

        // invalidate the other caches
        for(int i = 0; i < remote_caches.size(); i++){
            // CAS the remote cache's address to have the mask
            rdma_ptr<uint64_t> cache_line = static_cast<rdma_ptr<uint64_t>>(remote_caches[i][hash(ptr) % number_of_lines]);
            pool->template CompareAndSwap<uint64_t>(cache_line, ptr.raw(), ptr.raw() | mask);
            metrics.remote_cas++;
            // todo: batch compare and swap
        }
    }
};

typedef RemoteCacheImpl<> RemoteCache;
template<> inline thread_local CacheMetrics RemoteCache::metrics = CacheMetrics();
template<> inline thread_local rdma_capability_thread* RemoteCache::pool = nullptr;
