#pragma once

#include <atomic>
#include <cstdint>
#include <remus/logging/logging.h>
#include <remus/rdma/memory_pool.h>
#include <remus/rdma/rdma.h>
#include <remus/rdma/rdma_ptr.h>
#include <shared_mutex>
#include <vector>

#include "cached_ptr.h"
#include "mark_ptr.h"
#include "metrics.h"

using namespace remus::rdma;

class Object {};

// todo: fix memory issue (all of it is in rdma accessible memory when only the addresses need to be)? Better for cache?
struct CacheLine {
    uint64_t address;
    // int priority;
    std::shared_mutex* rwlock;
    int size;
    rdma_ptr<Object> local_ptr;
};

class RemoteCache {
private:
    rdma_ptr<CacheLine> origin_address;
    vector<rdma_ptr<CacheLine>> remote_caches;
    CacheLine* lines;
    int size;

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
public:
    /// Metrics for the thread across all caches
    thread_local static CacheMetrics metrics;
    thread_local static rdma_capability* pool;

    RemoteCache(rdma_capability* intializer, int size = 2000) {
        this->size = size;
        origin_address = intializer->Allocate<CacheLine>(size);
        REMUS_INFO("CacheLine start: {}, CacheLine end: {}", origin_address, origin_address + size);
        lines = (CacheLine*) origin_address.address();
        for(int i = 0; i < size; i++){
            lines[i].address = 0;
            // lines[i].priority = 0;
            lines[i].local_ptr = nullptr;
            lines[i].rwlock = new std::shared_mutex();
        }
    }

    /// Initialize the cache with the roots of the other caches
    void init(vector<uint64_t> peer_roots){
        for(int i = 0; i < peer_roots.size(); i++){
            rdma_ptr<CacheLine> p = rdma_ptr<CacheLine>(peer_roots[i]);
            // don't mess with local cache
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

    ~RemoteCache(){
        pool->Deallocate(origin_address, size);
    }

    /// Get the root of the constructed cache
    uint64_t root(){
        return origin_address.raw();
    }

    /// Not thread safe. Use aside from operations
    int count_empty_lines(){
        int count = 0;
        for(int i = 0; i < size; i++){
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
    inline CachedObject<T> Read(rdma_ptr<T> ptr, rdma_ptr<T> prealloc = nullptr){
        return ExtendedRead(ptr, 1, prealloc);
    }

    template <typename T>
    CachedObject<T> ExtendedRead(rdma_ptr<T> ptr, int count, rdma_ptr<T> prealloc = nullptr){
        // todo: do i need to mark the cache line as volatile?
        // todo: implement priorities
        // todo: resetting the address is not coherent with remote CAS?
        // todo: what happens if I extendedRead the same pointer with different size
        rdma_ptr<T> result;
        bool temp;
        if (is_marked(ptr)){
            ptr = unmark_ptr(ptr);
            temp = false;
            CacheLine* l = &lines[hash(ptr) % size];
            l->rwlock->lock(); // todo: upgrade lock on conditional write?
            if ((l->address & ~mask) == ptr.raw()){
                if (l->address & mask){
                    // Cache miss (coherence)
                    rdma_ptr<T> old_ptr = static_cast<rdma_ptr<T>>(lines->local_ptr);
                    // clear the invalid bit before reading 
                    //      ensure any writes that happen before this are noticed in the read
                    //      ensure any writes that happen after this are recorded in the bit
                    l->address = l->address & ~mask; 
                    atomic_thread_fence(std::memory_order_seq_cst);
                    l->local_ptr = static_cast<rdma_ptr<Object>>(pool->ExtendedRead(ptr, count));
                    l->size = count;
                    result = static_cast<rdma_ptr<T>>(l->local_ptr);
                    metrics.remote_reads++;
                    metrics.coherence_misses++;
                    // capability->Deallocate(old_ptr); // todo: EBR
                } else {
                    // Cache hit
                    result = static_cast<rdma_ptr<T>>(l->local_ptr);
                    metrics.hits++;
                    REMUS_ASSERT_DEBUG(l->size == count, "Size of read is equal to count");
                }
            } else {
                // Cache miss (compulsory or conflict)
                l->address = ptr.raw();
                rdma_ptr<T> old_ptr = static_cast<rdma_ptr<T>>(l->local_ptr);
                l->local_ptr = static_cast<rdma_ptr<Object>>(pool->ExtendedRead(ptr, count));
                l->size = count;
                result = static_cast<rdma_ptr<T>>(l->local_ptr);
                metrics.remote_reads++;
                metrics.conflict_misses++;
                // capability->Deallocate(old_ptr); // todo: EBR
            }
            l->rwlock->unlock();
        } else {
            temp = true;
            result = pool->ExtendedRead(ptr, count, prealloc);
            metrics.remote_reads++;
            metrics.allocation++;
        }
        return CachedObject(pool, result, count, temp);
    }

    // todo: what if partial write to location is at head (API)
    // INVALIDATE AFTER MULTIPLE WRITES

    template <typename T>
    void Write(CachedObject<T>& ptr, const T& val, rdma_ptr<T> prealloc = nullptr, internal::RDMAWriteBehavior write_behavior = internal::RDMAWriteWithAck){
        return Write(ptr.get(), val, prealloc, write_behavior);
    }

    template <typename T>
    void Write(rdma_ptr<T> ptr, const T& val, rdma_ptr<T> prealloc = nullptr, internal::RDMAWriteBehavior write_behavior = internal::RDMAWriteWithAck){
        if (is_marked(ptr)){
            ptr = unmark_ptr(ptr);
            CacheLine* l = &lines[hash(ptr) % size];

            l->rwlock->lock();
            if ((l->address & ~mask) == ptr.raw()){
                // update my own cache if it is in there
                rdma_ptr<T> new_obj = pool->Allocate<T>();
                metrics.allocation++;
                *new_obj = val;
                rdma_ptr<T> old_ptr = static_cast<rdma_ptr<T>>(l->local_ptr);
                l->local_ptr = static_cast<rdma_ptr<Object>>(new_obj);
                // capability->Deallocate(old_ptr); // todo: EBR
            }
            
            // write to the value
            // todo: check if ptr is local
            pool->Write(ptr, val, prealloc, write_behavior);
            metrics.remote_writes++;

            // invalidate the other caches
            for(int i = 0; i < remote_caches.size(); i++){
                // CAS the remote cache's address to have the mask
                rdma_ptr<uint64_t> cache_line = static_cast<rdma_ptr<uint64_t>>(remote_caches[i][hash(ptr) % size]);
                pool->CompareAndSwap<uint64_t>(cache_line, ptr.raw(), ptr.raw() | mask);
                metrics.remote_cas++;
                // todo: batch compare and swap
            }
            l->rwlock->unlock();
            // todo: downgrade rw lock to read lock after we make the local switch, thus allowing reads to pass through but blocking writes
        } else {
            pool->Write(ptr, val, prealloc, write_behavior);
            metrics.remote_writes++;
        }
    }

    template <typename T>
    void Invalidate(rdma_ptr<T> ptr){
        CacheLine* l = &lines[hash(ptr) % size];
        l->rwlock->lock();
        // Invalidate my own line
        //if ((l->address & ~mask) == ptr.raw()){
            l->address = l->address | mask;
        //}
        l->rwlock->unlock();
        // invalidate the other caches
        for(int i = 0; i < remote_caches.size(); i++){
            // CAS the remote cache's address to have the mask
            rdma_ptr<uint64_t> cache_line = static_cast<rdma_ptr<uint64_t>>(remote_caches[i][hash(ptr) % size]);
            pool->CompareAndSwap<uint64_t>(cache_line, ptr.raw(), ptr.raw() | mask);
            metrics.remote_cas++;
            // todo: batch compare and swap
        }
    }
};

inline thread_local CacheMetrics RemoteCache::metrics = CacheMetrics();
inline thread_local rdma_capability* RemoteCache::pool = nullptr;