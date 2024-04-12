#pragma once

#include <atomic>
#include <cstdint>
#include <remus/rdma/memory_pool.h>
#include <remus/rdma/rdma.h>
#include <remus/rdma/rdma_ptr.h>
#include <shared_mutex>

#include "mark_ptr.h"
#include "cached_ptr.h"
#include "metrics.h"

using namespace remus::rdma;

class Object {};

// todo: fix memory issue (all of it is in rdma accessible memory when only the addresses need to be)
struct CacheLine {
    uint64_t address;
    int priority;
    std::shared_mutex* rwlock;
    rdma_ptr<Object> local_ptr;
};

struct CacheDescriptor {
    int peer;
    uint64_t ptr;
};

class RemoteCache {
private:
    shared_ptr<rdma_capability> pool; // todo: static?
    rdma_ptr<CacheLine> origin_address;
    vector<rdma_ptr<CacheLine>> remote_caches;
    CacheLine* lines;
    int size;

    template <typename T>
    uint64_t hash(rdma_ptr<T> ptr){
        // auto hashed = ptr.id() ^ ptr.address();
        auto hashed = ptr.address() / 8;
        
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

    RemoteCache(shared_ptr<rdma_capability> pool) : pool(pool) {
        size = 2000;
        origin_address = pool->Allocate<CacheLine>(size);
        REMUS_INFO("CacheLine start: {}, CacheLine end: {}", origin_address, origin_address + size);
        lines = (CacheLine*) origin_address.address();
        for(int i = 0; i < size; i++){
            lines[i].address = 0;
            lines[i].priority = 0;
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
            remote_caches.push_back(p);
        }
    }

    ~RemoteCache(){
        pool->Deallocate(origin_address, size);
    }

    /// Get the root of the constructed cache
    uint64_t root(){
        return origin_address.raw();
    }

    void print_metrics(){
        std::cout << metrics << std::endl;
    }

    void reset_metrics(){
        metrics = CacheMetrics();
    }

    template <typename T>
    CachedObject<T> Read(rdma_ptr<T> ptr, rdma_ptr<T> prealloc = nullptr){
        // todo: do i need to mark the cache line as volatile?
        // todo: implement priorities
        // todo: resetting the address is not coherent with remote CAS?
        rdma_ptr<T> result;
        bool temp;
        if (is_marked(ptr)){
            ptr = unmark_ptr(ptr);
            temp = false;
            CacheLine* l = &lines[hash(ptr) % size];
            l->rwlock->lock_shared();
            if ((l->address & ~mask) == ptr.address()){
                if (l->address & mask){
                    // Cache miss (coherence)
                    rdma_ptr<T> old_ptr = static_cast<rdma_ptr<T>>(lines->local_ptr);
                    // clear the invalid bit before reading 
                    //      ensure any writes that happen before this are noticed in the read
                    //      ensure any writes that happen after this are recorded in the bit
                    l->address = l->address & ~mask; 
                    atomic_thread_fence(std::memory_order_seq_cst);
                    l->local_ptr = static_cast<rdma_ptr<Object>>(pool->Read(ptr));
                    result = static_cast<rdma_ptr<T>>(l->local_ptr);
                    metrics.remote_reads++;
                    metrics.coherence_misses++;
                    // capability->Deallocate(old_ptr); // todo: EBR
                } else {
                    // Cache hit
                    result = static_cast<rdma_ptr<T>>(l->local_ptr);
                    metrics.hits++;
                }
            } else {
                // Cache miss (compulsory or conflict)
                l->address = ptr.raw();
                rdma_ptr<T> old_ptr = static_cast<rdma_ptr<T>>(l->local_ptr);
                l->local_ptr = static_cast<rdma_ptr<Object>>(pool->Read(ptr));
                result = static_cast<rdma_ptr<T>>(l->local_ptr);
                metrics.remote_reads++;
                metrics.conflict_misses++;
                // capability->Deallocate(old_ptr); // todo: EBR
            }
            l->rwlock->unlock_shared();
        } else {
            temp = true;
            result = pool->Read(ptr, prealloc);
            metrics.remote_reads++;
            metrics.allocation++;
        }
        return CachedObject(pool, result, temp);
    }

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
            if ((l->address & ~mask) == ptr.address()){
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
};

inline thread_local CacheMetrics RemoteCache::metrics = CacheMetrics();