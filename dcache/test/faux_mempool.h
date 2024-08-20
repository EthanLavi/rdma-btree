#pragma once
#include <cstdint>
#include <cstdlib>

#include <cstring>
#include <remus/logging/logging.h>
#include <remus/rdma/rdma.h>
#include <shared_mutex>

using namespace remus::rdma;
using namespace std;

class CountingPool {
    struct AsyncJob {
        rdma_ptr<uint64_t> org;
        uint64_t new_val;
        AsyncJob(rdma_ptr<uint64_t> org, uint64_t new_val) : org(org), new_val(new_val){}
    };
    unordered_map<void*, int> allocat; // ptr to size
    unordered_map<void*, void*> ptr_map; // aligned ptr to original ptr
    unordered_map<int, std::vector<AsyncJob>*> async_jobs;
    int total_allocations;
    int total_deallocations;
    bool locality;
    shared_mutex mu;
    mutex alloc_mu;

public:
    CountingPool(bool all_local) : locality(all_local), total_allocations(0), total_deallocations(0) {}

    /// Returns a value that accounts for alignment of the type (for parity with slab allocator)
    template <typename T>
    rdma_ptr<T> Allocate(int size = 1){
        alloc_mu.lock();
        total_allocations++;
        int bytes = sizeof(T) * size;
        void* org_ptr = malloc(bytes * 2);
        T* p = (T*) (((long) org_ptr + alignof(T)) & (~(alignof(T) - 1)));
        allocat[(void*) p] = bytes;
        ptr_map[(void*) p] = org_ptr;
        alloc_mu.unlock();
        return rdma_ptr<T>(0, p);
    }

    template <typename T>
    void Deallocate(rdma_ptr<T> p, int size = 1){
        alloc_mu.lock();
        total_deallocations++;
        REMUS_ASSERT(p != nullptr, "Deallocating a nullptr");
        int bytes = sizeof(T) * size;
        void* add = (void*) p.address();
        REMUS_ASSERT(allocat.find(add) != allocat.end(), "Found double free with ptr {}", p);
        REMUS_ASSERT(allocat.at(add) == bytes, "Found free with ptr {} with wrong size (actual={} != freed={}) {}/{}", p, allocat.at(add), bytes, sizeof(T), size);
        allocat.erase(add);
        free(ptr_map[add]);
        alloc_mu.unlock();
    }

    template <typename T>
    rdma_ptr<T> Read(rdma_ptr<T> p, rdma_ptr<T> prealloc = nullptr){
        return ExtendedRead(p, 1, prealloc);
    }

    template <typename T>
    rdma_ptr<T> ExtendedRead(rdma_ptr<T> p, int size, rdma_ptr<T> prealloc = nullptr){
        if (p == nullptr) {
            REMUS_ERROR("p is a nullptr");
            abort();
        }
        if (prealloc == p){
            REMUS_ERROR("prealloc == p (read)");
            abort();
        }
        if (prealloc == nullptr){
            rdma_ptr<T> p_new = Allocate<T>(size);
            mu.lock_shared();
            memcpy(p_new.get(), p.get(), sizeof(T) * size);
            mu.unlock_shared();
            return p_new;
        } else {
            // read into prealloc instead of internally allocating
            mu.lock_shared();
            memcpy(prealloc.get(), p.get(), sizeof(T) * size);
            mu.unlock_shared();
            return prealloc;
        }        
    }

    template <typename T> T AtomicSwap(rdma_ptr<T> ptr, uint64_t swap, uint64_t hint = 0) {
        mu.lock();
        T old = *ptr;
        *ptr = swap;
        mu.unlock();
        return old;
    }


    template <typename T>
    void Write(rdma_ptr<T> ptr, const T& val, rdma_ptr<T> prealloc = nullptr, internal::RDMAWriteBehavior write_behavior = internal::RDMAWriteWithAck){
        if (prealloc == ptr){
            REMUS_ERROR("prealloc == p (write)");
            abort();
        }
        mu.lock();
        if (prealloc == nullptr){
            // removed the un-necessary allocate and deallocate step...
            *ptr = val;
        } else {
            // might rely on property that prealloc is written val before it gets written to the "remote" ptr
            *prealloc = val;
            *ptr = *prealloc;
        }
        mu.unlock();
    }

    template <typename T>
    T CompareAndSwap(rdma_ptr<T> ptr, uint64_t expected, uint64_t swap) {
        mu.lock();
        uint64_t prev = *ptr;
        if (prev == expected){
            *ptr = swap;
        }
        mu.unlock();
        return prev;
    }

    void CompareAndSwapAsync(rdma_ptr<uint64_t> ptr, rdma_ptr<uint64_t> result, uint64_t expected, uint64_t swap){
        mu.lock();
        if (async_jobs.find(ptr.id()) == async_jobs.end()){
            async_jobs[ptr.id()] = new std::vector<AsyncJob>();
        }
        uint64_t prev = *ptr;
        async_jobs[ptr.id()]->push_back(AsyncJob(result, prev));
        if (prev == expected){
            *ptr = swap;
        }
        mu.unlock();
    }

    void Await(uint64_t id){
        mu.lock();
        // update the ptrs for sake of catching bugs if I try to use prealloc result immediately
        if (async_jobs.find(id) != async_jobs.end()){
            AsyncJob job = async_jobs[id]->back();
            *job.org = job.new_val;
            async_jobs[id]->pop_back();
        }        
        mu.unlock();
    }

    template <typename T>
    bool is_local(rdma_ptr<T> p){
        return locality;
    }

    bool HasNoLeaks(){
        alloc_mu.lock();
        int size = allocat.size();
        alloc_mu.unlock();
        return size == 0;
    }

    void debug(){
        REMUS_WARN("Total allocations {}", total_allocations);
        REMUS_WARN("Total deallocations {}", total_deallocations);
        for(auto it = allocat.begin(); it != allocat.end(); it++){
            REMUS_WARN("{} was not freed", it->first);
        }
    }
};