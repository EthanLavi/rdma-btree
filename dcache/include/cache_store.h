#include <cstdint>
#include <rome/rdma/rdma.h>
#include <rome/rdma/rdma_ptr.h>
#include <shared_mutex>

#include "mark_ptr.h"

using namespace rome::rdma;

class Object {};

struct CacheLine {
    uint64_t address;
    int version;
    int priority;
    std::shared_mutex* rwlock;
    rdma_ptr<Object> ptr;
};

class RemoteCache {
private:
    rdma_capability* capability;
    rdma_ptr<CacheLine> lines;
    int size;

    template <typename T>
    uint64_t hash(rdma_ptr<T> ptr){
        auto hashed = ptr.id() ^ ptr.address();
        // mix13
        hashed ^= (hashed >> 33);
        hashed *= 0xff51afd7ed558ccd;
        hashed ^= (hashed >> 33);
        hashed *= 0xc4ceb9fe1a85ec53;
        hashed ^= (hashed >> 33);
        // todo: maybe come up with a better hash function for addresses
        // we know information about the addresses, can we seed our hash function?
        return hashed;
    }
public:
    RemoteCache(rdma_capability* capability) : capability(capability) {
        size = 1000;
        lines = capability->Allocate<CacheLine>(size);
        for(int i = 0; i < size; i++){
            lines[i]->address = 0;
            lines[i]->version = 0;
            lines[i]->priority = 0;
            lines[i]->ptr = nullptr;
            lines[i]->rwlock = new std::shared_mutex();
        }
    }

    template <typename T>
    rdma_ptr<T> Read(rdma_ptr<T> ptr, rdma_ptr<T> prealloc = nullptr){
        // todo: do i need to mark the cache line as volatile?
        if (is_marked(ptr)){
            rdma_ptr<CacheLine> l = lines[hash(ptr) % size];
            rdma_ptr<T> result;
            l->rwlock->lock_shared();
            if (l->address == ptr.raw()){
                if (l->address & mask){
                    // Cache miss (coherence)
                    capability->Read(ptr, (rdma_ptr<T>) l->address);
                } else {
                    // Cache hit
                    result = (rdma_ptr<T>) l->address;
                }
            } else {
                // Cache miss (Compulsory/Conflict)
            }
            l->rwlock->unlock_shared();
            return result;
        } else {
            return capability->Read(ptr, prealloc);
        }
    }
};