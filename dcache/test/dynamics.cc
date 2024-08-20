#include <dcache/mark_ptr.h>
#include <dcache/cache_store.h>

#include "faux_mempool.h"

// Set remote cache static variables
template<> inline thread_local CacheMetrics RemoteCacheImpl<CountingPool>::metrics = CacheMetrics();
template<> inline thread_local CountingPool* RemoteCacheImpl<CountingPool>::pool = nullptr;

#include <remus/logging/logging.h>
#include <remus/rdma/rdma.h>
#include <remus/rdma/rdma_ptr.h>

struct alignas (64) Structure {
    int x[8];
    int y[8];
};

typedef CachedObject<Structure> CachedPtr;

#define test(condition, message){ \
    if (!(condition)){ \
        REMUS_ERROR("Error: {}", message); \
        exit(1); \
    } \
}

void main_body(CountingPool* pool, RemoteCacheImpl<CountingPool>* cache){
    
}

int main(){
    // Construct a capability
    CountingPool* pool = new CountingPool(false);

    // Construct the remote cache
    RemoteCacheImpl<CountingPool>* cache = new RemoteCacheImpl<CountingPool>(pool, 0, 500);
    RemoteCacheImpl<CountingPool>::pool = pool; // set pool to other pool so we acccept our own cacheline
    cache->init({cache->root()}); // initialize with itself

    main_body(pool, cache);

    // Free memory
    cache->free_all_tmp_objects();
    delete cache;

    // Check for no leaked memory
    if (pool->HasNoLeaks()){
        REMUS_INFO("No Leaks In Cache Store");
    } else {
        REMUS_ERROR("Found Leaks in Cache Store");
        pool->debug();
        return 1;
    }

    // todo: get more code coverage (maybe cache swaps? idk)
    return 0;
}