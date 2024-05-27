#include "../include/mark_ptr.h"
#include "../include/cache_store.h"
#include "faux_mempool.h"

// Set remote cache static variables
template<> inline thread_local CacheMetrics RemoteCacheImpl<CountingPool>::metrics = CacheMetrics();
template<> inline thread_local CountingPool* RemoteCacheImpl<CountingPool>::pool = nullptr;

#include <cstring>
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
    // -- Test 1 -- //
    rdma_ptr<Structure> p = pool->Allocate<Structure>(2);
    memset((Structure*) p.address(), 0, sizeof(Structure) * 2);
    rdma_ptr<Structure> marked_p = mark_ptr(p);
    CachedPtr p2 = cache->ExtendedRead(marked_p, 2); // put it in cache
    p2->x[0] = 1; // write to the cached version
    REMUS_INFO("Test 1 -- PASSED");

    // -- Test 2 -- //
    Structure s = *p2;
    cache->Write(marked_p, *p2); // write it back
    test(p->x[0] == 1, "Write didn't occur");
    CachedPtr p3 = cache->ExtendedRead(marked_p, 2); // read it again
    test(p3->x[0] == 1, "Cache didn't invalidate the write"); // check we observed the cached result
    test(p3->x[1] == 0, "Check second value of x for safety"); 
    REMUS_INFO("Test 3 -- PASSED");

    // -- Test 3 -- //
    p->x[1] = 1; // write to the object not through the cache
    CachedPtr p4 = cache->ExtendedRead(marked_p, 2); // read it again
    test(p4->x[1] == 0, "Before invalidate, cache result is not flipped");
    cache->Invalidate(marked_p); // invalidate the object
    CachedPtr p5 = cache->ExtendedRead(marked_p, 2); // read it again
    test(p5->x[0] == 1, "Write persisted");
    test(p5->x[1] == 1, "Invalidate allowed us to observe the correct result"); 
    test(p5->x[2] == 0, "Check third value of x for safety");
    REMUS_INFO("Test 3 -- PASSED");

    pool->Deallocate<Structure>(p, 2);
}

int main(){
    // Construct a capability
    CountingPool* pool = new CountingPool(false);

    // Construct the remote cache
    RemoteCacheImpl<CountingPool>* cache = new RemoteCacheImpl<CountingPool>(pool); // todo: same test with different pool
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