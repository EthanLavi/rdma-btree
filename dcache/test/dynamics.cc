#include <dcache/mark_ptr.h>
#include <dcache/cache_store.h>

#include "faux_mempool.h"

// Set remote cache static variables
template<> inline thread_local CacheMetrics RemoteCacheImpl<CountingPool>::metrics = CacheMetrics();
template<> inline thread_local CountingPool* RemoteCacheImpl<CountingPool>::pool = nullptr;

#include <remus/logging/logging.h>
#include <remus/rdma/rdma.h>
#include <remus/rdma/rdma_ptr.h>

struct alignas (64) Structure64 {
    int x[8];
    int y[8];
};

struct alignas (128) Structure128 {
    int x[8];
    int y[8];
};

struct alignas (512) Structure512 {
    int x[8];
    int y[8];
};

struct alignas (2048) Structure2048 {
    int x[8];
    int y[8];
};

typedef CachedObject<Structure64> CachedPtr64;
typedef CachedObject<Structure128> CachedPtr128;
typedef CachedObject<Structure512> CachedPtr512;
typedef CachedObject<Structure2048> CachedPtr2048;


#define test(condition, message){ \
    if (!(condition)){ \
        REMUS_ERROR("Error: {}", message); \
        exit(1); \
    } \
}

void main_body1(CountingPool* pool, RemoteCacheImpl<CountingPool>* cache){
    REMUS_INFO("// ---- Test 1 ---- //");
    rdma_ptr<Structure64> s[256];
    
    for(int i = 0; i < 256; i++){
        s[i] = pool->Allocate<Structure64>();
    }

    // 32 rounds of the working set
    for(int round = 0; round < 32; round++){
        for(int i = 0; i < 256; i++){
            CachedPtr64 p = cache->Read<Structure64>(mark_ptr(s[i]), nullptr, 0);
        }
    }

    for(int i = 0; i < 256; i++){
        pool->Deallocate<Structure64>(s[i]);
    }
}

// 2048
void main_body2(CountingPool* pool, RemoteCacheImpl<CountingPool>* cache){
    REMUS_INFO("// ---- Test 2 ---- //");
    rdma_ptr<Structure2048> s[32];
    
    for(int i = 0; i < 32; i++){
        s[i] = pool->Allocate<Structure2048>();
    }

    // 16 rounds of the working set
    for(int round = 0; round < 16; round++){
        for(int i = 0; i < 32; i++){
            CachedPtr2048 p = cache->Read<Structure2048>(mark_ptr(s[i]), nullptr, 0);
        }
    }

    for(int i = 0; i < 32; i++){
        pool->Deallocate<Structure2048>(s[i]);
    }
}

// 128
void main_body3(CountingPool* pool, RemoteCacheImpl<CountingPool>* cache){
    REMUS_INFO("// ---- Test 3 ---- //");
    rdma_ptr<Structure128> s[32];
    
    for(int i = 0; i < 32; i++){
        s[i] = pool->Allocate<Structure128>();
    }

    // 16 rounds of the working set
    for(int round = 0; round < 16; round++){
        for(int i = 0; i < 32; i++){
            CachedPtr128 p = cache->Read<Structure128>(mark_ptr(s[i]), nullptr, 0);
        }
    }

    for(int i = 0; i < 32; i++){
        pool->Deallocate<Structure128>(s[i]);
    }
}

// 512
void main_body4(CountingPool* pool, RemoteCacheImpl<CountingPool>* cache){
    REMUS_INFO("// ---- Test 4 ---- //");
    rdma_ptr<Structure512> s[32];
    
    for(int i = 0; i < 32; i++){
        s[i] = pool->Allocate<Structure512>();
    }

    // 16 rounds of the working set
    for(int round = 0; round < 16; round++){
        for(int i = 0; i < 32; i++){
            CachedPtr512 p = cache->Read<Structure512>(mark_ptr(s[i]), nullptr, 0);
        }
    }

    for(int i = 0; i < 32; i++){
        pool->Deallocate<Structure512>(s[i]);
    }
}

void main_body5(CountingPool* pool, RemoteCacheImpl<CountingPool>* cache){
    REMUS_INFO("// ---- Test 5 ---- //");
    rdma_ptr<Structure64> s[256];
    
    for(int i = 0; i < 256; i++){
        s[i] = pool->Allocate<Structure64>();
    }

    // 32 rounds of the working set
    for(int round = 0; round < 32; round++){
        for(int i = 0; i < 256; i++){
            CachedPtr64 p = cache->Read<Structure64>(mark_ptr(s[i]), nullptr, i);
        }
    }

    for(int i = 0; i < 256; i++){
        pool->Deallocate<Structure64>(s[i]);
    }
}

int main(){
    // Construct a capability
    CountingPool* pool = new CountingPool(false);
    RemoteCacheImpl<CountingPool>::pool = pool;

    // Construct the remote cache
    RemoteCacheImpl<CountingPool>* cache = new RemoteCacheImpl<CountingPool>(pool, 0, 32, 256);
    cache->claim_master();
    cache->init({cache->root()}, 0);
    main_body1(pool, cache);
    cache->free_all_tmp_objects();
    delete cache;

    // Construct a new remote cache
    cache = new RemoteCacheImpl<CountingPool>(pool, 0, 512, 256);
    cache->claim_master();
    cache->init({cache->root()}, 0);
    main_body2(pool, cache);
    cache->free_all_tmp_objects();
    delete cache;

    // Construct a new remote cache
    cache = new RemoteCacheImpl<CountingPool>(pool, 0, 64, 256);
    cache->claim_master();
    cache->init({cache->root()}, 0);
    main_body3(pool, cache);
    cache->free_all_tmp_objects();
    delete cache;

    // Construct a new remote cache
    cache = new RemoteCacheImpl<CountingPool>(pool, 0, 256, 256);
    cache->claim_master();
    cache->init({cache->root()}, 0);
    main_body4(pool, cache);
    cache->free_all_tmp_objects();
    delete cache;

    // Construct a new remote cache
    cache = new RemoteCacheImpl<CountingPool>(pool, 0, 32, 256);
    cache->claim_master();
    cache->init({cache->root()}, 0);
    main_body5(pool, cache);
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