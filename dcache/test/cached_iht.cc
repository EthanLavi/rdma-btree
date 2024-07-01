#include <remus/logging/logging.h>
#include <remus/rdma/memory_pool.h>
#include <remus/rdma/rdma.h>

#include <dcache/cache_store.h>
#include <dcache/cached_ptr.h>

#include "../../iht/common.h"
#include "faux_mempool.h"
#include "faux_iht.h"

using namespace remus::rdma;

// Set remote cache static variables
template<> inline thread_local CacheMetrics RemoteCacheImpl<CountingPool>::metrics = CacheMetrics();
template<> inline thread_local CountingPool* RemoteCacheImpl<CountingPool>::pool = nullptr;

int main(){
    // Construct a capability
    CountingPool* pool = new CountingPool(false);

    // Construct the remote cache
    RemoteCacheImpl<CountingPool>* cache = new RemoteCacheImpl<CountingPool>(pool);
    RemoteCacheImpl<CountingPool>::pool = pool; // set pool to other pool so we acccept our own cacheline
    cache->init({cache->root()}); // initialize with itself

    Peer self = Peer(0);
    auto iht = new RdmaIHT<int, int, CNF_ELIST_SIZE, CNF_PLIST_SIZE>(self, CacheDepth::UpToLayer1, cache, pool);
    iht->InitAsFirst(pool);

    iht->populate(pool, 10000, 0, 20000, [=](int x){ return x; });
    REMUS_ASSERT(iht->count(pool) == 10000, "Found correct size in IHT");
    for(int i = 0; i < 10000; i++){
        iht->insert(pool, i, i); // ensure 1 was inserted
        REMUS_ASSERT(iht->contains(pool, i).value_or(0) == i, "Found correct value in IHT");
        REMUS_ASSERT(iht->remove(pool, i).value_or(0) == i, "Removed value at i");
    }

    // Free memory
    cache->free_all_tmp_objects();
    delete cache;
    iht->destroy(pool);

    // Check for no leaked memory
    if (pool->HasNoLeaks()){
        REMUS_INFO("No Leaks In Cache Store");
    } else {
        REMUS_ERROR("Found Leaks in Cache Store");
        pool->debug();
        return 1;
    }
    return 0;
}