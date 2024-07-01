#include <protos/workloaddriver.pb.h>
#include <remus/logging/logging.h>
#include <remus/util/cli.h>
#include <remus/util/tcp/tcp.h>
#include <remus/rdma/memory_pool.h>
#include <remus/rdma/rdma.h>

// #include "map_cache.h"

#include "../experiment.h"
#include "ds/rdmask_cached.h"

#include <dcache/cache_store.h>

using namespace remus::util;

typedef RdmaSkipList<int, int, 7, INT_MIN> RDMASK;

template<> inline thread_local CacheMetrics RemoteCacheImpl<CountingPool>::metrics = CacheMetrics();
template<> inline thread_local CountingPool* RemoteCacheImpl<CountingPool>::pool = nullptr;

inline void rdmask_run(BenchmarkParams& params, rdma_capability* capability, RemoteCache* cache, Peer& host, Peer& self){
    CountingPool* pool = new CountingPool(false);
    RemoteCacheImpl<CountingPool>* cach = new RemoteCacheImpl<CountingPool>(pool);
    RemoteCacheImpl<CountingPool>::pool = pool; // set pool to other pool so we acccept our own cacheline
    
    RDMASK sk = RDMASK(self, CacheDepth::RootOnly, cach, pool);
    sk.InitAsFirst(pool);
    REMUS_INFO("DONE INIT");
    REMUS_INFO("Insert(0, 0) = {}", sk.insert(pool, 0, 0).value_or(-1));
    REMUS_INFO("Insert(1, 1) = {}", sk.insert(pool, 1, 1).value_or(-1));
    REMUS_INFO("Insert(2, 2) = {}", sk.insert(pool, 2, 2).value_or(-1));
    REMUS_INFO("Insert(5, 5) = {}", sk.insert(pool, 5, 5).value_or(-1));
    REMUS_INFO("Insert(4, 4) = {}", sk.insert(pool, 4, 4).value_or(-1));
    REMUS_INFO("Insert(3, 3) = {}", sk.insert(pool, 3, 3).value_or(-1));
    sk.debug();
    for(int i = 0; i <= 5; i++)
        REMUS_ASSERT(sk.contains(pool, i).value_or(-1) == i, "Contains({}) is valid", i);

    sk.remove(pool, 2);
    sk.debug();

    cach->free_all_tmp_objects();
    sk.destroy(pool);
    // if (!pool->HasNoLeaks()){
    //     pool->debug();
    //     REMUS_FATAL("Leaked memory");
    // }
}