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

typedef RdmaSkipList<int, 7, INT_MIN, ULONG_MAX, ULONG_MAX - 1> RDMASK;

inline void rdmask_run(BenchmarkParams& params, rdma_capability* capability, RemoteCache* cache, Peer& host, Peer& self){
    CountingPool* pool = new CountingPool(false);
    RemoteCacheImpl<CountingPool>* cach = new RemoteCacheImpl<CountingPool>(pool);
    RemoteCacheImpl<CountingPool>::pool = pool; // set pool to other pool so we acccept our own cacheline

    RDMASK sk = RDMASK(self, CacheDepth::RootOnly, cach, pool, 1, {self});
    sk.InitAsFirst(pool);
    sk.RegisterThread();
    std::atomic<bool> do_cont;
    do_cont.store(true);
    std::thread t = std::thread([&](){
        RemoteCacheImpl<CountingPool>::pool = pool; // initialize the thread_local
        sk.helper_thread(&do_cont, pool);
    });
    REMUS_INFO("DONE INIT");
    sk.populate(pool, 64, 0, 100, std::function([=](int x){ return x; }));
    // REMUS_INFO("Remove(6) = {}", (int64_t) sk.remove(pool, 6).value_or(-1));
    // sk.debug();
    // REMUS_INFO("Insert(6) = {}", (int64_t) sk.insert(pool, 6, 6).value_or(-1));
    // sk.debug();

    int counter = 0;
    for(int i = 0; i < 10000; i++){
        if (sk.contains(pool, i).has_value()) counter++;
    }
    REMUS_INFO("matched_keys = {}", counter);
    REMUS_INFO("Count() = {}", sk.count(pool));

    do_cont.store(false);
    t.join();
    sk.debug();

    cach->free_all_tmp_objects();
    sk.destroy(pool);
    // if (!pool->HasNoLeaks()){
    //     pool->debug();
    //     REMUS_FATAL("Leaked memory");
    // }
}