#include <chrono>
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
#include <thread>

using namespace remus::util;

typedef RdmaSkipList<int, 7, INT_MIN, ULONG_MAX, ULONG_MAX - 1> RDMASK;
typedef node<int, 7> Node;

inline void rdmask_run(BenchmarkParams& params, rdma_capability* capability, RemoteCache* cache, Peer& host, Peer& self){
    CountingPool* pool = new CountingPool(true);
    RemoteCacheImpl<CountingPool>* cach = new RemoteCacheImpl<CountingPool>(pool);
    RemoteCacheImpl<CountingPool>::pool = pool; // set pool to other pool so we acccept our own cacheline

    EBRObjectPool<Node, 100>* ebr = new EBRObjectPool<Node, 100>(pool, 2);
    RDMASK sk = RDMASK(self, CacheDepth::RootOnly, cach, pool, 1, {self}, ebr);
    sk.InitAsFirst(pool);
    vector<LimboLists<Node>*> qs;
    qs.push_back(ebr->RegisterThread());
    std::atomic<bool> do_cont;
    do_cont.store(true);
    std::thread t = std::thread([&](){
        RemoteCacheImpl<CountingPool>::pool = pool; // initialize the thread_local
        ebr->RegisterThread();
        sk.helper_thread(&do_cont, pool, ebr, qs);

        cach->free_all_tmp_objects();
    });
    REMUS_INFO("DONE INIT");
    sk.populate(pool, 16, 0, 100, std::function([=](int x){ return x; }));

    int counter = 0;
    for(int i = 0; i < 10000; i++){
        if (sk.contains(pool, i).has_value()) counter++;
    }
    REMUS_INFO("matched_keys = {}", counter);
    REMUS_INFO("Count() = {}", sk.count(pool));

    /// Delete the data structure
    for(int i = 0; i <= 10000; i++){
        sk.remove(pool, i).value_or(-1);
    }

    this_thread::sleep_for(std::chrono::milliseconds(10)); // wait a second for helper thread to catch up

    do_cont.store(false);
    t.join();
    sk.debug();

    cach->free_all_tmp_objects();
    ebr->destroy(pool);
    sk.destroy(pool);
    delete cach;
    if (!pool->HasNoLeaks()){
        pool->debug();
        REMUS_FATAL("Leaked memory");
    } else {
        REMUS_INFO("No Leaks!");
    }
}