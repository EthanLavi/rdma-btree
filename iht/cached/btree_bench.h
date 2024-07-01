#include <climits>
#include <protos/workloaddriver.pb.h>
#include <remus/logging/logging.h>
#include <remus/util/cli.h>
#include <remus/util/tcp/tcp.h>
#include <remus/rdma/memory_pool.h>
#include <remus/rdma/rdma.h>

// #include "map_cache.h"
#include "ds/btree_cached.h"

#include "../experiment.h"

#include <dcache/cache_store.h>

using namespace remus::util;

typedef RdmaBPTree<int, int, 1, INT_MAX> BTree;

template<> inline thread_local CacheMetrics RemoteCacheImpl<CountingPool>::metrics = CacheMetrics();
template<> inline thread_local CountingPool* RemoteCacheImpl<CountingPool>::pool = nullptr;

inline void btree_run(BenchmarkParams& params, rdma_capability* capability, RemoteCache* cache, Peer& host, Peer& self){
    CountingPool* pool = new CountingPool(false);
    RemoteCacheImpl<CountingPool>* cach = new RemoteCacheImpl<CountingPool>(pool);
    RemoteCacheImpl<CountingPool>::pool = pool; // set pool to other pool so we acccept our own cacheline
    
    BTree tree = BTree(self, CacheDepth::RootOnly, cach, pool);
    tree.InitAsFirst(pool);
    REMUS_INFO("DONE INIT");

    // REMUS_INFO("Contains(1) = {}", tree.contains(pool, 1).value_or(-1));
    // REMUS_INFO("Insert(1, 10) = {}", tree.insert(pool, 1, 10).value_or(-1));
    // REMUS_INFO("Contains(1) = {}", tree.contains(pool, 1).value_or(-1));
    // REMUS_INFO("Remove(1) = {}", tree.remove(pool, 1).value_or(-1));
    // REMUS_INFO("Contains(1) = {}", tree.contains(pool, 1).value_or(-1));

    REMUS_INFO("Insert(0, 0) = {}", tree.insert(pool, 0, 0).value_or(-1));
    REMUS_INFO("Insert(1, 1) = {}", tree.insert(pool, 1, 1).value_or(-1));
    REMUS_INFO("Insert(2, 2) = {}", tree.insert(pool, 2, 2).value_or(-1));
    REMUS_INFO("Insert(3, 3) = {}", tree.insert(pool, 3, 3).value_or(-1));
    REMUS_INFO("Insert(4, 4) = {}", tree.insert(pool, 4, 4).value_or(-1));
    REMUS_INFO("Insert(5, 5) = {}", tree.insert(pool, 5, 5).value_or(-1));
    REMUS_INFO("Insert(6, 6) = {}", tree.insert(pool, 6, 6).value_or(-1));
    REMUS_INFO("Insert(7, 7) = {}", tree.insert(pool, 7, 7).value_or(-1));
    REMUS_INFO("Insert(8, 8) = {}", tree.insert(pool, 8, 8).value_or(-1));
    REMUS_INFO("Insert(9, 9) = {}", tree.insert(pool, 9, 9).value_or(-1));
    REMUS_INFO("Insert(10, 10) = {}", tree.insert(pool, 10, 10).value_or(-1));
    REMUS_INFO("Insert(11, 11) = {}", tree.insert(pool, 11, 11).value_or(-1));
    REMUS_INFO("Insert(12, 12) = {}", tree.insert(pool, 12, 12).value_or(-1));
    REMUS_INFO("Insert(13, 13) = {}", tree.insert(pool, 13, 13).value_or(-1));
    REMUS_INFO("Insert(14, 14) = {}", tree.insert(pool, 14, 14).value_or(-1));
    REMUS_INFO("Insert(15, 15) = {}", tree.insert(pool, 15, 15).value_or(-1));
    REMUS_INFO("Insert(16, 16) = {}", tree.insert(pool, 16, 16).value_or(-1));
    REMUS_INFO("Insert(17, 17) = {}", tree.insert(pool, 17, 17).value_or(-1));
    tree.debug();
    for(int i = 0; i <= 17; i++)
        REMUS_ASSERT(tree.contains(pool, i).value_or(-1) == i, "Contains({}) is valid", i);

    // for(int i = 0; i < 10000; i++){
    //     REMUS_ASSERT(tree.contains(pool, i).value_or(-1) == -1, "No contains {}", i);
    //     REMUS_ASSERT(tree.insert(pool, i, i * 2).value_or(-1) == -1, "Insert success {}", i);
    //     REMUS_ASSERT(tree.contains(pool, i).value_or(-1) == i * 2, "Contains {}", i);
    // }

    cach->free_all_tmp_objects();
    tree.destroy(pool);
    // if (!pool->HasNoLeaks()){
    //     pool->debug();
    //     REMUS_FATAL("Leaked memory");
    // }
}