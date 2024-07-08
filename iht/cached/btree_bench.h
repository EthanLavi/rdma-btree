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

typedef RdmaBPTree<int, 1> BTree;

inline void btree_run(BenchmarkParams& params, rdma_capability* capability, RemoteCache* cache, Peer& host, Peer& self){
    CountingPool* pool = new CountingPool(false);
    RemoteCacheImpl<CountingPool>* cach = new RemoteCacheImpl<CountingPool>(pool);
    RemoteCacheImpl<CountingPool>::pool = pool; // set pool to other pool so we acccept our own cacheline
    
    BTree tree = BTree(self, CacheDepth::RootOnly, cach, pool);
    tree.InitAsFirst(pool);
    REMUS_INFO("DONE INIT");

    // for(int i = 0; i < 1000; i++){
    //     REMUS_INFO("Insert({}, {}) = {}", i, i, tree.insert(pool, i, i).value_or(-1));
    //     for(int j = 0; j < i; j++){
    //         REMUS_ASSERT(tree.contains(pool, j).value_or(-1) == j, "Contains({}) is valid", j);
    //     }
    // }

    // tree.debug();
    tree.populate(pool, 1000, 0, 5000, std::function([=](int x){ return x; }));
    // tree.debug();
    REMUS_INFO(tree.count(pool));

    int second_cnt = 0;
    for(int i = 0; i <= 5000; i++){
        if (tree.contains(pool, i).has_value()) second_cnt++;
    }
    REMUS_INFO(second_cnt);

    cach->free_all_tmp_objects();
    tree.destroy(pool);
    // if (!pool->HasNoLeaks()){
    //     pool->debug();
    //     REMUS_FATAL("Leaked memory");
    // }
}