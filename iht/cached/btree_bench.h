#include <cstdint>
#include <barrier>
#include <memory>
#include <protos/workloaddriver.pb.h>
#include <remus/logging/logging.h>
#include <remus/util/cli.h>
#include <remus/util/tcp/tcp.h>
#include <remus/rdma/memory_pool.h>
#include <remus/rdma/rdma.h>

#include "bench_helper.h"
#include "ds/btree_cached.h"

#include "../experiment.h"
#include "../role_client.h"
#include "../tcp_barrier.h"
#include "../common.h"
#include "../experiment.h"
#include "../../dcache/test/faux_mempool.h"

#include <dcache/cache_store.h>

using namespace remus::util;
using namespace remus::rdma;

typedef RdmaBPTree<int, 3, rdma_capability_thread> BTree; // todo : increment size
typedef RdmaBPTree<int, 3, CountingPool> BTreeLocal;

inline void btree_run(BenchmarkParams& params, rdma_capability* capability, RemoteCache* cache, Peer& host, Peer& self){
    // Create a list of client and server  threads
    std::vector<std::thread> threads;
    if (params.node_id == 0){
        // If dedicated server-node, we must send IHT pointer and wait for clients to finish
        threads.emplace_back(std::thread([&](){
            auto pool = capability->RegisterThread();
            // Initialize X connections
            tcp::SocketManager* socket_handle = init_handle(params);

            // Collect and redistribute the CacheStore pointers
            collect_distribute(socket_handle, params);

            // Create a root ptr to the IHT
            Peer p = Peer();
            BTree btree = BTree(p, CacheDepth::None, cache, pool, true);
            rdma_ptr<anon_ptr> root_ptr = btree.InitAsFirst(pool);
            // Send the root pointer over
            tcp::message ptr_message = tcp::message(root_ptr.raw());
            socket_handle->send_to_all(&ptr_message);

            // Block until client is done, helping synchronize clients when they need
            ExperimentManager::ServerStopBarrier(socket_handle, 0); // before populate
            ExperimentManager::ServerStopBarrier(socket_handle, 0); // after populate
            ExperimentManager::ServerStopBarrier(socket_handle, 0); // after count
            ExperimentManager::ServerStopBarrier(socket_handle, params.runtime); // after operations

            // Collect and redistribute the size deltas
            collect_distribute(socket_handle, params);

            // Wait until clients are done with correctness exchange (they all run count afterwards)
            ExperimentManager::ServerStopBarrier(socket_handle, 0);
            delete socket_handle;
            REMUS_INFO("[SERVER THREAD] -- End of execution; -- ");
        }));
    }

    // Initialize T endpoints, one for each thread
    tcp::EndpointManager* endpoint_managers[params.thread_count];
    init_endpoints(endpoint_managers, params, host);

    // sleep for a short while to ensure the receiving end (SocketManager) is up and running
    // If the endpoint cant connect, it will just wait and retry later
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Barrier to start all the clients at the same time
    std::barrier client_sync = std::barrier(params.thread_count);
    WorkloadDriverResult workload_results[params.thread_count];
    for(int i = 0; i < params.thread_count; i++){
        threads.emplace_back(std::thread([&](int thread_index){
            // Get pool
            rdma_capability_thread* pool = capability->RegisterThread();
            tcp::EndpointManager* endpoint = endpoint_managers[thread_index];

             // initialize thread's thread_local pool
            RemoteCache::pool = pool; 
            // Exchange the root pointer of the other cache stores via TCP module
            vector<uint64_t> peer_roots;
            map_reduce(endpoint, params, cache->root(), std::function<void(uint64_t)>([&](uint64_t data){
                peer_roots.push_back(data);
            }));
            cache->init(peer_roots);

            std::shared_ptr<BTree> btree = std::make_shared<BTree>(self, params.cache_depth, cache, pool, false);
            // Get the data from the server to init the btree
            tcp::message ptr_message;
            endpoint->recv_server(&ptr_message);
            btree->InitFromPointer(rdma_ptr<anon_ptr>(ptr_message.get_first()));

            REMUS_DEBUG("Creating client");
            // Create and run a client in a thread
            int delta = 0;
            int populate_amount = 0;
            MapAPI* btree_as_map = new MapAPI(
                [&](int key, int value){
                    auto res = btree->insert(pool, key, value);
                    if (res == std::nullopt) delta++;
                    return res;
                },
                [&](int key){ return btree->contains(pool, key); },
                [&](int key){
                    auto res = btree->remove(pool, key);
                    if (res != std::nullopt) delta--;
                    return res;
                },
                [&](int op_count, int key_lb, int key_ub){
                    // capability->RegisterThread();
                    ExperimentManager::ClientArriveBarrier(endpoint);
                    delta += btree->populate(pool, op_count, key_lb, key_ub, [=](int key){ return key; });
                    ExperimentManager::ClientArriveBarrier(endpoint);
                    populate_amount = btree->count(pool);
                    // todo: warm up the cache?
                    ExperimentManager::ClientArriveBarrier(endpoint);
                    cache->print_metrics();
                    cache->reset_metrics();
                }
            );

            using client_t = Client<Map_Op<int, int>>;
            std::unique_ptr<client_t> client = client_t::Create(host, endpoint, params, &client_sync, btree_as_map, std::function<void()>([=](){}));
            double populate_frac = 0.5 / (double) (params.node_count * params.thread_count);

            StatusVal<WorkloadDriverResult> output = client_t::Run(std::move(client), thread_index, populate_frac);
            REMUS_ASSERT(output.status.t == StatusType::Ok && output.val.has_value(), "Client run failed");
            workload_results[thread_index] = output.val.value();

            // Check expected size
            int all_delta = 0;
            map_reduce(endpoint, params, delta, std::function<void(uint64_t)>([&](uint64_t d){
                all_delta += d;
            }));

            // add count after syncing via endpoint exchange
            int final_size = btree->count(pool);
            if (thread_index == 0){
                REMUS_DEBUG("Size (after populate) [{}]", populate_amount);
                REMUS_DEBUG("Size (final) [{}]", final_size);
                REMUS_DEBUG("Delta = {}", all_delta);
                // REMUS_INFO("BTree is valid? {}", btree->valid());
                // btree->debug();
                REMUS_ASSERT(final_size - all_delta == 0, "Initial size + delta ==? Final size");
            }

            ExperimentManager::ClientArriveBarrier(endpoint);
            REMUS_INFO("[CLIENT THREAD] -- End of execution; -- ");
            cache->print_metrics();
        }, i));
    }

    // Join all threads
    int i = 0;
    for (auto it = threads.begin(); it != threads.end(); it++){
        // For debug purposes, sometimes it helps to see which threads haven't deadlocked
        REMUS_DEBUG("Syncing {}", ++i);
        auto t = it;
        t->join();
    }
    delete_endpoints(endpoint_managers, params);

    save_result("btree_result.csv", workload_results, params, params.thread_count);
    // todo: optimize using a built-in flag into cachedobject so that we can skip verification
}

inline void btree_run_local(BenchmarkParams& params, rdma_capability* capability, RemoteCache* cache, Peer& host, Peer& self){
    CountingPool* pool = new CountingPool(false);
    RemoteCacheImpl<CountingPool>* cach = new RemoteCacheImpl<CountingPool>(pool);
    RemoteCacheImpl<CountingPool>::pool = pool; // set pool to other pool so we acccept our own cacheline

    BTreeLocal tree = BTreeLocal(self, CacheDepth::RootOnly, cach, pool, true);
    rdma_ptr<anon_ptr> ptr = tree.InitAsFirst(pool);
    REMUS_INFO("DONE INIT");

    // for(int i = 0; i < 1000; i++){
    //     REMUS_INFO("Insert({}, {}) = {}", i, i, tree.insert(pool, i, i).value_or(-1));
    //     for(int j = 0; j < i; j++){
    //         int val = tree.contains(pool, j).value_or(-1);
    //         if (val != j)
    //             tree.debug();
    //         REMUS_ASSERT(val == j, "Contains({}) is valid", j);
    //     }
    // }

    // tree.populate(pool, 20, 0, 50, std::function([=](int x){ return x; }));
    // tree.debug();
    // REMUS_INFO("Count = {}", tree.count(pool));

    // int second_cnt = 0;
    // for(int i = 0; i <= 5000; i++){
    //     if (tree.contains(pool, i).value_or(-1) == i) second_cnt++;
    // }
    // REMUS_INFO("Contain = {}", second_cnt);
    
    const int THREAD_COUNT = 40;
    std::vector<std::thread> threads;
    std::barrier<> barr(THREAD_COUNT);
    for(int tid = 0; tid != THREAD_COUNT; tid++){
        threads.push_back(std::thread([&](int start){
            barr.arrive_and_wait();
            RemoteCacheImpl<CountingPool>::pool = pool;
            BTreeLocal tree_tlocal = BTreeLocal(self, CacheDepth::RootOnly, cach, pool, false);
            tree_tlocal.InitFromPointer(ptr);
            tree_tlocal.populate(pool, 250, 0, 20000, std::function([=](int x){ return x; }));
        }, tid));
    }
    for(int tid = 0; tid != THREAD_COUNT; tid++){
        threads.at(tid).join();
    }
    tree.debug();
    REMUS_INFO("Tree is valid? {}", tree.valid());

    cach->free_all_tmp_objects();
    tree.destroy(pool);
    // if (!pool->HasNoLeaks()){
    //     pool->debug();
    //     REMUS_FATAL("Leaked memory");
    // }
}