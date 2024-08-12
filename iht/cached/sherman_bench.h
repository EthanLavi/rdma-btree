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
#include "ds/sherman.h"

#include "../experiment.h"
#include "../role_client.h"
#include "../tcp_barrier.h"
#include "../common.h"
#include "../experiment.h"
#include "../../dcache/test/faux_mempool.h"
#include "sherman/sherman_cache.h"

#include <dcache/cache_store.h>

using namespace remus::util;
using namespace remus::rdma;

inline void sherman_run_local(BenchmarkParams& params, rdma_capability* capability, RemoteCache* cache, Peer& host, Peer& self, std::vector<Peer> peers){
    using BTree = ShermanBPTree<int, 12, rdma_capability_thread>; // todo: increment size more?

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
            BTree btree = BTree(p, cache, pool, nullptr, nullptr, true);
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

    /// Create an ebr object
    using EBRLeaf = EBRObjectPool<BTree::BLeaf, 100, rdma_capability_thread>;
    using EBRNode = EBRObjectPoolAccompany<BTree::BNode, BTree::BLeaf, 100, rdma_capability_thread>;
    auto ebr_pool = capability->RegisterThread();
    EBRLeaf* ebr_leaf = new EBRLeaf(ebr_pool, params.thread_count);
    for(int i = 0; i < peers.size(); i++){
        REMUS_INFO("Peer({}, {}, {})", peers.at(i).id, peers.at(i).address, peers.at(i).port);
    }
    ebr_leaf->Init(capability, self.id, peers);
    REMUS_INFO("Init ebr");
    EBRNode* ebr_node = new EBRNode(ebr_leaf);

    // Barrier to start all the clients at the same time
    std::barrier client_sync = std::barrier(params.thread_count);
    WorkloadDriverResult workload_results[params.thread_count];
    for(int i = 0; i < params.thread_count; i++){
        threads.emplace_back(std::thread([&](int thread_index){
            // Get pool
            rdma_capability_thread* pool = capability->RegisterThread();
            tcp::EndpointManager* endpoint = endpoint_managers[thread_index];
            ebr_leaf->RegisterThread();
            ebr_node->RegisterThread();

             // initialize thread's thread_local pool
            RemoteCache::pool = pool; 
            // Exchange the root pointer of the other cache stores via TCP module
            vector<uint64_t> peer_roots;
            map_reduce(endpoint, params, cache->root(), std::function<void(uint64_t)>([&](uint64_t data){
                peer_roots.push_back(data);
            }));
            cache->init(peer_roots);

            std::shared_ptr<BTree> btree = std::make_shared<BTree>(self, cache, pool, ebr_leaf, ebr_node);
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
                if (params.node_count == 1 && (params.key_ub - params.key_lb) < 2000) {
                    btree->debug();
                    REMUS_INFO("BTree is valid? {}", btree->valid());
                }
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
}

inline void sherman_run_local(Peer& self){
    CountingPool* pool = new CountingPool(true);

    using BTreeLocal = ShermanBPTree<int, 1, CountingPool>;
    using Cache = IndexCache<BTreeLocal::BNode, 1, int>;
    Cache cache = Cache(1000);
    rdma_ptr<BTreeLocal::BNode> nptr = pool->Allocate<BTreeLocal::BNode>();
    BTreeLocal::BNode node = *nptr;
    node = BTreeLocal::BNode();
    node.set_range(1, 10);
    node.set_ptr(0, nptr);
    REMUS_ASSERT(cache.add_to_cache(&node), "Should be easy to add");
    rdma_ptr<BTreeLocal::BNode> addr; 
    const CacheEntry<BTreeLocal::BNode>* entry = cache.search_from_cache(5, &addr);
    REMUS_ASSERT(entry != nullptr, "Should be valid");
    REMUS_INFO("bnode={} range=[{}, {}) freq={}", (uint64_t) entry, entry->from, entry->to, entry->data->index_cache_freq);
    cache.bench();
    cache.statistics();
    return;

    RemoteCacheImpl<CountingPool>* cach = new RemoteCacheImpl<CountingPool>(pool, 0);
    RemoteCacheImpl<CountingPool>::pool = pool; // set pool to other pool so we acccept our own cacheline

    using EBRLeaf = EBRObjectPool<BTreeLocal::BLeaf, 100, CountingPool>;
    using EBRNode = EBRObjectPoolAccompany<BTreeLocal::BNode, BTreeLocal::BLeaf, 100, CountingPool>;
    EBRLeaf* ebr_leaf = new EBRLeaf(pool, 1);
    EBRNode* ebr_node = new EBRNode(ebr_leaf);
    ebr_leaf->RegisterThread();
    ebr_node->RegisterThread();

    BTreeLocal tree = BTreeLocal(self, cach, pool, ebr_leaf, ebr_node, true);
    rdma_ptr<anon_ptr> ptr = tree.InitAsFirst(pool);
    REMUS_INFO("DONE INIT");

    for(int i = 40; i >= 0; i--){
        tree.insert(pool, i, i);
    }

    REMUS_INFO("Count = {}", tree.count(pool));

    int second_cnt = 0;
    for(int i = 0; i <= 5000; i++){
        if (tree.contains(pool, i).value_or(-1) == i) second_cnt++;
    }
    REMUS_INFO("Contain = {}", second_cnt);

    for(int i = 5; i < 35; i++){
        tree.remove(pool, i);
        tree.remove(pool, i); // remove twice to trigger merge
    }
    tree.debug();
    
    REMUS_INFO("Tree is valid? {}", tree.valid());
    REMUS_INFO("Done!");
    cach->free_all_tmp_objects();
    ebr_leaf->destroy(pool);
    ebr_node->destroy(pool);
    tree.destroy(pool);
    delete cach;
    if (!pool->HasNoLeaks()){
        // pool->debug();
        REMUS_FATAL("Leaked memory");
    }
}