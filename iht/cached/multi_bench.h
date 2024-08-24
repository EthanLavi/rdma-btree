#pragma once

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
#include "ds/multi_list.h"

#include "../experiment.h"
#include "../role_client.h"
#include "../tcp_barrier.h"
#include "../common.h"
#include "../experiment.h"
#include "../../dcache/test/faux_mempool.h"

#include <dcache/cache_store.h>
#include <vector>

using namespace remus::util;
using namespace remus::rdma;

// todo: increment size here! log2(keyspace)?
#ifndef MAX_HEIGHT_MK
#define MAX_HEIGHT_MK 12
#endif

inline void multi_run(BenchmarkParams& params, rdma_capability* capability, RemoteCache* cache, Peer& host, Peer& self, std::vector<Peer> peers){
    using MultiList = RdmaMultiList<int, MAX_HEIGHT_MK, INT_MIN, ULONG_MAX, ULONG_MAX - 1, rdma_capability_thread>;
    using Node = node<int, MAX_HEIGHT_MK>;

    REMUS_ASSERT(params.thread_count >= 2, "Thread count should be at least 2 to account for the helper thread");
    
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
            MultiList sk = MultiList(self, params.cache_depth + 1, cache, pool, peers, nullptr);
            sk.set_key_range(params.key_lb, params.key_ub);
            rdma_ptr<anon_ptr> root_ptr = sk.InitAsFirst(pool);
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
    using EBR_Manager = EBRObjectPool<Node, 100, rdma_capability_thread>;
    EBR_Manager* ebr = new EBR_Manager(capability->RegisterThread(), params.thread_count);
    for(int i = 0; i < peers.size(); i++){
        REMUS_INFO("Peer({}, {}, {})", peers.at(i).id, peers.at(i).address, peers.at(i).port);
    }
    ebr->Init(capability, self.id, peers);
    REMUS_INFO("Init ebr");

    // Barrier to start all the clients at the same time
    std::barrier client_sync = std::barrier(params.thread_count - 1);
    WorkloadDriverResult workload_results[params.thread_count - 1];
    
    // LimboLists
    std::barrier init_sync = std::barrier(params.thread_count);
    vector<LimboLists<Node>*> qs;
    mutex mu;
    std::atomic<bool> do_cont;
    do_cont.store(true);
    int helper_tidx = params.thread_count - 1;

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
            cache->init(peer_roots, params.node_count - 1);

            std::shared_ptr<MultiList> sk = std::make_shared<MultiList>(self, params.cache_depth + 1, cache, pool, peers, ebr);
            sk->set_key_range(params.key_lb, params.key_ub);
            // Get the data from the server to init the btree
            tcp::message ptr_message;
            endpoint->recv_server(&ptr_message);
            sk->InitFromPointer(rdma_ptr<anon_ptr>(ptr_message.get_first()));

            REMUS_DEBUG(thread_index != helper_tidx ? "Creating client" : "Creating helper thread");

            int delta = 0;
            int populate_amount = 0;
            // Create and run a client in a thread
            if (thread_index == helper_tidx){
                init_sync.arrive_and_wait(); // wait until all threads have added to the limbo lists

                ExperimentManager::ClientArriveBarrier(endpoint); // before populate
                ExperimentManager::ClientArriveBarrier(endpoint); // after populate
                ExperimentManager::ClientArriveBarrier(endpoint); // after count

                REMUS_ASSERT(qs.size() == params.thread_count - 1, "Accurate # of LimboLists");
                ebr->RegisterThread();
                init_sync.arrive_and_drop(); // arrive pre-emptively
                sk->helper_thread(&do_cont, capability->RegisterThread(), ebr, qs); // then establish the helper thread
                // the helper thread will finish last because of init_sync

                ExperimentManager::ClientArriveBarrier(endpoint); // stop the client with this
            } else {
                MapAPI* rdmask_as_map = new MapAPI(
                    [&](MapCodes code, int param1, int param2, int param3){
                        if (code == Prepare){
                            if (params.node_id == 0 && thread_index == 0){
                            cache->claim_master();
                            }
                            // capability->RegisterThread();
                            ExperimentManager::ClientArriveBarrier(endpoint);
                            delta += sk->populate(pool, param1, param2, param3, [=](int key){ return key; });
                            ExperimentManager::ClientArriveBarrier(endpoint);
                            populate_amount = sk->count(pool); // ? IMPORTANT - Count hits every element which in effect warms up the cache
                                            // ? BENCHMARK EXECUTION STARTS WITH NO INVALID CACHE LINES
                            ExperimentManager::ClientArriveBarrier(endpoint);
                            cache->print_metrics();
                            cache->reset_metrics();
                            std::this_thread::sleep_for(std::chrono::seconds(3)); // wait 3 seconds for the helper thread to catch up
                        } else if (code == Get){
                            return sk->contains(pool, param1);
                        } else if (code == Remove){
                            auto res = sk->remove(pool, param1);
                            if (res != std::nullopt) delta--;
                            return res;
                        } else if (code == Insert){
                            auto res = sk->insert(pool, param1, param2);
                            if (res == std::nullopt) delta++;
                            return res;
                        } else {
                            REMUS_WARN("No valid code");
                        }
                        return optional<uint64_t>();
                    }
                );

                // Add to the vector of limbo lists and wait
                mu.lock();
                qs.push_back(ebr->RegisterThread());
                mu.unlock();
                init_sync.arrive_and_wait();

                using client_t = Client<Map_Op<int, int>>;
                std::unique_ptr<client_t> client = client_t::Create(host, endpoint, params, &client_sync, rdmask_as_map, [&](){
                    REMUS_INFO("Stopping helper thread (client finished)");
                    init_sync.arrive_and_wait(); // wait until all other threads have completed
                    do_cont.store(false); // stop the helper thread at end
                });
                double populate_frac = 0.5 / (double) (params.node_count * (params.thread_count - 1));

                StatusVal<WorkloadDriverResult> output = client_t::Run(std::move(client), thread_index, populate_frac);
                REMUS_ASSERT(output.status.t == StatusType::Ok && output.val.has_value(), "Client run failed");
                workload_results[thread_index] = output.val.value();
            }

            // Check expected size
            int all_delta = 0;
            map_reduce(endpoint, params, delta, std::function<void(uint64_t)>([&](uint64_t d){
                all_delta += d;
            }));

            // add count after syncing via endpoint exchange
            if (thread_index == 0){
                int final_size = sk->count(pool);
                REMUS_DEBUG("Size (after populate) [{}]", populate_amount);
                REMUS_DEBUG("Size (final) [{}]", final_size);
                REMUS_DEBUG("Delta = {}", all_delta);
                // debug print if everything is local for inspection? and is small enough
                if (params.node_count == 1 && (params.key_ub - params.key_lb) < 2000) sk->debug();
                REMUS_ASSERT(final_size - all_delta == 0, "Initial size + delta ==? Final size");
            }

            ExperimentManager::ClientArriveBarrier(endpoint);
            REMUS_INFO("[{} THREAD] -- End of execution; -- ", thread_index == helper_tidx ? "HELPER" : "CLIENT");
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

    save_result("multi_result.csv", workload_results, params, params.thread_count - 1);
}

inline void multi_run_tmp(BenchmarkParams& params, CountingPool* pool, RemoteCacheImpl<CountingPool>* cache, Peer& host, Peer& self, std::vector<Peer> peers){
    using Node = node<int, MAX_HEIGHT_MK>;
    using MultiListLocal = RdmaMultiList<int, MAX_HEIGHT_MK, INT_MIN, ULONG_MAX, ULONG_MAX - 1, CountingPool>;
    REMUS_ASSERT(params.thread_count >= 3, "Thread count should be at least 3 to account for the two helper thread");
    
    // Create a list of client and server  threads
    std::vector<std::thread> threads;
    if (params.node_id == 0){
        // If dedicated server-node, we must send IHT pointer and wait for clients to finish
        threads.emplace_back(std::thread([&](){
            // Initialize X connections
            tcp::SocketManager* socket_handle = init_handle(params);

            // Collect and redistribute the CacheStore pointers
            collect_distribute(socket_handle, params);

            // Create a root ptr to the IHT
            Peer p = Peer();
            MultiListLocal sk = MultiListLocal(self, params.cache_depth + 1, cache, pool, peers, nullptr);
            sk.set_key_range(params.key_lb, params.key_ub);
            rdma_ptr<anon_ptr> root_ptr = sk.InitAsFirst(pool);
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
    using EBR_Manager = EBRObjectPool<Node, 100, CountingPool>;
    EBR_Manager* ebr = new EBR_Manager(pool, params.thread_count);
    for(int i = 0; i < peers.size(); i++){
        REMUS_INFO("Peer({}, {}, {})", peers.at(i).id, peers.at(i).address, peers.at(i).port);
    }
    // ebr->Init(pool, self.id, peers);
    REMUS_INFO("Init ebr");

    // Barrier to start all the clients at the same time
    std::barrier client_sync = std::barrier(params.thread_count - 2);
    WorkloadDriverResult workload_results[params.thread_count - 2];
    
    // LimboLists
    std::barrier init_sync = std::barrier(params.thread_count);
    vector<LimboLists<Node>*> qs;
    mutex mu;
    std::atomic<bool> do_cont;
    do_cont.store(true);
    int helper_tidx = params.thread_count - 1;

    for(int i = 0; i < params.thread_count; i++){
        threads.emplace_back(std::thread([&](int thread_index){
            // Get pool
            tcp::EndpointManager* endpoint = endpoint_managers[thread_index];

             // initialize thread's thread_local pool
            RemoteCacheImpl<CountingPool>::pool = pool;
            // Exchange the root pointer of the other cache stores via TCP module
            vector<uint64_t> peer_roots;
            map_reduce(endpoint, params, cache->root(), std::function<void(uint64_t)>([&](uint64_t data){
                peer_roots.push_back(data);
            }));
            cache->init(peer_roots, params.node_count - 1);

            std::shared_ptr<MultiListLocal> sk = std::make_shared<MultiListLocal>(self, params.cache_depth + 1, cache, pool, peers, ebr);
            sk->set_key_range(params.key_lb, params.key_ub);
            // Get the data from the server to init the btree
            tcp::message ptr_message;
            endpoint->recv_server(&ptr_message);
            sk->InitFromPointer(rdma_ptr<anon_ptr>(ptr_message.get_first()));

            // todo: add second helper thread for debug purposes!
            REMUS_INFO("{}", thread_index);

            int delta = 0;
            int populate_amount = 0;
            // Create and run a client in a thread
            if (thread_index == helper_tidx || thread_index == helper_tidx - 1){
                REMUS_INFO("Helper thread {}", thread_index);
                init_sync.arrive_and_wait(); // wait until all threads have added to the limbo lists

                ExperimentManager::ClientArriveBarrier(endpoint); // before populate
                ExperimentManager::ClientArriveBarrier(endpoint); // after populate
                ExperimentManager::ClientArriveBarrier(endpoint); // after count

                REMUS_ASSERT(qs.size() == params.thread_count - 2, "Accurate # of LimboLists");
                ebr->RegisterThread();
                init_sync.arrive_and_drop(); // arrive pre-emptively
                if (thread_index == helper_tidx) // ! only run 1 helper thread
                    sk->helper_thread(&do_cont, pool, ebr, qs); // then establish the helper thread
                // the helper thread will finish last because of init_sync

                ExperimentManager::ClientArriveBarrier(endpoint); // stop the client with this
            } else {
                MapAPI* rdmask_as_map = new MapAPI(
                    [&](MapCodes code, int param1, int param2, int param3){
                        if (code == Prepare){
                            if (params.node_id == 0 && thread_index == 0){
                            cache->claim_master();
                            }
                            // capability->RegisterThread();
                            ExperimentManager::ClientArriveBarrier(endpoint);
                            delta += sk->populate(pool, param1, param2, param3, [=](int key){ return key; });
                            ExperimentManager::ClientArriveBarrier(endpoint);
                            populate_amount = sk->count(pool); // ? IMPORTANT - Count hits every element which in effect warms up the cache
                                            // ? BENCHMARK EXECUTION STARTS WITH NO INVALID CACHE LINES
                            ExperimentManager::ClientArriveBarrier(endpoint);
                            cache->print_metrics();
                            cache->reset_metrics();
                            std::this_thread::sleep_for(std::chrono::seconds(3)); // wait 3 seconds for the helper thread to catch up
                        } else if (code == Get){
                            return sk->contains(pool, param1);
                        } else if (code == Remove){
                            auto res = sk->remove(pool, param1);
                            if (res != std::nullopt) delta--;
                            return res;
                        } else if (code == Insert){
                            auto res = sk->insert(pool, param1, param2);
                            if (res == std::nullopt) delta++;
                            return res;
                        } else {
                            REMUS_WARN("No valid code");
                        }
                        return optional<uint64_t>();
                    }
                );

                // Add to the vector of limbo lists and wait
                mu.lock();
                qs.push_back(ebr->RegisterThread());
                mu.unlock();
                init_sync.arrive_and_wait();

                using client_t = Client<Map_Op<int, int>>;
                std::unique_ptr<client_t> client = client_t::Create(host, endpoint, params, &client_sync, rdmask_as_map, [&](){
                    REMUS_INFO("Stopping helper thread (client finished)");
                    init_sync.arrive_and_wait(); // wait until all other threads have completed
                    do_cont.store(false); // stop the helper thread at end
                });
                double populate_frac = 0.5 / (double) (params.node_count * (params.thread_count - 2));

                StatusVal<WorkloadDriverResult> output = client_t::Run(std::move(client), thread_index, populate_frac);
                REMUS_ASSERT(output.status.t == StatusType::Ok && output.val.has_value(), "Client run failed");
                workload_results[thread_index] = output.val.value();
            }

            // Check expected size
            int all_delta = 0;
            map_reduce(endpoint, params, delta, std::function<void(uint64_t)>([&](uint64_t d){
                all_delta += d;
            }));

            // add count after syncing via endpoint exchange
            if (thread_index == 0){
                int final_size = sk->count(pool);
                REMUS_INFO("Size (after populate) [{}]", populate_amount);
                REMUS_INFO("Size (final) [{}]", final_size);
                REMUS_INFO("Delta = {}", all_delta);
                // debug print if everything is local for inspection? and is small enough
                if (params.node_count == 1 && (params.key_ub - params.key_lb) < 2000) sk->debug();
                REMUS_ASSERT(final_size - all_delta == 0, "Initial size + delta ==? Final size");
            }

            ExperimentManager::ClientArriveBarrier(endpoint);
            REMUS_INFO("[{} THREAD] -- End of execution; -- ", thread_index == helper_tidx ? "HELPER" : "CLIENT");
            cache->print_metrics(thread_index == helper_tidx ? "Helper -> " : "");
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

    save_result("multi_result.csv", workload_results, params, params.thread_count - 2);
}

inline void multi_run_local(Peer& self){
    using Node = node<int, MAX_HEIGHT_MK>;
    using MultiListLocal = RdmaMultiList<int, MAX_HEIGHT_MK, INT_MIN, ULONG_MAX, ULONG_MAX - 1, CountingPool>;
    CountingPool* pool = new CountingPool(true);
    RemoteCacheImpl<CountingPool>* cach = new RemoteCacheImpl<CountingPool>(pool, 0);
    RemoteCacheImpl<CountingPool>::pool = pool; // set pool to other pool so we acccept our own cacheline

    static const int KEY_LB = 0;
    static const int KEY_UB = 100;

    if (true){
        BenchmarkParams params = BenchmarkParams();
        params.cache_depth = (CacheDepth::CacheDepth) 4;
        params.contains = 98;
        params.insert = 1;
        params.remove = 1;
        params.key_lb = 0;
        params.key_ub = 10000;
        params.node_count = 1;
        params.node_id = 0;
        params.thread_count = 3;
        params.op_count = 10000;
        params.runtime = 1;
        params.qp_per_conn = 1;
        params.structure = "multi";
        params.unlimited_stream = false;
        params.region_size = 28;
        params.distribution = "uniform";
        Peer host = self;
        vector<Peer> peers = {};
        multi_run_tmp(params, pool, cach, host, self, peers);
        return;
    }

    EBRObjectPool<Node, 100, CountingPool>* ebr = new EBRObjectPool<Node, 100, CountingPool>(pool, 3);
    // ebr->Init(rdma_capability *two_sided_pool, int node_id, vector<Peer> peers);
    // would use to connect to remote peers
    MultiListLocal sk = MultiListLocal(self, 2, cach, pool, {self}, ebr);
    sk.set_key_range(KEY_LB, KEY_UB);
    rdma_ptr<anon_ptr> remove_as_node = sk.InitAsFirst(pool);
    vector<LimboLists<Node>*> qs;
    qs.push_back(ebr->RegisterThread());
    std::atomic<bool> do_cont;
    do_cont.store(true);
    std::thread t1 = std::thread([&](){
        RemoteCacheImpl<CountingPool>::pool = pool; // initialize the thread_local
        ebr->RegisterThread();
        sk.helper_thread(&do_cont, pool, ebr, qs);

        cach->free_all_tmp_objects();
    });
    // std::thread t2 = std::thread([&](){
    //     RemoteCacheImpl<CountingPool>::pool = pool; // initialize the thread_local
    //     vector<LimboLists<Node>*> qs_fake;
    //     qs_fake.push_back(ebr->RegisterThread());
    //     sk.helper_thread(&do_cont, pool, ebr, qs_fake);

    //     cach->free_all_tmp_objects();
    // });
    REMUS_INFO("DONE INIT");
    sk.populate(pool, 32, KEY_LB, KEY_UB, std::function([=](int x){ return x; }));

    int counter = 0;
    for(int i = KEY_LB; i < KEY_UB; i++){
        if (sk.contains(pool, i).has_value()) counter++;
    }
    int full_count = sk.count(pool);
    REMUS_INFO("matched_keys = {}", counter);
    REMUS_INFO("Count() = {}", full_count);
    sk.debug();

    /// Delete the data structure
    for(int i = KEY_LB; i < KEY_UB; i++){
        sk.remove(pool, i).value_or(-1);
    }

    this_thread::sleep_for(std::chrono::milliseconds(10)); // wait a second for helper thread to catch up

    do_cont.store(false);
    t1.join();
    // t2.join();
    // sk.debug();

    cach->free_all_tmp_objects();
    ebr->destroy(pool);
    sk.destroy(pool);
    pool->Deallocate(rdma_ptr<MultiListLocal::CachedStart>(remove_as_node.raw()), sk.list_n());
    delete cach;
    if (!pool->HasNoLeaks()){
        pool->debug();
        REMUS_FATAL("Leaked memory");
    } else {
        REMUS_INFO("No Leaks!");
    }
}