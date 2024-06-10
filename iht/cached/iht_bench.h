#include <barrier>
#include <protos/workloaddriver.pb.h>
#include <remus/logging/logging.h>
#include <remus/util/cli.h>
#include <remus/util/tcp/tcp.h>
#include <remus/rdma/memory_pool.h>
#include <remus/rdma/rdma.h>

// #include "map_cache.h"
#include "ds/iht_ds_cached.h"

#include "../common.h"
#include "../experiment.h"
#include "../tcp_barrier.h"
#include "../role_client.h"

#include <dcache/cache_store.h>

using namespace remus::util;

#define PORT_NUM_TCP 19000
// typedef RDMALinearProbingMap<int, int, MAXKEY> KVStore;
typedef RdmaIHT<int, int, CNF_ELIST_SIZE, CNF_PLIST_SIZE> KVStore;

inline void iht_run(BenchmarkParams& params, rdma_capability* capability, RemoteCache* cache, Peer& host, Peer& self){
    // Create a list of client and server  threads
    std::vector<std::thread> threads;
    if (params.node_id == 0){
        // If dedicated server-node, we must send IHT pointer and wait for clients to finish
        threads.emplace_back(std::thread([&](){
            auto pool = capability->RegisterThread();
            // Initialize X connections
            tcp::SocketManager* socket_handle = new tcp::SocketManager(PORT_NUM_TCP);
            for(int i = 0; i < params.thread_count * params.node_count; i++){
                // TODO: Can we have a per-node connection?
                // I haven't gotten around to coming up with a clean way to reduce the number of sockets connected to the server
                socket_handle->accept_conn();
            }

            // Collect and redistribute the CacheStore pointers
            tcp::message root_ptrs[params.node_count * params.thread_count];
            socket_handle->recv_from_all(root_ptrs);
            for(int i = 0; i < params.node_count * params.thread_count; i++){
                socket_handle->send_to_all(&root_ptrs[i]);
            }

            // Create a root ptr to the IHT
            Peer p = Peer();
            KVStore iht = KVStore(p, CacheDepth::None, nullptr, pool);
            rdma_ptr<anon_ptr> root_ptr = iht.InitAsFirst(pool);
            // Send the root pointer over
            tcp::message ptr_message = tcp::message(root_ptr.raw());
            socket_handle->send_to_all(&ptr_message);

            // Block until client is done, helping synchronize clients when they need
            ExperimentManager::ServerStopBarrier(socket_handle, 0); // before populate
            ExperimentManager::ServerStopBarrier(socket_handle, 0); // after populate
            ExperimentManager::ServerStopBarrier(socket_handle, 0); // after count
            ExperimentManager::ServerStopBarrier(socket_handle, params.runtime); // after operations

            // Collect and redistribute the size deltas
            tcp::message deltas[params.node_count * params.thread_count];
            socket_handle->recv_from_all(deltas);
            for(int i = 0; i < params.node_count * params.thread_count; i++){
                socket_handle->send_to_all(&deltas[i]);
            }

            // Wait until clients are done with correctness exchange (they all run count afterwards)
            ExperimentManager::ServerStopBarrier(socket_handle, 0);
            delete socket_handle;
            REMUS_INFO("[SERVER THREAD] -- End of execution; -- ");
        }));
    }

    // Initialize T endpoints, one for each thread
    tcp::EndpointManager* endpoint_managers[params.thread_count];
    for(uint16_t i = 0; i < params.thread_count; i++){
        endpoint_managers[i] = new tcp::EndpointManager(PORT_NUM_TCP, host.address.c_str());
    }
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
            tcp::message data(cache->root());
            vector<uint64_t> peer_roots;
            endpoint->send_server(&data);
            for(int i = 0; i < params.node_count * params.thread_count; i++){
                endpoint->recv_server(&data);
                peer_roots.push_back(data.get_first());
            }
            cache->init(peer_roots);

            std::shared_ptr<KVStore> iht = std::make_shared<KVStore>(self, params.cache_depth, cache, pool);
            // Get the data from the server to init the IHT
            tcp::message ptr_message;
            endpoint->recv_server(&ptr_message);
            iht->InitFromPointer(rdma_ptr<anon_ptr>(ptr_message.get_first()));

            REMUS_DEBUG("Creating client");
            // Create and run a client in a thread
            int delta = 0;
            int populate_amount = 0;
            MapAPI* iht_as_map = new MapAPI(
                [&](int key, int value){
                    auto res = iht->insert(pool, key, value);
                    if (res == std::nullopt) delta++;
                    return res;
                },
                [&](int key){ return iht->contains(pool, key); },
                [&](int key){
                    auto res = iht->remove(pool, key);
                    if (res != std::nullopt) delta--;
                    return res;
                },
                [&](int op_count, int key_lb, int key_ub){
                    // capability->RegisterThread();
                    ExperimentManager::ClientArriveBarrier(endpoint);
                    delta += iht->populate(pool, op_count, key_lb, key_ub, [=](int key){ return key; });
                    ExperimentManager::ClientArriveBarrier(endpoint);
                    populate_amount = iht->count(pool); // ? IMPORTANT - Count hits every element which in effect warms up the cache
                                      // ? BENCHMARK EXECUTION STARTS WITH NO INVALID CACHE LINES
                    ExperimentManager::ClientArriveBarrier(endpoint);
                    cache->print_metrics();
                    cache->reset_metrics();
                }
            );
            // todo: for LinearProbingMap only REMUS_ASSERT(params.key_lb >= 2 && params.key_ub - params.key_lb <= 1000, "Keyspace is valid?");
            using client_t = Client<IHT_Op<int, int>>;
            std::unique_ptr<client_t> client = client_t::Create(host, endpoint, params, &client_sync, iht_as_map);
            double populate_frac = 0.5 / (double) (params.node_count * params.thread_count);

            StatusVal<WorkloadDriverResult> output = client_t::Run(std::move(client), thread_index, populate_frac);
            REMUS_ASSERT(output.status.t == StatusType::Ok && output.val.has_value(), "Client run failed");
            workload_results[thread_index] = output.val.value();

            // Check expected size
            int all_delta = 0;
            tcp::message d(delta);
            endpoint->send_server(&d);
            for(int i = 0; i < params.node_count * params.thread_count; i++){
                endpoint->recv_server(&d);
                all_delta += d.get_first();
            }
            // add count after syncing via endpoint exchange
            int final_size = iht->count(pool);
            REMUS_DEBUG("Size (after populate) [{}]", populate_amount);
            REMUS_DEBUG("Size (final) [{}]", final_size);
            REMUS_DEBUG("Delta = {}", all_delta);
            REMUS_ASSERT(final_size - all_delta == 0, "Initial size + delta ==? Final size");
            
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
    for(uint16_t i = 0; i < params.thread_count; i++){
        delete endpoint_managers[i];
    }
    Result result[params.thread_count];
    for (int i = 0; i < params.thread_count; i++) {
        result[i] = Result(params, workload_results[i]);
        REMUS_INFO("Protobuf Result {}\n{}", i, result[i].result_as_debug_string());
    }

    std::ofstream filestream("iht_result.csv");
    filestream << Result::result_as_string_header();
    for (int i = 0; i < params.thread_count; i++) {
        filestream << result[i].result_as_string();
    }
    filestream.close();
}