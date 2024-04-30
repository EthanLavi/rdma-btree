#include <vector>

#include <protos/workloaddriver.pb.h>
#include <remus/logging/logging.h>
#include <remus/util/cli.h>
#include <remus/util/tcp/tcp.h>
#include <remus/rdma/memory_pool.h>
#include <remus/rdma/rdma.h>

// #include "map_cache.h"
#include "iht_ds_cached.h"

#include "../common.h"
#include "../experiment.h"
#include "../role_client.h"
#include "../tcp_barrier.h"

#include "../../dcache/include/cache_store.h"

using namespace remus::util;

auto ARGS = {
    I64_ARG("--node_id", "The node's id. (nodeX in cloudlab should have X in this option)"),
    I64_ARG("--runtime", "How long to run the experiment for. Only valid if unlimited_stream"),
    BOOL_ARG_OPT("--unlimited_stream", "If the stream should be endless, stopping after runtime"),
    I64_ARG("--op_count", "How many operations to run. Only valid if not unlimited_stream"),
    I64_ARG("--region_size", "How big the region should be in 2^x bytes"),
    I64_ARG("--thread_count", "How many threads to spawn with the operations"),
    I64_ARG("--node_count", "How many nodes are in the experiment"),
    I64_ARG("--qp_max", "The max number of queue pairs to allocate for the experiment."),
    I64_ARG("--contains", "Percentage of operations are contains, (contains + insert + remove = 100)"),
    I64_ARG("--insert", "Percentage of operations are inserts, (contains + insert + remove = 100)"),
    I64_ARG("--remove", "Percentage of operations are removes, (contains + insert + remove = 100)"),
    I64_ARG("--key_lb", "The lower limit of the key range for operations"),
    I64_ARG("--key_ub", "The upper limit of the key range for operations"),
    I64_ARG_OPT("--cache_depth", "The depth of the cache for the IHT", 0),
};

#define PATH_MAX 4096
#define PORT_NUM 18000

// The optimial number of memory pools is mp=min(t, MAX_QP/n) where n is the number of nodes and t is the number of threads
// To distribute mp (memory pools) across t threads, it is best for t/mp to be a whole number
// IHT RDMA MINIMAL

#define MAXKEY 1000
// typedef RDMALinearProbingMap<int, int, MAXKEY> KVStore;
typedef RdmaIHT<int, int, CNF_ELIST_SIZE, CNF_PLIST_SIZE> KVStore;

int main(int argc, char **argv) {
    REMUS_INIT_LOG();

    ArgMap args;
    // import_args will validate that the newly added args don't conflict with
    // those already added.
    auto res = args.import_args(ARGS);
    if (res) {
        REMUS_FATAL(res.value());
    }
    // NB: Only call parse_args once.  If it fails, a mandatory arg was skipped
    res = args.parse_args(argc, argv);
    if (res) {
        args.usage();
        REMUS_FATAL(res.value());
    }

    // Extract the args to variables
    BenchmarkParams params = BenchmarkParams(args);
    REMUS_INFO("Running IHT with cache depth {}", params.cache_depth);

    // Check node count
    if (params.node_count <= 0 || params.thread_count <= 0){
        REMUS_FATAL("Cannot start experiment. Node/thread count was found to be 0");
    }
    // Check we are in this experiment
    if (params.node_id >= params.node_count){
        REMUS_INFO("Not in this experiment. Exiting");
        exit(0);
    }

    // Determine the number of memory pools to use in the experiment
    // Each memory pool represents 
    int mp = std::min(params.thread_count, (int) std::floor(params.qp_max / params.node_count));
    if (mp == 0) mp = 1; // Make sure if node_count > qp_max, we don't end up with 0 memory pools
    
    REMUS_INFO("Distributing {} MemoryPools across {} threads", mp, params.thread_count);

    // Start initializing a vector of peers
    std::vector<Peer> peers;
    for(uint16_t n = 0; n < mp * params.node_count; n++){
        // Create the ip_peer (really just node name)
        std::string ippeer = "node";
        std::string node_id = std::to_string((int) n / mp);
        ippeer.append(node_id);
        // Create the peer and add it to the list
        Peer next = Peer(n, ippeer, PORT_NUM + n + 1);
        peers.push_back(next);
    }
    // Print the peers included in this experiment
    // This is just for debugging to ensure they are what you expect
    for(int i = 0; i < peers.size(); i++){
        REMUS_DEBUG("Peer list {}:{}@{}", i, peers.at(i).id, peers.at(i).address);
    }
    Peer host = peers.at(0);
    // Initialize memory pools into an array
    std::vector<std::thread> mempool_threads;
    rdma_capability* pools[mp];
    // Create multiple memory pools to be shared (have to use threads since Init is blocking)
    uint32_t block_size = 1 << params.region_size;
    for(int i = 0; i < mp; i++){
        mempool_threads.emplace_back(std::thread([&](int mp_index, int self_index){
            Peer self = peers.at(self_index);
            REMUS_DEBUG(mp != params.thread_count ? "Is shared" : "Is not shared");
            rdma_capability* pool = new rdma_capability(self);
            pool->init_pool(block_size, peers);
            pools[mp_index] = pool;
        }, i, (params.node_id * mp) + i));
    }
    // Let the init finish
    for(int i = 0; i < mp; i++){
        mempool_threads[i].join();
    }

    // Create a list of client and server  threads
    std::vector<std::thread> threads;
    if (params.node_id == 0){
        // If dedicated server-node, we must send IHT pointer and wait for clients to finish
        threads.emplace_back(std::thread([&](){
            // Initialize X connections
            tcp::SocketManager* socket_handle = new tcp::SocketManager(PORT_NUM);
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
            KVStore iht = KVStore(p, CacheDepth::None, nullptr, pools[0]);
            rdma_ptr<anon_ptr> root_ptr = iht.InitAsFirst(pools[0]);
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
        endpoint_managers[i] = new tcp::EndpointManager(PORT_NUM, host.address.c_str());
    }
    // sleep for a short while to ensure the receiving end (SocketManager) is up and running
    // If the endpoint cant connect, it will just wait and retry later
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Create our remote cache (can initialize the cache space with any pool)
    RemoteCache* cache = new RemoteCache(pools[0], 1000);

    // Barrier to start all the clients at the same time
    std::barrier client_sync = std::barrier(params.thread_count);
    WorkloadDriverResult workload_results[params.thread_count];
    for(int i = 0; i < params.thread_count; i++){
        threads.emplace_back(std::thread([&](int thread_index){
            // Get pool
            int mempool_index = thread_index % mp;
            rdma_capability* pool = pools[mempool_index];

            // initialize thread's thread_local pool
            RemoteCache::pool = pool; 
            // Exchange the root pointer of the other cache stores via TCP module
            tcp::message data(cache->root());
            vector<uint64_t> peer_roots;
            endpoint_managers[thread_index]->send_server(&data);
            for(int i = 0; i < params.node_count * params.thread_count; i++){
                endpoint_managers[thread_index]->recv_server(&data);
                peer_roots.push_back(data.get_first());
            }
            cache->init(peer_roots);

            Peer self = peers.at((params.node_id * mp) + mempool_index);
            std::shared_ptr<KVStore> iht = std::make_shared<KVStore>(self, params.cache_depth, cache, pool);
            // Get the data from the server to init the IHT
            tcp::message ptr_message;
            endpoint_managers[thread_index]->recv_server(&ptr_message);
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
                    pool->RegisterThread();
                    ExperimentManager::ClientArriveBarrier(endpoint_managers[thread_index]);
                    delta += iht->populate(pool, op_count, key_lb, key_ub, [=](int key){ return key; });
                    ExperimentManager::ClientArriveBarrier(endpoint_managers[thread_index]);
                    populate_amount = iht->count(pool); // ? IMPORTANT - Count hits every element which in effect warms up the cache
                                      // ? BENCHMARK EXECUTION STARTS WITH NO INVALID CACHE LINES
                    ExperimentManager::ClientArriveBarrier(endpoint_managers[thread_index]);
                    cache->print_metrics();
                    cache->reset_metrics();
                }
            );
            // todo: for LinearProbingMap only REMUS_ASSERT(params.key_lb >= 2 && params.key_ub - params.key_lb <= 1000, "Keyspace is valid?");
            std::unique_ptr<Client<IHT_Op<int, int>>> client = Client<IHT_Op<int, int>>::Create(host, endpoint_managers[thread_index], params, &client_sync, iht_as_map);
            double populate_frac = 0.5 / (double) (params.node_count * params.thread_count);

            StatusVal<WorkloadDriverResult> output = Client<IHT_Op<int, int>>::Run(std::move(client), thread_index, populate_frac);
            if (output.status.t == StatusType::Ok && output.val.has_value()){
                workload_results[thread_index] = output.val.value();
            } else {
                REMUS_ERROR("Client run failed");
            }

            // Check expected size
            int all_delta = 0;
            tcp::message d(delta);
            endpoint_managers[thread_index]->send_server(&d);
            for(int i = 0; i < params.node_count * params.thread_count; i++){
                endpoint_managers[thread_index]->recv_server(&d);
                all_delta += d.get_first();
            }
            // add count after syncing via endpoint exchange
            int final_size = iht->count(pool);
            REMUS_DEBUG("Size (after populate) [{}]", populate_amount);
            REMUS_DEBUG("Size (final) [{}]", final_size);
            REMUS_DEBUG("Delta = {}", all_delta);
            REMUS_ASSERT(final_size - all_delta == 0, "Initial size + delta ==? Final size");
            
            ExperimentManager::ClientArriveBarrier(endpoint_managers[thread_index]);
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
    REMUS_INFO("[EXPERIMENT] -- End of execution; -- ");
    // todo: deleting cache/pools needs to work
    // delete cache;
    // for(int i = 0; i < mp; i++){
        // delete pools[mp];
    // }
    return 0;
}
