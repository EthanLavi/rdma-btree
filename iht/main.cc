#include <vector>

#include <protos/workloaddriver.pb.h>
#include <remus/logging/logging.h>
#include <remus/util/cli.h>
#include <remus/util/tcp/tcp.h>
#include <remus/rdma/memory_pool.h>
#include <remus/rdma/rdma.h>

#include "common.h"
#include "experiment.h"
#include "role_client.h"
#include "tcp_barrier.h"
#include "iht_ds.h"

auto ARGS = {
    remus::util::I64_ARG("--node_id", "The node's id. (nodeX in cloudlab should have X in this option)"),
    remus::util::I64_ARG("--runtime", "How long to run the experiment for. Only valid if unlimited_stream"),
    remus::util::BOOL_ARG_OPT("--unlimited_stream", "If the stream should be endless, stopping after runtime"),
    remus::util::I64_ARG("--op_count", "How many operations to run. Only valid if not unlimited_stream"),
    remus::util::I64_ARG("--region_size", "How big the region should be in 2^x bytes"),
    remus::util::I64_ARG("--thread_count", "How many threads to spawn with the operations"),
    remus::util::I64_ARG("--node_count", "How many nodes are in the experiment"),
    remus::util::I64_ARG("--qp_max", "The max number of queue pairs to allocate for the experiment."),
    remus::util::I64_ARG("--contains", "Percentage of operations are contains, (contains + insert + remove = 100)"),
    remus::util::I64_ARG("--insert", "Percentage of operations are inserts, (contains + insert + remove = 100)"),
    remus::util::I64_ARG("--remove", "Percentage of operations are removes, (contains + insert + remove = 100)"),
    remus::util::I64_ARG("--key_lb", "The lower limit of the key range for operations"),
    remus::util::I64_ARG("--key_ub", "The upper limit of the key range for operations"),
    remus::util::I64_ARG_OPT("--cache_depth", "The depth of the cache for the IHT", 0),
};

#define PATH_MAX 4096
#define PORT_NUM 18000

using namespace remus::rdma;

// The optimial number of memory pools is mp=min(t, MAX_QP/n) where n is the number of nodes and t is the number of threads
// To distribute mp (memory pools) across t threads, it is best for t/mp to be a whole number
// IHT RDMA MINIMAL

typedef RdmaIHT<int, int, CNF_ELIST_SIZE, CNF_PLIST_SIZE> IHT;

int main(int argc, char **argv) {
    REMUS_INIT_LOG();

    remus::util::ArgMap args;
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
    std::shared_ptr<rdma_capability> pools[mp];
    // Create multiple memory pools to be shared (have to use threads since Init is blocking)
    uint32_t block_size = 1 << params.region_size;
    for(int i = 0; i < mp; i++){
        mempool_threads.emplace_back(std::thread([&](int mp_index, int self_index){
            Peer self = peers.at(self_index);
            REMUS_DEBUG(mp != params.thread_count ? "Is shared" : "Is not shared");
            std::shared_ptr<rdma_capability> pool = std::make_shared<rdma_capability>(self);
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
            remus::util::tcp::SocketManager* socket_handle = new remus::util::tcp::SocketManager(PORT_NUM);
            for(int i = 0; i < params.thread_count * params.node_count; i++){
                // TODO: Can we have a per-node connection?
                // I haven't gotten around to coming up with a clean way to reduce the number of sockets connected to the server
                socket_handle->accept_conn();
            }
            // Create a root ptr to the IHT
            IHT iht = IHT(host, params.cache_depth, pools[0]);
            rdma_ptr<anon_ptr> root_ptr = iht.InitAsFirst(pools[0]);
            // Send the root pointer over
            remus::util::tcp::message ptr_message = remus::util::tcp::message(root_ptr.raw());
            socket_handle->send_to_all(&ptr_message);
            // We are the server
            ExperimentManager::ServerStopBarrier(socket_handle, params.runtime);
            REMUS_INFO("[SERVER THREAD] -- End of execution; -- ");
        }));
    }

    // Initialize T endpoints, one for each thread
    remus::util::tcp::EndpointManager* endpoint_managers[params.thread_count];
    for(uint16_t i = 0; i < params.thread_count; i++){
        endpoint_managers[i] = new remus::util::tcp::EndpointManager(PORT_NUM, host.address.c_str());
    }

    // Barrier to start all the clients at the same time
    //
    // [mfs]  This starts all the clients *on this thread*, but that's not really
    //        a sufficient barrier.  A tree barrier is needed, to coordinate
    //        across nodes.
    // [esl]  Is this something that would need to be implemented using rome?
    //        I'm not exactly sure what the implementation of an RDMA barrier would look like. If you have one in mind, lmk and I can start working on it.
    std::barrier client_sync = std::barrier(params.thread_count);
    // [mfs]  This seems like a misuse of protobufs: why would the local threads
    //        communicate via protobufs?
    // [esl]  Protobufs were a pain to code with. I think the ClientAdaptor returns a protobuf and I never understood why it didn't just return an object. 
    // TODO:  In the refactoring of the client adaptor, remove dependency on protobufs for a workload object
    WorkloadDriverResult workload_results[params.thread_count];
    for(int i = 0; i < params.thread_count; i++){
        threads.emplace_back(std::thread([&](int thread_index){
            int mempool_index = thread_index % mp;
            std::shared_ptr<rdma_capability> pool = pools[mempool_index];
            Peer self = peers.at((params.node_id * mp) + mempool_index);
            std::shared_ptr<IHT> iht = std::make_shared<IHT>(self, params.cache_depth, pool);
            // sleep for a short while to ensure the receiving end (SocketManager) is up and running
            // If the endpoint cant connect, it will just wait and retry later
            std::this_thread::sleep_for(std::chrono::milliseconds(10)); 
            // Get the data from the server to init the IHT
            remus::util::tcp::message ptr_message;
            endpoint_managers[thread_index]->recv_server(&ptr_message);
            iht->InitFromPointer(rdma_ptr<anon_ptr>(ptr_message.get_first()));
            
            REMUS_DEBUG("Creating client");
            // Create and run a client in a thread
            MapAPI* iht_as_map = new MapAPI(
                [&](int key, int value){ return iht->insert(pool, key, value); },
                [&](int key){ return iht->contains(pool, key); },
                [&](int key){ return iht->remove(pool, key); },
                [&](int op_count, int key_lb, int key_ub){ 
                    pool->RegisterThread();
                    iht->populate(pool, op_count, key_lb, key_ub, [=](int key){ return key; }); }
            );
            std::unique_ptr<Client<IHT_Op<int, int>>> client = Client<IHT_Op<int, int>>::Create(host, endpoint_managers[thread_index], params, &client_sync, iht_as_map);
            double populate_frac = 0.5 / (double) (params.node_count * params.thread_count);
            remus::util::StatusVal<WorkloadDriverResult> output = Client<IHT_Op<int, int>>::Run(std::move(client), thread_index, populate_frac);
            // [mfs]  It would be good to document how a client can fail, because
            // it seems like if even one client fails, on any machine, the
            //  whole experiment should be invalidated.
            // [esl] I agree. A strange thing though: I think the output of Client::Run is always OK.
            //       Any errors just crash the script, which lead to no results being generated?
            if (output.status.t == remus::util::StatusType::Ok && output.val.has_value()){
                workload_results[thread_index] = output.val.value();
            } else {
                REMUS_ERROR("Client run failed");
            }
            REMUS_INFO("[CLIENT THREAD] -- End of execution; -- ");
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
    for(int i = 0; i < params.thread_count; i++){
        delete endpoint_managers[i];
    }
    // [mfs]  Again, odd use of protobufs for relatively straightforward combining
    //        of results.  Or am I missing something, and each node is sending its
    //        results, so they are all accumulated at the main node?
    // [esl]  Each thread will create a result proto. The result struct will parse this and save it in a csv which the launch script can scp.
    Result result[params.thread_count];
    for (int i = 0; i < params.thread_count; i++) {
        result[i] = Result(params, workload_results[i]);
        REMUS_INFO("Protobuf Result {}\n{}", i, result[i].result_as_debug_string());
    }

    // [mfs] Does this produce one file per node?
    // [esl] Yes, this produces one file per node, 
    //       The launch.py script will scp this file and use the protobuf to interpret it
    std::ofstream filestream("iht_result.csv");
    filestream << Result::result_as_string_header();
    for (int i = 0; i < params.thread_count; i++) {
        filestream << result[i].result_as_string();
    }
    filestream.close();
    REMUS_INFO("[EXPERIMENT] -- End of execution; -- ");
    return 0;
}
