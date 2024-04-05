#include <protos/workloaddriver.pb.h>
#include <vector>

#include <remus/logging/logging.h>
#include <remus/util/cli.h>

#include "common.h"
#include "experiment.h"
#include "role_client.h"
#include "role_server.h"
#include "rpc.h"

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

// Copied from memory pool, cm initialization should be apart of cm class, not in memory pool
StatusVal<unordered_map<int, Connection*>> init_cm(ConnectionManager* cm, Peer self, const vector<Peer>& peers){
    auto status = cm->Start(self.address, self.port);
    RETURN_STATUSVAL_FROM_ERROR(status);
    REMUS_INFO("Starting with {}", self.address);
    // Go through the list of peers and connect to each of them
    for (const auto &p : peers) {
        REMUS_INFO("Init with {}", p.address);
        auto connected = cm->Connect(p.id, p.address, p.port);
        while (connected.status.t == Unavailable) {
            connected = cm->Connect(p.id, p.address, p.port);
        }
        RETURN_STATUSVAL_FROM_ERROR(connected.status);
        REMUS_INFO("Init done with {}", p.address);
    }

    // Test out the connection (receive)
    AckProto rm_proto;
    for (const auto &p : peers) {
      auto conn = cm->GetConnection(p.id);
      STATUSVAL_OR_DIE(conn);
      status = conn.val.value()->channel()->Send(rm_proto);
      RETURN_STATUSVAL_FROM_ERROR(status);
    }

    unordered_map<int, Connection*> connections;

    // Test out the connection (deliver)
    for (const auto &p : peers) {
      auto conn = cm->GetConnection(p.id);
      STATUSVAL_OR_DIE(conn);
      auto got =
          conn.val.value()->channel()->template Deliver<AckProto>();
      RETURN_STATUSVAL_FROM_ERROR(got.status);
      connections[p.id] = conn.val.value();
    }
    return {Status::Ok(), connections};
}

int main(int argc, char **argv) {
    REMUS_INIT_LOG();
    REMUS_INFO("Running twosided");

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

    // Start initializing a vector of peers
    // We have separate versions for the vector (with only different in ports used) in order to create two connection managers
    std::vector<Peer> recvs;
    std::vector<Peer> sends;
    Peer self_sender;
    Peer self_receiver;
    for(uint16_t n = 0; n < params.node_count; n++){
        // Create the ip_peer (really just node name)
        std::string ippeer = "node";
        std::string node_id = std::to_string(n);
        ippeer.append(node_id);
        // Create the peer and add it to the list
        Peer send_next = Peer(n, ippeer, PORT_NUM + n + 1);
        Peer recv_next = Peer(n, ippeer, PORT_NUM + n + 1001);
        if (n == params.node_id) {
            self_sender = send_next;
            self_receiver = recv_next;
        }
        sends.push_back(send_next);
        recvs.push_back(recv_next);
    }
    Peer host = sends.at(0); // portnum doesn't matter so we can get either from sends or recvs

    ConnectionManager* sender = new ConnectionManager(self_sender.id);
    ConnectionManager* receiver = new ConnectionManager(self_receiver.id);
    StatusVal<unordered_map<int, Connection*>> s1 = init_cm(sender, self_sender, sends);
    REMUS_ASSERT(s1.status.t == Ok, "Connection manager 1 was setup incorrectly");
    unordered_map<int, Connection*> sender_map = s1.val.value();
    StatusVal<unordered_map<int, Connection*>> s2 = init_cm(receiver, self_receiver, recvs);
    REMUS_ASSERT(s2.status.t == Ok, "Connection manager 2 was setup incorrectly");
    unordered_map<int, Connection*> receiver_map = s2.val.value();
    REMUS_INFO("Init 2 cms!");

    TwoSidedIHT* iht = new TwoSidedIHT(params.node_id, params.node_count, params.key_lb, params.key_ub, sender_map, receiver_map);
    REMUS_INFO("Init an iht");

    // ------------------------------------------------------------------------------------------- //
    // Create a list of client and server  threads
    std::vector<std::thread> threads;
    if (params.node_id == 0){
        // If dedicated server-node, we are responsible for sync-ing everyone at the end
        threads.emplace_back(std::thread([&](){
            // Initialize X connections
            tcp::SocketManager socket_handle = tcp::SocketManager(PORT_NUM);
            for(int i = 0; i < params.thread_count * params.node_count; i++){
                // TODO: Can we have a per-node connection?
                // I haven't gotten around to coming up with a clean way to reduce the number of sockets connected to the server
                socket_handle.accept_conn();
            }
            // We are the server
            ExperimentManager::ClientStopBarrier(socket_handle, params.runtime);
            REMUS_INFO("[SERVER THREAD] -- End of execution; -- ");
        }));
    }

    // Initialize T endpoints, one for each thread
    tcp::EndpointManager endpoint_managers[params.thread_count];
    for(uint16_t i = 0; i < params.thread_count; i++){
        endpoint_managers[i] = tcp::EndpointManager(PORT_NUM, host.address.c_str());
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
    WorkloadDriverResult workload_results[params.thread_count];
    for(int i = 0; i < params.thread_count; i++){
        threads.emplace_back(std::thread([&](int thread_index){
            // sleep for a short while to ensure the receiving end (SocketManager) is up and running
            // If the endpoint cant connect, it will just wait and retry later
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            
            REMUS_DEBUG("Creating client");
            // Create and run a client in a thread
            MapAPI* iht_as_map = new MapAPI(
                [&](int key, int value){ return iht->insert(key, value); },
                [&](int key){ return iht->get(key); },
                [&](int key){ return iht->remove(key); },
                [&](int op_count, int key_lb, int key_ub){ iht->populate(op_count, key_lb, key_ub, [=](int key){ return key; }); }
            );
            std::unique_ptr<Client<IHT_Op<int, int>>> client = Client<IHT_Op<int, int>>::Create(host, endpoint_managers[thread_index], params, &client_sync, iht_as_map);
            double populate_frac = 0.5 / (double) (params.node_count * params.thread_count);
            StatusVal<WorkloadDriverResult> output = Client<IHT_Op<int, int>>::Run(std::move(client), thread_index, populate_frac);
            // [mfs]  It would be good to document how a client can fail, because
            // it seems like if even one client fails, on any machine, the
            //  whole experiment should be invalidated.
            // [esl] I agree. A strange thing though: I think the output of Client::Run is always OK.
            //       Any errors just crash the script, which lead to no results being generated?
            if (output.status.t == StatusType::Ok && output.val.has_value()){
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
    // make sure iht can be deleted before ending the experiment
    delete iht;
    REMUS_INFO("[EXPERIMENT] -- End of execution; -- ");

    return 0;
}
