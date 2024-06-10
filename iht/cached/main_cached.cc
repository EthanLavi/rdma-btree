#include <protos/workloaddriver.pb.h>
#include <remus/logging/logging.h>
#include <remus/util/cli.h>
#include <remus/util/tcp/tcp.h>
#include <remus/rdma/memory_pool.h>
#include <remus/rdma/rdma.h>

// #include "map_cache.h"

#include "../experiment.h"
#include "iht_bench.h"

#include <dcache/cache_store.h>

using namespace remus::util;

auto ARGS = {
    I64_ARG("--node_id", "The node's id. (nodeX in cloudlab should have X in this option)"),
    I64_ARG("--runtime", "How long to run the experiment for. Only valid if unlimited_stream"),
    BOOL_ARG_OPT("--unlimited_stream", "If the stream should be endless, stopping after runtime"),
    I64_ARG("--op_count", "How many operations to run. Only valid if not unlimited_stream"),
    I64_ARG("--region_size", "How big the region should be in 2^x bytes"),
    I64_ARG("--thread_count", "How many threads to spawn with the operations"),
    I64_ARG("--node_count", "How many nodes are in the experiment"),
    I64_ARG("--qp_per_conn", "The max number of queue pairs to allocate for the experiment."),
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
    int mp = std::min(params.thread_count, params.qp_per_conn);
    if (mp == 0) mp = 1; // Make sure if thread_count > qp_per_conn, we don't end up with 0 memory pools

    REMUS_INFO("Distributing {} MemoryPools across {} threads", mp, params.thread_count);

    // Start initializing a vector of peers
    std::vector<Peer> peers;
    for(uint16_t n = 0; n < params.node_count; n++){
        // Create the ip_peer (really just node name)
        std::string node_id = std::to_string((int) n);
        // Create the peer and add it to the list
        Peer next = Peer(n, std::string("node") + node_id, PORT_NUM + n + 1);
        peers.push_back(next);
        REMUS_INFO("Peer list {}:{}@{}", n, next.id, next.address);
    }
    Peer host = peers.at(0);
    Peer self = peers.at(params.node_id);
    // Initialize a rdma_capability
    rdma_capability* capability = new rdma_capability(self, mp);
    uint32_t block_size = 1 << params.region_size;
    capability->init_pool(block_size, peers);

    // Create our remote cache (can initialize the cache space with any pool)
    auto pool = capability->RegisterThread();
    RemoteCache* cache = new RemoteCache(pool, 1000);

    iht_run(params, capability, cache, host, self);
    
    REMUS_INFO("[EXPERIMENT] -- End of execution; -- ");
    // todo: deleting cache/pools needs to work
    // delete cache;
    // for(int i = 0; i < mp; i++){
        // delete pools[mp];
    // }
    return 0;
}
