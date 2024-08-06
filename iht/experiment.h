#pragma once

#include <remus/util/cli.h>
#include <remus/workload/workload_driver.h>
#include <remus/logging/logging.h>

#include "common.h"

using namespace remus::util;
using namespace remus::metrics;

/// An object to hold the experimental params
/// @param node_id The node's id. (nodeX in cloudlab should have X in this option)
/// @param runtime How long to run the experiment for. Only valid if unlimited_stream
/// @param unlimited_stream If the stream should be endless, stopping after runtime
/// @param op_count How many operations to run. Only valid if not unlimited_stream
/// @param region_size How big the region should be in 2^x bytes
/// @param thread_count How many threads to spawn with the operations
/// @param node_count How many nodes are in the experiment
/// @param qp_per_conn The max number of queue pairs to allocate for the experiment.
/// @param contains Percentage of operations are contains, (contains + insert + remove = 100)
/// @param insert Percentage of operations are inserts, (contains + insert + remove = 100)
/// @param remove Percentage of operations are removes, (contains + insert + remove = 100)
/// @param key_lb The lower limit of the key range for operations
/// @param key_ub The upper limit of the key range for operations
class BenchmarkParams {
public:
    /// The node's id. (nodeX in cloudlab should have X in this option)
    int node_id;
    /// How long to run the experiment for. Only valid if unlimited_stream
    int runtime;
    /// If the stream should be endless, stopping after runtime
    bool unlimited_stream;
    /// How many operations to run. Only valid if not unlimited_stream
    int op_count;
    /// How big the region should be in 2^x bytes
    int region_size;
    /// How many threads to spawn with the operations
    int thread_count;
    /// How many nodes are in the experiment
    int node_count;
    /// The max number of queue pairs to allocate for the experiment.
    int qp_per_conn;
    /// Percentage of operations are contains, (contains + insert + remove = 100)
    int contains;
    //. Percentage of operations are inserts, (contains + insert + remove = 100)
    int insert;
    /// Percentage of operations are removes, (contains + insert + remove = 100)
    int remove;
    /// The lower limit of the key range for operations
    int key_lb;
    /// The upper limit of the key range for operations
    int key_ub;
    /// The structure type
    std::string structure;

    /// The cache depth of the data structure
    CacheDepth::CacheDepth cache_depth;

    BenchmarkParams() = default;

    BenchmarkParams(ArgMap args){
        node_id = args.iget("--node_id");
        runtime = args.iget("--runtime");
        unlimited_stream = args.bget("--unlimited_stream");
        op_count = args.iget("--op_count");
        region_size = args.iget("--region_size");
        thread_count = args.iget("--thread_count");
        node_count = args.iget("--node_count");
        qp_per_conn = args.iget("--qp_per_conn");
        contains = args.iget("--contains");
        insert = args.iget("--insert");
        remove = args.iget("--remove");
        key_lb = args.iget("--key_lb");
        key_ub = args.iget("--key_ub");
        structure = args.sget("--structure");
        int depth = args.iget("--cache_depth");
        switch (depth) {
            case CacheDepth::None:
                cache_depth = CacheDepth::None;
                break;
            case CacheDepth::RootOnly:
                cache_depth = CacheDepth::RootOnly;
                break;
            case CacheDepth::UpToLayer1:
                cache_depth = CacheDepth::UpToLayer1;
                break;
            case CacheDepth::UpToLayer2:
                cache_depth = CacheDepth::UpToLayer2;
                break;
            case CacheDepth::UpToLayer3:
                cache_depth = CacheDepth::UpToLayer3;
                break;
            default:
                cache_depth = (CacheDepth::CacheDepth) depth;
                REMUS_WARN("Unknown cache depth. Defaulting to {}", cache_depth);
                break;
        }
    }
};

class Result {
public:
    BenchmarkParams params;
    WorkloadDriverResult result;
   
    Result() = default;
    Result(BenchmarkParams params_, WorkloadDriverResult result_) : params(params_), result(std::move(result_)) {}

    static const std::string result_as_string_header() {
        return "node_id,structure,runtime,unlimited_stream,op_count,region_size,thread_count,node_count,qp_per_conn,contains,insert,remove,lb,ub,cache_depth,count,runtime_ns,units,mean,stdev,min,p50,p90,p95,p99,p999,max,units_2,mean_2,stdev_2,min_2,p50_2,p90_2,p95_2,p99_2,p999_2,max_2,\n";
    }

    std::string result_as_string(){
        std::string builder = "";
        builder += std::to_string(params.node_id) + ",";
        builder += params.structure + ",";
        builder += std::to_string(params.runtime) + ",";
        builder += std::to_string(params.unlimited_stream) + ",";
        builder += std::to_string(params.op_count) + ",";
        builder += std::to_string(params.region_size) + ",";
        builder += std::to_string(params.thread_count) + ",";
        builder += std::to_string(params.node_count) + ",";
        builder += std::to_string(params.qp_per_conn) + ",";
        builder += std::to_string(params.contains) + ",";
        builder += std::to_string(params.insert) + ",";
        builder += std::to_string(params.remove) + ",";
        builder += std::to_string(params.key_lb) + ",";
        builder += std::to_string(params.key_ub) + ",";
        builder += std::to_string(params.cache_depth) + ",";
        builder += std::to_string(result.ops.try_get_counter()->counter) + ",";
        builder += std::to_string(result.runtime.try_get_stopwatch()->runtime_ns) + ",";
        builder += result.qps.try_get_summary()->units + ",";
        builder += to_string(result.qps.try_get_summary()->mean) + ",";
        builder += to_string(result.qps.try_get_summary()->stddev) + ",";
        builder += to_string(result.qps.try_get_summary()->min) + ",";
        builder += std::to_string(result.qps.try_get_summary()->p50) + ",";
        builder += std::to_string(result.qps.try_get_summary()->p90) + ",";
        builder += std::to_string(result.qps.try_get_summary()->p95) + ",";
        builder += std::to_string(result.qps.try_get_summary()->p99) + ",";
        builder += std::to_string(result.qps.try_get_summary()->p999) + ",";
        builder += std::to_string(result.qps.try_get_summary()->max) + ",";
        builder += result.latency.try_get_summary()->units + ",";
        builder += to_string(result.latency.try_get_summary()->mean) + ",";
        builder += to_string(result.latency.try_get_summary()->stddev) + ",";
        builder += to_string(result.latency.try_get_summary()->min) + ",";
        builder += std::to_string(result.latency.try_get_summary()->p50) + ",";
        builder += std::to_string(result.latency.try_get_summary()->p90) + ",";
        builder += std::to_string(result.latency.try_get_summary()->p95) + ",";
        builder += std::to_string(result.latency.try_get_summary()->p99) + ",";
        builder += std::to_string(result.latency.try_get_summary()->p999) + ",";
        builder += std::to_string(result.latency.try_get_summary()->max) + ",";
        return builder + "\n";
    }

    std::string result_as_debug_string(){
        if (REMUS_LOG_LEVEL != INFO){
            std::string builder = "Experimental Result {\n";
            builder += "\tParams {\n";
            builder += "\t\tstructure: " + params.structure + "\n";
            builder += "\t\tnode_id: " + std::to_string(params.node_id) + "\n";
            builder += "\t\truntime: " + std::to_string(params.runtime) + "\n";
            builder += "\t\tunlimited_stream: " + std::to_string(params.unlimited_stream) + "\n";
            builder += "\t\top_count: " + std::to_string(params.op_count) + "\n";
            builder += "\t\tregion_size: " + std::to_string(params.region_size) + "\n";
            builder += "\t\tthread_count: " + std::to_string(params.thread_count) + "\n";
            builder += "\t\tnode_count: " + std::to_string(params.node_count) + "\n";
            builder += "\t\tqp_per_conn: " + std::to_string(params.qp_per_conn) + "\n";
            builder += "\t\tcontains: " + std::to_string(params.contains) + "\n";
            builder += "\t\tinsert: " + std::to_string(params.insert) + "\n";
            builder += "\t\tremove: " + std::to_string(params.remove) + "\n";
            builder += "\t\tkey_lb: " + std::to_string(params.key_lb) + "\n";
            builder += "\t\tkey_ub: " + std::to_string(params.key_ub) + "\n";
            builder += "\t\tcache_depth: " + std::to_string(params.cache_depth) + "\n";
            builder += "\t}\n";
            builder += result.serialize();
            return builder + "}";
        } else {
            return result.qps.try_get_summary()->serialize();
        }
    }
};