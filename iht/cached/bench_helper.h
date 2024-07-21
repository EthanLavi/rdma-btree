#pragma once

#include "../experiment.h"

#include <functional>
#include <remus/metrics/workload_driver_result.h>
#include <remus/util/tcp/tcp.h>
#include <remus/rdma/rdma.h>

using namespace remus::util;
using namespace remus::rdma;
using namespace remus::metrics;

#define PORT_NUM_TCP 19000

inline void init_endpoints(tcp::EndpointManager* endpoint_managers[], BenchmarkParams& params, Peer host){
    // Initialize T endpoints, one for each thread
    for(uint16_t i = 0; i < params.thread_count; i++){
        endpoint_managers[i] = new tcp::EndpointManager(PORT_NUM_TCP, host.address.c_str());
    }
}

inline void delete_endpoints(tcp::EndpointManager* endpoint_managers[], BenchmarkParams& params){
    for(uint16_t i = 0; i < params.thread_count; i++){
        delete endpoint_managers[i];
    }
}

inline tcp::SocketManager* init_handle(BenchmarkParams& params){
    tcp::SocketManager* socket_handle = new tcp::SocketManager(PORT_NUM_TCP);
    for(int i = 0; i < params.thread_count * params.node_count; i++){
        // TODO: Can we have a per-node connection?
        // I haven't gotten around to coming up with a clean way to reduce the number of sockets connected to the server
        socket_handle->accept_conn();
    }
    return socket_handle;
}

/// On the other end of map_reduce on the server side (coordinate_map_reeduce)
inline void collect_distribute(tcp::SocketManager* socket_handle, BenchmarkParams& params){
    tcp::message msgs[params.node_count * params.thread_count];
    socket_handle->recv_from_all(msgs);
    for(int i = 0; i < params.node_count * params.thread_count; i++){
        socket_handle->send_to_all(&msgs[i]);
    }
}

/// Send contribution to everyone and do_with on all values
inline void map_reduce(tcp::EndpointManager* endpoint, BenchmarkParams& params, uint64_t contribution, std::function<void(uint64_t)> do_with){
    tcp::message data(contribution);
    endpoint->send_server(&data);
    for(int i = 0; i < params.node_count * params.thread_count; i++){
        endpoint->recv_server(&data);
        do_with(data.get_first());
    }
}

inline void save_result(std::string filename, WorkloadDriverResult workload_results[], BenchmarkParams& params, int thread_count){
    Result result[thread_count];
    for (int i = 0; i < thread_count; i++) {
        result[i] = Result(params, workload_results[i]);
        REMUS_INFO("Protobuf Result {}\n{}", i, result[i].result_as_debug_string());
    }

    std::ofstream filestream(filename);
    filestream << Result::result_as_string_header();
    for (int i = 0; i < thread_count; i++) {
        filestream << result[i].result_as_string();
    }
    filestream.close();
}