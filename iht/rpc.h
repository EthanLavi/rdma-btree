#include "iht_local.h"

#include <remus/rdma/connection_map.h>
#include <remus/util/status.h>

#include <atomic>
#include <mutex>
#include <protos/experiment.pb.h>
#include <thread>
#include <unordered_map>

using namespace std;
using namespace remus::rdma::internal;
using namespace remus::util;

#define MAX_THREAD_POOL 8

// N.B.
// have two connection maps, one for receivers and one for senders
// the RPC will poll on the receivers and send the result on the senders
// the ops will send to the receivers and poll the result on the sender
// (therefore separate lines of communication and no overlapping in reading the queue)

enum {
    GET_REQ = 1,
    INS_REQ = 2, 
    RMV_REQ = 3, 
    GET_RES = 4, 
    INS_RES = 5,
    RMV_RES = 6,
    ERR = 7,
};

class TwoSidedIHT {
private:
    // The reference to the node's internal data
    iht_carumap<int, int, 8, 64>* internal_data_;
    // Lower bound and upper bound of the IHT's keyrange (partitioned among nodes)
    int self_id;
    int count;
    int keyspace_lb; // keyspace lower bound
    int keyspace_len; // keyspace size
    std::vector<std::mutex*> lock_table_client;
    std::vector<std::mutex*> lock_table_server;

    std::unordered_map<int, Connection*> sender_map;
    std::unordered_map<int, Connection*> receiver_map;

    /// convert a key to it's id. Will return -1 if it cannot be found
    int to_id(int key){
        REMUS_ASSERT(key >= keyspace_lb && key - keyspace_lb <= keyspace_len, "Keyspace access error");
        int id = (count * (key - keyspace_lb)) / keyspace_len;
        if (id == count) return id - 1; // map any overflow numbers to the last id
        return id;
    }

    std::vector<std::thread> t;
    volatile bool stop_listening = false;
public:
    TwoSidedIHT() = delete;
    ~TwoSidedIHT(){
        stop_listening = true;
        delete internal_data_;
        for(int i = 0; i < fmin(MAX_THREAD_POOL, count); i++){
            t[i].join();
        }
        // for(int i = 0; i < count; i++){
        //     delete lock_table_client[count];
        // }
        // for(int i = 0; i < count; i++){
        //     delete lock_table_server[count];
        // }
    }

    /// IHT RPC. One per node
    /// Keyspace lower bound and upper bound is inclusive. So (0-100) means 101 numbers
    TwoSidedIHT(int self_id, int count, int keyspace_lb, int keyspace_ub, std::unordered_map<int, Connection*>& sender_map, std::unordered_map<int, Connection*>& receiver_map) 
        : self_id(self_id), count(count), keyspace_lb(keyspace_lb), keyspace_len(keyspace_ub - keyspace_lb){
        // create a map to represent the internal data of the node
        internal_data_ = new iht_carumap<int, int, 8, 64>();
        REMUS_ASSERT(self_id >= 0 && self_id < count, "Invalid id given node count"); // assert id
        for(int i = 0; i < count; i++){
            lock_table_client.push_back(new std::mutex());
        }
        for(int i = 0; i < count; i++){
            lock_table_server.push_back(new std::mutex());
        }

        this->sender_map = sender_map;
        this->receiver_map = receiver_map;

        // todo: tune thread-pool size
        for(int i = 0; i < fmin(MAX_THREAD_POOL, count); i++){
            t.push_back(std::thread([&](int myid, int node_count){
                // Loop until stop_listening flag is set
                while (!stop_listening) {
                    // Continously iterate over the nodes
                    for(int id = 0; id < node_count; id++){
                        if (id == myid) continue; // skip my id
                        // Try to get a value
                        lock_table_server[id]->lock();
                        std::optional<IHTOPProto> maybe_req = receiver_map[id]->channel()->TryReceive<IHTOPProto>();
                        // value here referes to the optional and not the IHTOpProto itself...
                        if (!maybe_req.has_value()){
                            lock_table_server[id]->unlock();
                            continue;
                        }
                        IHTOPProto request = maybe_req.value();
                        auto op = request.op_type();
                        
                        // Do the request
                        IHTOPProto response;
                        response.set_key(request.key());
                        response.set_value(request.value());
                        if (op == GET_REQ){
                            int answer;
                            bool has_key = internal_data_->get(request.key(), answer);
                            response.set_op_type(has_key ? GET_RES : ERR);
                            response.set_value(answer);
                        } else if (op == INS_REQ){
                            optional<int> old_key = internal_data_->insert(request.key(), request.value());
                            response.set_op_type(!old_key.has_value() ? INS_RES : ERR);
                            if (old_key.has_value()) response.set_value(old_key.value());
                        } else if (op == RMV_REQ){
                            int answer;
                            bool has_key = internal_data_->remove(request.key(), answer);
                            response.set_op_type(has_key ? RMV_RES : ERR);
                            response.set_value(answer);
                        } else {
                            REMUS_ERROR("Request has unexpected opcode");
                        }

                        // Send the response
                        Status stat = sender_map[id]->channel()->Send(response);
                        lock_table_server[id]->unlock();
                        REMUS_ASSERT(stat.t == Ok, "Operation failed");
                    }
                }
            }, self_id, count));
        }
    }

    /// @brief Gets a value at the key.
    /// @param key the key to search on
    /// @return an optional containing the value, if the key exists
    std::optional<int> get(int key){
        int target_id = to_id(key);
        if (target_id == self_id){
            // don't use connections for self
            int val;
            if (internal_data_->get(key, val))
                return val;
            else
                return std::nullopt;
        }
        /// Send the proto
        IHTOPProto request;
        request.set_op_type(GET_REQ);
        request.set_key(key);
        request.set_value(0); // unneeded so 0
        lock_table_client[target_id]->lock();
        Status stat = receiver_map[target_id]->channel()->Send(request);
        REMUS_ASSERT(stat.t == Ok, "Operation failed");

        /// Receive the result
        StatusVal<IHTOPProto> maybe_result = sender_map[target_id]->channel()->Deliver<IHTOPProto>();
        lock_table_client[target_id]->unlock();
        REMUS_ASSERT(maybe_result.status.t == Ok, "Cannot get result");
        IHTOPProto result = maybe_result.val.value();
        if (result.op_type() == ERR) return nullopt;
        REMUS_ASSERT(result.op_type() == GET_RES, "Response to get has unexpected opcode, {}", result.op_type());
        return result.value();
    }

    /// @brief Insert a key and value into the iht. Result will become the value
    /// at the key if already present.
    /// @param key the key to insert
    /// @param value the value to associate with the key
    /// @return an empty optional if the insert was successful. Otherwise it's the value at the key.
    std::optional<int> insert(int key, int val){
        int target_id = to_id(key);
        if (target_id == self_id){
            // don't use connections for self. Just query the internal map
            return internal_data_->insert(key, val);
        }
        /// Send the proto
        IHTOPProto request;
        request.set_op_type(INS_REQ);
        request.set_key(key);
        request.set_value(val);
        lock_table_client[target_id]->lock();
        Status stat = receiver_map[target_id]->channel()->Send(request);
        REMUS_ASSERT(stat.t == Ok, "Operation failed");

        /// Receive the result
        StatusVal<IHTOPProto> maybe_result = sender_map[target_id]->channel()->Deliver<IHTOPProto>();
        lock_table_client[target_id]->unlock();
        REMUS_ASSERT(maybe_result.status.t == Ok, "Cannot get result");
        IHTOPProto result = maybe_result.val.value();
        if (result.op_type() == ERR) return result.value();
        REMUS_ASSERT(result.op_type() == INS_RES, "Response to insert has unexpected opcode, {}", result.op_type());
        return nullopt;
    }

    /// @brief Will remove a value at the key. Will stored the previous value in
    /// result.
    /// @param key the key to remove at
    /// @return an optional containing the old value if the remove was successful. Otherwise an empty optional.
    std::optional<int> remove(int key){
        int target_id = to_id(key);
        if (target_id == self_id){
            // don't use connections for self
            int val;
            if(internal_data_->remove(key, val))
                return val;
            else
                return std::nullopt;
        }
        /// Send the proto
        IHTOPProto request;
        request.set_op_type(RMV_REQ);
        request.set_key(key);
        request.set_value(0); // unneed so zero
        lock_table_client[target_id]->lock();
        Status stat = receiver_map[target_id]->channel()->Send(request);
        REMUS_ASSERT(stat.t == Ok, "Operation failed");

        /// Receive the result
        StatusVal<IHTOPProto> maybe_result = sender_map[target_id]->channel()->Deliver<IHTOPProto>();
        lock_table_client[target_id]->unlock();
        REMUS_ASSERT(maybe_result.status.t == Ok, "Cannot get result");
        IHTOPProto result = maybe_result.val.value();
        if (result.op_type() == ERR) return nullopt;
        REMUS_ASSERT(result.op_type() == RMV_RES, "Response to remove has unexpected opcode, {}", result.op_type());
        return result.value();    
    }

    /// @brief Populate only works when we have numerical keys. Will add data
    /// @param pool the capability providing one-sided RDMA
    /// @param op_count the number of values to insert. Recommended in total to do
    /// key_range / 2
    /// @param key_lb the lower bound for the key range
    /// @param key_ub the upper bound for the key range
    /// @param value the value to associate with each key. Currently, we have
    /// asserts for result to be equal to the key. Best to set value equal to key!
    void populate(int op_count, int key_lb, int key_ub, std::function<int(int)> value) {
        // Populate only works when we have numerical keys
        int key_range = key_ub - key_lb;
        // todo: Under-populating because of insert collisions?
        // Create a random operation generator that is
        // - evenly distributed among the key range
        std::uniform_real_distribution<double> dist = std::uniform_real_distribution<double>(0.0, 1.0);
        std::default_random_engine gen((unsigned) std::time(nullptr));
        for (int c = 0; c < op_count; c++) {
            int k = (dist(gen) * key_range) + key_lb;
            insert(k, value(k));
            // Wait some time before doing next insert...
            std::this_thread::sleep_for(std::chrono::nanoseconds(10));
        }
    }
};