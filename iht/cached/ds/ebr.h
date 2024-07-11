#include <cstdint>
#include <protos/rdma.pb.h>
#include <queue>
#include <remus/logging/logging.h>
#include <remus/rdma/memory_pool.h>
#include <vector>
#include <atomic>
#include <remus/rdma/peer.h>

#include "../../../dcache/test/faux_mempool.h"

using namespace remus::rdma;
using namespace std;

typedef CountingPool capability;

/// An object pool that can reallocate objects as well
template <class T, int WAIT>
class EBRObjectPool {
    /// version that the node can wait on. Once it sees it increments, in can rotate its free lists and increment the next node
    struct alignas(64) ebr_ref {
        long version;
    };

    capability* pool;
    rdma_ptr<ebr_ref> next_node_version; // a version for the next node in the chain
    
    int thread_count;
    rdma_ptr<ebr_ref> my_version;
    atomic<int> id_gen; // generator for the id
    ebr_ref* thread_slots; // each thread has a local version
    atomic<int> at_version;
    static thread_local int id; // the id matches a local version in the version_slots
    static thread_local int counter;
    static thread_local queue<rdma_ptr<T>>* free_lists[3]; // thread queues to use as free-lists

    EBRObjectPool(capability* pool, int thread_count) : pool(pool), thread_count(thread_count) {
        my_version = pool->Allocate<ebr_ref>();
        my_version->version = 0;
        id_gen.store(0);
        thread_slots = new ebr_ref[thread_count]();
        for(int i = 0; i < thread_count; i++){
            thread_slots[i]->version = 0;
        }
        at_version.store(0);
        // todo: delete? (until init is called, it's self-ebr)
        next_node_version = my_version;
    }

    ~EBRObjectPool(){
        delete[] thread_slots;
        delete free_lists[0];
        delete free_lists[1];
        delete free_lists[2];
    }

    /// Connect to the other peers
    void Init(rdma_capability* two_sided_pool, int node_id, vector<Peer> peers){
        if (peers.size() == 1) return; // just connect to self (will still use rdma for ebr)
        RemoteObjectProto my_proto;
        my_proto.set_raddr((uint64_t) my_version);
        RemoteObjectProto next_proto;
        
        int send_id = (node_id - 1) % peers.size();
        int recv_id = (node_id + 1) % peers.size();
        Peer sender, recvr;
        for(int i = 0; i < peers.size(); i++){
            if (peers.at(i).id == send_id) sender = peers.at(i);
            if (peers.at(i).id == recv_id) recvr = peers.at(i);
        }

        // If we are recv from the root, we recv first before sending
        if (recv_id == 0){
            auto got = two_sided_pool->Recv<RemoteObjectProto>(recvr);
            REMUS_ASSERT(got.status.t == remus::util::Ok, got.status.message.value());
            next_node_version = static_cast<rdma_ptr<ebr_ref>>(got.val.value().raddr());

            auto status_send = two_sided_pool->Send<RemoteObjectProto>(sender, my_proto);
            REMUS_ASSERT(status_send.t == remus::util::Ok, status_send.message.value());
        } else {
            auto status_send = two_sided_pool->Send<RemoteObjectProto>(sender, my_proto);
            REMUS_ASSERT(status_send.t == remus::util::Ok, status_send.message.value());

            auto got = two_sided_pool->Recv<RemoteObjectProto>(recvr);
            REMUS_ASSERT(got.status.t == remus::util::Ok, got.status.message.value());
            next_node_version = static_cast<rdma_ptr<ebr_ref>>(got.val.value().raddr());
        }
    }

    // Get an id for the thread...
    void RegisterThread(){
        id = id_gen.fetch_add(1);
        free_lists[0] = new queue<rdma_ptr<T>>();
        free_lists[1] = new queue<rdma_ptr<T>>();
        free_lists[2] = new queue<rdma_ptr<T>>();
    }

    /// Called at the end of every operation to indicate a finish 
    void match_version(){
        counter++;
        if (counter % WAIT != 0) return; // wait before cycling iterations
        REMUS_ASSERT(id != -1, "Forgot to call RegisterThread");
        int new_version = my_version->version + 1;
        // If my version is behind the current version, hop ahead
        if (thread_slots[id] < new_version){
            thread_slots[id] = new_version;

            // cycle free lists (its fine if deallocations gets pushed back two iterations)
            auto tmp = free_lists[0];
            free_lists[0] = free_lists[1];
            free_lists[1] = free_lists[2];
            free_lists[2] = tmp;
        }

        // Guard agaisnt a behind-thread
        for(int i = 0; i < thread_count; i++)
            if (thread_slots[i] != new_version) return;

        // All threads are up-to-date.
        // If the next-node version is not update_to_date, try to be the one to write to it
        if (new_version > at_version.load()){
            int v = at_version.exchange(new_version);
            if (v != new_version){ // actually I was the one that incremented the version
                // write to the next
                pool->Write(next_node_version, my_version, new_version, remus::rdma::internal::RDMAWriteWithNoAck); // we don't care when it completes
            }
        }
    }

    void deallocate(rdma_ptr<T> obj){
        free_lists[2]->push(obj); // add to the free list
    }

    /// Allocate from the pool. Might allocate locally using the pool but not guaranteed (could be a remote!)
    /// Guaranteed via EBR to be exclusive
    rdma_ptr<T> allocate(){
        if (free_lists[0]->empty()){
            return pool->Allocate<T>();
        } else {
            rdma_ptr<T> ret = free_lists[0]->back();
            free_lists[0]->pop();
            return ret;
        }
    }
};

template <class T, int WAIT>
inline thread_local int EBRObjectPool<T, WAIT>::id = -1;

template <class T, int WAIT>
inline thread_local int EBRObjectPool<T, WAIT>::counter = 0;

template <class T, int WAIT>
inline thread_local queue<rdma_ptr<T>>* EBRObjectPool<T, WAIT>::free_lists[3];