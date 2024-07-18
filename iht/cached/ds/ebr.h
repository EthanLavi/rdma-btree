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

template <class T>
struct LimboLists {
    atomic<queue<rdma_ptr<T>>*> free_lists[3];
    LimboLists() = default;
};

/// An object pool that can reallocate objects as well
template <class T, int OPS_PER_EPOCH>
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
    static thread_local LimboLists<T>* limbo; // thread queues to use as free-lists

public:
    EBRObjectPool(capability* pool, int thread_count) : pool(pool), thread_count(thread_count) {
        my_version = pool->Allocate<ebr_ref>();
        my_version->version = 0;
        id_gen.store(0);
        thread_slots = new ebr_ref[thread_count]();
        for(int i = 0; i < thread_count; i++){
            thread_slots[i].version = 0;
        }
        at_version.store(0);
        // todo: delete? (until init is called, it's self-ebr)
        next_node_version = my_version;
    }

    ~EBRObjectPool(){
        delete[] thread_slots;
    }

    void destroy(capability* pool){
        for(int i = 0; i < 3; i++){
            while(!limbo->free_lists[i].load()->empty()){
                rdma_ptr<T> to_free = limbo->free_lists[i].load()->front();
                if (pool->is_local(to_free)){
                    pool->Deallocate(to_free);
                }
                limbo->free_lists[i].load()->pop();
            }
        }
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
    LimboLists<T>* RegisterThread(){
        id = id_gen.fetch_add(1);
        EBRObjectPool<T, OPS_PER_EPOCH>::limbo = new LimboLists<T>();
        limbo->free_lists[0] = new queue<rdma_ptr<T>>();
        limbo->free_lists[1] = new queue<rdma_ptr<T>>();
        limbo->free_lists[2] = new queue<rdma_ptr<T>>();
        return limbo;
    }

    /// Called at the end of every operation to indicate a finish 
    void match_version(bool override_epoch = false){
        REMUS_ASSERT(id != -1, "Forgot to call RegisterThread");
        counter++;
        if (override_epoch || counter % OPS_PER_EPOCH != 0) return; // wait before cycling iterations
        long new_version = my_version->version + 1;
        // If my version is behind the current version, hop ahead
        if (thread_slots[id].version < new_version){
            thread_slots[id].version = new_version;

            // cycle free lists (its fine if deallocations gets pushed back two iterations)
            auto tmp = limbo->free_lists[0].load();
            limbo->free_lists[0].store(limbo->free_lists[1].load());
            limbo->free_lists[1].store(limbo->free_lists[2].load());
            limbo->free_lists[2].store(tmp);
        }

        // Guard agaisnt a behind-thread
        for(int i = 0; i < thread_count; i++)
            if (thread_slots[i].version != new_version) return;

        // All threads are up-to-date.
        // If the next-node version is not update_to_date, try to be the one to write to it
        if (new_version > at_version.load()){
            int v = at_version.exchange(new_version);
            if (v != new_version){ // actually I was the one that incremented the version
                // write to the next async (we don't care when it completes, as long as it isn't lost)
                pool->Write<ebr_ref>(next_node_version, (ebr_ref) new_version, my_version, remus::rdma::internal::RDMAWriteWithNoAck);
            }
        }
    }

    /// Requeue something that was pushed but wasn't used
    void requeue(rdma_ptr<T> obj){
        limbo->free_lists[0].load()->push(obj);
    }

    /// Technically, this method shouldn't be used since the queues being rotated enable it to be single producer, single consumer
    void deallocate(rdma_ptr<T> obj){
        limbo->free_lists[2].load()->push(obj); // add to the free list
    }

    /// Allocate from the pool. Might allocate locally using the pool but not guaranteed (could be a remote!)
    /// Guaranteed via EBR to be exclusive
    rdma_ptr<T> allocate(){
        if (limbo->free_lists[0].load()->empty()){
            return pool->Allocate<T>();
        } else {
            rdma_ptr<T> ret = limbo->free_lists[0].load()->back();
            limbo->free_lists[0].load()->pop();
            return ret;
        }
    }
};

template <class T, int WAIT>
inline thread_local int EBRObjectPool<T, WAIT>::id = -1;

template <class T, int WAIT>
inline thread_local int EBRObjectPool<T, WAIT>::counter = 0;

template <class T, int WAIT>
inline thread_local LimboLists<T>* EBRObjectPool<T, WAIT>::limbo = nullptr;