#include "../include/mark_ptr.h"
#include "../include/cache_store.h"

#include <cstdlib>
#include <cstring>
#include <remus/logging/logging.h>
#include <remus/rdma/rdma.h>
#include <remus/rdma/rdma_ptr.h>

struct alignas (64) Structure {
    int x[8];
    int y[8];
};

void test(bool condition, const char* message){
    if (!condition){
        REMUS_ERROR("Error: {}", message);
        exit(1);
    }
}

int main(){
    Peer self = Peer(0, "node0", 18000);
    Peer other = Peer(1, "node1", 18001);
    rdma_capability* capabilitySelf = new rdma_capability(self);
    capabilitySelf->RegisterThread();
    std::vector<Peer> peers = {self};
    capabilitySelf->init_pool(1 << 20, peers);
    rdma_capability* capabilityOther = new rdma_capability(other);
    RemoteCache* cache = new RemoteCache(capabilitySelf);
    RemoteCache::pool = capabilityOther; // set pool to other pool so we acccept our own cacheline
    cache->init({cache->root()}); // initialize with itself
    RemoteCache::pool = capabilitySelf; // change back to self pool

    rdma_ptr<Structure> p = capabilitySelf->Allocate<Structure>(2);
    memset((Structure*) p.address(), 0, sizeof(Structure) * 2);
    rdma_ptr<Structure> marked_p = mark_ptr(p);
    CachedObject<Structure> p2 = cache->ExtendedRead(marked_p, 2); // put it in cache
    p2->x[0] = 1; // write to the cached version

    cache->Write(marked_p, *p2); // write it back
    test(p->x[0] == 1, "Write didn't occur");
    CachedObject<Structure> p3 = cache->ExtendedRead(marked_p, 2); // read it again
    test(p3->x[0] == 1, "Cache didn't invalidate the write"); // check we observed the cached result
    test(p3->x[1] == 0, "Check second value of x for safety"); 
    p->x[1] = 1; // write to the object not through the cache
    CachedObject<Structure> p4 = cache->ExtendedRead(marked_p, 2); // read it again
    test(p4->x[1] == 0, "Before invalidate, cache result is not flipped");
    cache->Invalidate(marked_p); // invalidate the object
    CachedObject<Structure> p5 = cache->ExtendedRead(marked_p, 2); // read it again
    test(p5->x[0] == 1, "Write persisted"); 
    test(p5->x[1] == 1, "Invalidate allowed us to observe the correct result"); 
    test(p5->x[2] == 0, "Check third value of x for safety");

    REMUS_INFO("Passed all tests!");

    // Free memory
    delete cache;

    // todo: try to test without pool trick
    // todo: get more code coverage (maybe cache swaps? idk)
}