// #include "btree_bench.h"
#include "rdmask_bench.h"

int main(){
    Peer peer = Peer();
    BenchmarkParams params = BenchmarkParams();
    // btree_run(params, nullptr, nullptr, peer, peer);
    rdmask_run(params, nullptr, nullptr, peer, peer);
}