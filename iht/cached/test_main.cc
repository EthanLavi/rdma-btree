#include "btree_bench.h"
#include "rdmask_bench.h"

template<> inline thread_local CacheMetrics RemoteCacheImpl<CountingPool>::metrics = CacheMetrics();
template<> inline thread_local CountingPool* RemoteCacheImpl<CountingPool>::pool = nullptr;

int main(int argc, char* argv[]){
    if (argc == 1) {
        REMUS_ERROR("Missing datastructure");
        return 1;
    }
    std::string structure(argv[1]);
    Peer peer = Peer();
    BenchmarkParams params = BenchmarkParams();
    if (structure == "btree" || structure == "b")
        btree_run_local(params, nullptr, nullptr, peer, peer);
    else if (structure == "rdmask" || structure == "sk")
        rdmask_run_local(params, nullptr, nullptr, peer, peer);
    else
        REMUS_ERROR("No valid structure [btree|b OR rdmask|sk]");
}