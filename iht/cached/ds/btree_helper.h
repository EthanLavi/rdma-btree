#include <climits>
#include <cstdint>
#include <remus/rdma/rdma.h>

typedef int32_t K;
static const K SENTINEL = INT_MAX;

using namespace remus::rdma;

#define KLINE_SIZE 14
#define PLINE_SIZE 7

/// A line with keys
struct kline {
    long version;
    K keys[KLINE_SIZE];
};

/// A line with ptrs
struct pline {
    long version;
    rdma_ptr<pline> ptrs[PLINE_SIZE];
};