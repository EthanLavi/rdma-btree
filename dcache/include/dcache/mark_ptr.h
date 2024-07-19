#pragma once

#include <cstdint>
#include <remus/rdma/rdma_ptr.h>

using namespace remus::rdma;

constexpr uint64_t mask = (uint64_t) 1 << 63;

/// Mark for using cache protocol
template<typename T>
inline rdma_ptr<T> mark_ptr(rdma_ptr<T> ptr){
    return rdma_ptr<T>(ptr.raw() | mask);
}

/// Is marked for caching
template<typename T>
inline bool is_marked(rdma_ptr<T>& ptr){
    return ptr.raw() & mask;
}

/// Unmark a ptr from being "cached"
template<typename T>
inline rdma_ptr<T> unmark_ptr(rdma_ptr<T> ptr){
    return rdma_ptr<T>(ptr.raw() & ~mask);
}

// template<typename T>
// inline rdma_ptr<T> mark_ptr(rdma_ptr<T> ptr){
//     return ptr;
// }

// template<typename T>
// inline bool is_marked(rdma_ptr<T>& ptr){
//     return false;
// }

// template<typename T>
// inline rdma_ptr<T> unmark_ptr(rdma_ptr<T> ptr){
//     return ptr;
// }