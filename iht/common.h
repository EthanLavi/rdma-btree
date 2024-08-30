#pragma once

/// @brief an input to determine the depth of the IHT caching
namespace CacheDepth {
  enum CacheDepth {
    None = 0,
    RootOnly = 1,
    UpToLayer1 = 2,
    UpToLayer2 = 3,
    UpToLayer3 = 4,
  };
}


/// @brief a type used for templating remote pointers as anonymous (for exchanging over the network where the types are "lost")
struct anon_ptr {};

/// @brief  Map_Op is used by the Client Adaptor to pass in operations to Apply,
///         by forming a stream of Map_Ops.
template <typename K, typename V> struct Map_Op {
  int op_type;
  K key;
  V value;
  Map_Op(int op_type_, K key_, V value_)
      : op_type(op_type_), key(key_), value(value_){};
};

#define CONTAINS 0
#define INSERT 1
#define REMOVE 2
#define CNF_ELIST_SIZE 7
// this is the number of elements in each elist

#define CNF_PLIST_SIZE 32
// this is the starting number of buckets in the first layer. The number of buckets doubles everytime we go down a layer.
// This number should also be a multiple of 4, so we can use up all the PList space (aligned to 64 bytes and each bucket is 16 bytes)

// after 3 layers of caching, we'll have 64 to be the size of the first layer actually queried by RDMA
// We mod by the number of buckets - 1 (we dont use the last bucket so we get an even use of all our other buckets). 

// Our first three layers have 7, 15, and 31 buckets
// Next two are 63 and 127
// Since these numbers are co-prime, we will fill all our buckets.

// Note: we can choose a bad number for CNF_PLIST_SIZE. If we use 4, our sizes for the cache-able layers are 3, 7, and 15. 3 and 15 are not co-prime, meaning that buckets in the 3rd layer won't be completely filled. For example, if a key is a multiple of 3, we go in bucket 0 for layer 1 and we must go in a bucket that is also a multiple of 3 for layer 3. This is bad since we only cache a layer when all the buckets are filled.