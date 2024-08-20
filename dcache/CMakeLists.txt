# Add the include directory to the include search path
include_directories(../dcache/include)

add_executable(mark_ptr_test test/mark_ptr.cc)
target_link_libraries(mark_ptr_test PUBLIC remus::rdma remus::workload remus::util)
add_test(mark_ptr_test mark_ptr_test)

add_executable(cache_store_test test/cache_store.cc)
target_link_libraries(cache_store_test PUBLIC remus::rdma remus::workload remus::util)
add_test(cache_store_test cache_store_test)

add_executable(dynamics_test test/dynamics.cc)
target_link_libraries(dynamics_test PUBLIC remus::rdma remus::workload remus::util)
add_test(dynamics_test dynamics_test)

add_executable(object_pool_test test/object_pool.cc)
target_link_libraries(object_pool_test PUBLIC remus::rdma remus::workload remus::util)
add_test(object_pool_test object_pool_test)

add_executable(cached_ptr_test test/cached_ptr.cc)
target_link_libraries(cached_ptr_test PUBLIC remus::rdma remus::workload remus::util)
add_test(cached_ptr_test cached_ptr_test)

add_executable(cached_iht test/cached_iht.cc)
target_link_libraries(cached_iht PUBLIC remus::rdma remus::workload remus::util)
add_test(cached_iht cached_iht)

# cmake .. && make && make test VERBOSE=1