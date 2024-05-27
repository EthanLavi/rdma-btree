add_executable(mark_ptr_test test/mark_ptr.cc)
target_link_libraries(mark_ptr_test PUBLIC remus::rdma remus::workload remus::util)
add_test(mark_ptr_test test)

add_executable(cache_store_test test/cache_store.cc)
target_link_libraries(cache_store_test PUBLIC remus::rdma remus::workload remus::util)
add_test(cache_store_test test)

add_executable(object_pool_test test/object_pool.cc)
target_link_libraries(object_pool_test PUBLIC remus::rdma remus::workload remus::util)
add_test(object_pool_test test)

add_executable(cached_ptr_test test/cached_ptr.cc)
target_link_libraries(cached_ptr_test PUBLIC remus::rdma remus::workload remus::util)
add_test(cached_ptr_test test)

# todo: make real cmake tests