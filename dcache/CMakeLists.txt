add_executable(main main.cc)
target_link_libraries(main PUBLIC remus::rdma remus::workload remus::util)

add_executable(ptr_test test/mark_ptr.cc)
target_link_libraries(ptr_test PUBLIC remus::rdma remus::workload remus::util)
add_test(ptr_test test)

add_executable(cache_test test/cache_store.cc)
target_link_libraries(cache_test PUBLIC remus::rdma remus::workload remus::util)
add_test(cache_test test)
