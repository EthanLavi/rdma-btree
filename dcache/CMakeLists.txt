add_executable(main main.cc)
target_link_libraries(main PUBLIC rome::rdma rome::workload rome::util)


add_executable(ptr_test test/mark_ptr.cc)
target_link_libraries(ptr_test PUBLIC rome::rdma rome::workload rome::util)
add_test(ptr_test test)

