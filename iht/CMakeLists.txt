# Run a loop and compile separate executables for main, test, and the twosided competitor
set(E1 "iht_rome")
set(E2 "iht_rome_test")
set(E3 "iht_twosided")
add_executable(${E1} iht/main.cc)
add_executable(${E2} iht/test.cc)
add_executable(${E3} iht/compete.cc)
foreach(X IN LISTS E1 E2 E3)
    # Make the vendored spdlog library easier to include
    target_link_libraries(${X} PUBLIC remus::rdma remus::workload remus::util)

    # Set up the logging level
    target_compile_definitions(${X} PUBLIC REMUS_LOG_LEVEL=${LOG_LEVEL})

    # ### Set up additional compiler and linker flags
    # target_link_libraries(${X} PRIVATE fmt::fmt)
    # target_link_libraries(${X} PRIVATE spdlog::spdlog)
    # target_include_directories(${X} PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/../vendor/spdlog-1.12.0>)
    # # NB: -D flag for ROME_LOG_LEVEL
    # target_compile_definitions(${X} PUBLIC ROME_LOG_LEVEL=${LOG_LEVEL})
    # # Use this to turn on warnings during compilation
    # # target_compile_options(${X} PUBLIC -Wall -Wextra -Werror)
    # # NB: `protos` is a library we make by recursing into the `protos` subfolder
    # target_link_libraries(${X} PUBLIC protos rdma::ibverbs rdma::cm)
endforeach()