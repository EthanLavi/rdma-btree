# Add the include directory to the include search path
include_directories(../dcache/include)

# Run a loop and compile separate executables for main, test, and the twosided competitor
set(E1 "iht_rome")
set(E2 "iht_rome_test")
set(E4 "iht_rome_cached")
# set(E3 "iht_twosided")
# add_executable(${E1} main.cc)
add_executable(${E4} cached/main_cached.cc)
# add_executable(${E2} test.cc)
# add_executable(${E3} compete.cc), todo: add back 
foreach(X IN LISTS E4) # E3
    # Make the vendored spdlog library easier to include
    target_link_libraries(${X} PUBLIC remus::rdma remus::workload remus::util)

    # Set up the logging level
    target_compile_definitions(${X} PUBLIC REMUS_LOG_LEVEL=${LOG_LEVEL})
endforeach()