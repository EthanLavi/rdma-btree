cmake_minimum_required(VERSION 3.18)
project(remus LANGUAGES CXX VERSION 0.0.0)

set(CMAKE_CUDA_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(LOG_LEVEL "DEBUG" CACHE STRING "Log level options include TRACE, DEBUG, INFO, WARN, ERROR, CRITICAL, and OFF")
set(CXX_STANDARD 20 CACHE STRING "Uses C++20 or 23")

message(STATUS "Using standard ${CXX_STANDARD}")

set(CMAKE_CXX_STANDARD ${CXX_STANDARD})
set(CMAKE_CUDA_STANDARD ${CXX_STANDARD})
set(CMAKE_CUDA_ARCHITECTURES ${CUDA_ARCHITECTURES})

# Use a custom rule (in `cmake/`) to find the ibverbs and rdmacm libraries.
list(APPEND CMAKE_MODULE_PATH "/usr/local/lib/cmake/")
list(APPEND CMAKE_PREFIX_PATH "/usr/local/lib/cmake/")
find_package(RDMA REQUIRED)

find_package(Protobuf REQUIRED)
message(STATUS "Using protobuf version ${Protobuf_VERSION}")

find_package(fmt 8.1...<8.2 REQUIRED) # defines fmt::fmt

message(STATUS "Using fmt version ${fmt_VERSION}")

find_package(spdlog 1.9...<1.10 REQUIRED) #defines spdlog::spdlog
message(STATUS "Using spdlog version ${spdlog_VERSION}")

find_package(nlohmann_json REQUIRED) # should use 3.10 or 3.11
message(STATUS "Using nlohmann_json version ${nlohmann_json_VERSION}")

find_package(remus REQUIRED)
message(STATUS "Using remus version ${remus_VERSION}")

add_subdirectory(iht)
add_subdirectory(dcache)