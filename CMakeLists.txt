cmake_minimum_required (VERSION 3.0)
project (Sift)

set(CMAKE_CXX_STANDARD 11)

set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -I/usr/local/include -I. -pthread -std=c++11 \
                                         -Wall -Wno-switch -Wno-unused-variable -Wno-sign-compare -Wl,--no-as-needed \
                                         -MMD -MP -rdynamic -Wno-reorder -O2 -g")

set(CMAKE_EXE_LINKER_FLAGS  " ${CMAKE_EXE_LINKER_FLAGS} -Wl,--no-as-needed -Wl,--as-needed \
                                                       -ldl \
                                                       -lrdmacm -libverbs \
                                                       -pthread -lpthread")
#-lboost_serialization -lprofiler


include_directories("/usr/local/include" "/usr/include" ${CMAKE_CURRENT_SOURCE_DIR} "third_party/rocksdb/include")
link_directories("/usr/local/lib" "/usr/lib" "third_party/rocksdb/build")

find_package(Threads REQUIRED)

include_directories(${CMAKE_CURRENT_BINARY_DIR})

file(GLOB COMMON_HDR common/*.h util/queue.h util/bitmap.h)

set(NETWORK_SRC_DIR "src/network")
set(RDMA_SRC_DIR ${NETWORK_SRC_DIR}/rdma)

set(NETWORK_HDR_DIR "include/network")
set(RDMA_HDR_DIR ${NETWORK_HDR_DIR}/rdma)

set(RM_HDR_DIR "include/memory_replication")
set(RM_SRC_DIR "src/memory_replication")

set(UTIL_HDR_DIR "include/util")
set(UTIL_SRC_DIR "src/util")

include_directories(util/cache)
include_directories(util/rpc)
file(GLOB LRU_FILES util/cache/*.h util/cache/*.cpp)
add_library(lru_lib ${LRU_FILES})
target_link_libraries(lru_lib farmhash)
file(GLOB RPC_FILES util/rpc/*.h util/rpc/*.cpp)
add_library(rpc_lib ${RPC_FILES})

# RDMA Server Executable
set(RDMA_SERVER_SRC ${RDMA_SRC_DIR}/rdma_server.cpp ${NETWORK_SRC_DIR}/work_request.cpp ${RM_SRC_DIR}/persistent_store.cpp)
set(RDMA_SERVER_HDR ${RDMA_HDR_DIR}/rdma_server.h ${NETWORK_HDR_DIR}/work_request.h ${RM_HDR_DIR}/persistent_store.h)
add_executable(rdma_server ${RDMA_SERVER_SRC} ${RDMA_SERVER_HDR} ${COMMON_HDR})
target_link_libraries(rdma_server rdmacm ibverbs)

# RDMA Client Library
set(RDMA_CLIENT_SRC ${RDMA_SRC_DIR}/remote_rdma_client.cpp ${RDMA_SRC_DIR}/local_rdma_client.cpp ${NETWORK_SRC_DIR}/work_request.cpp src/util/serializable_address_value_pair.cpp)
set(RDMA_CLIENT_HDR ${RDMA_HDR_DIR}/remote_rdma_client.h ${RDMA_HDR_DIR}/local_rdma_client.h ${NETWORK_HDR_DIR}/work_request.h)
add_library(rdma_client ${RDMA_CLIENT_SRC} ${RDMA_CLIENT_HDR} ${COMMON_HDR})
target_link_libraries(rdma_client rdmacm ibverbs)

# Memory replication library
file(GLOB RM_SERVER_HDR ${RM_HDR_DIR}/*.h ${UTIL_HDR_DIR}/*.h)
file(GLOB RM_SERVER_SRC ${RM_SRC_DIR}/*.cpp ${UTIL_SRC_DIR}/*.cpp)
add_library(memory_replication ${RM_SERVER_HDR} ${RM_SERVER_SRC} ${COMMON_HDR})
target_link_libraries(memory_replication rdma_client rdmacm ibverbs)

set(KV_HDR_DIR "include/kv_store")
set(KV_SRC_DIR "src/kv_store")

# KV library
file(GLOB KV_HDR ${KV_HDR_DIR}/*.h)
file(GLOB KV_SRC ${KV_SRC_DIR}/*.cpp)
add_library(key_value ${KV_HDR} ${KV_SRC} ${COMMON_HDR})
target_link_libraries(key_value memory_replication lru_lib rpc_lib)

# Remote KV tests
add_executable(kv_remote_server "tests/kv/remote_server.cpp" "util/rpc/buffer.cpp" "util/rpc/connection.cpp" "util/rpc/kv_connection.cpp")
target_link_libraries(kv_remote_server key_value)

add_executable(kv_remote_client "tests/kv/remote_client.cpp")
target_link_libraries(kv_remote_client key_value)