cmake_minimum_required(VERSION 3.10)
project(bonsaikv C CXX ASM)

set(CMAKE_C_STANDARD 99)
set(CMAKE_CXX_STANDARD 11)

set(CMAKE_C_FLAGS "-fPIC")
set(CMAKE_CXX_FLAGS "-fPIC")

set(THIRD_PARTY_DIR ${CMAKE_SOURCE_DIR}/third_party)

include_directories(${THIRD_PARTY_DIR})

add_library(bonsaikv_index STATIC index_mt.cc)

add_library(bonsaikv SHARED kv.c utils.c rpm.c shim.c oplog.c dset.c gc.c pm.c alloc.c)
target_link_libraries(bonsaikv pthread jemalloc backtrace cjson ibverbs rdmacm bonsaikv_index ndctl urcu numa mlx5)

add_executable(ms test/ms.c)
target_link_libraries(ms bonsaikv)

add_executable(net test/net.c)
target_link_libraries(net bonsaikv)

add_executable(example test/example.c)
target_link_libraries(example bonsaikv)

add_executable(ycsb test/ycsb.c)
target_link_libraries(ycsb bonsaikv)
