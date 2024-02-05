cmake_minimum_required(VERSION 3.17)
project(bonsaikv C CXX ASM)

set(CMAKE_C_STANDARD 99)
set(CMAKE_CXX_STANDARD 11)

set(THIRD_PARTY_DIR ${CMAKE_SOURCE_DIR}/third_party)
set(MT_SOURCE ${THIRD_PARTY_DIR}/masstree)

include_directories(${THIRD_PARTY_DIR})

add_library(bonsaikv_index STATIC index_mt.cc)

add_library(bonsaikv SHARED kv.c utils.c asok.c rpm.c perf.c shim.c)
target_link_libraries(bonsaikv pthread jemalloc backtrace cjson ibverbs rdmacm bonsaikv_index)
