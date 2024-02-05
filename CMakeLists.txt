cmake_minimum_required(VERSION 3.0)
project(bonsaikv C)

set(CMAKE_C_STANDARD 99)

add_library(bonsaikv kv.c utils.c asok.c rpm.c perf.c)
target_link_libraries(bonsaikv pthread jemalloc backtrace cjson ibverbs rdmacm)
