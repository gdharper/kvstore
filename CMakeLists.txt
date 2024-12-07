cmake_minimum_required(VERSION 3.14)
project(kvstore)

set(CMAKE_CXX_STANDARD 20)

add_subdirectory(xxhash)

add_library(kvstore INTERFACE)
target_include_directories(kvstore INTERFACE inc)

add_executable(kvstore-test tool.cpp)
target_link_libraries(kvstore-test PRIVATE kvstore)
