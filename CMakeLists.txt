cmake_minimum_required(VERSION 3.20)
project(message-queue LANGUAGES CXX)

find_package(MPI REQUIRED)

add_library(message-queue INTERFACE)
target_include_directories(message-queue INTERFACE include)
target_link_libraries(message-queue INTERFACE MPI::MPI_CXX)
target_compile_options(message-queue INTERFACE -std=c++17)

add_executable(example example.cpp)
target_link_libraries(example message-queue)
