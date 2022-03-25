cmake_minimum_required(VERSION 3.15)
project(message-queue LANGUAGES CXX)

option(MESSAGE_QUEUE_BLOCKING_RECEIVE OFF "Receive messages blocking")

get_property(isMultiConfig GLOBAL PROPERTY GENERATOR_IS_MULTI_CONFIG)

if(isMultiConfig)
    if(NOT "Asan" IN_LIST CMAKE_CONFIGURATION_TYPES)
        list(APPEND CMAKE_CONFIGURATION_TYPES Asan)
    endif()
else()
    set(allowedBuildTypes Asan Debug Release RelWithDebInfo MinSizeRel)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "${allowedBuildTypes}")

    if(CMAKE_BUILD_TYPE AND NOT CMAKE_BUILD_TYPE IN_LIST allowedBuildTypes)
        message(FATAL_ERROR "Invalid build type: ${CMAKE_BUILD_TYPE}")
    endif()
endif()

set(CMAKE_C_FLAGS_ASAN
    "${CMAKE_C_FLAGS_DEBUG} -fsanitize=address -fno-omit-frame-pointer" CACHE STRING
    "Flags used by the C compiler for Asan build type or configuration." FORCE)

set(CMAKE_CXX_FLAGS_ASAN
    "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -fno-omit-frame-pointer" CACHE STRING
    "Flags used by the C++ compiler for Asan build type or configuration." FORCE)

set(CMAKE_EXE_LINKER_FLAGS_ASAN
    "${CMAKE_SHARED_LINKER_FLAGS_DEBUG} -fsanitize=address" CACHE STRING
    "Linker flags to be used to create executables for Asan build type." FORCE)

set(CMAKE_SHARED_LINKER_FLAGS_ASAN
    "${CMAKE_SHARED_LINKER_FLAGS_DEBUG} -fsanitize=address" CACHE STRING
    "Linker lags to be used to create shared libraries for Asan build type." FORCE)

find_package(MPI REQUIRED)

#set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -fsanitize=address ")
#set (CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -fno-omit-frame-pointer -fsanitize=address ")


#set(GRAPH_IO_GENERATORS OFF)
add_subdirectory(extern/graph-io)

add_subdirectory(extern/backward-cpp)
add_subdirectory(extern/cli11)

add_library(message-queue INTERFACE)
target_include_directories(message-queue INTERFACE include)
target_link_libraries(message-queue INTERFACE MPI::MPI_CXX)
target_compile_options(message-queue INTERFACE -std=c++17)
if (MESSAGE_QUEUE_BLOCKING_RECEIVE)
  target_compile_definitions(message-queue INTERFACE MESSAGE_QUEUE_BLOCKING_RECEIVE)
endif()

add_executable(example example.cpp)
target_link_libraries(example message-queue)

#add_executable(asynchronous asynchronous.cpp)
#target_link_libraries(asynchronous MPI::MPI_CXX)

add_executable(bfs bfs.cpp ${BACKWARD_ENABLE})
target_link_libraries(bfs message-queue graph-io CLI11::CLI11)
add_backward(bfs)
