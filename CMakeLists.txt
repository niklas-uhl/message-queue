cmake_minimum_required(VERSION 3.15)
project(message-queue LANGUAGES CXX)

option(MESSAGE_QUEUE_BLOCKING_RECEIVE "Receive messages blocking" OFF)
option(MESSAGE_QUEUE_MATCHED_RECV "Use MPI_Mrecv" OFF)
option(MESSAGE_QUEUE_BACKTRACE "Enable stacktraces" ON)
option(MESSAGE_QUEUE_USE_MY_TEST_FUNCTIONS
       "Use self-implemented variants of Test_some and Test_any" OFF)
message(STATUS BLOCKING_RECEIVE ${MESSAGE_QUEUE_BLOCKING_RECEIVE})

include(cmake/dependencies.cmake)

add_library(message-queue INTERFACE)
target_include_directories(message-queue INTERFACE include)
target_link_libraries(message-queue INTERFACE MPI::MPI_CXX
                                              message_queue_boost_dependencies)
target_compile_features(message-queue INTERFACE cxx_std_20)
if(MESSAGE_QUEUE_BLOCKING_RECEIVE)
  target_compile_definitions(message-queue
                             INTERFACE MESSAGE_QUEUE_BLOCKING_RECEIVE)
endif()
if(MESSAGE_QUEUE_MATCHED_RECV)
  target_compile_definitions(message-queue INTERFACE MESSAGE_QUEUE_MATCHED_RECV)
endif()
if(MESSAGE_QUEUE_USE_MY_TEST_FUNCTIONS)
  target_compile_definitions(message-queue INTERFACE MESSAGE_QUEUE_USE_MY_TEST_FUNCTIONS)
endif()
add_library(message-queue::message-queue ALIAS message-queue)

add_executable(message_hopping_example message_hopping_example.cpp)
target_link_libraries(message_hopping_example PUBLIC message-queue fmt::fmt kassert::kassert
                                     CLI11::CLI11)


# add_executable(asynchronous asynchronous.cpp)
# target_link_libraries(asynchronous MPI::MPI_CXX)

add_executable(async_bfs_example async_bfs_example.cpp)
target_link_libraries(async_bfs_example PUBLIC message-queue KaGen::KaGen CLI11::CLI11
                                 kassert::kassert fmt::fmt)
if(MESSAGE_QUEUE_BACKTRACE)
  target_link_libraries(async_bfs_example PUBLIC Backward::Backward)
endif()
add_sanitizers(async_bfs_example)

add_executable(message-queue::async_bfs_example ALIAS async_bfs_example)
