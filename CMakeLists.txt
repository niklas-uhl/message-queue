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
target_compile_options(message-queue INTERFACE -std=c++17)
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

add_executable(message_queue_example example.cpp)
target_link_libraries(message_queue_example PUBLIC message-queue fmt::fmt kassert::kassert
                                     CLI11::CLI11)

add_executable(message-queue::example ALIAS message_queue_example)

# add_executable(asynchronous asynchronous.cpp)
# target_link_libraries(asynchronous MPI::MPI_CXX)

add_executable(message_queue_bfs bfs.cpp)
target_link_libraries(message_queue_bfs PUBLIC message-queue KaGen::KaGen CLI11::CLI11
                                 kassert::kassert fmt::fmt)
if(MESSAGE_QUEUE_BACKTRACE)
  target_link_libraries(message_queue_bfs PUBLIC Backward::Backward)
endif()
add_sanitizers(message_queue_bfs)

add_executable(message-queue::bfs ALIAS message_queue_bfs)
