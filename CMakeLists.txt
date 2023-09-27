cmake_minimum_required(VERSION 3.15)
project(message-queue LANGUAGES CXX)

option(MESSAGE_QUEUE_BACKTRACE "Enable stacktraces" ON)
option(MESSAGE_QUEUE_USE_MY_TEST_FUNCTIONS
       "Use self-implemented variants of Test_some and Test_any" OFF)

include(cmake/dependencies.cmake)

add_library(message-queue INTERFACE)
target_include_directories(message-queue INTERFACE include)
target_link_libraries(message-queue INTERFACE MPI::MPI_CXX
                                              message_queue_boost_dependencies kassert::kassert)
target_compile_features(message-queue INTERFACE cxx_std_20)
if(MESSAGE_QUEUE_USE_MY_TEST_FUNCTIONS)
  target_compile_definitions(message-queue
                             INTERFACE MESSAGE_QUEUE_USE_MY_TEST_FUNCTIONS)
endif()
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION
                                            VERSION_LESS 12.0)
  target_compile_definitions(message-queue
                             INTERFACE MESSAGE_QUEUE_GCC_11_SPLIT_VIEW_BUG)
  message(
    WARNING
      "GCC 11 is known to not correctly implement split view. We have a fix, but consider upgrading your compiler."
  )
endif()
add_library(message-queue::message-queue ALIAS message-queue)

if (CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME OR MESSAGE_QUEUE_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()
