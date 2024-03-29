cmake_minimum_required(VERSION 3.15)
project(message-queue LANGUAGES CXX)

option(MESSAGE_QUEUE_BACKTRACE "Enable stacktraces" ON)
option(MESSAGE_QUEUE_USE_MY_TEST_FUNCTIONS
       "Use self-implemented variants of Test_some and Test_any" OFF)

include(cmake/dependencies.cmake)

try_compile(
  MESSAGE_QUEUE_HAS_LAZY_SPLIT_VIEW
  ${CMAKE_CURRENT_BINARY_DIR}
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/cmake/compile_check.cpp
  CXX_STANDARD 20
)

set(message_queue_source_files
  src/queue.cpp
  src/buffered_queue.cpp
  src/datatype.cpp
  src/debug_print.cpp
  src/concepts.cpp
  src/aggregators.cpp
  src/definitions.cpp
  src/indirection.cpp
)

add_library(message-queue ${message_queue_source_files})
target_include_directories(message-queue PUBLIC include)
target_link_libraries(message-queue PUBLIC MPI::MPI_CXX
                                              message_queue_dependencies kassert::kassert)
target_compile_features(message-queue PUBLIC cxx_std_20)
if(MESSAGE_QUEUE_USE_MY_TEST_FUNCTIONS)
  target_compile_definitions(message-queue PUBLIC MESSAGE_QUEUE_USE_MY_TEST_FUNCTIONS)
endif()
if (NOT MESSAGE_QUEUE_HAS_LAZY_SPLIT_VIEW)
  message(STATUS "Your compiler has std::views::split implementation which is lazy, see the caveats in the README")
  target_compile_definitions(message-queue PUBLIC MESSAGE_QUEUE_SPLIT_VIEW_IS_LAZY)
else()
  message(STATUS "Compiler supports std::views::split and std::views::lazy_split")
endif()
add_library(message-queue::message-queue ALIAS message-queue)

if (CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME OR MESSAGE_QUEUE_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()
