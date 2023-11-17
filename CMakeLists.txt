cmake_minimum_required(VERSION 3.24)
project(message-queue LANGUAGES CXX)

option(MESSAGE_QUEUE_BACKTRACE "Enable stacktraces" ON)
option(MESSAGE_QUEUE_USE_MY_TEST_FUNCTIONS
       "Use self-implemented variants of Test_some and Test_any" OFF)

option(
  MESSAGE_QUEUE_USE_BOOST
  "Use boost.mpi for automatic mpi type deduction instead off LGPL licensed parts of KaMPIng."
  OFF)

include(cmake/dependencies.cmake)

find_package(MPI REQUIRED)

include(cmake/setup_assertion_level.cmake)
find_package(kassert REQUIRED)
if (MESSAGE_QUEUE_USE_BOOST)
  find_package(Boost REQUIRED COMPONENTS mpi)
endif()

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
target_link_libraries(message-queue PUBLIC MPI::MPI_CXX kassert::kassert)
if (MESSAGE_QUEUE_USE_BOOST)
  target_link_libraries(message-queue PUBLIC Boost::mpi)
  target_compile_definitions(message-queue PUBLIC MESSAGE_QUEUE_USE_BOOST)
else()
  target_link_libraries(message-queue PUBLIC kamping::kamping)
endif()
target_compile_features(message-queue PUBLIC cxx_std_20)
if(MESSAGE_QUEUE_USE_MY_TEST_FUNCTIONS)
  target_compile_definitions(message-queue
                             PUBLIC MESSAGE_QUEUE_USE_MY_TEST_FUNCTIONS)
endif()
add_library(message-queue::message-queue ALIAS message-queue)

if (CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME OR MESSAGE_QUEUE_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()
