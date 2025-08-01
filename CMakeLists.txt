cmake_minimum_required(VERSION 3.24)
project(message-queue LANGUAGES CXX)

option(MESSAGE_QUEUE_BACKTRACE "Enable stacktraces" ON)

include(cmake/dependencies.cmake)

find_package(MPI REQUIRED)

include(cmake/setup_assertion_level.cmake)
FetchContent_MakeAvailable(kassert kamping)

try_compile(
  MESSAGE_QUEUE_HAS_LAZY_SPLIT_VIEW
  ${CMAKE_CURRENT_BINARY_DIR}
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/cmake/compile_check.cpp
  CXX_STANDARD 23
)

set(message_queue_source_files
  src/queue.cpp
  src/buffered_queue.cpp
  src/concepts.cpp
  src/aggregators.cpp
  src/definitions.cpp
  src/indirection.cpp
)

add_library(message-queue ${message_queue_source_files})
target_include_directories(message-queue PUBLIC include)
target_link_libraries(message-queue PUBLIC MPI::MPI_CXX kassert::kassert)
target_link_libraries(message-queue PUBLIC kamping::kamping)
  
target_compile_features(message-queue PUBLIC cxx_std_23)
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
