cmake_minimum_required(VERSION 3.15)
project(message-queue LANGUAGES CXX)

option(MESSAGE_QUEUE_BLOCKING_RECEIVE "Receive messages blocking" OFF)
option(MESSAGE_QUEUE_MATCHED_RECV "Use MPI_Mrecv" OFF)
option(MESSAGE_QUEUE_BACKTRACE "Enable stacktraces" ON)
message(STATUS BLOCKING_RECEIVE ${MESSAGE_QUEUE_BLOCKING_RECEIVE})

find_package(MPI REQUIRED)
find_package(Boost REQUIRED COMPONENTS mpi)

# set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer
# -fsanitize=address ") set (CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS}
# -fno-omit-frame-pointer -fsanitize=address ")

include(cmake/CPM.cmake)

cpmaddpackage("gh:CLIUtils/CLI11@2.3.2")
cpmaddpackage("gh:fmtlib/fmt#10.0.0")
cpmaddpackage("gh:KarlsruheGraphGeneration/KaGen#7dd137e")
set(MESSAGE_QUEUE_ASSERTION_LEVEL
    $<IF:$<CONFIG:Debug>,"normal","exceptions">
    CACHE STRING "Assertion level")
set_property(CACHE MESSAGE_QUEUE_ASSERTION_LEVEL
             PROPERTY STRINGS none exceptions light normal heavy)
message(STATUS "Assertion level: ${KAMPING_ASSERTION_LEVEL}")
string(
  CONCAT KASSERT_ASSERTION_LEVEL
         $<$<STREQUAL:${MESSAGE_QUEUE_ASSERTION_LEVEL},"none">:0>
         $<$<STREQUAL:"${MESSAGE_QUEUE_ASSERTION_LEVEL}","none">:0>
         $<$<STREQUAL:${MESSAGE_QUEUE_ASSERTION_LEVEL},"exceptions">:10>
         $<$<STREQUAL:"${MESSAGE_QUEUE_ASSERTION_LEVEL}","exceptions">:10>
         $<$<STREQUAL:${MESSAGE_QUEUE_ASSERTION_LEVEL},"light">:20>
         $<$<STREQUAL:"${MESSAGE_QUEUE_ASSERTION_LEVEL}","light">:20>
         $<$<STREQUAL:${MESSAGE_QUEUE_ASSERTION_LEVEL},"normal">:30>
         $<$<STREQUAL:"${MESSAGE_QUEUE_ASSERTION_LEVEL}","normal">:30>
         $<$<STREQUAL:${MESSAGE_QUEUE_ASSERTION_LEVEL},"heavy">:60>
         $<$<STREQUAL:"${MESSAGE_QUEUE_ASSERTION_LEVEL}","heavy">:60>)
cpmaddpackage("gh:kamping-site/kassert#e683aef")
cpmaddpackage(
  NAME
  sanitizers-cmake
  GITHUB_REPOSITORY
  "arsenm/sanitizers-cmake"
  GIT_TAG
  c3dc841
  DOWNLOAD_ONLY
  TRUE)

if(sanitizers-cmake_ADDED)
  list(APPEND CMAKE_MODULE_PATH ${sanitizers-cmake_SOURCE_DIR}/cmake)
endif()
find_package(Sanitizers)

#
# set(GRAPH_IO_GENERATORS OFF) add_subdirectory(extern/graph-io)

# if (NOT TARGET backward) add_subdirectory(extern/backward-cpp) endif() if (NOT
# TARGET CLI11) add_subdirectory(extern/cli11) endif()

add_library(message-queue INTERFACE)
target_include_directories(message-queue INTERFACE include)
target_link_libraries(message-queue INTERFACE MPI::MPI_CXX Boost::mpi)
target_compile_options(message-queue INTERFACE -std=c++17)
if(MESSAGE_QUEUE_BLOCKING_RECEIVE)
  target_compile_definitions(message-queue
                             INTERFACE MESSAGE_QUEUE_BLOCKING_RECEIVE)
endif()
if(MESSAGE_QUEUE_MATCHED_RECV)
  target_compile_definitions(message-queue INTERFACE MESSAGE_QUEUE_MATCHED_RECV)
endif()
add_executable(example example.cpp)
target_link_libraries(example message-queue)

# add_executable(asynchronous asynchronous.cpp)
# target_link_libraries(asynchronous MPI::MPI_CXX)

add_executable(bfs bfs.cpp)
target_link_libraries(bfs PUBLIC message-queue KaGen::KaGen CLI11::CLI11
                                 kassert::kassert fmt::fmt)
if(MESSAGE_QUEUE_BACKTRACE)
  cpmaddpackage("gh:kamping-site/bakward-mpi#a032e21")
  target_link_libraries(bfs PUBLIC Backward::Backward)
endif()
add_sanitizers(bfs)
