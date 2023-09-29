find_package(MPI REQUIRED)

include(cmake/CPM.cmake)

set(MESSAGE_QUEUE_ASSERTION_LEVEL
    $<IF:$<CONFIG:Debug>,"normal","exceptions">
    CACHE STRING "Assertion level")
set_property(CACHE MESSAGE_QUEUE_ASSERTION_LEVEL
             PROPERTY STRINGS none exceptions light normal heavy)
message(STATUS "Assertion level: ${MESSAGE_QUEUE_ASSERTION_LEVEL}")
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
if(NOT TARGET kassert)
  cpmaddpackage("gh:kamping-site/kassert#e683aef")
endif()

option(MESSAGE_QUEUE_BUILD_BOOST
       "Build Boost from source, instead of using find_package" OFF)

if(NOT MESSAGE_QUEUE_BUILD_BOOST)
  find_package(Boost COMPONENTS mpi headers)
  if(Boost_FOUND)
    add_library(message_queue_boost_dependencies INTERFACE)
    target_link_libraries(message_queue_boost_dependencies
                          INTERFACE Boost::mpi Boost::headers)
  else()
    message(
      FATAL_ERROR
        "Boost not on your system. Please set MESSAGE_QUEUE_BUILD_BOOST to ON to build it from source."
    )
  endif()
else()
  message(
    STATUS
      "Boost not found locally, downloading it and building from source. This may take several minutes."
  )
  set(BOOST_ENABLE_MPI ON)
  set(BOOST_INCLUDE_LIBRARIES mpi circular_buffer)
  cpmaddpackage(
    NAME
    boost
    VERSION
    1.82.0
    URL
    "https://github.com/boostorg/boost/releases/download/boost-1.82.0/boost-1.82.0.tar.xz"
  )
  add_library(message_queue_boost_dependencies INTERFACE)
  target_link_libraries(message_queue_boost_dependencies
                        INTERFACE Boost::mpi Boost::circular_buffer)
endif()
cpmaddpackage(
  NAME range-v3
  URL https://github.com/ericniebler/range-v3/archive/0.12.0.zip
  VERSION 0.12.0
  DOWNLOAD_ONLY TRUE)
if (range-v3_ADDED)
  add_library(range-v3 INTERFACE IMPORTED)
  target_include_directories(range-v3 INTERFACE ${range-v3_SOURCE_DIR}/include)
  add_library(range-v3::range-v3 ALIAS range-v3)
endif()

if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME OR MESSAGE_QUEUE_BUILD_EXAMPLES)
  cpmaddpackage("gh:CLIUtils/CLI11@2.3.2")
  cpmaddpackage("gh:fmtlib/fmt#10.0.0")
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

  if(MESSAGE_QUEUE_BACKTRACE)
    cpmaddpackage("gh:kamping-site/bakward-mpi#89de113")
  endif()
endif()
