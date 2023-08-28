# message-queue

Asynchronous (buffering) MPI message queue implementation 📬

## Usage
This library is header only. You need a C++ 20 ready compiler (tested with GCC11 and GCC13) and Boost. If you do not have a suitable Boost version installed, the message queue can be configured to build it for you (this may take some time). If you want this, set the CMake cache variable `MESSAGE_QUEUE_BUILD_BOOST` to `ON`.

To use it in your project, include this repo using `FetchContent`, as `git submodule` or your preferred way of CMake dependency management and (if needed) include it as subdirectory[^1] . You can link against it using

``` cmake
target_link_libraries(<your-target> PRIVATE message-queue::message_queue)
```

You most likely want to use the most current implementation with buffering enabled. To do so, include `message-queue/buffered_queue_v2.h`. For a usage example, see `message_buffering_example.cpp`.

[^1]: Note that this project itself uses [CPM.cmake](https://github.com/cpm-cmake/CPM.cmake) for its dependencies. By default, this requires an internet connection during CMake's configure step, which may not be feasible if your system is behind a firewall. In this case, configure the project locally with the cache variable `CPM_SOURCE_CACHE` set to a location where to download the dependencies. You can then transfer this directory to the remote machine and use it at as a source cache there.
