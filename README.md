# message-queue

Asynchronous (buffering) MPI message queue implementation :mailbox-with-mail:

## Usage
This library is header only. You need a C++ 20 ready compiler (tested with GCC11 and GCC13) and Boost. If you do not have a suitable Boost version installed, the message queue can be configured to build it for you (this may take some time). If you want this, set the CMake cache variable `MESSAGE_QUEUE_BUILD_BOOST` to `ON`.

To use it in your project, include this repo using `FetchContent`, as `git submodule` or your preferred way of CMake dependency management and (if needed) include it as subdirectory. You can link against it using

``` cmake
target_link_libraries(<your-target> PRIVATE message-queue::message_queue)
```

You most likely want to use the most current implementation with buffering enabled. To do so, include `message-queue/buffered_queue_v2.h`. For a usage example, see `message_buffering_example.cpp`.

