# 📬 message-queue

Asynchronous (buffering) MPI message queue implementation 📬

This has been developed as part of our work on [distributed triangle counting](https://github.com/niklas-uhl/katric).

If you use this code in the context of an academic publication, we kindly ask you to cite [the corresponding paper](https://doi.org/10.1109/IPDPS54959.2023.00076):

```bibtex
@inproceedings{sanders2023
  author       = {Peter Sanders and
                  Tim Niklas Uhl},
  title        = {Engineering a Distributed-Memory Triangle Counting Algorithm},
  booktitle    = {{IEEE} International Parallel and Distributed Processing Symposium,
                  {IPDPS} 2023, St. Petersburg, FL, USA, May 15-19, 2023},
  pages        = {702--712},
  publisher    = {{IEEE}},
  year         = {2023},
  url          = {https://doi.org/10.1109/IPDPS54959.2023.00076},
  doi          = {10.1109/IPDPS54959.2023.00076},
}
```

You can also find a [freely accessible postprint in the arXiv](https://arxiv.org/abs/2302.11443).

## Usage
This library is header only. You need a C++ 20 ready compiler (tested with GCC11 and GCC13) and Boost. If you do not have a suitable Boost version installed, the message queue can be configured to build it for you (this may take some time). If you want this, set the CMake cache variable `MESSAGE_QUEUE_BUILD_BOOST` to `ON`.

To use it in your project, include this repo using `FetchContent`, as `git submodule` or your preferred way of CMake dependency management and (if needed) include it as subdirectory[^1] . You can link against it using

``` cmake
# if using git submodules
add_subdirectory(path/to/submodule)

# if using FetchContent
FetchContent_Declare(message-queue
  GIT_REPOSITORY https://github.com/niklas-uhl/message-queue
  GIT_TAG main # or current commit
)
FetchContent_MakeAvailable(message-queue)

# using CPM (recommended)
CPMAddPackage("gh:niklas-uhl/message-queue#master")  # or current commit

# link against the target
target_link_libraries(<your-target> PRIVATE message-queue::message-queue)
```

You most likely want to use the most current implementation with buffering enabled. To do so, include `message-queue/buffered_queue_v2.h`. For a usage example, see `message_buffering_example.cpp`.

[^1]: Note that this project itself uses [CPM.cmake](https://github.com/cpm-cmake/CPM.cmake) for its dependencies. By default, this requires an internet connection during CMake's configure step, which may not be feasible if your system is behind a firewall. In this case, configure the project locally with the cache variable `CPM_SOURCE_CACHE` set to a location where to download the dependencies. You can then transfer this directory to the remote machine and use it at as a source cache there.
