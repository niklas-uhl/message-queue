// Copyright (c) 2021-2023 Tim Niklas Uhl
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#include "message-queue/queue.hpp"

namespace message_queue {
size_t internal::comm_size(MPI_Comm comm) {
    int size;
    MPI_Comm_size(comm, &size);
    return static_cast<size_t>(size);
}

std::optional<std::pair<int, MPI_Request*>> internal::RequestPool::get_some_inactive_request(int hint) {
    if (inactive_requests() == 0) {
        return {};
    }

    if (hint < 0) {
      // spdlog::info("using last_slot={} as hint", last_slot);
      hint = last_slot;
    } else {
      // spdlog::info("explicit hint={}", hint);
    }

    // first check the hinted position
    if (hint >= 0 && hint < capacity() && requests[hint] == MPI_REQUEST_NULL) {
        // spdlog::info("successfully used hint {}", hint);
        add_to_active_range(hint);
        track_max_active_requests();
        return {{hint, &requests[hint]}};
    }
    if (hint >= 0) {
      if (hint >= capacity()) {
        // spdlog::info("Hint {} not used, because invalid", hint);
      } else {
	spdlog::info("Hint {} not uses, because slot already had a request", hint);
      }
    }

    // first try to find a request in the active range if there is one
    if (active_requests_ < active_range.second - active_range.first) {
        for (auto i = active_range.first; i < active_range.second; i++) {
            if (requests[i] == MPI_REQUEST_NULL) {
                add_to_active_range(i);
                track_max_active_requests();
                return {{i, &requests[i]}};
            }
        }
    }
    // search right of the active range
    for (auto i = active_range.second; i < capacity(); i++) {
        if (requests[i] == MPI_REQUEST_NULL) {
            add_to_active_range(i);
            track_max_active_requests();
            return {{i, &requests[i]}};
        }
    }
    // search left of the active range starting at active_range.first
    for (auto i = active_range.first - 1; i >= 0; i--) {
        if (requests[i] == MPI_REQUEST_NULL) {
            add_to_active_range(i);
            track_max_active_requests();
            return {{i, &requests[i]}};
        }
    }
    // this should never happen
    return {};
}
}  // namespace message_queue
