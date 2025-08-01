// Copyright (c) 2021-2025 Tim Niklas Uhl
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
#pragma once

namespace message_queue::internal {
class RequestPool {
public:
    RequestPool(std::size_t capacity = 0) : requests(capacity, MPI_REQUEST_NULL), indices(capacity) {}

    std::optional<std::pair<int, MPI_Request*>> get_some_inactive_request(int hint = -1) {
        if (inactive_requests() == 0) {
            throw std::runtime_error("all slots are full");
            return {};
        }

        if (hint < 0) {
            hint = last_slot.value_or(-1);
            last_slot.reset();
        }

        // first check the hinted position
        if (hint >= 0 && hint < capacity() && requests[hint] == MPI_REQUEST_NULL) {
            add_to_active_range(hint);
            track_max_active_requests();
            return {{hint, &requests[hint]}};
        }
        auto it = std::ranges::find(requests, MPI_REQUEST_NULL);
        if (it == requests.end()) {
            throw std::runtime_error("find(requests) failed");
            return std::nullopt;
        }
        std::size_t i = std::distance(requests.begin(), it);
        add_to_active_range(i);
        track_max_active_requests();
        return {{i, &*it}};

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

    template <typename CompletionFunction>
    void test_some(CompletionFunction&& on_complete = [](int) {}) {
        int outcount = 0;
        max_test_size_ = std::max(max_test_size_, active_range.second - active_range.first);
        MPI_Testsome(active_range.second - active_range.first,  // count
                     requests.data() + active_range.first,      // requests
                     &outcount,                                 // outcount
                     indices.data(),                            // indices
                     MPI_STATUSES_IGNORE                        // statuses
        );
        if (outcount != MPI_UNDEFINED) {
            auto index_offset = active_range.first;
            std::for_each_n(indices.begin(), outcount, [&](int index) {
                // map index to the global index
                index += index_offset;
                remove_from_active_range(index);
                track_max_active_requests();
                on_complete(index);
            });
        }
    }

    int round_robin_index = 0;

    template <typename CompletionFunction>
    bool test_any(CompletionFunction&& on_complete = [](int) {}) {
        int flag = 0;
        int index = 0;
        max_test_size_ = std::max(max_test_size_, active_range.second - active_range.first);
        MPI_Testany(capacity(),        // - round_robin_index,
                                       // active_range.second - active_range.first,  // count
                    requests.data(),   // + round_robin_index,      // requests
                    &index,            // index
                    &flag,             // flag
                    MPI_STATUS_IGNORE  // status
        );
        if (flag && index != MPI_UNDEFINED) {
            // index += round_robin_index;
            remove_from_active_range(index);
            track_max_active_requests();
            on_complete(index);
        }  // else {
        //   MPI_Testany(round_robin_index,
        // 	      requests.data(),
        // 	      &index,
        // 	      &flag,
        // 	      MPI_STATUS_IGNORE
        // 	      );
        //   if (flag && index != MPI_UNDEFINED) {
        //     // index += round_robin_index;
        //     remove_from_active_range(index);
        //     track_max_active_requests();
        //     on_complete(index);
        //   }
        // }
        // round_robin_index++;
        // if (round_robin_index == capacity()) {
        //   round_robin_index = 0;
        // }
        return static_cast<bool>(flag);
    }

    std::size_t active_requests() const {
        return active_requests_;
    }

    std::size_t inactive_requests() const {
        return capacity() - active_requests();
    }

    std::size_t capacity() const {
        return requests.size();
    }

    int max_test_size() const {
        return max_test_size_;
    }

    size_t max_active_requests() const {
        return max_active_requests_;
    }

private:
    void track_max_active_requests() {
        max_active_requests_ = std::max(max_active_requests_, active_requests());
    }

    void add_to_active_range(int index) {
        active_requests_++;
        active_range.first = std::min(index, active_range.first);
        active_range.second = std::max(index + 1, active_range.second);
    }

    void remove_from_active_range(int index) {
        last_slot = index;
        active_requests_--;
        if (index == active_range.first) {
            active_range.first++;
        } else {
            if (index + 1 == active_range.second) {
                active_range.second--;
            }
        }
    }
    std::vector<MPI_Request> requests;
    std::vector<int> indices;
    std::optional<int> last_slot;
    size_t inactive_request_pointer = 0;
    size_t active_requests_ = 0;
    std::pair<int, int> active_range = {0, 0};
    int max_test_size_ = 0;
    std::size_t max_active_requests_ = 0;
};
}  // namespace message_queue::internal
