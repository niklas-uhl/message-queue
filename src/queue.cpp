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

    // first check the hinted position
    if (hint >= 0 && hint < capacity() && requests[hint] == MPI_REQUEST_NULL) {
        add_to_active_range(hint);
        track_max_active_requests();
        return {{hint, &requests[hint]}};
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
