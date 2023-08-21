#pragma once

#include <fmt/core.h>
#include <fmt/ranges.h>
#include <mpi.h>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <deque>
#include <kassert/kassert.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>
#include "boost/mpi/datatype.hpp"
#include "debug_print.h"
#include "message-queue/datatype.hpp"
#include "message-queue/debug_print.h"
#include "message-queue/message_statistics.h"

namespace message_queue {
class RequestPool {
public:
    RequestPool(std::size_t capacity = 0) : requests(capacity, MPI_REQUEST_NULL), indices(capacity) {}

    std::optional<std::pair<int, MPI_Request*>> get_some_inactive_request(int hint = -1) {
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

    template <typename CompletionFunction>
    void test_some(CompletionFunction&& on_complete = [](int) {}) {
        int outcount;
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

    template <typename CompletionFunction>
    void test_any(CompletionFunction&& on_complete = [](int) {}) {
        int flag;
        int index;
        max_test_size_ = std::max(max_test_size_, active_range.second - active_range.first);
        MPI_Testany(active_range.second - active_range.first,  // count
                    requests.data() + active_range.first,      // requests
                    &index,                                    // index
                    &flag,                                     // flag
                    MPI_STATUS_IGNORE                          // status
        );
        if (flag && index != MPI_UNDEFINED) {
            index += active_range.first;
            remove_from_active_range(index);
            track_max_active_requests();
            on_complete(index);
        }
    }

    /// my request functions {{{
    template <typename CompletionFunction>
    void my_test_any(CompletionFunction&& on_complete = [](int) {}) {
        max_test_size_ = std::max(max_test_size_, active_range.second - active_range.first);
        for (int i = active_range.first; i < active_range.second; i++) {
            if (requests[i] == MPI_REQUEST_NULL) {
                continue;
            }
            int flag;
            MPI_Test(&requests[i], &flag, MPI_STATUS_IGNORE);
            if (flag) {
                remove_from_active_range(i);
                track_max_active_requests();
                on_complete(i);
                return;
            }
        }
    }

    template <typename CompletionFunction>
    void my_test_some(CompletionFunction&& on_complete = [](int) {}) {
        max_test_size_ = std::max(max_test_size_, active_range.second - active_range.first);
        for (int i = active_range.first; i < active_range.second; i++) {
            if (requests[i] == MPI_REQUEST_NULL) {
                continue;
            }
            int flag;
            MPI_Test(&requests[i], &flag, MPI_STATUS_IGNORE);
            if (flag) {
                remove_from_active_range(i);
                track_max_active_requests();
                on_complete(i);
            }
        }
    }

    /// }}}

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
        if (index < active_range.first) {
            active_range.first = index;
        }
        if (index + 1 > active_range.second) {
            active_range.second = index + 1;
        }
        int rank;
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    }

    void remove_from_active_range(int index) {
        active_requests_--;
        if (index == active_range.first) {
            active_range.first++;
        } else {
            if (index + 1 == active_range.second) {
                active_range.second--;
            }
        }
        int rank;
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    }
    std::vector<MPI_Request> requests;
    std::vector<int> indices;
    size_t inactive_request_pointer = 0;
    size_t active_requests_ = 0;
    std::pair<int, int> active_range = {0, 0};
    int max_test_size_ = 0;
    std::size_t max_active_requests_ = 0;
};

template <typename T>
class MessageQueueV2 {
    template <class U, class Merger, class Splitter>
    friend class BufferedMessageQueue;
    template <class U, class Merger, class Splitter>
    friend class ConcurrentBufferedMessageQueue;
    // static_assert(boost::mpi::is_mpi_builtin_datatype<T>::value, "Only builtin MPI types are supported");

    enum class State { posted, initiated, completed };

    template <class S>
    struct MessageHandle {
        size_t message_size;
        std::vector<S> message;
        MPI_Request* request = nullptr;
        size_t request_id;
        int tag = MPI_ANY_TAG;

        bool test() {
            if (request == nullptr) {
                return false;
            }
            int finished = false;
            int err = MPI_Test(request, &finished, MPI_STATUS_IGNORE);
            check_mpi_error(err, __FILE__, __LINE__);
            if (finished) {
                return true;
            }
            return false;
        }
    };

    template <class S>
    struct SendHandle : MessageHandle<S> {
        PEID receiver = MPI_ANY_SOURCE;

        void initiate_send() {
            // atomic_debug(this->request_id);
            int err =
                MPI_Isend(this->message.data(), this->message.size(), message_queue::mpi_type_traits<S>::get_type(),
                          receiver, this->tag, MPI_COMM_WORLD, this->request);
            check_mpi_error(err, __FILE__, __LINE__);
        }
    };

    template <class S>
    struct ReceiveHandle : MessageHandle<S> {
#ifdef MESSAGE_QUEUE_MATCHED_RECV
        MPI_Message matched_message;
#endif
        MPI_Status status;
        PEID sender = MPI_ANY_SOURCE;

        void start_receive() {
            this->message.resize(this->message_size);
#ifdef MESSAGE_QUEUE_MATCHED_RECV
            MPI_Imrecv(this->message.data(), this->message.size(), message_queue::mpi_type_traits<S>::get_type(),
                       &matched_message, this->request);
#else
            MPI_Irecv(this->message.data(), this->message.size(), message_queue::mpi_type_traits<S>::get_type(),
                      this->sender, this->tag, MPI_COMM_WORLD, this->request);
#endif
        }

        void receive() {
            this->message.resize(this->message_size);
#ifdef MESSAGE_QUEUE_MATCHED_RECV
            MPI_Mrecv(this->message.data(), this->message.size(), message_queue::mpi_type_traits<S>::get_type(),
                      &this->matched_message, MPI_STATUS_IGNORE);
#else
            MPI_Recv(this->message.data(), this->message.size(), message_queue::mpi_type_traits<S>::get_type(),
                     this->sender, this->tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
#endif
        }
    };

    struct ProbeResult {
        MPI_Status status;
        MPI_Message matched_message;
        PEID sender;
        int tag;

        template <typename S>
        ReceiveHandle<S> handle() {
            ReceiveHandle<S> handle;
            int message_size;
            MPI_Get_count(&status, message_queue::mpi_type_traits<S>::get_type(), &message_size);
            handle.message_size = message_size;
#ifdef MESSAGE_QUEUE_MATCHED_RECV
            handle.matched_message = matched_message;
#endif
            handle.tag = status.MPI_TAG;
            handle.sender = status.MPI_SOURCE;
            return handle;
        }
    };

    static std::optional<ProbeResult> probe(PEID source = MPI_ANY_SOURCE, PEID tag = MPI_ANY_TAG) {
        ProbeResult result;
        int message_found = false;
#ifdef MESSAGE_QUEUE_MATCHED_RECV
        MPI_Improbe(source, tag, MPI_COMM_WORLD, &message_found, &result.matched_message, &result.status);
#else
        MPI_Iprobe(source, tag, MPI_COMM_WORLD, &message_found, &result.status);
#endif
        if (message_found) {
            result.sender = result.status.MPI_SOURCE;
            result.tag = result.status.MPI_TAG;
            return result;
        }
        return std::nullopt;
    }

    template <typename S>
    void post_message_impl(std::vector<S>&& message, PEID receiver, int tag) {
        SendHandle<S> handle;
        handle.message = std::move(message);
        handle.receiver = receiver;
        handle.tag = tag;
        handle.request_id = this->request_id_;
        outgoing_message_box.emplace_back(std::move(handle));
        this->request_id_++;
        try_send_something();
    }

    bool try_send_something(int hint = -1) {
        if (outgoing_message_box.empty()) {
            return false;
        }
        if (auto result = request_pool.get_some_inactive_request(hint)) {
            SendHandle<T> message_to_send = std::move(outgoing_message_box.front());
            outgoing_message_box.pop_front();
            auto [index, req_ptr] = *result;
            message_to_send.request = req_ptr;
            // atomic_debug(fmt::format("isend msg={}, to {}, using request {}", message_to_send.message,
            // message_to_send.receiver, index));
            in_transit_messages[index] = std::move(message_to_send);
            in_transit_messages[index].initiate_send();
            KASSERT(*message_to_send.request != MPI_REQUEST_NULL);
            return true;
        }
        return false;
    }

public:
    MessageQueueV2()
        : outgoing_message_box(),
          request_pool(),
          in_transit_messages(),
          messages_to_receive(),
          receive_requests(),
          request_id_(0),
          rank_(0),
          size_(0) {
        MPI_Comm_rank(MPI_COMM_WORLD, &rank_);
        MPI_Comm_size(MPI_COMM_WORLD, &size_);
        request_pool = RequestPool{static_cast<size_t>(size_)};
        in_transit_messages = std::vector<SendHandle<T>>{request_pool.capacity()};
    }

    void post_message(std::vector<T>&& message, PEID receiver, int tag = 0) {
        // atomic_debug(fmt::format("enqueued msg={}, to {}", message, receiver));
        // assert(receiver != rank_);
        if (message.empty()) {
            return;
        }
        assert(tag != control_wave_tag);
        // std::cout << message.size() <<"\n";
        size_t message_size = message.size();
        post_message_impl(std::move(message), receiver, tag);
        stats_.sent_messages.fetch_add(1, std::memory_order_relaxed);
        stats_.send_volume.fetch_add(message_size, std::memory_order_relaxed);
    }

    template <typename MessageHandler>
    bool poll(MessageHandler&& on_message) {
        // atomic_debug("Inner poll");
        static_assert(std::is_invocable_v<MessageHandler, std::vector<T>, PEID>);
        bool something_happenend = false;
        size_t i = 0;
        if (!use_test_any_) {
            auto start = std::chrono::high_resolution_clock::now();
            if (!use_custom_implementation_) {
                request_pool.test_some([&](int completed_request_index) {
                    in_transit_messages[completed_request_index] = {};
                    something_happenend = true;
                    try_send_something(completed_request_index);
                });
            } else {
                request_pool.my_test_some([&](int completed_request_index) {
                    in_transit_messages[completed_request_index] = {};
                    something_happenend = true;
                    try_send_something(completed_request_index);
                });
            }
            auto end = std::chrono::high_resolution_clock::now();
            test_some_time_ += (end - start);
        } else {
            bool progress = true;
            while (progress) {
                progress = false;
                auto start = std::chrono::high_resolution_clock::now();
                if (!use_custom_implementation_) {
                    request_pool.test_any([&](int completed_request_index) {
                        in_transit_messages[completed_request_index] = {};
                        something_happenend = true;
                        progress = true;
                        try_send_something(completed_request_index);
                    });
                } else {
                    request_pool.my_test_any([&](int completed_request_index) {
                        in_transit_messages[completed_request_index] = {};
                        something_happenend = true;
                        progress = true;
                        try_send_something(completed_request_index);
                    });
                }
                auto end = std::chrono::high_resolution_clock::now();
                test_any_time_ += (end - start);
            }
        }
        while (try_send_something()) {
        }
        while (auto probe_result = probe()) {
            something_happenend = true;
            auto recv_handle = probe_result->template handle<T>();
            // atomic_debug(fmt::format("probed msg from {}", recv_handle.sender));
            MPI_Request recv_req;
            recv_handle.request = &recv_req;
            recv_handle.start_receive();
            receive_requests.push_back(recv_req);
            messages_to_receive.emplace_back(std::move(recv_handle));
        }
        auto begin = std::chrono::high_resolution_clock::now();
        MPI_Waitall(static_cast<int>(receive_requests.size()), receive_requests.data(), MPI_STATUSES_IGNORE);
        auto end = std::chrono::high_resolution_clock::now();
        wait_all_time_ += (end - begin);
        receive_requests.clear();
        for (auto& handle : messages_to_receive) {
            // atomic_debug(fmt::format("received msg={} from {}", handle.message, handle.sender));
            stats_.received_messages.fetch_add(1, std::memory_order_relaxed);
            stats_.receive_volume.fetch_add(handle.message.size(), std::memory_order_relaxed);
            on_message(std::move(handle.message), handle.sender);
        }
        messages_to_receive.clear();
        return something_happenend;
    }

    template <typename MessageHandler>
    void terminate(MessageHandler&& on_message) {
        terminate_impl(on_message, []() {});
    }

    const MessageStatistics& stats() {
        return stats_;
    }

    void reset() {
        stats_ = MessageStatistics();
        request_id_ = 0;
        termination_state = TerminationState::active;
        number_of_waves = 0;
    }

    void reactivate() {
        termination_state = TerminationState::active;
    }

    PEID rank() const {
        return rank_;
    }

    PEID size() const {
        return size_;
    }

    template <typename MessageHandler, typename PreWaveHook>
    void terminate_impl(MessageHandler&& on_message, PreWaveHook&& pre_wave) {
        // atomic_debug("Inner terminate");
        std::pair<size_t, size_t> global_count = {0, 0};
        int wave_count = 0;
        while (true) {
            pre_wave();
            while (!outgoing_message_box.empty()) {
                poll(on_message);
            }
            // atomic_debug("Handles empty");
            std::pair<size_t, size_t> local_count = {stats_.sent_messages, stats_.received_messages};
            std::pair<size_t, size_t> reduced_count;
            MPI_Request reduce_request;
            MPI_Iallreduce(&local_count, &reduced_count, 2, message_queue::mpi_type_traits<size_t>::get_type(), MPI_SUM,
                           MPI_COMM_WORLD, &reduce_request);
            wave_count++;
            int reduce_finished = false;
            while (!reduce_finished) {
                poll(on_message);
                if (reduce_request == MPI_REQUEST_NULL) {
                    throw "Error";
                }
                int err = MPI_Test(&reduce_request, &reduce_finished, MPI_STATUS_IGNORE);
                if (err != MPI_SUCCESS) {
                    throw "Error";
                }
            }
            if (rank_ == 0) {
                // atomic_debug("Wave count " + std::to_string(wave_count));
            }
            if (reduced_count == global_count && global_count.first == global_count.second) {
                break;
            } else {
                global_count = reduced_count;
            }
        }
    }

    template <typename MessageHandler, typename PreWaveHook>
    bool try_terminate_impl(MessageHandler&& on_message, PreWaveHook&& pre_wave) {
        if (size_ == 1) {
            return true;
        }
        termination_state = TerminationState::trying_termination;
        int wave_count = 0;
        while (true) {
            pre_wave();
            while (!outgoing_message_box.empty()) {
                poll(on_message);
                // atomic_debug("Poll before");
                if (termination_state == TerminationState::active) {
                    // atomic_debug("Reactivated");
                    return false;
                }
            }
            std::pair<size_t, size_t> reduced_count;
            if (termination_request == MPI_REQUEST_NULL) {
                local_count = {stats_.sent_messages, stats_.received_messages};
                // atomic_debug("Start reduce");
                MPI_Iallreduce(&local_count, &reduced_count, 2, message_queue::mpi_type_traits<size_t>::get_type(), MPI_SUM,
                               MPI_COMM_WORLD, &termination_request);
            }
            wave_count++;
            int reduce_finished = false;
            while (!reduce_finished) {
                poll(on_message);
                // atomic_debug("Poll after inititated");
                int err = MPI_Test(&termination_request, &reduce_finished, MPI_STATUS_IGNORE);
                check_mpi_error(err, __FILE__, __LINE__);
                if (termination_state == TerminationState::active) {
                    // atomic_debug("Reactivated");
                    return false;
                }
            }
            // atomic_debug("Reduce finished");
            if (rank_ == 0) {
                // atomic_debug("Wave count " + std::to_string(wave_count));
            }
            if (reduced_count == global_count && global_count.first == global_count.second && global_count.first != 0) {
                // atomic_debug("Terminated");
                // atomic_debug(reduced_count);
                termination_state = TerminationState::terminated;
                return true;
            } else {
                global_count = reduced_count;
            }
        }
    }

    auto wait_all_time() const {
        return wait_all_time_;
    }

    auto test_some_time() const {
        return test_some_time_;
    }

    auto test_any_time() const {
        return test_any_time_;
    }

    auto test_time() const {
        if (use_test_any_) {
            return test_any_time();
        } else {
            return test_some_time();
        }
    }

    auto max_test_size() const {
        return request_pool.max_test_size();
    }

    auto max_active_requests() const {
        return request_pool.max_active_requests();
    }

    void use_test_any(bool use_it = true) {
        use_test_any_ = use_it;
    }

    void use_custom_implementation(bool use_it = true) {
        use_custom_implementation_ = use_it;
    }

private:
    enum class TerminationState { active, trying_termination, terminated };
    std::deque<SendHandle<T>> outgoing_message_box;
    RequestPool request_pool;
    std::vector<SendHandle<T>> in_transit_messages;
    std::vector<ReceiveHandle<T>> messages_to_receive;
    std::vector<MPI_Request> receive_requests;
    std::pair<size_t, size_t> local_count;
    std::pair<size_t, size_t> reduced_count;
    std::pair<size_t, size_t> global_count = {std::numeric_limits<size_t>::max(),
                                              std::numeric_limits<size_t>::max() - 1};
    MPI_Request termination_request = MPI_REQUEST_NULL;
    MessageStatistics stats_;
    int static const control_wave_tag = 478;
    size_t request_id_;
    PEID rank_;
    PEID size_;
    std::chrono::high_resolution_clock::duration wait_all_time_;
    std::chrono::high_resolution_clock::duration test_some_time_;
    std::chrono::high_resolution_clock::duration test_any_time_;
    TerminationState termination_state = TerminationState::active;
    size_t number_of_waves = 0;
    bool use_test_any_ = false;
    bool use_custom_implementation_ = false;
};

}  // namespace message_queue
