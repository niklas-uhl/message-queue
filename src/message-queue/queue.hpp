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

#include <mpi.h>
#include <algorithm>
#include <concepts>
#include <cstddef>
#include <deque>
#include <kamping/environment.hpp>
#include <kamping/mpi_datatype.hpp>
#include <kassert/kassert.hpp>
#include <limits>
#include <optional>
#include <span>
#include <utility>
#include <vector>
#include "./message_handles.hpp"
#include "./request_pool.hpp"
#include "./termination_counter.hpp"
#include "message-queue/concepts.hpp"

namespace message_queue {

static constexpr std::size_t DEFAULT_POLL_SKIP_THRESHOLD = 100;

enum class ReceiveMode : std::uint8_t { poll, posted_receives, persistent };

enum class TerminationState : std::uint8_t { active, trying_termination, terminated };

template <MPIType T,
          MPIBuffer<T> MessageContainer = std::vector<T>,
          MPIBuffer<T> ReceiveBufferContainer = std::vector<T>>
class MessageQueue {
    bool try_send_something_from_message_box(int hint = -1) {
        if (outgoing_message_box.empty()) {
            return false;
        }
        internal::handles::SendHandle<T, MessageContainer>& message_to_send = outgoing_message_box.front();
        if (auto returned_handle = try_send_something(hint, std::move(message_to_send))) {
            message_to_send = std::move(*returned_handle);
            return false;
        }
        outgoing_message_box.pop_front();
        return true;
    }

    auto try_send_something(int hint, internal::handles::SendHandle<T, MessageContainer>&& message_to_send)
        -> std::optional<internal::handles::SendHandle<T, MessageContainer>> {
        if (auto result = request_pool.get_some_inactive_request(hint)) {
            // internal::handles::SendHandle<T, MessageContainer> message_to_send =
            //     std::move(outgoing_message_box.front());

            auto [index, req_ptr] = *result;
            message_to_send.set_request(req_ptr);
            KASSERT(!in_transit_messages[index].message().has_value());
            in_transit_messages[index].swap(message_to_send);
            in_transit_messages[index].initiate_send();
            return std::nullopt;
        }
        return std::move(message_to_send);
    }

    void setup_receives() {
        for (size_t i = 0; i < receive_buffers.size(); i++) {
            auto& req = receive_requests[i];
            cancel_receive(i);
            auto& buf = receive_buffers[i];
            buf.resize(reserved_receive_buffer_size_);
            init_receive(i);
            restart_receive(i);
        }
    }
    void init_receive(size_t index) {
        switch (receive_mode_) {
            case ReceiveMode::poll:
            case ReceiveMode::posted_receives:
                return;
            case ReceiveMode::persistent:
                auto& buf = receive_buffers[index];
                auto& req = receive_requests[index];
                MPI_Recv_init(buf.data(), reserved_receive_buffer_size_, kamping::mpi_datatype<T>(), MPI_ANY_SOURCE,
                              SMALL_MESSAGE_TAG, comm_, &req);
                return;
        }
    }

    void restart_receive(size_t index) {
        auto& req = receive_requests[index];
        auto& buf = receive_buffers[index];
        switch (receive_mode_) {
            case ReceiveMode::poll:
                return;
            case ReceiveMode::posted_receives:
                MPI_Irecv(buf.data(), reserved_receive_buffer_size_, kamping::mpi_datatype<T>(), MPI_ANY_SOURCE,
                          SMALL_MESSAGE_TAG, comm_, &req);
                return;
            case ReceiveMode::persistent:
                MPI_Start(&req);
                return;
        }
    }

    void cancel_receive(size_t index) {
        if (receive_mode_ == ReceiveMode::poll) {
            return;
        }
        auto& req = receive_requests[index];
        if (req != MPI_REQUEST_NULL) {
            MPI_Cancel(&req);
            if (receive_mode_ == ReceiveMode::persistent) {
                MPI_Request_free(&req);
            }
        }
    }

    // void cleanup_receives(MessageHandler<T, std::span<T>> auot&& on_message) {
    //     for (MPI_Request& req : receive_requests) {
    //         assert(req != MPI_REQUEST_NULL)
    //             // if (req != MPI_REQUEST_NULL) {
    //             MPI_Cancel(&req);
    //         // }
    //     }
    //     MPI_Waitall(static_cast<int>(receive_requests.size()), receive_requests.data(), statuses.data());
    //     for (auto& [req, buf, status] : std::views::zip(receive_requests, receive_buffers, statuses)) {
    //         int cancelled = false;
    //         MPI_Test_cancelled(&status, &cancelled);
    //         if (!cancelled) {
    // 	        local_message_count.receive++;
    //             int count = 0;
    //             MPI_Get_count(&status, kamping::mpi_datatype<T>(), &count);
    //             auto message = std::span(buffer).first(count);
    //             on_message(
    //                 MessageEnvelope<decltype(message)>{std::move(message), status.MPI_SOURCE, rank_,
    //                 status.MPI_TAG});
    //         }
    //         MPI_Requests_free(&req);
    //     }
    // }

public:
    MessageQueue(MPI_Comm comm, size_t num_request_slots, size_t reserved_receive_buffer_size, ReceiveMode receive_mode)
        : outgoing_message_box(),
          request_pool(num_request_slots),
          in_transit_messages(num_request_slots),
          messages_to_receive(),
          reserved_receive_buffer_size_(reserved_receive_buffer_size),
          receive_buffers(num_request_slots),
          receive_requests(num_request_slots, MPI_REQUEST_NULL),
          indices(num_request_slots),
          statuses(num_request_slots),
          termination_(comm),
          comm_(comm),
          receive_mode_(receive_mode) {
        MPI_Comm_rank(comm_, &rank_);
        MPI_Comm_size(comm_, &size_);

        // ensure that we build the datatype as early as possible
        // cast to void to silence nodiscard warning
        static_cast<void>(kamping::mpi_datatype<T>());
        setup_receives();
    }

    MessageQueue(MessageQueue&&) = default;
    MessageQueue(MessageQueue const&) = delete;
    MessageQueue& operator=(MessageQueue&& other) {
        for (size_t i = 0; i < receive_requests.size(); i++) {
            cancel_receive(i);
        }

        this->outgoing_message_box = std::move(other.outgoing_message_box);
        this->request_pool = std::move(other.request_pool);
        this->in_transit_messages = other.in_transit_messages;
        this->messages_to_receive = std::move(other.messages_to_receive);
        this->reserved_receive_buffer_size_ = other.reserved_receive_buffer_size_;
        this->receive_buffers = std::move(other.receive_buffers);
        this->receive_requests = std::move(other.receive_requests);
        this->indices = std::move(other.indices);
        this->statuses = std::move(other.statuses);
        this->request_id_ = other.request_id_;
        this->comm_ = other.comm_;
        this->rank_ = other.rank_;
        this->size_ = other.size_;
        this->LARGE_MESSAGE_TAG = other.LARGE_MESSAGE_TAG;
        this->SMALL_MESSAGE_TAG = other.SMALL_MESSAGE_TAG;
        this->receive_mode_ = other.receive_mode_;
        this->allow_large_messages_ = other.allow_large_messages_;
        this->termination_state_ = other.termination_state_;
        return *this;
    }
    MessageQueue& operator=(MessageQueue const&) = delete;

    // MessageQueue(MPI_Comm comm = MPI_COMM_WORLD)
    // : MessageQueue(comm, internal::comm_size(comm), 32 * 1024 / sizeof(T)) {}

    ~MessageQueue() {
        if (receive_mode_ == ReceiveMode::poll) {
            return;
        }
        for (size_t i = 0; i < receive_requests.size(); i++) {
            cancel_receive(i);
        }
    }

    [[nodiscard]] size_t reserved_receive_buffer_size() const {
        return reserved_receive_buffer_size_;
    }

    void reserved_receive_buffer_size(size_t size) {
        reserved_receive_buffer_size_ = size;
        setup_receives();
    }

    void allow_large_messages(bool allow = true) {
        allow_large_messages_ = allow;
    }

    [[nodiscard]] size_t message_box_capacity() const {
        return message_box_capacity_;
    }

    void message_box_capacity(size_t capacity) {
        message_box_capacity_ = capacity;
    }

    [[nodiscard]] std::size_t empty_send_slots() const {
        return request_pool.inactive_requests();
    }

    [[nodiscard]] std::size_t used_send_slots() const {
        return request_pool.active_requests();
    }

    [[nodiscard]] std::size_t remaining_message_box_capacity() const {
        if (message_box_capacity_ == std::numeric_limits<std::size_t>::max()) {
            return std::numeric_limits<std::size_t>::max();
        }
        return message_box_capacity_ - outgoing_message_box.size();
    }

    [[nodiscard]] std::size_t total_remaining_capacity() const {
        if (remaining_message_box_capacity() == std::numeric_limits<std::size_t>::max()) {
            return std::numeric_limits<std::size_t>::max();
        }
        return empty_send_slots() + remaining_message_box_capacity();
    }

    /// Post a message to the message queue
    /// Posting a message may fail if the message box is full and no send slots are available.
    /// @return an optional containing the request id if the message was successfully posted, otherwise nullopt
    auto post_message(MessageContainer&& message, PEID receiver) -> std::optional<std::size_t> {
        // TODO check if everything works if the message is empty
        // if (message.size() == 0) {
        //     return std::nullopt;
        // }
        internal::handles::SendHandle<T, MessageContainer> handle(comm_);
        int tag = SMALL_MESSAGE_TAG;
        if (message.size() > reserved_receive_buffer_size_) {
            if (!allow_large_messages_) {
                throw std::runtime_error{"Large messages not allowed, enable them using allow_large_messages"};
            }
            tag = LARGE_MESSAGE_TAG;
        }
        handle.set_tag(tag);
        handle.set_message(std::move(message));
        handle.set_receiver(receiver);
        handle.set_request_id(this->request_id_);

        if (outgoing_message_box.empty() && empty_send_slots() > 0) {
            // we can try to send directly
            auto returned_handle = try_send_something(-1, std::move(handle));
            KASSERT(!returned_handle.has_value(),
                    "This should always succeed since we checked for an empty slot before.");
            auto receipt = std::optional{this->request_id_};
            this->request_id_++;
            termination_.track_send();
            return receipt;
        }
        // try appending to the message box
        try_send_something_from_message_box();  // ensure that we don't "waste" an empty slot
        if (outgoing_message_box.size() >= message_box_capacity_) {
            // the message box is full
            return std::nullopt;
        }
        outgoing_message_box.emplace_back(std::move(handle));
        auto receipt = std::optional{this->request_id_};
        this->request_id_++;
        try_send_something_from_message_box();
        termination_.track_send();
        return receipt;
    }

    /// For single element messages
    auto post_message(T message, PEID receiver) -> std::optional<std::size_t> {
        MessageContainer message_vector{std::move(message)};
        return post_message(std::move(message_vector), receiver);
    }

    bool progress_sending(SendFinishedCallback<MessageContainer> auto on_finished_sending) {
        constexpr bool move_back_message = std::invocable<decltype(on_finished_sending), std::size_t, MessageContainer>;
        // check for finished sends and try starting new ones
        return request_pool.test_any([&](int completed_request_index) {
            std::size_t request_id = in_transit_messages[completed_request_index].get_request_id();
            auto message = in_transit_messages[completed_request_index].extract_message();
            in_transit_messages[completed_request_index].emplace({comm_});
            if constexpr (move_back_message) {
                on_finished_sending(request_id, std::move(message));
            } else {
                on_finished_sending(request_id);
            }
            try_send_something_from_message_box(completed_request_index);
        });
    }

    bool probe_for_messages_persistent(MessageHandler<T, std::span<T>> auto&& on_message) {
        bool something_happenend = false;
        int request_completed = 0;
        // Tests for completion of either one or none of the operations associated with active
        // handles. In the former case, it returns flag = true, returns in index the index of this request
        // in the array, and returns in status the status of that operation.
        // In the latter case (no operation completed), it returns flag
        // = false, returns a value of MPI_UNDEFINED in index and status is undefined.
        // The array may contain null or inactive handles. If the array contains no active handles
        // then the call returns immediately with flag = true, index = MPI_UNDEFINED, and an empty
        // status.
        int err = MPI_Testany(static_cast<int>(receive_requests.size()), receive_requests.data(), indices.data(),
                              &request_completed, statuses.data());
        if (request_completed) {
            KASSERT(indices[0] != MPI_UNDEFINED,
                    "This should not happen, because we always have pending receive requests.");
            something_happenend = true;
            termination_.track_receive();
            auto request_index = indices[0];
            auto& status = statuses[0];
            auto& buffer = receive_buffers[request_index];
            int count = 0;
            MPI_Get_count(&status, kamping::mpi_datatype<T>(), &count);
            auto message = std::span(buffer).first(count);

            on_message(
                MessageEnvelope<decltype(message)>{std::move(message), status.MPI_SOURCE, rank_, status.MPI_TAG});

            restart_receive(request_index);
        }
        return something_happenend;
    }

    bool probe_for_message_probing(MessageHandler<T, std::span<T>> auto&& on_message,
                                   std::size_t max_probe_rounds = 1) {
        if (max_probe_rounds == 1) {
            return probe_for_one_message(on_message, MPI_ANY_SOURCE, SMALL_MESSAGE_TAG);
        }
        bool something_happenend = false;

        size_t num_recv_requests = 0;
        auto receive_chunk = [&] {
            MPI_Waitall(static_cast<int>(num_recv_requests), receive_requests.data(), MPI_STATUSES_IGNORE);

            for (size_t i = 0; i < messages_to_receive.size(); i++) {
                auto& handle = messages_to_receive[i];
                auto& buffer = receive_buffers[i];
                termination_.track_receive();
                auto message = std::span(buffer).first(handle.message_size());
                on_message(MessageEnvelope{std::move(message), handle.sender(), rank_, handle.tag()});
            }
            messages_to_receive.clear();
        };
        size_t rounds = 0;
        while (auto probe_result = internal::handles::probe(comm_, MPI_ANY_SOURCE, SMALL_MESSAGE_TAG)) {
            something_happenend = true;
            auto recv_handle = probe_result->template handle<T, ReceiveBufferContainer>();
            recv_handle.set_request(&receive_requests[num_recv_requests]);
            recv_handle.start_receive_into(receive_buffers[num_recv_requests]);
            messages_to_receive.emplace_back(std::move(recv_handle));
            num_recv_requests++;
            if (num_recv_requests >= receive_requests.size()) {
                receive_chunk();
                num_recv_requests = 0;
            }
            rounds++;
            if (rounds >= max_probe_rounds) {
                break;
            }
        }
        receive_chunk();
        return something_happenend;
    }

    bool probe_for_messages(MessageHandler<T, std::span<T>> auto&& on_message) {
        // probe for a single large message
        bool something_happened = false;
        if (allow_large_messages_) {
            something_happened = probe_for_one_message(on_message, MPI_ANY_SOURCE, LARGE_MESSAGE_TAG);
        }
        switch (receive_mode_) {
            case ReceiveMode::poll:
                something_happened |= probe_for_message_probing(on_message);
                break;
            case ReceiveMode::posted_receives:
            case ReceiveMode::persistent:
                something_happened |= probe_for_messages_persistent(on_message);
                break;
        }
        if (something_happened) {
            reactivate();
        }
        return something_happened;
    }

    bool probe_for_one_message(MessageHandler<T, MessageContainer> auto&& on_message,
                               PEID source = MPI_ANY_SOURCE,
                               int tag = MPI_ANY_TAG) {
        if (auto probe_result = internal::handles::probe(comm_, source, tag)) {
            auto recv_handle = probe_result->template handle<T, ReceiveBufferContainer>();
            MPI_Request recv_req = MPI_REQUEST_NULL;
            recv_handle.set_request(&recv_req);
            recv_handle.receive();
            termination_.track_receive();
            on_message(MessageEnvelope{recv_handle.extract_message(), recv_handle.sender(), rank_, recv_handle.tag()});
            return true;
        }
        return false;
    }

    auto poll(MessageHandler<T, MessageContainer> auto&& on_message) -> std::optional<std::pair<bool, bool>> {
        return poll(on_message, [](std::size_t) {});
    }

    auto poll(MessageHandler<T, MessageContainer> auto&& on_message,
              SendFinishedCallback<MessageContainer> auto&& on_finished_sending)
        -> std::optional<std::pair<bool, bool>> {
        bool probe_finished_something = probe_for_messages(std::forward<decltype(on_message)>(on_message));
        bool send_finished_something =
            progress_sending(std::forward<decltype(on_finished_sending)>(on_finished_sending));
        if (send_finished_something || probe_finished_something) {
            return std::pair{send_finished_something, probe_finished_something};
        }
        return std::nullopt;
    }

    auto poll_throttled(MessageHandler<T, MessageContainer> auto&& on_message,
                        SendFinishedCallback<MessageContainer> auto&& on_finished_sending,
                        std::size_t poll_skip_threshold = DEFAULT_POLL_SKIP_THRESHOLD) -> std::optional<std::pair<bool, bool>> {
        static std::size_t poll_count = 0;
        if (poll_count % poll_skip_threshold == 0) {
            poll_count++;
            return poll(std::forward<decltype(on_message)>(on_message),
                        std::forward<decltype(on_finished_sending)>(on_finished_sending));
        }
        return std::nullopt;
    }

    auto poll_throttled(MessageHandler<T, MessageContainer> auto&& on_message,
                        std::size_t poll_skip_threshold = DEFAULT_POLL_SKIP_THRESHOLD) {
        return poll_throttled(std::forward<decltype(on_message)>(on_message), [](std::size_t) {}, poll_skip_threshold);
    }

    void reactivate() {
        if (synchronous_mode_) {
            return;
        }
        termination_state_ = TerminationState::active;
    }

    [[nodiscard]] TerminationState termination_state() const {
        return termination_state_;
    }

    [[nodiscard]] PEID rank() const {
        return rank_;
    }

    [[nodiscard]] PEID size() const {
        return size_;
    }

    [[nodiscard]] MPI_Comm communicator() const {
        return comm_;
    }

    void poll_until_message_box_empty(
        MessageHandler<T, MessageContainer> auto&& on_message,
        SendFinishedCallback<MessageContainer> auto&& on_finished_sending,
        std::predicate<> auto&& should_stop_polling = [] { return false; }) {
        while (!outgoing_message_box.empty()) {
            poll(std::forward<decltype(on_message)>(on_message),
                 std::forward<decltype(on_finished_sending)>(on_finished_sending));
            if (should_stop_polling()) {
                return;
            }
        }
    }

    [[nodiscard]] bool terminate(MessageHandler<T, MessageContainer> auto&& on_message,
                                 SendFinishedCallback<MessageContainer> auto&& on_finished_sending,
                                 std::invocable<> auto&& before_next_message_counting_round_hook,
                                 std::invocable<> auto&& progress_hook) {
        termination_state_ = TerminationState::trying_termination;
        while (true) {
            before_next_message_counting_round_hook();
            if (termination_state_ == TerminationState::active) {
                return false;
            }
            poll_until_message_box_empty(std::forward<decltype(on_message)>(on_message),
                                         std::forward<decltype(on_finished_sending)>(on_finished_sending),
                                         [&] { return termination_state_ == TerminationState::active; });
            if (termination_state_ == TerminationState::active) {
                return false;
            }
            termination_.start_message_counting();
            // poll at least once, so we don't miss any messages
            // if the the message box is empty upon calling this function
            // we never get to poll if message counting finishes instantly
            do {
                poll(std::forward<decltype(on_message)>(on_message),
                     std::forward<decltype(on_finished_sending)>(on_finished_sending));
                progress_hook();
                if (termination_state_ == TerminationState::active) {
                    return false;
                }
            } while (!termination_.message_counting_finished());
            if (termination_.terminated()) {
                termination_state_ = TerminationState::terminated;
                return true;
            }
        }
    }

    [[nodiscard]] bool terminate(MessageHandler<T, MessageContainer> auto&& on_message,
                                 std::invocable<> auto&& before_next_message_counting_round_hook) {
        return terminate(
            std::forward<decltype(on_message)>(on_message), [](std::size_t) {},
            std::forward<decltype(before_next_message_counting_round_hook)>(before_next_message_counting_round_hook),
            []() {});
    }

    bool terminate(MessageHandler<T, MessageContainer> auto&& on_message) {
        return terminate(on_message, [] {});
    }

    /// if this mode is active, no incoming messages will cancel the termination process
    /// this allows using the queue as a somewhat async sparse-all-to-all
    void synchronous_mode(bool use_it = true) {
        synchronous_mode_ = use_it;
    }

private:
    std::deque<internal::handles::SendHandle<T, MessageContainer>> outgoing_message_box;
    std::size_t message_box_capacity_ = std::numeric_limits<size_t>::max();
    internal::RequestPool request_pool;
    std::vector<internal::handles::SendHandle<T, MessageContainer>> in_transit_messages;
    std::vector<internal::handles::ReceiveHandle<T, ReceiveBufferContainer>> messages_to_receive;
    size_t reserved_receive_buffer_size_;
    std::vector<ReceiveBufferContainer> receive_buffers;
    std::vector<MPI_Request> receive_requests;
    std::vector<int> indices;
    std::vector<MPI_Status> statuses;
    internal::TerminationCounter termination_;
    size_t request_id_ = 0;
    MPI_Comm comm_;
    PEID rank_ = 0;
    PEID size_ = 0;
    int LARGE_MESSAGE_TAG = kamping::Environment<>::tag_upper_bound() - 1;
    int SMALL_MESSAGE_TAG = kamping::Environment<>::tag_upper_bound() - 2;
    ReceiveMode receive_mode_;
    bool allow_large_messages_ = false;
    TerminationState termination_state_ = TerminationState::active;
    bool synchronous_mode_ = false;
};

}  // namespace message_queue
