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
#include <utility>
#include <vector>

#include "./concepts.hpp"
#include "./message_handles.hpp"
#include "./receiver.hpp"
#include "./request_pool.hpp"
#include "./termination_counter.hpp"

namespace message_queue {

static constexpr std::size_t DEFAULT_POLL_SKIP_THRESHOLD = 100;

enum class TerminationState : std::uint8_t { active, trying_termination, terminated };

template <MPIType T,
          MPIBuffer<T> MessageContainer = std::vector<T>,
          MPIBuffer<T> ReceiveBufferContainer = std::vector<T>>
class MessageQueue {
public:
    MessageQueue(MPI_Comm comm, size_t num_request_slots, size_t reserved_receive_buffer_size)

        : comm_(comm),
          termination_(comm),
          receiver_(comm, SMALL_MESSAGE_TAG, termination_, num_request_slots, reserved_receive_buffer_size),
          large_message_receiver_(comm, LARGE_MESSAGE_TAG, termination_),
          outgoing_message_box_(),
          request_pool(num_request_slots),
          in_transit_messages_(num_request_slots),
          reserved_receive_buffer_size_(reserved_receive_buffer_size) {
        MPI_Comm_rank(comm_, &rank_);
        MPI_Comm_size(comm_, &size_);

        // ensure that we build the datatype as early as possible
        // cast to void to silence nodiscard warning
        static_cast<void>(kamping::mpi_datatype<T>());
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

        if (outgoing_message_box_.empty() && empty_send_slots() > 0) {
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
        if (outgoing_message_box_.size() >= message_box_capacity_) {
            // the message box is full
            return std::nullopt;
        }
        outgoing_message_box_.emplace_back(std::move(handle));
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

    auto poll(MessageHandler<T, MessageContainer> auto&& on_message) -> std::optional<std::pair<bool, bool>> {
        return poll(on_message, [](std::size_t) {});
    }

    auto poll(MessageHandler<T, MessageContainer> auto&& on_message,
              SendFinishedCallback<MessageContainer> auto&& on_finished_sending)
        -> std::optional<std::pair<bool, bool>> {
        bool received_large_message = false;
        if (allow_large_messages_) {
            received_large_message =
                large_message_receiver_.probe_for_one_message(std::forward<decltype(on_message)>(on_message));
        }
        bool received_something =
            receiver_.probe_for_messages(std::forward<decltype(on_message)>(on_message)) || received_large_message;
        if (received_something) {
            reactivate();
        }
        bool send_finished_something =
            progress_sending(std::forward<decltype(on_finished_sending)>(on_finished_sending));
        if (send_finished_something || received_something) {
            return std::pair{send_finished_something, received_something};
        }
        return std::nullopt;
    }

    auto poll_throttled(MessageHandler<T, MessageContainer> auto&& on_message,
                        SendFinishedCallback<MessageContainer> auto&& on_finished_sending,
                        std::size_t poll_skip_threshold = DEFAULT_POLL_SKIP_THRESHOLD)
        -> std::optional<std::pair<bool, bool>> {
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

    [[nodiscard]] size_t reserved_receive_buffer_size() const {
        return reserved_receive_buffer_size_;
    }

    void allow_large_messages(bool allow = true) {
        allow_large_messages_ = allow;
    }

    bool progress_sending(SendFinishedCallback<MessageContainer> auto on_finished_sending) {
        constexpr bool move_back_message = std::invocable<decltype(on_finished_sending), std::size_t, MessageContainer>;
        // check for finished sends and try starting new ones
        return request_pool.test_any([&](int completed_request_index) {
            std::size_t request_id = in_transit_messages_[completed_request_index].get_request_id();
            auto message = in_transit_messages_[completed_request_index].extract_message();
            in_transit_messages_[completed_request_index].emplace({comm_});
            if constexpr (move_back_message) {
                on_finished_sending(request_id, std::move(message));
            } else {
                on_finished_sending(request_id);
            }
            try_send_something_from_message_box(completed_request_index);
        });
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

    [[nodiscard]] std::size_t total_remaining_capacity() const {
        if (remaining_message_box_capacity() == std::numeric_limits<std::size_t>::max()) {
            return std::numeric_limits<std::size_t>::max();
        }
        return empty_send_slots() + remaining_message_box_capacity();
    }

    [[nodiscard]] TerminationState termination_state() const {
        return termination_state_;
    }

private:
    bool try_send_something_from_message_box(int hint = -1) {
        if (outgoing_message_box_.empty()) {
            return false;
        }
        internal::handles::SendHandle<T, MessageContainer>& message_to_send = outgoing_message_box_.front();
        if (auto returned_handle = try_send_something(hint, std::move(message_to_send))) {
            message_to_send = std::move(*returned_handle);
            return false;
        }
        outgoing_message_box_.pop_front();
        return true;
    }

    auto try_send_something(int hint, internal::handles::SendHandle<T, MessageContainer>&& message_to_send)
        -> std::optional<internal::handles::SendHandle<T, MessageContainer>> {
        if (auto result = request_pool.get_some_inactive_request(hint)) {
            // internal::handles::SendHandle<T, MessageContainer> message_to_send =
            //     std::move(outgoing_message_box.front());

            auto [index, req_ptr] = *result;
            message_to_send.set_request(req_ptr);
            KASSERT(!in_transit_messages_[index].message().has_value());
            in_transit_messages_[index].swap(message_to_send);
            in_transit_messages_[index].initiate_send();
            return std::nullopt;
        }
        return std::move(message_to_send);
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
        return message_box_capacity_ - outgoing_message_box_.size();
    }

    void poll_until_message_box_empty(
        MessageHandler<T, MessageContainer> auto&& on_message,
        SendFinishedCallback<MessageContainer> auto&& on_finished_sending,
        std::predicate<> auto&& should_stop_polling = [] { return false; }) {
        while (!outgoing_message_box_.empty()) {
            poll(std::forward<decltype(on_message)>(on_message),
                 std::forward<decltype(on_finished_sending)>(on_finished_sending));
            if (should_stop_polling()) {
                return;
            }
        }
    }
    MPI_Comm comm_;
    int SMALL_MESSAGE_TAG = kamping::Environment<>::tag_upper_bound() - 1;
    int LARGE_MESSAGE_TAG = kamping::Environment<>::tag_upper_bound() - 2;
    internal::TerminationCounter termination_;
    PersistentReceiver<T, ReceiveBufferContainer> receiver_;
    AllocatingProbeReceiver<T, ReceiveBufferContainer> large_message_receiver_;
    std::deque<internal::handles::SendHandle<T, MessageContainer>> outgoing_message_box_;
    std::size_t message_box_capacity_ = std::numeric_limits<size_t>::max();
    internal::RequestPool request_pool;
    std::vector<internal::handles::SendHandle<T, MessageContainer>> in_transit_messages_;
    size_t reserved_receive_buffer_size_;
    size_t request_id_ = 0;
    PEID rank_ = 0;
    PEID size_ = 0;
    bool allow_large_messages_ = false;
    TerminationState termination_state_ = TerminationState::active;
    bool synchronous_mode_ = false;
};

}  // namespace message_queue
