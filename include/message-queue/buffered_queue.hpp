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

#pragma once

#include <mpi.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <ranges>
#include <unordered_map>
#include <vector>

#include "message-queue/aggregators.hpp"
#include "message-queue/concepts.hpp"
#include "message-queue/queue.hpp"

namespace message_queue {

enum class FlushStrategy { local, global, random, largest };

struct Config {
    size_t num_request_slots = 8;
    ReceiveMode receive_mode = ReceiveMode::persistent;
    FlushStrategy flush_strategy = FlushStrategy::global;
    // size_t global_threshold_bytes = std::numeric_limits<size_t>::max();
    std::size_t local_threshold_bytes = std::numeric_limits<size_t>::max();
};

template <typename MessageType,
          MPIType BufferType = MessageType,
          MPIBuffer<BufferType> BufferContainer = std::vector<BufferType>,
          MPIBuffer<BufferType> ReceiveBufferContainer = std::vector<BufferType>,
          aggregation::Merger<MessageType, BufferContainer> Merger = aggregation::AppendMerger,
          aggregation::Splitter<MessageType, BufferContainer> Splitter = aggregation::NoSplitter,
          aggregation::BufferCleaner<BufferContainer> BufferCleaner = aggregation::NoOpCleaner>
class BufferedMessageQueue {
private:
    using BufferMap = std::unordered_map<PEID, BufferContainer>;

public:
    using message_type = MessageType;
    using buffer_type = BufferType;
    using buffer_container_type = BufferContainer;
    using merger_type = Merger;
    using splitter_type = Splitter;
    using buffer_cleaner_type = BufferCleaner;

    BufferedMessageQueue(MPI_Comm comm = MPI_COMM_WORLD,
                         size_t num_request_slots = 8,
                         ReceiveMode receive_mode = ReceiveMode::persistent,
                         Merger merger = Merger{},
                         Splitter splitter = Splitter{},
                         BufferCleaner cleaner = BufferCleaner{})
        : queue_(comm, num_request_slots, 32 * 1024 / sizeof(BufferType), receive_mode),
          merge(std::move(merger)),
          split(std::move(splitter)),
          pre_send_cleanup(std::move(cleaner)) {
      // spdlog::info("message_size = {}", sizeof(buffer_type));
    }

    BufferedMessageQueue(MPI_Comm comm,
                         Config const& config,
                         Merger merger = Merger{},
                         Splitter splitter = Splitter{},
                         BufferCleaner cleaner = BufferCleaner{})
        : queue_(comm, config.num_request_slots, config.local_threshold_bytes, config.receive_mode),
          merge(std::move(merger)),
          split(std::move(splitter)),
          pre_send_cleanup(std::move(cleaner)) {}

    // BufferedMessageQueue(MPI_Comm comm = MPI_COMM_WORLD,
    //                      Merger merger = Merger{},
    //                      Splitter splitter = Splitter{},
    //                      BufferCleaner cleaner = BufferCleaner{})
    //     : BufferedMessageQueue(comm,
    //                            internal::comm_size(comm),
    //                            std::move(merger),
    //                            std::move(splitter),
    //                            std::move(cleaner)) {}

    BufferedMessageQueue(BufferedMessageQueue&&) = default;
    BufferedMessageQueue(BufferedMessageQueue const&) = delete;
    BufferedMessageQueue& operator=(BufferedMessageQueue&&) = default;
    BufferedMessageQueue& operator=(BufferedMessageQueue const&) = delete;

    /// Note: messages have to be passed as rvalues. If you want to send static
    /// data without an additional copy, wrap it in a std::ranges::ref_view.
    bool post_message(InputMessageRange<MessageType> auto&& message,
                      PEID receiver,
                      PEID envelope_sender,
                      PEID envelope_receiver,
                      int tag) {
        auto it = buffers_.find(receiver);
        if (it == buffers_.end()) {
            it = buffers_.emplace(receiver, BufferContainer{}).first;
        }
        auto& buffer = it->second;
        auto envelope = MessageEnvelope{std::move(message), envelope_sender, envelope_receiver, tag};
        size_t estimated_new_buffer_size;
        if constexpr (aggregation::EstimatingMerger<Merger, MessageType, BufferContainer>) {
            estimated_new_buffer_size = merge.estimate_new_buffer_size(buffer, receiver, queue_.rank(), envelope);
        } else {
            estimated_new_buffer_size = buffer.size() + envelope.message.size();
        }
        auto old_buffer_size = buffer.size();
        bool overflow = false;
        if (check_for_buffer_overflow(buffer, estimated_new_buffer_size - old_buffer_size)) {
            resolve_overflow(it);
            overflow = true;
        }
        PEID rank;
        merge(buffer, receiver, queue_.rank(), std::move(envelope));
        auto new_buffer_size = buffer.size();
        global_buffer_size_ += new_buffer_size - old_buffer_size;
        return overflow;
    }

    /// Note: messages have to be passed as rvalues. If you want to send static
    /// data without an additional copy, wrap it in a std::ranges::ref_view.
    bool post_message(InputMessageRange<MessageType> auto&& message, PEID receiver, int tag = 0) {
        return post_message(std::move(message), receiver, rank(), receiver, tag);
    }

    bool post_message(MessageType message, PEID receiver, int tag = 0) {
        return post_message(std::ranges::views::single(message), receiver, tag);
    }

    /// Flush buffer for \p receiver. If the buffer is empty, or does not exist, this is a no-op.
    /// \param receiver The rank of the receiver
    /// \return true if the buffer had some data to flush, false otherwise
    bool flush_buffer(PEID receiver) {
        auto it = buffers_.find(receiver);
        if (it != buffers_.end()) {
            bool buffer_was_empty = it->second.empty();
            flush_buffer_impl(it);
            return buffer_was_empty;
        }
        return false;
    }

    void flush_all_buffers() {
        flush_all_buffers_impl(buffers_.end(), [] { return false; });
    }

    void flush_all_buffers_and_poll_until_reactivated(MessageHandler<MessageType> auto&& on_message) {
        flush_all_buffers_impl(buffers_.end(), [&] {
            poll(std::forward<decltype(on_message)>(on_message));
            return termination_state() == TerminationState::active;
        });
    }

    void flush_largest_buffer() {
        flush_largest_buffer_impl(buffers_.end());
    }

    /// Note: Message handlers take a MessageEnvelope as single argument. The Envelope
    /// (not necessarily the underlying data) is moved to the handler when
    /// called.
    bool poll(MessageHandler<MessageType> auto&& on_message) {
        return queue_.poll(split_handler(on_message));
    }

    /// Note: Message handlers take a MessageEnvelope as single argument. The Envelope
    /// (not necessarily the underlying data) is moved to the handler when
    /// called.
    [[nodiscard]] bool terminate(MessageHandler<MessageType> auto&& on_message) {
        return terminate(std::forward<decltype(on_message)>(on_message), []() {});
    }

    /// Note: Message handlers take a MessageEnvelope as single argument. The Envelope
    /// (not necessarily the underlying data) is moved to the handler when
    /// called.
    [[nodiscard]] bool terminate(MessageHandler<MessageType> auto&& on_message, std::invocable<> auto&& progress_hook) {
        auto before_next_message_counting_round_hook = [&] {
            flush_all_buffers_and_poll_until_reactivated(on_message);
            // flush_all_buffers();
        };
        in_terminate = true;
        bool ret = queue_.terminate(split_handler(on_message), before_next_message_counting_round_hook, progress_hook);
        in_terminate = false;
        return ret;
    }

    void reactivate() {
        queue_.reactivate();
    }

    TerminationState termination_state() const {
        return queue_.termination_state();
    }

    bool progress_sending() {
        return queue_.progress_sending();
    }

    bool probe_for_messages(MessageHandler<MessageType> auto&& on_message) {
        return queue_.probe_for_messages(split_handler(on_message));
    }

    /// on_message may be called multiple times, because this receives a whole buffer and applies the splitter to it
    bool probe_for_one_message(MessageHandler<MessageType> auto&& on_message,
                               PEID source = MPI_ANY_SOURCE,
                               int tag = MPI_ANY_TAG) {
        return queue_.probe_for_messages(split_handler(on_message));
    }

    void global_threshold(size_t threshold) {
        if (threshold == std::numeric_limits<size_t>::max()) {
            global_threshold_bytes(std::numeric_limits<size_t>::max());
        } else {
            global_threshold_bytes(threshold * sizeof(BufferType));
        }
    }

    void global_threshold_bytes(size_t threshold) {
        global_threshold_bytes_ = threshold;
        if (threshold != std::numeric_limits<size_t>::max()) {
            queue_.reserved_receive_buffer_size((threshold + sizeof(BufferType) - 1) / sizeof(BufferType));
        } else {
            queue_.allow_large_messages();
        }
        if (check_for_global_buffer_overflow(0)) {
            flush_all_buffers();
        }
    }

    size_t global_threshold() const {
        return global_threshold_bytes_ / sizeof(BufferType);
    }

    size_t global_threshold_bytes() const {
        return global_threshold_bytes_;
    }

    void local_threshold(size_t threshold) {
        if (threshold == std::numeric_limits<size_t>::max()) {
            local_threshold_bytes(std::numeric_limits<size_t>::max());
        } else {
            local_threshold_bytes(threshold * sizeof(BufferType));
        }
    }

    void local_threshold_bytes(size_t threshold) {
        local_threshold_bytes_ = threshold;
        if (threshold != std::numeric_limits<size_t>::max()) {
            queue_.reserved_receive_buffer_size((threshold + sizeof(BufferType) - 1) / sizeof(BufferType));
        } else {
            queue_.allow_large_messages();
        }
        for (auto& [receiver, buffer] : buffers_) {
            if (check_for_local_buffer_overflow(buffer, 0)) {
                flush_buffer(receiver);
            }
        }
    }

    size_t local_threshold_bytes() const {
        return local_threshold_bytes_;
    }

    size_t local_threshold() const {
        return global_threshold_bytes_ / sizeof(BufferType);
    }

    void flush_strategy(FlushStrategy strategy) {
        flush_strategy_ = strategy;
    }

    PEID rank() const {
        return queue_.rank();
    }

    PEID size() const {
        return queue_.size();
    }

    MPI_Comm communicator() const {
        return queue_.communicator();
    }

    auto& underlying() {
        return queue_;
    }

    /// if this mode is active, no incoming messages will cancel the termination process
    /// this allows using the queue as a somewhat async sparse-all-to-all
    void synchronous_mode(bool use_it = true) {
        queue_.synchronous_mode(use_it);
    }

private:
    auto flush_buffer_impl(BufferMap::iterator buffer_it, bool erase = true) {
        KASSERT(buffer_it != buffers_.end(), "Trying to flush non-existing buffer.");
        auto& [receiver, buffer] = *buffer_it;
        if (buffer.empty()) {
            return ++buffer_it;
        }
        auto pre_cleanup_buffer_size = buffer.size();
        pre_send_cleanup(buffer, receiver);
        // we don't send if the cleanup has emptied the buffer
        if (buffer.empty()) {
            return ++buffer_it;
        }
        flush_buffer_calls++;
        if (in_terminate) {
            flush_buffer_calls_in_terminate++;
        }
        queue_.post_message(std::move(buffer), receiver);
        global_buffer_size_ -= pre_cleanup_buffer_size;
        if (erase) {
            return buffers_.erase(buffer_it);
        } else {
            return ++buffer_it;
        }
    }

    /// if after_flush_hook return true, this breaks the loop
    void flush_all_buffers_impl(BufferMap::iterator current_buffer, std::predicate<> auto&& after_flush_hook) {
        auto it = buffers_.begin();
        while (it != buffers_.end()) {
            it = flush_buffer_impl(it, it != current_buffer);
            if (after_flush_hook()) {
                return;
            }
        }
    }

    void flush_largest_buffer_impl(BufferMap::iterator current_buffer) {
        auto largest_buffer = std::max_element(buffers_.begin(), buffers_.end(),
                                               [](auto& a, auto& b) { return a.second.size() < b.second.size(); });
        if (largest_buffer != buffers_.end()) {
            flush_buffer_impl(largest_buffer, largest_buffer != current_buffer);
        }
    }

    auto split_handler(MessageHandler<MessageType> auto&& on_message) {
        return [&](Envelope<BufferType> auto buffer) {
            for (Envelope<MessageType> auto env : split(buffer.message, buffer.sender, queue_.rank())) {
                on_message(std::move(env));
            }
        };
    }

    void resolve_overflow(BufferMap::iterator current_buffer) {
        switch (flush_strategy_) {
            case FlushStrategy::local:
                flush_buffer_impl(current_buffer, /*erase=*/false);
                break;
            case FlushStrategy::global:
                flush_all_buffers_impl(current_buffer, [] { return false; });
                break;
            case FlushStrategy::random:
                throw std::runtime_error("Random flush strategy not implemented");
            case FlushStrategy::largest:
                flush_largest_buffer_impl(current_buffer);
                break;
        }
    }

    bool check_for_global_buffer_overflow(std::uint64_t buffer_size_delta) const {
        if (global_threshold_bytes_ == std::numeric_limits<size_t>::max()) {
            return false;
        }
        return (global_buffer_size_ + buffer_size_delta) * sizeof(BufferType) > global_threshold_bytes_;
    }

    bool check_for_local_buffer_overflow(BufferContainer const& buffer, std::uint64_t buffer_size_delta) const {
        if (local_threshold_bytes_ == std::numeric_limits<size_t>::max()) {
            return false;
        }
        return (buffer.size() + buffer_size_delta) * sizeof(BufferType) > local_threshold_bytes_;
    }

    bool check_for_buffer_overflow(BufferContainer const& buffer, std::uint64_t buffer_size_delta) const {
        return check_for_global_buffer_overflow(buffer_size_delta) ||
               check_for_local_buffer_overflow(buffer, buffer_size_delta);
    }
    MessageQueue<BufferType, ReceiveBufferContainer> queue_;
    BufferMap buffers_;
    Merger merge;
    Splitter split;
    BufferCleaner pre_send_cleanup;
    size_t global_buffer_size_ = 0;
    size_t global_threshold_bytes_ = std::numeric_limits<size_t>::max();
    size_t local_threshold_bytes_ = std::numeric_limits<size_t>::max();
    FlushStrategy flush_strategy_ = FlushStrategy::global;

public:
    size_t flush_buffer_calls = 0;
    size_t flush_buffer_calls_in_terminate = 0;
    bool in_terminate = false;
};

template <typename MessageType,
          MPIType BufferType = MessageType,
          MPIBuffer BufferContainer = std::vector<BufferType>,
          MPIBuffer<BufferType> ReceiveBufferContainer = std::vector<BufferType>,
          aggregation::Merger<MessageType, BufferContainer> Merger = aggregation::AppendMerger,
          aggregation::Splitter<MessageType, BufferContainer> Splitter = aggregation::NoSplitter,
          aggregation::BufferCleaner<BufferContainer> BufferCleaner = aggregation::NoOpCleaner>
    requires std::same_as<BufferType, std::ranges::range_value_t<BufferContainer>>
auto make_buffered_queue(MPI_Comm comm,
                         size_t num_request_slots = 8,
                         ReceiveMode receive_mode = ReceiveMode::poll,
                         Merger merger = Merger{},
                         Splitter splitter = Splitter{},
                         BufferCleaner cleaner = BufferCleaner{}) {
    return BufferedMessageQueue<MessageType, BufferType, BufferContainer, ReceiveBufferContainer, Merger, Splitter,
                                BufferCleaner>(comm, num_request_slots, receive_mode, std::move(merger),
                                               std::move(splitter), std::move(cleaner));
}

template <typename MessageType,
          MPIType BufferType = MessageType,
          MPIBuffer<BufferType> BufferContainer = std::vector<BufferType>,
          MPIBuffer<BufferType> ReceiveBufferContainer = std::vector<BufferType>,
          aggregation::Splitter<MessageType, BufferContainer> Splitter = aggregation::NoSplitter,
          aggregation::BufferCleaner<BufferContainer> BufferCleaner = aggregation::NoOpCleaner>
    requires std::same_as<BufferType, std::ranges::range_value_t<BufferContainer>>
auto make_buffered_queue(MPI_Comm comm,
                         size_t num_request_slots = 8,
                         ReceiveMode receive_mode = ReceiveMode::poll,
                         Splitter splitter = Splitter{},
                         BufferCleaner cleaner = BufferCleaner{}) {
    return BufferedMessageQueue<MessageType, BufferType, BufferContainer, ReceiveBufferContainer,
                                aggregation::AppendMerger, Splitter, BufferCleaner>(
        comm, num_request_slots, receive_mode, aggregation::AppendMerger{}, std::move(splitter), std::move(cleaner));
}

template <typename MessageType,
          MPIType BufferType = MessageType,
          MPIBuffer<BufferType> BufferContainer = std::vector<BufferType>,
          MPIBuffer<BufferType> ReceiveBufferContainer = std::vector<BufferType>,
          aggregation::BufferCleaner<BufferContainer> BufferCleaner = aggregation::NoOpCleaner>
    requires std::same_as<BufferType, std::ranges::range_value_t<BufferContainer>>
auto make_buffered_queue(MPI_Comm comm = MPI_COMM_WORLD,
                         size_t num_request_slots = 8,
                         ReceiveMode receive_mode = ReceiveMode::poll,
                         BufferCleaner cleaner = BufferCleaner{}) {
    return BufferedMessageQueue<MessageType, BufferType, BufferContainer, ReceiveBufferContainer,
                                aggregation::AppendMerger, aggregation::NoSplitter, BufferCleaner>(
        comm, num_request_slots, receive_mode, aggregation::AppendMerger{}, aggregation::NoSplitter{},
        std::move(cleaner));
}

}  // namespace message_queue
