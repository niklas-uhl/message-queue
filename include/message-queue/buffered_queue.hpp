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
#include <ranges>
#include <unordered_map>
#include <vector>

#include "message-queue/aggregators.hpp"
#include "message-queue/concepts.hpp"
#include "message-queue/queue.hpp"

namespace message_queue {

enum class FlushStrategy { local, global, random, largest };

template <typename MessageType,
          MPIType BufferType = MessageType,
          MPIBuffer<BufferType> BufferContainer = std::vector<BufferType>,
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

    BufferedMessageQueue(MPI_Comm comm,
                         size_t num_request_slots,
                         Merger merger = Merger{},
                         Splitter splitter = Splitter{},
                         BufferCleaner cleaner = BufferCleaner{})
        : queue_(comm, num_request_slots),
          merge(std::move(merger)),
          split(std::move(splitter)),
          pre_send_cleanup(std::move(cleaner)) {}

    BufferedMessageQueue(MPI_Comm comm = MPI_COMM_WORLD,
                         Merger merger = Merger{},
                         Splitter splitter = Splitter{},
                         BufferCleaner cleaner = BufferCleaner{})
        : BufferedMessageQueue(comm,
                               internal::comm_size(comm),
                               std::move(merger),
                               std::move(splitter),
                               std::move(cleaner)) {}

    bool post_message(MessageRange<MessageType> auto const& message,
                      PEID receiver,
                      PEID envelope_sender,
                      PEID envelope_receiver,
                      int tag) {
        auto envelope = MessageEnvelope{
            .message = std::move(message), .sender = envelope_sender, .receiver = envelope_receiver, .tag = tag};
        size_t estimated_new_buffer_size;
        if constexpr (aggregation::EstimatingMerger<Merger, MessageType, BufferContainer>) {
            estimated_new_buffer_size =
                merge.estimate_new_buffer_size(buffers_[receiver], receiver, queue_.rank(), envelope);
        } else {
            estimated_new_buffer_size = buffers_[receiver].size() + message.size();
        }
        auto old_buffer_size = buffers_[receiver].size();
        bool overflow = false;
        if (check_for_buffer_overflow(receiver, estimated_new_buffer_size - old_buffer_size)) {
            flush_buffer(receiver);
            overflow = true;
        }
        PEID rank;
        merge(buffers_[receiver], receiver, queue_.rank(), std::move(envelope));
        auto new_buffer_size = buffers_[receiver].size();
        global_buffer_size_ += new_buffer_size - old_buffer_size;
        return overflow;
    }

    bool post_message(MessageRange<MessageType> auto const& message, PEID receiver, int tag = 0) {
        return post_message(message, receiver, rank(), receiver, tag);
    }

    bool post_message(MessageType message, PEID receiver, int tag = 0) {
        return post_message(std::ranges::views::single(message), receiver, tag);
    }

    void flush_buffer(PEID receiver) {
        auto it = buffers_.find(receiver);
        KASSERT(buffers_.end() != it, "Trying to flush non-existing buffer for receiver " << receiver);
        flush_buffer_impl(it);
    }

    void flush_largest_buffer() {
        auto largest_buffer = std::max_element(buffers_.begin(), buffers_.end(),
                                               [](auto& a, auto& b) { return a.second.size() < b.second.size(); });
        if (largest_buffer != buffers_.end()) {
            flush_buffer_impl(largest_buffer);
        }
    }

    void flush_all_buffers() {
        auto it = buffers_.begin();
        while (it != buffers_.end()) {
            it = flush_buffer_impl(it);
        }
    }

    bool poll(MessageHandler<MessageType> auto&& on_message) {
        return queue_.poll(split_handler(on_message));
    }

    bool terminate(MessageHandler<MessageType> auto&& on_message) {
        auto before_next_message_counting_round_hook = [&] {
            flush_all_buffers();
        };
        return queue_.terminate(split_handler(on_message), before_next_message_counting_round_hook);
    }

    void reactivate() {
        queue_.reactivate();
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
        global_threshold_ = threshold;
        if (check_for_global_buffer_overflow(0)) {
            flush_all_buffers();
        }
    }

    size_t global_threshold() const {
        return global_threshold_;
    }

    void local_threshold(size_t threshold) {
        local_threshold_ = threshold;
        for (auto& [receiver, buffer] : buffers_) {
            if (check_for_local_buffer_overflow(receiver, 0)) {
                flush_buffer(receiver);
            }
        }
    }

    size_t local_threshold() const {
        return local_threshold_;
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

private:
    auto flush_buffer_impl(BufferMap::iterator buffer_it) {
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
        queue_.post_message(std::move(buffer), receiver);
        global_buffer_size_ -= pre_cleanup_buffer_size;
        return buffers_.erase(buffer_it);
    }

    auto split_handler(MessageHandler<MessageType> auto&& on_message) {
        return [&](Envelope<BufferType> auto buffer) {
            for (Envelope<MessageType> auto env : split(buffer.message, buffer.sender, queue_.rank())) {
                on_message(std::move(env));
            }
        };
    }

    void resolve_overflow(PEID receiver) {
        switch (flush_strategy_) {
            case FlushStrategy::local:
                flush_buffer(receiver);
                break;
            case FlushStrategy::global:
                flush_all_buffers();
                break;
            case FlushStrategy::random:
                throw std::runtime_error("Random flush strategy not implemented");
            case FlushStrategy::largest:
                flush_largest_buffer();
                break;
        }
    }

    bool check_for_global_buffer_overflow(std::uint64_t buffer_size_delta) const {
        return global_buffer_size_ + buffer_size_delta > global_threshold_;
    }

    bool check_for_local_buffer_overflow(PEID receiver, std::uint64_t buffer_size_delta) const {
        return buffers_.at(receiver).size() + buffer_size_delta > local_threshold_;
    }

    bool check_for_buffer_overflow(PEID receiver, std::uint64_t buffer_size_delta) const {
        return check_for_global_buffer_overflow(buffer_size_delta) ||
               check_for_local_buffer_overflow(receiver, buffer_size_delta);
    }
    MessageQueue<BufferType> queue_;
    BufferMap buffers_;
    Merger merge;
    Splitter split;
    BufferCleaner pre_send_cleanup;
    size_t global_buffer_size_ = 0;
    size_t global_threshold_ = std::numeric_limits<size_t>::max();
    size_t local_threshold_ = std::numeric_limits<size_t>::max();
    FlushStrategy flush_strategy_ = FlushStrategy::global;
};

template <typename MessageType,
          MPIType BufferType = MessageType,
          MPIBuffer BufferContainer = std::vector<BufferType>,
          aggregation::Merger<MessageType, BufferContainer> Merger = aggregation::AppendMerger,
          aggregation::Splitter<MessageType, BufferContainer> Splitter = aggregation::NoSplitter,
          aggregation::BufferCleaner<BufferContainer> BufferCleaner = aggregation::NoOpCleaner>
    requires std::same_as<BufferType, std::ranges::range_value_t<BufferContainer>>
auto make_buffered_queue(MPI_Comm comm,
                         Merger merger = Merger{},
                         Splitter splitter = Splitter{},
                         BufferCleaner cleaner = BufferCleaner{}) {
    return BufferedMessageQueue<MessageType, BufferType, BufferContainer, Merger, Splitter, BufferCleaner>(
        comm, std::move(merger), std::move(splitter), std::move(cleaner));
}

template <typename MessageType,
          MPIType BufferType = MessageType,
          MPIBuffer BufferContainer = std::vector<BufferType>,
          aggregation::Splitter<MessageType, BufferContainer> Splitter = aggregation::NoSplitter,
          aggregation::BufferCleaner<BufferContainer> BufferCleaner = aggregation::NoOpCleaner>
    requires std::same_as<BufferType, std::ranges::range_value_t<BufferContainer>>
auto make_buffered_queue(MPI_Comm comm, Splitter splitter = Splitter{}, BufferCleaner cleaner = BufferCleaner{}) {
    return BufferedMessageQueue<MessageType, BufferType, BufferContainer, aggregation::AppendMerger, Splitter,
                                BufferCleaner>(comm, aggregation::AppendMerger{}, std::move(splitter),
                                               std::move(cleaner));
}

template <typename MessageType,
          MPIType BufferType = MessageType,
          MPIBuffer BufferContainer = std::vector<BufferType>,
          aggregation::BufferCleaner<BufferContainer> BufferCleaner = aggregation::NoOpCleaner>
    requires std::same_as<BufferType, std::ranges::range_value_t<BufferContainer>>
auto make_buffered_queue(MPI_Comm comm = MPI_COMM_WORLD, BufferCleaner cleaner = BufferCleaner{}) {
    return BufferedMessageQueue<MessageType, BufferType, BufferContainer, aggregation::AppendMerger,
                                aggregation::NoSplitter, BufferCleaner>(comm, aggregation::AppendMerger{},
                                                                        aggregation::NoSplitter{}, std::move(cleaner));
}

}  // namespace message_queue