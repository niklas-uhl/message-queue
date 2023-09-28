#pragma once
#include <mpi.h>
#include <concepts>
#include <iostream>
#include <iterator>
#include <random>
#include <ranges>
#include <vector>

#include "message-queue/datatype.hpp"
#include "message-queue/queue_v2.h"

namespace message_queue {

namespace aggregation {

template <class P>
concept Pair = requires(P p) {
    typename P::first_type;
    typename P::second_type;
    p.first;
    p.second;
    { p.first } -> std::same_as<typename P::first_type&>;
    { p.second } -> std::same_as<typename P::second_type&>;
};

template <typename MergerType, typename MessageContainer, typename BufferContainer>
concept Merger = requires(MergerType merge, BufferContainer& buffer, MessageContainer const& message, int tag) {
    merge(buffer, message, tag);
};

template <typename MergerType, typename MessageContainer, typename BufferContainer>
concept EstimatingMerger =
    Merger<MergerType, MessageContainer, BufferContainer> &&
    requires(MergerType merge, BufferContainer& buffer, MessageContainer const& message, int tag) {
        { merge.estimate_new_buffer_size(buffer, message, tag) } -> std::same_as<size_t>;
    };

template <typename Range, typename T>
concept SplitRange =
    std::ranges::forward_range<Range> && Pair<std::ranges::range_value_t<Range>> &&
    std::same_as<int, typename std::ranges::range_value_t<Range>::first_type> &&
#if defined(MESSAGE_QUEUE_GCC_11_SPLIT_VIEW_BUG)
    std::ranges::forward_range<typename std::ranges::range_value_t<Range>::second_type> &&
#else
    std::ranges::contiguous_range<typename std::ranges::range_value_t<Range>::second_type> &&
#endif
    std::same_as<T, std::ranges::range_value_t<typename std::ranges::range_value_t<Range>::second_type>>;

template <typename SplitterType, typename MessageType, typename BufferContainer>
concept Splitter = requires(SplitterType split, BufferContainer const& buffer) {
    { split(buffer) } ;//-> SplitRange<MessageType>;
};

template <typename BufferCleanerType, typename BufferContainer>
concept BufferCleaner = requires(BufferCleanerType pre_send_cleanup, BufferContainer& buffer, PEID receiver) {
    pre_send_cleanup(buffer, receiver);
};

struct AppendMerger {
    template <typename MessageContainer, typename BufferContainer>
    void operator()(BufferContainer& buffer, MessageContainer const& message, int tag) const {
        buffer.insert(std::end(buffer), std::begin(message), std::end(message));
    }
    template <typename MessageContainer, typename BufferContainer>
    size_t estimate_new_buffer_size(BufferContainer& buffer, MessageContainer const& message, int tag) const {
        return buffer.size() + message.size();
    };
};
static_assert(EstimatingMerger<AppendMerger, std::vector<int>, std::vector<int>>);

struct NoSplitter {
    template <typename BufferContainer>
    auto operator()(BufferContainer const& buffer) const {
        return std::ranges::single_view(std::make_pair(0, buffer));
    }
};
static_assert(Splitter<NoSplitter, int, std::vector<int>>);

template <MPIType BufferType>
struct SentinelMerger {
    SentinelMerger(BufferType sentinel) : sentinel_(sentinel) {}

    template <typename MessageContainer, typename BufferContainer>
        requires std::same_as<BufferType, typename BufferContainer::value_type>
    void operator()(BufferContainer& buffer, MessageContainer const& message, int tag) const {
        buffer.insert(std::end(buffer), std::begin(message), std::end(message));
        buffer.push_back(sentinel_);
    }
    template <typename MessageContainer, typename BufferContainer>
        requires std::same_as<BufferType, typename BufferContainer::value_type>
    size_t estimate_new_buffer_size(BufferContainer& buffer, MessageContainer const& message, int tag) const {
        return buffer.size() + message.size() + 1;
    };
    BufferType sentinel_;
};
static_assert(EstimatingMerger<SentinelMerger<int>, std::vector<int>, std::vector<int>>);

template <MPIType BufferType>
struct SentinelSplitter {
    SentinelSplitter(BufferType sentinel) : sentinel_(sentinel) {}

    template <typename BufferContainer>
        requires std::same_as<BufferType, typename BufferContainer::value_type>
    auto operator()(BufferContainer const& buffer) const {
        return std::views::split(buffer, sentinel_) |
               std::views::transform([](auto&& range) { return std::make_pair(0, std::move(range)); });
    }
    BufferType sentinel_;
};

static_assert(Splitter<SentinelSplitter<int>, int, std::vector<int>>);

struct NoOpCleaner {
    template <typename BufferContainer>
    void operator()(BufferContainer& buffer, PEID) const {}
};
static_assert(BufferCleaner<NoOpCleaner, std::vector<int>>);

}  // namespace aggregation

enum class FlushStrategy { local, global, random, largest };

template <typename MessageType,
          MPIType BufferType = MessageType,
          typename MessageContainer = std::vector<MessageType>,
          typename BufferContainer = std::vector<BufferType>,
          aggregation::Merger<MessageContainer, BufferContainer> Merger = aggregation::AppendMerger,
          aggregation::Splitter<MessageType, BufferContainer> Splitter = aggregation::NoSplitter,
          aggregation::BufferCleaner<BufferContainer> BufferCleaner = aggregation::NoOpCleaner>
class BufferedMessageQueueV2 {
public:
    using BufferMap = std::unordered_map<PEID, BufferContainer>;

    BufferedMessageQueueV2(MPI_Comm comm,
                           size_t num_request_slots,
                           Merger merger = Merger{},
                           Splitter splitter = Splitter{},
                           BufferCleaner cleaner = BufferCleaner{})
        : queue_(comm, num_request_slots),
          merge(std::move(merger)),
          split(std::move(splitter)),
          pre_send_cleanup(std::move(cleaner)) {}

    BufferedMessageQueueV2(MPI_Comm comm = MPI_COMM_WORLD,
                           Merger merger = Merger{},
                           Splitter splitter = Splitter{},
                           BufferCleaner cleaner = BufferCleaner{})
        : BufferedMessageQueueV2(comm,
                                 internal::comm_size(comm),
                                 std::move(merger),
                                 std::move(splitter),
                                 std::move(cleaner)) {}

    bool post_message(MessageContainer&& message, PEID receiver, int tag = 0) {
        size_t estimated_new_buffer_size;
        if constexpr (aggregation::EstimatingMerger<Merger, MessageContainer, BufferContainer>) {
            estimated_new_buffer_size = merge.estimate_new_buffer_size(buffers_[receiver], message, tag);
        } else {
            estimated_new_buffer_size = buffers_[receiver].size() + message.size();
        }
        auto old_buffer_size = buffers_[receiver].size();
        bool overflow = false;
        if (check_for_buffer_overflow(receiver, estimated_new_buffer_size - old_buffer_size)) {
            flush_buffer(receiver);
            overflow = true;
        }
        merge(buffers_[receiver], message, tag);
        auto new_buffer_size = buffers_[receiver].size();
        global_buffer_size_ += new_buffer_size - old_buffer_size;
        return overflow;
    }

    bool post_message(MessageType message, PEID receiver, int tag = 0) {
        MessageContainer message_vector{std::move(message)};
        return post_message(std::move(message_vector), receiver, tag);
    }

    void flush_buffer(PEID receiver) {
        auto it = buffers_.find(receiver);
        KASSERT(buffers_.end() != it, "Trying to flush non-existing buffer for receiver " << receiver);
        flush_buffer_impl(it);
    }

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

    bool poll(MessageHandler<MessageType, MessageContainer> auto&& on_message) {
        auto split_on_message = [&](auto message, PEID sender, int /*tag*/) {
            for (auto&& [tag, message] : split(message)) {
                on_message(std::move(message), sender, tag);
            }
        };
        return queue_.poll(split_on_message);
    }

    bool terminate(MessageHandler<MessageType, MessageContainer> auto&& on_message) {
        auto split_on_message = [&](BufferContainer message, PEID sender, int /*tag*/) {
            for (auto&& [tag, message] : split(message)) {
                on_message(std::move(message), sender, tag);
            }
        };

        auto before_next_message_counting_round_hook = [&] {
            flush_all_buffers();
        };
        return queue_.terminate(split_on_message, before_next_message_counting_round_hook);
    }

    void reactivate() {
        queue_.reactivate();
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

private:
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
    MessageQueueV2<BufferType> queue_;
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
          typename MessageContainer = std::vector<MessageType>,
          typename BufferContainer = std::vector<BufferType>,
          aggregation::BufferCleaner<BufferContainer> Cleaner>
auto make_buffered_queue_with_cleaner(MPI_Comm comm, Cleaner cleaner) {
    return BufferedMessageQueueV2<MessageType, BufferType, MessageContainer, BufferContainer, aggregation::AppendMerger,
                                  aggregation::NoSplitter, Cleaner>(
        comm, aggregation::AppendMerger{}, aggregation::NoSplitter{}, std::move(cleaner));
}

template <typename MessageType,
          MPIType BufferType = MessageType,
          typename MessageContainer = std::vector<MessageType>,
          typename BufferContainer = std::vector<BufferType>,
          aggregation::Merger<MessageContainer, BufferContainer> Merger = aggregation::AppendMerger,
          aggregation::Splitter<MessageType, BufferContainer> Splitter = aggregation::NoSplitter,
          aggregation::BufferCleaner<BufferContainer> BufferCleaner = aggregation::NoOpCleaner>
auto make_buffered_queue(MPI_Comm comm = MPI_COMM_WORLD,
                       Merger merger = Merger{},
                       Splitter splitter = Splitter{},
                       BufferCleaner cleaner = BufferCleaner{}) {
    return BufferedMessageQueueV2<MessageType, BufferType, MessageContainer, BufferContainer, Merger, Splitter,
                                  BufferCleaner>(comm, std::move(merger), std::move(splitter), std::move(cleaner));
}

}  // namespace message_queue
