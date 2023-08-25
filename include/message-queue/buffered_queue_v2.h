#pragma once
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

template <typename MergerType, typename T, template <typename...> typename ContainerType>
concept Merger = requires(MergerType merge, ContainerType<T>& buffer, ContainerType<T> const& message, int tag) {
    merge(buffer, message, tag);
};

template <typename MergerType, typename T, template <typename...> typename ContainerType>
concept EstimatingMerger =
    Merger<MergerType, T, ContainerType> &&
    requires(MergerType merge, ContainerType<T>& buffer, ContainerType<T> const& message, int tag) {
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

template <typename SplitterType, typename T, template <typename...> typename ContainerType>
concept Splitter = requires(SplitterType split, ContainerType<T> const& buffer) {
    { split(buffer) } -> SplitRange<T>;
};

template <typename BufferCleanerType, typename T, template <typename...> typename ContainerType>
concept BufferCleaner =
    requires(BufferCleanerType pre_send_cleanup, ContainerType<T>& buffer) { pre_send_cleanup(buffer); };

template <typename T, template <typename...> typename ContainerType>
struct AppendMerger {
    void operator()(ContainerType<T>& buffer, ContainerType<T> const& message, int tag) const {
        buffer.insert(std::end(buffer), std::begin(message), std::end(message));
    }
    size_t estimate_new_buffer_size(ContainerType<T>& buffer, ContainerType<T> const& message, int tag) const {
        return buffer.size() + message.size();
    };
};
static_assert(EstimatingMerger<AppendMerger<int, std::vector>, int, std::vector>);

template <typename T, template <typename...> typename ContainerType>
struct NoSplitter {
    auto operator()(ContainerType<T> const& buffer) const {
        return std::ranges::single_view(std::make_pair(0, buffer));
    }
};
static_assert(Splitter<NoSplitter<int, std::vector>, int, std::vector>);

template <typename T, template <typename...> typename ContainerType>
struct SentinelMerger {
    SentinelMerger(int sentinel) : sentinel_(sentinel) {}
    void operator()(ContainerType<T>& buffer, ContainerType<T> const& message, int tag) const {
        buffer.insert(std::end(buffer), std::begin(message), std::end(message));
        buffer.push_back(sentinel_);
    }
    size_t estimate_new_buffer_size(ContainerType<T>& buffer, ContainerType<T> const& message, int tag) const {
        return buffer.size() + message.size() + 1;
    };
    T sentinel_;
};
static_assert(EstimatingMerger<SentinelMerger<int, std::vector>, int, std::vector>);

template <typename T, template <typename...> typename ContainerType>
struct SentinelSplitter {
    SentinelSplitter(int sentinel) : sentinel_(sentinel) {}
    auto operator()(ContainerType<T> const& buffer) const {
        return std::views::split(buffer, sentinel_) |
               std::views::transform([](auto&& range) { return std::make_pair(0, std::move(range)); });
    }
    T sentinel_;
};

static_assert(Splitter<SentinelSplitter<int, std::vector>, int, std::vector>);

template <typename T, template <typename...> typename ContainerType>
struct NoOpCleaner {
    void operator()(ContainerType<T>& buffer) const {}
};
static_assert(BufferCleaner<NoOpCleaner<int, std::vector>, int, std::vector>);

}  // namespace aggregation

enum class FlushStrategy { local, global, random, largest };

template <MPIType T,
          template <typename...> typename BufferContainerType = std::vector,
          typename Merger = aggregation::AppendMerger<T, BufferContainerType>,
          typename Splitter = aggregation::NoSplitter<T, BufferContainerType>,
          typename BufferCleaner = aggregation::NoOpCleaner<T, BufferContainerType>>
class BufferedMessageQueueV2 {
public:
    BufferedMessageQueueV2(size_t num_request_slots = internal::world_size(),
                           Merger&& merger = Merger{},
                           Splitter&& splitter = Splitter{},
                           BufferCleaner&& cleaner = BufferCleaner{})
        : queue_(num_request_slots),
          merge(std::forward<Merger>(merger)),
          split(std::forward<Splitter>(splitter)),
          pre_send_cleanup(std::forward<BufferCleaner>(cleaner)) {}

    bool post_message(std::vector<T>&& message, PEID receiver, int tag) {
        size_t estimated_new_buffer_size;
        if constexpr (aggregation::EstimatingMerger<Merger, T, BufferContainerType>) {
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

    bool post_message(T message, PEID receiver) {
        std::vector message_vector{std::move(message)};
        return post_message(std::move(message_vector), receiver, 0);
    }

    void flush_buffer(PEID receiver) {
        auto& buffer = buffers_[receiver];
        pre_send_cleanup(buffer);
        if (buffer.empty()) {
            return;
        }
        global_buffer_size_ -= buffer.size();
        queue_.post_message(std::move(buffer), receiver);
        //buffers_.erase(receiver);
    }

    void flush_largest_buffer() {
        auto largest_buffer = std::max_element(buffers_.begin(), buffers_.end(), [](auto& a, auto& b) {
            return a.second.size() < b.second.size();
        });
        if (largest_buffer != buffers_.end()) {
            flush_buffer(largest_buffer->first);
        }
    }

    void flush_all_buffers() {
        for (auto& [receiver, buffer] : buffers_) {
            flush_buffer(receiver);
        }
    }

    bool poll(MessageHandler<T, std::vector> auto&& on_message) {
        auto split_on_message = [&](auto message, PEID sender, int /*tag*/) {
            for (auto&& [tag, message] : split(message)) {
                on_message(std::move(message), sender, tag);
            }
        };
        return queue_.poll(split_on_message);
    }

    bool terminate(MessageHandler<T, std::vector> auto&& on_message) {
        auto split_on_message = [&](auto message, PEID sender, int /*tag*/) {
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
        switch(flush_strategy_) {
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

    MessageQueueV2<T> queue_;
    std::unordered_map<PEID, BufferContainerType<T>> buffers_;
    Merger&& merge;
    Splitter&& split;
    BufferCleaner&& pre_send_cleanup;
    size_t global_buffer_size_ = 0;
    size_t global_threshold_ = std::numeric_limits<size_t>::max();
    size_t local_threshold_ = std::numeric_limits<size_t>::max();
    FlushStrategy flush_strategy_ = FlushStrategy::global;
};

}  // namespace message_queue
