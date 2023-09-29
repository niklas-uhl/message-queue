#pragma once

#include <mpi.h>
#include <algorithm>
#include <iterator>
#include <ranges>
#include <unordered_map>
#include <vector>

#include "message-queue/queue_v2.h"

namespace message_queue {

template <typename Range>
concept MessageRange = std::ranges::input_range<Range> && std::ranges::sized_range<Range>;

template <typename Range, typename MessageType>
concept TypedMessageRange = MessageRange<Range> && std::same_as<MessageType, std::ranges::range_value_t<Range>>;

static_assert(TypedMessageRange<std::vector<int>, int>);
static_assert(TypedMessageRange<std::ranges::single_view<int>, int>);
static_assert(TypedMessageRange<std::ranges::empty_view<int>, int>);

template <typename E, typename MessageType>
concept Envelope = requires(E envelope) {
    { envelope.tag } -> std::same_as<int&>;
    { envelope.message } -> TypedMessageRange<MessageType>;
} && TypedMessageRange<typename E::message_range_type, MessageType>;

enum class EnvelopeType { tag, full };

template <MessageRange MessageRangeType>
struct TagEnvelope {
    using message_range_type = MessageRangeType;
    using message_value_type = std::ranges::range_value_t<message_range_type>;
    static constexpr EnvelopeType type = EnvelopeType::tag;
    int tag;
    MessageRangeType message;
};
static_assert(Envelope<TagEnvelope<std::vector<int>>, int>);

template <MessageRange MessageRangeType>
struct FullEnvelope {
    using message_range_type = MessageRangeType;
    using message_value_type = std::ranges::range_value_t<message_range_type>;
    static constexpr EnvelopeType type = EnvelopeType::full;
    int tag;
    PEID sender;
    PEID receiver;
    MessageRangeType message;
};
static_assert(Envelope<FullEnvelope<std::vector<int>>, int>);

namespace aggregation {

template <typename MergerType, typename MessageType, typename BufferContainer>
concept TagEnvelopeMerger =
    requires(MergerType merge, BufferContainer& buffer, TagEnvelope<std::ranges::empty_view<MessageType>> envelope) {
        merge(buffer, envelope);
    };

template <typename MergerType, typename MessageType, typename BufferContainer>
concept FullEnvelopeMerger =
    requires(MergerType merge, BufferContainer& buffer, FullEnvelope<std::ranges::empty_view<MessageType>> envelope) {
        merge(buffer, envelope);
    };

template <typename MergerType, typename MessageType, typename BufferContainer>
concept Merger = TagEnvelopeMerger<MergerType, MessageType, BufferContainer> ||
                 FullEnvelopeMerger<MergerType, MessageType, BufferContainer>;

template <typename MergerType, typename MessageType, typename BufferContainer>
concept EstimatingMerger =
    Merger<MergerType, MessageType, BufferContainer> && requires(MergerType merge, BufferContainer& buffer, int tag) {
        { merge.estimate_new_buffer_size(buffer, std::ranges::views::empty<MessageType>, tag) } -> std::same_as<size_t>;
    };

template <typename Range, typename T>
concept SplitRange = std::ranges::forward_range<Range> && Envelope<std::ranges::range_value_t<Range>, T>;

template <typename SplitterType, typename MessageType, typename BufferContainer>
concept Splitter = requires(SplitterType split, BufferContainer const& buffer) {
    { split(buffer) } -> SplitRange<MessageType>;
};

template<typename Merger, typename Splitter>
static constexpr bool merger_and_splitter_use_same_envelope = true;

template <typename BufferCleanerType, typename BufferContainer>
concept BufferCleaner = requires(BufferCleanerType pre_send_cleanup, BufferContainer& buffer, PEID receiver) {
    pre_send_cleanup(buffer, receiver);
};

struct AppendMerger {
    template <typename MessageContainer, typename BufferContainer>
    void operator()(BufferContainer& buffer, TagEnvelope<MessageContainer> const& envelope) const {
        buffer.insert(std::end(buffer), std::begin(envelope.message), std::end(envelope.message));
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
        return std::ranges::single_view(TagEnvelope{0, buffer});
    }
};
static_assert(Splitter<NoSplitter, int, std::vector<int>>);

template <MPIType BufferType>
struct SentinelMerger {
    SentinelMerger(BufferType sentinel) : sentinel_(sentinel) {}

    template <typename MessageContainer, typename BufferContainer>
        requires std::same_as<BufferType, typename BufferContainer::value_type>
    void operator()(BufferContainer& buffer, TagEnvelope<MessageContainer> const& envelope) const {
        buffer.insert(std::end(buffer), std::begin(envelope.message), std::end(envelope.message));
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
        return std::views::split(buffer, sentinel_) | std::views::transform([](auto&& range) {
                   return TagEnvelope{0, std::move(range)};
               });
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
          // MessageContainerType MessageContainer = std::vector<MessageType>,
          MPIBuffer BufferContainer = std::vector<BufferType>,
          aggregation::Merger<MessageType, BufferContainer> Merger = aggregation::AppendMerger,
          aggregation::Splitter<MessageType, BufferContainer> Splitter = aggregation::NoSplitter,
          aggregation::BufferCleaner<BufferContainer> BufferCleaner = aggregation::NoOpCleaner>
    requires std::same_as<BufferType, std::ranges::range_value_t<BufferContainer>>
class BufferedMessageQueueV2 {
private:
    using BufferMap = std::unordered_map<PEID, BufferContainer>;

public:
    using message_type = MessageType;
    using buffer_type = BufferType;
    // using message_container_type = MessageContainer;
    using buffer_container_type = BufferContainer;
    using merger_type = Merger;
    using splitter_type = Splitter;
    using buffer_cleaner_type = BufferCleaner;

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

    bool post_message(TypedMessageRange<MessageType> auto const& message, PEID receiver, int tag = 0) {
        size_t estimated_new_buffer_size;
        if constexpr (aggregation::EstimatingMerger<Merger, MessageType, BufferContainer>) {
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
        static constexpr EnvelopeType envelope_type = std::ranges::range_value_t<decltype(split(std::declval<BufferContainer>()))>::type;
        if constexpr (envelope_type == EnvelopeType::tag) {
            merge(buffers_[receiver], TagEnvelope{tag, std::move(message)});
        } else if constexpr (envelope_type == EnvelopeType::full) {
            PEID rank;
            merge(buffers_[receiver], FullEnvelope{tag, queue_.rank(), receiver, std::move(message)});
        }
        auto new_buffer_size = buffers_[receiver].size();
        global_buffer_size_ += new_buffer_size - old_buffer_size;
        return overflow;
    }

    bool post_message(MessageType message, PEID receiver, int tag = 0) {
        return post_message(std::ranges::views::single(message), receiver, tag);
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

    bool poll(MessageHandler<MessageType> auto&& on_message) {
        auto split_on_message = [&](auto message, PEID sender, int /*tag*/) {
            for (auto&& [tag, message] : split(message)) {
                on_message(std::move(message), sender, tag);
            }
        };
        return queue_.poll(split_on_message);
    }

    bool terminate(MessageHandler<MessageType> auto&& on_message) {
        auto split_on_message = [&](TypedMessageRange<BufferType> auto message, PEID sender, int /*tag*/) {
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
          MPIBuffer BufferContainer = std::vector<BufferType>,
          aggregation::Merger<MessageType, BufferContainer> Merger = aggregation::AppendMerger,
          aggregation::Splitter<MessageType, BufferContainer> Splitter = aggregation::NoSplitter,
          aggregation::BufferCleaner<BufferContainer> BufferCleaner = aggregation::NoOpCleaner>
    requires std::same_as<BufferType, std::ranges::range_value_t<BufferContainer>>
auto make_buffered_queue(MPI_Comm comm = MPI_COMM_WORLD,
                         Merger merger = Merger{},
                         Splitter splitter = Splitter{},
                         BufferCleaner cleaner = BufferCleaner{}) {
    return BufferedMessageQueueV2<MessageType, BufferType, BufferContainer, Merger, Splitter, BufferCleaner>(
        comm, std::move(merger), std::move(splitter), std::move(cleaner));
}

template <typename MessageType,
          MPIType BufferType = MessageType,
          MPIBuffer BufferContainer = std::vector<BufferType>,
          aggregation::Splitter<MessageType, BufferContainer> Splitter = aggregation::NoSplitter,
          aggregation::BufferCleaner<BufferContainer> BufferCleaner = aggregation::NoOpCleaner>
    requires std::same_as<BufferType, std::ranges::range_value_t<BufferContainer>>
auto make_buffered_queue(MPI_Comm comm = MPI_COMM_WORLD,
                         Splitter splitter = Splitter{},
                         BufferCleaner cleaner = BufferCleaner{}) {
    return BufferedMessageQueueV2<MessageType, BufferType, BufferContainer, aggregation::AppendMerger, Splitter,
                                  BufferCleaner>(comm, aggregation::AppendMerger{}, std::move(splitter),
                                                 std::move(cleaner));
}

template <typename MessageType,
          MPIType BufferType = MessageType,
          MPIBuffer BufferContainer = std::vector<BufferType>,
          aggregation::BufferCleaner<BufferContainer> BufferCleaner = aggregation::NoOpCleaner>
    requires std::same_as<BufferType, std::ranges::range_value_t<BufferContainer>>
auto make_buffered_queue(MPI_Comm comm = MPI_COMM_WORLD, BufferCleaner cleaner = BufferCleaner{}) {
    return BufferedMessageQueueV2<MessageType, BufferType, BufferContainer, aggregation::AppendMerger,
                                  aggregation::NoSplitter, BufferCleaner>(
        comm, aggregation::AppendMerger{}, aggregation::NoSplitter{}, std::move(cleaner));
}

}  // namespace message_queue
