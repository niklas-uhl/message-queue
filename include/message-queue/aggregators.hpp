#pragma once

#include "message-queue/concepts.hpp"
#include <iterator>

namespace message_queue::aggregation {

struct AppendMerger {
    template <MPIBuffer BufferContainer>
    void operator()(BufferContainer& buffer,
                    PEID buffer_destination,
                    PEID my_rank,
                    Envelope<std::ranges::range_value_t<BufferContainer>> auto envelope) const {
        buffer.insert(std::end(buffer), std::begin(envelope.message), std::end(envelope.message));
    }
    template <typename MessageContainer, typename BufferContainer>
    size_t estimate_new_buffer_size(BufferContainer const& buffer,
                                    PEID buffer_destination,
                                    PEID my_rank,
                                    FullEnvelope<MessageContainer> const& envelope) const {
        return buffer.size() + envelope.message.size();
    };
};
static_assert(Merger<AppendMerger, int, std::vector<int>>);
static_assert(EstimatingMerger<AppendMerger, int, std::vector<int>>);

struct NoSplitter {
    auto operator()(MPIBuffer auto const& buffer, PEID buffer_origin, PEID my_rank) const {
        return std::ranges::single_view(
            FullEnvelope{.message = buffer, .sender = buffer_origin, .receiver = my_rank, .tag = 0});
    }
};
static_assert(Splitter<NoSplitter, int, std::vector<int>>);

template <MPIType BufferType>
struct SentinelMerger {
    SentinelMerger(BufferType sentinel) : sentinel_(sentinel) {}

    void operator()(MPIBuffer<BufferType> auto& buffer,
                    PEID buffer_destination,
                    PEID my_rank,
                    Envelope<BufferType> auto envelope) const {
        buffer.insert(std::end(buffer), std::begin(envelope.message), std::end(envelope.message));
        buffer.push_back(sentinel_);
    }
    size_t estimate_new_buffer_size(MPIBuffer<BufferType> auto const& buffer,
                                    PEID buffer_destination,
                                    PEID my_rank,
                                    Envelope<BufferType> auto const& envelope) const {
        return buffer.size() + envelope.message.size() + 1;
    };
    BufferType sentinel_;
};
static_assert(Merger<SentinelMerger<int>, int, std::vector<int>>);
static_assert(EstimatingMerger<SentinelMerger<int>, int, std::vector<int>>);

template <MPIType BufferType>
struct SentinelSplitter {
    SentinelSplitter(BufferType sentinel) : sentinel_(sentinel) {}

    auto operator()(MPIBuffer<BufferType> auto const& buffer, PEID buffer_origin, PEID my_rank) const {
        return std::views::split(buffer, sentinel_) |
               std::views::transform([&, buffer_origin = buffer_origin, my_rank = my_rank](auto&& range) {
                   return FullEnvelope{
                       .message = std::move(range), .sender = buffer_origin, .receiver = my_rank, .tag = 0};
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

}  // namespace message_queue::aggregation
