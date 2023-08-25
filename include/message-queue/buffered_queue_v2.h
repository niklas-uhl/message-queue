#pragma once
#include <concepts>
#include <iostream>
#include <iterator>
#include <ranges>
#include <vector>

namespace message_queue {

namespace aggregation {
template <typename MergerType, typename T, template <typename...> typename ContainerType>
concept Merger = requires(MergerType merge, ContainerType<T>& buffer, ContainerType<T> const& message, int tag) {
    merge(buffer, message, tag);
};

template <typename Range, typename T>
concept SplitRange =
    std::ranges::forward_range<Range> && std::ranges::contiguous_range<std::ranges::range_value_t<Range>> &&
    std::same_as<T, std::ranges::range_value_t<std::ranges::range_value_t<Range>>>;

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
};
static_assert(Merger<AppendMerger<int, std::vector>, int, std::vector>);

template <typename T, template <typename...> typename ContainerType>
struct NoSplitter {
    auto operator()(ContainerType<T> const& buffer) const {
        return std::ranges::single_view(buffer);
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
    T sentinel_;
};
static_assert(Merger<SentinelMerger<int, std::vector>, int, std::vector>);

template <typename T, template <typename...> typename ContainerType>
struct SentinelSplitter {
    SentinelSplitter(int sentinel) : sentinel_(sentinel) {}
    auto operator()(ContainerType<T> const& buffer) const {
        return std::views::split(buffer, sentinel_);
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

}  // namespace message_queue
