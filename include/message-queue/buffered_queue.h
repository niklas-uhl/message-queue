#pragma once

#include <cstddef>
#include <functional>
#include <type_traits>
#include <unordered_map>
#include "message-queue/debug_print.h"
#include "message-queue/queue.h"

namespace message_queue {

template <class T, typename Merger, typename Splitter>
class BufferedMessageQueue {
    static_assert(std::is_invocable_v<
                  Splitter,
                  std::vector<T>&,
                  std::function<void(typename std::vector<T>::iterator, typename std::vector<T>::iterator, PEID)>,
                  PEID>);
    static_assert(std::is_invocable_v<Merger, std::vector<T>&, std::vector<T>, int>);

public:
    BufferedMessageQueue(Merger&& merge, Splitter&& split)
        : queue_(),
          buffers_(),
          buffer_ocupacy_(0),
          threshold_(std::numeric_limits<size_t>::max()),
          overflows_(0),
          merge(merge),
          split(split) {}

    void post_message(std::vector<T>&& message, PEID receiver, int tag = 0) {
        auto& buffer = buffers_[receiver];
        size_t old_buffer_size = buffer.size();
        merge(buffer, std::forward<std::vector<T>>(message), tag);
        size_t new_buffer_size = buffer.size();
        buffer_ocupacy_ += (new_buffer_size - old_buffer_size);
        if (buffer_ocupacy_ > threshold_) {
            overflows_++;
            flush_all();
        }
        // atomic_debug(buffer);
    }

    void set_threshold(size_t threshold) {
        threshold_ = threshold;
        if (buffer_ocupacy_ > threshold_) {
            overflows_++;
            flush_all();
        }
    }

    void flush(PEID receiver) {
        auto& buffer = buffers_[receiver];
        if (!buffer.empty()) {
            size_t buffer_size = buffer.size();
            queue_.post_message(std::move(buffer), receiver);
            buffer_ocupacy_ -= buffer_size;
        }
    }

    void flush_all() {
        for (auto& kv : buffers_) {
            if (!kv.second.empty()) {
                size_t buffer_size = kv.second.size();
                queue_.post_message(std::move(kv.second), kv.first);
                buffer_ocupacy_ -= buffer_size;
            }
        }
    }

    template <typename MessageHandler>
    bool poll(MessageHandler&& on_message) {
        static_assert(std::is_invocable_v<MessageHandler, typename std::vector<T>::iterator,
                                          typename std::vector<T>::iterator, PEID>);
        return queue_.poll([&](std::vector<T> message, PEID sender) { split(message, on_message, sender); });
    }

    template <typename MessageHandler>
    void terminate(MessageHandler&& on_message) {
        static_assert(std::is_invocable_v<MessageHandler, typename std::vector<T>::iterator,
                                          typename std::vector<T>::iterator, PEID>);
        queue_.terminate_impl([&](std::vector<T> message, PEID sender) { split(message, on_message, sender); },
                              [&]() { flush_all(); });
        /* for (auto buffer : buffers_) { */
        /*     atomic_debug(buffer); */
        /* } */
    }

    template <typename MessageHandler>
    bool try_terminate(MessageHandler&& on_message) {
        static_assert(std::is_invocable_v<MessageHandler, typename std::vector<T>::iterator,
                                          typename std::vector<T>::iterator, PEID>);
        atomic_debug("Try terminate");
        return queue_.try_terminate_impl([&](PEID sender, std::vector<T> message) { split(message, on_message, sender); },
                              [&]() { flush_all(); });
        /* for (auto buffer : buffers_) { */
        /*     atomic_debug(buffer); */
        /* } */
    }

    void reactivate() {
        queue_.reactivate();
    }

    size_t overflows() const {
        return overflows_;
    }

    const MessageStatistics& stats() {
        return queue_.stats();
    }

    void reset() {
        queue_.reset();
        buffers_.clear();
        buffer_ocupacy_ = 0;
        overflows_ = 0;
    }

private:
    MessageQueue<T> queue_;
    std::unordered_map<PEID, std::vector<T>> buffers_;
    size_t buffer_ocupacy_;
    size_t threshold_;
    size_t overflows_;
    Merger merge;
    Splitter split;
};

template <class T, typename Merger, typename Splitter>
auto make_buffered_queue(Merger&& merger, Splitter&& splitter) {
    return BufferedMessageQueue<T, Merger, Splitter>(std::forward<Merger>(merger), std::forward<Splitter>(splitter));
}

}  // namespace message_queue
