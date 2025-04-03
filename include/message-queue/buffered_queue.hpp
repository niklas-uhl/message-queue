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
#include <cstdint>
#include <limits>
#include <ranges>
#include <unordered_map>
#include <vector>

#include "message-queue/aggregators.hpp"
#include "message-queue/concepts.hpp"
#include "message-queue/queue.hpp"

#include <kamping/measurements/timer.hpp>

#include <spdlog/spdlog.h>

namespace message_queue {

enum class FlushStrategy { local, global, random, largest };

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
    using BufferList = std::vector<BufferContainer>;

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
          in_transit_buffers_(num_request_slots),
          available_in_transit_slots_(in_transit_buffers_.size()),
          merge(std::move(merger)),
          split(std::move(splitter)),
          pre_send_cleanup(std::move(cleaner)) {
      spdlog::info("message_size = {}", sizeof(buffer_type));
    }
  ~BufferedMessageQueue() {
    spdlog::info("poll_pre_overflow = {}, overflows = {}, avg = {}", poll_pre_overflow, overflows,
		 overflows > 0 ? poll_pre_overflow / overflows : 0
		 );
  }

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

    [[nodiscard]] auto num_send_buffers() const -> std::size_t {
        return num_send_buffers_;
    }

    void max_num_send_buffers(std::size_t num_buffers) {
        max_num_send_buffers_ = num_buffers;
    }

    void reserve_send_buffers(std::size_t num_buffers) {
        auto buffer_size = local_threshold();
        if (buffer_size == std::numeric_limits<size_t>::max()) {
            buffer_size = 0;
        }
        reserve_send_buffers(num_buffers, buffer_size);
    }

    void reserve_send_buffers(std::size_t num_buffers, std::size_t buffer_size) {
        if (num_send_buffers() + num_buffers > max_num_send_buffers_) {
            throw std::runtime_error("Exceeded maximum number of send buffers.");
        }
        auto old_size = buffer_free_list_.size();
        buffer_free_list_.resize(old_size + num_buffers);
        for (auto& buf : std::ranges::subrange(buffer_free_list_.begin() + old_size, buffer_free_list_.end())) {
            num_send_buffers_++;
            buf.reserve(buffer_size);
        }
    }

    auto get_some_free_buffer() -> std::optional<BufferContainer> {
        // spdlog::debug("free_list.size={}", buffer_free_list_.size());
        if (buffer_free_list_.empty()) {
            if (num_send_buffers_ < max_num_send_buffers_) {
                reserve_send_buffers(1);
            } else {
                if (buffers_.size() - 1 >= max_num_send_buffers_ /* minus the buffer we try to create */) {
		  spdlog::debug("Flushing largest buffer to recover buffer space.");
		  flush_largest_buffer();
                }
                // spdlog::debug("free_list={} + queue.used_send_slots={} + buffers_.size()={} - 1 == num_send_buffers={}",
                //               buffer_free_list_.size(), queue_.used_send_slots(), buffers_.size(), num_send_buffers_);
		// KASSERT(buffer_free_list_.size() + queue_.used_send_slots() + buffers_.size() - 1 == num_send_buffers_);
                return std::nullopt;
            }
        }
        KASSERT(!buffer_free_list_.empty());
        auto buffer = std::move(buffer_free_list_.back());
        buffer_free_list_.pop_back();
	// spdlog::debug("free_list={} + queue.used_send_slots={} + buffers_.size()={} == num_send_buffers={}", buffer_free_list_.size(),
        //               queue_.used_send_slots(), buffers_.size(), num_send_buffers_);
        // KASSERT(buffer_free_list_.size() + queue_.used_send_slots() + buffers_.size() == num_send_buffers_);
        return buffer;
    };

    /// Note: messages have to be passed as rvalues. If you want to send static
    /// data without an additional copy, wrap it in a std::ranges::ref_view.
    bool post_message_impl(InputMessageRange<MessageType> auto&& message,
                           PEID receiver,
                           PEID envelope_sender,
                           PEID envelope_receiver,
                           int tag,
                           OverflowHandler<BufferMap> auto&& handle_overflow,
                           BufferProvider<BufferContainer> auto&& get_new_buffer) {
        auto it = buffers_.find(receiver);
        if (it == buffers_.end()) {
	    it = buffers_.emplace(receiver, BufferContainer {}).first;
	    it->second = get_new_buffer();
        }

        auto& buffer = it->second;
        auto envelope =
            MessageEnvelope{std::forward<decltype(message)>(message), envelope_sender, envelope_receiver, tag};
        size_t estimated_new_buffer_size = 0;
        if constexpr (aggregation::EstimatingMerger<Merger, MessageType, BufferContainer>) {
            estimated_new_buffer_size = merge.estimate_new_buffer_size(buffer, receiver, queue_.rank(), envelope);
        } else {
            estimated_new_buffer_size = buffer.size() + envelope.message.size();
        }
        auto old_buffer_size = buffer.size();
        bool overflow = false;
        if (check_for_buffer_overflow(buffer, estimated_new_buffer_size - old_buffer_size)) {
            overflow = true;
            handle_overflow(it);
            // KASSERT(buffer.empty());
            buffer = get_new_buffer();
        }
        PEID rank = 0;
        auto old_buffer_capacity = buffer.capacity();
        merge(buffer, receiver, queue_.rank(), std::move(envelope));
        auto new_buffer_capacity = buffer.capacity();
        // if (old_buffer_capacity != new_buffer_capacity) {
        //     spdlog::debug("Merge changed buffer capacity from {} to {}", old_buffer_capacity, new_buffer_capacity);
        // }
        auto new_buffer_size = buffer.size();
        global_buffer_size_ += new_buffer_size - old_buffer_size;
        return overflow;
    }

    int poll_pre_overflow = 0;
    int overflows = 0;

    bool post_message_blocking(InputMessageRange<MessageType> auto &&message,
                               PEID receiver, PEID envelope_sender,
                               PEID envelope_receiver, int tag,
                               MessageHandler<MessageType> auto &&on_message) {
      using namespace kamping::measurements;
      auto ret =  post_message_impl(
          std::forward<decltype(message)>(message), receiver, envelope_sender,
          envelope_receiver, tag,
				    [&](auto it) {
				      overflows++;
            timer().start("poll_pre_overflow");
            while (true) {
	      poll_pre_overflow++;
              auto res = poll(std::forward<decltype(on_message)>(on_message));
              if (res && res->first) { // finished some send
                break;
              }
            }
	    timer().stop_and_add();
            bool success = resolve_overflow(it);
            if (success) {
              return;
              // break;
            }
            // KASSERT(false);
          },
          [&] {
            while (true) {
              auto buf = get_some_free_buffer();
              if (buf.has_value()) {
                return std::move(*buf);
              }
	      throw std::runtime_error("This should not happen");
              // KASSERT(false);
              // spdlog::debug("Polling");
              poll(std::forward<decltype(on_message)>(on_message));
            }
          });
      return ret;
    }

    /// Note: messages have to be passed as rvalues. If you want to send static
    /// data without an additional copy, wrap it in a std::ranges::ref_view.
    bool post_message_blocking(InputMessageRange<MessageType> auto&& message,
                               PEID receiver,
                               MessageHandler<MessageType> auto&& on_message,
                               int tag = 0) {
        return post_message_blocking(std::forward<decltype(message)>(message), receiver, rank(), receiver, tag,
                                     std::forward<decltype(on_message)>(on_message));
    }

    bool post_message_blocking(MessageType message,
                               PEID receiver,
                               MessageHandler<MessageType> auto&& on_message,
                               int tag = 0) {
        return post_message_blocking(std::ranges::views::single(message), receiver,
                                     std::forward<decltype(on_message)>(on_message), tag);
    }

    /// Note: messages have to be passed as rvalues. If you want to send static
    /// data without an additional copy, wrap it in a std::ranges::ref_view.
    ///
    /// if the message box capacity of the underlying queue is bounded, than this may fail and throw an exception
    bool post_message(InputMessageRange<MessageType> auto&& message,
                      PEID receiver,
                      PEID envelope_sender,
                      PEID envelope_receiver,
                      int tag) {
        return post_message_impl(
            std::forward<decltype(message)>(message), receiver, envelope_sender, envelope_receiver, tag,
            [&](auto it) {
                bool success = resolve_overflow(it);
                if (!success) {
                    throw std::runtime_error(
                        "Failed to resolve overflow, because sending to the underlying queue failed.");
                }
            },
            [&] {
                auto buf = get_some_free_buffer();
                if (!buf.has_value()) {
                    throw std::runtime_error("Failed to resolve overflow, because no free buffer was available.");
                }
                return std::move(*buf);
            });
    }

    /// Note: messages have to be passed as rvalues. If you want to send static
    /// data without an additional copy, wrap it in a std::ranges::ref_view
    ////
    /// if the message box capacity of the underlying queue is bounded, than this may fail and throw an exception
    bool post_message(InputMessageRange<MessageType> auto&& message, PEID receiver, int tag = 0) {
        return post_message(std::forward<decltype(message)>(message), receiver, rank(), receiver, tag);
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
            auto new_it = flush_buffer_impl(it);
            // if (new_it == it) {
            //   return false;
            // }
            return buffer_was_empty;
        }
        return false;
    }

    void flush_all_buffers() {
      flush_all_buffers_impl(buffers_.end(), [] {}, [] { return false; });
    }

    void flush_all_buffers_and_poll_until_reactivated(
        MessageHandler<MessageType> auto &&on_message) {
      flush_all_buffers_impl(
          buffers_.end(),
          [&] { // pre_flush_hook
            poll(std::forward<decltype(on_message)>(on_message));
          },
          [&](bool current_flush_successful) { // post flush_hook
            // if (!current_flush_successful) {
            // poll(std::forward<decltype(on_message)>(on_message));
            // }
            return termination_state() == TerminationState::active;
          },
          false);
    }

    void flush_largest_buffer() {
        flush_largest_buffer_impl(buffers_.end());
    }

    /// Note: Message handlers take a MessageEnvelope as single argument. The
    /// Envelope (not necessarily the underlying data) is moved to the handler
    /// when called.
    auto poll(MessageHandler<MessageType> auto &&on_message)
        -> std::optional<std::pair<bool, bool>> {
      return queue_.poll(split_handler(on_message),
                         [&](std::size_t receipt, BufferContainer buffer) {
                           recover_buffer(receipt, std::move(buffer));
                         });
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
        bool ret = queue_.terminate(
            split_handler(on_message),
            [&](std::size_t receipt, BufferContainer buffer) { recover_buffer(receipt, std::move(buffer)); },
            before_next_message_counting_round_hook, progress_hook);
        in_terminate = false;
        return ret;
    }

    void reactivate() {
        queue_.reactivate();
    }

    [[nodiscard]] TerminationState termination_state() const {
        return queue_.termination_state();
    }

    bool progress_sending() {
        return queue_.progress_sending(
            [&](std::size_t receipt, BufferContainer buffer) { recover_buffer(receipt, std::move(buffer)); });
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

    [[nodiscard]] size_t global_threshold() const {
        if (global_threshold_bytes_ == std::numeric_limits<std::size_t>::max()) {
            return std::numeric_limits<std::size_t>::max();
        }
        return global_threshold_bytes_ / sizeof(BufferType);
    }

    [[nodiscard]] size_t global_threshold_bytes() const {
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

    [[nodiscard]] size_t local_threshold_bytes() const {
        return local_threshold_bytes_;
    }

    [[nodiscard]] size_t local_threshold() const {
        if (local_threshold_bytes_ == std::numeric_limits<std::size_t>::max()) {
            return std::numeric_limits<std::size_t>::max();
        }
        return local_threshold_bytes_ / sizeof(BufferType);
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
    /// @return an iterator to the next buffer, or the input iterator if flushing failed
    auto flush_buffer_impl(BufferMap::iterator buffer_it, bool erase = true) -> BufferMap::iterator {
        KASSERT(buffer_it != buffers_.end(), "Trying to flush non-existing buffer.");
        auto& [receiver, buffer] = *buffer_it;
        if (buffer.empty()) {
            // spdlog::debug("Buffer for rank {} is empty", receiver);
            return ++buffer_it;
        }
        auto pre_cleanup_buffer_size = buffer.size();
        pre_send_cleanup(buffer, receiver);
        // we don't send if the cleanup has emptied the buffer
        if (buffer.empty()) {
            // spdlog::debug("Buffer for rank {} is empty after cleanup", receiver);
            return ++buffer_it;
        }
        flush_buffer_calls++;
        if (in_terminate) {
            flush_buffer_calls_in_terminate++;
        }
        // auto it = std::ranges::find_if(in_transit_buffers_, [](auto& slot) { return !slot.has_value(); });
        // if (it == in_transit_buffers_.end()) {
        //   return buffer_it;
        // }
        // (*it)->second.swap(buffer);
        // available_in_transit_slots_--;
        // auto buf = std::ranges::ref_view{(*it)->second};
        if (queue_.total_remaining_capacity() == 0) {
            return buffer_it;
        }
        auto receipt = queue_.post_message(std::move(buffer_it->second), receiver);
        if (!receipt.has_value()) {
          throw std::runtime_error("posting should never fail");
        }
        KASSERT(receipt.has_value());
        // posting the message should never fail, because we check for remaining capacity
        global_buffer_size_ -= pre_cleanup_buffer_size;
        if (erase) {
            return buffers_.erase(buffer_it);
        }
        return ++buffer_it;
    }

    /// if post_flush_hook return true, this breaks the loop
    template <typename PreFlushHook, typename PostFlushHook>
      requires std::invocable<PreFlushHook> &&
               (std::predicate<PostFlushHook> ||
                std::predicate<PostFlushHook, bool>)
    bool flush_all_buffers_impl(BufferMap::iterator current_buffer,
                                PreFlushHook &&pre_flush_hook,
                                PostFlushHook &&post_flush_hook,
                                bool break_when_flush_fails = true) {
      auto it = buffers_.begin();
      bool flushed_something = false;
      while (it != buffers_.end()) {
	pre_flush_hook();
        bool current_flush_successful = false;
        auto new_it = flush_buffer_impl(it, it != current_buffer);
        if (new_it != it) {
          flushed_something = true;
          it = new_it;
          current_flush_successful = true;
        } else {
          if (break_when_flush_fails) {
            return flushed_something;
          }
        }
	bool should_break = [&] {
	  if constexpr (std::predicate<PostFlushHook>) {
	    return post_flush_hook();
	  } else {
	    return post_flush_hook(current_flush_successful);
	  }
	}();
	if (should_break) {
	  return flushed_something;
	}
      }
      return flushed_something;
    }

    [[nodiscard]] bool flush_largest_buffer_impl(BufferMap::iterator current_buffer) {
        auto largest_buffer = std::max_element(buffers_.begin(), buffers_.end(),
                                               [](auto& a, auto& b) { return a.second.size() < b.second.size(); });
        if (largest_buffer != buffers_.end()) {
            auto it = flush_buffer_impl(largest_buffer, largest_buffer != current_buffer);
            return it != largest_buffer;
        }
        return true;
    }

    auto split_handler(MessageHandler<MessageType> auto&& on_message) {
        return [&](Envelope<BufferType> auto buffer) {
            for (Envelope<MessageType> auto env : split(buffer.message, buffer.sender, queue_.rank())) {
                on_message(std::move(env));
            }
        };
    }

    auto recover_buffer(std::size_t receipt, BufferContainer buffer) {
        // find slot associated with receipt and clean it up.
        // auto it = std::ranges::find_if(in_transit_buffers_,
        //                                [&](std::optional<std::pair<std::size_t, BufferContainer>>& elem) {
        //                                    if (!elem.has_value()) {
        //                                        return false;
        //                                    }
        //                                    return elem->first == receipt;
        //                                });
        // KASSERT(it != in_transit_buffers_.end());
        // auto buf = std::move(*it).value();
        // available_in_transit_slots_++;
        // KASSERT(!it->has_value());
        auto old_capacity = buffer.capacity();
        buffer.resize(0);  // this does not reduce the capacity
        auto new_capacity = buffer.capacity();
        // if (old_capacity != new_capacity) {
        //     spdlog::debug("Changing buffer capacity from {} to {}", old_capacity, new_capacity);
        // }
        buffer_free_list_.emplace_back(std::move(buffer));
    }

    /// @return returns false iff resolve failed
    bool resolve_overflow(BufferMap::iterator current_buffer) {
        switch (flush_strategy_) {
            case FlushStrategy::local: {
                auto ret = flush_buffer_impl(current_buffer, /*erase=*/false);
                return ret != current_buffer;
            }
            case FlushStrategy::global: {
              return flush_all_buffers_impl(current_buffer, [] {}, [] { return false; });
            }
            case FlushStrategy::random: {
                throw std::runtime_error("Random flush strategy not implemented");
                return false;
            }
            case FlushStrategy::largest: {
                return flush_largest_buffer_impl(current_buffer);
            }
        }
        // unreachable
        return false;
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
    MessageQueue<BufferType, BufferContainer, ReceiveBufferContainer> queue_;
    BufferMap buffers_;
    BufferList buffer_free_list_;
    std::size_t num_send_buffers_ = 0;
    std::size_t max_num_send_buffers_ = std::numeric_limits<std::size_t>::max();
    std::vector<std::optional<std::pair<std::size_t, BufferContainer>>> in_transit_buffers_;
    std::size_t available_in_transit_slots_;
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
