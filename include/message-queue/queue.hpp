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
#include <kamping/mpi_datatype.hpp>
#include <kassert/kassert.hpp>
#include <limits>
#include <optional>
#include <ranges>  // IWYU pragma: keep
#include <span>
#include <utility>
#include <vector>
#include "message-queue/concepts.hpp"
#include "message-queue/debug_print.hpp"

namespace message_queue {

namespace internal {
size_t comm_size(MPI_Comm comm = MPI_COMM_WORLD);

class RequestPool {
public:
    RequestPool(std::size_t capacity = 0) : requests(capacity, MPI_REQUEST_NULL), indices(capacity) {}

    std::optional<std::pair<int, MPI_Request*>> get_some_inactive_request(int hint = -1);

    template <typename CompletionFunction>
    void test_some(CompletionFunction&& on_complete = [](int) {}) {
        int outcount;
        max_test_size_ = std::max(max_test_size_, active_range.second - active_range.first);
        MPI_Testsome(active_range.second - active_range.first,  // count
                     requests.data() + active_range.first,      // requests
                     &outcount,                                 // outcount
                     indices.data(),                            // indices
                     MPI_STATUSES_IGNORE                        // statuses
        );
        if (outcount != MPI_UNDEFINED) {
            auto index_offset = active_range.first;
            std::for_each_n(indices.begin(), outcount, [&](int index) {
                // map index to the global index
                index += index_offset;
                remove_from_active_range(index);
                track_max_active_requests();
                on_complete(index);
            });
        }
    }

    template <typename CompletionFunction>
    void test_any(CompletionFunction&& on_complete = [](int) {}) {
        int flag;
        int index;
        max_test_size_ = std::max(max_test_size_, active_range.second - active_range.first);
        MPI_Testany(active_range.second - active_range.first,  // count
                    requests.data() + active_range.first,      // requests
                    &index,                                    // index
                    &flag,                                     // flag
                    MPI_STATUS_IGNORE                          // status
        );
        if (flag && index != MPI_UNDEFINED) {
            index += active_range.first;
            remove_from_active_range(index);
            track_max_active_requests();
            on_complete(index);
        }
    }

    /// my request functions {{{
    template <typename CompletionFunction>
    void my_test_any(CompletionFunction&& on_complete = [](int) {}) {
        max_test_size_ = std::max(max_test_size_, active_range.second - active_range.first);
        for (int i = active_range.first; i < active_range.second; i++) {
            if (requests[i] == MPI_REQUEST_NULL) {
                continue;
            }
            int flag;
            MPI_Test(&requests[i], &flag, MPI_STATUS_IGNORE);
            if (flag) {
                remove_from_active_range(i);
                track_max_active_requests();
                on_complete(i);
                return;
            }
        }
    }

    template <typename CompletionFunction>
    void my_test_some(CompletionFunction&& on_complete = [](int) {}) {
        max_test_size_ = std::max(max_test_size_, active_range.second - active_range.first);
        for (int i = active_range.first; i < active_range.second; i++) {
            if (requests[i] == MPI_REQUEST_NULL) {
                continue;
            }
            int flag;
            MPI_Test(&requests[i], &flag, MPI_STATUS_IGNORE);
            if (flag) {
                remove_from_active_range(i);
                track_max_active_requests();
                on_complete(i);
            }
        }
    }

    /// }}}

    std::size_t active_requests() const {
        return active_requests_;
    }

    std::size_t inactive_requests() const {
        return capacity() - active_requests();
    }

    std::size_t capacity() const {
        return requests.size();
    }

    int max_test_size() const {
        return max_test_size_;
    }

    size_t max_active_requests() const {
        return max_active_requests_;
    }

private:
    void track_max_active_requests() {
        max_active_requests_ = std::max(max_active_requests_, active_requests());
    }

    void add_to_active_range(int index) {
        active_requests_++;
        if (index < active_range.first) {
            active_range.first = index;
        }
        if (index + 1 > active_range.second) {
            active_range.second = index + 1;
        }
    }

    void remove_from_active_range(int index) {
        active_requests_--;
        if (index == active_range.first) {
            active_range.first++;
        } else {
            if (index + 1 == active_range.second) {
                active_range.second--;
            }
        }
    }
    std::vector<MPI_Request> requests;
    std::vector<int> indices;
    size_t inactive_request_pointer = 0;
    size_t active_requests_ = 0;
    std::pair<int, int> active_range = {0, 0};
    int max_test_size_ = 0;
    std::size_t max_active_requests_ = 0;
};

namespace handles {
template <MPIType S, MPIBuffer MessageContainer = std::vector<S>>
requires std::same_as<S, typename std::ranges::range_value_t<MessageContainer>>
class MessageHandle {
public:
    // MessageHandle() {};
    // MessageHandle(MessageHandle const&) = delete;
    // MessageHandle(MessageHandle&&) = default;
    // MessageHandle& operator=(MessageHandle const&) = delete;
    // MessageHandle& operator=(MessageHandle&&) = delete;

    bool test() {
        if (request_ == nullptr) {
            return false;
        }
        int finished = false;
        int err = MPI_Test(request_, &finished, MPI_STATUS_IGNORE);
        check_mpi_error(err, __FILE__, __LINE__);
        if (finished) {
            return true;
        }
        return false;
    }

    void set_request(MPI_Request* req) {
        request_ = req;
    }

    int tag() const {
        return tag_;
    }

    MessageContainer extract_message() {
        return std::move(*this->message_);
    }

protected:
    size_t message_size_ = 0;
    std::optional<MessageContainer> message_;
    MPI_Request* request_ = nullptr;
    size_t request_id_;
    int tag_ = MPI_ANY_TAG;
};

template <MPIType S, MPIBuffer MessageContainer = std::vector<S>>
class SendHandle : public MessageHandle<S, MessageContainer> {
public:
    SendHandle(MPI_Comm comm = MPI_COMM_NULL) : MessageHandle<S, MessageContainer>(), comm_(comm) {}

    void initiate_send() {
        // std::cout << "tag=" << this->tag_ << " size=" << this->message_.size() << "\n";
        // atomic_debug(fmt::format("Isend m={}, receiver={}, tag={}, comm={}", this->message_, receiver_, this->tag_,
        // format(this->comm_)));
        int err = MPI_Isend(std::data(*this->message_), std::size(*this->message_), kamping::mpi_datatype<S>(), receiver_,
                            this->tag_, this->comm_, this->request_);
        check_mpi_error(err, __FILE__, __LINE__);
    }

    void set_message(MessageContainer message) {
        this->message_ = std::move(message);
    }

    void set_receiver(PEID receiver) {
        this->receiver_ = receiver;
    }

    void set_tag(int tag) {
        this->tag_ = tag;
    }

    void set_request_id(size_t request_id) {
        this->request_id_ = request_id;
    }

    [[nodiscard]] auto get_request_id() const -> std::size_t {
        return this->request_id_;
    }

    PEID receiver() const {
        return receiver_;
    }

    void swap(SendHandle& other) {
        std::swap(this->receiver_, other.receiver_);
        std::swap(this->comm_, other.comm_);
        std::swap(this->request_, other.request_);
        std::swap(this->request_id_, other.request_id_);
        std::swap(this->tag_, other.tag_);
        std::swap(this->message_size_, other.message_size_);
	std::swap(this->message_, other.message_);
    }
    void emplace(SendHandle&& other) {
        this->swap(other);
    }

private:
    PEID receiver_ = MPI_ANY_SOURCE;
    MPI_Comm comm_;
};

template <MPIType S, MPIBuffer MessageContainer = std::vector<S>>
class ReceiveHandle : public MessageHandle<S, MessageContainer> {
    MPI_Message matched_message_ = MPI_MESSAGE_NO_PROC;
    MPI_Status status_;
    PEID sender_ = MPI_ANY_SOURCE;

public:
    void start_receive() {
        this->message_.resize(this->message_size_);
        // atomic_debug(fmt::format("Imrecv msg={}", format(matched_message_)));
        MPI_Imrecv(this->message_.data(), this->message_.size(), kamping::mpi_datatype<S>(), &matched_message_,
                   this->request_);
    }

    void start_receive_into(MPIBuffer<S> auto& buffer) {
        MPI_Imrecv(buffer.data(), buffer.size(), kamping::mpi_datatype<S>(), &matched_message_, this->request_);
    }

    size_t message_size() const {
        return this->message_size_;
    }

    void receive() {
        this->message_->resize(this->message_size_);
        // atomic_debug(fmt::format("Mrecv msg={}", format(matched_message_)));
        MPI_Mrecv(std::data(*this->message_), std::size(*this->message_), kamping::mpi_datatype<S>(), &this->matched_message_,
                  MPI_STATUS_IGNORE);
    }

    PEID sender() const {
        return sender_;
    }

    
    friend struct ProbeResult;
};

struct ProbeResult {
    MPI_Status status;
    MPI_Message matched_message;
    MPI_Comm comm;
    PEID sender;
    int tag;

    template <MPIType S, MPIBuffer MessageContainer = std::vector<S>>
    ReceiveHandle<S, MessageContainer> handle() {
        ReceiveHandle<S, MessageContainer> handle;
        int message_size;
        MPI_Get_count(&status, kamping::mpi_datatype<S>(), &message_size);
        handle.message_size_ = message_size;
        handle.matched_message_ = matched_message;
        handle.tag_ = status.MPI_TAG;
        handle.sender_ = status.MPI_SOURCE;
        return handle;
    }
};

static std::optional<ProbeResult> probe(MPI_Comm comm, PEID source = MPI_ANY_SOURCE, PEID tag = MPI_ANY_TAG) {
    ProbeResult result;
    int message_found = false;
    // atomic_debug(fmt::format("Improbe source={}, tag={}, comm={}", format_rank(source), format_tag(tag),
    // format(comm)));
    MPI_Improbe(source, tag, comm, &message_found, &result.matched_message, &result.status);
    if (message_found) {
        // atomic_debug(fmt::format("probe succeeded with source={}, tag={}, comm={}, msg", result.status.MPI_SOURCE,
        //                          result.status.MPI_TAG, format(comm), format(result.matched_message)));
        result.sender = result.status.MPI_SOURCE;
        result.tag = result.status.MPI_TAG;
        result.comm = comm;
        return result;
    }
    return std::nullopt;
}
}  // namespace handles

struct MessageCounter {
    size_t send;
    size_t receive;
    auto operator<=>(const MessageCounter&) const = default;
};

}  // namespace internal

enum class ReceiveMode { poll, posted_receives, persistent };

enum class TerminationState { active, trying_termination, terminated };

template <MPIType T,
          MPIBuffer<T> MessageContainer = std::vector<T>,
          MPIBuffer<T> ReceiveBufferContainer = std::vector<T>>
class MessageQueue {
    enum class State { posted, initiated, completed };

    bool try_send_something_from_message_box(int hint = -1) {
        if (outgoing_message_box.empty()) {
            return false;
        }
        internal::handles::SendHandle<T, MessageContainer>& message_to_send = outgoing_message_box.front();
        if (auto returned_handle = try_send_something(hint, std::move(message_to_send))) {
            message_to_send = std::move(*returned_handle);
            return false;
        }
        outgoing_message_box.pop_front();
        return true;
    }

    auto try_send_something(int hint, internal::handles::SendHandle<T, MessageContainer>&& message_to_send)
        -> std::optional<internal::handles::SendHandle<T, MessageContainer>> {
        if (auto result = request_pool.get_some_inactive_request(hint)) {
            // internal::handles::SendHandle<T, MessageContainer> message_to_send =
            //     std::move(outgoing_message_box.front());

            auto [index, req_ptr] = *result;
            message_to_send.set_request(req_ptr);
            // atomic_debug(fmt::format("isend msg={}, to {}, using request {}", message_to_send.message,
            // message_to_send.receiver, index));
            in_transit_messages[index].swap(message_to_send);
            in_transit_messages[index].initiate_send();
            // KASSERT(*message_to_send.request_ != MPI_REQUEST_NULL);
            return std::nullopt;
        }
        return std::move(message_to_send);
    }

    static int comm_tag_ub(MPI_Comm comm) {
        int flag;
        int* tag_ub;
        MPI_Comm_get_attr(MPI_COMM_WORLD, MPI_TAG_UB, &tag_ub, &flag);
        return *tag_ub;
    }

    void setup_receives() {
        for (size_t i = 0; i < receive_buffers.size(); i++) {
            auto& req = receive_requests[i];
            cancel_receive(i);
            auto& buf = receive_buffers[i];
            buf.resize(reserved_receive_buffer_size_);
            init_receive(i);
            restart_receive(i);
        }
    }
    void init_receive(size_t index) {
        switch (receive_mode_) {
            case ReceiveMode::poll:
                return;
            case ReceiveMode::posted_receives:
                return;
            case ReceiveMode::persistent:
                auto& buf = receive_buffers[index];
                auto& req = receive_requests[index];
                MPI_Recv_init(buf.data(), reserved_receive_buffer_size_, kamping::mpi_datatype<T>(), MPI_ANY_SOURCE,
                              SMALL_MESSAGE_TAG, comm_, &req);
                return;
        }
    }

    void restart_receive(size_t index) {
        auto& req = receive_requests[index];
        auto& buf = receive_buffers[index];
        switch (receive_mode_) {
            case ReceiveMode::poll:
                return;
            case ReceiveMode::posted_receives:
                MPI_Irecv(buf.data(), reserved_receive_buffer_size_, kamping::mpi_datatype<T>(), MPI_ANY_SOURCE,
                          SMALL_MESSAGE_TAG, comm_, &req);
                return;
            case ReceiveMode::persistent:
                MPI_Start(&req);
                return;
        }
    }

    void cancel_receive(size_t index) {
        if (receive_mode_ == ReceiveMode::poll) {
            return;
        }
        auto& req = receive_requests[index];
        if (req != MPI_REQUEST_NULL) {
            MPI_Cancel(&req);
            if (receive_mode_ == ReceiveMode::persistent) {
                MPI_Request_free(&req);
            }
        }
    }

public:
    MessageQueue(MPI_Comm comm, size_t num_request_slots, size_t reserved_receive_buffer_size, ReceiveMode receive_mode)
        : outgoing_message_box(),
          request_pool(num_request_slots),
          in_transit_messages(num_request_slots),
          messages_to_receive(),
          reserved_receive_buffer_size_(reserved_receive_buffer_size),
          receive_buffers(num_request_slots),
          receive_requests(num_request_slots, MPI_REQUEST_NULL),
          indices(num_request_slots),
          statuses(num_request_slots),
          request_id_(0),
          comm_(comm),
          rank_(0),
          size_(0),
          TAG_UB(comm_tag_ub(comm)),
          LARGE_MESSAGE_TAG(TAG_UB - 1),
          SMALL_MESSAGE_TAG(TAG_UB - 2),
          receive_mode_(receive_mode) {
        MPI_Comm_rank(comm_, &rank_);
        MPI_Comm_size(comm_, &size_);

        // ensure that we build the datatype as early as possible
        // cast to void to silence nodiscard warning
        static_cast<void>(kamping::mpi_datatype<T>());
        setup_receives();
    }

    MessageQueue(MessageQueue&&) = default;
    MessageQueue(MessageQueue const&) = delete;
    MessageQueue& operator=(MessageQueue&& other) {
        for (size_t i = 0; i < receive_requests.size(); i++) {
            cancel_receive(i);
        }

        this->outgoing_message_box = std::move(other.outgoing_message_box);
        this->request_pool = std::move(other.request_pool);
        this->in_transit_messages = other.in_transit_messages;
        this->messages_to_receive = std::move(other.messages_to_receive);
        this->reserved_receive_buffer_size_ = other.reserved_receive_buffer_size_;
        this->receive_buffers = std::move(other.receive_buffers);
        this->receive_requests = std::move(other.receive_requests);
        this->indices = std::move(other.indices);
        this->statuses = std::move(other.statuses);
        this->local_message_count = other.local_message_count;
        this->message_count_reduce_buffer = other.message_count_reduce_buffer;
        this->global_message_count = other.global_message_count;
        this->termination_request = other.termination_request;
        this->request_id_ = other.request_id_;
        this->comm_ = other.comm_;
        this->rank_ = other.rank_;
        this->size_ = other.size_;
        this->TAG_UB = other.TAG_UB;
        this->LARGE_MESSAGE_TAG = other.LARGE_MESSAGE_TAG;
        this->SMALL_MESSAGE_TAG = other.SMALL_MESSAGE_TAG;
        this->receive_mode_ = other.receive_mode_;
        this->allow_large_messages_ = other.allow_large_messages_;
        this->termination_state_ = other.termination_state_;
        this->number_of_waves = other.number_of_waves;
        this->max_probe_rounds_ = other.max_probe_rounds_;
        this->use_test_any_ = other.use_test_any_;
        this->use_custom_implementation_ = other.use_custom_implementation_;
        return *this;
    }
    MessageQueue& operator=(MessageQueue const&) = delete;

    // MessageQueue(MPI_Comm comm = MPI_COMM_WORLD)
    // : MessageQueue(comm, internal::comm_size(comm), 32 * 1024 / sizeof(T)) {}

    ~MessageQueue() {
        if (receive_mode_ == ReceiveMode::poll) {
            return;
        }
        for (size_t i = 0; i < receive_requests.size(); i++) {
            cancel_receive(i);
        }
    }

    size_t reserved_receive_buffer_size() const {
        return reserved_receive_buffer_size_;
    }

    void reserved_receive_buffer_size(size_t size) {
        reserved_receive_buffer_size_ = size;
        setup_receives();
    }

    void allow_large_messages(bool allow = true) {
        allow_large_messages_ = allow;
    }

    [[nodiscard]] size_t message_box_capacity() const {
        return message_box_capacity_;
    }

    void message_box_capacity(size_t capacity) {
        message_box_capacity_ = capacity;
    }

    /// Post a message to the message queue
    /// Posting a message may fail if the message box is full and no send slots are available.
    /// @return an optional containing the request id if the message was successfully posted, otherwise nullopt
    auto post_message(MessageContainer&& message, PEID receiver) -> std::optional<std::size_t> {
        // atomic_debug(fmt::format("enqueued msg={}, to {}", message, receiver));
        // assert(receiver != rank_);
        if (message.size() == 0) {
            return std::nullopt;
        }
        internal::handles::SendHandle<T, MessageContainer> handle(comm_);
        // std::cout << "message.size=" << message.size();
        // std::cout << " tag=" << handle.tag() << "\n";
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
        if (outgoing_message_box.empty()) {
            // we can try to send directly
            auto returned_handle = try_send_something(-1, std::move(handle));
            if (returned_handle.has_value()) {
                // no slots available
                return std::nullopt;
            } else {
                // succeeded
                auto receipt = std::optional{this->request_id_};
                this->request_id_++;
                local_message_count.send++;
                return receipt;
            }
        } else {
            // try appending to the message box
            if (outgoing_message_box.size() >= message_box_capacity_) {
                // the message box is full
                return std::nullopt;
            }
            outgoing_message_box.emplace_back(std::move(handle));
            auto receipt = std::optional{this->request_id_};
            this->request_id_++;
            try_send_something_from_message_box();
            local_message_count.send++;
            return receipt;
        }
    }

    auto post_message(T message, PEID receiver) -> std::optional<std::size_t> {
        // atomic_debug(fmt::format("enqueued msg={}, to {}", message, receiver));
        // assert(receiver != rank_);
        MessageContainer message_vector{std::move(message)};
        return post_message(std::move(message_vector), receiver);
    }
  

    bool progress_sending(SendFinishedCallback<MessageContainer> auto on_finished_sending) {
        constexpr bool move_back_message = std::invocable<decltype(on_finished_sending), std::size_t, MessageContainer>;
        // check for finished sends and try starting new ones
        bool something_happenend = false;
        if (true || !use_test_any_) {
            if (!use_custom_implementation_) {
                request_pool.test_some([&](int completed_request_index) {
                    std::size_t request_id = in_transit_messages[completed_request_index].get_request_id();
                    auto message = in_transit_messages[completed_request_index].extract_message();
                    in_transit_messages[completed_request_index].emplace({comm_});
                    something_happenend = true;
		    if constexpr(move_back_message) {
		      on_finished_sending(request_id, std::move(message));
                    } else {
		      on_finished_sending(request_id);                        
                    }                       
                    try_send_something_from_message_box(completed_request_index);
                });
            } else {
                request_pool.my_test_some([&](int completed_request_index) {
                    std::size_t request_id = in_transit_messages[completed_request_index].get_request_id();
                    auto message = in_transit_messages[completed_request_index].extract_message();                    
                    in_transit_messages[completed_request_index].emplace({comm_});
                    something_happenend = true;
		    if constexpr(move_back_message) {
		      on_finished_sending(request_id, std::move(message));
                    } else {
		      on_finished_sending(request_id);                        
                    }                                           
                    try_send_something_from_message_box(completed_request_index);
                });
            }
        } else {
            bool progress = true;
            while (progress) {
                progress = false;
                if (!use_custom_implementation_) {
                    request_pool.test_any([&](int completed_request_index) {
                        std::size_t request_id = in_transit_messages[completed_request_index].get_request_id();
			auto message = in_transit_messages[completed_request_index].extract_message();                        
                        in_transit_messages[completed_request_index].emplace({comm_});
                        something_happenend = true;
                        progress = true;
			if constexpr(move_back_message) {
			  on_finished_sending(request_id, std::move(message));
			} else {
			  on_finished_sending(request_id);                        
			}                       
                        try_send_something_from_message_box(completed_request_index);
                    });
                } else {
                    request_pool.my_test_any([&](int completed_request_index) {
                        std::size_t request_id = in_transit_messages[completed_request_index].get_request_id();
			auto message = in_transit_messages[completed_request_index].extract_message();                        
                        in_transit_messages[completed_request_index].emplace({comm_});
                        something_happenend = true;
                        progress = true;
		    if constexpr(move_back_message) {
		      on_finished_sending(request_id, std::move(message));
                    } else {
		      on_finished_sending(request_id);                        
                    }                                               
                        try_send_something_from_message_box(completed_request_index);
                    });
                }
            }
        }
        while (try_send_something_from_message_box()) {
        }
        return something_happenend;
    }

    bool probe_for_messages_persistent(MessageHandler<T, std::span<T>> auto&& on_message) {
        int outcount = 1;
        // std::vector<int> indices(receive_requests.size());
        // std::vector<MPI_Status> statuses(receive_requests.size());
        int count = 0;
        bool something_happenend = false;
        size_t rounds = 0;
        while (outcount > 0 && rounds < max_probe_rounds_) {
            if (!use_test_any_) {
                // if here no active handles, return flag=true, index=MPI_UNDEFINED
                MPI_Testsome(static_cast<int>(receive_requests.size()), receive_requests.data(), &outcount,
                             indices.data(), statuses.data());
            } else {
                int flag;
                MPI_Testany(static_cast<int>(receive_requests.size()), receive_requests.data(), &indices[0], &flag,
                            &statuses[0]);
                if (flag) {
                    if (indices[0] == MPI_UNDEFINED) {
                        throw "This should not happen";
                    }
                    outcount = 1;
                } else {
                    outcount = 0;
                }
            }
            // std::cout << "outcount=" << outcount << "\n";
            if (outcount == MPI_UNDEFINED) {
                throw "This should not happen";
            }
            for (int i = 0; i < outcount; i++) {
                something_happenend = true;
                local_message_count.receive++;
                auto request_index = indices[i];
                auto& status = statuses[i];
                auto& buffer = receive_buffers[request_index];
                MPI_Get_count(&status, kamping::mpi_datatype<T>(), &count);
                auto message = std::span(buffer).first(count);
                on_message(
                    MessageEnvelope<decltype(message)>{std::move(message), status.MPI_SOURCE, rank_, status.MPI_TAG});
                restart_receive(request_index);
            }
            rounds++;
        }
        return something_happenend;
    }
    bool probe_for_message_probing(MessageHandler<T, std::span<T>> auto&& on_message) {
        bool something_happenend = false;

        size_t num_recv_requests = 0;
        auto receive_chunk = [&] {
            MPI_Waitall(static_cast<int>(num_recv_requests), receive_requests.data(), MPI_STATUSES_IGNORE);

            for (size_t i = 0; i < messages_to_receive.size(); i++) {
                auto& handle = messages_to_receive[i];
                auto& buffer = receive_buffers[i];
                // atomic_debug(fmt::format("received msg={} from {}", handle.message, handle.sender));
                local_message_count.receive++;
                auto message = std::span(buffer).first(handle.message_size());
                on_message(MessageEnvelope{std::move(message), handle.sender(), rank_, handle.tag()});
            }
            messages_to_receive.clear();
        };
        size_t rounds = 0;
        while (auto probe_result = internal::handles::probe(comm_, MPI_ANY_SOURCE, SMALL_MESSAGE_TAG)) {
            something_happenend = true;
            auto recv_handle = probe_result->template handle<T, ReceiveBufferContainer>();
            // atomic_debug(fmt::format("probed msg from {}", recv_handle.sender));
            MPI_Request recv_req;
            recv_handle.set_request(&receive_requests[num_recv_requests]);
            recv_handle.start_receive_into(receive_buffers[num_recv_requests]);
            messages_to_receive.emplace_back(std::move(recv_handle));
            num_recv_requests++;
            if (num_recv_requests >= receive_requests.size()) {
                receive_chunk();
                num_recv_requests = 0;
            }
            rounds++;
            if (rounds >= max_probe_rounds_) {
                break;
            }
        }
        receive_chunk();
        return something_happenend;
    }

    bool probe_for_messages(MessageHandler<T, std::span<T>> auto&& on_message) {
        // probe for a single large message
        bool something_happened = false;
        if (allow_large_messages_) {
            something_happened = probe_for_one_message(on_message, MPI_ANY_SOURCE, LARGE_MESSAGE_TAG);
        }
        if (receive_mode_ == ReceiveMode::poll) {
            something_happened |= probe_for_message_probing(on_message);
        } else {
            something_happened |= probe_for_messages_persistent(on_message);
        }
        if (something_happened) {
            reactivate();
        }
        return something_happened;
    }

    bool probe_for_one_message(MessageHandler<T, MessageContainer> auto&& on_message,
                               PEID source = MPI_ANY_SOURCE,
                               int tag = MPI_ANY_TAG) {
        if (auto probe_result = internal::handles::probe(comm_, source, tag)) {
            auto recv_handle = probe_result->template handle<T, ReceiveBufferContainer>();
            MPI_Request recv_req = MPI_REQUEST_NULL;
            recv_handle.set_request(&recv_req);
            recv_handle.receive();
            local_message_count.receive++;
            on_message(MessageEnvelope{recv_handle.extract_message(), recv_handle.sender(), rank_, recv_handle.tag()});
            return true;
        }
        return false;
    }

    bool poll(MessageHandler<T, MessageContainer> auto&& on_message) {
      return poll(on_message, [](std::size_t){});
    }

    bool poll(MessageHandler<T, MessageContainer> auto&& on_message, SendFinishedCallback<MessageContainer> auto on_finished_sending) { 
        bool something_happenend = false;
        something_happenend |= progress_sending(on_finished_sending);
        something_happenend |= probe_for_messages(on_message);
        return something_happenend;
    }


    void reset() {
        local_message_count = {.send=0, .receive=0};
        message_count_reduce_buffer = {.send=0, .receive=0};
        global_message_count = {.send=std::numeric_limits<size_t>::max(), .receive=std::numeric_limits<size_t>::max() - 1};
        request_id_ = 0;
        termination_state_ = TerminationState::active;
        number_of_waves = 0;
    }

    void reactivate() {
        if (synchronous_mode_) {
            return;
        }
        termination_state_ = TerminationState::active;
    }

    [[nodiscard]] TerminationState termination_state() const {
        return termination_state_;
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

    bool check_termination_condition() {
        bool terminated = message_count_reduce_buffer == global_message_count &&
                          global_message_count.send == global_message_count.receive;
        if (!terminated) {
            // store for double counting
            global_message_count = message_count_reduce_buffer;
        }
        termination_state_ = TerminationState::terminated;
        return terminated;
    }

    void poll_until_message_box_empty(
        MessageHandler<T, MessageContainer> auto&& on_message,
        std::predicate<> auto&& should_stop_polling = [] { return false; }) {
        while (!outgoing_message_box.empty()) {
            poll(on_message);
            if (should_stop_polling()) {
                return;
            }
        }
    }

    void start_message_counting() {
        if (termination_request == MPI_REQUEST_NULL) {
            message_count_reduce_buffer = local_message_count;
            // atomic_debug(fmt::format("Start reduce with {}", message_count_reduce_buffer));
            MPI_Iallreduce(MPI_IN_PLACE, &message_count_reduce_buffer, 2, kamping::mpi_datatype<size_t>(), MPI_SUM,
                           comm_, &termination_request);
        }
    }

    bool message_counting_finished() {
        if (termination_request == MPI_REQUEST_NULL) {
            return true;
        }
        int reduce_finished = false;
        int err = MPI_Test(&termination_request, &reduce_finished, MPI_STATUS_IGNORE);
        check_mpi_error(err, __FILE__, __LINE__);
        return reduce_finished;
    }

    [[nodiscard]] bool terminate(MessageHandler<T, MessageContainer> auto&& on_message,
                                 std::invocable<> auto&& before_next_message_counting_round_hook,
                                 std::invocable<> auto&& progress_hook) {
        termination_state_ = TerminationState::trying_termination;
        while (true) {
            before_next_message_counting_round_hook();
            if (termination_state_ == TerminationState::active) {
                return false;
            }
            poll_until_message_box_empty(on_message, [&] { return termination_state_ == TerminationState::active; });
            if (termination_state_ == TerminationState::active) {
                return false;
            }
            start_message_counting();
            // poll at least once, so we don't miss any messages
            // if the the message box is empty upon calling this function
            // we never get to poll if message counting finishes instantly
            do {
                poll(on_message);
                progress_hook();
                if (termination_state_ == TerminationState::active) {
                    return false;
                }
            } while (!message_counting_finished());
            if (check_termination_condition()) {
                termination_state_ = TerminationState::terminated;
                return true;
            }
        }
    }

    [[nodiscard]] bool terminate(MessageHandler<T, MessageContainer> auto&& on_message,
                                 std::invocable<> auto&& before_next_message_counting_round_hook) {
        return terminate(
            std::forward<decltype(on_message)>(on_message),
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

    auto max_active_requests() const {
        return request_pool.max_active_requests();
    }

    void use_test_any(bool use_it = true) {
        use_test_any_ = use_it;
    }

    void max_probe_rounds(size_t rounds) {
        max_probe_rounds_ = rounds;
    }

    void use_custom_implementation(bool use_it = true) {
        use_custom_implementation_ = use_it;
    }

private:
    std::deque<internal::handles::SendHandle<T, MessageContainer>> outgoing_message_box;
    std::size_t message_box_capacity_ = std::numeric_limits<size_t>::max();
    internal::RequestPool request_pool;
    std::vector<internal::handles::SendHandle<T, MessageContainer>> in_transit_messages;
    std::vector<internal::handles::ReceiveHandle<T, ReceiveBufferContainer>> messages_to_receive;
    size_t reserved_receive_buffer_size_;
    std::vector<ReceiveBufferContainer> receive_buffers;
    std::vector<MPI_Request> receive_requests;
    std::vector<int> indices;
    std::vector<MPI_Status> statuses;
    internal::MessageCounter local_message_count = {0, 0};
    internal::MessageCounter message_count_reduce_buffer;
    internal::MessageCounter global_message_count = {std::numeric_limits<size_t>::max(),
                                                     std::numeric_limits<size_t>::max() - 1};
    MPI_Request termination_request = MPI_REQUEST_NULL;
    // MessageStatistics stats_;
    size_t request_id_;
    MPI_Comm comm_;
    PEID rank_;
    PEID size_;
    int TAG_UB;
    int LARGE_MESSAGE_TAG;
    int SMALL_MESSAGE_TAG;
    ReceiveMode receive_mode_;
    bool allow_large_messages_ = false;
    TerminationState termination_state_ = TerminationState::active;
    size_t number_of_waves = 0;
    size_t max_probe_rounds_ = std::numeric_limits<size_t>::max();
    bool use_test_any_ = false;
    bool use_custom_implementation_ = false;
    bool synchronous_mode_ = false;
};

}  // namespace message_queue
