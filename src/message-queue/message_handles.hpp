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

#include "./concepts.hpp"

namespace message_queue::internal {

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

    std::optional<MessageContainer> const& message() const {
        return message_;
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
        int err = MPI_Isend(std::data(*this->message_), std::size(*this->message_), kamping::mpi_datatype<S>(),
                            receiver_, this->tag_, this->comm_, this->request_);
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
        MPI_Mrecv(std::data(*this->message_), std::size(*this->message_), kamping::mpi_datatype<S>(),
                  &this->matched_message_, MPI_STATUS_IGNORE);
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
}  // namespace message_queue::internal
