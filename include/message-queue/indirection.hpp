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
#include <cmath>
#include <kassert/kassert.hpp>
#include "message-queue/concepts.hpp"  // IWYU pragma: keep
#include "message-queue/definitions.hpp"

namespace message_queue {
template <typename T>
concept IndirectionScheme = requires(T scheme, MPI_Comm comm, PEID sender, PEID receiver) {
    // { T{comm} };
    { scheme.next_hop(sender, receiver) } -> std::same_as<PEID>;
    { scheme.should_redirect(sender, receiver) } -> std::same_as<bool>;
};

class GridIndirectionScheme {
public:
    GridIndirectionScheme(MPI_Comm comm) : comm_(comm), grid_size_(std::round(std::sqrt(size()))) {}

    PEID next_hop(PEID sender, PEID receiver) const {
        auto proxy = get_proxy(rank(), receiver);
        return proxy;
    }
    bool should_redirect(PEID sender, PEID receiver) const {
        return receiver != rank();
    }

private:
    int rank() const {
        int my_rank;
        MPI_Comm_rank(comm_, &my_rank);
        return my_rank;
    }
    int size() const {
        int my_size;
        MPI_Comm_size(comm_, &my_size);
        return my_size;
    }
    struct GridPosition {
        int row;
        int column;
        bool operator==(const GridPosition& rhs) const {
            return row == rhs.row && column == rhs.column;
        }
    };

    GridPosition rank_to_grid_position(PEID mpi_rank) const {
        return GridPosition{mpi_rank / grid_size_, mpi_rank % grid_size_};
    }

    PEID grid_position_to_rank(GridPosition grid_position) const {
        return grid_position.row * grid_size_ + grid_position.column;
    }
    PEID get_proxy(PEID from, PEID to) const {
        auto from_pos = rank_to_grid_position(from);
        auto to_pos = rank_to_grid_position(to);
        GridPosition proxy = {from_pos.row, to_pos.column};
        if (grid_position_to_rank(proxy) >= size()) {
            proxy = {from_pos.column, to_pos.column};
        }
        if (proxy == from_pos) {
            proxy = to_pos;
        }
        KASSERT(grid_position_to_rank(proxy) < size());
        return grid_position_to_rank(proxy);
    }
    MPI_Comm comm_;
    PEID grid_size_;
};

static_assert(IndirectionScheme<GridIndirectionScheme>);

template <IndirectionScheme Indirector, typename BufferedQueueType>
class IndirectionAdapter : public BufferedQueueType {
public:
    using queue_type = BufferedQueueType;
    IndirectionAdapter(BufferedQueueType queue, Indirector indirector)
        : BufferedQueueType(std::move(queue)), indirection_(std::move(indirector)) {}

    bool post_message(InputMessageRange<typename queue_type::message_type> auto&& message,
                      PEID receiver,
                      PEID envelope_sender,
                      PEID envelope_receiver,
                      int tag,
                      bool direct_send = false) {
        PEID next_hop;
        if (direct_send) {
            next_hop = receiver;
        } else {
            next_hop = indirection_.next_hop(envelope_sender, envelope_receiver);
        }
        return queue_type::post_message(std::move(message), next_hop, envelope_sender, envelope_receiver, tag);
    }

    /// Note: messages have to be passed as rvalues. If you want to send static
    /// data without an additional copy, wrap it in a std::ranges::ref_view.
    bool post_message(InputMessageRange<typename queue_type::message_type> auto&& message,
                      PEID receiver,
                      int tag = 0,
                      bool direct_send = false) {
        return post_message(std::move(message), receiver, this->rank(), receiver, tag, direct_send);
    }

    /// Note: messages have to be passed as rvalues. If you want to send static
    /// data without an additional copy, wrap it in a std::ranges::ref_view.
    bool post_message(typename queue_type::message_type message, PEID receiver, int tag = 0, bool direct_send = false) {
        return post_message(std::ranges::views::single(message), receiver, tag, direct_send);
    }

    /// Note: Message handlers take a MessageEnvelope as single argument. The Envelope
    /// (not necessarily the underlying data) is moved to the handler when
    /// called.
    auto poll(MessageHandler<typename queue_type::message_type> auto&& on_message) -> std::optional<std::pair<bool, bool>> {
        return queue_type::poll(redirection_handler(on_message));
    }

    /// Note: Message handlers take a MessageEnvelope as single argument. The Envelope
    /// (not necessarily the underlying data) is moved to the handler when
    /// called.
    [[nodiscard]] bool terminate(MessageHandler<typename queue_type::message_type> auto&& on_message) {
        return terminate(std::forward<decltype(on_message)>(on_message), []() {});
    }

    /// Note: Message handlers take a MessageEnvelope as single argument. The Envelope
    /// (not necessarily the underlying data) is moved to the handler when
    /// called.
    [[nodiscard]] bool terminate(MessageHandler<typename queue_type::message_type> auto&& on_message,
                                 std::invocable<> auto&& progress_hook) {
        return queue_type::terminate(redirection_handler(on_message), progress_hook);
    }

private:
    auto redirection_handler(MessageHandler<typename queue_type::message_type> auto&& on_message) {
        return [&](Envelope<typename queue_type::message_type> auto envelope) {
            if (indirection_.should_redirect(envelope.sender, envelope.receiver)) {
                post_message(std::move(envelope.message), envelope.receiver, envelope.sender, envelope.receiver,
                             envelope.tag);
            } else {
                KASSERT(envelope.receiver == this->rank());
                on_message(std::move(envelope));
            }
        };
    }
    Indirector indirection_;
};

}  // namespace message_queue
