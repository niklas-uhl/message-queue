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

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <message-queue/buffered_queue.hpp>
#include <message-queue/concepts.hpp>
#include <message-queue/indirection.hpp>
#include <span>

auto main() -> int {
    MPI_Init(nullptr, nullptr);
    auto merger = [](auto& buffer, message_queue::PEID buffer_destination, message_queue::PEID my_rank, auto envelope) {
        if (!buffer.empty()) {
            buffer.push_back(-1);
        }
        buffer.push_back(envelope.sender);
        buffer.push_back(envelope.receiver);
        buffer.push_back(envelope.tag);
        buffer.insert(buffer.end(), envelope.message.begin(), envelope.message.end());
        // for demonstration purposes, when we merge, we also append the current rank to the buffer.
        // this allows us to keep a trace of the path the message took.
        buffer.push_back(my_rank);
    };
    auto splitter = [](auto const& buffer, message_queue::PEID buffer_origin, message_queue::PEID my_rank) {
        return buffer | std::ranges::views::split(-1) | std::ranges::views::transform([](auto&& chunk) {
#ifdef MESSAGE_QUEUE_SPLIT_VIEW_IS_LAZY
                   auto size = std::ranges::distance(chunk);
                   auto sized_chunk = std::span(chunk.begin().base(), size);
#else
                   auto sized_chunk = std::move(chunk);
#endif
                   auto sender = sized_chunk[0];
                   auto receiver = sized_chunk[1];
                   auto tag = sized_chunk[2];
                   auto message = sized_chunk | std::ranges::views::drop(3);
                   return message_queue::MessageEnvelope{std::move(message), sender, receiver, tag};
               });
    };
    {
        auto queue = message_queue::make_buffered_queue<int>(MPI_COMM_WORLD, 8, message_queue::ReceiveMode::poll,
                                                             merger, splitter);
	queue.synchronous_mode();
        auto indirection =
            message_queue::IndirectionAdapter{std::move(queue), message_queue::GridIndirectionScheme{MPI_COMM_WORLD}};
        indirection.post_message(42, 0);
        auto _ = indirection.terminate([](message_queue::Envelope<int> auto envelope) {
            message_queue::atomic_debug(fmt::format("Message {} from {} arrived via {}.",
                                                    envelope.message | std::ranges::views::take(1), envelope.sender,
                                                    envelope.message | std::ranges::views::drop(1)));
        });
    }
    MPI_Finalize();
    return 0;
}
