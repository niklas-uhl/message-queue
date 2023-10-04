#include <fmt/format.h>
#include <fmt/ranges.h>
#include <message-queue/buffered_queue_v2.hpp>
#include <message-queue/concepts.hpp>
#include <message-queue/indirection.hpp>

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
                   auto sender = chunk[0];
                   auto receiver = chunk[1];
                   auto tag = chunk[2];
                   auto message = chunk | std::ranges::views::drop(3);
                   return message_queue::MessageEnvelope{
                       .message = std::move(message), .sender = sender, .receiver = receiver, .tag = tag};
               });
    };
    auto queue = message_queue::make_buffered_queue<int>(MPI_COMM_WORLD, merger, splitter);
    auto indirection =
        message_queue::IndirectionAdapter<message_queue::GridIndirectionScheme, decltype(queue)>{std::move(queue)};
    indirection.post_message(42, 0);
    indirection.terminate([](message_queue::Envelope<int> auto envelope) {
        message_queue::atomic_debug(fmt::format("Message {} from {} arrived via {}.",
                                                envelope.message | std::ranges::views::take(1), envelope.sender,
                                                envelope.message | std::ranges::views::drop(1)));
    });
    MPI_Finalize();
    return 0;
}
