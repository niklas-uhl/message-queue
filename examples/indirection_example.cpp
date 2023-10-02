#include <fmt/format.h>
#include <fmt/ranges.h>
#include <message-queue/buffered_queue_v2.hpp>
#include <message-queue/concepts.hpp>
#include <message-queue/indirection.hpp>

auto main() -> int {
    MPI_Init(nullptr, nullptr);
    auto queue = message_queue::make_buffered_queue<int>();
    auto indirection =
        message_queue::IndirectionAdapter<message_queue::GridIndirectionScheme, decltype(queue)>{std::move(queue)};
    indirection.post_message(42, 0);
    indirection.terminate([](message_queue::Envelope<int> auto envelope) {
        message_queue::atomic_debug(fmt::format("Message {} from {} arrived.", envelope.message, envelope.sender));
    });
    MPI_Finalize();
    return 0;
}
