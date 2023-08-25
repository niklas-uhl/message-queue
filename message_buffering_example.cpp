#include <message-queue/buffered_queue_v2.h>
#include <random>
#include <fmt/format.h>

auto main() -> int {
    MPI_Init(nullptr, nullptr);
    auto queue = message_queue::BufferedMessageQueueV2<int>{};
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    DEBUG_BARRIER(rank);
    size_t number_of_messages = 5;
    std::mt19937 gen;
    std::uniform_int_distribution<int> dist(0, size - 1);
    queue.global_threshold(2);
    for (auto i = 0; i < number_of_messages; ++i) {
        int val = dist(gen);
        queue.post_message(val, val);
    }
    queue.terminate([&](auto msg, auto sender, auto tag) {
        message_queue::atomic_debug(fmt::format("Message {} from {} arrived.", msg, sender));
    });
    MPI_Finalize();
    return 0;
}
