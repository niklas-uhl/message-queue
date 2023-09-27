#include <fmt/format.h>
#include <fmt/ranges.h>
#include <message-queue/buffered_queue_v2.h>
#include <CLI/CLI.hpp>
#include <random>

auto main(int argc, char* argv[]) -> int {
    MPI_Init(nullptr, nullptr);
    CLI::App app;
    app.option_defaults()->always_capture_default();

    size_t global_threshold = std::numeric_limits<size_t>::max();
    app.add_option("--global_threshold", global_threshold, "The global threshold for the queue");

    size_t local_threshold = std::numeric_limits<size_t>::max();
    app.add_option("--local_threshold", local_threshold, "The local threshold for the queue");

    size_t number_of_messages = 5;
    app.add_option("--number_of_messages", number_of_messages, "The number of messages to send from each PE");

    CLI11_PARSE(app, argc, argv);

    auto printing_cleaner = [](auto& buf, message_queue::PEID receiver) {
        message_queue::atomic_debug(fmt::format("Preparing buffer {} to {}.", buf, receiver));
    };
    auto queue = message_queue::make_buffered_queue_with_cleaner<int>(MPI_COMM_WORLD, printing_cleaner);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    DEBUG_BARRIER(rank);
    std::mt19937 gen;
    std::uniform_int_distribution<int> dist(0, size - 1);
    if (global_threshold != std::numeric_limits<size_t>::max()) {
        queue.global_threshold(global_threshold);
    }
    if (local_threshold != std::numeric_limits<size_t>::max()) {
        queue.local_threshold(local_threshold);
    }
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
