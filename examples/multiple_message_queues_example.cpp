#include <message-queue/buffered_queue_v2.h>
#include <random>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <CLI/CLI.hpp>

auto main(int argc, char* argv[]) -> int {
    MPI_Init(nullptr, nullptr);
    CLI::App app;
    app.option_defaults()->always_capture_default();

    size_t number_of_messages = 5;
    app.add_option("--number_of_messages", number_of_messages, "The number of messages to send from each PE");

    CLI11_PARSE(app, argc, argv);

    auto printing_cleaner = [](auto& buf, message_queue::PEID receiver) {
        message_queue::atomic_debug(fmt::format("Preparing buffer {} to {}.", buf, receiver));
    };
    auto queue1 = message_queue::make_buffered_queue_with_cleaner<int>(MPI_COMM_WORLD, printing_cleaner);
    MPI_Comm other_comm;
    MPI_Comm_dup(MPI_COMM_WORLD, &other_comm);
    auto queue2 = message_queue::make_buffered_queue_with_cleaner<int>(other_comm, printing_cleaner);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    DEBUG_BARRIER(rank);
    std::mt19937 gen;
    std::uniform_int_distribution<int> dist(0, size - 1);
    for (auto i = 0; i < number_of_messages; ++i) {
        int val = dist(gen);
        queue1.post_message(1, val);
        queue2.post_message(2, val);
    }
    queue2.terminate([&](auto msg, auto sender, auto tag) {
        message_queue::atomic_debug(fmt::format("Message {} from {} arrived.", msg, sender));
    });
    queue1.terminate([&](auto msg, auto sender, auto tag) {
        message_queue::atomic_debug(fmt::format("Message {} from {} arrived.", msg, sender));
    });
    MPI_Finalize();
    return 0;
}
