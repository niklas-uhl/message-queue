#include <fmt/format.h>
#include <fmt/ranges.h>
#include <CLI/CLI.hpp>
#include <message-queue/buffered_queue_v2.hpp>
#include <random>
#include <range/v3/all.hpp>

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

    auto merge = [](auto& buf, message_queue::PEID buffer_destination, message_queue::PEID my_rank,
                    message_queue::Envelope auto msg) {
        if (!buf.empty()) {
            buf.emplace_back(-1);
        }
        buf.emplace_back(msg.tag);
        for (auto elem : msg.message) {
            buf.emplace_back(elem.first);
            buf.emplace_back(elem.second);
        }
    };
    auto split = [](message_queue::MPIBuffer<int> auto const& buf, message_queue::PEID buffer_origin,
                    message_queue::PEID my_rank) {
        return buf | std::ranges::views::split(-1) |
               std::ranges::views::transform([&, buffer_origin = buffer_origin, my_rank = my_rank](auto const& chunk) {
                   int tag = chunk[0];
                   auto message = chunk | ranges::views::drop(1) | ranges::views::chunk(2) |
                                  std::ranges::views::transform(
                                      [&](auto const& chunk) { return std::make_pair(chunk[0], chunk[1]); });
                   return message_queue::MessageEnvelope{
                       .message = std::move(message), .sender = buffer_origin, .receiver = my_rank, .tag = tag};
               });
    };
    auto printing_cleaner = [](auto& buf, message_queue::PEID receiver) {
        message_queue::atomic_debug(fmt::format("Preparing buffer {} to {}.", buf, receiver));
    };
    auto queue =
        message_queue::make_buffered_queue<std::pair<int, int>, int>(MPI_COMM_WORLD, merge, split, printing_cleaner);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    std::mt19937 gen;
    std::uniform_int_distribution<int> dist(0, size - 1);
    std::uniform_int_distribution<int> message_size_dist(1, 10);
    if (global_threshold != std::numeric_limits<size_t>::max()) {
        queue.global_threshold(global_threshold);
    }
    if (local_threshold != std::numeric_limits<size_t>::max()) {
        queue.local_threshold(local_threshold);
    }
    for (auto i = 0; i < number_of_messages; ++i) {
        int destination = dist(gen);
        int message_size = message_size_dist(gen);
        auto message = ranges::views::ints(1, message_size) |
                       std::views::transform([](int i) { return std::pair(i, 42); }) | ranges::to<std::vector>();
        queue.post_message(std::move(message), destination, rank);
    }
    queue.post_message(std::pair{0, 0}, 0);

    size_t zero_message_counter = 0;
    auto handler = [&](message_queue::Envelope<std::pair<int, int>> auto const& envelope) {
        message_queue::atomic_debug(
            fmt::format("Message {} (tag={}) from {} arrived.", envelope.message, envelope.tag, envelope.sender));

        if (envelope.message.size() == 1 && envelope.message[0] == std::pair{0, 0}) {
            KASSERT(rank == 0 && envelope.tag == 0);
            zero_message_counter++;
        } else {
            for (auto [i, val] : envelope.message | ranges::views::enumerate) {
                KASSERT(i + 1 == val.first);
                KASSERT(42 == val.second);
            }
        }
    };
    queue.terminate(handler);
    if (rank == 0) {
        KASSERT(zero_message_counter == size);
    } else {
        KASSERT(zero_message_counter == 0);
    }
    MPI_Finalize();
    return 0;
}
