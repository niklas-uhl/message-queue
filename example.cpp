#include <mpi.h>
#include <CLI/CLI.hpp>
#include <iostream>
#include <random>
#include <string>
#include "message-queue/debug_print.h"
#include "message-queue/queue.h"
#include "message-queue/queue_v2.h"

template <typename Functor>
void with_queue(int version, Functor&& func) {
    switch (version) {
        case 1: {
            auto queue = message_queue::MessageQueue<int>{};
            func(queue);
            break;
        }
        case 2: {
            auto queue = message_queue::MessageQueueV2<int>{};
            func(queue);
            break;
        }
        default:
            throw std::runtime_error("Unsupported queue version");
    }
}

template <typename T>
static constexpr bool is_queue_v2_v = std::is_same_v<std::remove_reference_t<T>, message_queue::MessageQueueV2<int>>;

int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);
    // PMPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    CLI::App app;
    app.option_defaults()->always_capture_default();
    int queue_version = 1;
    app.add_option("--queue_version", queue_version)->expected(1, 2);
    std::size_t number_of_messages = 10;
    app.add_option("--number_of_messages", number_of_messages, "The number of messages to send from each PE");
    std::size_t iterations = 1;
    app.add_option("--iterations", iterations);
    bool use_test_any = false;
    app.add_flag("--use_test_any", use_test_any);
    bool use_custom_implementations = false;
    app.add_flag("--use_custom_implementations", use_custom_implementations);

    CLI11_PARSE(app, argc, argv);

    DEBUG_BARRIER(rank);
    const int message_size = 10;

    auto merger = [](std::vector<int>& buffer, std::vector<int> msg, int) {
        for (auto elem : msg) {
            buffer.emplace_back(elem);
        }
        return msg.size();
    };
    auto splitter = [](std::vector<int>& buffer, auto on_message, message_queue::PEID sender) {
        for (size_t i = 0; i < buffer.size(); i += message_size) {
            on_message(buffer.cbegin() + i, buffer.cbegin() + i + message_size, sender);
        }
    };
    for (size_t i = 0; i < iterations; i++) {
        MPI_Barrier(MPI_COMM_WORLD);
        double start = MPI_Wtime();
        double wait_all_time = 0;
        double test_some_time = 0;
        double test_any_time = 0;
        double test_time = 0;
        int local_max_test_size = 0;
        with_queue(queue_version, [&](auto& queue) {
            if constexpr (is_queue_v2_v<decltype(queue)>) {
                if (use_test_any) {
                    queue.use_test_any();
                }
            }
            std::default_random_engine eng;
            eng.seed(rank);
            std::bernoulli_distribution bernoulli_dist(0.0001);
            std::uniform_int_distribution<size_t> rank_dist(1, size - 1);
            // auto queue = message_queue::make_mesqueue<int>(std::move(merger), std::move(splitter));
            // queue.set_threshold(200);
            message_queue::PEID receiver = rank_dist(eng);
            std::vector<int> message(message_size);
            message[0] = rank;
            message[1] = 0;
            for (size_t i = 0; i < number_of_messages; ++i) {
                message[2] = i;
                queue.post_message(std::vector<int>(message), (rank + rank_dist(eng)) % size);
            }
            auto on_message = [&](auto msg, message_queue::PEID sender) {
                if (bernoulli_dist(eng)) {
                    auto begin = msg.begin();
                    std::stringstream ss;
                    ss << "Message " << *(begin + 2) << " from " << *begin << " arrived after " << *(begin + 1)
                       << " hops.";
                    message_queue::atomic_debug(ss.str());
                } else {
                    KASSERT(msg.size() > 1);
                    msg[1]++;
                    queue.post_message(std::move(msg), (rank + rank_dist(eng)) % size);
                }
            };
            queue.poll(on_message);
            queue.terminate(on_message);
            if constexpr (is_queue_v2_v<decltype(queue)>) {
                using namespace std::chrono;
                wait_all_time = duration_cast<duration<double>>(queue.wait_all_time()).count();
                test_some_time = duration_cast<duration<double>>(queue.test_some_time()).count();
                test_any_time = duration_cast<duration<double>>(queue.test_any_time()).count();
                test_time = duration_cast<duration<double>>(queue.test_time()).count();
                local_max_test_size = queue.max_test_size();
            }
        });
        MPI_Barrier(MPI_COMM_WORLD);
        double end = MPI_Wtime();
        double local_times[4] = {wait_all_time, test_some_time, test_any_time, test_time};
        double max_times[4];
        MPI_Reduce(&local_times, &max_times, 4, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        int global_max_test_size;
        MPI_Reduce(&local_max_test_size, &global_max_test_size, 1, MPI_INT, MPI_MAX, 0, MPI_COMM_WORLD);
        // print CLI options
        std::unordered_map<std::string, std::string> stats;
        for (const auto& option : app.get_options()) {
            if (option->get_single_name() == "help") {
                continue;
            }
            stats[option->get_single_name()] = option->as<std::string>();
            if (stats[option->get_single_name()].empty()) {
                stats[option->get_single_name()] = "false";
            }
        }
        stats["ranks"] = fmt::format("{}", size);
        stats["time"] = fmt::format("{}", end - start);
        stats["wait_all_time"] = fmt::format("{}", max_times[0]);
        stats["test_some_time"] = fmt::format("{}", max_times[1]);
        stats["test_any_time"] = fmt::format("{}", max_times[2]);
        stats["test_time"] = fmt::format("{}", max_times[3]);
        stats["iteration"] = fmt::format("{}", i);
        stats["max_test_size"] = fmt::format("{}", global_max_test_size);

        if (rank == 0) {
            std::cout << "RESULT";
            for (const auto& [key, value] : stats) {
                std::cout << " " << key << "=" << value;
            }
            std::cout << "\n";
        }
    }
    // message_queue::atomic_debug(queue.overflows());
    return MPI_Finalize();
}
