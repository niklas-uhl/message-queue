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

#include <mpi.h>
#include <CLI/CLI.hpp>
#include <format>
#include <iostream>
#include <print>
#include <random>
#include <string>
#include "../tests/testing_helpers.hpp"
#include "message-queue/queue.hpp"

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
    bool use_custom_implementations = false;
    app.add_flag("--use_custom_implementations", use_custom_implementations);

    CLI11_PARSE(app, argc, argv);

    const int message_size = 10;

    for (size_t i = 0; i < iterations; i++) {
        MPI_Barrier(MPI_COMM_WORLD);
        double start = MPI_Wtime();
        int local_max_test_size = 0;
        size_t local_max_active_requests = 0;

        using MessageContainer = std::vector<int>;

        auto queue = message_queue::MessageQueue<int, MessageContainer>{MPI_COMM_WORLD, 8, 100,
                                                                        message_queue::ReceiveMode::poll};
        std::default_random_engine eng;
        eng.seed(rank);
        std::bernoulli_distribution bernoulli_dist(0.1);
        std::uniform_int_distribution<size_t> rank_dist(1, size - 1);
        message_queue::PEID receiver = rank_dist(eng);
        MessageContainer message(message_size);
        message[0] = rank;
        message[1] = 0;
        for (size_t i = 0; i < number_of_messages; ++i) {
            message[2] = i;
            queue.post_message(MessageContainer(message), (rank + rank_dist(eng)) % size);
        }
        auto on_message = [&](message_queue::Envelope<int> auto envelope) {
            if (bernoulli_dist(eng)) {
                auto begin = envelope.message.begin();
                std::print("Message {} from {} arrived after {} hops.\n", *(begin + 2), *begin, *(begin + 1));
            } else {
                KASSERT(envelope.message.size() > 1);
                std::vector msg(envelope.message.begin(), envelope.message.end());
                msg[1]++;
                queue.post_message(std::move(msg),
                                   (rank + rank_dist(eng)) % size);
            }
        };
        queue.poll(on_message);
        queue.terminate(on_message);
        using namespace std::chrono;
        MPI_Barrier(MPI_COMM_WORLD);
        double end = MPI_Wtime();
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
        stats["ranks"] = std::format("{}", size);
        stats["time"] = std::format("{}", end - start);
        stats["iteration"] = std::format("{}", i);

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
