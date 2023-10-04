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
#include <CLI/CLI.hpp>
#include <message-queue/buffered_queue.hpp>
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
    auto queue = message_queue::make_buffered_queue<int>(MPI_COMM_WORLD, printing_cleaner);
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
    queue.terminate([&](message_queue::Envelope<int> auto envelope) {
        message_queue::atomic_debug(fmt::format("Message {} from {} arrived.", envelope.message, envelope.sender));
    });
    MPI_Finalize();
    return 0;
}
