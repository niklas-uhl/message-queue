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

#include <CLI/CLI.hpp>
#include <message-queue/buffered_queue.hpp>
#include <message-queue/queue_builder.hpp>
#include <print>
#include <random>

std::map<std::string, message_queue::FlushStrategy> flush_strategy_map{
    {"global", message_queue::FlushStrategy::global},
    {"local", message_queue::FlushStrategy::local},
    {"random", message_queue::FlushStrategy::random},
    {"largest", message_queue::FlushStrategy::largest},
};

static int pending_receives = 0;
static int max_pending_receives = 0;

int MPI_Imrecv(void* buf, int count, MPI_Datatype type, MPI_Message* message, MPI_Request* request) {
    pending_receives++;
    max_pending_receives = std::max(max_pending_receives, pending_receives);
    return PMPI_Imrecv(buf, count, type, message, request);
}

int MPI_Waitall(int count, MPI_Request* array_of_requests, MPI_Status* array_of_statuses) {
    if (pending_receives != count) {
        std::cout << "pending_receives = " << pending_receives << ", count = " << count << "\n";
    }
    pending_receives -= count;
    return PMPI_Waitall(count, array_of_requests, array_of_statuses);
}

int MPI_Finalize() {
    std::cout << "max_pending_receives = " << max_pending_receives << "\n";
    return PMPI_Finalize();
}

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

    message_queue::FlushStrategy flush_strategy = message_queue::FlushStrategy::global;
    app.add_option("--flush_strategy", flush_strategy, "The flush strategy to use")
        ->transform(CLI::CheckedTransformer(flush_strategy_map, CLI::ignore_case));

    CLI11_PARSE(app, argc, argv);

    auto printing_cleaner = [](auto& buf, message_queue::PEID receiver) {
        std::print("Preparing buffer {} to {}.\n", buf, receiver);
    };
    {
        auto queue =
            message_queue::BufferedMessageQueueBuilder<int>().with_buffer_cleaner(std::move(printing_cleaner)).build();

        queue.synchronous_mode();
        int rank, size;
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        MPI_Comm_size(MPI_COMM_WORLD, &size);
        std::mt19937 gen;
        std::uniform_int_distribution<int> dist(0, size - 1);
        if (global_threshold != std::numeric_limits<size_t>::max()) {
            queue.global_threshold(global_threshold);
        }
        if (local_threshold != std::numeric_limits<size_t>::max()) {
            queue.local_threshold(local_threshold);
        }
        queue.flush_strategy(flush_strategy);
        for (auto i = 0; i < number_of_messages; ++i) {
            int val = dist(gen);
            queue.post_message(val, val);
        }
        auto _ = queue.terminate([&](message_queue::Envelope<int> auto envelope) {
            std::println("Message {} from {} arrived.", envelope.message, envelope.sender);
        });
    }
    MPI_Finalize();
    return 0;
}
