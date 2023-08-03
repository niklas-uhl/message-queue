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

    CLI11_PARSE(app, argc, argv);

    DEBUG_BARRIER(rank);
    const int message_size = 10;

    std::default_random_engine eng;
    eng.seed(rank);
    std::bernoulli_distribution bernoulli_dist(0.0001);
    std::uniform_int_distribution<size_t> rank_dist(1, size - 1);

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
    with_queue(queue_version, [&] (auto& queue){
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
                ss << "Message " << *(begin + 2) << " from " << *begin << " arrived after " << *(begin + 1) << " hops.";
                message_queue::atomic_debug(ss.str());
            } else {
                KASSERT(msg.size() > 1);
                msg[1]++;
                queue.post_message(std::move(msg), (rank + rank_dist(eng)) % size);
            }
        };
        queue.poll(on_message);
        queue.terminate(on_message);
    });
    // message_queue::atomic_debug(queue.overflows());
    return MPI_Finalize();
}
