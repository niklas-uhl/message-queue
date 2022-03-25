#include <mpi.h>
#include <iostream>
#include <random>
#include <string>
#include "message-queue/buffered_queue.h"
#include "message-queue/debug_print.h"

using std::move;

int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
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
    auto splitter = [](std::vector<int>& buffer, auto on_message, PEID sender) {
        for (size_t i = 0; i < buffer.size(); i += message_size) {
            on_message(buffer.begin() + i, buffer.begin() + i + message_size, sender);
        }
    };
    auto queue = message_queue::make_buffered_queue<int>(std::move(merger), std::move(splitter));
    queue.set_threshold(200);
    PEID receiver = rank_dist(eng);
    std::vector<int> message(message_size);
    message[0] = rank;
    message[1] = 0;
    for (size_t i = 0; i < 10; ++i) {
        message[2] = i;
        queue.post_message(std::vector(message), (rank + rank_dist(eng)) % size);
    }
    auto on_message = [&](auto begin, auto end, PEID sender) {
        if (bernoulli_dist(eng)) {
            std::stringstream ss;
            ss << "Message " << *(begin + 2) << " from " << *begin << " arrived after " << *(begin + 1) << " hops.";
            atomic_debug(ss.str());
        } else {
            auto msg = std::vector(begin, end);
            msg[1]++;
            queue.post_message(std::move(msg), (rank + rank_dist(eng)) % size);
        }
    };
    queue.poll(on_message);
    queue.terminate(on_message);
    atomic_debug(queue.overflows());
    return MPI_Finalize();
}
