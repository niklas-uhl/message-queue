#include <mpi.h>
#include <iostream>
#include <random>
#include <string>
#include "message-queue/debug_print.h"
#include "message-queue/queue.h"

int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    std::default_random_engine eng;
    std::bernoulli_distribution bernoulli_dist(0.001);
    std::uniform_int_distribution<size_t> rank_dist(1, size - 1);

    MessageQueue<int> queue;
    PEID receiver = rank_dist(eng);
    std::vector<int> message(10000);
    message[0] = rank;
    message[1] = 0;
    queue.post_message(std::move(message), receiver);
    auto on_message = [&](PEID sender, auto message) {
        if (bernoulli_dist(eng)) {
            std::stringstream ss;
            ss << "Message from " << message[0] << " arrived after " << message[1] << " hops.";
            atomic_debug(ss.str());
        } else {
            message[1]++;
            queue.post_message(std::move(message), (rank + rank_dist(eng)) % size);
        }
    };
    queue.poll(on_message);
    queue.terminate(on_message);
    return MPI_Finalize();
}
