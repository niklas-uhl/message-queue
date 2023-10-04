#include <mpi.h>
#include <stdio.h>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

void busywait(double time) {
    double start = MPI_Wtime();
    double end = MPI_Wtime();
    while (end - start <= time) {
        end = MPI_Wtime();
    }
}

int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    MPI_Datatype bigtype;
    MPI_Type_contiguous(100'000, MPI_INT, &bigtype);
    MPI_Type_commit(&bigtype);
    size_t elements = 1000;
    std::vector<int> message(elements * 100'000);
    std::cout << message[0];
    // std::array<int, 10000000> message;
    for (size_t i = 0; i <= 90; i += 5) {
        if (rank == 0) {
            double start, end;
            start = MPI_Wtime();
            MPI_Request req;
            int err = MPI_Issend(message.data(), message.size() / 100'000, bigtype, 1, 0, MPI_COMM_WORLD, &req);
            busywait(i);
            MPI_Wait(&req, MPI_STATUS_IGNORE);
            end = MPI_Wtime();
            std::cout << i << "\t" << end - start << "\n";
        } else if (rank == 1) {
            MPI_Recv(message.data(), message.size() / 100'000, bigtype, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD,
                     MPI_STATUS_IGNORE);
        }
        MPI_Barrier(MPI_COMM_WORLD);
    }
    return MPI_Finalize();
}
