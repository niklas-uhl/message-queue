#pragma once

#include <mpi.h>
#include <cmath>
#include <kassert/kassert.hpp>
#include "definitions.h"

namespace message_queue {
class GridIndirectionScheme {
public:
    GridIndirectionScheme(MPI_Comm comm) : comm_(comm), grid_size_(std::round(std::sqrt(size()))) {}
    PEID next_hop(PEID sender, PEID receiver) const {
        return get_proxy(rank(), receiver);
    }
    bool should_redirect(PEID sender, PEID receiver) const {
        return receiver != rank();
    }

private:
    int rank() const {
        int my_rank;
        MPI_Comm_rank(comm_, &my_rank);
        return my_rank;
    }
    int size() const {
        int my_size;
        MPI_Comm_rank(comm_, &my_size);
        return my_size;
    }
    struct GridPosition {
        int row;
        int column;
        bool operator==(const GridPosition& rhs) {
            return row == rhs.row && column == rhs.column;
        }
    };

    GridPosition rank_to_grid_position(PEID mpi_rank) const {
        return GridPosition{rank() / grid_size_, rank() % grid_size_};
    }

    PEID grid_position_to_rank(GridPosition grid_position) const {
        return grid_position.row * grid_size_ + grid_position.column;
    }
    PEID get_proxy(PEID from, PEID to) const {
        auto from_pos = rank_to_grid_position(from);
        auto to_pos = rank_to_grid_position(to);
        GridPosition proxy = {from_pos.row, to_pos.column};
        if (grid_position_to_rank(proxy) >= size()) {
            proxy = {from_pos.column, to_pos.column};
        }
        if (proxy == from_pos) {
            proxy = to_pos;
        }
        KASSERT(grid_position_to_rank(proxy) < size());
        return grid_position_to_rank(proxy);
    }
    MPI_Comm comm_;
    PEID grid_size_;
}

}  // namespace message_queue
