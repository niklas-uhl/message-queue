#include <graph-io/graph_io.h>
#include <mpi.h>
#include <algorithm>
#include <cstddef>
#include <deque>
#include <ios>
#include <limits>
#include <numeric>
#include <queue>
#include <string>
#include <thread>
#include <vector>
#include "backward.hpp"
#include "graph-io/definitions.h"
#include "graph-io/distributed_graph_io.h"
#include "graph-io/graph_definitions.h"
#include "message-queue/buffered_queue.h"
#include "message-queue/debug_print.h"
#include "message-queue/queue.h"

struct GraphWrapper {
    GraphWrapper(graphio::LocalGraphView&& G)
        : graph(G), idx(G.build_indexer()), loc(G.build_locator(MPI_COMM_WORLD)), labels(G.local_node_count()) {
        for (size_t i = 0; i < labels.size(); ++i) {
            labels[i] = std::numeric_limits<size_t>::max();
        }
    }
    graphio::LocalGraphView graph;
    graphio::LocalGraphView::Indexer idx;
    graphio::LocalGraphView::NodeLocator loc;
    std::vector<size_t> labels;
};

template <class T>
struct Frontier {
    std::deque<T> frontier;
    std::deque<T> new_frontier;

    void flip() {
        frontier.swap(new_frontier);
        new_frontier.clear();
    }
    void push(T node) {
        new_frontier.push_back(node);
    }

    auto begin() {
        return frontier.begin();
    }

    auto end() {
        return frontier.end();
    }

    bool empty() {
        return frontier.empty();
    }

    bool globally_empty() {
        bool locally_empty = empty();
        bool globally_empty;
        MPI_Allreduce(&locally_empty, &globally_empty, 1, MPI_CXX_BOOL, MPI_LAND, MPI_COMM_WORLD);
        return globally_empty;
    }
};

std::vector<size_t> run_sequential(const std::string& input, graphio::InputFormat format) {
    PEID rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if (rank != 0) {
        return {};
    }
    auto G = graphio::read_graph(input, format);
    std::vector<size_t> labels(G.first_out_.size() - 1, std::numeric_limits<size_t>::max());
    Frontier<size_t> frontier;
    frontier.push(0);
    labels[0] = 0;
    size_t level = 1;
    frontier.flip();
    while (!frontier.empty()) {
        for (auto v : frontier) {
            for (auto nb_it = G.head_.begin() + G.first_out_[v]; nb_it != G.head_.begin() + G.first_out_[v + 1];
                 ++nb_it) {
                auto u = *nb_it;
                auto& label = labels[u];
                if (label == std::numeric_limits<size_t>::max()) {
                    label = level;
                    frontier.push(u);
                }
            }
        }
        frontier.flip();
        level++;
    }
    return labels;
}

double run_distributed(const std::string& input, graphio::InputFormat format) {
    PEID rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    // MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN);

    // DEBUG_BARRIER(rank);
    auto G_in = graphio::read_local_graph(input, format, rank, size);
    GraphWrapper G(std::move(G_in));

    MPI_Barrier(MPI_COMM_WORLD);
    double start = MPI_Wtime();
    graphio::NodeId source = 0;
    // Frontier<size_t> frontier;
    auto merge = [](std::vector<graphio::NodeId>& buffer, std::vector<graphio::NodeId> msg, int tag) {
        for (auto elem : msg) {
            buffer.emplace_back(elem);
        }
    };

    auto split = [](std::vector<graphio::NodeId>& buffer, auto on_message, PEID sender) {
        for (size_t i = 0; i < buffer.size(); ++i) {
            on_message(buffer.begin() + i, buffer.begin() + i + 1, sender);
        }
    };
    auto queue = message_queue::make_buffered_queue<graphio::NodeId>(merge, split);
    std::deque<graphio::NodeId> frontier;
    if (G.loc.is_local(source)) {
        frontier.push_back(source);
        G.labels[G.idx.get_index(source)] = 0;
    }
    size_t level = 1;
    auto globally_empty = [&]() {
        bool locally_empty = frontier.empty();
        bool globally_empty;
        MPI_Allreduce(&locally_empty, &globally_empty, 1, MPI_CXX_BOOL, MPI_LAND, MPI_COMM_WORLD);
        return globally_empty;
    };
    while (!globally_empty()) {
        while (!frontier.empty()) {
            graphio::NodeId v = frontier.front();
            frontier.pop_front();
            G.idx.for_each_neighbor(v, [&](graphio::NodeId u) { queue.post_message({u}, G.loc.rank(u)); });
        }
        queue.terminate([&](auto begin, auto end, PEID sender [[maybe_unused]]) {
            graphio::NodeId v = *begin;
            size_t& v_label = G.labels[G.idx.get_index(v)];
            if (v_label == std::numeric_limits<size_t>::max()) {
                v_label = level;
                frontier.push_back(v);
            }
        });
        level++;
    }
    MPI_Barrier(MPI_COMM_WORLD);
    double end = MPI_Wtime();
    // atomic_debug(level);
    //  std::vector<int> counts(size);
    //  std::vector<int> displs(size);
    //  if (rank == 0) {
    //      counts.resize(size);
    //      displs.resize(size);
    //  }
    //  int local_count = G.graph.local_node_count();
    //  MPI_Gather(&local_count, 1, MPI_INT, counts.data(), 1, MPI_INT, 0, MPI_COMM_WORLD);
    //  std::vector<size_t> all_labels;
    //  if (rank == 0) {
    //      std::exclusive_scan(counts.begin(), counts.end(), displs.begin(), 0);
    //      all_labels.resize(displs.back() + counts.back());
    //  }
    //  MPI_Gatherv(G.labels.data(), G.labels.size(), MPI_UINT64_T, all_labels.data(), counts.data(), displs.data(),
    //              MPI_UINT64_T, 0, MPI_COMM_WORLD);
    return end - start;
}

double run_distributed_async(const std::string& input, graphio::InputFormat format) {
    PEID rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    // MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN);

    // DEBUG_BARRIER(rank);
    auto G_in = graphio::read_local_graph(input, format, rank, size);
    GraphWrapper G(std::move(G_in));

    MPI_Barrier(MPI_COMM_WORLD);
    double start = MPI_Wtime();
    graphio::NodeId source = 0;
    // Frontier<size_t> frontier;
    auto merge = [](std::vector<graphio::NodeId>& buffer, std::vector<graphio::NodeId> msg, int tag) {
        for (auto elem : msg) {
            buffer.emplace_back(elem);
        }
        // atomic_debug(buffer);
    };

    auto split = [](std::vector<graphio::NodeId>& buffer, auto on_message, PEID sender) {
        for (size_t i = 0; i < buffer.size(); i += 2) {
            on_message(buffer.cbegin() + i, buffer.cbegin() + i + 2, sender);
        }
    };
    // auto queue = message_queue::MessageQueue<graphio::NodeId>();
    auto queue = message_queue::make_buffered_queue<graphio::NodeId>(merge, split);
    std::deque<std::pair<graphio::NodeId, size_t>> local_queue;
    auto on_message = [&](/*/std::vector<graphio::NodeId> message*/ auto begin, auto end,
                          PEID sender [[maybe_unused]]) {
        queue.reactivate();
        graphio::NodeId v = *begin;
        // graphio::NodeId v = message[0];
        std::stringstream out;
        out << "Receive " << v << " from " << sender;
        // atomic_debug(out.str());
        assert(G.loc.is_local(v));
        size_t label = *(begin + 1);
        // size_t label = message[1];
        local_queue.emplace_back(v, label);
    };
    // queue.set_threshold(100);
    queue.set_threshold(G.graph.local_node_count());
    if (G.loc.is_local(source)) {
        auto msg = {source, 0ul};
        on_message(msg.begin(), msg.end(), rank);
    } else {
        while (!queue.poll(on_message)) {
        };
    }
    size_t discovered_nodes = 0;
    do {
        do {
            while (!local_queue.empty()) {
                graphio::NodeId v;
                size_t label;
                std::tie(v, label) = local_queue.front();
                local_queue.pop_front();
                std::stringstream out;
                out << "Pop " << v;
                // atomic_debug(out.str());
                if (!G.loc.is_local(v)) {
                    std::stringstream out;
                    out << "Sending " << v << " to " << G.loc.rank(v);
                    // atomic_debug(out.str());
                    queue.post_message({v, label}, G.loc.rank(v));
                    queue.poll(on_message);
                    continue;
                }
                size_t& v_label = G.labels[G.idx.get_index(v)];
                if (label < v_label) {
                    std::stringstream out;
                    discovered_nodes++;
                    out << "Discovered " << v;
                    // atomic_debug(out.str());
                    v_label = label;
                    G.idx.for_each_neighbor(v, [&](graphio::NodeId u) {
                        if (G.loc.is_local(u)) {
                            local_queue.emplace_back(u, label + 1);
                        } else {
                            queue.post_message({u, label + 1}, G.loc.rank(u));
                        }
                    });
                } else {
                    std::stringstream out;
                    out << "Discarding " << v;
                    // atomic_debug(out.str());
                }
                queue.poll(on_message);
            }
            //size_t new_threshold = G.graph.local_node_count() - discovered_nodes;
            // atomic_debug(new_threshold);
            //queue.set_threshold(new_threshold);
            queue.flush_all();
        } while (queue.poll(on_message));
        queue.flush_all();
    } while (!queue.try_terminate(on_message));
    MPI_Barrier(MPI_COMM_WORLD);
    double end = MPI_Wtime();

    // queue.terminate(on_message);
    atomic_debug(queue.stats().sent_messages);

    // std::vector<int> counts(size);
    // std::vector<int> displs(size);
    // if (rank == 0) {
    //     counts.resize(size);
    //     displs.resize(size);
    // }
    // int local_count = G.graph.local_node_count();
    // MPI_Gather(&local_count, 1, MPI_INT, counts.data(), 1, MPI_INT, 0, MPI_COMM_WORLD);
    // std::vector<size_t> all_labels;
    // if (rank == 0) {
    //     std::exclusive_scan(counts.begin(), counts.end(), displs.begin(), 0);
    //     all_labels.resize(displs.back() + counts.back());
    // }
    // MPI_Gatherv(G.labels.data(), G.labels.size(), MPI_UINT64_T, all_labels.data(), counts.data(), displs.data(),
    //             MPI_UINT64_T, 0, MPI_COMM_WORLD);
    // return all_labels;
    return end - start;
}

int main(int argc, char* argv[]) {
    (void)argc;
    (void)argv;
    MPI_Init(&argc, &argv);
    backward::SignalHandling sh;
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    // MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN);
    auto time = run_distributed(argv[1], graphio::input_types.at(argv[2]));
    if (rank == 0) {
        std::cout << "Distributed algorithm finished in " << time << "s.\n";
    }

    auto time_async = run_distributed_async(argv[1], graphio::input_types.at(argv[2]));
    if (rank == 0) {
        std::cout << "Async algorithm finished in " << time_async << "s.\n";
    }
    // for (size_t i = 0; i < labels_async.size(); i++) {
    //     std::cout << i << "\t" << labels_async[i] << "\n";
    // }

    // auto labels_seq = run_sequential(argv[1], graphio::input_types.at(argv[2]));
    // if (rank == 0) {
    //     std::cout << "Sequential algorithm finished.\n";
    // }

    // if (rank == 0) {
    //     bool correct = labels == labels_async;
    //     std::cout << "Distributed algorithm works: \t" << std::boolalpha << correct << std::endl;
    // }

    return MPI_Finalize();
}
