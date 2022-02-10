#include <graph-io/graph_io.h>
#include <mpi.h>
#include <algorithm>
#include <cstddef>
#include <limits>
#include <queue>
#include <vector>
#include "graph-io/definitions.h"
#include "graph-io/distributed_graph_io.h"
#include "graph-io/graph_definitions.h"
#include "message-queue/debug_print.h"
#include "message-queue/queue.h"

struct GraphWrapper {
    GraphWrapper(graphio::LocalGraphView&& G)
        : graph(G), idx(G.build_indexer()), loc(G.build_locator(MPI_COMM_WORLD)), labels(G.local_node_count()) {
        for (size_t i = 0; i < labels.size(); ++i) {
            labels[i] = G.node_info[i].global_id;
        }
    }
    graphio::LocalGraphView graph;
    graphio::LocalGraphView::Indexer idx;
    graphio::LocalGraphView::NodeLocator loc;
    std::vector<size_t> labels;
};

using UpdateQueue = std::queue<std::pair<graphio::NodeId, size_t>>;

void notify_neighbors(graphio::NodeId node, GraphWrapper& G, MessageQueue<size_t>& queue, UpdateQueue& updates) {
    G.idx.for_each_neighbor(node, [&](graphio::NodeId neighbor) {
        if (G.loc.is_local(neighbor)) {
            std::cout << "Post update\n";
            updates.emplace(neighbor, G.labels[G.idx.get_index(node)]);
        } else {
            // std::cout << "Send update\n";
            // queue.post_message({G.labels[G.idx.get_index(node)], neighbor}, G.loc.rank(neighbor));
        }
    });
}

void update_labels(GraphWrapper& G, MessageQueue<size_t>& queue, UpdateQueue& updates) {
    while (!updates.empty()) {
        auto [label, node] = updates.front();
        updates.pop();
        if (label < G.labels[G.idx.get_index(node)]) {
            G.labels[G.idx.get_index(node)] = label;
            notify_neighbors(node, G, queue, updates);
            std::cout << "Updating label of " << node << " to " << label << "\n";
        }
    }
}

int main(int argc, char* argv[]) {
    (void)argc;
    (void)argv;
    MPI_Init(&argc, &argv);

    PEID rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    //MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN);

    DEBUG_BARRIER(rank);
    auto format = graphio::input_types.at(argv[2]);
    auto G_in = graphio::read_local_graph(argv[1], format, rank, size);
    GraphWrapper G(std::move(G_in));
    UpdateQueue update_queue;

    //std::cout << "IO done\n";
    MessageQueue<size_t> queue;
    auto update_labels_locally = [&]() {
        size_t nodes_changed = 0;
        size_t rounds = 0;
        do {
            atomic_debug("New round");
            nodes_changed = 0;
            for (auto node_info : G.graph.node_info) {
                graphio::NodeId u = node_info.global_id;
                size_t min_label = std::numeric_limits<size_t>::max();
                G.idx.for_each_neighbor(u, [&](graphio::NodeId neighbor) {
                    if (G.loc.is_local(neighbor)) {
                        min_label = std::min(min_label, G.labels[G.idx.get_index(neighbor)]);
                    }
                });
                if (min_label < G.labels[G.idx.get_index(u)]) {
                    G.labels[G.idx.get_index(u)] = min_label;
                    nodes_changed++;
                }
            }
            rounds++;
            atomic_debug(nodes_changed);
        } while (nodes_changed > 0);
        return rounds;
    };
    auto propagate_labels = [&]() {
        for (auto node_info : G.graph.node_info) {
            graphio::NodeId u = node_info.global_id;
            size_t label = G.labels[G.idx.get_index(u)];
            G.idx.for_each_neighbor(u, [&](graphio::NodeId neighbor) {
                if (!G.loc.is_local(neighbor)) {
                    PEID neighbor_rank = G.loc.rank(neighbor);
                    queue.post_message({label, neighbor}, neighbor_rank);
                }
            });
        }
    };
    auto on_message = [&](PEID sender, auto msg) {
        size_t label = msg[0];
        size_t node = msg[1];
        size_t old_label = G.labels[G.idx.get_index(node)];
        atomic_debug("Received message");
        G.labels[G.idx.get_index(node)] = std::min(label, old_label);
    };
    size_t rounds = 0;
    do {
    rounds = update_labels_locally();
        propagate_labels();
        queue.poll(on_message);
        std::cout << rounds << "\n";
    } while (rounds > 1);
    queue.terminate(on_message);
    update_labels_locally();
    // for (auto node_info : G.graph.node_info) {
    //     graphio::NodeId u = node_info.global_id;
    //     std::cout << "R" << rank << ": " << u << "\t" << G.labels[G.idx.get_index(u)] << "\n";
    // }
    return MPI_Finalize();
}
