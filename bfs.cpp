#include <graph-io/graph_io.h>
#include <mpi.h>
#include <CLI/CLI.hpp>
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
#include "CLI/App.hpp"
#include "backward.hpp"
#include "graph-io/definitions.h"
#include "graph-io/distributed_graph_io.h"
#include "graph-io/gen_parameters.h"
#include "graph-io/graph_definitions.h"
#include "graph-io/local_graph_view.h"
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

    void reset_labels() {
        for (size_t i = 0; i < labels.size(); ++i) {
            labels[i] = std::numeric_limits<size_t>::max();
        }
    }
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

double run_distributed(GraphWrapper& G) {
    PEID rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    // MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN);

    // DEBUG_BARRIER(rank);

    MPI_Barrier(MPI_COMM_WORLD);
    double start = MPI_Wtime();
    graphio::NodeId source = 0;
    // Frontier<size_t> frontier;
    auto merge = [](std::vector<graphio::NodeId>& buffer, std::vector<graphio::NodeId> msg, int tag) {
        for (auto elem : msg) {
            buffer.emplace_back(elem);
        }
        return msg.size();
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

struct AsyncParameters {
    bool buffering = false;
    bool dynamic_buffering = false;
    double dampening_factor = 0.5;
    bool poll_after_post = false;
    bool poll_after_pop = false;
    size_t flush_level = 0;
};
struct AsyncParameterSet {
    std::vector<bool> buffering;
    std::vector<bool> dynamic_buffering;
    std::vector<double> dampening_factor;
    std::vector<bool> poll_after_post;
    std::vector<bool> poll_after_pop;
    std::vector<size_t> flush_level;

    std::vector<AsyncParameters> expand() {
        std::vector<AsyncParameters> parameter_set;
        for (auto buffering_value : !buffering.empty() ? buffering : std::vector<bool>{false}) {
            for (auto dynamic_buffering_value :
                 !dynamic_buffering.empty() ? dynamic_buffering : std::vector<bool>{false}) {
                for (auto dampening_factor_value :
                     !dampening_factor.empty() ? dampening_factor : std::vector<double>{.5}) {
                    for (auto poll_after_post_value :
                         !poll_after_post.empty() ? poll_after_post : std::vector<bool>{false}) {
                        for (auto poll_after_pop_value :
                             !poll_after_pop.empty() ? poll_after_pop : std::vector<bool>{false}) {
                            for (auto flush_level_value : !flush_level.empty() ? flush_level : std::vector<size_t>{0}) {
                                AsyncParameters params;
                                params.buffering = buffering_value;
                                params.dynamic_buffering = dynamic_buffering_value;
                                params.dampening_factor = dampening_factor_value;
                                params.poll_after_post = poll_after_post_value;
                                params.poll_after_pop = poll_after_pop_value;
                                params.flush_level = flush_level_value;
                                parameter_set.emplace_back(std::move(params));
                            }
                        }
                    }
                }
            }
        }
        return parameter_set;
    }
};

double run_distributed_async(GraphWrapper& G, const AsyncParameters& params) {
    PEID rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    // MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN);

    // DEBUG_BARRIER(rank);

    MPI_Barrier(MPI_COMM_WORLD);
    double start = MPI_Wtime();
    graphio::NodeId source = 0;
    // Frontier<size_t> frontier;
    auto merge = [](std::vector<graphio::NodeId>& buffer, std::vector<graphio::NodeId> msg, int tag) {
        for (auto elem : msg) {
            buffer.emplace_back(elem);
        }
        return msg.size();
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
    if (!params.buffering) {
        queue.set_threshold(0);
    } else {
        queue.set_threshold(G.graph.local_node_count());
    }
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
                assert(G.loc.is_local(v));
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
                            if (params.poll_after_post) {
                                queue.poll(on_message);
                            }
                        }
                    });
                } else {
                    std::stringstream out;
                    out << "Discarding " << v;
                    // atomic_debug(out.str());
                }
                if (params.poll_after_pop) {
                    queue.poll(on_message);
                }
            }
            if (params.dynamic_buffering) {
                size_t new_threshold = queue.buffer_ocupacy() * params.dampening_factor;
                // atomic_debug(new_threshold);
                queue.set_threshold(new_threshold);
            }
            if (params.flush_level > 1) {
                queue.flush_all();
            }
        } while (queue.poll(on_message));
        if (params.flush_level > 0) {
            queue.flush_all();
        }
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
    MPI_Init(&argc, &argv);
    backward::SignalHandling sh;

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    CLI::App app;

    graphio::GeneratorParameters gen_params;
    std::string input;
    app.add_option("INPUT", input)->required();
    std::string input_format = "metis";
    app.add_option("--input_format", input_format);
    app.add_option("--n", gen_params.n);
    size_t iterations = 1;
    app.add_option("--iterations", iterations);
    bool async = false;
    auto async_opt = app.add_option("--async", async);

    auto single_run = app.add_subcommand("single");
    AsyncParameters async_params;
    single_run->add_option("--buffering", async_params.buffering)->needs(async_opt);
    single_run->add_option("--dynamic_buffering", async_params.dynamic_buffering)->needs(async_opt);
    single_run->add_option("--dampening_factor", async_params.dampening_factor)->needs(async_opt);
    single_run->add_option("--poll_after_post", async_params.poll_after_post)->needs(async_opt);
    single_run->add_option("--poll_after_pop", async_params.poll_after_pop)->needs(async_opt);
    single_run->add_option("--flush_level", async_params.flush_level)->needs(async_opt);

    auto benchmark = app.add_subcommand("bench");
    AsyncParameterSet parameter_set;
    benchmark->add_option("--buffering", parameter_set.buffering)->needs(async_opt);
    benchmark->add_option("--dynamic_buffering", parameter_set.dynamic_buffering)->needs(async_opt);
    benchmark->add_option("--dampening_factor", parameter_set.dampening_factor)->needs(async_opt);
    benchmark->add_option("--poll_after_post", parameter_set.poll_after_post)->needs(async_opt);
    benchmark->add_option("--poll_after_pop", parameter_set.poll_after_pop)->needs(async_opt);
    benchmark->add_option("--flush_level", parameter_set.flush_level)->needs(async_opt);

    int retval = -1;
    try {
        app.parse(argc, argv);
    } catch (const CLI ::ParseError& e) {
        if (rank == 0) {
            retval = app.exit(e);
        } else {
            retval = 1;
        }
    }
    if (retval > -1) {
        MPI_Finalize();
        exit(retval);
    }
    // MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN);

    graphio::LocalGraphView G_in;
    if (input == "gnm") {
        gen_params.m = gen_params.n + 4;
        gen_params.generator = "gnm_undirected";
        G_in = graphio::gen_local_graph(gen_params, rank, size);
    } else {
        G_in = graphio::read_local_graph(input, graphio::input_types.at(input_format), rank, size);
    }
    GraphWrapper G(std::move(G_in));

    auto single_config_run = [&](const AsyncParameters& params) {
        for (size_t iteration = 0; iteration < iterations + 1; iteration++) {
            double time;
            if (async) {
                time = run_distributed_async(G, params);
            } else {
                time = run_distributed(G);
            }

            if (rank == 0) {
                std::string input_name;
                if (input == "gnm") {
                    input_name = "gnm(" + std::to_string(gen_params.n) + "," + std::to_string(gen_params.m) + ")";
                } else {
                    input_name = std::filesystem::path(input).stem();
                }
                std::stringstream out;
                if (iteration > 0) {
                    // clang-format off
                out << "RESULT" << std::boolalpha
                    << " input=" << input_name
                    << " p=" << size
                    << " async=" << async
                    << " iteration=" << iteration
                    << " time=" << time
                    << " buffering=" << params.buffering
                    << " dynamic_buffering=" << params.dynamic_buffering
                    << " dampening_factor=" << params.dampening_factor
                    << " poll_after_post=" << params.poll_after_post
                    << " poll_after_pop=" << params.poll_after_pop
                    << " flush_level=" << params.flush_level
                    << std::endl;
                std::cout << out.str();
                    // clang-format on
                }
            }
            G.reset_labels();
        }
    };
    if (*single_run) {
        single_config_run(async_params);
    } else if (*benchmark) {
        for (auto params : parameter_set.expand()) {
            single_config_run(params);
        }
    }
    return MPI_Finalize();
}
