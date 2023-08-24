#include <context.h>
#include <kagen/kagen.h>
#include <mpi.h>
#include <CLI/App.hpp>
#include <CLI/CLI.hpp>
#include <algorithm>
#include <backward.hpp>
#include <cstddef>
#include <deque>
#include <ios>
#include <kassert/kassert.hpp>
#include <limits>
#include <numeric>
#include <queue>
#include <string>
#include <thread>
#include <vector>
#include "message-queue/buffered_queue.h"
#include "message-queue/debug_print.h"
#include "message-queue/queue.h"

using message_queue::PEID;
using NodeId = kagen::SInt;

// parameters {{{
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
// }}}

struct GraphWrapper {
    GraphWrapper(kagen::Graph&& G) : graph(G), my_rank(), labels(G.NumberOfLocalVertices()), vertex_distribution() {
        for (size_t i = 0; i < labels.size(); ++i) {
            labels[i] = std::numeric_limits<size_t>::max();
        }
        KASSERT(graph.xadj.size() == graph.NumberOfLocalVertices() + 1);
        KASSERT(graph.representation == kagen::GraphRepresentation::CSR);
        vertex_distribution = kagen::BuildVertexDistribution<NodeId>(graph, KAGEN_MPI_SINT, MPI_COMM_WORLD);
        MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    }

    kagen::Graph graph;
    PEID my_rank;
    std::vector<size_t> labels;
    std::vector<NodeId> vertex_distribution;

    void reset_labels() {
        for (size_t i = 0; i < labels.size(); ++i) {
            labels[i] = std::numeric_limits<size_t>::max();
        }
    }
    bool is_local(NodeId node) const {
        return node >= vertex_distribution[my_rank] && node < vertex_distribution[my_rank + 1];
    }

    PEID rank(NodeId node) const {
        auto it = std::upper_bound(vertex_distribution.begin(), vertex_distribution.end(), node);
        return std::distance(vertex_distribution.begin(), it) - 1;
    }

    size_t get_index(NodeId node) const {
        KASSERT(is_local(node));
        auto idx = node - vertex_distribution[my_rank];
        return idx;
    }
    template <typename NodeFunc>
    void for_each_neighbor(NodeId node, NodeFunc&& on_neighbor) const {
        auto begin = graph.xadj[get_index(node)];
        auto end = graph.xadj[get_index(node) + 1];
        for (auto edge_id = begin; edge_id < end; edge_id++) {
            on_neighbor(graph.adjncy[edge_id]);
        }
    }
};

template <class T>
struct Frontier {
    Frontier() : frontier(), new_frontier() {}
    std::deque<T> frontier;
    std::deque<T> new_frontier;

    void flip() {
        frontier.swap(new_frontier);
        new_frontier.clear();
        //MPI_Barrier(MPI_COMM_WORLD);
    }

    void push(T node) {
        new_frontier.push_back(node);
    }

    auto begin() const {
        return frontier.begin();
    }

    T const& front() const {
        return *begin();
    }

    void pop_front() {
        frontier.pop_front();
    }

    auto end() const {
        return frontier.end();
    }

    bool empty() const {
        return frontier.empty();
    }

    bool globally_empty() const {
        bool locally_empty = empty();
        bool globally_empty;
        MPI_Allreduce(&locally_empty, &globally_empty, 1, MPI_CXX_BOOL, MPI_LAND, MPI_COMM_WORLD);
        return globally_empty;
    }
};

double run_distributed(GraphWrapper& G) {
    PEID rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    DEBUG_BARRIER(rank);

    MPI_Barrier(MPI_COMM_WORLD);
    double start = MPI_Wtime();
    NodeId source = 0;
    // Frontier<size_t> frontier;
    auto merge = [](std::vector<NodeId>& buffer, std::vector<NodeId> msg, int tag) {
        for (auto elem : msg) {
            buffer.emplace_back(elem);
        }
        return msg.size();
    };

    auto split = [](std::vector<NodeId>& buffer, auto on_message, PEID sender) {
        for (size_t i = 0; i < buffer.size(); ++i) {
            on_message(buffer.begin() + i, buffer.begin() + i + 1, sender);
        }
    };
    auto queue = message_queue::make_buffered_queue<NodeId>(merge, split);
    Frontier<NodeId> frontier;
    if (G.is_local(source)) {
        frontier.push(source);
        G.labels[G.get_index(source)] = 0;
        frontier.flip();
    }
    size_t level = 1;
    while (!frontier.globally_empty()) {
        while (!frontier.empty()) {
            NodeId v = frontier.front();
            frontier.pop_front();
            G.for_each_neighbor(v, [&](NodeId u) { queue.post_message({u}, G.rank(u)); });
        }
        queue.terminate([&](auto begin, auto end, PEID sender [[maybe_unused]]) {
            NodeId v = *begin;
            size_t& v_label = G.labels[G.get_index(v)];
            if (v_label == std::numeric_limits<size_t>::max()) {
                v_label = level;
                frontier.push(v);
            }
        });
        level++;
        frontier.flip();
    }
    MPI_Barrier(MPI_COMM_WORLD);
    double end = MPI_Wtime();
    return end - start;
}

double run_distributed_async(GraphWrapper& G, const AsyncParameters& params) {
    PEID rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    // MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN);

    // DEBUG_BARRIER(rank);

    MPI_Barrier(MPI_COMM_WORLD);
    double start = MPI_Wtime();
    NodeId source = 0;
    // Frontier<size_t> frontier;
    auto merge = [](std::vector<NodeId>& buffer, std::vector<NodeId> msg, int tag) {
        for (auto elem : msg) {
            buffer.emplace_back(elem);
        }
        return msg.size();
        // atomic_debug(buffer);
    };

    auto split = [](std::vector<NodeId>& buffer, auto on_message, PEID sender) {
        for (size_t i = 0; i < buffer.size(); i += 2) {
            on_message(buffer.cbegin() + i, buffer.cbegin() + i + 2, sender);
        }
    };
    // auto queue = message_queue::MessageQueue<graphio::NodeId>();
    auto queue = message_queue::make_buffered_queue<NodeId>(merge, split);
    std::deque<std::pair<NodeId, size_t>> local_queue;
    auto on_message = [&](auto begin, auto end,
                          PEID sender [[maybe_unused]]) {
        queue.reactivate();
        NodeId v = *begin;
        // graphio::NodeId v = message[0];
        std::stringstream out;
        out << "Receive " << v << " from " << sender;
        // atomic_debug(out.str());
        KASSERT(G.is_local(v));
        size_t label = *(begin + 1);
        // size_t label = message[1];
        local_queue.emplace_back(v, label);
    };
    // queue.set_threshold(100);
    if (!params.buffering) {
        queue.set_threshold(0);
    } else {
        queue.set_threshold(G.graph.NumberOfLocalVertices());
    }
    if (G.is_local(source)) {
        auto msg = {source, NodeId{0}};
        on_message(msg.begin(), msg.end(), rank);
    } else {
        while (!queue.poll(on_message)) {
        };
    }
    size_t discovered_nodes = 0;
    do {
        do {
            while (!local_queue.empty()) {
                NodeId v;
                size_t label;
                std::tie(v, label) = local_queue.front();
                local_queue.pop_front();
                std::stringstream out;
                out << "Pop " << v;
                // atomic_debug(out.str());
                KASSERT(G.is_local(v));
                size_t& v_label = G.labels[G.get_index(v)];
                if (label < v_label) {
                    std::stringstream out;
                    discovered_nodes++;
                    out << "Discovered " << v;
                    // atomic_debug(out.str());
                    v_label = label;
                    G.for_each_neighbor(v, [&](NodeId u) {
                        if (G.is_local(u)) {
                            local_queue.emplace_back(u, label + 1);
                        } else {
                            queue.post_message({u, label + 1}, G.rank(u));
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
    return end - start;
}

int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);
    backward::SignalHandling sh;

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    CLI::App app;

    // graphio::GeneratorParameters gen_params;
    std::string input;
    app.add_option("INPUT", input)->required();
    // app.add_option("--n", gen_params.n);
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

    kagen::KaGen gen(MPI_COMM_WORLD);
    gen.UseCSRRepresentation();
    gen.EnableOutput(true);
    gen.EnableBasicStatistics();
    kagen::Graph G_gen;
    try {
        G_gen = gen.GenerateFromOptionString(input);
    } catch (std::runtime_error e) {
        // if (rank == 0) {
        std::cerr << "KaGen failed with error: " << e.what() << "\n";
        //}
        MPI_Finalize();
        exit(1);
    }
    GraphWrapper G{std::move(G_gen)};

    auto single_config_run = [&](const AsyncParameters& params) {
        for (size_t iteration = 0; iteration < iterations + 1; iteration++) {
            double time;
            if (async) {
                time = run_distributed_async(G, params);
            } else {
                time = run_distributed(G);
            }
            size_t max_label = *std::max_element(G.labels.begin(), G.labels.end());
            size_t global_max_label;
            MPI_Reduce(&max_label, &global_max_label, 1, MPI_UINT64_T, MPI_MAX, 0, MPI_COMM_WORLD);
            if (rank == 0) {
                std::stringstream out;
                if (iteration > 0) {
                    // clang-format off
                out << "RESULT" << std::boolalpha
                    << " input=" << input
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
                    << " max_level=" << global_max_label
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
