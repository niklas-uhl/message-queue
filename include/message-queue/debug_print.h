//
// Created by Tim Niklas Uhl on 19.11.20.
//

#pragma once

#include <mpi.h>
#include <unistd.h>
#include <array>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <istream>
#include <iterator>
#include <ostream>
#include <sstream>
#include <string>
#include <vector>

namespace message_queue {
using PEID = int;

#ifndef DEBUG_BARRIER
#ifndef NDEBUG
#define DEBUG_BARRIER(rank)                                                                    \
    {                                                                                          \
        if (std::getenv("DEBUG_BARRIER") != nullptr) {                                         \
            std::string value(std::getenv("DEBUG_BARRIER"));                                   \
            std::string delimiter = ":";                                                       \
            size_t pos = 0;                                                                    \
            std::string token;                                                                 \
            std::vector<int> PEs;                                                              \
            while ((pos = value.find(delimiter)) != std::string::npos) {                       \
                token = value.substr(0, pos);                                                  \
                PEs.push_back(std::atoi(token.c_str()));                                       \
                value.erase(0, pos + delimiter.length());                                      \
            }                                                                                  \
            PEs.push_back(std::atoi(value.c_str()));                                           \
            if (std::find(PEs.begin(), PEs.end(), rank) != PEs.end()) {                        \
                volatile int i = 0;                                                            \
                char hostname[256];                                                            \
                gethostname(hostname, sizeof(hostname));                                       \
                printf("PID %d on %s (rank %d) ready for attach\n", getpid(), hostname, rank); \
                fflush(stdout);                                                                \
                while (0 == i)                                                                 \
                    sleep(5);                                                                  \
            }                                                                                  \
        }                                                                                      \
    };
#else
#define DEBUG_BARRIER(rank)
#endif
#endif

struct MPIException : public std::exception {
    MPIException(const std::string& msg) : msg_() {
        PEID rank;
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        msg_ = "[R" + std::to_string(rank) + "] " + msg;
    }
    const char* what() const throw() {
        return msg_.c_str();
    }

private:
    std::string msg_;
};

inline void check_mpi_error(int errcode, const std::string& file, int line) {
    if (errcode != MPI_SUCCESS) {
        std::array<char, MPI_MAX_ERROR_STRING> buf;
        int resultlen;
        MPI_Error_string(errcode, buf.data(), &resultlen);
        std::string msg(buf.begin(), buf.begin() + resultlen);
        msg = msg + " in " + file + ":" + std::to_string(line);
        throw MPIException(msg);
    }
}
template <class T>
inline std::ostream& operator<<(std::ostream& out, const std::vector<T>& v) {
    if (v.empty()) {
        out << "[]";
    } else {
        out << "[";
        for (const auto& elem : v) {
            out << elem << ", ";
            ;
        }
        out << "\b\b]";
    }
    return out;
}

template <class T, class V>
inline std::ostream& operator<<(std::ostream& out, const std::pair<T, V>& p) {
    return out << "<" << p.first << ", " << p.second << ">";
}
template <class MessageType>
inline void atomic_debug(MessageType message, std::ostream& out = std::cout, bool newline = true) {
    std::stringstream sout;
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    sout << "[R" << rank << "] " << message;
    if (newline) {
        sout << std::endl;
    }
    out << sout.str();
}

constexpr unsigned long long log2(unsigned long long x) {
#if defined __has_builtin
#if __has_builtin(__builtin_clzl)
#define builtin_clzl(y) __builtin_clzl(y)
#endif
#endif
#ifdef builtin_clzl
    return 8 * sizeof(unsigned long long) - builtin_clzl(x) - 1;
#else
    int log = 0;
    while (x >>= 1)
        ++log;
    return log;
#endif
}
}  // namespace message_queue
