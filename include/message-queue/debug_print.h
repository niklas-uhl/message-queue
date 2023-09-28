//
// Created by Tim Niklas Uhl on 19.11.20.
//

#pragma once

#include <mpi.h>
#include <unistd.h>
#include <array>
#include <exception>
#include <iostream>
#include <ostream>
#include <sstream>
#include <string>

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

inline std::string format(MPI_Comm comm) {
    if (comm == MPI_COMM_WORLD) {
        return "[WORLD]";
    } else if (comm == MPI_COMM_NULL) {
        return "[NULL]";
    } else {
        int size;
        MPI_Comm_size(comm, &size);
        std::stringstream ss;
        ss << "[COMM<" << size << ">@";
        ss << reinterpret_cast<void*>(comm);
        ss << "]";
        return ss.str();
    }
}
inline std::string format(MPI_Message msg) {
    if (msg == MPI_MESSAGE_NULL) {
        return "[MESSAGE_NULL]";
    } else if (msg == MPI_MESSAGE_NO_PROC) {
        return "[MESSAGE_NO_PROC]";
    } else {
        std::stringstream ss;
        ss << "[MESSAGE@";
        ss << reinterpret_cast<void*>(msg);
        ss << "]";
        return ss.str();
    }
}

inline std::string format_rank(int rank) {
    if (rank == MPI_ANY_SOURCE) {
        return "ANY_SOURCE";
    } else if (rank == MPI_PROC_NULL) {
        return "PROC_NULL";
    } else {
        return std::to_string(rank);
    }
}

inline std::string format_tag(int tag) {
    if (tag == MPI_ANY_TAG) {
        return "ANY_TAG";
    } else {
        return std::to_string(tag);
    }
}

}  // namespace message_queue
