#include "message-queue/debug_print.hpp"

#include <array>

namespace message_queue {
void check_mpi_error(int errcode, const std::string& file, int line) {
    if (errcode != MPI_SUCCESS) {
        std::array<char, MPI_MAX_ERROR_STRING> buf;
        int resultlen;
        MPI_Error_string(errcode, buf.data(), &resultlen);
        std::string msg(buf.begin(), buf.begin() + resultlen);
        msg = msg + " in " + file + ":" + std::to_string(line);
        throw MPIException(msg);
    }
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
namespace mpi_formatter {
std::string format_comm(MPI_Comm comm) {
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

std::string format_message(MPI_Message msg) {
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

std::string format_rank(int rank) {
    if (rank == MPI_ANY_SOURCE) {
        return "ANY_SOURCE";
    } else if (rank == MPI_PROC_NULL) {
        return "PROC_NULL";
    } else {
        return std::to_string(rank);
    }
}

std::string format_tag(int tag) {
    if (tag == MPI_ANY_TAG) {
        return "ANY_TAG";
    } else {
        return std::to_string(tag);
    }
}
}  // namespace mpi_formatter
}  // namespace message_queue
