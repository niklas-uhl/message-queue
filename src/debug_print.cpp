// Copyright (c) 2021-2023 Tim Niklas Uhl
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#include "message-queue/debug_print.hpp"

#include <array>

namespace message_queue {
void check_mpi_error(int errcode, std::string_view file, int line) {
    if (errcode != MPI_SUCCESS) {
        std::array<char, MPI_MAX_ERROR_STRING> buf;
        int resultlen;
        MPI_Error_string(errcode, buf.data(), &resultlen);
        std::string msg(buf.begin(), buf.begin() + resultlen);
        msg = msg + " in " + std::string{file} + ":" + std::to_string(line);
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
