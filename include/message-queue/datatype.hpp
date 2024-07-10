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

#pragma once

#include <tuple>
#include <kamping/mpi_datatype.hpp>
#include <utility>

namespace message_queue {
template <typename T>
constexpr bool is_builtin_mpi_type = mq_kamping::mpi_type_traits<T>::is_builtin;
template <typename T, typename Enable = void>
struct mpi_type_traits {};

template <typename T>
struct mpi_type_traits<T, std::enable_if_t<is_builtin_mpi_type<T>>> {
    static MPI_Datatype get_type() {
      return mq_kamping::mpi_type_traits<T>::data_type();
    }
};
template <typename T1, typename T2>
struct mpi_type_traits<
    std::pair<T1, T2>,
    std::enable_if_t<!is_builtin_mpi_type<std::pair<T1, T2>> && is_builtin_mpi_type<T1> && is_builtin_mpi_type<T2>>> {
    static MPI_Datatype get_type() {
        static MPI_Datatype type = construct();
        return type;
    }

    static MPI_Datatype construct() {
        std::pair<T1, T2> t;
        MPI_Datatype types[2] = {mpi_type_traits<T1>::get_type(), mpi_type_traits<T2>::get_type()};
        int blocklens[2] = {1, 1};
        MPI_Aint disp[2];
        MPI_Get_address(&t.first, &disp[0]);
        MPI_Get_address(&t.second, &disp[1]);
        disp[1] -= disp[0];
        disp[0] = 0;
        MPI_Datatype type;
        MPI_Type_create_struct(2, blocklens, disp, types, &type);
        MPI_Type_commit(&type);
        return type;
    }
};

template <typename... Ts>
struct mpi_type_traits<std::tuple<Ts...>,
                       std::enable_if_t<(sizeof...(Ts) > 0 && (is_builtin_mpi_type<Ts> && ...))>> {
  static MPI_Datatype get_type() {
    static MPI_Datatype type = construct();
    return type;
  }

  static MPI_Datatype construct() {
    std::tuple<Ts...> t;
    constexpr std::size_t tuple_size = sizeof...(Ts);

    MPI_Datatype types[tuple_size] = {mpi_type_traits<Ts>::get_type()...};
    int blocklens[tuple_size];
    MPI_Aint disp[tuple_size];
    MPI_Aint base;
    MPI_Get_address(&t, &base);

    // Calculate displacements for each tuple element using std::apply and fold expressions
    size_t i = 0;
    std::apply(
        [&](auto&... elem) {
          (
              [&] {
                MPI_Get_address(&elem, &disp[i]);
                disp[i] -= base;
                blocklens[i] = 1;
                i++;
              }(),
              ...);
        },
        t);

    MPI_Datatype type;
    MPI_Type_create_struct(tuple_size, blocklens, disp, types, &type);
    MPI_Type_commit(&type);
    return type;
  }
};

template <typename T>
concept MPIType = requires(T t) {
    { mpi_type_traits<T>::get_type() } -> std::same_as<MPI_Datatype>;
};

}  // namespace message_queue
