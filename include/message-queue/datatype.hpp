#pragma once

#include <boost/mpi/datatype.hpp>
#include <utility>

namespace message_queue {
template <typename T, typename Enable = void>
struct mpi_type_traits {};

template <typename T>
struct mpi_type_traits<T, std::enable_if_t<boost::mpi::is_mpi_builtin_datatype<T>::value>> {
    static MPI_Datatype get_type() {
        return boost::mpi::get_mpi_datatype<T>();
    }
};
template <typename T1, typename T2>
struct mpi_type_traits<std::pair<T1, T2>,
                       std::enable_if_t<!boost::mpi::is_mpi_builtin_datatype<std::pair<T1, T2>>::value &&
                                        boost::mpi::is_mpi_builtin_datatype<T1>::value &&
                                        boost::mpi::is_mpi_builtin_datatype<T2>::value>> {
    static MPI_Datatype get_type() {
        static MPI_Datatype type = construct();
        return type;
    }

    static MPI_Datatype construct() {
        std::pair<T1, T2> t;
        MPI_Datatype types[2] = {boost::mpi::get_mpi_datatype<T1>(), boost::mpi::get_mpi_datatype<T2>()};
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

template <typename T>
concept MPIType = requires(T t) {
    { mpi_type_traits<T>::get_type() } -> std::same_as<MPI_Datatype>;
};

}  // namespace message_queue