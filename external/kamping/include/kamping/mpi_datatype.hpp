// The contents of this file have been extracted from a file which is part of KaMPIng, licensed under LGPLv3.
//
// As this header file only consists of small templates declarations, it allows
// to be included in other projects with their license of choice.
// See section 3 of the LGPLv3 license for details.
//
// Adapted from KaMPIng by Tim Niklas Uhl, 2023.
//
// Copyright 2021-2022 The KaMPIng Authors
//
// KaMPIng is free software : you can redistribute it and/or modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later
// version. KaMPIng is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
// implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
// for more details.
//
// You should have received a copy of the GNU Lesser General Public License along with KaMPIng.  If not, see
// <https://www.gnu.org/licenses/>.

/// @file
/// @brief Utility that maps C++ types to types that can be understood by MPI.

#pragma once

#include <cassert>
#include <complex>
#include <cstdint>
#include <type_traits>

#include <mpi.h>

namespace kamping {
/// @brief Wrapper around bool to allow handling containers of boolean values
class kabool {
public:
    /// @brief default constructor for a \c kabool with value \c false
    constexpr kabool() noexcept : _value() {}
    /// @brief constructor to construct a \c kabool out of a \c bool
    constexpr kabool(bool value) noexcept : _value(value) {}

    /// @brief implicit cast of \c kabool to \c bool
    inline constexpr operator bool() const noexcept {
        return _value;
    }

private:
    bool _value; /// < the wrapped boolean value
};

/// @addtogroup kamping_mpi_utility
/// @{

/// @brief the members specify which group the datatype belongs to according to the type groups specified in
/// Section 5.9.2 of the MPI 3.1 standard.
enum class TypeCategory { integer, floating, complex, logical, byte, undefined };

#ifdef KAMPING_DOXYGEN_ONLY
/// @brief maps C++ types to builtin \c MPI_Datatypes
///
/// the members specify which group the datatype belongs to according to the type groups specified in Section 5.9.2 of
/// the MPI 3.1 standard.
/// @tparam T Type to map to an \c MPI_Datatype.
template <typename T>
struct mpi_type_traits {
    /// @brief \c true, if the type maps to a builtin \c MPI_Datatype.
    static constexpr bool is_builtin;
    /// @brief Category the type belongs to according to the MPI standard.
    static constexpr TypeCategory category;
    /// @brief This member function is only available if \c is_builtin is true. If this is the case, it returns the \c
    /// MPI_Datatype
    /// @returns Constant of type \c MPI_Datatype mapping to type \c T according the the MPI standard.
    static MPI_Datatype data_type();
};
#else
/// @brief Base type for non-builtin types.
struct is_builtin_mpi_type_false {
    static constexpr bool         is_builtin = false;
    static constexpr TypeCategory category   = TypeCategory::undefined;
};

/// @brief Base type for builtin types.
struct is_builtin_mpi_type_true : is_builtin_mpi_type_false {
    static constexpr bool is_builtin = true;
};

/// @brief Base template for implementation.
template <typename T>
struct mpi_type_traits_impl : is_builtin_mpi_type_false {};

// template specializations of mpi_type_traits_impl

template <>
struct mpi_type_traits_impl<char> : is_builtin_mpi_type_true {
    static MPI_Datatype data_type() {
        return MPI_CHAR;
    }
};

template <>
struct mpi_type_traits_impl<signed char> : is_builtin_mpi_type_true {
    static MPI_Datatype data_type() {
        return MPI_SIGNED_CHAR;
    }
    static constexpr TypeCategory category = TypeCategory::integer;
};

template <>
struct mpi_type_traits_impl<unsigned char> : is_builtin_mpi_type_true {
    static MPI_Datatype data_type() {
        return MPI_UNSIGNED_CHAR;
    }
    static constexpr TypeCategory category = TypeCategory::integer;
};

template <>
struct mpi_type_traits_impl<wchar_t> : is_builtin_mpi_type_true {
    static MPI_Datatype data_type() {
        return MPI_WCHAR;
    }
};

template <>
struct mpi_type_traits_impl<short int> : is_builtin_mpi_type_true {
    static MPI_Datatype data_type() {
        return MPI_SHORT;
    }
    static constexpr TypeCategory category = TypeCategory::integer;
};

template <>
struct mpi_type_traits_impl<unsigned short int> : is_builtin_mpi_type_true {
    static MPI_Datatype data_type() {
        return MPI_UNSIGNED_SHORT;
    }
    static constexpr TypeCategory category = TypeCategory::integer;
};

template <>
struct mpi_type_traits_impl<int> : is_builtin_mpi_type_true {
    static MPI_Datatype data_type() {
        return MPI_INT;
    }
    static constexpr TypeCategory category = TypeCategory::integer;
};

template <>
struct mpi_type_traits_impl<unsigned int> : is_builtin_mpi_type_true {
    static MPI_Datatype data_type() {
        return MPI_UNSIGNED;
    }
    static constexpr TypeCategory category = TypeCategory::integer;
};

template <>
struct mpi_type_traits_impl<long int> : is_builtin_mpi_type_true {
    static MPI_Datatype data_type() {
        return MPI_LONG;
    }
    static constexpr TypeCategory category = TypeCategory::integer;
};

template <>
struct mpi_type_traits_impl<unsigned long int> : is_builtin_mpi_type_true {
    static MPI_Datatype data_type() {
        return MPI_UNSIGNED_LONG;
    }
    static constexpr TypeCategory category = TypeCategory::integer;
};

template <>
struct mpi_type_traits_impl<long long int> : is_builtin_mpi_type_true {
    static MPI_Datatype data_type() {
        return MPI_LONG_LONG;
    }
    static constexpr TypeCategory category = TypeCategory::integer;
};

template <>
struct mpi_type_traits_impl<unsigned long long int> : is_builtin_mpi_type_true {
    static MPI_Datatype data_type() {
        return MPI_UNSIGNED_LONG_LONG;
    }
    static constexpr TypeCategory category = TypeCategory::integer;
};

template <>
struct mpi_type_traits_impl<float> : is_builtin_mpi_type_true {
    static MPI_Datatype data_type() {
        return MPI_FLOAT;
    }
    static constexpr TypeCategory category = TypeCategory::floating;
};

template <>
struct mpi_type_traits_impl<double> : is_builtin_mpi_type_true {
    static MPI_Datatype data_type() {
        return MPI_DOUBLE;
    }
    static constexpr TypeCategory category = TypeCategory::floating;
};

template <>
struct mpi_type_traits_impl<long double> : is_builtin_mpi_type_true {
    static MPI_Datatype data_type() {
        return MPI_LONG_DOUBLE;
    }
    static constexpr TypeCategory category = TypeCategory::floating;
};

template <>
struct mpi_type_traits_impl<bool> : is_builtin_mpi_type_true {
    static MPI_Datatype data_type() {
        return MPI_CXX_BOOL;
    }
    static constexpr TypeCategory category = TypeCategory::logical;
};

template <>
struct mpi_type_traits_impl<kabool> : is_builtin_mpi_type_true {
    static MPI_Datatype data_type() {
        return MPI_CXX_BOOL;
    }
    static constexpr TypeCategory category = TypeCategory::logical;
};

template <>
struct mpi_type_traits_impl<std::complex<float>> : is_builtin_mpi_type_true {
    static MPI_Datatype data_type() {
        return MPI_CXX_FLOAT_COMPLEX;
    }
    static constexpr TypeCategory category = TypeCategory::complex;
};
template <>
struct mpi_type_traits_impl<std::complex<double>> : is_builtin_mpi_type_true {
    static MPI_Datatype data_type() {
        return MPI_CXX_DOUBLE_COMPLEX;
    }
    static constexpr TypeCategory category = TypeCategory::complex;
};

template <>
struct mpi_type_traits_impl<std::complex<long double>> : is_builtin_mpi_type_true {
    static MPI_Datatype data_type() {
        return MPI_CXX_LONG_DOUBLE_COMPLEX;
    }
    static constexpr TypeCategory category = TypeCategory::complex;
};

/// @brief wrapper for \c mpi_type_traits_impl which removes const and volatile qualifiers
template <typename T>
struct mpi_type_traits : mpi_type_traits_impl<std::remove_cv_t<T>> {};
#endif

/// @}

} // namespace kamping
