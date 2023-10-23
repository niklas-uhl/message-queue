find_package(MPI REQUIRED)
add_library(kamping_datatype INTERFACE)
target_include_directories(kamping_datatype INTERFACE ${CMAKE_CURRENT_LIST_DIR}/include)
target_link_libraries(kamping_datatype INTERFACE MPI::MPI_CXX)
target_compile_features(kamping_datatype INTERFACE cxx_std_17)

add_library(kamping::kamping ALIAS kamping_datatype)
