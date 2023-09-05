add_executable(message_hopping_example message_hopping_example.cpp)
target_link_libraries(
  message_hopping_example PUBLIC message-queue fmt::fmt kassert::kassert
                                 CLI11::CLI11)

# add_executable(asynchronous asynchronous.cpp)
# target_link_libraries(asynchronous MPI::MPI_CXX)

add_executable(async_bfs_example async_bfs_example.cpp)
target_link_libraries(
  async_bfs_example PUBLIC message-queue KaGen::KaGen CLI11::CLI11
                           kassert::kassert fmt::fmt)
if(MESSAGE_QUEUE_BACKTRACE)
  target_link_libraries(async_bfs_example PUBLIC Backward::Backward)
endif()
add_sanitizers(async_bfs_example)

add_executable(message_buffering_example message_buffering_example.cpp)
target_link_libraries(
  message_buffering_example PUBLIC message-queue CLI11::CLI11
                                   kassert::kassert fmt::fmt)
