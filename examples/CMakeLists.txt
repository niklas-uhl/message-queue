find_package(range-v3 REQUIRED)
find_package(CLI11 REQUIRED)
find_package(fmt REQUIRED)
if(MESSAGE_QUEUE_BACKTRACE)
  find_package(bakward-mpi REQUIRED)
endif()

add_executable(message_hopping_example message_hopping_example.cpp)
target_link_libraries(
  message_hopping_example PUBLIC message-queue fmt::fmt kassert::kassert
                                 CLI11::CLI11)

add_executable(message_buffering_example message_buffering_example.cpp)
target_link_libraries(
  message_buffering_example PUBLIC message-queue CLI11::CLI11 kassert::kassert
                                   fmt::fmt)

add_executable(multiple_message_queues_example
               multiple_message_queues_example.cpp)
target_link_libraries(
  multiple_message_queues_example PUBLIC message-queue CLI11::CLI11
                                         kassert::kassert fmt::fmt)

add_executable(message_buffering_with_disjoint_message_and_buffer_type
               message_buffering_with_disjoint_message_and_buffer_type.cpp)
target_link_libraries(
  message_buffering_with_disjoint_message_and_buffer_type
  PUBLIC message-queue CLI11::CLI11 kassert::kassert fmt::fmt range-v3::range-v3)

add_executable(indirection_example indirection_example.cpp ${BACKWARD_ENABLE})
target_link_libraries(
  indirection_example PUBLIC message-queue CLI11::CLI11 kassert::kassert
  fmt::fmt range-v3::range-v3)
add_backward(indirection_example)
