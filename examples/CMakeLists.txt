FetchContent_MakeAvailable(range-v3 CLI11)

add_library(range-v3 INTERFACE IMPORTED)
target_include_directories(range-v3 INTERFACE ${range-v3_SOURCE_DIR}/include)
add_library(range-v3::range-v3 ALIAS range-v3)

add_executable(message_hopping_example message_hopping_example.cpp)
target_link_libraries(
  message_hopping_example PUBLIC message-queue kassert::kassert
                                 CLI11::CLI11)

add_executable(message_buffering_example message_buffering_example.cpp)
target_link_libraries(
  message_buffering_example PUBLIC message-queue CLI11::CLI11 kassert::kassert
                                   )

add_executable(multiple_message_queues_example
               multiple_message_queues_example.cpp)
target_link_libraries(
  multiple_message_queues_example PUBLIC message-queue CLI11::CLI11
                                         kassert::kassert)

add_executable(message_buffering_with_disjoint_message_and_buffer_type
               message_buffering_with_disjoint_message_and_buffer_type.cpp)
target_link_libraries(
  message_buffering_with_disjoint_message_and_buffer_type
  PUBLIC message-queue CLI11::CLI11 kassert::kassert range-v3::range-v3)

add_executable(indirection_example indirection_example.cpp)
target_link_libraries(
  indirection_example PUBLIC message-queue CLI11::CLI11 kassert::kassert
  range-v3::range-v3)
