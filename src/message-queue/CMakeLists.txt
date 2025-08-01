target_sources(
  message-queue
  INTERFACE
  FILE_SET HEADERS
  aggregators.hpp
  buffered_queue.hppxo
  concepts.hpp
  definitions.hpp
  indirection.hpp
  queue.hpp
  request_pool.hpp
  message_handles.hpp
  termination_counter.hpp
)
