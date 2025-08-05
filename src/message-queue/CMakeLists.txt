target_sources(
  message-queue
  INTERFACE
  FILE_SET HEADERS
  aggregators.hpp
  buffered_queue.hpp
  queue_builder.hpp
  concepts.hpp
  definitions.hpp
  indirection.hpp
  queue.hpp
  request_pool.hpp
  message_handles.hpp
  termination_counter.hpp
  fixed_size_buffer.hpp
  
)
