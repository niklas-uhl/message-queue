target_sources(
  message-queue
  INTERFACE
  FILE_SET HEADERS
  aggregators.hpp
  buffered_queue.hpp
  concepts.hpp
  definitions.hpp
  indirection.hpp
  queue.hpp
  request_pool.hpp
)
