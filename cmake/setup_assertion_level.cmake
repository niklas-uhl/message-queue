set(MESSAGE_QUEUE_ASSERTION_LEVEL
    $<IF:$<CONFIG:Debug>,"normal","exceptions">
    CACHE STRING "Assertion level")
set_property(CACHE MESSAGE_QUEUE_ASSERTION_LEVEL
             PROPERTY STRINGS none exceptions light normal heavy)
message(STATUS "Assertion level: ${MESSAGE_QUEUE_ASSERTION_LEVEL}")
string(
  CONCAT KASSERT_ASSERTION_LEVEL
         $<$<STREQUAL:${MESSAGE_QUEUE_ASSERTION_LEVEL},"none">:0>
         $<$<STREQUAL:"${MESSAGE_QUEUE_ASSERTION_LEVEL}","none">:0>
         $<$<STREQUAL:${MESSAGE_QUEUE_ASSERTION_LEVEL},"exceptions">:10>
         $<$<STREQUAL:"${MESSAGE_QUEUE_ASSERTION_LEVEL}","exceptions">:10>
         $<$<STREQUAL:${MESSAGE_QUEUE_ASSERTION_LEVEL},"light">:20>
         $<$<STREQUAL:"${MESSAGE_QUEUE_ASSERTION_LEVEL}","light">:20>
         $<$<STREQUAL:${MESSAGE_QUEUE_ASSERTION_LEVEL},"normal">:30>
         $<$<STREQUAL:"${MESSAGE_QUEUE_ASSERTION_LEVEL}","normal">:30>
         $<$<STREQUAL:${MESSAGE_QUEUE_ASSERTION_LEVEL},"heavy">:60>
         $<$<STREQUAL:"${MESSAGE_QUEUE_ASSERTION_LEVEL}","heavy">:60>)
