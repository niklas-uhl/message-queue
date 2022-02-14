#pragma once

#include <cstddef>

namespace message_queue {
struct MessageStatistics {
    size_t sent_messages;
    size_t received_messages;
    size_t send_volume;
    size_t receive_volume;

    explicit MessageStatistics() : sent_messages(0), received_messages(0), send_volume(0), receive_volume(0) {}

    void add(const MessageStatistics& rhs) {
        sent_messages += rhs.sent_messages;
        received_messages += rhs.received_messages;
        send_volume += rhs.send_volume;
        receive_volume += rhs.receive_volume;
    }
};
}
