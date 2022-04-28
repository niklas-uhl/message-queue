#pragma once

#include <cstddef>
#include <atomic>

namespace message_queue {
struct MessageStatistics {
    std::atomic<size_t> sent_messages;
    std::atomic<size_t> received_messages;
    std::atomic<size_t> send_volume;
    std::atomic<size_t> receive_volume;

    explicit MessageStatistics() : sent_messages(0), received_messages(0), send_volume(0), receive_volume(0) {}
    MessageStatistics& operator=(const MessageStatistics& stats) {
        sent_messages = stats.sent_messages.load();
        received_messages = stats.received_messages.load();
        send_volume = stats.send_volume.load();
        receive_volume = stats.receive_volume.load();
        return *this;
    }
    explicit MessageStatistics(const MessageStatistics& stats) {
        sent_messages = stats.sent_messages.load();
        received_messages = stats.received_messages.load();
        send_volume = stats.send_volume.load();
        receive_volume = stats.receive_volume.load();
    }

    void add(const MessageStatistics& rhs) {
        sent_messages += rhs.sent_messages;
        received_messages += rhs.received_messages;
        send_volume += rhs.send_volume;
        receive_volume += rhs.receive_volume;
    }
};
}
