#pragma once

#include <mpi.h>
#include <cstddef>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>
#include "debug_print.h"
#include "message-queue/debug_print.h"
#include "message-queue/mpi_datatype.h"

template <typename T>
class MessageQueue {
    static_assert(kamping::mpi_type_traits<T>::is_builtin, "Only builtin MPI types are supported");

    enum class State { posted, initiated, completed };

    template <class S>
    struct MessageHandle {
        size_t message_size;
        std::vector<S> message;
        MPI_Request request;
        size_t request_id;
        int tag = MPI_ANY_TAG;
        State state = State::posted;

        bool test() {
            if (state == State::completed) {
                return true;
            }
            if (state == State::posted) {
                return false;
            }
            int finished = false;
            MPI_Status status;
            assert(state == State::initiated);
            assert(this->request != MPI_REQUEST_NULL);
            int err = MPI_Test(&request, &finished, &status);
            check_mpi_error(err, __FILE__, __LINE__);
            if (finished) {
                state = State::completed;
                return true;
            }
            return false;
        }
    };

    template <class S>
    using MessageHandlePtr = std::unique_ptr<MessageHandle<S>>;

    template <class S>
    struct SendHandle : MessageHandle<S> {
        PEID receiver = MPI_ANY_SOURCE;

        void initiate_send() {
            int err = MPI_Isend(this->message.data(), this->message.size(), kamping::mpi_type_traits<S>::data_type(),
                                receiver, this->tag, MPI_COMM_WORLD, &this->request);
            check_mpi_error(err, __FILE__, __LINE__);
            this->state = State::initiated;
        }
    };

    template <class S>
    using SendHandlePtr = std::unique_ptr<SendHandle<S>>;

    template <class S>
    struct ReceiveHandle : MessageHandle<S> {
        MPI_Message matched_message;
        MPI_Status status;
        PEID sender = MPI_ANY_SOURCE;

        void start_receive() {
            if (this->state != State::posted) {
                return;
            }
            this->state = State::initiated;
            this->message.resize(this->message_size);
            MPI_Imrecv(this->message.data(), this->message.size(), kamping::mpi_type_traits<S>::data_type(),
                       &matched_message, &(this->request));
        }
    };

    template <class S>
    using ReceiveHandlePtr = std::unique_ptr<ReceiveHandle<S>>;

    struct ProbeResult {
        MPI_Status status;
        MPI_Message matched_message;
        PEID sender;
        int tag;

        template <typename S>
        ReceiveHandlePtr<S> handle() {
            auto handle = std::make_unique<ReceiveHandle<S>>();
            int message_size;
            MPI_Get_count(&status, kamping::mpi_type_traits<S>::data_type(), &message_size);
            handle->message_size = message_size;
            handle->matched_message = matched_message;
            handle->tag = status.MPI_TAG;
            handle->sender = status.MPI_SOURCE;
            return handle;
        }
    };

    static std::optional<ProbeResult> probe(PEID source = MPI_ANY_SOURCE, PEID tag = MPI_ANY_TAG) {
        ProbeResult result;
        int message_found = false;
        MPI_Improbe(source, tag, MPI_COMM_WORLD, &message_found, &result.matched_message, &result.status);
        if (message_found) {
            result.sender = result.status.MPI_SOURCE;
            result.tag = result.status.MPI_TAG;
            return result;
        }
        return std::nullopt;
    }

    template <typename S>
    void post_message_impl(std::vector<SendHandlePtr<S>>& handles, std::vector<S>&& message, PEID receiver, int tag) {
        handles.emplace_back(new SendHandle<S>{});
        SendHandlePtr<S>& handle = handles.back();
        handle->message = std::forward<std::vector<S>>(message);
        handle->receiver = receiver;
        handle->tag = tag;
        handle->request_id = this->request_id_;
        this->request_id_++;
        handle->initiate_send();
    }

public:
    MessageQueue()
        : send_handles_(), recv_handles_(), send_count_(0), recv_count_(0), request_id_(0), rank_(0), size_(0) {
        MPI_Comm_rank(MPI_COMM_WORLD, &rank_);
        MPI_Comm_size(MPI_COMM_WORLD, &size_);
    }

    void post_message(std::vector<T>&& message, PEID receiver, int tag = 0) {
        assert(tag != control_wave_tag);
        // std::cout << message.size() <<"\n";
        post_message_impl(send_handles_, std::move(message), receiver, tag);
        send_count_++;
    }

    template <typename MessageHandler>
    void poll(MessageHandler&& on_message) {
        static_assert(std::is_invocable_v<MessageHandler, PEID, std::vector<T>>);
        auto check_and_remove = [&](auto& handles, auto on_request_finish) {
            size_t i = 0;
            while (i < handles.size()) {
                auto& handle = handles[i];
                if (handle->test()) {
                    on_request_finish(*handle);
                    if (i < handles.size() - 1) {
                        handles[i] = std::move(handles.back());
                    }
                    handles.resize(handles.size() - 1);
                } else {
                    i++;
                }
            }
        };
        check_and_remove(send_handles_, [](auto) {});
        check_and_remove(control_send_handles_, [](auto) {});
        while (true) {
            std::optional result = probe();
            if (!result.has_value()) {
                break;
            }
            if (result.value().tag == control_wave_tag) {
                auto handle = result.value().template handle<size_t>();
                handle->request_id = this->request_id_;
                this->request_id_++;
                control_recv_handles_.emplace_back(std::move(handle));
                control_recv_handles_.back()->start_receive();
            } else {
                auto handle = result.value().template handle<T>();
                handle->request_id = this->request_id_;
                this->request_id_++;
                recv_handles_.emplace_back(std::move(handle));
                recv_handles_.back()->start_receive();
            }
        }
        //size_t i = 0;
        check_and_remove(recv_handles_, [&](ReceiveHandle<T>& handle) {
            recv_count_++;
            on_message(handle.sender, std::move(handle.message));
        });
        check_and_remove(control_recv_handles_, [&](ReceiveHandle<size_t>& handle) {
            if (handle.tag == control_wave_tag) {
                if (handle.message[2] == static_cast<size_t>(rank_)) {
                    if (handle.message[0] == handle.message[1]) {
                        if (termination_state == TerminationState::first_wave) {
                            termination_state = TerminationState::first_wave_success;
                        } else if (termination_state == TerminationState::second_wave) {
                            termination_state = TerminationState::terminated;
                        } else {
                            assert(false);
                        }
                    } else {
                        termination_state = TerminationState::active;
                    }
                } else {
                    this->post_message_impl(
                        control_send_handles_,
                        {handle.message[0] + send_count_, handle.message[1] + recv_count_, handle.message[2]},
                        (rank_ + 1) % size_, control_wave_tag);
                }
            }
        });
    }

    bool advance_control_wave() {
        auto start_wave = [&]() {
            number_of_waves++;
            this->post_message_impl<size_t>(control_send_handles_,
                                            {send_count_, recv_count_, static_cast<size_t>(rank_)}, (rank_ + 1) % size_,
                                            control_wave_tag);
        };
        if (termination_state == TerminationState::active) {
            // atomic_debug("Starting new wave");
            start_wave();
            termination_state = TerminationState::first_wave;
        }
        if (termination_state == TerminationState::first_wave_success) {
            // atomic_debug("Starting second wave");
            start_wave();
            termination_state = TerminationState::second_wave;
        }
        if (termination_state == TerminationState::terminated) {
            atomic_debug("Terminated");
            return false;
        }
        return true;
    }

    template <typename MessageHandler>
    void terminate(MessageHandler&& on_message) {
        std::pair<size_t, size_t> global_count = {0, 0};
        int wave_count = 0;
        while (true) {
            while (!send_handles_.empty() && !recv_handles_.empty()) {
                poll(on_message);
            }
            std::pair<size_t, size_t> local_count = {send_count_, recv_count_};
            std::pair<size_t, size_t> reduced_count;
            MPI_Request reduce_request;
            MPI_Iallreduce(&local_count, &reduced_count, 2, kamping::mpi_type_traits<size_t>::data_type(), MPI_SUM,
                           MPI_COMM_WORLD, &reduce_request);
            wave_count++;
            int reduce_finished = false;
            while (!reduce_finished) {
                poll(on_message);
                if (reduce_request == MPI_REQUEST_NULL) {
                    throw "Error";
                }
                int err = MPI_Test(&reduce_request, &reduce_finished, MPI_STATUS_IGNORE);
                if (err != MPI_SUCCESS) {
                    throw "Error";
                }
            }
            if (rank_ == 0) {
                // atomic_debug("Wave count " + std::to_string(wave_count));
            }
            if (reduced_count == global_count && global_count.first == global_count.second) {
                break;
            } else {
                global_count = reduced_count;
            }
        }
    }

private:
    enum class TerminationState { active, first_wave, first_wave_success, second_wave, terminated };
    std::vector<SendHandlePtr<T>> send_handles_;
    std::vector<ReceiveHandlePtr<T>> recv_handles_;
    std::vector<SendHandlePtr<size_t>> control_send_handles_;
    std::vector<ReceiveHandlePtr<size_t>> control_recv_handles_;
    size_t send_count_ = 0;
    size_t recv_count_ = 0;
    int static const control_wave_tag = 478;
    size_t request_id_;
    PEID rank_;
    PEID size_;
    TerminationState termination_state = TerminationState::active;
    size_t number_of_waves = 0;
};
