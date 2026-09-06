// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_TESTS_UNIT_FAKE_TRANSPORT_H
#define TANGO_BULK_TESTS_UNIT_FAKE_TRANSPORT_H

#include <core/delivery_queue.h>

#include <tango-bulk/subscription.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

namespace TangoBulkTests
{

using namespace TangoBulk;
using namespace std::chrono_literals;

struct ScriptedFrame
{
    std::shared_ptr<std::vector<std::uint16_t>> payload;
    FrameView::Fields fields;

    const std::byte *bytes() const noexcept
    {
        return reinterpret_cast<const std::byte *>(payload->data());
    }
};

struct Script
{
    std::mutex mutex;

    std::deque<Status> open_results;

    std::deque<Status> renew_results;

    bool probe_arrives{true};

    bool reshape_after_first{false};

    int channel_throws{0};

    bool refuse_with_error_message{false};

    int garble_replies{0};

    Protocol::SessionId session_id{{std::byte{0xA1}, std::byte{0xB2}, std::byte{0xC3}}};
    Protocol::StreamId stream_id{0x5EED};
    std::uint32_t lease_ttl_ms{200};
    std::uint32_t renew_interval_ms{20};
    std::vector<std::byte> server_address{std::byte{9}, std::byte{8}, std::byte{7}};

    std::atomic<int> transports_built{0};

    std::atomic<int> activations{0};
    std::atomic<int> opens{0};
    std::atomic<int> renews{0};
    std::atomic<int> closes{0};

    std::set<std::thread::id> channel_threads;
    std::atomic<int> channel_inside{0};
    std::atomic<int> channel_max_concurrent{0};

    void refuse_next_renew(Status status)
    {
        std::lock_guard<std::mutex> lock(mutex);
        renew_results.push_back(status);
    }

    bool probes()
    {
        std::lock_guard<std::mutex> lock(mutex);
        return probe_arrives;
    }

    int grants_made{0};


    std::vector<std::byte> open_reply(std::uint64_t correlation_id)
    {
        std::lock_guard<std::mutex> lock(mutex);

        if(garble_replies > 0)
        {
            --garble_replies;
            return {std::byte{0xDE}, std::byte{0xAD}};
        }

        const Status status = next_status(open_results);
        if(status != Status::Ok && refuse_with_error_message)
        {
            Protocol::ErrorMessage error;
            error.status = status;
            error.message = "the fake refused the open";
            return Protocol::encode(error, correlation_id);
        }

        Protocol::OpenReply reply;
        reply.status = status;
        reply.session_id = session_id;
        reply.stream_id = stream_id;
        reply.lease_ttl_ms = lease_ttl_ms;
        reply.renew_interval_ms = renew_interval_ms;
        reply.geometry = grant_locked();
        reply.server_ucx_address = server_address;
        return Protocol::encode(reply, correlation_id);
    }

    std::vector<std::byte> renew_reply(std::uint64_t correlation_id)
    {
        std::lock_guard<std::mutex> lock(mutex);

        const Status status = next_status(renew_results);
        if(status != Status::Ok && refuse_with_error_message)
        {
            Protocol::ErrorMessage error;
            error.status = status;
            error.message = "the fake refused the renewal";
            return Protocol::encode(error, correlation_id);
        }

        Protocol::RenewReply reply;
        reply.session_id = session_id;
        reply.status = status;

        reply.lease_ttl_ms = lease_ttl_ms;
        reply.renew_interval_ms = renew_interval_ms;
        reply.geometry = last_grant;
        return Protocol::encode(reply, correlation_id);
    }

    std::vector<std::byte> close_reply(std::uint64_t correlation_id)
    {
        std::lock_guard<std::mutex> lock(mutex);

        Protocol::CloseReply reply;
        reply.session_id = session_id;
        reply.status = Status::Ok;
        return Protocol::encode(reply, correlation_id);
    }

    Protocol::GeometryBlock grant_locked()
    {
        const bool reshaped = reshape_after_first && grants_made > 0;
        ++grants_made;

        Protocol::GeometryBlock granted;
        granted.generation = 1;
        granted.element_type = ElementType::UInt16;
        granted.element_size = 2;
        granted.rank = 2;
        granted.max_frame_bytes = 64u << 10;
        granted.ring_depth = 4;
        granted.credit_window = 2;
        granted.shape = reshaped ? std::array<std::uint64_t, k_max_rank>{4, 16, 0, 0}
                                 : std::array<std::uint64_t, k_max_rank>{8, 8, 0, 0};
        granted.strides = reshaped ? std::array<std::uint64_t, k_max_rank>{32, 2, 0, 0}
                                   : std::array<std::uint64_t, k_max_rank>{16, 2, 0, 0};

        last_grant = granted;
        return granted;
    }

    Protocol::GeometryBlock last_grant{};

    static Status next_status(std::deque<Status> &scripted)
    {
        if(scripted.empty())
        {
            return Status::Ok;
        }
        const Status status = scripted.front();
        scripted.pop_front();
        return status;
    }


    std::shared_ptr<detail::DeliveryQueue> delivery;

    std::deque<ScriptedFrame> staged;

    void attach(std::shared_ptr<detail::DeliveryQueue> queue)
    {
        std::lock_guard<std::mutex> lock(mutex);
        delivery = std::move(queue);

        for(ScriptedFrame &frame : staged)
        {
            deliver_locked(frame);
        }
        staged.clear();
    }

    void receive(std::uint64_t sequence)
    {
        ScriptedFrame frame;
        frame.payload = std::make_shared<std::vector<std::uint16_t>>(
            8 * 8, static_cast<std::uint16_t>(sequence));

        frame.fields.element_type = ElementType::UInt16;
        frame.fields.element_size = 2;
        frame.fields.rank = 2;
        frame.fields.shape = {8, 8, 0, 0};
        frame.fields.strides = {16, 2, 0, 0};
        frame.fields.payload_bytes = 8 * 8 * sizeof(std::uint16_t);
        frame.fields.sequence = sequence;
        frame.fields.event_counter = sequence;
        frame.fields.generation = 1;

        std::lock_guard<std::mutex> lock(mutex);
        if(delivery)
        {
            deliver_locked(frame);
        }
        else
        {
            staged.push_back(std::move(frame));
        }
    }

  private:
    void deliver_locked(ScriptedFrame &frame)
    {
        delivery->push(FrameView::detached(frame.payload, frame.bytes(), frame.fields));
    }
};

class FakeTransport final : public detail::SubscriberTransport
{
  public:
    FakeTransport(Script &script, std::shared_ptr<detail::DeliveryQueue> delivery) :
        script_(script)
    {
        script_.transports_built.fetch_add(1, std::memory_order_relaxed);
        script_.attach(std::move(delivery));
    }


    const std::vector<std::byte> &local_address() const noexcept override
    {
        return k_client_address;
    }

    Status activate(Protocol::StreamId,
                    const Protocol::GeometryBlock &,
                    const std::vector<std::byte> &) override
    {
        script_.activations.fetch_add(1, std::memory_order_relaxed);

        state_.store(script_.probes() ? SubscriberState::Active : SubscriberState::Probing,
                     std::memory_order_release);
        return Status::Ok;
    }

    BulkError last_error() const noexcept override
    {
        return failed_ ? BulkError{Status::TransportFailure, "the fake was told to fail", "transport"}
                       : BulkError{};
    }

    SubscriberState state() const noexcept override
    {
        return state_.load(std::memory_order_acquire);
    }

    SubscriberCounters counters() const noexcept override
    {
        SubscriberCounters out;
        out.sessions_opened = 1;
        return out;
    }

    void fail() noexcept
    {
        failed_ = true;
        state_.store(SubscriberState::Failed, std::memory_order_release);
    }

  private:
    static inline const std::vector<std::byte> k_client_address{
        std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};

    Script &script_;
    std::atomic<SubscriberState> state_{SubscriberState::Opening};
    std::atomic<bool> failed_{false};
};

inline detail::TransportFactory fake_factory(Script &script,
                                             std::shared_ptr<FakeTransport *> latest = nullptr)
{
    return [&script, latest](const SubscriberConfig &,
                             std::shared_ptr<detail::DeliveryQueue> delivery)
        -> std::unique_ptr<detail::SubscriberTransport>
    {
        auto transport = std::make_unique<FakeTransport>(script, std::move(delivery));
        if(latest)
        {
        }
        return transport;
    };
}

inline detail::CoordinationChannel fake_channel(Script &script)
{
    return [&script](Protocol::CoordType kind,
                     const std::vector<std::byte> &request) -> std::vector<std::byte>
    {
        const int inside = script.channel_inside.fetch_add(1, std::memory_order_acq_rel) + 1;
        int observed = script.channel_max_concurrent.load(std::memory_order_relaxed);
        while(inside > observed &&
              !script.channel_max_concurrent.compare_exchange_weak(observed, inside))
        {
        }
        struct Leave
        {
            Script &script;
            ~Leave()
            {
                script.channel_inside.fetch_sub(1, std::memory_order_acq_rel);
            }
        } leave{script};

        {
            std::lock_guard<std::mutex> lock(script.mutex);
            script.channel_threads.insert(std::this_thread::get_id());
            if(script.channel_throws > 0)
            {
                --script.channel_throws;
                throw BulkException(
                    BulkError{Status::TransportFailure, "the device is unreachable", "tango"});
            }
        }

        std::this_thread::sleep_for(std::chrono::microseconds{200});

        Protocol::Envelope envelope;
        if(Protocol::decode_envelope(request.data(), request.size(), envelope) != Status::Ok)
        {
            throw BulkException(BulkError{
                Status::MalformedMessage, "the client sent something undecodable", "tango"});
        }

        switch(kind)
        {
        case Protocol::CoordType::Open:
            script.opens.fetch_add(1, std::memory_order_relaxed);
            return script.open_reply(envelope.correlation_id);
        case Protocol::CoordType::Renew:
            script.renews.fetch_add(1, std::memory_order_relaxed);
            return script.renew_reply(envelope.correlation_id);
        case Protocol::CoordType::Close:
            script.closes.fetch_add(1, std::memory_order_relaxed);
            return script.close_reply(envelope.correlation_id);
        default:
            break;
        }

        Protocol::ErrorMessage unexpected;
        unexpected.status = Status::Internal;
        unexpected.message = "the fake was asked for a message it does not serve";
        return Protocol::encode(unexpected, envelope.correlation_id);
    };
}

inline SubscriberConfig subscription_config()
{
    SubscriberConfig config;
    config.stream_name = "bulk.unit";
    config.max_frame_bytes = 64u << 10;
    config.ring_depth = 4;
    config.credit_window = 2;
    config.delivery_queue_depth = 8;
    config.delivery_mode = DeliveryMode::Manual;
    config.reconnect_backoff_ms = 5;
    config.reconnect_max_attempts = 3;
    config.command_timeout_ms = 500;
    config.probe_timeout_ms = 60;
    return config;
}

inline SubscriptionCallbacks noop_callbacks()
{
    SubscriptionCallbacks callbacks;
    callbacks.on_frame = [](FrameView) {};
    callbacks.on_state = [](SubscriberState, const BulkError &) {};
    return callbacks;
}

template <typename Predicate>
bool eventually(Predicate predicate, std::chrono::milliseconds budget = 4s)
{
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while(std::chrono::steady_clock::now() < deadline)
    {
        if(predicate())
        {
            return true;
        }
        std::this_thread::sleep_for(1ms);
    }
    return predicate();
}

} // namespace TangoBulkTests

#endif // TANGO_BULK_TESTS_UNIT_FAKE_TRANSPORT_H
