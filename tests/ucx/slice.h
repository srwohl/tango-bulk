// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_TESTS_UCX_SLICE_H
#define TANGO_BULK_TESTS_UCX_SLICE_H

#include <ucx/subscriber_engine.h>

#include <core/delivery_queue.h>
#include <core/publisher_internal.h>
#include <core/cpu_topology.h>

#include <tango-bulk/publisher.h>

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <thread>
#include <utility>
#include <vector>

/// Scaffolding shared by the UCX tests: configuration, the `Open` handshake,
/// and the two waits every case needs.
///
/// It lives in a header rather than being copied because both test files drive
/// the same handshake, and a second copy of it would be a second thing to keep
/// in step with 4.1 and 4.2.
namespace TangoBulkTests
{

using namespace TangoBulk;
using namespace std::chrono_literals;

constexpr std::uint64_t k_frame_bytes = 256u << 10; ///< comfortably rendezvous-sized
constexpr std::uint32_t k_ring_depth = 8;
constexpr std::uint32_t k_credit_window = 4;

inline PublisherConfig publisher_config()
{
    PublisherConfig config;
    config.stream_name = "bulk.slice";
    config.max_frame_bytes = k_frame_bytes;
    config.ring_depth = k_ring_depth;
    config.credit_window = k_credit_window;
    config.publish_queue_depth = 8;
    return config;
}

inline SubscriberConfig subscriber_config()
{
    SubscriberConfig config;
    config.stream_name = "bulk.slice";
    config.max_frame_bytes = k_frame_bytes;
    config.ring_depth = k_ring_depth;
    config.credit_window = k_credit_window;
    config.delivery_queue_depth = 32;
    // No dispatch thread yet, so asking for one is a construction error rather
    // than a silent upgrade.
    config.delivery_mode = DeliveryMode::Manual;
    return config;
}

/// Spin until `predicate` holds or the budget runs out; returns whether it held.
template <typename Predicate>
bool eventually(Predicate predicate, std::chrono::milliseconds budget = 5s)
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

inline std::shared_ptr<detail::DeliveryQueue> queue_for(const SubscriberConfig &config)
{
    return std::make_shared<detail::DeliveryQueue>(config.delivery_queue_depth,
                                                   config.drop_policy);
}

struct Subscriber
{
    SubscriberConfig config;
    std::shared_ptr<detail::DeliveryQueue> delivery;
    detail::SubscriberEngine engine;

    explicit Subscriber(SubscriberConfig cfg = subscriber_config()) :
        config(std::move(cfg)),
        delivery(queue_for(config)),
        engine(config, delivery)
    {
    }

    std::vector<std::byte> make_open_request(std::uint64_t correlation_id)
    {
        Protocol::OpenRequest request;
        request.version_min = Protocol::k_version_major;
        request.version_max = Protocol::k_version_major;
        request.requested_caps = Protocol::k_caps_credit_coalescing | Protocol::k_caps_probe;
        request.client_instance_id = Protocol::generate_client_instance_id();
        request.requested_max_frame_bytes = config.max_frame_bytes;
        request.requested_ring_depth = config.ring_depth;
        request.requested_credit_window = config.credit_window;
        request.requested_memory_kind = config.receive_memory_kind;
        request.requested_transport = Protocol::Transport::ActiveMessage;
        request.drop_policy = config.drop_policy;
        request.stream_name = config.stream_name;
        request.client_ucx_address = engine.local_address();
        return Protocol::encode(request, correlation_id);
    }

    Status adopt_open_reply(const std::byte *data, std::size_t size)
    {
        Protocol::Envelope envelope;
        if(Protocol::decode_envelope(data, size, envelope) != Status::Ok)
        {
            return Status::MalformedMessage;
        }

        if(envelope.msg_type == Protocol::CoordType::Error)
        {
            Protocol::ErrorMessage error;
            const Status decoded = Protocol::decode(data, size, error);
            return decoded == Status::Ok ? error.status : Status::MalformedMessage;
        }

        Protocol::OpenReply reply;
        if(Protocol::decode(data, size, reply) != Status::Ok)
        {
            return Status::MalformedMessage;
        }
        if(reply.status != Status::Ok)
        {
            return reply.status;
        }
        if(reply.geometry.validate() != Status::Ok ||
           reply.geometry.max_frame_bytes > config.max_frame_bytes ||
           reply.geometry.ring_depth > config.ring_depth)
        {
            return Status::GeometryMismatch;
        }

        session_id_ = reply.session_id;
        stream_id_ = reply.stream_id;
        server_address_ = reply.server_ucx_address;
        granted_ = reply.geometry;
        lease_ttl_ms_ = reply.lease_ttl_ms;
        renew_interval_ms_ = reply.renew_interval_ms;

        return engine.activate(stream_id_, granted_, server_address_);
    }

    std::vector<std::byte> make_renew_request(std::uint64_t correlation_id)
    {
        Protocol::RenewRequest request;
        request.session_id = session_id_;
        request.client_frames_delivered = delivery->stats().taken;
        request.client_credits_returned = engine.counters().credits_returned;
        request.client_state = static_cast<std::uint32_t>(engine.state());
        return Protocol::encode(request, correlation_id);
    }

    Status adopt_renew_reply(const std::byte *data, std::size_t size)
    {
        Protocol::Envelope envelope;
        if(Protocol::decode_envelope(data, size, envelope) != Status::Ok)
        {
            last_renew_lost_ = false;
            return Status::MalformedMessage;
        }
        if(envelope.msg_type == Protocol::CoordType::Error)
        {
            Protocol::ErrorMessage error;
            const Status decoded = Protocol::decode(data, size, error);
            last_renew_lost_ = false;
            return decoded == Status::Ok ? error.status : Status::MalformedMessage;
        }

        Protocol::RenewReply reply;
        if(Protocol::decode(data, size, reply) != Status::Ok)
        {
            last_renew_lost_ = false;
            return Status::MalformedMessage;
        }
        if(reply.status != Status::Ok)
        {
            last_renew_lost_ = reply.status != Status::RenewTooFrequent;
            return reply.status;
        }

        lease_ttl_ms_ = reply.lease_ttl_ms;
        renew_interval_ms_ = reply.renew_interval_ms;
        last_renew_lost_ = false;
        return Status::Ok;
    }

    std::vector<std::byte> make_close_request(std::uint64_t correlation_id) const
    {
        Protocol::CloseRequest request;
        request.session_id = session_id_;
        request.reason = Protocol::CloseReason::ClientShutdown;
        return Protocol::encode(request, correlation_id);
    }

    const Protocol::GeometryBlock &granted_geometry() const noexcept
    {
        return granted_;
    }

    std::uint32_t granted_ring_depth() const noexcept
    {
        return granted_.ring_depth;
    }

    bool last_renew_lost() const noexcept
    {
        return last_renew_lost_;
    }

  private:
    Protocol::SessionId session_id_{};
    Protocol::StreamId stream_id_{0};
    std::vector<std::byte> server_address_;
    Protocol::GeometryBlock granted_{};
    std::uint32_t lease_ttl_ms_{0};
    std::uint32_t renew_interval_ms_{0};
    bool last_renew_lost_{false};
};

/// Run the `Open` exchange through the internal encoded coordination adapter.
///
/// No Tango process, no DeviceProxy, no commands -- the bytes are the real
/// protocol bytes and only the transport carrying them is short-circuited.
/// Returns the reply so a caller can inspect the grant.
inline std::vector<std::byte> exchange_open(BulkPublisher &publisher,
                                            Subscriber &subscriber,
                                            std::uint64_t correlation_id = 1)
{
    const std::vector<std::byte> request = subscriber.make_open_request(correlation_id);
    return detail::PublisherAccess::coordination(publisher, request.data(), request.size());
}

/// Wait out the `Probe`/`ProbeAck` round trip on both sides.
///
/// 4.2 arms a session on `ProbeAck`, not on `Open`, so a session is granted a
/// moment before it is eligible for a frame.  `armed_target` is how many armed
/// sessions the publisher should end up with.
inline bool await_armed(BulkPublisher &publisher,
                        Subscriber &subscriber,
                        std::size_t armed_target = 1)
{
    return eventually([&] { return publisher.session_count() == armed_target; }) &&
           eventually([&] { return subscriber.engine.state() == SubscriberState::Active; });
}

/// Open a session and wait until frames may flow.
inline void open_session(BulkPublisher &publisher,
                         Subscriber &subscriber,
                         std::size_t armed_target = 1)
{
    const std::vector<std::byte> reply = exchange_open(publisher, subscriber);
    REQUIRE(subscriber.adopt_open_reply(reply.data(), reply.size()) == Status::Ok);
    REQUIRE(await_armed(publisher, subscriber, armed_target));
}

inline FrameMetadata meta_for(std::uint64_t payload_bytes, std::uint64_t counter)
{
    FrameMetadata meta;
    meta.element_type = ElementType::UInt8;
    meta.rank = 1;
    meta.shape[0] = payload_bytes;
    meta.event_counter = counter;
    meta.quality = 7;
    return meta;
}

/// A pattern that depends on both the seed and the offset, so a frame delivered
/// from the wrong slot or truncated mid-payload does not accidentally match.
inline void fill(const BulkPublisher::SlotHandle &lease, std::uint64_t bytes, unsigned seed)
{
    auto *p = static_cast<unsigned char *>(lease.data());
    for(std::uint64_t i = 0; i < bytes; ++i)
    {
        p[i] = static_cast<unsigned char>((seed * 31u + static_cast<unsigned>(i)) & 0xFFu);
    }
}

inline bool payload_matches(const FrameView &view, unsigned seed)
{
    const auto *p = reinterpret_cast<const unsigned char *>(view.data());
    for(std::size_t i = 0; i < view.size(); ++i)
    {
        if(p[i] != static_cast<unsigned char>((seed * 31u + static_cast<unsigned>(i)) & 0xFFu))
        {
            return false;
        }
    }
    return true;
}

inline PublishResult publish_one(BulkPublisher &publisher, std::uint64_t bytes, unsigned seed)
{
    BulkPublisher::SlotHandle lease = publisher.try_acquire();
    REQUIRE(lease);
    fill(lease, bytes, seed);
    return publisher.publish(std::move(lease), meta_for(bytes, seed));
}

inline std::size_t drain(detail::DeliveryQueue &delivery,
                         std::chrono::milliseconds timeout,
                         const FrameCallback &cb,
                         std::size_t max_frames = 0)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    std::size_t dispatched = 0;

    while(max_frames == 0 || dispatched < max_frames)
    {
        FrameView view;
        const bool got =
            dispatched == 0 ? delivery.take(view, deadline) : delivery.try_take(view);

        if(!got)
        {
            break;
        }

        ++dispatched;
        if(cb)
        {
            cb(std::move(view));
        }
        view.reset();
    }

    return dispatched;
}

/// Poll until `want` frames have been delivered, or the budget runs out.
///
/// The returned views are *retained*, which means their credits are withheld.
/// That is the point in most cases; where it is not, the caller clears them.
inline std::vector<FrameView> collect(detail::DeliveryQueue &delivery,
                                      std::size_t want,
                                      std::chrono::milliseconds budget = 5s)
{
    std::vector<FrameView> views;
    const auto deadline = std::chrono::steady_clock::now() + budget;

    while(views.size() < want && std::chrono::steady_clock::now() < deadline)
    {
        drain(delivery, 10ms, [&views](FrameView view) { views.push_back(std::move(view)); });
    }

    return views;
}

/// One publisher and one subscriber, opened and armed, in this process.
struct Slice
{
    BulkPublisher publisher;
    Subscriber subscriber;

    explicit Slice(PublisherConfig pub = publisher_config(),
                   SubscriberConfig sub = subscriber_config()) :
        publisher(std::move(pub)),
        subscriber(std::move(sub))
    {
        open_session(publisher, subscriber);
    }

    /// Close the session before either side is destroyed.
    ///
    /// Not tidiness.  Members die in reverse declaration order, so without this
    /// the subscriber's worker and receive ring would go away while the
    /// publisher still had frames in flight to them.  `Close` runs 4.2's
    /// teardown in its mandated order: stop submitting, drain outstanding
    /// operations, close the endpoint, and only then release the slots those
    /// operations were reading from.
    ~Slice()
    {
        const std::vector<std::byte> request = subscriber.make_close_request(2);
        detail::PublisherAccess::coordination(publisher, request.data(), request.size());
    }

    Slice(const Slice &) = delete;
    Slice &operator=(const Slice &) = delete;

    PublishResult publish(std::uint64_t bytes, unsigned seed)
    {
        return publish_one(publisher, bytes, seed);
    }
};

} // namespace TangoBulkTests

#endif // TANGO_BULK_TESTS_UCX_SLICE_H
