// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_TESTS_UCX_SLICE_H
#define TANGO_BULK_TESTS_UCX_SLICE_H

#include <ucx/subscriber_engine.h>

#include <core/delivery_queue.h>
#include <core/session_client.h>
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

/// The delivery queue a subscription would have created for this engine.
///
/// A transport no longer owns its queue: the subscription does, so that it
/// survives a reconnect and so that taking a frame never reaches through the
/// transport. A test driving an engine directly is therefore standing in for
/// the subscription, and it says so by holding the queue itself -- one named
/// variable per engine, rather than a helper that would hide the very ownership
/// these tests exist to exercise.
inline std::shared_ptr<detail::DeliveryQueue> queue_for(const SubscriberConfig &config)
{
    return std::make_shared<detail::DeliveryQueue>(config.delivery_queue_depth,
                                                   config.drop_policy);
}

/// One subscriber, assembled the way a subscription assembles one.
///
/// A transport owns neither of these any more. The delivery queue belongs to
/// the subscription so that it survives a reconnect, and the session contract
/// -- the grant, the lease terms, the identifiers `Renew` and `Close` quote --
/// belongs to it too, so that a grant is settled and checked *before* a
/// transport is started on it. A test driving an engine directly is standing in
/// for the subscription, and this is what that costs: two members and the open
/// handshake spelled out.
struct Subscriber
{
    SubscriberConfig config;
    std::shared_ptr<detail::DeliveryQueue> delivery;
    detail::SessionClient session;
    detail::SubscriberEngine engine;

    explicit Subscriber(SubscriberConfig cfg = subscriber_config()) :
        config(std::move(cfg)),
        delivery(queue_for(config)),
        engine(config, delivery)
    {
    }

    std::vector<std::byte> make_open_request(std::uint64_t correlation_id)
    {
        return session.make_open_request(config, engine.local_address(), correlation_id);
    }

    /// Adopt the grant, then start the transport on it -- in that order, which
    /// is the ordering the seam now enforces rather than merely documents.
    Status adopt_open_reply(const std::byte *data, std::size_t size)
    {
        const Status status = session.adopt_open_reply(data, size, config);
        if(status != Status::Ok)
        {
            return status;
        }

        return engine.activate(
            session.stream_id(), session.granted_geometry(), session.server_address());
    }

    std::vector<std::byte> make_renew_request(std::uint64_t correlation_id)
    {
        return session.make_renew_request(correlation_id,
                                          delivery->stats().taken,
                                          engine.counters().credits_returned,
                                          static_cast<std::uint32_t>(engine.state()));
    }

    Status adopt_renew_reply(const std::byte *data, std::size_t size)
    {
        return session.adopt_renew_reply(data, size).status;
    }

    std::vector<std::byte> make_close_request(std::uint64_t correlation_id) const
    {
        return session.make_close_request(correlation_id);
    }
};

/// Take up to `max_frames` within `timeout`, invoking `cb` on this thread.
///
/// What `SubscriberEngine::poll()` used to be, as a free function over the
/// queue -- which is where the frames are now.
/// Run the `Open` exchange, straight through `handle_coordination`.
///
/// No Tango process, no DeviceProxy, no commands -- the bytes are the real
/// protocol bytes and only the transport carrying them is short-circuited.
/// Returns the reply so a caller can inspect the grant.
inline std::vector<std::byte> exchange_open(BulkPublisher &publisher,
                                            Subscriber &subscriber,
                                            std::uint64_t correlation_id = 1)
{
    const std::vector<std::byte> request = subscriber.make_open_request(correlation_id);
    return publisher.handle_coordination(request.data(), request.size());
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
inline void fill(const BulkSource::Lease &lease, std::uint64_t bytes, unsigned seed)
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
    BulkSource::Lease lease = publisher.source().try_acquire();
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
        publisher.handle_coordination(request.data(), request.size());
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
