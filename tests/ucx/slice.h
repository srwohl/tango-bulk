// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_TESTS_UCX_SLICE_H
#define TANGO_BULK_TESTS_UCX_SLICE_H

#include <core/publisher_internal.h>
#include <core/subscription_internal.h>

#include <tango-bulk/publisher.h>
#include <tango-bulk/subscription.h>

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

/// Shared UCX integration-test scaffolding. The subscriber crosses the same
/// Subscription seam as production; only coordination is short-circuited to
/// the in-process publisher.
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
    config.delivery_mode = DeliveryMode::Pull;
    return config;
}

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

using CoordinationObserver =
    std::function<void(Protocol::CoordType, const std::vector<std::byte> &)>;
using CoordinationReplyTransform =
    std::function<void(Protocol::CoordType, std::vector<std::byte> &)>;

inline std::unique_ptr<Subscription> open_subscription(
    BulkPublisher &publisher,
    SubscriberConfig config = subscriber_config(),
    CoordinationObserver observer = {},
    CoordinationReplyTransform transform = {})
{
    config.delivery_mode = DeliveryMode::Pull;
    return detail::SubscriptionFactory::open_default(
        std::move(config),
        [&publisher, observer = std::move(observer), transform = std::move(transform)](
            Protocol::CoordType type,
            const std::vector<std::byte> &request,
            std::chrono::steady_clock::time_point /*deadline*/) mutable
        {
            std::vector<std::byte> reply = detail::PublisherAccess::coordination(
                publisher, request.data(), request.size(), type);
            if(transform)
            {
                transform(type, reply);
            }
            if(observer)
            {
                observer(type, reply);
            }
            return reply;
        },
        SubscriptionCallbacks{});
}

inline std::size_t drain(Subscription &subscription,
                         std::chrono::milliseconds timeout,
                         const FrameCallback &callback,
                         std::size_t max_frames = 0)
{
    std::size_t drained = 0;
    while(max_frames == 0 || drained < max_frames)
    {
        std::optional<FrameView> frame = drained == 0 ? subscription.read_for(timeout)
                                                       : subscription.try_read();
        if(!frame)
        {
            break;
        }

        ++drained;
        if(callback)
        {
            callback(std::move(*frame));
        }
    }

    return drained;
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

inline std::vector<FrameView> collect(Subscription &subscription,
                                      std::size_t want,
                                      std::chrono::milliseconds budget = 5s)
{
    std::vector<FrameView> views;
    const auto deadline = std::chrono::steady_clock::now() + budget;

    while(views.size() < want && std::chrono::steady_clock::now() < deadline)
    {
        drain(subscription, 10ms, [&views](FrameView view) { views.push_back(std::move(view)); });
    }

    return views;
}

/// One publisher and one Pull Subscription, established through the real UCX
/// transport and closed by Subscription destruction.
struct Slice
{
    BulkPublisher publisher;
    std::unique_ptr<Subscription> subscription;

    explicit Slice(PublisherConfig pub = publisher_config(),
                   SubscriberConfig sub = subscriber_config()) :
        publisher(std::move(pub)),
        subscription(open_subscription(publisher, std::move(sub)))
    {
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
