// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include "slice.h"
#include "transport_fixture.h"

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <vector>

using namespace TangoBulk;
using namespace TangoBulkTests;

namespace
{

SubscriptionOptions copied_options()
{
    SubscriptionOptions options = subscription_options();
    options.ownership = DeliveryOwnership::Copy;
    return options;
}

/// Publish one frame, retrying while the publisher's slots or queue are
/// momentarily full: copied delivery returns credit on its own schedule.
void publish_eventually(BulkPublisher &publisher, std::uint64_t bytes, unsigned seed)
{
    REQUIRE(eventually(
        [&]
        {
            BulkPublisher::SlotHandle lease = publisher.try_acquire();
            if(!lease)
            {
                return false;
            }
            fill(lease, bytes, seed);
            return publisher.publish(std::move(lease), meta_for(bytes, seed)) ==
                   PublishResult::Accepted;
        }));
}

} // namespace

TEST_CASE("A copied frame lies outside the registered ring and returns its credit at delivery",
          "[ucx][copy]")
{
    BulkPublisher publisher(publisher_config());
    TransportFixture transport(copied_options());
    transport.open(publisher);

    REQUIRE(publish_one(publisher, k_frame_bytes, 0x21) == PublishResult::Accepted);
    const std::vector<FrameView> views = collect_transport(*transport.delivery, 1);
    REQUIRE(views.size() == 1);

    const FrameView &view = views.front();
    CHECK_FALSE(view.borrowed());
    CHECK_FALSE(in_receive_ring(transport.engine, view.data()));
    CHECK(view.size() == k_frame_bytes);
    CHECK(view.sequence() == 0);
    CHECK(view.event_counter() == 0x21);
    CHECK(payload_matches(view, 0x21));

    // The application still holds the frame; the publisher has its slot back.
    CHECK(eventually([&] { return publisher.counters().frames_credited == 1; }));

    const SubscriberCounters counters = transport.engine.counters();
    CHECK(counters.frames_copied == 1);
    CHECK(counters.bytes_copied == k_frame_bytes);
    CHECK(counters.copy_pool_exhausted == 0);
    CHECK(counters.views_outstanding == 0);
    CHECK(counters.credits_returned == 1);
}

TEST_CASE("PreferFresh evicts the oldest queued copied frame", "[ucx][copy]")
{
    SubscriptionOptions options = copied_options();
    options.queue_policy = QueuePolicy::PreferFresh;
    Slice slice(publisher_config(), options);

    // The delivery queue holds ring_depth frames (ADR 0008). Nothing reads
    // while more than that arrive, so the oldest are evicted.
    constexpr std::size_t k_extra = 4;
    constexpr std::size_t k_total = k_ring_depth + k_extra;
    for(unsigned seed = 0; seed < k_total; ++seed)
    {
        publish_eventually(slice.publisher, 8192, seed);
    }
    REQUIRE(eventually(
        [&] { return slice.subscription->snapshot().counters.frames_received == k_total; }));

    const std::vector<FrameView> views = collect(*slice.subscription, k_ring_depth);
    REQUIRE(views.size() == k_ring_depth);
    for(std::size_t i = 0; i < views.size(); ++i)
    {
        CHECK(views[i].sequence() == k_extra + i);
        CHECK(payload_matches(views[i], static_cast<unsigned>(k_extra + i)));
    }
    CHECK_FALSE(slice.subscription->try_read());

    const SubscriberCounters counters = slice.subscription->snapshot().counters;
    CHECK(counters.frames_dropped_queue_full == k_extra);
    CHECK(counters.frames_copied == k_total);
}

TEST_CASE("Retaining every copied frame of a long run never stalls the publisher",
          "[ucx][copy]")
{
    Slice slice(publisher_config(), copied_options());

    // More frames than the copy pool holds, every one kept.
    constexpr std::size_t k_total = 4 * k_ring_depth;
    std::vector<FrameView> retained;
    for(unsigned seed = 0; seed < k_total; ++seed)
    {
        publish_eventually(slice.publisher, 8192, seed);
        std::optional<FrameView> frame = slice.subscription->read_for(5s);
        REQUIRE(frame);
        retained.push_back(std::move(*frame));
    }

    CHECK(eventually([&] { return slice.publisher.counters().frames_credited == k_total; }));
    CHECK(slice.publisher.counters().dropped_credit_stalled == 0);

    // The snapshot's transport counters are sampled on the control thread.
    CHECK(eventually(
        [&] { return slice.subscription->snapshot().counters.frames_copied == k_total; }));
    const SubscriberCounters counters = slice.subscription->snapshot().counters;
    CHECK(counters.copy_pool_exhausted > 0);
    CHECK(counters.views_outstanding == 0);

    for(unsigned seed = 0; seed < k_total; ++seed)
    {
        CHECK_FALSE(retained[seed].borrowed());
        CHECK(payload_matches(retained[seed], seed));
    }
}

TEST_CASE("Copied frames outlive the Subscription that delivered them", "[ucx][copy]")
{
    std::vector<FrameView> retained;
    {
        Slice slice(publisher_config(), copied_options());
        for(unsigned seed = 0; seed < 3; ++seed)
        {
            publish_eventually(slice.publisher, 8192, seed);
            std::optional<FrameView> frame = slice.subscription->read_for(5s);
            REQUIRE(frame);
            retained.push_back(std::move(*frame));
        }
    }

    for(unsigned seed = 0; seed < 3; ++seed)
    {
        CHECK(payload_matches(retained[seed], seed));
    }
}
