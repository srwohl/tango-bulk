// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include "slice.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <thread>
#include <vector>

/// IMPLEMENTATION_SPEC.md 9.3's exit criteria, one TEST_CASE each.
///
/// The list is deliberately short and deliberately closed.  This is the first
/// time the ownership rules in 5.4 and 5.5 are executed rather than written
/// down, and the value of these cases is that each one fails for exactly one
/// reason.  Breadth belongs in tests/unit, where it costs milliseconds and no
/// transport; every case here starts two UCX workers and pins memory.
///
/// Session lifecycle -- leases, renewal, expiry -- is 9.4/M3 and lives in
/// test_session_lifecycle.cpp.
namespace
{
using namespace TangoBulkTests;
} // namespace

TEST_CASE("A published frame arrives intact with its metadata", "[m2][slice]")
{
    Slice slice;
    const std::uint64_t bytes = 4096;

    REQUIRE(slice.publish(bytes, 0x5A) == PublishResult::Accepted);

    const std::vector<FrameView> views = collect(slice.subscriber, 1);
    REQUIRE(views.size() == 1);

    const FrameView &view = views.front();
    CHECK(view);
    CHECK(view.size() == bytes);
    CHECK(view.sequence() == 0);
    CHECK(view.event_counter() == 0x5A);
    CHECK(view.element_type() == ElementType::UInt8);
    CHECK(view.element_size() == 1);
    CHECK(view.rank() == 1);
    CHECK(view.shape()[0] == bytes);
    CHECK(view.quality() == 7);
    CHECK(view.memory_kind() == MemoryKind::Host);
    CHECK(view.endian() == Endian::Little);
    CHECK(view.dropped_before() == 0);

    // 2.2: a zero timestamp means "stamp at publish", so by the time it is on
    // the wire it must be a real one.
    CHECK(view.timestamp_ns() != 0);

    CHECK(payload_matches(view, 0x5A));
}

TEST_CASE("The delivered payload lives in the registered receive ring", "[m2][slice]")
{
    Slice slice;

    REQUIRE(slice.publish(k_frame_bytes, 0x11) == PublishResult::Accepted);

    const std::vector<FrameView> views = collect(slice.subscriber, 1);
    REQUIRE(views.size() == 1);
    const FrameView &view = views.front();

    // The pointer half of 9.3's zero-copy criterion: the address handed to the
    // application is inside the registered region, not in a staging buffer the
    // library copied out of.
    CHECK(slice.subscriber.ring_contains(view.data()));
    CHECK(view.data() == slice.subscriber.slot_address(0));

    // The registration half.  bytes_copied() counts only payload that went
    // through an eager staging copy; for a rendezvous-sized frame there must be
    // none, and no amount of reading the code substitutes for the count.
    CHECK(slice.subscriber.bytes_copied() == 0);
    CHECK(payload_matches(view, 0x11));
}

TEST_CASE("Slots recycle: sequence s lands in slot s % ring_depth", "[m2][slice]")
{
    Slice slice;
    const std::uint32_t depth = slice.subscriber.granted_ring_depth();
    REQUIRE(depth == k_ring_depth);

    const std::size_t total = 4u * depth;
    const std::uint64_t bytes = 8192;

    for(std::size_t n = 0; n < total; ++n)
    {
        const auto seed = static_cast<unsigned>(n);
        REQUIRE(slice.publish(bytes, seed) == PublishResult::Accepted);

        std::vector<FrameView> views = collect(slice.subscriber, 1);
        REQUIRE(views.size() == 1);

        const FrameView &view = views.front();
        CHECK(view.sequence() == n);
        CHECK(view.data() == slice.subscriber.slot_address(n % depth));
        CHECK(payload_matches(view, seed));

        // Released here, at the end of the iteration, which is what keeps the
        // credit window open for the next one.
        views.clear();
    }

    // Every producer slot came back.  A wraparound bug that leaked one slot per
    // lap shows up here and nowhere else.
    CHECK(eventually([&] { return slice.publisher.counters().frames_credited == total; }));
    CHECK(eventually([&] { return slice.publisher.source().retained() == 0; }));
}

TEST_CASE("Publisher uses the credit window negotiated for a smaller client ring",
          "[m2][slice]")
{
    PublisherConfig pub = publisher_config();
    pub.ring_depth = 8;
    pub.credit_window = 4;

    SubscriberConfig sub = subscriber_config();
    sub.ring_depth = 2;
    sub.credit_window = 2;

    Slice slice(pub, sub);
    REQUIRE(slice.subscriber.granted_ring_depth() == 2);

    // Fill beyond the client's negotiated window before polling.  BestEffort
    // may skip frames for this session, but it must not assign them sequences:
    // doing so sends 2 and 3 into slots still occupied by 0 and 1.
    for(unsigned seed = 0; seed < 4; ++seed)
        REQUIRE(slice.publish(4096, seed) == PublishResult::Accepted);

    std::vector<FrameView> first = collect(slice.subscriber, 2);
    REQUIRE(first.size() == 2);
    CHECK(first[0].sequence() == 0);
    CHECK(first[1].sequence() == 1);
    first.clear();
    REQUIRE(eventually([&] { return slice.publisher.counters().frames_credited == 2; }));

    // Once those two credits return, sequence assignment resumes contiguously.
    // A publisher still using its configured width of 4 produces 4 and 5 here,
    // leaving the subscriber's ReleaseTracker permanently waiting for 2.
    REQUIRE(slice.publish(4096, 4) == PublishResult::Accepted);
    REQUIRE(slice.publish(4096, 5) == PublishResult::Accepted);

    std::vector<FrameView> second = collect(slice.subscriber, 2);
    REQUIRE(second.size() == 2);
    CHECK(second[0].sequence() == 2);
    CHECK(second[1].sequence() == 3);
    second.clear();
    CHECK(eventually([&] { return slice.publisher.counters().frames_credited == 4; }));
}

TEST_CASE("A retained view withholds exactly one credit", "[m2][slice]")
{
    Slice slice;
    const std::uint64_t bytes = 4096;

    for(std::uint32_t n = 0; n < k_credit_window; ++n)
    {
        REQUIRE(slice.publish(bytes, n) == PublishResult::Accepted);
    }

    std::vector<FrameView> held = collect(slice.subscriber, k_credit_window);
    REQUIRE(held.size() == k_credit_window);
    CHECK(slice.subscriber.counters().views_outstanding == k_credit_window);

    // The window is full and every view is retained, so the publisher stalls.
    {
        BulkSource::Lease lease = slice.publisher.source().try_acquire();
        REQUIRE(lease);
        CHECK(slice.publisher.publish(std::move(lease), meta_for(bytes, 99)) ==
              PublishResult::CreditStalled);
    }

    CHECK(slice.publisher.counters().frames_credited == 0);

    // Release the oldest.  Exactly one credit returns -- not zero, and not the
    // whole window.
    held.erase(held.begin());

    REQUIRE(eventually([&] { return slice.publisher.counters().frames_credited == 1; }));
    CHECK(slice.subscriber.counters().views_outstanding == k_credit_window - 1);

    // ...and the publisher resumes, which is the other half of the criterion:
    // the stall must be a stall, not a wedge.
    REQUIRE(slice.publish(bytes, 100) == PublishResult::Accepted);
}

TEST_CASE("Out-of-order release advances the ack only across the contiguous prefix",
          "[m2][slice]")
{
    Slice slice;
    const std::uint64_t bytes = 4096;

    for(std::uint32_t n = 0; n < k_credit_window; ++n)
    {
        REQUIRE(slice.publish(bytes, n) == PublishResult::Accepted);
    }

    std::vector<FrameView> held = collect(slice.subscriber, k_credit_window);
    REQUIRE(held.size() == k_credit_window);
    REQUIRE(held[0].sequence() == 0);
    REQUIRE(held[1].sequence() == 1);

    // Release sequence 1 while 0 is still held.  Credit is cumulative (3.12), so
    // nothing may move: acking 1 here would tell the publisher that 0 was
    // released too, and 0 is exactly the slot still being read.
    held[1].reset();

    std::this_thread::sleep_for(100ms);
    CHECK(slice.publisher.counters().frames_credited == 0);

    // Releasing 0 closes the gap, and both are credited at once.
    held[0].reset();

    REQUIRE(eventually([&] { return slice.publisher.counters().frames_credited == 2; }));
    CHECK(slice.publisher.counters().frames_credited == 2);

    // One Credit message carried both releases: that ratio is the coalescing
    // measurement 2.5 asks for, not a log line.
    CHECK(slice.subscriber.counters().credit_messages_sent <=
          slice.subscriber.counters().credits_returned);
}

TEST_CASE("With every view retained, publish reports CreditStalled and never blocks",
          "[m2][slice]")
{
    Slice slice;
    const std::uint64_t bytes = 4096;

    // Deliberately never polled: the views sit in the delivery queue holding
    // their leases, so no credit is ever returned.
    std::uint32_t accepted = 0;
    std::uint32_t stalled = 0;

    for(std::uint32_t n = 0; n < k_ring_depth; ++n)
    {
        BulkSource::Lease lease = slice.publisher.source().try_acquire();
        REQUIRE(lease);
        fill(lease, bytes, n);

        const PublishResult result = slice.publisher.publish(std::move(lease), meta_for(bytes, n));
        if(result == PublishResult::Accepted)
        {
            ++accepted;
        }
        else
        {
            CHECK(result == PublishResult::CreditStalled);
            ++stalled;
        }
    }

    // The window, and nothing else, is what bounded it.
    CHECK(accepted == k_credit_window);
    CHECK(stalled == k_ring_depth - k_credit_window);

    const PublisherCounters counters = slice.publisher.counters();
    CHECK(counters.dropped_credit_stalled == stalled);
    CHECK(counters.frames_credited == 0);

    // No slot was reused: every accepted frame still holds its own, and every
    // stalled frame gave its slot straight back.
    CHECK(counters.leases_retained == accepted);
    CHECK(slice.publisher.source().retained() == accepted);
}

TEST_CASE("A full publish queue returns QueueFull and leaves the lease usable", "[m2][slice]")
{
    // Sized so the application thread can outrun the engine: a wide credit
    // window so admission control does not intervene first, and the shallowest
    // publish queue the spec allows.  Without that gap this criterion is not
    // reachable at all, because CreditStalled is checked before the queue.
    PublisherConfig pub = publisher_config();
    pub.ring_depth = 64;
    pub.credit_window = 64;
    pub.publish_queue_depth = 8;

    SubscriberConfig sub = subscriber_config();
    sub.ring_depth = 64;
    sub.credit_window = 64;

    Slice slice(pub, sub);

    // Fill the leases first, so the publish burst is nothing but publish calls.
    std::vector<BulkSource::Lease> leases;
    for(std::uint32_t n = 0; n < 64; ++n)
    {
        BulkSource::Lease lease = slice.publisher.source().try_acquire();
        REQUIRE(lease);
        fill(lease, k_frame_bytes, n);
        leases.push_back(std::move(lease));
    }

    bool saw_queue_full = false;

    for(std::size_t n = 0; n < leases.size(); ++n)
    {
        const void *before = leases[n].data();
        const std::size_t index_before = leases[n].index();

        const PublishResult result =
            slice.publisher.publish(std::move(leases[n]), meta_for(k_frame_bytes, n));

        if(result != PublishResult::QueueFull)
        {
            continue;
        }

        saw_queue_full = true;

        // 5.4: "publish() returning QueueFull does not consume the lease; the
        // caller still owns it and may retry or drop it."  Still engaged, still
        // the same slot, still the bytes it was filled with.
        REQUIRE(leases[n]);
        CHECK(leases[n].data() == before);
        CHECK(leases[n].index() == index_before);
        CHECK(leases[n].capacity() == k_frame_bytes);

        // And usable: a retry once the engine has drained must be accepted.
        REQUIRE(eventually(
            [&]
            {
                return slice.publisher.publish(std::move(leases[n]), meta_for(k_frame_bytes, n)) ==
                       PublishResult::Accepted;
            },
            5s));
        break;
    }

    REQUIRE(saw_queue_full);
    CHECK(slice.publisher.counters().dropped_queue_full >= 1);
}

TEST_CASE("A frame that contradicts the granted geometry retires the session",
          "[m2][slice]")
{
    // 6.2 makes the grant the contract: the array description is settled at
    // Open, and changing a term of it means closing and reopening. Nothing on
    // the producer side enforces that -- publish() resolves a frame's metadata
    // against max_frame_bytes and the lease capacity, and never against the
    // publisher's own declared geometry -- so a publisher will happily send an
    // array it did not grant. This is the check that makes granted_geometry() a
    // guarantee rather than a hint.
    PublisherConfig publisher_cfg = publisher_config();
    publisher_cfg.frame_metadata.element_type = ElementType::UInt8;
    publisher_cfg.frame_metadata.element_size = 1;
    publisher_cfg.frame_metadata.rank = 1;
    publisher_cfg.frame_metadata.shape[0] = k_frame_bytes;

    BulkPublisher publisher(publisher_cfg);
    detail::SubscriberEngine subscriber(subscriber_config());
    open_session(publisher, subscriber);

    REQUIRE(subscriber.granted_geometry().rank == 1);
    REQUIRE(subscriber.granted_geometry().element_type == ElementType::UInt8);

    // A conforming frame first, so the failure below cannot be blamed on the
    // path never having worked.
    {
        auto lease = publisher.source().try_acquire();
        REQUIRE(lease);
        fill(lease, k_frame_bytes, 1);
        REQUIRE(publisher.publish(std::move(lease), meta_for(k_frame_bytes, 1)) ==
                PublishResult::Accepted);
    }

    FrameView received;
    REQUIRE(eventually([&] {
        subscriber.poll(10ms, [&](FrameView view) { received = std::move(view); });
        return static_cast<bool>(received);
    }));
    received.reset();
    CHECK(subscriber.state() == SubscriberState::Active);

    // Now the same stream, described as a different array: UInt16 over half as
    // many elements. It decodes perfectly and it is not what was agreed.
    {
        auto lease = publisher.source().try_acquire();
        REQUIRE(lease);
        fill(lease, k_frame_bytes, 2);

        FrameMetadata lying;
        lying.element_type = ElementType::UInt16;
        lying.element_size = 2;
        lying.rank = 1;
        lying.shape[0] = k_frame_bytes / 2;
        lying.event_counter = 2;

        REQUIRE(publisher.publish(std::move(lease), lying) == PublishResult::Accepted);
    }

    REQUIRE(eventually([&] {
        subscriber.poll(10ms, [](FrameView) {});
        return subscriber.state() == SubscriberState::Failed;
    }));

    // Counted as its own thing, not as a malformed header: it decoded, and the
    // problem is that it disagreed.
    CHECK(subscriber.counters().frames_dropped_geometry_mismatch == 1);
    CHECK(subscriber.counters().frames_dropped_bad_header == 0);

    // And the reason reaches the layer above, which is what lets an application
    // tell a broken contract from a broken link.
    const BulkError why = subscriber.last_error();
    CHECK(why.status == Status::GeometryMismatch);
    CHECK(why.message.find("different array") != std::string::npos);

    // The contradicting frame was never delivered.
    CHECK(subscriber.counters().frames_delivered == 1);
}

TEST_CASE("Views outlive the subscriber that delivered them", "[m2][slice]")
{
    // 9.3's last criterion is "ASan/UBSan clean ... including teardown with
    // views still outstanding".  A view points into the registered receive ring,
    // so this passes only if the ring, its registration, and the UCX context
    // outlive the engine -- which is why the lease shares ownership of the arena
    // rather than pointing at it.
    std::vector<FrameView> held;
    const std::uint64_t bytes = 4096;

    {
        Slice slice;

        for(std::uint32_t n = 0; n < 2; ++n)
        {
            REQUIRE(slice.publish(bytes, n) == PublishResult::Accepted);
        }

        held = collect(slice.subscriber, 2);
        REQUIRE(held.size() == 2);
    }

    // The publisher and subscriber are both gone.  The frames are not.
    for(std::size_t n = 0; n < held.size(); ++n)
    {
        CHECK(held[n]);
        CHECK(held[n].size() == bytes);
        CHECK(payload_matches(held[n], static_cast<unsigned>(n)));
    }

    // Releasing now pushes credits into a queue nobody will ever drain, which
    // must be uneventful rather than a use-after-free.
    held.clear();
}
