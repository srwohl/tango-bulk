// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include "slice.h"

#include <poll.h>
#include <thread>

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

    const std::vector<FrameView> views = collect(*slice.subscriber.delivery, 1);
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

    const std::vector<FrameView> views = collect(*slice.subscriber.delivery, 1);
    REQUIRE(views.size() == 1);
    const FrameView &view = views.front();

    // The pointer half of 9.3's zero-copy criterion: the address handed to the
    // application is inside the registered region, not in a staging buffer the
    // library copied out of.
    CHECK(slice.subscriber.engine.ring_contains(view.data()));
    CHECK(view.data() == slice.subscriber.engine.slot_address(0));

    // The registration half.  bytes_copied() counts only payload that went
    // through an eager staging copy; for a rendezvous-sized frame there must be
    // none, and no amount of reading the code substitutes for the count.
    CHECK(slice.subscriber.engine.bytes_copied() == 0);
    CHECK(payload_matches(view, 0x11));
}

TEST_CASE("Slots recycle: sequence s lands in slot s % ring_depth", "[m2][slice]")
{
    Slice slice;
    const std::uint32_t depth = slice.subscriber.session.granted_ring_depth();
    REQUIRE(depth == k_ring_depth);

    const std::size_t total = 4u * depth;
    const std::uint64_t bytes = 8192;

    for(std::size_t n = 0; n < total; ++n)
    {
        const auto seed = static_cast<unsigned>(n);
        REQUIRE(slice.publish(bytes, seed) == PublishResult::Accepted);

        std::vector<FrameView> views = collect(*slice.subscriber.delivery, 1);
        REQUIRE(views.size() == 1);

        const FrameView &view = views.front();
        CHECK(view.sequence() == n);
        CHECK(view.data() == slice.subscriber.engine.slot_address(n % depth));
        CHECK(payload_matches(view, seed));

        // Released here, at the end of the iteration, which is what keeps the
        // credit window open for the next one.
        views.clear();
    }

    // Every producer slot came back.  A wraparound bug that leaked one slot per
    // lap shows up here and nowhere else.
    CHECK(eventually([&] { return slice.publisher.counters().frames_credited == total; }));
    CHECK(eventually([&] { return slice.publisher.retained() == 0; }));
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
    REQUIRE(slice.subscriber.session.granted_ring_depth() == 2);

    // Fill beyond the client's negotiated window before polling.  BestEffort
    // may skip frames for this session, but it must not assign them sequences:
    // doing so sends 2 and 3 into slots still occupied by 0 and 1.
    for(unsigned seed = 0; seed < 4; ++seed)
        REQUIRE(slice.publish(4096, seed) == PublishResult::Accepted);

    std::vector<FrameView> first = collect(*slice.subscriber.delivery, 2);
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

    std::vector<FrameView> second = collect(*slice.subscriber.delivery, 2);
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

    std::vector<FrameView> held = collect(*slice.subscriber.delivery, k_credit_window);
    REQUIRE(held.size() == k_credit_window);
    CHECK(slice.subscriber.engine.counters().views_outstanding == k_credit_window);

    // The window is full and every view is retained, so the publisher stalls.
    {
        BulkPublisher::SlotHandle lease = slice.publisher.try_acquire();
        REQUIRE(lease);
        CHECK(slice.publisher.publish(std::move(lease), meta_for(bytes, 99)) ==
              PublishResult::CreditStalled);
    }

    CHECK(slice.publisher.counters().frames_credited == 0);

    // Release the oldest.  Exactly one credit returns -- not zero, and not the
    // whole window.
    held.erase(held.begin());

    REQUIRE(eventually([&] { return slice.publisher.counters().frames_credited == 1; }));
    CHECK(slice.subscriber.engine.counters().views_outstanding == k_credit_window - 1);

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

    std::vector<FrameView> held = collect(*slice.subscriber.delivery, k_credit_window);
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
    CHECK(slice.subscriber.engine.counters().credit_messages_sent <=
          slice.subscriber.engine.counters().credits_returned);
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
        BulkPublisher::SlotHandle lease = slice.publisher.try_acquire();
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
    CHECK(slice.publisher.retained() == accepted);
}

TEST_CASE("A slot handle outlives the publisher that issued it", "[m2][slice]")
{
    // ADR 0003: closing ends participation, not the validity of memory an
    // application still holds. This was a use-after-free until the ring moved
    // into shared storage -- the handle carried a raw pointer into a ring the
    // publisher owned by value, and a Python binding's non-deterministic
    // destruction order makes that the default outcome rather than an edge case.
    BulkPublisher::SlotHandle handle;

    {
        BulkPublisher publisher(publisher_config());
        handle = publisher.try_acquire();
        REQUIRE(handle);

        fill(handle, k_frame_bytes, 5);
        CHECK(publisher.retained() == 1);
    }

    // The publisher is gone. The registration, the ring and the free list the
    // slot goes back to are not, because this handle still holds a share.
    REQUIRE(handle);
    CHECK(handle.capacity() >= k_frame_bytes);

    const auto *bytes = static_cast<const unsigned char *>(handle.data());
    bool intact = true;
    for(std::uint64_t i = 0; i < k_frame_bytes && intact; ++i)
    {
        intact = bytes[i] == static_cast<unsigned char>((5u * 31u + static_cast<unsigned>(i)) & 0xFFu);
    }
    CHECK(intact);

    // And releasing it is safe with nothing else referencing the storage: the
    // slot goes back to a free list that is destroyed immediately afterwards.
    handle.reset();
    CHECK_FALSE(handle);
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
    std::vector<BulkPublisher::SlotHandle> leases;
    for(std::uint32_t n = 0; n < 64; ++n)
    {
        BulkPublisher::SlotHandle lease = slice.publisher.try_acquire();
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

TEST_CASE("A consumer can wait on the transport's descriptor from its own loop",
          "[m2][slice]")
{
    // The ergonomic claim: an event loop that already owns the waiting -- epoll,
    // select, asyncio's add_reader -- needs nothing from this layer but a
    // descriptor and one empty poll. If this works, `async for` is a pure-Python
    // addition with no further C++.
    BulkPublisher publisher(publisher_config());
    Subscriber subscriber(subscriber_config());
    open_session(publisher, subscriber);

    REQUIRE(subscriber.delivery->fd() >= 0);

    // Nothing queued, so an armed wait must time out rather than fire. The
    // empty poll is what arms; there is no separate step to forget.
    REQUIRE(drain(*subscriber.delivery, 0ms, [](FrameView) {}, 1) == 0);
    {
        pollfd pfd{};
        pfd.fd = subscriber.delivery->fd();
        pfd.events = POLLIN;
        CHECK(::poll(&pfd, 1, 40) == 0);
    }

    // Look, then block -- exactly the two steps fd() documents.
    REQUIRE(drain(*subscriber.delivery, 0ms, [](FrameView) {}, 1) == 0);

    std::thread producer([&] {
        std::this_thread::sleep_for(60ms);
        auto lease = publisher.try_acquire();
        REQUIRE(lease);
        fill(lease, k_frame_bytes, 9);
        REQUIRE(publisher.publish(std::move(lease), meta_for(k_frame_bytes, 9)) ==
                PublishResult::Accepted);
    });

    pollfd pfd{};
    pfd.fd = subscriber.delivery->fd();
    pfd.events = POLLIN;

    const auto started = std::chrono::steady_clock::now();
    const int ready = ::poll(&pfd, 1, 5000);
    const auto waited = std::chrono::steady_clock::now() - started;
    producer.join();

    // Woken by the frame, not by the timeout.
    REQUIRE(ready == 1);
    CHECK((pfd.revents & POLLIN) != 0);
    CHECK(waited < 3s);

    FrameView got;
    REQUIRE(eventually([&] {
        drain(*subscriber.delivery, 10ms, [&](FrameView view) { got = std::move(view); }, 1);
        return static_cast<bool>(got);
    }));
    CHECK(payload_matches(got, 9));
    got.reset();
}

TEST_CASE("An unarmed descriptor is never signalled, which is what makes it free",
          "[m2][slice]")
{
    // The other half of the contract: a consumer that is keeping up never arms,
    // so the engine never writes, so the fast path costs no syscall at all. A
    // test
    // that only checked "the fd fires" would pass just as well against an
    // engine that wrote on every frame.
    BulkPublisher publisher(publisher_config());
    Subscriber subscriber(subscriber_config());
    open_session(publisher, subscriber);

    for(int i = 0; i < 4; ++i)
    {
        auto lease = publisher.try_acquire();
        REQUIRE(lease);
        fill(lease, k_frame_bytes, static_cast<unsigned>(i));
        REQUIRE(publisher.publish(std::move(lease),
                                  meta_for(k_frame_bytes, static_cast<std::uint64_t>(i))) ==
                PublishResult::Accepted);
    }

    REQUIRE(eventually([&] { return subscriber.engine.counters().frames_received >= 4; }));

    // Four frames queued and nobody armed: the descriptor stays quiet.
    pollfd pfd{};
    pfd.fd = subscriber.delivery->fd();
    pfd.events = POLLIN;
    CHECK(::poll(&pfd, 1, 60) == 0);

    // And the frames are still there -- the descriptor is a notification
    // sidecar, never the queue itself.
    std::size_t drained = 0;
    REQUIRE(eventually([&] {
        drained += drain(*subscriber.delivery, 20ms, [](FrameView view) { view.reset(); });
        return drained == 4;
    }));
}

TEST_CASE("poll() takes at most max_frames, and withholds only their credit",
          "[m2][slice]")
{
    // Draining unconditionally means one call can hold queue_depth credits at
    // once. A consumer that wants exactly one frame -- a read(), an iterator
    // step -- needs to be able to say so, or it needs a delivery path of its
    // own, which is how a binding ends up with two.
    BulkPublisher publisher(publisher_config());
    Subscriber subscriber(subscriber_config());
    open_session(publisher, subscriber);

    constexpr int k_published = 4;
    for(int i = 0; i < k_published; ++i)
    {
        auto lease = publisher.try_acquire();
        REQUIRE(lease);
        fill(lease, k_frame_bytes, static_cast<unsigned>(i));
        REQUIRE(publisher.publish(std::move(lease),
                                  meta_for(k_frame_bytes, static_cast<std::uint64_t>(i))) ==
                PublishResult::Accepted);
    }

    REQUIRE(eventually(
        [&] { return subscriber.engine.counters().frames_received >= k_published; }));

    // One at a time, and the frame is released before the next call so the
    // credit accounting is about the budget rather than about retention.
    for(int i = 0; i < k_published; ++i)
    {
        std::size_t seen = 0;
        const std::size_t dispatched =
            drain(*subscriber.delivery, 50ms, [&](FrameView view) { ++seen; view.reset(); }, 1);

        CHECK(dispatched == 1);
        CHECK(seen == 1);
        CHECK(subscriber.delivery->stats().taken ==
              static_cast<std::uint64_t>(i) + 1);
    }

    // Nothing left, and asking for one more does not invent a frame.
    CHECK(drain(*subscriber.delivery, 10ms, [](FrameView) {}, 1) == 0);

    // The default is still drain-everything, which is what every existing
    // caller relies on.
    for(int i = 0; i < k_published; ++i)
    {
        auto lease = publisher.try_acquire();
        REQUIRE(lease);
        fill(lease, k_frame_bytes, static_cast<unsigned>(i));
        REQUIRE(publisher.publish(std::move(lease),
                                  meta_for(k_frame_bytes,
                                           static_cast<std::uint64_t>(k_published + i))) ==
                PublishResult::Accepted);
    }

    REQUIRE(eventually([&] {
        return subscriber.engine.counters().frames_received >= 2 * k_published;
    }));

    std::size_t drained = 0;
    REQUIRE(eventually([&] {
        drained += drain(*subscriber.delivery, 50ms, [](FrameView view) { view.reset(); });
        return drained == k_published;
    }));
}

TEST_CASE("A publisher refuses to send an array it did not declare", "[m2][slice]")
{
    // The producer half of 6.2. A device is far better placed to notice that it
    // is publishing something it never declared than a consumer is to discover
    // it after the bytes are on the wire and tear down a session over it.
    PublisherConfig config = publisher_config();
    config.frame_metadata.element_type = ElementType::UInt8;
    config.frame_metadata.element_size = 1;
    config.frame_metadata.rank = 1;
    config.frame_metadata.shape[0] = k_frame_bytes;

    BulkPublisher publisher(config);
    Subscriber subscriber(subscriber_config());
    open_session(publisher, subscriber);

    // What it declared, accepted.
    {
        auto lease = publisher.try_acquire();
        REQUIRE(lease);
        fill(lease, k_frame_bytes, 1);
        CHECK(publisher.publish(std::move(lease), meta_for(k_frame_bytes, 1)) ==
              PublishResult::Accepted);
    }

    // The same bytes described as a different array: refused, and the lease is
    // consumed rather than left engaged, exactly as any other BadMetadata.
    {
        auto lease = publisher.try_acquire();
        REQUIRE(lease);
        fill(lease, k_frame_bytes, 2);

        FrameMetadata undeclared;
        undeclared.element_type = ElementType::UInt16;
        undeclared.element_size = 2;
        undeclared.rank = 1;
        undeclared.shape[0] = k_frame_bytes / 2;

        CHECK(publisher.publish(std::move(lease), undeclared) == PublishResult::BadMetadata);
    }

    CHECK(publisher.counters().dropped_bad_metadata == 1);

    // The session is untouched: refusing one frame is not a reason to break a
    // stream, which is exactly why catching it here beats catching it there.
    CHECK(subscriber.engine.state() == SubscriberState::Active);
    CHECK(publisher.session_count() == 1);
}

TEST_CASE("A frame that contradicts the granted geometry retires the session",
          "[m2][slice]")
{
    // The consumer half, and it needs a non-conforming peer to provoke -- which
    // is the point. With the producer check in place, a publisher built from
    // this library cannot contradict its own grant, so this defends against one
    // that was not: a different implementation, an older version, a header that
    // decodes cleanly and lies.
    //
    // Built by forging the OpenReply. tests/ucx already carries real protocol
    // bytes by hand, so a peer that grants one thing and sends another is a
    // decode, an edit and a re-encode -- no test hook in the library, and
    // nothing the library could do to stop a peer behaving this way.
    PublisherConfig config = publisher_config();
    config.frame_metadata.element_type = ElementType::UInt8;
    config.frame_metadata.element_size = 1;
    config.frame_metadata.rank = 1;
    config.frame_metadata.shape[0] = k_frame_bytes;

    BulkPublisher publisher(config);
    Subscriber subscriber(subscriber_config());

    const std::vector<std::byte> honest = exchange_open(publisher, subscriber);

    Protocol::Envelope envelope;
    Protocol::OpenReply reply;
    REQUIRE(Protocol::decode(honest.data(), honest.size(), reply, &envelope) == Status::Ok);
    REQUIRE(reply.status == Status::Ok);

    // Same payload size, different array: the grant now says UInt16 over half
    // as many elements, which is not what the publisher will send.
    reply.geometry.element_type = ElementType::UInt16;
    reply.geometry.element_size = 2;
    reply.geometry.rank = 1;
    reply.geometry.shape = {k_frame_bytes / 2, 0, 0, 0};
    reply.geometry.strides = {2, 0, 0, 0};
    REQUIRE(reply.geometry.validate() == Status::Ok);

    const std::vector<std::byte> forged = Protocol::encode(reply, envelope.correlation_id);
    REQUIRE(subscriber.adopt_open_reply(forged.data(), forged.size()) == Status::Ok);
    REQUIRE(await_armed(publisher, subscriber));

    REQUIRE(subscriber.session.granted_geometry().element_type == ElementType::UInt16);

    // The publisher sends what it actually declared, which now contradicts what
    // this subscriber believes it was granted.
    {
        auto lease = publisher.try_acquire();
        REQUIRE(lease);
        fill(lease, k_frame_bytes, 3);
        REQUIRE(publisher.publish(std::move(lease), meta_for(k_frame_bytes, 3)) ==
                PublishResult::Accepted);
    }

    REQUIRE(eventually([&] {
        drain(*subscriber.delivery, 10ms, [](FrameView) {});
        return subscriber.engine.state() == SubscriberState::Failed;
    }));

    // Counted as its own thing, not as a malformed header: it decoded, and the
    // problem is that it disagreed.
    CHECK(subscriber.engine.counters().frames_dropped_geometry_mismatch == 1);
    CHECK(subscriber.engine.counters().frames_dropped_bad_header == 0);
    CHECK(subscriber.delivery->stats().taken == 0);

    const BulkError why = subscriber.engine.last_error();
    CHECK(why.status == Status::GeometryMismatch);
    CHECK(why.message.find("different array") != std::string::npos);
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

        held = collect(*slice.subscriber.delivery, 2);
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
