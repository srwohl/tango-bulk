// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <ucx/subscriber_engine.h>

#include <tango-bulk/publisher.h>

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <thread>
#include <vector>

/// IMPLEMENTATION_SPEC.md 9.3's exit criteria, one TEST_CASE each.
///
/// The list is deliberately short and deliberately closed.  This milestone is
/// the first time the ownership rules in 5.4 and 5.5 are executed rather than
/// written down, and the value of these cases is that each one fails for exactly
/// one reason.  Breadth belongs in tests/unit, where it costs milliseconds and
/// no transport; every case here starts two UCX workers and pins memory.
namespace
{

using namespace TangoBulk;
using namespace std::chrono_literals;

constexpr std::uint64_t k_frame_bytes = 256u << 10; ///< comfortably rendezvous-sized
constexpr std::uint32_t k_ring_depth = 8;
constexpr std::uint32_t k_credit_window = 4;

PublisherConfig publisher_config()
{
    PublisherConfig config;
    config.stream_name = "m2.slice";
    config.max_frame_bytes = k_frame_bytes;
    config.ring_depth = k_ring_depth;
    config.credit_window = k_credit_window;
    config.publish_queue_depth = 8;
    return config;
}

SubscriberConfig subscriber_config()
{
    SubscriberConfig config;
    config.stream_name = "m2.slice";
    config.max_frame_bytes = k_frame_bytes;
    config.ring_depth = k_ring_depth;
    config.credit_window = k_credit_window;
    config.delivery_queue_depth = 32;
    // 9.3: Manual only.  There is no dispatch thread in M2 and asking for one
    // is a construction error, not a silent upgrade.
    config.delivery_mode = DeliveryMode::Manual;
    return config;
}

/// One publisher and one subscriber, opened, in this process over UCX loopback.
///
/// The `Open` exchange goes straight through `handle_coordination` -- no Tango
/// process, no DeviceProxy, no commands, which is what 9.3 means by driving the
/// coordination plane directly.  The bytes are the real protocol bytes; only the
/// transport carrying them is short-circuited.
struct Slice
{
    BulkPublisher publisher;
    detail::SubscriberEngine subscriber;

    explicit Slice(PublisherConfig pub = publisher_config(),
                   SubscriberConfig sub = subscriber_config()) :
        publisher(std::move(pub)),
        subscriber(std::move(sub))
    {
        const std::vector<std::byte> request = subscriber.make_open_request(1);
        const std::vector<std::byte> reply =
            publisher.handle_coordination(request.data(), request.size());

        REQUIRE(subscriber.adopt_open_reply(reply.data(), reply.size()) == Status::Ok);
        REQUIRE(subscriber.state() == SubscriberState::Active);
        REQUIRE(publisher.session_count() == 1);
    }

    /// Close the session before either side is destroyed.
    ///
    /// Not tidiness.  Members die in reverse declaration order, so without this
    /// the subscriber's worker and receive ring would go away while the
    /// publisher still had frames in flight to them -- and UCX reports that as
    /// an arbiter assertion inside endpoint destruction, a long way from the
    /// cause.  `Close` runs 4.2's teardown in its mandated order: stop
    /// submitting, drain outstanding operations, close the endpoint, and only
    /// then release the slots those operations were reading from.
    ~Slice()
    {
        const std::vector<std::byte> request = subscriber.make_close_request(2);
        publisher.handle_coordination(request.data(), request.size());
    }

    Slice(const Slice &) = delete;
    Slice &operator=(const Slice &) = delete;
};

FrameMetadata meta_for(std::uint64_t payload_bytes, std::uint64_t counter)
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
void fill(const BulkSource::Lease &lease, std::uint64_t bytes, unsigned seed)
{
    auto *p = static_cast<unsigned char *>(lease.data());
    for(std::uint64_t i = 0; i < bytes; ++i)
    {
        p[i] = static_cast<unsigned char>((seed * 31u + static_cast<unsigned>(i)) & 0xFFu);
    }
}

bool payload_matches(const FrameView &view, unsigned seed)
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

PublishResult publish_one(Slice &slice, std::uint64_t bytes, unsigned seed)
{
    BulkSource::Lease lease = slice.publisher.source().try_acquire();
    REQUIRE(lease);
    fill(lease, bytes, seed);
    return slice.publisher.publish(std::move(lease), meta_for(bytes, seed));
}

/// Poll until `want` frames have been delivered, or the budget runs out.
///
/// The returned views are *retained*, which means their credits are withheld.
/// That is the point in most of these cases; where it is not, the caller clears
/// the vector.
std::vector<FrameView> collect(detail::SubscriberEngine &subscriber,
                               std::size_t want,
                               std::chrono::milliseconds budget = 5s)
{
    std::vector<FrameView> views;
    const auto deadline = std::chrono::steady_clock::now() + budget;

    while(views.size() < want && std::chrono::steady_clock::now() < deadline)
    {
        subscriber.poll(10ms, [&views](FrameView view) { views.push_back(std::move(view)); });
    }

    return views;
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

} // namespace

TEST_CASE("A published frame arrives intact with its metadata", "[m2][slice]")
{
    Slice slice;
    const std::uint64_t bytes = 4096;

    REQUIRE(publish_one(slice, bytes, 0x5A) == PublishResult::Accepted);

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

    REQUIRE(publish_one(slice, k_frame_bytes, 0x11) == PublishResult::Accepted);

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
        REQUIRE(publish_one(slice, bytes, seed) == PublishResult::Accepted);

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

TEST_CASE("A retained view withholds exactly one credit", "[m2][slice]")
{
    Slice slice;
    const std::uint64_t bytes = 4096;

    for(std::uint32_t n = 0; n < k_credit_window; ++n)
    {
        REQUIRE(publish_one(slice, bytes, n) == PublishResult::Accepted);
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
    REQUIRE(publish_one(slice, bytes, 100) == PublishResult::Accepted);
}

TEST_CASE("Out-of-order release advances the ack only across the contiguous prefix",
          "[m2][slice]")
{
    Slice slice;
    const std::uint64_t bytes = 4096;

    for(std::uint32_t n = 0; n < k_credit_window; ++n)
    {
        REQUIRE(publish_one(slice, bytes, n) == PublishResult::Accepted);
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
            REQUIRE(publish_one(slice, bytes, n) == PublishResult::Accepted);
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
