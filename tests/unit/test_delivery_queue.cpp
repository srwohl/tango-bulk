// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <core/delivery_queue.h>

#include <catch2/catch_test_macros.hpp>

#include <poll.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

/// The delivery queue, its queue policy and its readiness descriptor, with no
/// NIC and no session.
///
/// All of this used to live inside `SubscriberEngine`, where reaching it needed
/// a UCX loopback and a real publisher -- so the drop policy, the high-water
/// gauge and the whole lost-wakeup protocol were only ever exercised
/// incidentally, by tests aimed at something else. They are the parts most
/// worth testing directly: a dropped frame that fails to return its credit
/// stalls a stream, and a lost wakeup hangs a consumer, and neither shows up as
/// a crash.
namespace
{

using namespace TangoBulk;
using namespace std::chrono_literals;

/// A frame over storage the test holds, so its lifetime can be observed.
///
/// `payload.use_count()` is the assertion that matters for credit: a frame the
/// queue dropped or handed over has released the storage exactly when the last
/// view of it went away.
FrameView frame_over(const std::shared_ptr<std::vector<std::uint16_t>> &payload,
                     std::uint64_t sequence)
{
    FrameView::Fields fields;
    fields.element_type = ElementType::UInt16;
    fields.element_size = 2;
    fields.rank = 1;
    fields.shape = {payload->size(), 0, 0, 0};
    fields.strides = {2, 0, 0, 0};
    fields.payload_bytes = payload->size() * sizeof(std::uint16_t);
    fields.sequence = sequence;

    return FrameView::detached(
        payload, reinterpret_cast<const std::byte *>(payload->data()), fields);
}

std::shared_ptr<std::vector<std::uint16_t>> storage(std::uint16_t fill)
{
    return std::make_shared<std::vector<std::uint16_t>>(8, fill);
}

/// Whether the descriptor is readable right now.
bool readable(int fd)
{
    pollfd descriptor{};
    descriptor.fd = fd;
    descriptor.events = POLLIN;
    return ::poll(&descriptor, 1, 0) > 0;
}

// -- the queue ---------------------------------------------------------------

TEST_CASE("a pushed frame comes back out in order", "[core][delivery]")
{
    detail::DeliveryQueue queue(4, DropPolicy::DropNewest);

    for(std::uint64_t sequence = 1; sequence <= 3; ++sequence)
    {
        CHECK(queue.push(frame_over(storage(static_cast<std::uint16_t>(sequence)), sequence)));
    }

    CHECK(queue.size() == 3);
    CHECK(queue.high_water() == 3);

    for(std::uint64_t sequence = 1; sequence <= 3; ++sequence)
    {
        FrameView frame;
        REQUIRE(queue.try_take(frame));
        CHECK(frame.sequence() == sequence);
        CHECK(*reinterpret_cast<const std::uint16_t *>(frame.data()) == sequence);
    }

    FrameView none;
    CHECK_FALSE(queue.try_take(none));
    CHECK(queue.taken() == 3);
    CHECK(queue.dropped() == 0);
}

TEST_CASE("capacity is rounded up to a power of two, and reported", "[core][delivery]")
{
    detail::DeliveryQueue queue(5, DropPolicy::DropNewest);
    CHECK(queue.capacity() == 8);
}

// -- the queue policy --------------------------------------------------------

TEST_CASE("DropNewest refuses the arriving frame and keeps the queued ones",
          "[core][delivery]")
{
    detail::DeliveryQueue queue(2, DropPolicy::DropNewest);

    CHECK(queue.push(frame_over(storage(1), 1)));
    CHECK(queue.push(frame_over(storage(2), 2)));

    const auto rejected = storage(3);
    CHECK_FALSE(queue.push(frame_over(rejected, 3)));

    // Refused, and released on the spot. Holding a rejected frame would be a
    // withheld credit that nothing is ever going to return.
    CHECK(rejected.use_count() == 1);
    CHECK(queue.dropped() == 1);

    FrameView frame;
    REQUIRE(queue.try_take(frame));
    CHECK(frame.sequence() == 1);
}

TEST_CASE("DropOldest evicts the head to make room for the arriving frame",
          "[core][delivery]")
{
    detail::DeliveryQueue queue(2, DropPolicy::DropOldest);

    const auto evicted = storage(1);
    CHECK(queue.push(frame_over(evicted, 1)));
    CHECK(queue.push(frame_over(storage(2), 2)));
    CHECK(queue.push(frame_over(storage(3), 3)));

    // The evicted frame's credit went back when it was dropped, not later.
    CHECK(evicted.use_count() == 1);
    CHECK(queue.dropped() == 1);

    FrameView frame;
    REQUIRE(queue.try_take(frame));
    CHECK(frame.sequence() == 2);
    REQUIRE(queue.try_take(frame));
    CHECK(frame.sequence() == 3);
}

TEST_CASE("a frame the caller kept outlives the queue", "[core][delivery]")
{
    // ADR 0003, at the smallest scale it is expressible: the queue is not what
    // keeps a delivered frame's storage alive, the view is.
    const auto payload = storage(9);
    FrameView kept;

    {
        detail::DeliveryQueue queue(2, DropPolicy::DropNewest);
        CHECK(queue.push(frame_over(payload, 9)));
        REQUIRE(queue.try_take(kept));
    }

    REQUIRE(static_cast<bool>(kept));
    CHECK(kept.sequence() == 9);
    CHECK(*reinterpret_cast<const std::uint16_t *>(kept.data()) == 9);

    kept.reset();
    CHECK(payload.use_count() == 1);
}

TEST_CASE("frames still queued when the queue dies return their credit",
          "[core][delivery]")
{
    const auto abandoned = storage(4);

    {
        detail::DeliveryQueue queue(2, DropPolicy::DropNewest);
        CHECK(queue.push(frame_over(abandoned, 4)));
        CHECK(abandoned.use_count() == 2);
    }

    CHECK(abandoned.use_count() == 1);
}

TEST_CASE("discard() empties the queue and returns every credit", "[core][delivery]")
{
    // What a subscription does when the session that filled the queue is gone.
    // The frames are valid bytes from a contract that no longer holds.
    detail::DeliveryQueue queue(4, DropPolicy::DropNewest);

    const auto first = storage(1);
    const auto second = storage(2);
    CHECK(queue.push(frame_over(first, 1)));
    CHECK(queue.push(frame_over(second, 2)));

    CHECK(queue.discard() == 2);
    CHECK(queue.size() == 0);
    CHECK(first.use_count() == 1);
    CHECK(second.use_count() == 1);

    // Counted as its own thing. A frame let go because its session ended is not
    // a frame the queue had no room for, and one counter cannot mean both.
    CHECK(queue.discarded() == 2);
    CHECK(queue.dropped() == 0);
    CHECK(queue.taken() == 0);

    FrameView none;
    CHECK_FALSE(queue.try_take(none));
    CHECK(queue.discard() == 0);
}

TEST_CASE("discard() leaves a frame already handed over alone", "[core][delivery]")
{
    detail::DeliveryQueue queue(4, DropPolicy::DropNewest);

    const auto taken = storage(3);
    CHECK(queue.push(frame_over(taken, 3)));

    FrameView held;
    REQUIRE(queue.try_take(held));

    CHECK(queue.discard() == 0);
    REQUIRE(static_cast<bool>(held));
    CHECK(*reinterpret_cast<const std::uint16_t *>(held.data()) == 3);
}

// -- waiting and waking ------------------------------------------------------

TEST_CASE("take() waits out its deadline when nothing arrives", "[core][delivery]")
{
    detail::DeliveryQueue queue(2, DropPolicy::DropNewest);

    const auto started = std::chrono::steady_clock::now();
    FrameView frame;
    CHECK_FALSE(queue.take(frame, started + 40ms));
    const auto elapsed = std::chrono::steady_clock::now() - started;

    CHECK(elapsed >= 40ms);
    CHECK(elapsed < 2s);
}

TEST_CASE("take() returns as soon as a producer pushes and notifies",
          "[core][delivery]")
{
    detail::DeliveryQueue queue(4, DropPolicy::DropNewest);

    std::thread producer([&queue] {
        std::this_thread::sleep_for(20ms);
        queue.push(frame_over(storage(7), 7));
        queue.notify();
    });

    const auto started = std::chrono::steady_clock::now();
    FrameView frame;
    const bool got = queue.take(frame, started + 5s);
    const auto elapsed = std::chrono::steady_clock::now() - started;
    producer.join();

    REQUIRE(got);
    CHECK(frame.sequence() == 7);

    // Woken, not timed out. The descriptor is the whole reason this is
    // milliseconds rather than seconds.
    CHECK(elapsed < 2s);
}

TEST_CASE("stop() breaks a blocked take() out with nothing queued",
          "[core][delivery]")
{
    // The terminal wakeup: close, interruption and teardown all have to reach a
    // consumer that is asleep, and none of them has a frame to hand it.
    detail::DeliveryQueue queue(2, DropPolicy::DropNewest);

    std::thread stopper([&queue] {
        std::this_thread::sleep_for(20ms);
        queue.stop();
    });

    const auto started = std::chrono::steady_clock::now();
    FrameView frame;
    const bool got = queue.take(frame, started + 5s);
    const auto elapsed = std::chrono::steady_clock::now() - started;
    stopper.join();

    CHECK_FALSE(got);
    CHECK_FALSE(static_cast<bool>(frame));
    CHECK(elapsed < 2s);
}

TEST_CASE("a stopped queue hands over what it already holds, then stops waiting",
          "[core][delivery]")
{
    // Section 7.10: interruption is a mark-and-wake, not a drain. What happens
    // to the remainder is the owner's decision, so the queue must not throw it
    // away on the way out -- and must not block for more either.
    detail::DeliveryQueue queue(4, DropPolicy::DropNewest);

    CHECK(queue.push(frame_over(storage(1), 1)));
    CHECK(queue.push(frame_over(storage(2), 2)));
    queue.stop();
    CHECK(queue.stopped());

    const auto started = std::chrono::steady_clock::now();

    FrameView frame;
    REQUIRE(queue.take(frame, started + 5s));
    CHECK(frame.sequence() == 1);
    REQUIRE(queue.take(frame, started + 5s));
    CHECK(frame.sequence() == 2);
    CHECK_FALSE(queue.take(frame, started + 5s));

    // None of the three waited: the first two had a frame, the third was
    // stopped. A five-second deadline would have made a non-sticky stop obvious.
    CHECK(std::chrono::steady_clock::now() - started < 2s);

    // Sticky. A second consumer arriving later must not block either.
    CHECK_FALSE(queue.take(frame, std::chrono::steady_clock::now() + 5s));
}

// -- readiness ---------------------------------------------------------------

TEST_CASE("notify() with nobody armed makes no signal at all", "[core][delivery]")
{
    // The property that makes a descriptor affordable on the frame path: a
    // consumer that is keeping up never arms, so the producer never writes.
    detail::DeliveryQueue queue(4, DropPolicy::DropNewest);
    REQUIRE(queue.fd() >= 0);

    CHECK(queue.push(frame_over(storage(1), 1)));
    queue.notify();
    queue.notify();

    CHECK_FALSE(readable(queue.fd()));
}

TEST_CASE("an empty try_take() arms, so the next notify() is seen",
          "[core][delivery]")
{
    // The contract for a caller that owns its event loop. Without the arm
    // inside try_take(), such a caller looks, finds nothing, waits on a
    // descriptor nobody will write, and hangs with a frame queued behind it.
    detail::DeliveryQueue queue(4, DropPolicy::DropNewest);
    REQUIRE(queue.fd() >= 0);

    FrameView frame;
    CHECK_FALSE(queue.try_take(frame));
    CHECK_FALSE(readable(queue.fd()));

    CHECK(queue.push(frame_over(storage(5), 5)));
    queue.notify();

    CHECK(readable(queue.fd()));
    REQUIRE(queue.try_take(frame));
    CHECK(frame.sequence() == 5);
}

TEST_CASE("a frame that arrives between the look and the wait is not lost",
          "[core][delivery]")
{
    // The lost-wakeup race, provoked deliberately: the frame is pushed after
    // the consumer has decided the queue is empty. The arm inside try_take() is
    // what makes the following take() return it immediately rather than sleep
    // for the full deadline.
    detail::DeliveryQueue queue(4, DropPolicy::DropNewest);

    FrameView frame;
    CHECK_FALSE(queue.try_take(frame));

    queue.push(frame_over(storage(6), 6));
    queue.notify();

    const auto started = std::chrono::steady_clock::now();
    REQUIRE(queue.take(frame, started + 5s));
    CHECK(frame.sequence() == 6);
    CHECK(std::chrono::steady_clock::now() - started < 2s);
}

TEST_CASE("many frames survive a producer and a consumer running at once",
          "[core][delivery]")
{
    detail::DeliveryQueue queue(64, DropPolicy::DropNewest);
    constexpr std::uint64_t k_frames = 2'000;

    // The producer waits for room rather than retrying a refused push: with one
    // producer, room observed is room still there, and a refused push is a
    // *dropped frame* under the queue policy rather than backpressure. Nothing
    // in the queue's contract offers a producer a way to try again.
    std::atomic<bool> refused{false};

    std::thread producer([&queue, &refused] {
        for(std::uint64_t sequence = 1; sequence <= k_frames; ++sequence)
        {
            while(queue.size() >= queue.capacity())
            {
                std::this_thread::yield();
            }

            if(!queue.push(frame_over(storage(1), sequence)))
            {
                refused.store(true);
            }
            queue.notify();
        }
    });

    std::uint64_t expected = 1;
    const auto deadline = std::chrono::steady_clock::now() + 30s;

    while(expected <= k_frames && std::chrono::steady_clock::now() < deadline)
    {
        FrameView frame;
        if(queue.take(frame, std::chrono::steady_clock::now() + 100ms))
        {
            CHECK(frame.sequence() == expected);
            ++expected;
        }
    }

    producer.join();

    // Every frame, exactly once, in order. DropNewest never fires because the
    // producer waits for room rather than overrunning it.
    CHECK_FALSE(refused.load());
    CHECK(expected == k_frames + 1);
    CHECK(queue.taken() == k_frames);
    CHECK(queue.dropped() == 0);
}

} // namespace
