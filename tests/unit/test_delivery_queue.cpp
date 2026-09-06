// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <core/delivery_queue.h>
#include <core/frame_fields.h>

#include <catch2/catch_test_macros.hpp>

#include <poll.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

namespace
{

using namespace TangoBulk;
using namespace std::chrono_literals;

FrameView frame_over(const std::shared_ptr<std::vector<std::uint16_t>> &payload,
                     std::uint64_t sequence)
{
    detail::FrameFields fields;
    fields.element_type = ElementType::UInt16;
    fields.element_size = 2;
    fields.rank = 1;
    fields.shape = {payload->size(), 0, 0, 0};
    fields.strides = {2, 0, 0, 0};
    fields.payload_bytes = payload->size() * sizeof(std::uint16_t);
    fields.sequence = sequence;

    return detail::DetachedFrameFactory::make(
        payload, reinterpret_cast<const std::byte *>(payload->data()), fields);
}

std::shared_ptr<std::vector<std::uint16_t>> storage(std::uint16_t fill)
{
    return std::make_shared<std::vector<std::uint16_t>>(8, fill);
}

bool readable(int fd)
{
    pollfd descriptor{};
    descriptor.fd = fd;
    descriptor.events = POLLIN;
    return ::poll(&descriptor, 1, 0) > 0;
}


TEST_CASE("a pushed frame comes back out in order", "[core][delivery]")
{
    detail::DeliveryQueue queue(4, DropPolicy::DropNewest);

    for(std::uint64_t sequence = 1; sequence <= 3; ++sequence)
    {
        CHECK(queue.push(frame_over(storage(static_cast<std::uint16_t>(sequence)), sequence)));
    }

    CHECK(queue.stats().depth == 3);
    CHECK(queue.stats().high_water == 3);

    for(std::uint64_t sequence = 1; sequence <= 3; ++sequence)
    {
        FrameView frame;
        REQUIRE(queue.try_take(frame));
        CHECK(frame.sequence() == sequence);
        CHECK(*reinterpret_cast<const std::uint16_t *>(frame.data()) == sequence);
    }

    FrameView none;
    CHECK_FALSE(queue.try_take(none));
    CHECK(queue.stats().taken == 3);
    CHECK(queue.stats().dropped == 0);
}

TEST_CASE("capacity is rounded up to a power of two, and reported", "[core][delivery]")
{
    detail::DeliveryQueue queue(5, DropPolicy::DropNewest);
    CHECK(queue.stats().capacity == 8);
}


TEST_CASE("DropNewest refuses the arriving frame and keeps the queued ones",
          "[core][delivery]")
{
    detail::DeliveryQueue queue(2, DropPolicy::DropNewest);

    CHECK(queue.push(frame_over(storage(1), 1)));
    CHECK(queue.push(frame_over(storage(2), 2)));

    const auto rejected = storage(3);
    CHECK_FALSE(queue.push(frame_over(rejected, 3)));

    CHECK(rejected.use_count() == 1);
    CHECK(queue.stats().dropped == 1);

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

    CHECK(evicted.use_count() == 1);
    CHECK(queue.stats().dropped == 1);

    FrameView frame;
    REQUIRE(queue.try_take(frame));
    CHECK(frame.sequence() == 2);
    REQUIRE(queue.try_take(frame));
    CHECK(frame.sequence() == 3);
}

TEST_CASE("a frame the caller kept outlives the queue", "[core][delivery]")
{
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
    detail::DeliveryQueue queue(4, DropPolicy::DropNewest);

    const auto first = storage(1);
    const auto second = storage(2);
    CHECK(queue.push(frame_over(first, 1)));
    CHECK(queue.push(frame_over(second, 2)));

    CHECK(queue.discard() == 2);
    CHECK(queue.stats().depth == 0);
    CHECK(first.use_count() == 1);
    CHECK(second.use_count() == 1);

    CHECK(queue.stats().discarded == 2);
    CHECK(queue.stats().dropped == 0);
    CHECK(queue.stats().taken == 0);

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

    CHECK(elapsed < 2s);
}

TEST_CASE("stop() breaks a blocked take() out with nothing queued",
          "[core][delivery]")
{
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
    detail::DeliveryQueue queue(4, DropPolicy::DropNewest);

    CHECK(queue.push(frame_over(storage(1), 1)));
    CHECK(queue.push(frame_over(storage(2), 2)));
    queue.stop();

    const auto started = std::chrono::steady_clock::now();

    FrameView frame;
    REQUIRE(queue.take(frame, started + 5s));
    CHECK(frame.sequence() == 1);
    REQUIRE(queue.take(frame, started + 5s));
    CHECK(frame.sequence() == 2);
    CHECK_FALSE(queue.take(frame, started + 5s));

    CHECK(std::chrono::steady_clock::now() - started < 2s);

    CHECK_FALSE(queue.take(frame, std::chrono::steady_clock::now() + 5s));
}


TEST_CASE("notify() with nobody armed makes no signal at all", "[core][delivery]")
{
    detail::DeliveryQueue queue(4, DropPolicy::DropNewest);
    REQUIRE(queue.fd() >= 0);

    CHECK(queue.push(frame_over(storage(1), 1)));

    CHECK_FALSE(readable(queue.fd()));
}

TEST_CASE("an empty try_take() arms, so the next notify() is seen",
          "[core][delivery]")
{
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
    detail::DeliveryQueue queue(4, DropPolicy::DropNewest);

    FrameView frame;
    CHECK_FALSE(queue.try_take(frame));

    queue.push(frame_over(storage(6), 6));

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

    std::atomic<bool> refused{false};

    std::thread producer([&queue, &refused] {
        for(std::uint64_t sequence = 1; sequence <= k_frames; ++sequence)
        {
            while(queue.stats().depth >= queue.stats().capacity)
            {
                std::this_thread::yield();
            }

            if(!queue.push(frame_over(storage(1), sequence)))
            {
                refused.store(true);
            }
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

    CHECK_FALSE(refused.load());
    CHECK(expected == k_frames + 1);
    CHECK(queue.stats().taken == k_frames);
    CHECK(queue.stats().dropped == 0);
}

} // namespace
