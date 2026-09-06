// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include "fake_transport.h"

#include <catch2/catch_test_macros.hpp>

#include <poll.h>

#include <algorithm>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

namespace
{

using namespace TangoBulk;
using namespace TangoBulkTests;


TEST_CASE("the reconnect backoff doubles and is capped at the lease", "[core][subscription]")
{
    CHECK(backoff_delay(0, 200, 1'000) == std::chrono::milliseconds{200});
    CHECK(backoff_delay(1, 200, 1'000) == std::chrono::milliseconds{400});
    CHECK(backoff_delay(2, 200, 1'000) == std::chrono::milliseconds{800});

    CHECK(backoff_delay(3, 200, 1'000) == std::chrono::milliseconds{1'000});
    CHECK(backoff_delay(9, 200, 1'000) == std::chrono::milliseconds{1'000});

    CHECK(backoff_delay(0, 500, 0) == std::chrono::milliseconds{1});
    CHECK(backoff_delay(4, 500, 0) == std::chrono::milliseconds{1});
}


TEST_CASE("a subscription cannot be opened without somewhere to deliver",
          "[core][subscription]")
{
    Script script;

    SubscriptionCallbacks missing_frame = noop_callbacks();
    missing_frame.on_frame = nullptr;
    CHECK_THROWS_AS(Subscription::open(subscription_config(),
                                                    fake_channel(script),
                                                    fake_factory(script),
                                                    std::move(missing_frame)),
                    BulkException);

    SubscriptionCallbacks missing_state = noop_callbacks();
    missing_state.on_state = nullptr;
    CHECK_THROWS_AS(Subscription::open(subscription_config(),
                                                    fake_channel(script),
                                                    fake_factory(script),
                                                    std::move(missing_state)),
                    BulkException);

    CHECK(script.transports_built.load() == 0);
    CHECK(script.opens.load() == 0);
}

TEST_CASE("a transport factory that returns nothing is refused, not dereferenced",
          "[core][subscription]")
{
    Script script;
    SubscriberConfig config = subscription_config();
    config.reconnect_policy = ReconnectPolicy::FailFast;

    CHECK_THROWS_AS(
        Subscription::open(
            config,
            fake_channel(script),
            [](const SubscriberConfig &, std::shared_ptr<detail::DeliveryQueue>)
            { return std::unique_ptr<detail::SubscriberTransport>{}; },
            noop_callbacks()),
        BulkException);
}

TEST_CASE("a granted session that never probes gives up on its own budget",
          "[core][subscription]")
{
    Script script;
    script.probe_arrives = false;

    SubscriberConfig config = subscription_config();
    config.reconnect_policy = ReconnectPolicy::FailFast;
    config.probe_timeout_ms = 40;

    const auto started = std::chrono::steady_clock::now();
    CHECK_THROWS_AS(Subscription::open(config,
                                                    fake_channel(script),
                                                    fake_factory(script),
                                                    noop_callbacks()),
                    BulkException);
    const auto elapsed = std::chrono::steady_clock::now() - started;

    CHECK(elapsed >= std::chrono::milliseconds{40});
    CHECK(elapsed < std::chrono::milliseconds{500});

    CHECK(script.closes.load() >= 1);
}


TEST_CASE("RenewTooFrequent slows the timer and does not end the session",
          "[core][subscription]")
{
    Script script;
    {
        std::lock_guard<std::mutex> lock(script.mutex);
        script.renew_results = {Status::RenewTooFrequent, Status::RenewTooFrequent};
    }

    auto subscription = Subscription::open(
        subscription_config(), fake_channel(script), fake_factory(script), noop_callbacks());

    REQUIRE(subscription->state() == SubscriberState::Active);
    REQUIRE(eventually([&script] { return script.renews.load() >= 3; }));

    CHECK(subscription->state() == SubscriberState::Active);
    CHECK(subscription->counters().reconnects == 0);

    CHECK(script.transports_built.load() == 1);
}

TEST_CASE("a lost session is not resurrected, it is replaced", "[core][subscription]")
{
    Script script;
    {
        std::lock_guard<std::mutex> lock(script.mutex);
        script.renew_results = {Status::SessionExpired};
    }

    auto subscription = Subscription::open(
        subscription_config(), fake_channel(script), fake_factory(script), noop_callbacks());

    REQUIRE(eventually([&script] { return script.transports_built.load() >= 2; }));
    CHECK(eventually([&subscription] { return subscription->state() == SubscriberState::Active; }));
    CHECK(script.opens.load() >= 2);
    CHECK(subscription->counters().reconnects >= 1);
}


TEST_CASE("reconnect gives up after the configured attempts and says so",
          "[core][subscription]")
{
    Script script;
    {
        std::lock_guard<std::mutex> lock(script.mutex);
        script.open_results = {Status::Ok,
                               Status::UnknownStream,
                               Status::UnknownStream,
                               Status::UnknownStream,
                               Status::UnknownStream,
                               Status::UnknownStream};
        script.renew_results = {Status::SessionExpired};
    }

    SubscriberConfig config = subscription_config();
    config.reconnect_max_attempts = 3;

    config.delivery_mode = DeliveryMode::DispatchThread;

    std::mutex seen_mutex;
    std::vector<SubscriberState> seen;
    BulkError final_error;

    SubscriptionCallbacks callbacks = noop_callbacks();
    callbacks.on_state = [&](SubscriberState state, const BulkError &error) {
        std::lock_guard<std::mutex> lock(seen_mutex);
        seen.push_back(state);
        if(state == SubscriberState::Failed)
        {
            final_error = error;
        }
    };

    auto subscription = Subscription::open(
        config, fake_channel(script), fake_factory(script), std::move(callbacks));

    REQUIRE(eventually([&subscription] { return subscription->state() == SubscriberState::Failed; }));

    CHECK(subscription->counters().reconnects == 3);

    REQUIRE(eventually([&] {
        std::lock_guard<std::mutex> lock(seen_mutex);
        return std::count(seen.begin(), seen.end(), SubscriberState::Failed) == 1;
    }));

    {
        std::lock_guard<std::mutex> lock(seen_mutex);
        CHECK(std::count(seen.begin(), seen.end(), SubscriberState::Reconnecting) == 3);
    }

    CHECK(final_error.message.find("reconnect attempts exhausted") != std::string::npos);
    CHECK(final_error.message.find("UnknownStream") != std::string::npos);
}

TEST_CASE("FailFast does not retry", "[core][subscription]")
{
    Script script;
    {
        std::lock_guard<std::mutex> lock(script.mutex);
        script.open_results = {Status::Ok};
        script.renew_results = {Status::SessionExpired};
    }

    SubscriberConfig config = subscription_config();
    config.reconnect_policy = ReconnectPolicy::FailFast;

    auto subscription = Subscription::open(
        config, fake_channel(script), fake_factory(script), noop_callbacks());

    REQUIRE(eventually([&subscription] { return subscription->state() == SubscriberState::Failed; }));
    CHECK(subscription->counters().reconnects == 0);
    CHECK(script.transports_built.load() == 1);
}

TEST_CASE("teardown during a reconnect backoff neither hangs nor reopens",
          "[core][subscription]")
{
    Script script;
    {
        std::lock_guard<std::mutex> lock(script.mutex);
        script.open_results = {Status::Ok};
        script.renew_results = {Status::SessionExpired};
    }

    SubscriberConfig config = subscription_config();
    config.reconnect_backoff_ms = 400; // long enough to be inside it
    config.reconnect_max_attempts = 10;

    auto subscription = Subscription::open(
        config, fake_channel(script), fake_factory(script), noop_callbacks());

    REQUIRE(eventually(
        [&subscription] { return subscription->state() == SubscriberState::Reconnecting; }));

    const int opens_before = script.opens.load();

    const auto started = std::chrono::steady_clock::now();
    subscription.reset(); // the destructor is how a session stops
    const auto elapsed = std::chrono::steady_clock::now() - started;

    CHECK(elapsed < std::chrono::milliseconds{300});

    CHECK(script.opens.load() == opens_before);
}


TEST_CASE("a reopened session that describes a different array is refused",
          "[core][subscription]")
{
    Script script;
    {
        std::lock_guard<std::mutex> lock(script.mutex);
        script.renew_results = {Status::SessionExpired}; // force one reconnect
        script.reshape_after_first = true;
    }

    SubscriberConfig config = subscription_config();
    config.delivery_mode = DeliveryMode::DispatchThread;
    config.reconnect_max_attempts = 10;

    std::mutex seen_mutex;
    BulkError final_error;
    SubscriptionCallbacks callbacks = noop_callbacks();
    callbacks.on_state = [&](SubscriberState state, const BulkError &error) {
        if(state == SubscriberState::Failed)
        {
            std::lock_guard<std::mutex> lock(seen_mutex);
            final_error = error;
        }
    };

    auto subscription = Subscription::open(
        config, fake_channel(script), fake_factory(script), std::move(callbacks));

    REQUIRE(subscription->granted_geometry().shape[0] == 8);

    REQUIRE(eventually([&subscription] { return subscription->state() == SubscriberState::Failed; }));

    CHECK(subscription->counters().geometry_changes == 1);

    CHECK(subscription->counters().reconnects <= 1);

    REQUIRE(eventually([&] {
        std::lock_guard<std::mutex> lock(seen_mutex);
        return final_error.status != Status::Ok;
    }));

    {
        std::lock_guard<std::mutex> lock(seen_mutex);
        CHECK(final_error.status == Status::GeometryMismatch);
        CHECK(final_error.message.find("different array") != std::string::npos);
    }

    CHECK(subscription->granted_geometry().generation == 0);

    CHECK(script.transports_built.load() >= 2);
    CHECK(script.activations.load() == 1);
}

TEST_CASE("a reopened session with the same array is adopted normally",
          "[core][subscription]")
{
    Script script;
    {
        std::lock_guard<std::mutex> lock(script.mutex);
        script.renew_results = {Status::SessionExpired};
        script.reshape_after_first = false;
    }

    auto subscription = Subscription::open(
        subscription_config(), fake_channel(script), fake_factory(script), noop_callbacks());

    REQUIRE(eventually([&script] { return script.transports_built.load() >= 2; }));
    REQUIRE(eventually([&subscription] { return subscription->state() == SubscriberState::Active; }));

    CHECK(subscription->counters().geometry_changes == 0);
    CHECK(subscription->granted_geometry().shape[0] == 8);
}


TEST_CASE("coordination calls never overlap", "[core][subscription]")
{
    Script script;

    auto subscription = Subscription::open(
        subscription_config(), fake_channel(script), fake_factory(script), noop_callbacks());

    REQUIRE(eventually([&script] { return script.renews.load() >= 3; }));
    subscription.reset(); // sends Close, so teardown is covered too

    CHECK(script.channel_max_concurrent.load() == 1);

    std::lock_guard<std::mutex> lock(script.mutex);
    CHECK(script.channel_threads.size() == 2);
    CHECK(script.channel_threads.count(std::this_thread::get_id()) == 1);
}


TEST_CASE("the granted geometry survives to the interface", "[core][subscription]")
{
    Script script;

    auto subscription = Subscription::open(
        subscription_config(), fake_channel(script), fake_factory(script), noop_callbacks());

    const Protocol::GeometryBlock granted = subscription->granted_geometry();

    CHECK(granted.generation == 1);
    CHECK(granted.element_type == ElementType::UInt16);
    CHECK(granted.element_size == 2);
    CHECK(granted.rank == 2);
    CHECK(granted.shape[0] == 8);
    CHECK(granted.shape[1] == 8);
    CHECK(granted.strides[0] == 16);
    CHECK(granted.strides[1] == 2);

    subscription.reset();
    CHECK(subscription == nullptr);
}

TEST_CASE("no session means an all-zero geometry, not a stale one",
          "[core][subscription]")
{
    Script script;
    script.probe_arrives = false;

    SubscriberConfig config = subscription_config();
    config.reconnect_policy = ReconnectPolicy::FailFast;
    config.probe_timeout_ms = 30;

    CHECK_THROWS_AS(Subscription::open(config,
                                                    fake_channel(script),
                                                    fake_factory(script),
                                                    noop_callbacks()),
                    BulkException);

    const Protocol::GeometryBlock none;
    CHECK(none.generation == 0);
}


TEST_CASE("poll() is refused when a dispatch thread is already delivering",
          "[core][subscription]")
{
    Script script;
    SubscriberConfig config = subscription_config();
    config.delivery_mode = DeliveryMode::DispatchThread;

    auto subscription = Subscription::open(
        config, fake_channel(script), fake_factory(script), noop_callbacks());

    CHECK_THROWS_AS(subscription->poll(std::chrono::milliseconds{1}), BulkException);
}


struct Delivered
{
    std::mutex mutex;
    std::vector<std::uint64_t> sequences;
    std::vector<std::uint16_t> first_elements;
    std::vector<std::size_t> sizes;

    std::set<std::thread::id> threads;

    void record(const FrameView &frame)
    {
        std::lock_guard<std::mutex> lock(mutex);
        sequences.push_back(frame.sequence());
        sizes.push_back(frame.size());
        first_elements.push_back(
            frame.data() != nullptr ? *reinterpret_cast<const std::uint16_t *>(frame.data()) : 0);
        threads.insert(std::this_thread::get_id());
    }

    std::size_t count()
    {
        std::lock_guard<std::mutex> lock(mutex);
        return sequences.size();
    }

    std::size_t thread_count()
    {
        std::lock_guard<std::mutex> lock(mutex);
        return threads.size();
    }

    bool on(std::thread::id id)
    {
        std::lock_guard<std::mutex> lock(mutex);
        return threads.count(id) != 0;
    }
};

TEST_CASE("a frame the transport received reaches the application", "[core][subscription]")
{
    Script script;
    Delivered seen;

    script.receive(1);
    script.receive(2);
    script.receive(3);

    SubscriptionCallbacks callbacks = noop_callbacks();
    callbacks.on_frame = [&seen](FrameView frame) { seen.record(frame); };

    auto subscription = Subscription::open(
        subscription_config(), fake_channel(script), fake_factory(script), std::move(callbacks));

    CHECK(subscription->poll(std::chrono::milliseconds{200}) == 3);

    REQUIRE(seen.count() == 3);
    CHECK(seen.sequences == std::vector<std::uint64_t>{1, 2, 3});

    CHECK(seen.first_elements == std::vector<std::uint16_t>{1, 2, 3});
    CHECK(seen.sizes == std::vector<std::size_t>(3, std::size_t{128}));
}

TEST_CASE("frames arriving after the open are delivered too", "[core][subscription]")
{
    Script script;
    Delivered seen;

    SubscriptionCallbacks callbacks = noop_callbacks();
    callbacks.on_frame = [&seen](FrameView frame) { seen.record(frame); };

    auto subscription = Subscription::open(
        subscription_config(), fake_channel(script), fake_factory(script), std::move(callbacks));

    CHECK(subscription->poll(std::chrono::milliseconds{5}) == 0);

    script.receive(7);
    CHECK(subscription->poll(std::chrono::milliseconds{500}) == 1);
    REQUIRE(seen.count() == 1);
    CHECK(seen.sequences.front() == 7);
}

TEST_CASE("a dispatch thread delivers without the application asking",
          "[core][subscription]")
{
    Script script;
    Delivered seen;

    SubscriberConfig config = subscription_config();
    config.delivery_mode = DeliveryMode::DispatchThread;

    SubscriptionCallbacks callbacks = noop_callbacks();
    callbacks.on_frame = [&seen](FrameView frame) { seen.record(frame); };

    auto subscription = Subscription::open(
        config, fake_channel(script), fake_factory(script), std::move(callbacks));

    for(std::uint64_t sequence = 1; sequence <= 4; ++sequence)
    {
        script.receive(sequence);
    }

    REQUIRE(eventually([&seen] { return seen.count() == 4; }));

    CHECK(seen.sequences == std::vector<std::uint64_t>{1, 2, 3, 4});
    CHECK(seen.thread_count() == 1);
    CHECK_FALSE(seen.on(std::this_thread::get_id()));
}

TEST_CASE("poll() delivers no more frames than it was asked for", "[core][subscription]")
{
    Script script;
    Delivered seen;

    for(std::uint64_t sequence = 1; sequence <= 5; ++sequence)
    {
        script.receive(sequence);
    }

    SubscriptionCallbacks callbacks = noop_callbacks();
    callbacks.on_frame = [&seen](FrameView frame) { seen.record(frame); };

    auto subscription = Subscription::open(
        subscription_config(), fake_channel(script), fake_factory(script), std::move(callbacks));

    CHECK(subscription->poll(std::chrono::milliseconds{200}, 2) == 2);
    CHECK(seen.count() == 2);

    CHECK(subscription->poll(std::chrono::milliseconds{200}) == 3);
    REQUIRE(seen.count() == 5);
    CHECK(seen.sequences == std::vector<std::uint64_t>{1, 2, 3, 4, 5});
}

TEST_CASE("frames are delivered on the polling thread, never on a coordination thread",
          "[core][subscription]")
{
    Script script;
    Delivered seen;

    script.receive(1);
    script.receive(2);

    SubscriptionCallbacks callbacks = noop_callbacks();
    callbacks.on_frame = [&seen](FrameView frame) { seen.record(frame); };

    auto subscription = Subscription::open(
        subscription_config(), fake_channel(script), fake_factory(script), std::move(callbacks));

    CHECK(subscription->poll(std::chrono::milliseconds{200}) == 2);

    CHECK(seen.thread_count() == 1);
    CHECK(seen.on(std::this_thread::get_id()));
}

TEST_CASE("a frame the application kept outlives the session that delivered it",
          "[core][subscription]")
{
    Script script;
    script.receive(42);

    FrameView retained;

    SubscriptionCallbacks callbacks = noop_callbacks();
    callbacks.on_frame = [&retained](FrameView frame) { retained = std::move(frame); };

    auto subscription = Subscription::open(
        subscription_config(), fake_channel(script), fake_factory(script), std::move(callbacks));

    CHECK(subscription->poll(std::chrono::milliseconds{200}) == 1);
    REQUIRE(static_cast<bool>(retained));

    subscription.reset();

    REQUIRE(static_cast<bool>(retained));
    CHECK(retained.sequence() == 42);
    CHECK(retained.size() == 128);
    CHECK(retained.rank() == 2);
    CHECK(*reinterpret_cast<const std::uint16_t *>(retained.data()) == 42);
}

TEST_CASE("a frame delivered before a reconnect survives the session that replaced it",
          "[core][subscription]")
{
    Script script;
    script.receive(11);

    FrameView retained;

    SubscriptionCallbacks callbacks = noop_callbacks();
    callbacks.on_frame = [&retained](FrameView frame) { retained = std::move(frame); };

    auto subscription = Subscription::open(
        subscription_config(), fake_channel(script), fake_factory(script), std::move(callbacks));

    CHECK(subscription->poll(std::chrono::milliseconds{200}) == 1);
    REQUIRE(static_cast<bool>(retained));

    script.refuse_next_renew(Status::SessionExpired);
    REQUIRE(eventually([&script] { return script.transports_built.load() >= 2; }));

    CHECK(retained.sequence() == 11);
    CHECK(*reinterpret_cast<const std::uint16_t *>(retained.data()) == 11);

    script.receive(12);
    CHECK(eventually([&] { return subscription->poll(std::chrono::milliseconds{50}) == 1; }));
}


bool readable(int fd)
{
    pollfd descriptor{};
    descriptor.fd = fd;
    descriptor.events = POLLIN;
    return ::poll(&descriptor, 1, 0) > 0;
}

TEST_CASE("a consumer blocked in poll() does not delay replacing the transport",
          "[core][subscription]")
{
    Script script;

    auto subscription = Subscription::open(
        subscription_config(), fake_channel(script), fake_factory(script), noop_callbacks());

    REQUIRE(eventually([&script] { return script.transports_built.load() == 1; }));

    std::atomic<bool> still_polling{true};
    std::thread consumer([&] {
        subscription->poll(std::chrono::seconds{2});
        still_polling.store(false);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds{50});

    const auto started = std::chrono::steady_clock::now();
    script.refuse_next_renew(Status::SessionExpired);

    const bool replaced = eventually([&script] { return script.transports_built.load() >= 2; },
                                     std::chrono::milliseconds{1'000});
    const auto elapsed = std::chrono::steady_clock::now() - started;

    CHECK(replaced);
    CHECK(elapsed < std::chrono::milliseconds{1'000});

    CHECK(still_polling.load());

    script.receive(1);
    consumer.join();
}

TEST_CASE("the readiness descriptor survives a reconnect", "[core][subscription]")
{
    Script script;

    auto subscription = Subscription::open(
        subscription_config(), fake_channel(script), fake_factory(script), noop_callbacks());

    const int before = subscription->fd();
    REQUIRE(before >= 0);

    script.refuse_next_renew(Status::SessionExpired);
    REQUIRE(eventually([&script] { return script.transports_built.load() >= 2; }));

    CHECK(subscription->fd() == before);

    Delivered seen;
    script.receive(9);
    REQUIRE(eventually([&] { return subscription->poll(std::chrono::milliseconds{50}) == 1; }));
}

TEST_CASE("an empty poll arms the descriptor, and a frame makes it readable",
          "[core][subscription]")
{
    Script script;
    Delivered seen;

    SubscriptionCallbacks callbacks = noop_callbacks();
    callbacks.on_frame = [&seen](FrameView frame) { seen.record(frame); };

    auto subscription = Subscription::open(
        subscription_config(), fake_channel(script), fake_factory(script), std::move(callbacks));

    REQUIRE(subscription->fd() >= 0);

    CHECK(subscription->poll(std::chrono::milliseconds{0}) == 0);
    CHECK_FALSE(readable(subscription->fd()));

    script.receive(4);

    CHECK(readable(subscription->fd()));
    CHECK(subscription->poll(std::chrono::milliseconds{0}) == 1);
    REQUIRE(seen.count() == 1);
    CHECK(seen.sequences.front() == 4);
}

TEST_CASE("frames the retired session left behind are discarded, not delivered later",
          "[core][subscription]")
{
    Script script;
    Delivered seen;

    SubscriptionCallbacks callbacks = noop_callbacks();
    callbacks.on_frame = [&seen](FrameView frame) { seen.record(frame); };

    auto subscription = Subscription::open(
        subscription_config(), fake_channel(script), fake_factory(script), std::move(callbacks));

    script.receive(1);
    REQUIRE(eventually([&subscription] {
        return subscription->counters().delivery_queue_depth == 1;
    }));

    script.refuse_next_renew(Status::SessionExpired);
    REQUIRE(eventually([&script] { return script.transports_built.load() >= 2; }));

    CHECK(subscription->poll(std::chrono::milliseconds{50}) == 0);
    CHECK(seen.count() == 0);

    script.receive(2);
    REQUIRE(eventually([&] { return subscription->poll(std::chrono::milliseconds{50}) == 1; }));
    REQUIRE(seen.count() == 1);
    CHECK(seen.sequences.front() == 2);
}


TEST_CASE("a refusal sent as an Error message is adopted like any other",
          "[core][subscription]")
{
    Script script;
    script.refuse_with_error_message = true;
    script.open_results = {Status::NotAuthorized};

    SubscriberConfig config = subscription_config();
    config.reconnect_policy = ReconnectPolicy::FailFast;

    try
    {
        Subscription::open(
            config, fake_channel(script), fake_factory(script), noop_callbacks());
        FAIL("the open should have been refused");
    }
    catch(const BulkException &e)
    {
        CHECK(e.error().status == Status::NotAuthorized);
    }

    CHECK(script.opens.load() == 1);
    CHECK(script.closes.load() == 0);
}

TEST_CASE("a reply that does not decode is a refusal, not a crash",
          "[core][subscription]")
{
    Script script;
    script.garble_replies = 1;

    SubscriberConfig config = subscription_config();
    config.reconnect_policy = ReconnectPolicy::FailFast;

    try
    {
        Subscription::open(
            config, fake_channel(script), fake_factory(script), noop_callbacks());
        FAIL("an undecodable reply should have been refused");
    }
    catch(const BulkException &e)
    {
        CHECK(e.error().status == Status::MalformedMessage);
    }
}

TEST_CASE("the lease terms the publisher granted are what the renew timer uses",
          "[core][subscription]")
{
    Script script;
    script.renew_interval_ms = 15;
    script.lease_ttl_ms = 150;

    auto subscription = Subscription::open(
        subscription_config(), fake_channel(script), fake_factory(script), noop_callbacks());

    REQUIRE(eventually([&script] { return script.renews.load() >= 3; },
                       std::chrono::milliseconds{800}));
}

} // namespace
