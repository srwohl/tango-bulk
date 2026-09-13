// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include "fake_transport.h"

#include <catch2/catch_test_macros.hpp>

#include <poll.h>

#include <atomic>
#include <exception>
#include <mutex>
#include <set>
#include <stdexcept>
#include <thread>
#include <vector>

namespace
{

using namespace TangoBulk;
using namespace TangoBulkTests;

SubscriberState state_of(const Subscription &subscription)
{
    return subscription.snapshot().state;
}

SubscriberCounters counters_of(const Subscription &subscription)
{
    return subscription.snapshot().counters;
}

TEST_CASE("invalid options are refused before anything is built", "[core][subscription]")
{
    Script script;

    SubscriptionOptions options = subscription_options();
    options.stream_name = "not a stream name";

    CHECK_THROWS_AS(open_test_subscription(options, fake_channel(script), fake_factory(script)),
                    ConfigurationError);

    CHECK(script.transports_built.load() == 0);
    CHECK(script.opens.load() == 0);
}

TEST_CASE("a granted session that never probes gives up within its lease",
          "[core][subscription]")
{
    Script script;
    script.probe_arrives = false;
    script.lease_ttl_ms = 80;

    SubscriptionOptions options = subscription_options();
    options.recovery_policy = RecoveryPolicy::Fail;

    const auto started = std::chrono::steady_clock::now();
    CHECK_THROWS_AS(open_test_subscription(options, fake_channel(script), fake_factory(script)),
                    EstablishmentError);
    const auto elapsed = std::chrono::steady_clock::now() - started;

    CHECK(elapsed >= std::chrono::milliseconds{80});
    CHECK(elapsed < std::chrono::milliseconds{800});

    CHECK(script.closes.load() >= 1);
}

TEST_CASE("bounded retry completes initial establishment before returning",
          "[core][subscription]")
{
    Script script;
    script.open_results = {Status::UnknownStream, Status::Ok};

    auto subscription =
        open_test_subscription(subscription_options(), fake_channel(script), fake_factory(script));

    CHECK(state_of(*subscription) == SubscriberState::Active);
    CHECK(script.opens.load() == 2);
    CHECK(subscription->geometry().generation == 1);
}

TEST_CASE("initial retries share one absolute coordination deadline", "[core][subscription]")
{
    Script script;
    script.open_results = {Status::TransportFailure};

    SubscriptionOptions options = subscription_options();
    options.establishment_timeout_ms = 200;

    const auto started = std::chrono::steady_clock::now();
    auto subscription = open_test_subscription(options, fake_channel(script), fake_factory(script));

    std::vector<std::chrono::steady_clock::time_point> open_deadlines;
    {
        std::lock_guard<std::mutex> lock(script.mutex);
        for(const auto &[kind, deadline] : script.channel_deadlines)
        {
            if(kind == Protocol::CoordType::Open)
            {
                open_deadlines.push_back(deadline);
            }
        }
    }

    REQUIRE(open_deadlines.size() == 2);
    CHECK(open_deadlines[0] == open_deadlines[1]);
    CHECK(open_deadlines[0] > started);
    // Allow the clock to advance between recording `started` and entering the
    // factory; the contract under test is that the retry does not extend the
    // same deadline, not that the caller's timestamp is the construction time.
    CHECK(open_deadlines[0] <= started + std::chrono::milliseconds{250});
}

TEST_CASE("RenewTooFrequent slows the timer and does not end the session",
          "[core][subscription]")
{
    Script script;
    {
        std::lock_guard<std::mutex> lock(script.mutex);
        script.renew_results = {Status::RenewTooFrequent, Status::RenewTooFrequent};
    }

    auto subscription =
        open_test_subscription(subscription_options(), fake_channel(script), fake_factory(script));

    REQUIRE(state_of(*subscription) == SubscriberState::Active);
    REQUIRE(eventually([&script] { return script.renews.load() >= 3; }));

    CHECK(state_of(*subscription) == SubscriberState::Active);
    CHECK(counters_of(*subscription).reconnects == 0);

    CHECK(script.transports_built.load() == 1);
}

TEST_CASE("a lost session is not resurrected, it is replaced", "[core][subscription]")
{
    Script script;
    {
        std::lock_guard<std::mutex> lock(script.mutex);
        script.renew_results = {Status::SessionExpired};
    }

    auto subscription =
        open_test_subscription(subscription_options(), fake_channel(script), fake_factory(script));

    REQUIRE(eventually([&script] { return script.transports_built.load() >= 2; }));
    CHECK(eventually([&subscription] { return state_of(*subscription) == SubscriberState::Active; }));
    CHECK(script.opens.load() >= 2);
    CHECK(counters_of(*subscription).reconnects >= 1);
}

TEST_CASE("the client instance id is stable across reopen", "[core][subscription]")
{
    Script script;
    script.refuse_next_renew(Status::SessionExpired);

    auto subscription =
        open_test_subscription(subscription_options(), fake_channel(script), fake_factory(script));

    REQUIRE(eventually([&script] { return script.transports_built.load() >= 2; }));

    std::lock_guard<std::mutex> lock(script.mutex);
    REQUIRE(script.client_ids.size() >= 2);
    CHECK_FALSE(script.client_ids.front().is_zero());
    for(const Protocol::ClientInstanceId &id : script.client_ids)
    {
        CHECK(id == script.client_ids.front());
    }
}

TEST_CASE("disabled recovery does not retry", "[core][subscription]")
{
    Script script;
    {
        std::lock_guard<std::mutex> lock(script.mutex);
        script.open_results = {Status::Ok};
        script.renew_results = {Status::SessionExpired};
    }

    SubscriptionOptions options = subscription_options();
    options.recovery_policy = RecoveryPolicy::Fail;

    auto subscription = open_test_subscription(options, fake_channel(script), fake_factory(script));

    REQUIRE(eventually([&subscription] { return state_of(*subscription) == SubscriberState::Failed; }));
    CHECK(counters_of(*subscription).reconnects == 0);
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

    auto subscription =
        open_test_subscription(subscription_options(), fake_channel(script), fake_factory(script));

    REQUIRE(eventually(
        [&subscription] { return state_of(*subscription) == SubscriberState::Reconnecting; }));

    const int opens_before = script.opens.load();

    const auto started = std::chrono::steady_clock::now();
    subscription.reset(); // the destructor is how a session stops
    const auto elapsed = std::chrono::steady_clock::now() - started;

    CHECK(elapsed < std::chrono::milliseconds{300});

    CHECK(script.opens.load() == opens_before);
}

/// Records the terminal event a push callback receives, if any.
struct TerminalSink
{
    std::mutex mutex;
    std::exception_ptr error;
    std::atomic<int> terminal_events{0};
    std::atomic<int> frames{0};

    FrameCallback callback()
    {
        return [this](FrameEvent event)
        {
            if(event.terminal())
            {
                std::lock_guard<std::mutex> lock(mutex);
                error = event.error;
                terminal_events.fetch_add(1);
                return;
            }
            frames.fetch_add(1);
        };
    }

    template <typename Exception>
    bool holds()
    {
        std::lock_guard<std::mutex> lock(mutex);
        if(!error)
        {
            return false;
        }
        try
        {
            std::rethrow_exception(error);
        }
        catch(const Exception &)
        {
            return true;
        }
        catch(...)
        {
        }
        return false;
    }
};

TEST_CASE("a reopened session that describes a different array is refused",
          "[core][subscription]")
{
    Script script;
    {
        std::lock_guard<std::mutex> lock(script.mutex);
        script.renew_results = {Status::SessionExpired}; // force one reconnect
        script.reshape_after_first = true;
    }

    TerminalSink sink;
    auto subscription =
        open_test_subscription(push_options(sink.callback()), fake_channel(script), fake_factory(script));

    REQUIRE(subscription->geometry().shape[0] == 8);

    REQUIRE(eventually([&subscription] { return state_of(*subscription) == SubscriberState::Failed; }));

    CHECK(counters_of(*subscription).geometry_changes == 1);

    CHECK(counters_of(*subscription).reconnects <= 1);

    const SubscriptionSnapshot snapshot = subscription->snapshot();
    CHECK(snapshot.error.status == Status::GeometryMismatch);
    CHECK(snapshot.error.message.find("different array") != std::string::npos);

    CHECK(subscription->geometry().generation == 0);

    CHECK(script.transports_built.load() >= 2);
    CHECK(script.activations.load() == 1);

    // The callback learns of it once, as the last event, typed.
    REQUIRE(eventually([&sink] { return sink.terminal_events.load() == 1; }));
    CHECK(sink.holds<GeometryChanged>());
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

    auto subscription =
        open_test_subscription(subscription_options(), fake_channel(script), fake_factory(script));

    REQUIRE(eventually([&script] { return script.transports_built.load() >= 2; }));
    REQUIRE(eventually([&subscription] { return state_of(*subscription) == SubscriberState::Active; }));

    CHECK(counters_of(*subscription).geometry_changes == 0);
    CHECK(subscription->geometry().shape[0] == 8);
}

TEST_CASE("a geometry expectation the grant does not meet fails establishment",
          "[core][subscription]")
{
    Script script;

    SubscriptionOptions options = subscription_options();
    options.recovery_policy = RecoveryPolicy::Fail;
    options.expect.element_type = ElementType::Float64;

    try
    {
        open_test_subscription(options, fake_channel(script), fake_factory(script));
        FAIL("the expectation should have refused the grant");
    }
    catch(const EstablishmentError &e)
    {
        CHECK(e.error().status == Status::GeometryMismatch);
    }

    // The session was granted and then given back; no transport was ever armed.
    CHECK(script.opens.load() == 1);
    CHECK(script.closes.load() == 1);
    CHECK(script.activations.load() == 0);
}

TEST_CASE("a geometry expectation the grant meets is accepted", "[core][subscription]")
{
    Script script;

    SubscriptionOptions options = subscription_options();
    options.expect.element_type = ElementType::UInt16;
    options.expect.rank = 2;
    options.expect.shape = std::array<std::uint64_t, k_max_rank>{8, 8, 0, 0};

    auto subscription = open_test_subscription(options, fake_channel(script), fake_factory(script));
    CHECK(state_of(*subscription) == SubscriberState::Active);
}

TEST_CASE("a shape expectation without a rank is invalid", "[core][subscription]")
{
    SubscriptionOptions options = subscription_options();
    options.expect.shape = std::array<std::uint64_t, k_max_rank>{8, 8, 0, 0};
    CHECK(options.validate() == Status::GeometryMismatch);

    options.expect.rank = 2;
    CHECK(options.validate() == Status::Ok);
}

TEST_CASE("coordination calls never overlap", "[core][subscription]")
{
    Script script;

    auto subscription =
        open_test_subscription(subscription_options(), fake_channel(script), fake_factory(script));

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

    auto subscription =
        open_test_subscription(subscription_options(), fake_channel(script), fake_factory(script));

    const Geometry granted = subscription->geometry();

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

TEST_CASE("the snapshot carries the lease terms and renewal health", "[core][subscription]")
{
    Script script;

    auto subscription =
        open_test_subscription(subscription_options(), fake_channel(script), fake_factory(script));

    REQUIRE(eventually([&script] { return script.renews.load() >= 2; }));

    const SubscriptionSnapshot snapshot = subscription->snapshot();
    CHECK(snapshot.state == SubscriberState::Active);
    CHECK_FALSE(snapshot.session_id.empty());
    CHECK(snapshot.lease_ttl_ms == 200);
    CHECK(snapshot.renew_interval_ms == 20);
    CHECK(snapshot.last_renewal_steady_ns > 0);
    CHECK(snapshot.sampled_at_steady_ns >= snapshot.last_renewal_steady_ns);
    CHECK(snapshot.counters.renewals_sent >= 2);
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

    FrameCallback callback()
    {
        return [this](FrameEvent event)
        {
            if(!event.terminal())
            {
                record(event.frame);
            }
        };
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

    auto subscription =
        open_test_subscription(push_options(seen.callback()), fake_channel(script), fake_factory(script));

    REQUIRE(eventually([&seen] { return seen.count() == 3; }));

    REQUIRE(seen.count() == 3);
    CHECK(seen.sequences == std::vector<std::uint64_t>{1, 2, 3});

    CHECK(seen.first_elements == std::vector<std::uint16_t>{1, 2, 3});
    CHECK(seen.sizes == std::vector<std::size_t>(3, std::size_t{128}));
}

TEST_CASE("Pull delivery needs no callback and returns frames directly", "[core][subscription]")
{
    Script script;

    auto subscription =
        open_test_subscription(subscription_options(), fake_channel(script), fake_factory(script));

    script.receive(9);
    const std::optional<FrameView> frame = subscription->read_for(200ms);

    REQUIRE(frame.has_value());
    CHECK(frame->sequence() == 9);
    CHECK(subscription->try_read() == std::nullopt);
    CHECK(subscription->fd() >= 0);
}

TEST_CASE("pull operations on a push subscription are caller misuse", "[core][subscription]")
{
    Script script;

    auto subscription =
        open_test_subscription(push_options(), fake_channel(script), fake_factory(script));

    CHECK_THROWS_AS(subscription->try_read(), DeliveryModeError);
    CHECK_THROWS_AS(subscription->read_for(1ms), DeliveryModeError);
    CHECK(subscription->fd() == -1);
    CHECK(state_of(*subscription) == SubscriberState::Active);
}

TEST_CASE("orderly close and interruption report their own types", "[core][subscription]")
{
    Script script;

    auto closed =
        open_test_subscription(subscription_options(), fake_channel(script), fake_factory(script));
    closed->close();
    CHECK_THROWS_AS(closed->try_read(), StreamClosed);
    CHECK_THROWS_AS(closed->read_for(1ms), StreamClosed);

    Script other;
    auto interrupted =
        open_test_subscription(subscription_options(), fake_channel(other), fake_factory(other));
    interrupted->interrupt();
    CHECK_THROWS_AS(interrupted->try_read(), Interrupted);
    CHECK_THROWS_AS(interrupted->read_for(1ms), Interrupted);
}

TEST_CASE("a terminal session failure reaches a pull reader after its frames",
          "[core][subscription]")
{
    Script script;

    SubscriptionOptions options = subscription_options();
    options.recovery_policy = RecoveryPolicy::Fail;

    auto subscription = open_test_subscription(options, fake_channel(script), fake_factory(script));

    script.receive(5);
    script.refuse_next_renew(Status::SessionExpired);
    REQUIRE(eventually([&subscription] { return state_of(*subscription) == SubscriberState::Failed; }));

    const std::optional<FrameView> frame = subscription->read_for(200ms);
    REQUIRE(frame.has_value());
    CHECK(frame->sequence() == 5);

    CHECK_THROWS_AS(subscription->try_read(), SessionLost);
}

TEST_CASE("a terminal session failure reaches the push callback exactly once",
          "[core][subscription]")
{
    Script script;
    TerminalSink sink;

    SubscriptionOptions options = push_options(sink.callback());
    options.recovery_policy = RecoveryPolicy::Fail;

    auto subscription = open_test_subscription(options, fake_channel(script), fake_factory(script));

    script.receive(1);
    script.refuse_next_renew(Status::SessionExpired);

    REQUIRE(eventually([&sink] { return sink.terminal_events.load() == 1; }));
    CHECK(sink.frames.load() == 1);
    CHECK(sink.holds<SessionLost>());
    CHECK(state_of(*subscription) == SubscriberState::Failed);

    // Nothing follows the terminal event, close included.
    subscription->close();
    CHECK(sink.terminal_events.load() == 1);
}

TEST_CASE("a dispatch callback may destroy its subscription",
          "[core][subscription]")
{
    Script script;

    std::unique_ptr<Subscription> subscription;
    std::atomic<bool> callback_finished{false};

    SubscriptionOptions options = push_options([&](FrameEvent) {
        subscription.reset();
        callback_finished.store(true, std::memory_order_release);
    });

    subscription = open_test_subscription(options, fake_channel(script), fake_factory(script));

    script.receive(1);
    REQUIRE(eventually([&callback_finished] {
        return callback_finished.load(std::memory_order_acquire);
    }));
    CHECK_FALSE(subscription);
}

TEST_CASE("a dispatch callback failure terminates delivery once",
          "[core][subscription]")
{
    Script script;
    script.receive(1);
    script.receive(2);

    auto subscription = open_test_subscription(
        push_options([](FrameEvent) { throw std::runtime_error("callback failed"); }),
        fake_channel(script),
        fake_factory(script));

    REQUIRE(eventually([&subscription] { return state_of(*subscription) == SubscriberState::Failed; }));
    CHECK(counters_of(*subscription).delivery_queue_depth == 0);
}

TEST_CASE("close is explicit, idempotent, and returns the subscription to Closed",
          "[core][subscription]")
{
    Script script;
    auto subscription =
        open_test_subscription(subscription_options(), fake_channel(script), fake_factory(script));

    script.receive(1);
    REQUIRE(eventually([&subscription] {
        return counters_of(*subscription).delivery_queue_depth == 1;
    }));
    subscription->close();
    CHECK(state_of(*subscription) == SubscriberState::Closed);
    CHECK(counters_of(*subscription).delivery_queue_depth == 0);
    CHECK(script.closes.load() == 1);

    subscription->close();
    CHECK(state_of(*subscription) == SubscriberState::Closed);
    CHECK(script.closes.load() == 1);
}

TEST_CASE("interrupt is sticky and distinct from an orderly close",
          "[core][subscription]")
{
    Script script;
    auto subscription =
        open_test_subscription(subscription_options(), fake_channel(script), fake_factory(script));

    subscription->interrupt();
    CHECK(subscription->snapshot().interrupted);
    CHECK(script.closes.load() == 0);

    subscription->interrupt();
    CHECK(subscription->snapshot().interrupted);

    subscription->close();
    CHECK(script.closes.load() == 1);
}

TEST_CASE("frames arriving after the open are delivered too", "[core][subscription]")
{
    Script script;
    Delivered seen;

    auto subscription =
        open_test_subscription(push_options(seen.callback()), fake_channel(script), fake_factory(script));

    script.receive(7);
    REQUIRE(eventually([&seen] { return seen.count() == 1; }));
    CHECK(seen.sequences.front() == 7);
}

TEST_CASE("a dispatch thread delivers without the application asking",
          "[core][subscription]")
{
    Script script;
    Delivered seen;

    auto subscription =
        open_test_subscription(push_options(seen.callback()), fake_channel(script), fake_factory(script));

    for(std::uint64_t sequence = 1; sequence <= 4; ++sequence)
    {
        script.receive(sequence);
    }

    REQUIRE(eventually([&seen] { return seen.count() == 4; }));

    CHECK(seen.sequences == std::vector<std::uint64_t>{1, 2, 3, 4});
    CHECK(seen.thread_count() == 1);
    CHECK_FALSE(seen.on(std::this_thread::get_id()));
}

TEST_CASE("a frame the application kept outlives the session that delivered it",
          "[core][subscription]")
{
    Script script;
    script.receive(42);

    FrameView retained;
    std::mutex retained_mutex;

    auto subscription = open_test_subscription(
        push_options([&retained, &retained_mutex](FrameEvent event)
                     {
                         const std::lock_guard<std::mutex> lock(retained_mutex);
                         retained = std::move(event.frame);
                     }),
        fake_channel(script),
        fake_factory(script));

    REQUIRE(eventually([&retained, &retained_mutex]
                       {
                           const std::lock_guard<std::mutex> lock(retained_mutex);
                           return static_cast<bool>(retained);
                       }));

    subscription.reset();

    const std::lock_guard<std::mutex> lock(retained_mutex);
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

    std::vector<FrameView> retained;
    std::mutex retained_mutex;

    auto subscription = open_test_subscription(
        push_options([&retained, &retained_mutex](FrameEvent event)
                     {
                         if(event.terminal())
                         {
                             return;
                         }
                         const std::lock_guard<std::mutex> lock(retained_mutex);
                         retained.push_back(std::move(event.frame));
                     }),
        fake_channel(script),
        fake_factory(script));

    REQUIRE(eventually([&retained, &retained_mutex]
                       {
                           const std::lock_guard<std::mutex> lock(retained_mutex);
                           return retained.size() >= 1;
                       }));

    script.refuse_next_renew(Status::SessionExpired);
    REQUIRE(eventually([&script] { return script.transports_built.load() >= 2; }));

    {
        const std::lock_guard<std::mutex> lock(retained_mutex);
        REQUIRE(retained.size() >= 1);
        CHECK(retained[0].sequence() == 11);
        CHECK(*reinterpret_cast<const std::uint16_t *>(retained[0].data()) == 11);
    }

    script.receive(12);
    REQUIRE(eventually([&retained, &retained_mutex]
                       {
                           const std::lock_guard<std::mutex> lock(retained_mutex);
                           return retained.size() >= 2;
                       }));
    const std::lock_guard<std::mutex> lock(retained_mutex);
    CHECK(retained[1].sequence() == 12);
}

/// A caller region the way an application would supply one.
struct CountingAllocator
{
    std::atomic<int> calls{0};
    std::uint64_t asked{0};

    ReceiveAllocator allocator()
    {
        return [this](std::uint64_t bytes)
        {
            calls.fetch_add(1);
            asked = bytes;
            auto storage = std::make_shared<std::vector<std::byte>>(bytes);
            return ReceiveRegion{std::shared_ptr<void>(storage, storage->data()),
                                 bytes,
                                 MemoryKind::Host};
        };
    }
};

TEST_CASE("the receive allocator runs once and its region reaches every transport",
          "[core][subscription]")
{
    Script script;
    CountingAllocator memory;

    SubscriptionOptions options = subscription_options();
    options.receive_allocator = memory.allocator();

    auto subscription = open_test_subscription(options, fake_channel(script), fake_factory(script));
    REQUIRE(state_of(*subscription) == SubscriberState::Active);
    CHECK(memory.calls.load() == 1);
    CHECK(memory.asked == options.receive_plan->pinned_bytes());
    CHECK(script.regions_supplied.load() == 1);

    script.refuse_next_renew(Status::SessionExpired);
    REQUIRE(eventually([&script] { return script.transports_built.load() >= 2; }));
    REQUIRE(eventually([&subscription] { return state_of(*subscription) == SubscriberState::Active; }));

    CHECK(memory.calls.load() == 1);
    CHECK(script.regions_supplied.load() == 2);
}

TEST_CASE("a region smaller than the plan fails establishment as exhaustion",
          "[core][subscription]")
{
    Script script;

    SubscriptionOptions options = subscription_options();
    options.recovery_policy = RecoveryPolicy::Fail;
    options.receive_allocator = [](std::uint64_t bytes)
    {
        auto storage = std::make_shared<std::vector<std::byte>>(bytes / 2);
        return ReceiveRegion{
            std::shared_ptr<void>(storage, storage->data()), bytes / 2, MemoryKind::Host};
    };

    CHECK_THROWS_AS(open_test_subscription(options, fake_channel(script), fake_factory(script)),
                    ResourceExhausted);
    CHECK(script.transports_built.load() == 0);
}

TEST_CASE("retained receive storage is not reused while a delivered frame refers to it",
          "[core][subscription]")
{
    Script script;
    script.lease_ttl_ms = 100;
    script.receive(1);

    CountingAllocator memory;
    FrameView retained;
    std::mutex retained_mutex;

    SubscriptionOptions options = push_options(
        [&retained, &retained_mutex](FrameEvent event)
        {
            if(event.terminal())
            {
                return;
            }
            const std::lock_guard<std::mutex> lock(retained_mutex);
            retained = std::move(event.frame);
        });
    options.receive_allocator = memory.allocator();

    auto subscription = open_test_subscription(options, fake_channel(script), fake_factory(script));

    REQUIRE(eventually([&retained, &retained_mutex]
                       {
                           const std::lock_guard<std::mutex> lock(retained_mutex);
                           return static_cast<bool>(retained);
                       }));

    // The frame holds the region; the replacement session may not have it.
    script.refuse_next_renew(Status::SessionExpired);
    REQUIRE(eventually([&subscription] { return state_of(*subscription) == SubscriberState::Failed; }));

    const SubscriptionSnapshot snapshot = subscription->snapshot();
    CHECK(snapshot.error.status == Status::ResourceExhausted);
    CHECK(snapshot.counters.reconnects >= 1);
    CHECK(script.transports_built.load() == 1);
    CHECK(memory.calls.load() == 1);

    // The frame is still valid: closing ended participation, not the memory.
    const std::lock_guard<std::mutex> lock(retained_mutex);
    CHECK(retained.sequence() == 1);
    CHECK(*reinterpret_cast<const std::uint16_t *>(retained.data()) == 1);
}

bool readable(int fd)
{
    pollfd descriptor{};
    descriptor.fd = fd;
    descriptor.events = POLLIN;
    return ::poll(&descriptor, 1, 0) > 0;
}

TEST_CASE("a Pull reader blocked on delivery does not delay replacing the transport",
          "[core][subscription]")
{
    Script script;

    auto subscription =
        open_test_subscription(subscription_options(), fake_channel(script), fake_factory(script));

    REQUIRE(eventually([&script] { return script.transports_built.load() == 1; }));

    std::atomic<bool> still_reading{true};
    FrameView received;
    std::thread consumer([&] {
        const std::optional<FrameView> frame = subscription->read_for(std::chrono::seconds{2});
        if(frame)
        {
            received = std::move(*frame);
        }
        still_reading.store(false);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds{50});

    const auto started = std::chrono::steady_clock::now();
    script.refuse_next_renew(Status::SessionExpired);

    const bool replaced = eventually([&script] { return script.transports_built.load() >= 2; },
                                     std::chrono::milliseconds{1'000});
    const auto elapsed = std::chrono::steady_clock::now() - started;

    CHECK(replaced);
    CHECK(elapsed < std::chrono::milliseconds{1'000});

    CHECK(still_reading.load());

    script.receive(1);
    consumer.join();
    REQUIRE(static_cast<bool>(received));
    CHECK(received.sequence() == 1);
}

TEST_CASE("the readiness descriptor survives a reconnect", "[core][subscription]")
{
    Script script;

    auto subscription =
        open_test_subscription(subscription_options(), fake_channel(script), fake_factory(script));

    const int before = subscription->fd();
    REQUIRE(before >= 0);

    script.refuse_next_renew(Status::SessionExpired);
    REQUIRE(eventually([&script] { return script.transports_built.load() >= 2; }));

    CHECK(subscription->fd() == before);

    script.receive(9);
    const std::optional<FrameView> frame = subscription->read_for(50ms);
    REQUIRE(frame.has_value());
    CHECK(frame->sequence() == 9);
}

TEST_CASE("an empty Pull read arms the descriptor, and a frame makes it readable",
          "[core][subscription]")
{
    Script script;

    auto subscription =
        open_test_subscription(subscription_options(), fake_channel(script), fake_factory(script));

    REQUIRE(subscription->fd() >= 0);

    CHECK_FALSE(subscription->try_read().has_value());
    CHECK_FALSE(readable(subscription->fd()));

    script.receive(4);

    CHECK(readable(subscription->fd()));
    const std::optional<FrameView> frame = subscription->try_read();
    REQUIRE(frame.has_value());
    CHECK(frame->sequence() == 4);
}

TEST_CASE("frames the retired session left behind are discarded, not delivered later",
          "[core][subscription]")
{
    Script script;

    auto subscription =
        open_test_subscription(subscription_options(), fake_channel(script), fake_factory(script));

    script.receive(1);
    REQUIRE(eventually([&subscription] {
        return counters_of(*subscription).delivery_queue_depth == 1;
    }));

    script.refuse_next_renew(Status::SessionExpired);
    REQUIRE(eventually([&script] { return script.transports_built.load() >= 2; }));

    CHECK_FALSE(subscription->try_read().has_value());

    script.receive(2);
    const std::optional<FrameView> frame = subscription->read_for(50ms);
    REQUIRE(frame.has_value());
    CHECK(frame->sequence() == 2);
}

TEST_CASE("a refusal sent as an Error message is adopted like any other",
          "[core][subscription]")
{
    Script script;
    script.refuse_with_error_message = true;
    script.open_results = {Status::NotAuthorized};

    SubscriptionOptions options = subscription_options();
    options.recovery_policy = RecoveryPolicy::Fail;

    try
    {
        open_test_subscription(options, fake_channel(script), fake_factory(script));
        FAIL("the open should have been refused");
    }
    catch(const EstablishmentError &e)
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

    SubscriptionOptions options = subscription_options();
    options.recovery_policy = RecoveryPolicy::Fail;

    try
    {
        open_test_subscription(options, fake_channel(script), fake_factory(script));
        FAIL("an undecodable reply should have been refused");
    }
    catch(const EstablishmentError &e)
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

    auto subscription =
        open_test_subscription(subscription_options(), fake_channel(script), fake_factory(script));

    REQUIRE(eventually([&script] { return script.renews.load() >= 3; },
                       std::chrono::milliseconds{800}));
}

} // namespace
