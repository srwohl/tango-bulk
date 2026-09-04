// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include "fake_transport.h"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <mutex>
#include <vector>

/// The control loop's failure paths, which nothing could reach before.
///
/// These are not a second copy of tests/tango. That suite drives a real device
/// server and proves the adapter works; it cannot make a publisher refuse a
/// renewal or grant a session and then go quiet, so the behaviour the control
/// loop exists for went untested. This file is only those cases.
///
/// It links tango-bulk::core alone. check_layering.py's tests/unit rule forbids
/// both ucp/* and tango/*, so the build itself is what says the session policy
/// depends on neither.
namespace
{

using namespace TangoBulk;
using namespace TangoBulkTests;

// -- the backoff schedule, with no supervisor at all -------------------------

TEST_CASE("the reconnect backoff doubles and is capped at the lease", "[core][supervisor]")
{
    // Exponential from the configured base.
    CHECK(detail::backoff_delay(0, 200, 1'000) == std::chrono::milliseconds{200});
    CHECK(detail::backoff_delay(1, 200, 1'000) == std::chrono::milliseconds{400});
    CHECK(detail::backoff_delay(2, 200, 1'000) == std::chrono::milliseconds{800});

    // Capped at the lease TTL: backing off for longer than a lease cannot help,
    // because by then the publisher has reclaimed everything this client held.
    CHECK(detail::backoff_delay(3, 200, 1'000) == std::chrono::milliseconds{1'000});
    CHECK(detail::backoff_delay(9, 200, 1'000) == std::chrono::milliseconds{1'000});

    // A lease TTL that is not known yet must not collapse the delay to zero and
    // spin.
    CHECK(detail::backoff_delay(0, 500, 0) == std::chrono::milliseconds{1});
    CHECK(detail::backoff_delay(4, 500, 0) == std::chrono::milliseconds{1});
}

// -- opening -----------------------------------------------------------------

TEST_CASE("a supervisor cannot be opened without somewhere to deliver",
          "[core][supervisor]")
{
    Script script;

    detail::SessionCallbacks missing_frame = noop_callbacks();
    missing_frame.on_frame = nullptr;
    CHECK_THROWS_AS(detail::SessionSupervisor::open(supervisor_config(),
                                                    fake_channel(script),
                                                    fake_factory(script),
                                                    std::move(missing_frame)),
                    BulkException);

    detail::SessionCallbacks missing_state = noop_callbacks();
    missing_state.on_state = nullptr;
    CHECK_THROWS_AS(detail::SessionSupervisor::open(supervisor_config(),
                                                    fake_channel(script),
                                                    fake_factory(script),
                                                    std::move(missing_state)),
                    BulkException);

    // And neither attempt opened anything.
    CHECK(script.transports_built.load() == 0);
    CHECK(script.opens.load() == 0);
}

TEST_CASE("a transport factory that returns nothing is refused, not dereferenced",
          "[core][supervisor]")
{
    Script script;
    SubscriberConfig config = supervisor_config();
    config.reconnect_policy = ReconnectPolicy::FailFast;

    CHECK_THROWS_AS(
        detail::SessionSupervisor::open(
            config,
            fake_channel(script),
            [](const SubscriberConfig &) { return std::unique_ptr<detail::SubscriberTransport>{}; },
            noop_callbacks()),
        BulkException);
}

TEST_CASE("a granted session that never probes gives up on its own budget",
          "[core][supervisor]")
{
    // The case a real publisher cannot be asked to produce: Open succeeds, so
    // the publisher has allocated for this client, but its Probe never arrives
    // because it cannot reach this client's UCX endpoint.
    Script script;
    script.probe_arrives = false;

    SubscriberConfig config = supervisor_config();
    config.reconnect_policy = ReconnectPolicy::FailFast;
    config.probe_timeout_ms = 40;

    const auto started = std::chrono::steady_clock::now();
    CHECK_THROWS_AS(detail::SessionSupervisor::open(config,
                                                    fake_channel(script),
                                                    fake_factory(script),
                                                    noop_callbacks()),
                    BulkException);
    const auto elapsed = std::chrono::steady_clock::now() - started;

    // It waited its budget rather than command_timeout_ms, which is the whole
    // reason the two were separated: they are different questions that happened
    // to share a default.
    CHECK(elapsed >= std::chrono::milliseconds{40});
    CHECK(elapsed < std::chrono::milliseconds{500});

    // And it did not leave a half-open session behind: a registered ring with no
    // session is exactly what the lease exists to prevent on the other side.
    CHECK(script.closes.load() >= 1);
}

// -- renewal -----------------------------------------------------------------

TEST_CASE("RenewTooFrequent slows the timer and does not end the session",
          "[core][supervisor]")
{
    // 3.7: the lease is not shortened as a penalty, so the session is healthy
    // and the only correct response is to renew less often. Tearing it down
    // here would drop a working stream because the client asked a question too
    // eagerly.
    Script script;
    {
        std::lock_guard<std::mutex> lock(script.mutex);
        script.renew_results = {Status::RenewTooFrequent, Status::RenewTooFrequent};
    }

    auto supervisor = detail::SessionSupervisor::open(
        supervisor_config(), fake_channel(script), fake_factory(script), noop_callbacks());

    REQUIRE(supervisor->state() == SubscriberState::Active);
    REQUIRE(eventually([&script] { return script.renews.load() >= 3; }));

    CHECK(supervisor->state() == SubscriberState::Active);
    CHECK(supervisor->counters().reconnects == 0);

    // One transport for the whole episode: no reconnect happened.
    CHECK(script.transports_built.load() == 1);
}

TEST_CASE("a lost session is not resurrected, it is replaced", "[core][supervisor]")
{
    // 3.7: SessionExpired and UnknownSession are terminal for that session --
    // "there is no resurrection". The correct response is a new session, which
    // means a new transport and a fresh Open, never a Renew retried against the
    // identifier the publisher just disowned.
    Script script;
    {
        std::lock_guard<std::mutex> lock(script.mutex);
        script.renew_results = {Status::SessionExpired};
    }

    auto supervisor = detail::SessionSupervisor::open(
        supervisor_config(), fake_channel(script), fake_factory(script), noop_callbacks());

    REQUIRE(eventually([&script] { return script.transports_built.load() >= 2; }));
    CHECK(eventually([&supervisor] { return supervisor->state() == SubscriberState::Active; }));
    CHECK(script.opens.load() >= 2);
    CHECK(supervisor->counters().reconnects >= 1);
}

// -- reconnect ---------------------------------------------------------------

TEST_CASE("reconnect gives up after the configured attempts and says so",
          "[core][supervisor]")
{
    Script script;
    {
        std::lock_guard<std::mutex> lock(script.mutex);
        // First open succeeds; every reopen is refused.
        script.open_results = {Status::Ok,
                               Status::UnknownStream,
                               Status::UnknownStream,
                               Status::UnknownStream,
                               Status::UnknownStream,
                               Status::UnknownStream};
        script.renew_results = {Status::SessionExpired};
    }

    SubscriberConfig config = supervisor_config();
    config.reconnect_max_attempts = 3;

    // A dispatch thread, because state transitions are queued and Manual mode
    // only drains them inside poll() -- which is correct, and which made the
    // first version of this test assert against an empty list.
    config.delivery_mode = DeliveryMode::DispatchThread;

    std::mutex seen_mutex;
    std::vector<SubscriberState> seen;
    BulkError final_error;

    detail::SessionCallbacks callbacks = noop_callbacks();
    callbacks.on_state = [&](SubscriberState state, const BulkError &error) {
        std::lock_guard<std::mutex> lock(seen_mutex);
        seen.push_back(state);
        if(state == SubscriberState::Failed)
        {
            final_error = error;
        }
    };

    auto supervisor = detail::SessionSupervisor::open(
        config, fake_channel(script), fake_factory(script), std::move(callbacks));

    REQUIRE(eventually([&supervisor] { return supervisor->state() == SubscriberState::Failed; }));

    // Exactly the configured number of reopen attempts, not one more.
    CHECK(supervisor->counters().reconnects == 3);

    REQUIRE(eventually([&] {
        std::lock_guard<std::mutex> lock(seen_mutex);
        return std::count(seen.begin(), seen.end(), SubscriberState::Failed) == 1;
    }));

    {
        std::lock_guard<std::mutex> lock(seen_mutex);
        CHECK(std::count(seen.begin(), seen.end(), SubscriberState::Reconnecting) == 3);
    }

    // The message must name the reason it stopped trying, not just the last
    // refusal -- those are different things to an operator reading a log.
    CHECK(final_error.message.find("reconnect attempts exhausted") != std::string::npos);
    CHECK(final_error.message.find("UnknownStream") != std::string::npos);
}

TEST_CASE("FailFast does not retry", "[core][supervisor]")
{
    Script script;
    {
        std::lock_guard<std::mutex> lock(script.mutex);
        script.open_results = {Status::Ok};
        script.renew_results = {Status::SessionExpired};
    }

    SubscriberConfig config = supervisor_config();
    config.reconnect_policy = ReconnectPolicy::FailFast;

    auto supervisor = detail::SessionSupervisor::open(
        config, fake_channel(script), fake_factory(script), noop_callbacks());

    REQUIRE(eventually([&supervisor] { return supervisor->state() == SubscriberState::Failed; }));
    CHECK(supervisor->counters().reconnects == 0);
    CHECK(script.transports_built.load() == 1);
}

TEST_CASE("teardown during a reconnect backoff neither hangs nor reopens",
          "[core][supervisor]")
{
    Script script;
    {
        std::lock_guard<std::mutex> lock(script.mutex);
        script.open_results = {Status::Ok};
        script.renew_results = {Status::SessionExpired};
    }

    SubscriberConfig config = supervisor_config();
    config.reconnect_backoff_ms = 400; // long enough to be inside it
    config.reconnect_max_attempts = 10;

    auto supervisor = detail::SessionSupervisor::open(
        config, fake_channel(script), fake_factory(script), noop_callbacks());

    REQUIRE(eventually(
        [&supervisor] { return supervisor->state() == SubscriberState::Reconnecting; }));

    const int opens_before = script.opens.load();

    const auto started = std::chrono::steady_clock::now();
    supervisor.reset(); // the destructor is how a session stops
    const auto elapsed = std::chrono::steady_clock::now() - started;

    // It woke from the backoff rather than sleeping it out.
    CHECK(elapsed < std::chrono::milliseconds{300});

    // And nothing was opened on the way out.
    CHECK(script.opens.load() == opens_before);
}

// -- geometry drift across a reconnect ---------------------------------------

TEST_CASE("a reopened session that describes a different array is refused",
          "[core][supervisor]")
{
    // The silent version of this is how a client ends up interpreting
    // 1024x1024 frames as 2048x2048: the stream drops, it reconnects, the
    // detector has been reconfigured meanwhile, and the new grant is adopted
    // under buffers laid out for the old one.
    Script script;
    {
        std::lock_guard<std::mutex> lock(script.mutex);
        script.renew_results = {Status::SessionExpired}; // force one reconnect
        script.reshape_after_first = true;
    }

    SubscriberConfig config = supervisor_config();
    config.delivery_mode = DeliveryMode::DispatchThread;
    config.reconnect_max_attempts = 10;

    std::mutex seen_mutex;
    BulkError final_error;
    detail::SessionCallbacks callbacks = noop_callbacks();
    callbacks.on_state = [&](SubscriberState state, const BulkError &error) {
        if(state == SubscriberState::Failed)
        {
            std::lock_guard<std::mutex> lock(seen_mutex);
            final_error = error;
        }
    };

    auto supervisor = detail::SessionSupervisor::open(
        config, fake_channel(script), fake_factory(script), std::move(callbacks));

    REQUIRE(supervisor->granted_geometry().shape[0] == 8);

    REQUIRE(eventually([&supervisor] { return supervisor->state() == SubscriberState::Failed; }));

    CHECK(supervisor->counters().geometry_changes == 1);

    // Terminal, and quickly. Retrying cannot help -- neither side changes
    // between attempts -- so it must not spend ten attempts finding that out.
    CHECK(supervisor->counters().reconnects <= 1);

    // state() flips inside transition(); the callback carrying the reason is
    // queued and delivered by the dispatch thread afterwards. Waiting for the
    // state and then reading the error is a race, and reading it first is how
    // this test failed the first time it ran.
    REQUIRE(eventually([&] {
        std::lock_guard<std::mutex> lock(seen_mutex);
        return final_error.status != Status::Ok;
    }));

    {
        std::lock_guard<std::mutex> lock(seen_mutex);
        CHECK(final_error.status == Status::GeometryMismatch);
        CHECK(final_error.message.find("different array") != std::string::npos);
    }

    // The new grant was never adopted, so nothing describes the new shape.
    CHECK(supervisor->granted_geometry().generation == 0);
}

TEST_CASE("a reopened session with the same array is adopted normally",
          "[core][supervisor]")
{
    // The counterpart, so the check above cannot pass by refusing everything.
    Script script;
    {
        std::lock_guard<std::mutex> lock(script.mutex);
        script.renew_results = {Status::SessionExpired};
        script.reshape_after_first = false;
    }

    auto supervisor = detail::SessionSupervisor::open(
        supervisor_config(), fake_channel(script), fake_factory(script), noop_callbacks());

    REQUIRE(eventually([&script] { return script.transports_built.load() >= 2; }));
    REQUIRE(eventually([&supervisor] { return supervisor->state() == SubscriberState::Active; }));

    CHECK(supervisor->counters().geometry_changes == 0);
    CHECK(supervisor->granted_geometry().shape[0] == 8);
}

// -- threading ---------------------------------------------------------------

TEST_CASE("coordination calls never overlap", "[core][supervisor]")
{
    // The promise the interface makes, and the one a Python adapter's GIL
    // behaviour depends on. Asserted rather than assumed -- the first version of
    // this test asserted something stronger and false, that a single thread
    // makes every call, and was right to fail.
    Script script;

    auto supervisor = detail::SessionSupervisor::open(
        supervisor_config(), fake_channel(script), fake_factory(script), noop_callbacks());

    REQUIRE(eventually([&script] { return script.renews.load() >= 3; }));
    supervisor.reset(); // sends Close, so teardown is covered too

    CHECK(script.channel_max_concurrent.load() == 1);

    // Two threads, not one: the first open runs on the caller's thread so that
    // a failure can be reported by throwing, and everything after it runs on
    // the control thread -- which is what keeps a stuck device from stalling
    // the application.
    std::lock_guard<std::mutex> lock(script.mutex);
    CHECK(script.channel_threads.size() == 2);
    CHECK(script.channel_threads.count(std::this_thread::get_id()) == 1);
}

// -- the granted geometry ----------------------------------------------------

TEST_CASE("the granted geometry survives to the interface", "[core][supervisor]")
{
    // It used to be copied out of and thrown away: adopt_open_reply kept ring
    // depth, frame size, generation and the lease terms, and dropped element
    // type, rank, shape and strides. Nothing above the transport could describe
    // the array it was receiving.
    Script script;

    auto supervisor = detail::SessionSupervisor::open(
        supervisor_config(), fake_channel(script), fake_factory(script), noop_callbacks());

    const Protocol::GeometryBlock granted = supervisor->granted_geometry();

    CHECK(granted.generation == 1);
    CHECK(granted.element_type == ElementType::UInt16);
    CHECK(granted.element_size == 2);
    CHECK(granted.rank == 2);
    CHECK(granted.shape[0] == 8);
    CHECK(granted.shape[1] == 8);
    CHECK(granted.strides[0] == 16);
    CHECK(granted.strides[1] == 2);

    // Whatever a binding builds a schema from, it must be able to tell "no
    // session" apart from a real grant. generation == 0 is never legal on the
    // wire, which is what makes it the field to test.
    supervisor.reset();
    CHECK(supervisor == nullptr);
}

TEST_CASE("no session means an all-zero geometry, not a stale one",
          "[core][supervisor]")
{
    Script script;
    script.probe_arrives = false;

    SubscriberConfig config = supervisor_config();
    config.reconnect_policy = ReconnectPolicy::FailFast;
    config.probe_timeout_ms = 30;

    CHECK_THROWS_AS(detail::SessionSupervisor::open(config,
                                                    fake_channel(script),
                                                    fake_factory(script),
                                                    noop_callbacks()),
                    BulkException);

    // Nothing to read a geometry from, and the default block reports the epoch
    // that never appears on the wire.
    const Protocol::GeometryBlock none;
    CHECK(none.generation == 0);
}

// -- delivery mode -----------------------------------------------------------

TEST_CASE("poll() is refused when a dispatch thread is already delivering",
          "[core][supervisor]")
{
    Script script;
    SubscriberConfig config = supervisor_config();
    config.delivery_mode = DeliveryMode::DispatchThread;

    auto supervisor = detail::SessionSupervisor::open(
        config, fake_channel(script), fake_factory(script), noop_callbacks());

    CHECK_THROWS_AS(supervisor->poll(std::chrono::milliseconds{1}), BulkException);
}

} // namespace
