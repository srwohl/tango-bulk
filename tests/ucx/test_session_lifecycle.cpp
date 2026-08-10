// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include "slice.h"

#include <tango-bulk/protocol.h>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <thread>
#include <vector>

/// M3: session lifecycle and fault containment.
///
/// The milestone's exit condition, from MVP_PLAN.md, is one sentence: "killing a
/// client cannot exhaust producer slots beyond the configured lease deadline;
/// recovery does not require restarting the device server."  Everything here is
/// either that sentence or one of the interlocks it depends on.
///
/// Cases that wait on a lease use the shortest TTL 6.1 permits, 1000 ms, so the
/// timing is real rather than mocked.  That is the price of testing a timer.
namespace
{

using namespace TangoBulkTests;
using TangoBulk::Protocol::SessionState;

PublisherConfig short_lease_config()
{
    PublisherConfig config = publisher_config();
    config.lease_ttl_ms = 1'000;   ///< k_min_lease_ttl_ms
    config.renew_interval_ms = 300; ///< within [TTL/10, TTL/2]
    return config;
}

Protocol::OpenReply decode_open_reply(const std::vector<std::byte> &bytes)
{
    Protocol::OpenReply reply;
    REQUIRE(Protocol::decode(bytes.data(), bytes.size(), reply) == Status::Ok);
    return reply;
}

Protocol::RenewReply renew(BulkPublisher &publisher,
                           const Protocol::SessionId &id,
                           std::uint64_t correlation_id = 7)
{
    Protocol::RenewRequest request;
    request.session_id = id;
    const std::vector<std::byte> encoded = Protocol::encode(request, correlation_id);
    const std::vector<std::byte> raw =
        publisher.handle_coordination(encoded.data(), encoded.size());

    Protocol::RenewReply reply;
    REQUIRE(Protocol::decode(raw.data(), raw.size(), reply) == Status::Ok);
    return reply;
}

Protocol::CloseReply close(BulkPublisher &publisher,
                           const Protocol::SessionId &id,
                           std::uint64_t correlation_id = 9)
{
    Protocol::CloseRequest request;
    request.session_id = id;
    const std::vector<std::byte> encoded = Protocol::encode(request, correlation_id);
    const std::vector<std::byte> raw =
        publisher.handle_coordination(encoded.data(), encoded.size());

    Protocol::CloseReply reply;
    REQUIRE(Protocol::decode(raw.data(), raw.size(), reply) == Status::Ok);
    return reply;
}

Protocol::QueryReply query(BulkPublisher &publisher,
                           const Protocol::SessionId &id = {},
                           std::uint64_t correlation_id = 11)
{
    Protocol::QueryRequest request;
    request.session_id = id;
    const std::vector<std::byte> encoded = Protocol::encode(request, correlation_id);
    const std::vector<std::byte> raw =
        publisher.handle_coordination(encoded.data(), encoded.size());

    Protocol::QueryReply reply;
    REQUIRE(Protocol::decode(raw.data(), raw.size(), reply) == Status::Ok);
    return reply;
}

/// The value of `key` in a `key=value;` blob, or empty if it is absent.
///
/// Split into fields rather than searched for `key=`, because a substring search
/// is wrong here in a way that flatters the test: `dropped_no_session=0` ends
/// with `session=0`, so asking whether the blob mentions `session` would find a
/// counter that has nothing to do with any session.
std::string counter(const std::string &blob, const std::string &key)
{
    for(std::size_t start = 0; start < blob.size();)
    {
        const std::size_t end = std::min(blob.find(';', start), blob.size());
        const std::size_t equals = blob.find('=', start);

        if(equals < end && blob.compare(start, equals - start, key) == 0)
        {
            return blob.substr(equals + 1, end - equals - 1);
        }

        start = end + 1;
    }

    return {};
}

} // namespace

TEST_CASE("Open grants unpredictable identifiers and the negotiated lease terms", "[m3][session]")
{
    PublisherConfig config = publisher_config();
    config.lease_ttl_ms = 4'000;
    config.renew_interval_ms = 1'000;

    BulkPublisher publisher(config);
    detail::SubscriberEngine first(subscriber_config());
    detail::SubscriberEngine second(subscriber_config());

    const Protocol::OpenReply a = decode_open_reply(exchange_open(publisher, first, 1));
    const Protocol::OpenReply b = decode_open_reply(exchange_open(publisher, second, 2));

    REQUIRE(a.status == Status::Ok);
    REQUIRE(b.status == Status::Ok);

    // 3.2: clients never choose these, and 4.4 makes a second Open a *new*
    // session rather than a reuse of the first.
    CHECK(a.session_id != b.session_id);
    CHECK(a.stream_id != b.stream_id);
    CHECK_FALSE(a.session_id.is_zero());
    CHECK(a.stream_id != 0);

    // Both sessions belong to the same server incarnation; 4.4 makes a changed
    // value the client's signal to discard cached geometry and sequence state.
    CHECK(a.server_epoch_id == b.server_epoch_id);
    CHECK(a.server_epoch_id != 0);

    CHECK(a.lease_ttl_ms == 4'000);
    CHECK(a.renew_interval_ms == 1'000);
    CHECK(publisher.counters().sessions_opened == 2);

    CHECK(close(publisher, a.session_id).status == Status::Ok);
    CHECK(close(publisher, b.session_id).status == Status::Ok);
}

TEST_CASE("A granted session carries no frame until ProbeAck arms it", "[m3][session]")
{
    BulkPublisher publisher(publisher_config());
    detail::SubscriberEngine subscriber(subscriber_config());

    const std::vector<std::byte> reply = exchange_open(publisher, subscriber);

    // 4.2: "The publisher MUST NOT submit a frame to a session in `Open`."  The
    // subscriber has not adopted the reply yet, so it has no endpoint and no
    // engine thread -- it cannot have answered a probe, which makes this the one
    // point in the handshake where the interlock can be observed directly rather
    // than raced against.
    REQUIRE(publisher.session_count() == 0);
    CHECK(publish_one(publisher, 4096, 1) == PublishResult::NoSession);
    CHECK(publisher.counters().dropped_no_session == 1);
    CHECK(publisher.counters().frames_submitted == 0);

    REQUIRE(subscriber.adopt_open_reply(reply.data(), reply.size()) == Status::Ok);
    REQUIRE(await_armed(publisher, subscriber));

    CHECK(publish_one(publisher, 4096, 2) == PublishResult::Accepted);
    REQUIRE(collect(subscriber, 1).size() == 1);

    const std::vector<std::byte> request = subscriber.make_close_request(2);
    publisher.handle_coordination(request.data(), request.size());
}

TEST_CASE("Renewal keeps a session alive past its lease", "[m3][session]")
{
    Slice slice(short_lease_config(), subscriber_config());

    const auto until = std::chrono::steady_clock::now() + 1'500ms;
    std::uint64_t correlation = 100;

    while(std::chrono::steady_clock::now() < until)
    {
        const std::vector<std::byte> request = slice.subscriber.make_renew_request(++correlation);
        const std::vector<std::byte> reply =
            slice.publisher.handle_coordination(request.data(), request.size());
        REQUIRE(slice.subscriber.adopt_renew_reply(reply.data(), reply.size()) == Status::Ok);
        std::this_thread::sleep_for(200ms);
    }

    // One and a half TTLs have passed and the session is still here.
    CHECK(slice.publisher.session_count() == 1);
    CHECK(slice.publisher.counters().sessions_expired == 0);
    CHECK(slice.publisher.counters().renewals_accepted >= 6);
    CHECK(slice.subscriber.state() == SubscriberState::Active);
}

TEST_CASE("An unrenewed session expires on schedule with no frames in flight", "[m3][session]")
{
    BulkPublisher publisher(short_lease_config());
    detail::SubscriberEngine subscriber(subscriber_config());

    const std::vector<std::byte> raw = exchange_open(publisher, subscriber);
    const Protocol::OpenReply granted = decode_open_reply(raw);
    REQUIRE(subscriber.adopt_open_reply(raw.data(), raw.size()) == Status::Ok);
    REQUIRE(await_armed(publisher, subscriber));

    // Nothing is published and nothing is closed.  4.2: expiry is "driven by a
    // timer ... independent of frame traffic.  A publisher producing zero frames
    // still expires a dead client on schedule."  Only the timer can move this.
    REQUIRE(eventually([&] { return publisher.counters().sessions_expired == 1; }));

    CHECK(publisher.session_count() == 0);
    CHECK(publisher.counters().frames_submitted == 0);
    CHECK(publisher.counters().sessions_closed == 0);

    // 3.7: no resurrection, and the reason is reported.  A lease that ran out is
    // SessionExpired; a session the client closed would be UnknownSession.
    const Protocol::RenewReply late = renew(publisher, granted.session_id);
    CHECK(late.status == Status::SessionExpired);
    CHECK(late.server_state == SessionState::Closed);

    // The client learns from the reply that this session is over for good.
    const std::vector<std::byte> encoded = Protocol::encode(late, 7);
    CHECK(subscriber.adopt_renew_reply(encoded.data(), encoded.size()) ==
          Status::SessionExpired);
    CHECK(subscriber.state() == SubscriberState::Failed);
    CHECK(subscriber.counters().renewals_failed == 1);
}

TEST_CASE("A client that vanishes releases its slots on the lease, and the stream recovers",
          "[m3][session]")
{
    BulkPublisher publisher(short_lease_config());

    std::vector<FrameView> stranded;

    {
        detail::SubscriberEngine subscriber(subscriber_config());
        open_session(publisher, subscriber);

        for(std::uint32_t n = 0; n < k_credit_window; ++n)
        {
            REQUIRE(publish_one(publisher, 4096, n) == PublishResult::Accepted);
        }

        stranded = collect(subscriber, k_credit_window);
        REQUIRE(stranded.size() == k_credit_window);

        // Every view is retained, so 5.5 withholds every credit, so 5.4 retains
        // every producer slot.
        REQUIRE(eventually([&] { return publisher.source().retained() == k_credit_window; }));

        // The subscriber is destroyed here with no `Close`: the client crashed.
        // Its views outlive it and its credits will never come back.
    }

    // Nothing on this side can be asked to release those slots -- there is no
    // peer left to ask.  This is the whole of M3's exit condition: the lease, and
    // only the lease, bounds how long a dead client can hold pinned memory.
    REQUIRE(eventually([&] { return publisher.source().retained() == 0; }));
    CHECK(publisher.counters().sessions_expired == 1);
    CHECK(publisher.counters().sessions_closed == 0);
    CHECK(publisher.session_count() == 0);

    // ...and the device server is still serving.  A restarted client opens a new
    // session on the same publisher and the stream resumes.
    detail::SubscriberEngine restarted(subscriber_config());
    open_session(publisher, restarted);

    REQUIRE(publish_one(publisher, 4096, 42) == PublishResult::Accepted);
    const std::vector<FrameView> delivered = collect(restarted, 1);
    REQUIRE(delivered.size() == 1);
    CHECK(payload_matches(delivered.front(), 42));

    // 4.4: sequences restart at 0 for a new session; they are not continued.
    CHECK(delivered.front().sequence() == 0);

    const std::vector<std::byte> request = restarted.make_close_request(3);
    publisher.handle_coordination(request.data(), request.size());

    // The frames the dead client never released are still readable here.
    for(std::size_t n = 0; n < stranded.size(); ++n)
    {
        CHECK(payload_matches(stranded[n], static_cast<unsigned>(n)));
    }
}

TEST_CASE("Two sessions coexist and a slot returns only when both have credited it",
          "[m3][session]")
{
    BulkPublisher publisher(publisher_config());
    detail::SubscriberEngine first(subscriber_config());
    detail::SubscriberEngine second(subscriber_config());

    open_session(publisher, first, 1);
    open_session(publisher, second, 2);
    REQUIRE(publisher.session_count() == 2);

    REQUIRE(publish_one(publisher, 4096, 0x77) == PublishResult::Accepted);

    std::vector<FrameView> a = collect(first, 1);
    std::vector<FrameView> b = collect(second, 1);
    REQUIRE(a.size() == 1);
    REQUIRE(b.size() == 1);

    CHECK(payload_matches(a.front(), 0x77));
    CHECK(payload_matches(b.front(), 0x77));

    // Each session has its own sequence space (3.11), so both see sequence 0 for
    // a frame that was published once.
    CHECK(a.front().sequence() == 0);
    CHECK(b.front().sequence() == 0);

    // 5.4: retained "until *every* session it was successfully submitted to has
    // credited its sequence".  One release is not enough.
    CHECK(publisher.source().retained() == 1);
    a.clear();
    std::this_thread::sleep_for(150ms);
    CHECK(publisher.source().retained() == 1);
    CHECK(publisher.counters().frames_credited == 0);

    b.clear();
    REQUIRE(eventually([&] { return publisher.source().retained() == 0; }));
    CHECK(publisher.counters().frames_credited == 1);

    const std::vector<std::byte> close_first = first.make_close_request(11);
    publisher.handle_coordination(close_first.data(), close_first.size());
    const std::vector<std::byte> close_second = second.make_close_request(12);
    publisher.handle_coordination(close_second.data(), close_second.size());
}

TEST_CASE("Close is idempotent and Renew tells a closed session from an unknown one",
          "[m3][session]")
{
    BulkPublisher publisher(publisher_config());
    detail::SubscriberEngine subscriber(subscriber_config());

    const std::vector<std::byte> raw = exchange_open(publisher, subscriber);
    const Protocol::OpenReply granted = decode_open_reply(raw);
    REQUIRE(subscriber.adopt_open_reply(raw.data(), raw.size()) == Status::Ok);
    REQUIRE(await_armed(publisher, subscriber));

    // A live session renews, and the reply carries the server's own view of it.
    const Protocol::RenewReply ok = renew(publisher, granted.session_id);
    CHECK(ok.status == Status::Ok);
    CHECK((ok.server_state == SessionState::Armed || ok.server_state == SessionState::Active));
    CHECK(ok.geometry.ring_depth == k_ring_depth);
    CHECK(publisher.counters().renewals_accepted == 1);

    // 3.7: an identifier nobody ever issued is UnknownSession, not Error -- the
    // client has to be able to tell "you are gone" from "your message was
    // garbage".
    Protocol::SessionId stranger;
    stranger.bytes[0] = std::byte{0xAB};
    CHECK(renew(publisher, stranger).status == Status::UnknownSession);

    CHECK(close(publisher, granted.session_id).status == Status::Ok);

    // 3.8: "A repeat Close for a session already closed returns
    // CloseReply{UnknownSession} and performs no work.  It is never an Error."
    CHECK(close(publisher, granted.session_id, 10).status == Status::UnknownSession);

    REQUIRE(eventually([&] { return publisher.counters().sessions_closed == 1; }));
    CHECK(publisher.counters().sessions_expired == 0);
    CHECK(publisher.session_count() == 0);

    // The seat keeps the identifier, so the answer names what happened to it.
    CHECK(renew(publisher, granted.session_id).status == Status::UnknownSession);
}

TEST_CASE("Renewing faster than the rate limit is refused without shortening the lease",
          "[m3][session]")
{
    PublisherConfig config = short_lease_config();
    config.max_renewals_per_ttl = 3;

    BulkPublisher publisher(config);
    detail::SubscriberEngine subscriber(subscriber_config());

    const std::vector<std::byte> raw = exchange_open(publisher, subscriber);
    const Protocol::OpenReply granted = decode_open_reply(raw);
    REQUIRE(subscriber.adopt_open_reply(raw.data(), raw.size()) == Status::Ok);
    REQUIRE(await_armed(publisher, subscriber));

    for(std::uint32_t n = 0; n < 3; ++n)
    {
        CHECK(renew(publisher, granted.session_id).status == Status::Ok);
    }

    // 3.7: over the limit the reply is refused and counted.
    CHECK(renew(publisher, granted.session_id).status == Status::RenewTooFrequent);
    CHECK(publisher.counters().renewals_rejected == 1);
    CHECK(publisher.counters().renewals_accepted == 3);

    // "The lease is **not** shortened as a penalty": the session that was just
    // refused is still armed and still carries frames.
    CHECK(publisher.session_count() == 1);
    CHECK(publish_one(publisher, 4096, 5) == PublishResult::Accepted);
    CHECK(collect(subscriber, 1).size() == 1);

    close(publisher, granted.session_id);
}

TEST_CASE("A publisher admits no more sessions than it was configured for", "[m3][session]")
{
    PublisherConfig config = publisher_config();
    config.max_sessions = 2;

    BulkPublisher publisher(config);
    detail::SubscriberEngine first(subscriber_config());
    detail::SubscriberEngine second(subscriber_config());
    detail::SubscriberEngine third(subscriber_config());

    const Protocol::OpenReply a = decode_open_reply(exchange_open(publisher, first, 1));
    const Protocol::OpenReply b = decode_open_reply(exchange_open(publisher, second, 2));
    REQUIRE(a.status == Status::Ok);
    REQUIRE(b.status == Status::Ok);

    // 6.1 bounds sessions per publisher, and 4.4 says a client that leaks them
    // "hits TooManySessions and that is correct feedback".
    const std::vector<std::byte> refused = exchange_open(publisher, third, 3);
    Protocol::ErrorMessage error;
    REQUIRE(Protocol::decode(refused.data(), refused.size(), error) == Status::Ok);
    CHECK(error.status == Status::TooManySessions);
    CHECK(publisher.counters().sessions_rejected == 1);

    // A closed seat is reusable, so the third client gets in once one leaves.
    CHECK(close(publisher, a.session_id).status == Status::Ok);
    REQUIRE(eventually([&] { return publisher.counters().sessions_closed == 1; }));

    const Protocol::OpenReply c = decode_open_reply(exchange_open(publisher, third, 4));
    CHECK(c.status == Status::Ok);
    CHECK(c.session_id != a.session_id);

    close(publisher, b.session_id);
    close(publisher, c.session_id);
}

TEST_CASE("engine_cpu_affinity pins the engine, and the placement report proves it",
          "[m3][placement]")
{
    // The regression guard for docs/EXTRACTION.md deviation 25. Before it,
    // `engine_cpu_affinity` was a public field in 2.4 that nothing read: setting
    // it returned Ok, constructed cleanly, ran cleanly, and did nothing. What
    // makes that a bug rather than an omission is that no measurement could tell
    // -- so this asserts the observable consequence, not the call.
    const std::size_t cpus = detail::allowed_cpu_count();
    if(cpus < 2)
    {
        SUCCEED("needs at least two permitted CPUs to tell pinned from unpinned");
        return;
    }

    SubscriberConfig sub = subscriber_config();
    sub.engine_cpu_affinity = 1;

    BulkPublisher publisher(publisher_config());
    detail::SubscriberEngine subscriber(sub);
    open_session(publisher, subscriber);

    const detail::Locality &where = subscriber.locality();

    CHECK(where.engine_cpu == 1);

    // The rest of the report has to be populated too, or a future change could
    // satisfy the line above while the observation quietly stopped working.
    CHECK_FALSE(where.transport.empty());
    CHECK_FALSE(where.device.empty());
    CHECK(where.host_nodes >= 1);

    // Deliberately not asserted: which NUMA node anything is on, or that the
    // placement is Local. Both depend on the host, and a test that demanded a
    // particular topology would fail on the dual-socket machine this work is for.
    CHECK(detail::to_string(where.placement()) != nullptr);

    const std::vector<std::byte> request = subscriber.make_close_request(2);
    publisher.handle_coordination(request.data(), request.size());
}

TEST_CASE("Query answers server-wide and per session", "[m4][session]")
{
    // The Tango-facing `bulk_query()` only ever asks the server-wide question,
    // because no public API hands out a `session_id`.  3.8's other half -- a
    // Query naming one session -- is reachable only from a caller that decoded
    // the `OpenReply` itself, which is exactly what this file does.
    BulkPublisher publisher(short_lease_config());
    detail::SubscriberEngine subscriber(subscriber_config());

    const std::vector<std::byte> raw = exchange_open(publisher, subscriber);
    const Protocol::OpenReply granted = decode_open_reply(raw);

    REQUIRE(subscriber.adopt_open_reply(raw.data(), raw.size()) == Status::Ok);
    REQUIRE(await_armed(publisher, subscriber));

    SECTION("server-wide")
    {
        const Protocol::QueryReply reply = query(publisher);

        CHECK(reply.status == Status::Ok);
        CHECK(reply.active_sessions == 1);
        CHECK(reply.generation == publisher.generation());
        CHECK(reply.geometry.validate() == Status::Ok);
        CHECK(reply.geometry.max_frame_bytes == k_frame_bytes);
        CHECK(counter(reply.counters, "stream") == "bulk.slice");
        CHECK(counter(reply.counters, "sessions_opened") == "1");

        // A server-wide Query says nothing about any particular session, which
        // is what makes it safe to expose at OPERATOR level.
        CHECK(counter(reply.counters, "session").empty());
    }

    SECTION("one session")
    {
        const Protocol::QueryReply reply = query(publisher, granted.session_id);

        CHECK(reply.status == Status::Ok);
        CHECK(reply.session_id == granted.session_id);
        CHECK(counter(reply.counters, "session_state") == "Armed");

        // 3.2: identifiers appear only in the truncated form, here and in logs.
        const std::string quoted = counter(reply.counters, "session");
        CHECK_FALSE(quoted.empty());
        CHECK(quoted != Protocol::to_hex(granted.session_id));

        const std::string remaining = counter(reply.counters, "session_lease_ms_remaining");
        CHECK_FALSE(remaining.empty());
        CHECK(std::stoull(remaining) <= 1'000);
    }

    SECTION("a session this publisher does not have")
    {
        // The same answer 3.7 gives a Renew, so an operator's Query and a
        // client's Renew cannot disagree about whether a session still exists.
        Protocol::SessionId stranger{};
        stranger.bytes[0] = std::byte{0x5A};

        CHECK(query(publisher, stranger).status == Status::UnknownSession);
    }

    close(publisher, granted.session_id);
}
