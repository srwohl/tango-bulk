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
        detail::PublisherAccess::coordination(publisher, encoded.data(), encoded.size());

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
        detail::PublisherAccess::coordination(publisher, encoded.data(), encoded.size());

    Protocol::CloseReply reply;
    REQUIRE(Protocol::decode(raw.data(), raw.size(), reply) == Status::Ok);
    return reply;
}

Protocol::ErrorMessage decode_error_reply(const std::vector<std::byte> &bytes,
                                          Protocol::Envelope &envelope)
{
    Protocol::ErrorMessage error;
    REQUIRE(Protocol::decode(bytes.data(), bytes.size(), error, &envelope) == Status::Ok);
    return error;
}

} // namespace

TEST_CASE("publisher snapshot is an owner value and yields a discovery offer",
          "[observation][stream-offer]")
{
    BulkPublisher publisher(publisher_config());

    const PublisherSnapshot snapshot = publisher.snapshot();
    REQUIRE(snapshot.stream_name == "bulk.slice");
    REQUIRE(snapshot.accepting);
    REQUIRE(snapshot.sampled_at_steady_ns != 0);
    CHECK(snapshot.active_sessions == 0);
    CHECK(snapshot.geometry.max_frame_bytes == k_frame_bytes);
    CHECK(snapshot.geometry.ring_depth == k_ring_depth);
    CHECK(snapshot.counters.sessions_opened == 0);
    CHECK(snapshot.sessions.empty());
    CHECK(snapshot.transport == "ActiveMessage");
    CHECK(snapshot.worst_lag_frames == 0);
    CHECK(snapshot.frames_dropped() == 0);

    const StreamOffer offer = snapshot.stream_offer();
    CHECK(offer.status == Status::Ok);
    CHECK(offer.stream_name == "bulk.slice");
    CHECK(offer.geometry == snapshot.geometry);
    CHECK(offer.validate() == Status::Ok);
}

TEST_CASE("encoded coordination preserves correlation on typed publisher failures",
          "[coordination][adapter]")
{
    BulkPublisher publisher(publisher_config());

    Protocol::OpenRequest request;
    request.stream_name = publisher_config().stream_name;
    request.requested_max_frame_bytes = k_frame_bytes;
    request.requested_ring_depth = k_ring_depth;
    request.requested_credit_window = k_credit_window;
    request.client_ucx_address = {std::byte{0x01}};
    request.requested_transport = static_cast<Protocol::Transport>(99);

    constexpr std::uint64_t correlation_id = 0x0102030405060708ull;
    const std::vector<std::byte> encoded = Protocol::encode(request, correlation_id);
    const std::vector<std::byte> raw =
        detail::PublisherAccess::coordination(publisher, encoded.data(), encoded.size());

    Protocol::ErrorMessage error;
    Protocol::Envelope envelope;
    REQUIRE(Protocol::decode(raw.data(), raw.size(), error, &envelope) == Status::Ok);
    CHECK(envelope.correlation_id == correlation_id);
    CHECK(error.status == Status::MalformedMessage);
    CHECK(publisher.session_count() == 0);
}

TEST_CASE("Open grants unpredictable identifiers and the negotiated lease terms", "[m3][session]")
{
    PublisherConfig config = publisher_config();
    config.lease_ttl_ms = 4'000;
    config.renew_interval_ms = 1'000;

    BulkPublisher publisher(config);
    std::vector<Protocol::OpenReply> grants;
    const auto record_open = [&grants](Protocol::CoordType type,
                                       const std::vector<std::byte> &reply)
    {
        if(type == Protocol::CoordType::Open)
        {
            grants.push_back(decode_open_reply(reply));
        }
    };

    std::unique_ptr<Subscription> first = open_subscription(publisher, subscriber_config(), record_open);
    std::unique_ptr<Subscription> second = open_subscription(publisher, subscriber_config(), record_open);
    REQUIRE(grants.size() == 2);
    const Protocol::OpenReply &a = grants[0];
    const Protocol::OpenReply &b = grants[1];

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

    first->close();
    second->close();
}

TEST_CASE("A granted session carries no frame until ProbeAck arms it", "[m3][session]")
{
    BulkPublisher publisher(publisher_config());
    bool observed_unarmed = false;
    const auto observe_open = [&publisher, &observed_unarmed](Protocol::CoordType type,
                                                               const std::vector<std::byte> &)
    {
        if(type != Protocol::CoordType::Open)
        {
            return;
        }

        observed_unarmed = publisher.session_count() == 0;
        CHECK(publish_one(publisher, 4096, 1) == PublishResult::NoSession);
        CHECK(publisher.counters().dropped_no_session == 1);
        CHECK(publisher.counters().frames_submitted == 0);
    };

    std::unique_ptr<Subscription> subscription =
        open_subscription(publisher, subscriber_config(), observe_open);

    CHECK(observed_unarmed);
    CHECK(subscription->state() == SubscriberState::Active);

    CHECK(publish_one(publisher, 4096, 2) == PublishResult::Accepted);
    REQUIRE(collect(*subscription, 1).size() == 1);
}

TEST_CASE("Renewal keeps a session alive past its lease", "[m3][session]")
{
    Slice slice(short_lease_config(), subscriber_config());

    const auto until = std::chrono::steady_clock::now() + 1'500ms;
    while(std::chrono::steady_clock::now() < until)
        std::this_thread::sleep_for(20ms);

    // One and a half TTLs have passed and the session is still here.
    CHECK(slice.publisher.session_count() == 1);
    CHECK(slice.publisher.counters().sessions_expired == 0);
    CHECK(slice.publisher.counters().renewals_accepted >= 4);
    CHECK(slice.subscription->state() == SubscriberState::Active);
}

TEST_CASE("An unrenewed session expires on schedule with no frames in flight", "[m3][session]")
{
    BulkPublisher publisher(short_lease_config());
    SubscriberConfig config = subscriber_config();
    config.reconnect_policy = ReconnectPolicy::FailFast;

    std::vector<Protocol::OpenReply> grants;
    std::unique_ptr<Subscription> subscription = detail::SubscriptionFactory::open_default(
        config,
        [&publisher, &grants](Protocol::CoordType type,
                              const std::vector<std::byte> &request,
                              std::chrono::steady_clock::time_point /*deadline*/)
        {
            if(type == Protocol::CoordType::Renew)
            {
                // Let the publisher's timer expire the session while the
                // subscriber's one control exchange is blocked.
                std::this_thread::sleep_for(1'200ms);
            }
            const std::vector<std::byte> reply = detail::PublisherAccess::coordination(
                publisher, request.data(), request.size(), type);
            if(type == Protocol::CoordType::Open)
                grants.push_back(decode_open_reply(reply));
            return reply;
        },
        SubscriptionCallbacks{});
    REQUIRE(grants.size() == 1);
    const Protocol::OpenReply &granted = grants.front();

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

    // The client learns from the blocked renewal that this session is over for
    // good; no second lifecycle constructor is involved.
    REQUIRE(eventually([&] { return subscription->state() == SubscriberState::Failed; }));
    CHECK(subscription->snapshot().error.status == Status::SessionExpired);
}

TEST_CASE("Closing a Subscription releases its slots and the stream recovers",
          "[m3][session]")
{
    BulkPublisher publisher(short_lease_config());

    std::vector<FrameView> stranded;

    {
        std::unique_ptr<Subscription> subscription = open_subscription(publisher);

        for(std::uint32_t n = 0; n < k_credit_window; ++n)
        {
            REQUIRE(publish_one(publisher, 4096, n) == PublishResult::Accepted);
        }

        stranded = collect(*subscription, k_credit_window);
        REQUIRE(stranded.size() == k_credit_window);

        // Every view is retained, so 5.5 withholds every credit, so 5.4 retains
        // every producer slot.
        REQUIRE(eventually([&] {
            return publisher.counters().leases_retained == k_credit_window;
        }));

        // Subscription destruction performs the bounded, orderly Close after
        // retaining views have been detached from the lifecycle owner.
    }

    // The lifecycle owner has released the publisher session, so retained views
    // no longer keep publisher slots alive.
    REQUIRE(eventually([&] { return publisher.counters().leases_retained == 0; }));
    CHECK(publisher.counters().sessions_expired == 0);
    CHECK(publisher.counters().sessions_closed == 1);
    CHECK(publisher.session_count() == 0);

    // ...and the device server is still serving.  A restarted client opens a new
    // session on the same publisher and the stream resumes.
    std::unique_ptr<Subscription> restarted = open_subscription(publisher);

    REQUIRE(publish_one(publisher, 4096, 42) == PublishResult::Accepted);
    const std::vector<FrameView> delivered = collect(*restarted, 1);
    REQUIRE(delivered.size() == 1);
    CHECK(payload_matches(delivered.front(), 42));

    // 4.4: sequences restart at 0 for a new session; they are not continued.
    CHECK(delivered.front().sequence() == 0);

    restarted->close();

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
    std::unique_ptr<Subscription> first = open_subscription(publisher);
    std::unique_ptr<Subscription> second = open_subscription(publisher);

    REQUIRE(publisher.session_count() == 2);

    REQUIRE(publish_one(publisher, 4096, 0x77) == PublishResult::Accepted);

    std::vector<FrameView> a = collect(*first, 1);
    std::vector<FrameView> b = collect(*second, 1);
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
    CHECK(publisher.counters().leases_retained == 1);
    a.clear();
    std::this_thread::sleep_for(150ms);
    CHECK(publisher.counters().leases_retained == 1);
    CHECK(publisher.counters().frames_credited == 0);

    b.clear();
    REQUIRE(eventually([&] { return publisher.counters().leases_retained == 0; }));
    CHECK(publisher.counters().frames_credited == 1);

    first->close();
    second->close();
}

TEST_CASE("Close is idempotent and Renew tells a closed session from an unknown one",
          "[m3][session]")
{
    BulkPublisher publisher(publisher_config());
    std::vector<Protocol::OpenReply> grants;
    std::unique_ptr<Subscription> subscription = open_subscription(
        publisher,
        subscriber_config(),
        [&grants](Protocol::CoordType type, const std::vector<std::byte> &reply)
        {
            if(type == Protocol::CoordType::Open)
                grants.push_back(decode_open_reply(reply));
        });
    REQUIRE(grants.size() == 1);
    const Protocol::OpenReply &granted = grants.front();

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
    CHECK(subscription->state() == SubscriberState::Active);

    // The seat keeps the identifier, so the answer names what happened to it.
    CHECK(renew(publisher, granted.session_id).status == Status::UnknownSession);
}

TEST_CASE("Renewing faster than the rate limit is refused without shortening the lease",
          "[m3][session]")
{
    PublisherConfig config = short_lease_config();
    config.max_renewals_per_ttl = 3;

    BulkPublisher publisher(config);
    std::vector<Protocol::OpenReply> grants;
    std::unique_ptr<Subscription> subscription = open_subscription(
        publisher,
        subscriber_config(),
        [&grants](Protocol::CoordType type, const std::vector<std::byte> &reply)
        {
            if(type == Protocol::CoordType::Open)
                grants.push_back(decode_open_reply(reply));
        });
    REQUIRE(grants.size() == 1);
    const Protocol::OpenReply &granted = grants.front();

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
    REQUIRE(collect(*subscription, 1).size() == 1);

    close(publisher, granted.session_id);
}

TEST_CASE("A publisher admits no more sessions than it was configured for", "[m3][session]")
{
    PublisherConfig config = publisher_config();
    config.max_sessions = 2;

    BulkPublisher publisher(config);
    SubscriberConfig subscriber = subscriber_config();
    subscriber.reconnect_policy = ReconnectPolicy::FailFast;
    std::vector<Protocol::OpenReply> grants;
    const auto record_open = [&grants](Protocol::CoordType type,
                                       const std::vector<std::byte> &reply)
    {
        if(type == Protocol::CoordType::Open)
            grants.push_back(decode_open_reply(reply));
    };
    std::unique_ptr<Subscription> first = open_subscription(publisher, subscriber, record_open);
    std::unique_ptr<Subscription> second = open_subscription(publisher, subscriber, record_open);
    REQUIRE(grants.size() == 2);
    const Protocol::OpenReply &a = grants[0];
    const Protocol::OpenReply &b = grants[1];
    CHECK(b.status == Status::Ok);
    CHECK(a.session_id != b.session_id);

    // 6.1 bounds sessions per publisher, and 4.4 says a client that leaks them
    // "hits TooManySessions and that is correct feedback".
    BulkError refusal;
    try
    {
        (void)detail::SubscriptionFactory::open_default(
            subscriber,
            [&publisher](Protocol::CoordType type,
                         const std::vector<std::byte> &request,
                         std::chrono::steady_clock::time_point)
            {
                return detail::PublisherAccess::coordination(
                    publisher, request.data(), request.size(), type);
            },
            SubscriptionCallbacks{});
        FAIL("a third subscription should be refused");
    }
    catch(const BulkException &error)
    {
        refusal = error.error();
    }
    CHECK(refusal.status == Status::TooManySessions);
    CHECK(publisher.counters().sessions_rejected == 1);

    // A closed seat is reusable, so the third client gets in once one leaves.
    first->close();
    REQUIRE(eventually([&] { return publisher.counters().sessions_closed == 1; }));

    std::unique_ptr<Subscription> third = open_subscription(publisher, subscriber, record_open);
    REQUIRE(grants.size() == 3);
    const Protocol::OpenReply &c = grants[2];
    CHECK(c.session_id != a.session_id);

    second->close();
    third->close();
}

TEST_CASE("coordination adapter echoes correlation on outer failures", "[m4][coordination]")
{
    BulkPublisher publisher(publisher_config());

    // This is structurally valid coordination data, but the address cannot be
    // used to create a UCX endpoint.  The failure therefore leaves the typed
    // publisher path through its outer exception conversion.
    Protocol::OpenRequest request;
    request.stream_name = "bulk.slice";
    request.client_instance_id = Protocol::generate_client_instance_id();
    request.requested_max_frame_bytes = k_frame_bytes;
    request.requested_ring_depth = k_ring_depth;
    request.requested_credit_window = k_credit_window;
    request.client_ucx_address = {std::byte{0}};

    constexpr std::uint64_t correlation_id = 0x0102030405060708ull;
    const std::vector<std::byte> encoded = Protocol::encode(request, correlation_id);
    const std::vector<std::byte> raw =
        detail::PublisherAccess::coordination(publisher, encoded.data(), encoded.size());

    Protocol::Envelope envelope;
    REQUIRE(Protocol::decode_envelope(raw.data(), raw.size(), envelope) == Status::Ok);
    CHECK(envelope.msg_type == Protocol::CoordType::Error);
    CHECK(envelope.correlation_id == correlation_id);

    Protocol::ErrorMessage error;
    REQUIRE(Protocol::decode(raw.data(), raw.size(), error) == Status::Ok);
    CHECK(error.status == Status::TransportFailure);
    CHECK(publisher.session_count() == 0);
}

TEST_CASE("coordination adapter preserves recoverable correlations on envelope failures",
          "[coordination][adapter]")
{
    BulkPublisher publisher(publisher_config());
    constexpr std::uint64_t correlation_id = 0x0102030405060708ull;

    SECTION("malformed envelope")
    {
        std::vector<std::byte> request =
            Protocol::encode(Protocol::CloseRequest{}, correlation_id);
        request[0] = std::byte{0};

        const std::vector<std::byte> raw =
            detail::PublisherAccess::coordination(publisher, request.data(), request.size());
        Protocol::Envelope envelope;
        const Protocol::ErrorMessage error = decode_error_reply(raw, envelope);

        CHECK(error.status == Status::MalformedMessage);
        CHECK(envelope.correlation_id == correlation_id);
    }

    SECTION("unsupported version")
    {
        std::vector<std::byte> request =
            Protocol::encode(Protocol::CloseRequest{}, correlation_id);
        request[4] = std::byte{2};

        const std::vector<std::byte> raw =
            detail::PublisherAccess::coordination(publisher, request.data(), request.size());
        Protocol::Envelope envelope;
        const Protocol::ErrorMessage error = decode_error_reply(raw, envelope);

        CHECK(error.status == Status::UnsupportedVersion);
        CHECK(envelope.correlation_id == correlation_id);
    }
}

TEST_CASE("coordination adapter rejects the wrong request class and replies",
          "[coordination][adapter]")
{
    BulkPublisher publisher(publisher_config());
    constexpr std::uint64_t correlation_id = 0x0A0B0C0D0E0F1011ull;

    SECTION("expected command mismatch")
    {
        const std::vector<std::byte> request =
            Protocol::encode(Protocol::RenewRequest{}, correlation_id);
        const std::vector<std::byte> raw = detail::PublisherAccess::coordination(
            publisher,
            request.data(),
            request.size(),
            Protocol::CoordType::Open);
        Protocol::Envelope envelope;
        const Protocol::ErrorMessage error = decode_error_reply(raw, envelope);

        CHECK(error.status == Status::MalformedMessage);
        CHECK(envelope.correlation_id == correlation_id);
        CHECK(publisher.session_count() == 0);
    }

    SECTION("reply received as a request")
    {
        const std::vector<std::byte> request =
            Protocol::encode(Protocol::CloseReply{}, correlation_id);
        const std::vector<std::byte> raw =
            detail::PublisherAccess::coordination(publisher, request.data(), request.size());
        Protocol::Envelope envelope;
        const Protocol::ErrorMessage error = decode_error_reply(raw, envelope);

        CHECK(error.status == Status::MalformedMessage);
        CHECK(envelope.correlation_id == correlation_id);
    }
}

TEST_CASE("encoded coordination ingress is nonthrowing for short input",
          "[coordination][adapter]")
{
    BulkPublisher publisher(publisher_config());
    std::vector<std::byte> raw;

    CHECK_NOTHROW(raw = detail::PublisherAccess::coordination(publisher, nullptr, 0));
    REQUIRE_FALSE(raw.empty());

    Protocol::ErrorMessage error;
    REQUIRE(Protocol::decode(raw.data(), raw.size(), error) == Status::Ok);
    CHECK(error.status == Status::MalformedMessage);
}
