// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

// Per-session flow policy: RFC 7.2 and 7.3.
//
// The publisher has no fan-out mode to configure. What it does when a
// subscriber runs out of credit is that subscriber's declaration at Open, so
// these tests drive the wire directly through TransportFixture -- the C++
// Subscription cannot ask for Lossless until the interface step lands, and the
// publisher's behaviour must not wait on it.

#include "slice.h"
#include "transport_fixture.h"

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <optional>
#include <vector>

using namespace TangoBulk;
using namespace TangoBulkTests;

namespace
{

PublisherConfig lossless_config()
{
    PublisherConfig config = publisher_config();
    config.allow_lossless = true;
    return config;
}

/// Publish until the publisher stops accepting, and report how it stopped.
///
/// The count matters as much as the verdict: a lossless session must absorb its
/// whole negotiated window before it pushes back, or the backpressure is
/// arriving early and the window is a fiction.
PublishResult publish_until_refused(BulkPublisher &publisher, unsigned &accepted)
{
    accepted = 0;
    for(unsigned i = 0; i < k_ring_depth * 4; ++i)
    {
        BulkPublisher::SlotHandle lease = publisher.try_acquire();
        if(!lease)
        {
            return PublishResult::QueueFull;
        }

        fill(lease, k_frame_bytes, i);
        const PublishResult result =
            publisher.publish(std::move(lease), meta_for(k_frame_bytes, i));
        if(result != PublishResult::Accepted)
        {
            return result;
        }

        ++accepted;
    }

    return PublishResult::Accepted;
}

} // namespace

TEST_CASE("a publisher that does not offer lossless refuses the request",
          "[ucx][flow]")
{
    // The default. A deployment gets the power to stall acquisition only by
    // asking for it, and a client that asked for every frame is told no rather
    // than quietly handed a lossy session.
    BulkPublisher publisher(publisher_config());
    REQUIRE_FALSE(publisher_config().allow_lossless);

    Protocol::OpenRequest request;
    request.stream_name = publisher_config().stream_name;
    request.requested_max_frame_bytes = k_frame_bytes;
    request.requested_ring_depth = k_ring_depth;
    request.requested_credit_window = k_credit_window;
    request.client_ucx_address = {std::byte{0x01}};
    request.flow = Protocol::FlowPolicy::Lossless;

    const std::vector<std::byte> encoded = Protocol::encode(request, 11);
    const std::vector<std::byte> raw =
        detail::PublisherAccess::coordination(publisher, encoded.data(), encoded.size());

    Protocol::ErrorMessage error;
    REQUIRE(Protocol::decode(raw.data(), raw.size(), error) == Status::Ok);
    CHECK(error.status == Status::NotAuthorized);

    // Refused before the endpoint is built, which is why the address above can
    // be a stub: the policy answer does not depend on the client being
    // reachable, and paying for a connection to refuse it would be backwards.
    CHECK(publisher.session_count() == 0);
    CHECK(publisher.counters().sessions_opened == 0);
}

TEST_CASE("a lossy subscriber that stops taking frames is skipped, not waited for",
          "[ucx][flow]")
{
    BulkPublisher publisher(lossless_config());
    TransportFixture transport;
    transport.open(publisher);

    unsigned accepted = 0;
    const PublishResult refusal = publish_until_refused(publisher, accepted);

    // CreditStalled drops the frame and consumes the lease; WouldBlock would
    // mean the publisher had decided to wait for this subscriber, which is
    // exactly what a lossy session must never be able to make it do.
    CHECK(refusal != PublishResult::WouldBlock);
    CHECK(publisher.counters().dropped_credit_stalled != 0);
}

TEST_CASE("a lossless subscriber that stops taking frames holds the publisher back",
          "[ucx][flow]")
{
    BulkPublisher publisher(lossless_config());
    TransportFixture transport;
    transport.flow = Protocol::FlowPolicy::Lossless;
    transport.open(publisher);

    unsigned accepted = 0;
    const PublishResult refusal = publish_until_refused(publisher, accepted);

    REQUIRE(refusal == PublishResult::WouldBlock);

    // Retryable, and nothing was dropped: WouldBlock returns the producer's
    // filled slot, which is the whole difference from CreditStalled.
    CHECK(publisher.counters().dropped_credit_stalled == 0);
    CHECK(publisher.counters().frames_published == accepted);

    // Releasing a view returns its credit -- the scope is the interlock, so the
    // vector has to die before the publisher can move. Nothing was lost in the
    // meantime, which is what lossless bought.
    {
        const std::vector<FrameView> views = collect_transport(*transport.delivery, 1);
        REQUIRE(views.size() == 1);
    }

    REQUIRE(eventually(
        [&]
        {
            BulkPublisher::SlotHandle lease = publisher.try_acquire();
            if(!lease)
            {
                return false;
            }
            fill(lease, k_frame_bytes, 0xAB);
            return publisher.publish(std::move(lease), meta_for(k_frame_bytes, 0xAB)) ==
                   PublishResult::Accepted;
        }));
}

TEST_CASE("a lossless session that owes credit past its lease TTL is evicted and counted",
          "[ucx][flow]")
{
    // RFC 7.3. The client here is alive and renewing; what it has stopped doing
    // is returning credit, and the lease is the only deadline that bounds it.
    PublisherConfig config = lossless_config();
    config.lease_ttl_ms = k_min_lease_ttl_ms;
    config.renew_interval_ms = 300;

    BulkPublisher publisher(config);
    TransportFixture transport;
    transport.flow = Protocol::FlowPolicy::Lossless;
    transport.open(publisher);

    unsigned accepted = 0;
    REQUIRE(publish_until_refused(publisher, accepted) == PublishResult::WouldBlock);

    REQUIRE(eventually(
        [&]
        {
            // Renewing keeps the lease deadline ahead of now, so an eviction
            // here can only be the credit deadline.
            (void)transport.renew();
            return publisher.counters().sessions_evicted_stalled != 0;
        },
        10s));

    CHECK(publisher.session_count() == 0);
    CHECK(publisher.counters().sessions_expired != 0);
}

TEST_CASE("an idle lossless session that owes nothing is never evicted for it",
          "[ucx][flow]")
{
    // The deadline is only consulted while the session owes credit. Without
    // that, a healthy client attached to a stream that is not publishing would
    // be thrown off for the publisher's own silence.
    PublisherConfig config = lossless_config();
    config.lease_ttl_ms = k_min_lease_ttl_ms;
    config.renew_interval_ms = 300;

    BulkPublisher publisher(config);
    TransportFixture transport;
    transport.flow = Protocol::FlowPolicy::Lossless;
    transport.open(publisher);

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(2 * k_min_lease_ttl_ms);
    while(std::chrono::steady_clock::now() < deadline)
    {
        REQUIRE(transport.renew() == Status::Ok);
        std::this_thread::sleep_for(100ms);
    }

    CHECK(publisher.counters().sessions_evicted_stalled == 0);
    CHECK(publisher.session_count() == 1);
}

TEST_CASE("a refused lossless request reaches the caller rather than being downgraded",
          "[ucx][flow]")
{
    // The interface half of the publisher's refusal. A writer that asked for
    // every frame must not be handed a Subscription that drops them and a
    // success return; the refusal is the answer, and the caller sees it.
    SubscriptionOptions options = subscription_options();
    options.flow = FlowPolicy::Lossless;

    BulkPublisher publisher(publisher_config()); // allow_lossless defaults false

    CHECK_THROWS_AS(open_subscription(publisher, options), EstablishmentError);
    CHECK(publisher.session_count() == 0);

    // The same options against a publisher that grants lossless do open.
    BulkPublisher granting(lossless_config());
    std::unique_ptr<Subscription> subscription = open_subscription(granting, options);
    REQUIRE(subscription);
    CHECK(granting.snapshot().sessions.front().flow == "Lossless");
}

TEST_CASE("a lossless copied frame keeps withholding credit while it is queued",
          "[ucx][flow]")
{
    // The one place the delivery queue and the credit path meet.
    //
    // Copied delivery normally returns the slot's credit at commit, so the
    // publisher never learns how long the application keeps a frame -- which is
    // exactly right for a lossy session and exactly wrong for a lossless one.
    // A frame sitting in the local queue has not reached anybody; crediting it
    // would let the publisher run ahead and the queue would then have to drop,
    // which is the loss the session was promised would not happen. So the
    // credit stays withheld until the application takes the frame.
    SubscriptionOptions options = subscription_options();
    options.flow = FlowPolicy::Lossless;
    options.ownership = DeliveryOwnership::Copy;

    BulkPublisher publisher(lossless_config());
    std::unique_ptr<Subscription> subscription = open_subscription(publisher, options);

    // Nothing is read, so every frame published lands in the local queue.
    unsigned accepted = 0;
    const PublishResult refusal = publish_until_refused(publisher, accepted);

    CHECK(refusal == PublishResult::WouldBlock);
    CHECK(publisher.counters().dropped_credit_stalled == 0);

    // Reading one frame is the handoff, and it is what lets the publisher move.
    // Exactly one: collect() would drain the queue and prove nothing about
    // which claim released which credit.
    std::optional<FrameView> taken = subscription->read_for(1s);
    REQUIRE(taken);

    REQUIRE(eventually(
        [&]
        {
            BulkPublisher::SlotHandle lease = publisher.try_acquire();
            if(!lease)
            {
                return false;
            }
            fill(lease, k_frame_bytes, 0xCD);
            return publisher.publish(std::move(lease), meta_for(k_frame_bytes, 0xCD)) ==
                   PublishResult::Accepted;
        }));

    // A copied frame withholds credit only while queued: the application still
    // holds this view, and the publisher moved anyway.
    CHECK_FALSE(taken->borrowed());
    CHECK(static_cast<bool>(*taken));
}

TEST_CASE("a lossy copied frame returns its credit without waiting to be read",
          "[ucx][flow]")
{
    // The contrast that makes the previous test about flow rather than about
    // copying: same delivery mode, same unread queue, opposite answer.
    SubscriptionOptions options = subscription_options();
    options.flow = FlowPolicy::Lossy;
    options.ownership = DeliveryOwnership::Copy;

    BulkPublisher publisher(lossless_config());
    std::unique_ptr<Subscription> subscription = open_subscription(publisher, options);

    unsigned accepted = 0;
    const PublishResult refusal = publish_until_refused(publisher, accepted);

    // Never WouldBlock: a lossy session is skipped, never waited for.
    CHECK(refusal != PublishResult::WouldBlock);

    // And nothing is ever read here, yet the publisher gets past its window
    // anyway -- because a lossy copy returns its credit at commit rather than
    // at handoff. Asserting progress over time rather than a count from the
    // first burst, since the credit round trip is asynchronous.
    REQUIRE(eventually(
        [&]
        {
            BulkPublisher::SlotHandle lease = publisher.try_acquire();
            if(lease)
            {
                fill(lease, k_frame_bytes, 0x5A);
                (void) publisher.publish(std::move(lease), meta_for(k_frame_bytes, 0x5A));
            }
            return publisher.counters().frames_published > k_credit_window;
        }));
}

TEST_CASE("the client label and flow policy reach the session observation",
          "[ucx][flow]")
{
    // RFC 9.3: the operator-facing handle is the label the client supplied, not
    // the session id, which is a bearer credential.
    BulkPublisher publisher(lossless_config());
    TransportFixture transport;
    transport.flow = Protocol::FlowPolicy::Lossless;
    transport.client_label = "hdf5-writer-2";
    transport.open(publisher);

    const PublisherSnapshot snapshot = publisher.snapshot();
    REQUIRE(snapshot.sessions.size() == 1);
    CHECK(snapshot.sessions.front().client_label == "hdf5-writer-2");
    CHECK(snapshot.sessions.front().flow == "Lossless");

    // The ordinal names the session, and the id is never published whole.
    CHECK(snapshot.sessions.front().ordinal == 1);
    CHECK(snapshot.sessions.front().session_id.size() < 2 * sizeof(Protocol::SessionId));
}
