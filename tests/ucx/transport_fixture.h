// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_TESTS_UCX_TRANSPORT_FIXTURE_H
#define TANGO_BULK_TESTS_UCX_TRANSPORT_FIXTURE_H

#include "slice.h"

#include <ucx/subscriber_engine.h>

#include <core/delivery_queue.h>
#include <core/publisher_internal.h>

#include <core/protocol.h>

#include <memory>
#include <utility>
#include <vector>

namespace TangoBulkTests
{

/// Direct transport setup for tests that inspect UCX-only instrumentation.
/// It does not own lifecycle policy; all lifecycle assertions use Slice.
struct TransportFixture
{
    SubscriptionOptions config;
    std::shared_ptr<detail::DeliveryQueue> delivery;
    std::shared_ptr<detail::DeliveryIngress> ingress;
    detail::SubscriberEngine engine;

    /// What this fixture asks for at Open. The C++ Subscription cannot request
    /// either yet -- that is step 5 -- so the publisher's per-session flow and
    /// label are driven from here, which is the layer that speaks the wire.
    Protocol::FlowPolicy flow{Protocol::FlowPolicy::Lossy};
    std::string client_label;

    explicit TransportFixture(SubscriptionOptions opts = subscription_options(),
                              detail::TransportOptions transport = {}) :
        config(std::move(opts)),
        delivery(std::make_shared<detail::DeliveryQueue>(config.receive_plan->ring_depth,
                                                         config.queue_policy)),
        ingress(delivery->make_ingress()),
        engine(*config.receive_plan,
               config.ownership,
               ReceiveRegion{},
               config.pinned_budget_bytes,
               std::move(transport),
               ingress)
    {
    }

    void open(BulkPublisher &publisher)
    {
        publisher_ = &publisher;

        Protocol::OpenRequest request;
        request.version_min = Protocol::k_version_major;
        request.version_max = Protocol::k_version_major;
        request.client_instance_id = Protocol::generate_client_instance_id();
        request.requested_max_frame_bytes = config.receive_plan->max_frame_bytes;
        request.requested_ring_depth = config.receive_plan->ring_depth;
        request.requested_credit_window = config.receive_plan->credit_window;
        request.requested_memory_kind = MemoryKind::Host;
        request.flow = flow;
        request.stream_name = config.stream_name;
        request.client_label = client_label;
        request.client_ucx_address = engine.local_address();

        const std::vector<std::byte> encoded = Protocol::encode(request, 1);
        const std::vector<std::byte> raw = detail::PublisherAccess::coordination(
            publisher, encoded.data(), encoded.size(), Protocol::CoordType::Open);

        Protocol::OpenReply reply;
        REQUIRE(Protocol::decode(raw.data(), raw.size(), reply) == Status::Ok);
        REQUIRE(reply.status == Status::Ok);
        session_id_ = reply.session_id;
        REQUIRE(engine.activate(reply.stream_id, reply.geometry, reply.server_ucx_address) ==
                Status::Ok);
        REQUIRE(eventually([&] { return publisher.session_count() == 1; }));
        REQUIRE(eventually([&] { return engine.state() == SubscriberState::Active; }));
    }

    /// One Renew, as a client that is alive but not consuming would send.
    ///
    /// It is what separates the lease deadline from the credit deadline: a
    /// session that keeps renewing proves the process is up, and only the
    /// credit deadline can then say it has stopped doing its job.
    Status renew()
    {
        Protocol::RenewRequest request;
        request.session_id = session_id_;
        const std::vector<std::byte> encoded = Protocol::encode(request, 3);
        const std::vector<std::byte> raw = detail::PublisherAccess::coordination(
            *publisher_, encoded.data(), encoded.size(), Protocol::CoordType::Renew);

        Protocol::RenewReply reply;
        if(Protocol::decode(raw.data(), raw.size(), reply) != Status::Ok)
        {
            return Status::MalformedMessage;
        }

        return reply.status;
    }

    ~TransportFixture()
    {
        if(publisher_ != nullptr && !session_id_.is_zero())
        {
            Protocol::CloseRequest request;
            request.session_id = session_id_;
            request.reason = Protocol::CloseReason::ClientShutdown;
            const std::vector<std::byte> encoded = Protocol::encode(request, 2);
            (void)detail::PublisherAccess::coordination(
                *publisher_, encoded.data(), encoded.size(), Protocol::CoordType::Close);
        }
    }

    TransportFixture(const TransportFixture &) = delete;
    TransportFixture &operator=(const TransportFixture &) = delete;

  private:
    BulkPublisher *publisher_{nullptr};
    Protocol::SessionId session_id_{};
};

/// The zero-copy assertions of RFC 9.3, written where they are made.
///
/// The engine reports where its registered ring is; deciding whether a pointer
/// landed in it, and in which slot, is the test's own arithmetic rather than a
/// library member that exists for one caller.
inline bool in_receive_ring(const detail::SubscriberEngine &engine, const void *p) noexcept
{
    const auto ring = engine.ring_placement();
    const auto *byte = static_cast<const std::byte *>(p);
    return byte >= ring.base && byte < ring.base + ring.depth * ring.slot_bytes;
}

inline const std::byte *receive_slot(const detail::SubscriberEngine &engine,
                                     std::uint64_t index) noexcept
{
    const auto ring = engine.ring_placement();
    return ring.base + index * ring.slot_bytes;
}

inline std::vector<FrameView> collect_transport(detail::DeliveryQueue &delivery,
                                                std::size_t want,
                                                std::chrono::milliseconds budget = 5s)
{
    std::vector<FrameView> views;
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while(views.size() < want && std::chrono::steady_clock::now() < deadline)
    {
        detail::DeliveryRead read = delivery.read_result(deadline);
        if(read.kind != detail::DeliveryRead::Kind::Frame)
            break;
        views.push_back(std::move(read.frame));
    }
    return views;
}

} // namespace TangoBulkTests

#endif // TANGO_BULK_TESTS_UCX_TRANSPORT_FIXTURE_H
