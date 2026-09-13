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
        request.requested_caps = Protocol::k_caps_credit_coalescing | Protocol::k_caps_probe;
        request.client_instance_id = Protocol::generate_client_instance_id();
        request.requested_max_frame_bytes = config.receive_plan->max_frame_bytes;
        request.requested_ring_depth = config.receive_plan->ring_depth;
        request.requested_credit_window = config.receive_plan->credit_window;
        request.requested_memory_kind = MemoryKind::Host;
        request.requested_transport = Protocol::Transport::ActiveMessage;
        request.drop_policy = Protocol::DropPolicy::DropNewest;
        request.stream_name = config.stream_name;
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

inline std::vector<FrameView> collect_transport(detail::DeliveryQueue &delivery,
                                                std::size_t want,
                                                std::chrono::milliseconds budget = 5s)
{
    std::vector<FrameView> views;
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while(views.size() < want && std::chrono::steady_clock::now() < deadline)
    {
        FrameView view;
        if(!delivery.take(view, deadline))
            break;
        views.push_back(std::move(view));
    }
    return views;
}

} // namespace TangoBulkTests

#endif // TANGO_BULK_TESTS_UCX_TRANSPORT_FIXTURE_H
