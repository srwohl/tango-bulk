// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include "transport_fixture.h"

#include <core/cpu_topology.h>

#include <catch2/catch_test_macros.hpp>

using namespace TangoBulk;
using namespace TangoBulkTests;

TEST_CASE("The transport places a rendezvous payload in its registered ring",
          "[ucx][transport]")
{
    BulkPublisher publisher(publisher_config());
    TransportFixture transport;
    transport.open(publisher);

    REQUIRE(publish_one(publisher, k_frame_bytes, 0x11) == PublishResult::Accepted);
    const std::vector<FrameView> views = collect_transport(*transport.delivery, 1);
    REQUIRE(views.size() == 1);

    CHECK(in_receive_ring(transport.engine, views.front().data()));
    CHECK(views.front().data() == receive_slot(transport.engine, 0));
    CHECK(transport.engine.counters().bytes_copied == 0);
    CHECK(views.front().borrowed());
    CHECK(payload_matches(views.front(), 0x11));
}

TEST_CASE("The transport maps sequence numbers onto receive slots",
          "[ucx][transport]")
{
    BulkPublisher publisher(publisher_config());
    TransportFixture transport;
    transport.open(publisher);

    const std::uint32_t depth = transport.engine.ring_placement().depth;
    REQUIRE(depth == k_ring_depth);

    for(std::size_t sequence = 0; sequence < 4u * depth; ++sequence)
    {
        const auto seed = static_cast<unsigned>(sequence);
        REQUIRE(publish_one(publisher, 8192, seed) == PublishResult::Accepted);
        const std::vector<FrameView> views = collect_transport(*transport.delivery, 1);
        REQUIRE(views.size() == 1);
        CHECK(views.front().sequence() == sequence);
        CHECK(views.front().data() == receive_slot(transport.engine, sequence % depth));
        CHECK(payload_matches(views.front(), seed));
    }

    CHECK(eventually([&] { return publisher.counters().frames_credited == 4u * depth; }));
}

TEST_CASE("engine_cpu_affinity is reflected by the transport placement report",
          "[ucx][transport]")
{
    if(detail::allowed_cpu_count() < 2)
    {
        SUCCEED("needs at least two permitted CPUs to inspect affinity");
        return;
    }

    detail::TransportOptions pinned;
    pinned.engine_cpu_affinity = 1;

    BulkPublisher publisher(publisher_config());
    TransportFixture transport(subscription_options(), pinned);
    transport.open(publisher);

    const detail::Locality &where = transport.engine.locality();
    CHECK(where.engine_cpu == 1);
    CHECK_FALSE(where.transport.empty());
    CHECK_FALSE(where.device.empty());
    CHECK(where.host_nodes >= 1);
    CHECK(detail::to_string(where.placement()) != nullptr);
}
