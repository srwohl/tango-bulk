// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/limits.h>
#include <tango-bulk/subscriber.h>

#include <core/subscription_internal.h>

#include <catch2/catch_test_macros.hpp>

using namespace TangoBulk;

namespace
{

Geometry granted_geometry()
{
    Geometry geometry;
    geometry.generation = 1;
    geometry.element_type = ElementType::UInt8;
    geometry.element_size = 1;
    geometry.rank = 1;
    geometry.max_frame_bytes = 16ull << 20;
    geometry.ring_depth = 64;
    geometry.credit_window = 32;
    geometry.shape[0] = 4096;
    geometry.strides[0] = 1;
    return geometry;
}

} // namespace

TEST_CASE("a complete receive upper plan owns its physical byte count", "[core][receive-plan]")
{
    const ReceivePlan plan = ReceivePlan::from_limits(8ull << 20, 32, 16);

    CHECK(plan.max_frame_bytes == 8ull << 20);
    CHECK(plan.ring_depth == 32);
    CHECK(plan.credit_window == 16);
    CHECK(plan.pinned_bytes == 256ull << 20);
    CHECK(plan.validate() == Status::Ok);
}

TEST_CASE("receive plan validation reports the violated domain", "[core][receive-plan]")
{
    ReceivePlan plan = ReceivePlan::from_limits(8ull << 20, 32, 16);

    SECTION("frame size")
    {
        plan.max_frame_bytes = k_max_frame_bytes_hard_cap + 1;
        CHECK(plan.validate() == Status::FrameTooLarge);
    }

    SECTION("depth")
    {
        plan.ring_depth = k_max_ring_depth + 1;
        CHECK(plan.validate() == Status::DepthTooLarge);
    }

    SECTION("credit")
    {
        plan.credit_window = plan.ring_depth + 1;
        CHECK(plan.validate() == Status::DepthTooLarge);
    }

    SECTION("derived bytes")
    {
        plan.pinned_bytes -= 1;
        CHECK(plan.validate() == Status::ResourceExhausted);
    }
}

TEST_CASE("a granted plan is clamped downward without changing its byte formula",
          "[core][receive-plan]")
{
    const ReceivePlan upper = ReceivePlan::from_limits(32ull << 20, 128, 64);
    const ReceivePlan actual = ReceivePlan::intersect(upper, granted_geometry());

    CHECK(actual.max_frame_bytes == 16ull << 20);
    CHECK(actual.ring_depth == 64);
    CHECK(actual.credit_window == 32);
    CHECK(actual.pinned_bytes == 1ull << 30);
    CHECK(actual.validate() == Status::Ok);
}

TEST_CASE("an overflowing receive plan cannot become valid by wrapping its byte count",
          "[core][receive-plan]")
{
    const ReceivePlan plan = ReceivePlan::from_limits(~std::uint64_t{0}, k_max_ring_depth, 1);

    CHECK(plan.validate() == Status::ResourceExhausted);
}

TEST_CASE("a stream offer is a safe pre-open receive-plan upper bound",
          "[core][stream-offer]")
{
    StreamOffer offer;
    offer.status = Status::Ok;
    offer.stream_name = "bulk.unit";
    offer.geometry = granted_geometry();

    REQUIRE(offer.validate() == Status::Ok);
    CHECK(offer.available());

    const ReceivePlan derived = ReceivePlan::derive(offer, 64ull << 20);
    CHECK(derived.max_frame_bytes == 16ull << 20);
    CHECK(derived.ring_depth == 4);
    CHECK(derived.credit_window == 4);
    CHECK(derived.pinned_bytes == 64ull << 20);
    CHECK(derived.validate() == Status::Ok);

    const ReceivePlan caller_upper = ReceivePlan::from_limits(8ull << 20, 32, 16);
    const ReceivePlan intersected = ReceivePlan::intersect(caller_upper, offer);
    CHECK(intersected.max_frame_bytes == 8ull << 20);
    CHECK(intersected.ring_depth == 32);
    CHECK(intersected.credit_window == 16);
    CHECK(intersected.pinned_bytes == 256ull << 20);
    CHECK(intersected.validate() == Status::Ok);
}

TEST_CASE("an unsafe or unavailable offer never produces an allocation plan",
          "[core][stream-offer]")
{
    StreamOffer offer;
    offer.status = Status::Ok;
    offer.stream_name = "bulk.unit";
    offer.geometry = granted_geometry();

    SECTION("the budget cannot hold the minimum ring")
    {
        CHECK(ReceivePlan::derive(offer, offer.geometry.max_frame_bytes) == ReceivePlan{});
    }

    SECTION("an unavailable stream is typed and empty")
    {
        offer.status = Status::UnknownStream;
        offer.message = "BulkStreams is empty";

        CHECK(offer.validate() == Status::UnknownStream);
        CHECK_FALSE(offer.available());
        CHECK(ReceivePlan::derive(offer, 64ull << 20) == ReceivePlan{});
    }

    SECTION("a newer row version is refused")
    {
        offer.version = StreamOffer::k_version + 1;
        CHECK(offer.validate() == Status::UnsupportedVersion);
        CHECK_FALSE(offer.available());
    }
}

TEST_CASE("in-memory discovery returns copies and typed missing-stream results",
          "[core][stream-offer]")
{
    StreamOffer offer;
    offer.status = Status::Ok;
    offer.stream_name = "bulk.unit";
    offer.geometry = granted_geometry();

    detail::InMemoryStreamDiscovery discovery({offer});

    StreamOffer found = discovery.discover("bulk.unit");
    REQUIRE(found == offer);

    found.geometry.ring_depth = 2;
    CHECK(discovery.discover("bulk.unit").geometry.ring_depth == 64);

    const StreamOffer missing = discovery.discover("other");
    CHECK(missing.status == Status::UnknownStream);
    CHECK(missing.stream_name == "other");
    CHECK_FALSE(missing.available());
}

TEST_CASE("SubscriberConfig exposes one normalized upper receive plan",
          "[core][receive-plan]")
{
    SubscriberConfig config;
    config.stream_name = "bulk.unit";
    config.max_frame_bytes = 8ull << 20;
    config.ring_depth = 32;
    config.credit_window = 16;
    config.pinned_memory_limit_bytes = 16ull << 20;

    CHECK(config.validate() == Status::Ok);
    const ReceivePlan budget_fit = config.upper_receive_plan();
    CHECK(budget_fit == ReceivePlan::from_limits(8ull << 20, 2, 2));

    config.receive_plan = ReceivePlan::from_limits(4ull << 20, 4, 4);
    CHECK(config.validate() == Status::Ok);
    CHECK(config.upper_receive_plan() == *config.receive_plan);

    config.receive_plan = ReceivePlan::from_limits(8ull << 20, 32, 16);
    CHECK(config.validate() == Status::ResourceExhausted);

    config.receive_plan.reset();
    config.max_frame_bytes = 16ull << 20;
    config.ring_depth = 2;
    config.credit_window = 2;
    config.pinned_memory_limit_bytes = 16ull << 20;
    CHECK(config.validate() == Status::ResourceExhausted);

    config.max_frame_bytes = 4ull << 20;
    config.ring_depth = 2;
    config.credit_window = 2;
    config.receive_buffer = std::shared_ptr<void>(
        ::operator new(32ull << 20), [](void *pointer) { ::operator delete(pointer); });
    config.receive_buffer_bytes = 32ull << 20;
    config.pinned_memory_limit_bytes = 16ull << 20;
    CHECK(config.validate() == Status::ResourceExhausted);
}

TEST_CASE("discovery and budget are both applied before Open", "[core][stream-offer]")
{
    StreamOffer offer;
    offer.status = Status::Ok;
    offer.stream_name = "bulk.unit";
    offer.geometry = granted_geometry();

    SubscriberConfig config;
    config.stream_name = offer.stream_name;
    config.max_frame_bytes = 32ull << 20;
    config.ring_depth = 128;
    config.credit_window = 64;
    config.pinned_memory_limit_bytes = 64ull << 20;
    config.discovery_offer = offer;

    REQUIRE(config.validate() == Status::Ok);
    CHECK(config.upper_receive_plan() == ReceivePlan::from_limits(16ull << 20, 4, 4));
}
