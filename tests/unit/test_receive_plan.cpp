// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/limits.h>
#include <tango-bulk/subscriber.h>

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

StreamOffer stream_offer()
{
    StreamOffer offer;
    offer.status = Status::Ok;
    offer.stream_name = "bulk.unit";
    offer.geometry = granted_geometry();
    return offer;
}

} // namespace

TEST_CASE("a receive plan derives its physical byte count", "[core][receive-plan]")
{
    ReceivePlan plan{8ull << 20, 32, 16};

    CHECK(plan.pinned_bytes() == 256ull << 20);
    CHECK(plan.validate() == Status::Ok);

    SECTION("frame size is bounded")
    {
        plan.max_frame_bytes = k_max_frame_bytes_hard_cap + 1;
        CHECK(plan.validate() == Status::FrameTooLarge);
    }

    SECTION("ring depth is bounded")
    {
        plan.ring_depth = k_max_ring_depth + 1;
        CHECK(plan.validate() == Status::DepthTooLarge);
    }

    SECTION("credit cannot exceed the ring")
    {
        plan.credit_window = plan.ring_depth + 1;
        CHECK(plan.validate() == Status::DepthTooLarge);
    }

    SECTION("byte multiplication cannot wrap")
    {
        plan = ReceivePlan{~std::uint64_t{0}, k_max_ring_depth, 1};
        CHECK(plan.validate() == Status::ResourceExhausted);
    }
}

TEST_CASE("BulkStreams rows have one versioned fixed field order", "[core][stream-offer]")
{
    StreamOffer offer = stream_offer();
    offer.age_ms = 17;

    const std::string row = offer.to_bulk_stream_row();
    CHECK(row == "1|bulk.unit|1|1|1|1|16777216|64|32|4096,0,0,0|1,0,0,0|17");
    CHECK(StreamOffer::from_bulk_stream_row(row) == offer);
}

TEST_CASE("BulkStreams rows reject unsafe observations", "[core][stream-offer]")
{
    const StreamOffer offer = stream_offer();

    SECTION("malformed row")
    {
        const StreamOffer malformed = StreamOffer::from_bulk_stream_row("1|bulk.unit");
        CHECK(malformed.status == Status::MalformedMessage);
        CHECK(malformed.outcome() == StreamOffer::Outcome::Malformed);
    }

    SECTION("unsupported version")
    {
        const std::string row = offer.to_bulk_stream_row();
        const StreamOffer unsupported = StreamOffer::from_bulk_stream_row(
            std::string("2") + row.substr(row.find('|')));
        CHECK(unsupported.validate() == Status::UnsupportedVersion);
        CHECK_FALSE(unsupported.available());
    }

    SECTION("stale observation")
    {
        StreamOffer stale = offer;
        stale.age_ms = StreamOffer::k_max_age_ms + 1;
        stale = StreamOffer::from_bulk_stream_row(stale.to_bulk_stream_row());
        CHECK(stale.validate() == Status::TransportFailure);
        CHECK(stale.outcome() == StreamOffer::Outcome::Stale);
        CHECK_FALSE(stale.available());
    }

    SECTION("unavailable stream")
    {
        StreamOffer unavailable = offer;
        unavailable.status = Status::UnknownStream;
        unavailable.message = "BulkStreams is empty";
        CHECK(unavailable.validate() == Status::UnknownStream);
        CHECK_FALSE(unavailable.available());
    }
}
