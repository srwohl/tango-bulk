// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

// The BulkStreams row codec, tested next to the adapter that owns the format.
//
// It lives here rather than in tests/unit because the row exists only because
// BulkStreams is a Tango string spectrum; the core type it carries has no
// opinion about how it travels.

#include "bulk_stream_row.h"

#include <catch2/catch_test_macros.hpp>

#include <string>

using namespace TangoBulk;

namespace
{

StreamOffer stream_offer()
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

    StreamOffer offer;
    offer.status = Status::Ok;
    offer.stream_name = "bulk.unit";
    offer.geometry = geometry;
    return offer;
}

} // namespace

TEST_CASE("BulkStreams rows have one versioned fixed field order", "[tango][stream-offer]")
{
    StreamOffer offer = stream_offer();
    offer.age_ms = 17;

    const std::string row = detail::stream_offer_to_row(offer);
    CHECK(row == "1|bulk.unit|1|1|1|1|16777216|64|32|4096,0,0,0|1,0,0,0|17");
    CHECK(detail::stream_offer_from_row(row) == offer);
}

TEST_CASE("BulkStreams rows reject unsafe observations", "[tango][stream-offer]")
{
    const StreamOffer offer = stream_offer();

    SECTION("malformed row")
    {
        const StreamOffer malformed = detail::stream_offer_from_row("1|bulk.unit");
        CHECK(malformed.status == Status::MalformedMessage);
        CHECK(malformed.outcome() == StreamOffer::Outcome::Malformed);
    }

    SECTION("unsupported version")
    {
        const std::string row = detail::stream_offer_to_row(offer);
        const StreamOffer unsupported = detail::stream_offer_from_row(
            std::string("2") + row.substr(row.find('|')));
        CHECK(unsupported.validate() == Status::UnsupportedVersion);
        CHECK_FALSE(unsupported.available());
    }

    SECTION("stale observation")
    {
        StreamOffer stale = offer;
        stale.age_ms = StreamOffer::k_max_age_ms + 1;
        stale = detail::stream_offer_from_row(detail::stream_offer_to_row(stale));
        CHECK(stale.validate() == Status::TransportFailure);
        CHECK(stale.outcome() == StreamOffer::Outcome::Stale);
        CHECK_FALSE(stale.available());
    }

    SECTION("an unavailable offer has no row")
    {
        StreamOffer unavailable = offer;
        unavailable.status = Status::UnknownStream;
        unavailable.message = "BulkStreams is empty";
        CHECK(unavailable.validate() == Status::UnknownStream);
        CHECK_FALSE(unavailable.available());
        CHECK(detail::stream_offer_to_row(unavailable).empty());
    }
}
