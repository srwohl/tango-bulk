// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

// GeometryBlock validation (spec 3.4) and FrameMetadata (spec 2.2).

#include <tango-bulk/frame.h>
#include <tango-bulk/limits.h>
#include <tango-bulk/protocol.h>

#include <catch2/catch_test_macros.hpp>

using namespace TangoBulk;
using namespace TangoBulk::Protocol;

namespace
{

GeometryBlock valid()
{
    GeometryBlock g;
    g.generation = 1;
    g.element_type = ElementType::UInt16;
    g.element_size = 2;
    g.rank = 2;
    g.max_frame_bytes = 8ull << 20;
    g.ring_depth = 32;
    g.credit_window = 16;
    g.shape = {1024, 1024, 0, 0};
    g.strides = {2048, 2, 0, 0};
    return g;
}

} // namespace

TEST_CASE("a well-formed geometry validates", "[core][geometry]")
{
    CHECK(valid().validate() == Status::Ok);
}

TEST_CASE("generation zero is never legal on the wire", "[core][geometry]")
{
    // Zero means "never armed" and is a local sentinel only.  A geometry that
    // arrived carrying it would make "no epoch" and "epoch zero" the same value.
    GeometryBlock g = valid();
    g.generation = 0;
    CHECK(g.validate() == Status::GeometryMismatch);
}

TEST_CASE("rank bounds are enforced", "[core][geometry]")
{
    GeometryBlock g = valid();

    SECTION("above the maximum")
    {
        g.rank = 5;
        CHECK(g.validate() == Status::GeometryMismatch);
    }

    SECTION("shape beyond the rank must be zero")
    {
        g.shape[2] = 1;
        CHECK(g.validate() == Status::GeometryMismatch);
    }

    SECTION("strides beyond the rank must be zero")
    {
        g.strides[2] = 1;
        CHECK(g.validate() == Status::GeometryMismatch);
    }

    SECTION("rank zero with an empty shape is fine")
    {
        g.rank = 0;
        g.shape = {};
        g.strides = {};
        CHECK(g.validate() == Status::Ok);
    }
}

TEST_CASE("element_size bounds are enforced", "[core][geometry]")
{
    GeometryBlock g = valid();

    SECTION("zero")
    {
        g.element_type = ElementType::Unknown;
        g.element_size = 0;
        CHECK(g.validate() == Status::GeometryMismatch);
    }

    SECTION("above 256")
    {
        g.element_type = ElementType::Unknown;
        g.element_size = 257;
        CHECK(g.validate() == Status::GeometryMismatch);
    }

    SECTION("disagreeing with a named element type")
    {
        g.element_size = 4; // UInt16 is 2
        CHECK(g.validate() == Status::GeometryMismatch);
    }

    SECTION("an unnamed element type is rejected")
    {
        g.element_type = static_cast<ElementType>(4242);
        CHECK(g.validate() == Status::GeometryMismatch);
    }

    SECTION("opaque Unknown with a plausible size is allowed")
    {
        // 3.4 constrains element_size only when the type is named, so an opaque
        // description with rank 0 is representable.
        g.element_type = ElementType::Unknown;
        g.element_size = 1;
        g.rank = 0;
        g.shape = {};
        g.strides = {};
        CHECK(g.validate() == Status::Ok);
    }
}

TEST_CASE("frame-size bounds are enforced", "[core][geometry]")
{
    GeometryBlock g = valid();

    SECTION("zero")
    {
        g.max_frame_bytes = 0;
        CHECK(g.validate() == Status::FrameTooLarge);
    }

    SECTION("above the hard cap")
    {
        g.max_frame_bytes = k_max_frame_bytes_hard_cap + 1;
        CHECK(g.validate() == Status::FrameTooLarge);
    }

    SECTION("at the hard cap")
    {
        g.max_frame_bytes = k_max_frame_bytes_hard_cap;
        CHECK(g.validate() == Status::Ok);
    }
}

TEST_CASE("ring depth and credit window bounds are enforced", "[core][geometry]")
{
    GeometryBlock g = valid();

    SECTION("depth below the minimum")
    {
        g.ring_depth = 1;
        g.credit_window = 1;
        CHECK(g.validate() == Status::DepthTooLarge);
    }

    SECTION("depth above the maximum")
    {
        g.ring_depth = k_max_ring_depth + 1;
        CHECK(g.validate() == Status::DepthTooLarge);
    }

    SECTION("credit window of zero")
    {
        g.credit_window = 0;
        CHECK(g.validate() == Status::DepthTooLarge);
    }

    SECTION("credit window wider than the ring")
    {
        g.credit_window = g.ring_depth + 1;
        CHECK(g.validate() == Status::DepthTooLarge);
    }

    SECTION("credit window equal to the ring")
    {
        g.credit_window = g.ring_depth;
        CHECK(g.validate() == Status::Ok);
    }
}

TEST_CASE("shape and stride products are overflow-checked", "[core][geometry]")
{
    GeometryBlock g = valid();

    SECTION("the element product exceeds max_frame_bytes")
    {
        g.shape = {4096, 4096, 0, 0}; // 32 MiB of UInt16, against an 8 MiB grant
        CHECK(g.validate() == Status::GeometryMismatch);
    }

    SECTION("the element product overflows")
    {
        // Without an overflow check these multiply to a small number and sail
        // through the size comparison.
        g.shape = {1ull << 62, 1ull << 62, 0, 0};
        CHECK(g.validate() == Status::GeometryMismatch);
    }

    SECTION("element count times element_size overflows")
    {
        g.rank = 1;
        g.shape = {0x2000'0000'0000'0000ull, 0, 0, 0};
        g.strides = {2, 0, 0, 0};
        CHECK(g.validate() == Status::GeometryMismatch);
    }

    SECTION("a stride extent overflows")
    {
        g.strides = {~std::uint64_t{0}, 2, 0, 0};
        CHECK(g.validate() == Status::GeometryMismatch);
    }

    SECTION("a stride extent exceeds max_frame_bytes")
    {
        g.strides = {1ull << 40, 2, 0, 0};
        CHECK(g.validate() == Status::GeometryMismatch);
    }
}

TEST_CASE("the reachable span is bounded, not the per-axis extent", "[core][geometry]")
{
    // The defect this rule replaced: checking shape[i] * strides[i] <= limit for
    // each axis independently is a different bound, and a strictly weaker one.
    SECTION("a geometry whose last element ends past the payload is refused")
    {
        FrameMetadata meta;
        meta.element_type = ElementType::UInt16;
        meta.element_size = 2;
        meta.rank = 2;
        meta.shape = {2, 2, 0, 0};
        meta.strides = {4, 4, 0, 0};
        meta.payload_bytes = 8;

        // Every per-axis product is 2 * 4 == 8, and elements * element_size is
        // also 8, so the old rule accepted this. The highest-indexed element
        // begins at (2-1)*4 + (2-1)*4 == 8 and ends at 10: an export built from
        // this description reads two bytes past the payload.
        CHECK(meta.validate(8) == Status::GeometryMismatch);

        // One more byte of payload and the same description is sound.
        meta.payload_bytes = 10;
        CHECK(meta.validate(10) == Status::Ok);
    }

    SECTION("a C-contiguous layout fits exactly, with nothing to spare")
    {
        FrameMetadata meta;
        meta.element_type = ElementType::UInt16;
        meta.element_size = 2;
        meta.rank = 2;
        meta.shape = {480, 640, 0, 0};
        meta.strides = {640 * 2, 2, 0, 0};
        meta.payload_bytes = 480 * 640 * 2;

        CHECK(meta.validate(meta.payload_bytes) == Status::Ok);
        CHECK(meta.validate(meta.payload_bytes - 1) == Status::FrameTooLarge);
    }

    SECTION("trailing row padding is not required to fit")
    {
        // A padded layout: 640 pixels in a 700-pixel pitch. The old rule needed
        // room for shape[0] * pitch -- including the padding after the LAST row,
        // which nothing ever reads. The span ends at the last element.
        FrameMetadata meta;
        meta.element_type = ElementType::UInt16;
        meta.element_size = 2;
        meta.rank = 2;
        meta.shape = {480, 640, 0, 0};
        meta.strides = {700 * 2, 2, 0, 0};

        const std::uint64_t span = 479 * 700 * 2 + 639 * 2 + 2;
        meta.payload_bytes = span;

        CHECK(meta.validate(span) == Status::Ok);
        CHECK(span < 480ull * 700 * 2); // strictly less than the old requirement
    }

    SECTION("an empty axis reaches nothing and does not wrap")
    {
        // shape[i] - 1 would underflow to ~0 on a zero extent, so the empty case
        // is answered before the span is computed.
        GeometryBlock g = valid();
        g.shape = {0, 1024, 0, 0};
        g.strides = {2048, 2, 0, 0};
        CHECK(g.validate() == Status::Ok);
    }

    SECTION("the span itself is overflow-checked")
    {
        GeometryBlock g = valid();
        g.rank = 2;
        g.shape = {2, 2, 0, 0};

        // (shape - 1) * stride does not overflow on either axis, but their sum
        // does. A rule that checked each axis alone would never see it.
        g.strides = {1ull << 63, 1ull << 63, 0, 0};
        CHECK(g.validate() == Status::GeometryMismatch);
    }

    SECTION("a rank-0 opaque frame still needs room for one element")
    {
        FrameMetadata meta;
        meta.element_type = ElementType::Byte;
        meta.element_size = 1;
        meta.rank = 0;
        meta.payload_bytes = 4096;
        CHECK(meta.validate(4096) == Status::Ok);
    }
}

TEST_CASE("geometry equality compares every field", "[core][geometry]")
{
    // The epoch interlock compares geometries, so a field left out of the
    // comparison is a field that can change without anyone re-arming.
    const GeometryBlock base = valid();

    CHECK(base == valid());

    const auto differs = [&base](auto mutate)
    {
        GeometryBlock other = base;
        mutate(other);
        return base != other;
    };

    CHECK(differs([](GeometryBlock &g) { g.generation = 2; }));
    CHECK(differs([](GeometryBlock &g) { g.element_type = ElementType::Int16; }));
    CHECK(differs([](GeometryBlock &g) { g.element_size = 4; }));
    CHECK(differs([](GeometryBlock &g) { g.rank = 1; }));
    CHECK(differs([](GeometryBlock &g) { g.max_frame_bytes = 1; }));
    CHECK(differs([](GeometryBlock &g) { g.ring_depth = 64; }));
    CHECK(differs([](GeometryBlock &g) { g.credit_window = 8; }));
    CHECK(differs([](GeometryBlock &g) { g.shape[1] = 7; }));
    CHECK(differs([](GeometryBlock &g) { g.strides[1] = 7; }));
}

// ---------------------------------------------------------------------------
// FrameMetadata
// ---------------------------------------------------------------------------

TEST_CASE("resolve fills in what the producer left out", "[core][geometry]")
{
    FrameMetadata meta;
    meta.element_type = ElementType::Float32;
    meta.rank = 2;
    meta.shape = {480, 640, 0, 0};

    REQUIRE(meta.resolve(8ull << 20) == Status::Ok);

    CHECK(meta.element_size == 4);
    CHECK(meta.payload_bytes == 480ull * 640 * 4);

    // C-contiguous strides in bytes, innermost dimension last.
    CHECK(meta.strides[0] == 640 * 4);
    CHECK(meta.strides[1] == 4);
    CHECK(meta.strides[2] == 0);
    CHECK(meta.strides[3] == 0);

    CHECK(meta.validate(8ull << 20) == Status::Ok);
}

TEST_CASE("resolve leaves explicit values alone", "[core][geometry]")
{
    FrameMetadata meta;
    meta.element_type = ElementType::UInt8;
    meta.element_size = 1;
    meta.rank = 1;
    meta.shape = {100, 0, 0, 0};
    meta.strides = {1, 0, 0, 0};
    meta.payload_bytes = 4096; // a padded buffer, larger than the array needs

    REQUIRE(meta.resolve(8ull << 20) == Status::Ok);

    CHECK(meta.payload_bytes == 4096);
    CHECK(meta.strides[0] == 1);
}

TEST_CASE("resolve leaves the metadata untouched when it fails", "[core][geometry]")
{
    FrameMetadata meta;
    meta.element_type = ElementType::Float64;
    meta.rank = 2;
    meta.shape = {4096, 4096, 0, 0}; // 128 MiB against a 1 MiB ceiling

    const FrameMetadata before = meta;

    CHECK(meta.resolve(1ull << 20) != Status::Ok);

    CHECK(meta.element_size == before.element_size);
    CHECK(meta.payload_bytes == before.payload_bytes);
    CHECK(meta.strides == before.strides);
}

TEST_CASE("a rank without an element type is rejected", "[core][geometry]")
{
    // Stricter than GeometryBlock on purpose: the rank claims the payload is an
    // array of something, and a producer is in a position to say what.
    FrameMetadata meta;
    meta.element_type = ElementType::Unknown;
    meta.element_size = 1;
    meta.rank = 1;
    meta.shape = {16, 0, 0, 0};
    meta.strides = {1, 0, 0, 0};
    meta.payload_bytes = 16;

    CHECK(meta.validate(8ull << 20) == Status::GeometryMismatch);
}

TEST_CASE("an opaque rank-0 frame is accepted", "[core][geometry]")
{
    FrameMetadata meta;
    meta.element_type = ElementType::Byte;
    meta.element_size = 1;
    meta.rank = 0;
    meta.payload_bytes = 65'536;

    CHECK(meta.validate(8ull << 20) == Status::Ok);
}

TEST_CASE("FrameMetadata enforces payload bounds", "[core][geometry]")
{
    FrameMetadata meta;
    meta.element_type = ElementType::Byte;
    meta.element_size = 1;
    meta.rank = 0;

    SECTION("zero payload")
    {
        meta.payload_bytes = 0;
        CHECK(meta.validate(8ull << 20) == Status::GeometryMismatch);
    }

    SECTION("above the caller's ceiling")
    {
        meta.payload_bytes = (8ull << 20) + 1;
        CHECK(meta.validate(8ull << 20) == Status::FrameTooLarge);
    }

    SECTION("above the hard cap")
    {
        meta.payload_bytes = k_max_frame_bytes_hard_cap + 1;
        CHECK(meta.validate(k_max_frame_bytes_hard_cap + 4096) == Status::FrameTooLarge);
    }
}

TEST_CASE("validate does not infer", "[core][geometry]")
{
    // resolve() is where inference happens.  If validate() also inferred, a
    // metadata that was never resolved would pass and the frame would ship with
    // a payload_bytes of zero.
    FrameMetadata meta;
    meta.element_type = ElementType::Float32;
    meta.rank = 1;
    meta.shape = {16, 0, 0, 0};

    CHECK(meta.validate(8ull << 20) != Status::Ok);
}
