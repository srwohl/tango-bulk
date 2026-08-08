// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/frame.h>
#include <tango-bulk/publisher.h>
#include <tango-bulk/subscriber.h>

#include <catch2/catch_test_macros.hpp>

#include <cstring>

using namespace TangoBulk;

TEST_CASE("element_size_of covers every declared element type", "[core][enums]")
{
    CHECK(element_size_of(ElementType::Unknown) == 0);
    CHECK(element_size_of(ElementType::UInt8) == 1);
    CHECK(element_size_of(ElementType::Int8) == 1);
    CHECK(element_size_of(ElementType::Byte) == 1);
    CHECK(element_size_of(ElementType::UInt16) == 2);
    CHECK(element_size_of(ElementType::Int16) == 2);
    CHECK(element_size_of(ElementType::UInt32) == 4);
    CHECK(element_size_of(ElementType::Int32) == 4);
    CHECK(element_size_of(ElementType::Float32) == 4);
    CHECK(element_size_of(ElementType::UInt64) == 8);
    CHECK(element_size_of(ElementType::Int64) == 8);
    CHECK(element_size_of(ElementType::Float64) == 8);
}

TEST_CASE("wire enum values are pinned for protocol major 1", "[core][enums]")
{
    // These are on the wire.  A renumbering is a protocol break, so the test
    // exists to make that break visible here rather than in an integration run
    // against a peer built from a different commit.
    CHECK(static_cast<std::uint32_t>(ElementType::Unknown) == 0);
    CHECK(static_cast<std::uint32_t>(ElementType::Byte) == 11);
    CHECK(static_cast<std::uint32_t>(MemoryKind::Host) == 0);
    CHECK(static_cast<std::uint32_t>(MemoryKind::Cuda) == 1);
    CHECK(static_cast<std::uint32_t>(MemoryKind::Rocm) == 2);
    CHECK(static_cast<std::uint32_t>(Endian::Little) == 0);
    CHECK(static_cast<std::uint32_t>(Endian::Big) == 1);

    CHECK(static_cast<std::uint16_t>(Status::Ok) == 0);
    CHECK(static_cast<std::uint16_t>(Status::Internal) == 15);
}

TEST_CASE("to_string never returns null", "[core][enums]")
{
    // The logging path must not be the thing that crashes, including for a
    // value that arrived off the wire and was never validated.
    for(std::uint16_t raw = 0; raw < 32; ++raw)
    {
        const char *text = to_string(static_cast<Status>(raw));
        REQUIRE(text != nullptr);
        CHECK(std::strlen(text) > 0);
    }

    CHECK(std::strcmp(to_string(SubscriberState::Active), "Active") == 0);
    CHECK(std::strcmp(to_string(PublishResult::CreditStalled), "CreditStalled") == 0);
    CHECK(std::strcmp(to_string(ElementType::Float32), "Float32") == 0);
    CHECK(std::strcmp(to_string(MemoryKind::Host), "Host") == 0);
}
