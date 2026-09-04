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

TEST_CASE("the session-state projection is total and lossy in one direction",
          "[core][enums]")
{
    // Total: every SubscriberState maps, including a value that never came from
    // this library. A projection with a hole in it would report NoSession for a
    // live session, which is the one wrong answer that matters.
    for(std::uint32_t raw = 0; raw < 8; ++raw)
    {
        const SubscriptionState projected = to_subscription_state(static_cast<SubscriberState>(raw));
        CHECK(std::strlen(to_string(projected)) > 0);
    }

    CHECK(to_subscription_state(SubscriberState::Active) == SubscriptionState::Active);

    // A grant is held or being obtained.  Probing counts: the publisher has
    // already allocated for this client.
    CHECK(to_subscription_state(SubscriberState::Opening) == SubscriptionState::Opening);
    CHECK(to_subscription_state(SubscriberState::Probing) == SubscriptionState::Opening);
    CHECK(to_subscription_state(SubscriberState::Reconnecting) == SubscriptionState::Opening);

    // The documented loss: an orderly shutdown and a subscriber that gave up
    // are indistinguishable once projected.  Asserted rather than merely
    // written down, so that narrowing it later has to be a deliberate edit.
    CHECK(to_subscription_state(SubscriberState::Closed) == SubscriptionState::NoSession);
    CHECK(to_subscription_state(SubscriberState::Failed) == SubscriptionState::NoSession);
    CHECK(to_subscription_state(SubscriberState::Closed) ==
          to_subscription_state(SubscriberState::Failed));
}
