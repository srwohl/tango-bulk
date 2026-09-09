// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <core/pinned_ledger.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <limits>
#include <utility>

using namespace TangoBulk::detail;

TEST_CASE("pinned sizing rejects multiplication overflow", "[core][pinned]")
{
    std::uint64_t bytes = 17;
    CHECK(PinnedLedger::checked_bytes(8, 4, bytes));
    CHECK(bytes == 32);

    CHECK_FALSE(PinnedLedger::checked_bytes(std::numeric_limits<std::uint64_t>::max(), 2, bytes));
    CHECK(bytes == 32);
}

TEST_CASE("pinned reservations are process-wide, move-only, and rollback-safe",
          "[core][pinned]")
{
    constexpr std::uint64_t process_limit = 64;
    const std::uint64_t baseline = PinnedLedger::current();

    {
        auto first = PinnedLedger::Reservation::try_acquire(32, baseline + process_limit);
        REQUIRE(first);
        CHECK(first.bytes() == 32);
        CHECK(PinnedLedger::current() == baseline + 32);

        auto second = PinnedLedger::Reservation::try_acquire(33, baseline + process_limit);
        CHECK_FALSE(second);
        CHECK(PinnedLedger::current() == baseline + 32);

        auto moved = std::move(first);
        CHECK_FALSE(first);
        CHECK(moved.bytes() == 32);

        auto replacement = PinnedLedger::Reservation::try_acquire(16, baseline + process_limit);
        REQUIRE(replacement);
        CHECK(PinnedLedger::current() == baseline + 48);

        moved = std::move(replacement);
        CHECK_FALSE(replacement);
        CHECK(moved.bytes() == 16);
        CHECK(PinnedLedger::current() == baseline + 16);

        moved.reset();
        CHECK(PinnedLedger::current() == baseline);

        auto extendable = PinnedLedger::Reservation::try_acquire(8, baseline + process_limit);
        REQUIRE(extendable);
        CHECK(extendable.extend(8, baseline + process_limit));
        CHECK(extendable.bytes() == 16);
        CHECK(PinnedLedger::current() == baseline + 16);
        CHECK_FALSE(extendable.extend(49, baseline + process_limit));
        CHECK(extendable.bytes() == 16);
        CHECK(PinnedLedger::current() == baseline + 16);
    }

    CHECK(PinnedLedger::current() == baseline);
}

TEST_CASE("a second owner cannot use a different process limit to overcommit",
          "[core][pinned]")
{
    const std::uint64_t baseline = PinnedLedger::current();

    {
        auto owner = PinnedLedger::Reservation::try_acquire(32, baseline + 64);
        REQUIRE(owner);

        auto other_owner = PinnedLedger::Reservation::try_acquire(1, baseline + 16);
        CHECK_FALSE(other_owner);
        CHECK(PinnedLedger::current() == baseline + 32);
    }

    CHECK(PinnedLedger::current() == baseline);
}
