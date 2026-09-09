// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <ucx/registered_ring.h>
#include <ucx/ucx_context.h>

#include <core/pinned_ledger.h>

#include <tango-bulk/frame.h>
#include <tango-bulk/errors.h>

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>

using namespace TangoBulk;
using namespace TangoBulk::detail;

TEST_CASE("caller-owned receive rings use the documented packed layout", "[ucx][ring]")
{
    // Deliberately not page-aligned, like the detector frame shape that exposed
    // an out-of-bounds final CUDA slot when the implementation silently rounded
    // each stride to 4 KiB. Keep the fixture small so it needs little memlock.
    constexpr std::uint64_t slot_bytes = 5'000;
    constexpr std::uint32_t depth = 16;
    constexpr std::uint64_t arena_bytes = slot_bytes * depth;

    std::shared_ptr<void> arena(
        ::operator new(static_cast<std::size_t>(arena_bytes)),
        [](void *pointer) { ::operator delete(pointer); });

    UcxContext context("");
    RegisteredRing ring(context,
                        slot_bytes,
                        depth,
                        arena,
                        arena_bytes,
                        MemoryKind::Host);

    CHECK(ring.stride() == slot_bytes);
    CHECK(ring.slot(depth - 1) + slot_bytes ==
          static_cast<std::byte *>(arena.get()) + arena_bytes);
}

TEST_CASE("caller-owned registered memory participates in the pinned budget",
          "[ucx][registered-memory]")
{
    constexpr std::uint64_t budget = 16ull << 20;
    constexpr std::uint64_t first_bytes = budget;
    constexpr std::uint64_t second_bytes = 4096;

    UcxContext context("");
    const std::uint64_t baseline = PinnedLedger::current();
    const std::uint64_t process_limit = baseline + budget;

    auto first_owner = std::shared_ptr<void>(
        ::operator new(static_cast<std::size_t>(first_bytes)),
        [](void *pointer) { ::operator delete(pointer); });
    auto second_owner = std::shared_ptr<void>(
        ::operator new(static_cast<std::size_t>(second_bytes)),
        [](void *pointer) { ::operator delete(pointer); });

    {
        RegisteredMemory first = RegisteredMemory::adopted(
            context, std::move(first_owner), first_bytes, MemoryKind::Host, process_limit);
        CHECK(PinnedLedger::current() == baseline + first_bytes);

        RegisteredMemory moved = std::move(first);
        CHECK_THROWS_AS(RegisteredMemory::adopted(context,
                                                  std::move(second_owner),
                                                  second_bytes,
                                                  MemoryKind::Host,
                                                  process_limit),
                        BulkException);
        CHECK(PinnedLedger::current() == baseline + first_bytes);
    }

    CHECK(PinnedLedger::current() == baseline);

    auto released_owner = std::shared_ptr<void>(
        ::operator new(static_cast<std::size_t>(second_bytes)),
        [](void *pointer) { ::operator delete(pointer); });
    {
        RegisteredMemory released = RegisteredMemory::adopted(
            context, std::move(released_owner), second_bytes, MemoryKind::Host, process_limit);
        CHECK(PinnedLedger::current() == baseline + second_bytes);
    }
    CHECK(PinnedLedger::current() == baseline);
}
