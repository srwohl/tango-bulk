// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/limits.h>
#include <tango-bulk/subscriber.h>

#include <catch2/catch_test_macros.hpp>

using namespace TangoBulk;

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
