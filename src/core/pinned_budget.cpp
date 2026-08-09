// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <core/pinned_budget.h>

#include <sys/resource.h>

#include <atomic>
#include <cstdio>
#include <mutex>

namespace TangoBulk::detail
{
namespace
{

std::atomic<std::uint64_t> &total() noexcept
{
    static std::atomic<std::uint64_t> value{0};
    return value;
}

} // namespace

bool PinnedBudget::try_reserve(std::uint64_t bytes, std::uint64_t limit) noexcept
{
    std::atomic<std::uint64_t> &counter = total();

    std::uint64_t observed = counter.load(std::memory_order_relaxed);
    for(;;)
    {
        // Overflow-checked, because `bytes` is derived from a granted geometry
        // and a ring depth, and both of those started life on the wire.
        if(bytes > limit || observed > limit - bytes)
        {
            return false;
        }

        if(counter.compare_exchange_weak(observed,
                                         observed + bytes,
                                         std::memory_order_acq_rel,
                                         std::memory_order_relaxed))
        {
            return true;
        }
    }
}

void PinnedBudget::release(std::uint64_t bytes) noexcept
{
    total().fetch_sub(bytes, std::memory_order_acq_rel);
}

std::uint64_t PinnedBudget::current() noexcept
{
    return total().load(std::memory_order_relaxed);
}

void PinnedBudget::check_memlock(std::uint64_t limit) noexcept
{
    static std::once_flag once;

    std::call_once(
        once,
        [](std::uint64_t configured)
        {
            struct rlimit rl
            {
            };

            if(getrlimit(RLIMIT_MEMLOCK, &rl) != 0)
            {
                return;
            }

            if(rl.rlim_cur == RLIM_INFINITY)
            {
                return;
            }

            const auto allowed = static_cast<std::uint64_t>(rl.rlim_cur);
            if(configured > allowed)
            {
                std::fprintf(stderr,
                             "tango-bulk: warning: pinned_memory_limit_bytes is %llu but "
                             "RLIMIT_MEMLOCK is %llu. Registration will fail with an opaque "
                             "UCX error once the ring exceeds the kernel limit; raise "
                             "'ulimit -l' or lower the configured limit.\n",
                             static_cast<unsigned long long>(configured),
                             static_cast<unsigned long long>(allowed));
            }
        },
        limit);
}

} // namespace TangoBulk::detail
