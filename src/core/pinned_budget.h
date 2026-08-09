// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_CORE_PINNED_BUDGET_H
#define TANGO_BULK_SRC_CORE_PINNED_BUDGET_H

#include <cstdint>

/// Process-wide accounting for pinned (registered) memory.
///
/// IMPLEMENTATION_SPEC.md 6.1: before any `ucp_mem_map`, the requested bytes are
/// added to a process-wide total and the request is rejected with
/// ResourceExhausted if the total would exceed the configured limit.  Process-
/// wide rather than per-publisher because RLIMIT_MEMLOCK is per-process: two
/// publishers each inside their own budget can still take the process over the
/// kernel's.
///
/// The reservation happens *before* the mapping, not after, so a rejection costs
/// nothing and never leaves a partially registered ring behind.
namespace TangoBulk::detail
{

class PinnedBudget
{
  public:
    /// Atomically reserve `bytes` if doing so keeps the process total at or
    /// below `limit`.  All-or-nothing: on false, nothing was reserved.
    static bool try_reserve(std::uint64_t bytes, std::uint64_t limit) noexcept;

    static void release(std::uint64_t bytes) noexcept;

    static std::uint64_t current() noexcept;

    /// Warn once per process if `limit` exceeds RLIMIT_MEMLOCK.
    ///
    /// Worth its own call because `ucp_mem_map` failing for want of a memlock
    /// allowance returns a UCX status that does not name the cause, and the
    /// resulting bug report is "UCX said UCS_ERR_IO_ERROR".  Saying it up front,
    /// at configuration time, is the difference between a five-minute fix and an
    /// afternoon.
    static void check_memlock(std::uint64_t limit) noexcept;
};

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_CORE_PINNED_BUDGET_H
