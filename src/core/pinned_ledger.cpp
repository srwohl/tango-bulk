// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <core/pinned_ledger.h>

#include <sys/resource.h>

#include <atomic>
#include <cstdio>
#include <limits>
#include <mutex>
#include <set>

namespace TangoBulk::detail
{
namespace
{

std::atomic<std::uint64_t> &total() noexcept
{
    static std::atomic<std::uint64_t> value{0};
    return value;
}

std::mutex &warning_mutex() noexcept
{
    static std::mutex mutex;
    return mutex;
}

std::set<std::uint64_t> &warned_limits() noexcept
{
    static std::set<std::uint64_t> limits;
    return limits;
}

} // namespace

bool PinnedLedger::checked_bytes(std::uint64_t frame_bytes,
                                 std::uint32_t ring_depth,
                                 std::uint64_t &result) noexcept
{
    if(ring_depth != 0 && frame_bytes > std::numeric_limits<std::uint64_t>::max() / ring_depth)
    {
        return false;
    }

    result = frame_bytes * ring_depth;
    return true;
}

bool PinnedLedger::try_reserve(std::uint64_t bytes, std::uint64_t process_limit) noexcept
{
    check_memlock(process_limit);

    std::atomic<std::uint64_t> &counter = total();
    std::uint64_t observed = counter.load(std::memory_order_relaxed);

    for(;;)
    {
        // Check before subtracting so neither the request nor the addition can
        // wrap. A failed admission is all-or-nothing.
        if(bytes > process_limit || observed > process_limit - bytes)
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

void PinnedLedger::release(std::uint64_t bytes) noexcept
{
    if(bytes == 0)
    {
        return;
    }

    // A valid Reservation can never release more than the live total. Keep
    // this raw hook fail-safe as well, so a stale adapter bug cannot turn the
    // process gauge into UINT64_MAX.
    std::atomic<std::uint64_t> &counter = total();
    std::uint64_t observed = counter.load(std::memory_order_relaxed);
    for(;;)
    {
        const std::uint64_t next = observed < bytes ? 0 : observed - bytes;
        if(counter.compare_exchange_weak(observed,
                                         next,
                                         std::memory_order_acq_rel,
                                         std::memory_order_relaxed))
        {
            return;
        }
    }
}

std::uint64_t PinnedLedger::current() noexcept
{
    return total().load(std::memory_order_relaxed);
}

void PinnedLedger::check_memlock(std::uint64_t process_limit) noexcept
{
    struct rlimit limits
    {
    };

    if(getrlimit(RLIMIT_MEMLOCK, &limits) != 0 || limits.rlim_cur == RLIM_INFINITY)
    {
        return;
    }

    const auto allowed = static_cast<std::uint64_t>(limits.rlim_cur);
    if(process_limit <= allowed)
    {
        return;
    }

    try
    {
        std::lock_guard<std::mutex> lock(warning_mutex());
        if(!warned_limits().insert(process_limit).second)
        {
            return;
        }
    }
    catch(...)
    {
        // Diagnostics must never turn an otherwise valid reservation into an
        // exception, especially while a failure path is rolling back.
    }

    std::fprintf(stderr,
                 "tango-bulk: warning: pinned_memory_limit_bytes is %llu but "
                 "RLIMIT_MEMLOCK is %llu. Registration may fail once the process "
                 "exceeds the kernel limit; raise 'ulimit -l' or lower the "
                 "configured limit.\n",
                 static_cast<unsigned long long>(process_limit),
                 static_cast<unsigned long long>(allowed));
}

PinnedLedger::Reservation::~Reservation() noexcept
{
    reset();
}

PinnedLedger::Reservation::Reservation(Reservation &&other) noexcept : bytes_(other.bytes_)
{
    other.bytes_ = 0;
}

PinnedLedger::Reservation &PinnedLedger::Reservation::operator=(Reservation &&other) noexcept
{
    if(this != &other)
    {
        reset();
        bytes_ = other.bytes_;
        other.bytes_ = 0;
    }
    return *this;
}

PinnedLedger::Reservation PinnedLedger::Reservation::try_acquire(
    std::uint64_t bytes,
    std::uint64_t process_limit) noexcept
{
    if(!PinnedLedger::try_reserve(bytes, process_limit))
    {
        return {};
    }

    // Zero-byte admission is a successful no-op at the low-level API, but it
    // remains an empty RAII value so it cannot accidentally release another
    // reservation.
    return Reservation(bytes);
}

bool PinnedLedger::Reservation::extend(std::uint64_t bytes,
                                       std::uint64_t process_limit) noexcept
{
    if(bytes == 0)
    {
        return true;
    }

    if(bytes_ > std::numeric_limits<std::uint64_t>::max() - bytes)
    {
        return false;
    }

    if(!PinnedLedger::try_reserve(bytes, process_limit))
    {
        return false;
    }

    bytes_ += bytes;
    return true;
}

void PinnedLedger::Reservation::reset() noexcept
{
    if(bytes_ != 0)
    {
        PinnedLedger::release(bytes_);
        bytes_ = 0;
    }
}

} // namespace TangoBulk::detail
