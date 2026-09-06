// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_UCX_PRODUCER_SLOTS_H
#define TANGO_BULK_SRC_UCX_PRODUCER_SLOTS_H

#include <ucx/registered_ring.h>
#include <ucx/ucx_context.h>

#include <core/bounded_queue.h>
#include <tango-bulk/counters.h>

#include <atomic>
#include <cassert>
#include <cstdint>
#include <memory>

namespace TangoBulk::detail
{

/// Everything an unpublished SlotHandle must keep alive: the registered ring,
/// the context needed to unmap it, and the free list a released slot goes back
/// to.
///
/// Owned by `shared_ptr` and shared with every outstanding handle, so a handle
/// that outlives its publisher keeps its storage mapped and has somewhere safe
/// to return the slot (ADR 0003). The publisher used to hold the ring by value
/// and the handle a raw pointer into it, which made that a use-after-free.
class ProducerSlots
{
  public:
    ProducerSlots(std::shared_ptr<UcxContext> context,
                  std::uint64_t slot_bytes,
                  std::uint32_t depth,
                  bool pad_stride,
                  std::uint64_t pinned_limit) :
        context_(std::move(context)),
        ring_(*context_, slot_bytes, depth, pad_stride, pinned_limit),
        free_slots_(depth)
    {
        for(std::uint32_t i = 0; i < depth; ++i)
        {
            std::uint32_t index = i;
            free_slots_.try_push(std::move(index));
        }
    }

    /// Take a slot, or fail. Never blocks and never allocates.
    bool try_acquire(std::uint32_t &index) noexcept
    {
        if(!free_slots_.try_pop(index))
        {
            return false;
        }

        retained_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    /// Return a slot to the free list.
    ///
    /// Three callers, and the rule is about when each may do it rather than
    /// about doing anything different: an unpublished handle being destroyed, a
    /// publish() that dropped the frame, and the engine dropping the last
    /// session reference.
    void release(std::size_t index) noexcept
    {
        auto value = static_cast<std::uint32_t>(index);
        const bool pushed = free_slots_.try_push(std::move(value));

        // The free list is exactly ring_depth deep and a slot is in it or held,
        // never both. A failure here means a double release, which would go on
        // to corrupt a frame in flight.
        (void) pushed;
        assert(pushed);
        retained_.fetch_sub(1, std::memory_order_relaxed);
    }

    RegisteredRing &ring() noexcept
    {
        return ring_;
    }

    const RegisteredRing &ring() const noexcept
    {
        return ring_;
    }

    std::uint64_t retained() const noexcept
    {
        return retained_.load(std::memory_order_relaxed);
    }

  private:
    std::shared_ptr<UcxContext> context_;
    RegisteredRing ring_;
    BoundedQueue<std::uint32_t> free_slots_;

    /// Slots not in the free list. The free list already knows this, but it
    /// cannot say so without popping, and the counter is read by an observer.
    std::atomic<std::uint64_t> retained_{0};
};

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_UCX_PRODUCER_SLOTS_H
