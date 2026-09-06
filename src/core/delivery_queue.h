// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_CORE_DELIVERY_QUEUE_H
#define TANGO_BULK_SRC_CORE_DELIVERY_QUEUE_H

#include <core/bounded_queue.h>

#include <tango-bulk/frame.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>

/// The queue a subscription hands frames over on, and the readiness that goes
/// with it.
///
/// One bounded queue, a queue policy for when it is full, and one descriptor a
/// waiting consumer can be woken on.  All three used to be members of
/// `SubscriberEngine`, spread across `commit()`, `poll()`, `await_frame()`,
/// `signal_consumer()` and `drain_wakeup()`, which had two consequences worth
/// naming.  The queue could not be tested without a NIC, and it could not be
/// *moved* -- and moving it is the point: CONTEXT.md says the local delivery
/// queue belongs to a Subscription, while the code has it belonging to a
/// transport that a reconnect throws away.
///
/// Nothing here knows about UCX, sessions or leases.  A `FrameView` already
/// carries everything that keeps its payload alive, so a queued frame does not
/// need the transport that produced it to still exist -- which is exactly what
/// makes ADR 0003's retention property expressible.
namespace TangoBulk::detail
{

class DeliveryQueue
{
  public:
    /// `capacity` is rounded up to a power of two by the queue underneath.
    ///
    /// `policy` decides what a full queue does: `DropNewest` refuses the
    /// arriving frame, `DropOldest` evicts the head to make room.  Both return
    /// the dropped frame's credit immediately, because dropping a `FrameView`
    /// *is* returning its credit.
    DeliveryQueue(std::size_t capacity, DropPolicy policy);
    ~DeliveryQueue();

    DeliveryQueue(const DeliveryQueue &) = delete;
    DeliveryQueue &operator=(const DeliveryQueue &) = delete;

    // -- producer ------------------------------------------------------------

    /// Enqueue one frame, applying the queue policy if there is no room.
    /// Returns whether it was queued; a false return has already released the
    /// frame and its credit.
    ///
    /// Never blocks, never allocates, never throws, and makes no syscall --
    /// which is what lets it be called from inside `ucp_worker_progress`.
    /// Waking a consumer is deliberately NOT part of it; see `notify()`.
    bool push(FrameView frame) noexcept;

    /// Wake a consumer that has declared itself about to block.
    ///
    /// Separate from `push()` because of where each one is called from: a
    /// transport receives frames inside its progress callback, and this writes
    /// a descriptor.  The engine already routes credit and probe acks out of
    /// the callback and into its loop for that reason, and notification is the
    /// third thing on that list, not an exception to it.
    ///
    /// Writes only when a consumer is armed and there is something to take, so
    /// a consumer that is keeping up costs zero syscalls -- the property that
    /// makes a descriptor affordable on the frame path at all.
    void notify() noexcept;

    // -- consumer ------------------------------------------------------------

    /// Claim one frame if there is one, without waiting.
    ///
    /// On finding nothing it arms and re-checks internally before reporting
    /// empty, so a caller driving its own event loop off `fd()` cannot lose a
    /// frame pushed between the look and the wait.  The arm/re-check/drain
    /// protocol is this module's, never a caller's (ADR 0005).
    bool try_take(FrameView &out) noexcept;

    /// Claim one frame, waiting until `deadline`.  Returns false on expiry, on
    /// a stop, and immediately for a deadline already past -- in every case
    /// leaving the consumer armed, exactly as `try_take()` does.
    bool take(FrameView &out, std::chrono::steady_clock::time_point deadline) noexcept;

    /// A descriptor that becomes readable when a frame can be claimed, or -1
    /// if one could not be created.
    ///
    /// For a caller that owns its event loop -- `epoll`, `select`,
    /// `loop.add_reader()`.  Pair it with `try_take()`, which does the arming;
    /// adding it to an event loop and never calling `try_take()` produces
    /// something that fires once and then never again.
    int fd() const noexcept
    {
        return wakeup_fd_;
    }

    /// Stop blocking, permanently.
    ///
    /// The terminal wakeup: close, interruption and teardown all need a blocked
    /// consumer to come back, and none of them has a frame to hand it. Sticky
    /// rather than a one-shot nudge, because a nudge is not what any of those
    /// three want -- a woken consumer that finds nothing simply blocks again,
    /// which is what teardown was trying to stop. Section 7.6 settles this the
    /// same way: interruption is a sticky local stop, not an event.
    ///
    /// Frames already queued are still claimable afterwards; `take()` pops
    /// before it consults this, and stops blocking only once the queue is
    /// empty. What to do with the remainder is the owner's decision, not the
    /// queue's: close discards it, interruption keeps it (section 7.10).
    ///
    /// Idempotent, constant-time, nonthrowing, and safe from any thread.
    void stop() noexcept;

    bool stopped() const noexcept
    {
        return stopped_.load(std::memory_order_acquire);
    }

    // -- observation ---------------------------------------------------------

    std::size_t size() const noexcept
    {
        return queue_.size();
    }

    std::size_t capacity() const noexcept
    {
        return queue_.capacity();
    }

    /// Frames handed to a consumer, frames the queue policy discarded, and the
    /// deepest the queue has ever been.
    std::uint64_t taken() const noexcept
    {
        return taken_.load(std::memory_order_relaxed);
    }

    std::uint64_t dropped() const noexcept
    {
        return dropped_.load(std::memory_order_relaxed);
    }

    std::uint64_t high_water() const noexcept
    {
        return high_water_.load(std::memory_order_relaxed);
    }

  private:
    /// Declare the intent to block.  Idempotent, and must be followed by a
    /// re-check: that is what closes the lost-wakeup race.
    void arm() noexcept
    {
        consumer_waiting_.store(true, std::memory_order_release);
    }

    /// Consume a pending signal so it does not persist.
    void drain() noexcept;

    /// Block until `deadline`, a frame arrives, or the wait is interrupted.
    /// Returns whether it is worth looking at the queue again.
    bool await(std::chrono::steady_clock::time_point deadline) noexcept;

    BoundedQueue<FrameView> queue_;
    DropPolicy policy_;

    /// eventfd, or -1 if it could not be created.
    ///
    /// A queue without one still works: `await()` falls back to a short sleep.
    /// Losing the wakeup path costs latency and interruptibility, not
    /// correctness, so it must not stop a subscription from opening.
    int wakeup_fd_{-1};

    /// Set by a consumer immediately before it blocks, cleared by whichever of
    /// the two gets there first.  The producer writes the descriptor only on
    /// the true-to-false transition, which is what makes the common case free.
    ///
    /// One flag for one consumer.  ADR 0008 permits competing SPMC readers, and
    /// two of them sharing this flag can lose a wakeup -- one reader's arm
    /// consumed by the other's exchange.  That is a live defect
    /// (docs/ARCHITECTURE_SIMPLIFICATION.md section 4.2, last bullet) carried
    /// here unchanged rather than fixed in passing: it needs its own stress
    /// test, and it is the one thing in this module that is not a move.
    std::atomic<bool> consumer_waiting_{false};

    /// Set once by `stop()` and never cleared: a subscription that has been
    /// torn down does not come back, and neither does an interrupted one.
    std::atomic<bool> stopped_{false};

    std::atomic<std::uint64_t> taken_{0};
    std::atomic<std::uint64_t> dropped_{0};
    std::atomic<std::uint64_t> high_water_{0};
};

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_CORE_DELIVERY_QUEUE_H
