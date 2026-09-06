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

/// The bounded queue a subscription hands frames over on.
///
/// Owned by the subscription, not by the transport that fills it, so it
/// survives a reconnect and a delivered frame never needs its producer alive.
/// A `FrameView` carries everything that keeps its payload valid.
namespace TangoBulk::detail
{

class DeliveryQueue
{
  public:
    struct Stats
    {
        std::size_t depth{0};
        std::size_t capacity{0};
        std::uint64_t taken{0};      ///< handed to a consumer
        std::uint64_t dropped{0};    ///< refused or evicted by the queue policy
        std::uint64_t discarded{0};  ///< let go because a session retired
        std::uint64_t high_water{0};
    };

    /// `capacity` is rounded up to a power of two. `policy` decides what a full
    /// queue does: `DropNewest` refuses the arriving frame, `DropOldest` evicts
    /// the head.
    DeliveryQueue(std::size_t capacity, DropPolicy policy);
    ~DeliveryQueue();

    DeliveryQueue(const DeliveryQueue &) = delete;
    DeliveryQueue &operator=(const DeliveryQueue &) = delete;

    /// Enqueue one frame and wake a waiting consumer. Returns whether it was
    /// queued; a false return has already released the frame and its credit.
    ///
    /// Never blocks, never allocates, never throws. Safe to call from a UCX
    /// progress callback: the wakeup writes a descriptor only when a consumer
    /// is armed, which a consumer that is keeping up never is.
    bool push(FrameView frame) noexcept;

    /// Claim one frame if there is one. Arms and re-checks before reporting
    /// empty, so a caller driving its own loop off `fd()` cannot miss a frame
    /// pushed between the look and the wait.
    bool try_take(FrameView &out) noexcept;

    /// Claim one frame, waiting until `deadline`. False on expiry or on a stop,
    /// leaving the consumer armed either way.
    bool take(FrameView &out, std::chrono::steady_clock::time_point deadline) noexcept;

    /// Stop blocking, permanently and from any thread. Frames already queued
    /// stay claimable; `take()` simply stops waiting for more.
    ///
    /// Sticky because a one-shot wake does not end anything: a woken consumer
    /// that finds nothing blocks again.
    void stop() noexcept;

    /// Drop everything queued, returning each frame's credit. Returns how many.
    ///
    /// For a subscription whose session has ended: the frames were granted
    /// under a contract that no longer holds, and the next session may describe
    /// a different array.
    std::size_t discard() noexcept;

    /// Readable when a frame can be claimed, or -1 if none could be created.
    /// For a caller with its own event loop; pair it with `try_take()`.
    int fd() const noexcept
    {
        return wakeup_fd_;
    }

    Stats stats() const noexcept;

  private:
    void arm() noexcept
    {
        consumer_waiting_.store(true, std::memory_order_release);
    }

    void drain() noexcept;

    /// Block until `deadline`, a frame arrives, or the wait is interrupted.
    bool await(std::chrono::steady_clock::time_point deadline) noexcept;

    BoundedQueue<FrameView> queue_;
    DropPolicy policy_;

    /// eventfd, or -1. Without one `await()` falls back to a short sleep:
    /// a latency and interruptibility loss, not a correctness one.
    int wakeup_fd_{-1};

    /// Set by a consumer before it blocks, cleared by whichever of the two gets
    /// there first. The producer writes the descriptor only on the true-to-false
    /// transition.
    ///
    /// One flag for one consumer. Two competing SPMC readers can lose a wakeup
    /// here -- one reader's arm consumed by the other's exchange. ADR 0008
    /// permits such readers; this does not yet serve them safely.
    std::atomic<bool> consumer_waiting_{false};

    std::atomic<bool> stopped_{false};
    std::atomic<std::uint64_t> taken_{0};
    std::atomic<std::uint64_t> dropped_{0};
    std::atomic<std::uint64_t> discarded_{0};
    std::atomic<std::uint64_t> high_water_{0};
};

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_CORE_DELIVERY_QUEUE_H
