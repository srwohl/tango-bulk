// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_CORE_BOUNDED_QUEUE_H
#define TANGO_BULK_SRC_CORE_BOUNDED_QUEUE_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <new>
#include <utility>
#include <vector>

/// The one queue type behind all three queues in IMPLEMENTATION_SPEC.md 5.3.
///
/// Every queue on the data path has the same requirements: bounded, so a slow
/// consumer cannot grow memory without limit; non-blocking, because 5.1 says an
/// acquisition thread must never block and the engine thread must never wait on
/// an application; and allocation-free after construction, because 2.1 forbids
/// the data path from allocating at all.
///
/// This is Vyukov's bounded MPMC queue.  Each cell carries a sequence counter
/// that says whose turn it is, so a producer and a consumer never contend on the
/// same word, and a failed push or pop is a load and a compare rather than a
/// lock.
///
/// MPMC is not surplus to requirements, which this comment used to claim it was.
/// The delivery queue is not SPSC: `DeliveryQueue::push`'s `DropOldest` branch
/// pops from the producer's thread to make room, so the producer is a second
/// consumer whenever the queue is full -- and ADR 0008 goes further and permits
/// competing SPMC readers on the pull side. One reviewed implementation is worth
/// more than three specialised ones in any case, and the extra cost is a single
/// CAS on an uncontended word.
namespace TangoBulk::detail
{

template <typename T>
class BoundedQueue
{
  public:
    /// `capacity` is rounded up to a power of two, which is what lets the index
    /// arithmetic be a mask instead of a modulo.  The rounded value is what
    /// capacity() reports, so a caller that cares can see what it got.
    explicit BoundedQueue(std::size_t capacity) :
        buffer_(round_up_pow2(capacity)),
        mask_(buffer_.size() - 1)
    {
        for(std::size_t i = 0; i < buffer_.size(); ++i)
        {
            buffer_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    BoundedQueue(const BoundedQueue &) = delete;
    BoundedQueue &operator=(const BoundedQueue &) = delete;

    /// Returns false when full.  Never blocks, never allocates, never throws.
    /// On false, `item` is untouched -- which is what lets publish() hand the
    /// lease back to the caller on QueueFull rather than dropping it.
    bool try_push(T &&item) noexcept
    {
        Cell *cell = nullptr;
        std::size_t pos = enqueue_pos_.load(std::memory_order_relaxed);

        for(;;)
        {
            cell = &buffer_[pos & mask_];
            const std::size_t seq = cell->sequence.load(std::memory_order_acquire);
            const std::intptr_t diff =
                static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos);

            if(diff == 0)
            {
                if(enqueue_pos_.compare_exchange_weak(
                       pos, pos + 1, std::memory_order_relaxed))
                {
                    break;
                }
            }
            else if(diff < 0)
            {
                return false; // full
            }
            else
            {
                pos = enqueue_pos_.load(std::memory_order_relaxed);
            }
        }

        cell->data = std::move(item);
        cell->sequence.store(pos + 1, std::memory_order_release);
        size_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    /// Returns false when empty.  Never blocks, never allocates, never throws.
    bool try_pop(T &out) noexcept
    {
        Cell *cell = nullptr;
        std::size_t pos = dequeue_pos_.load(std::memory_order_relaxed);

        for(;;)
        {
            cell = &buffer_[pos & mask_];
            const std::size_t seq = cell->sequence.load(std::memory_order_acquire);
            const std::intptr_t diff =
                static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos + 1);

            if(diff == 0)
            {
                if(dequeue_pos_.compare_exchange_weak(
                       pos, pos + 1, std::memory_order_relaxed))
                {
                    break;
                }
            }
            else if(diff < 0)
            {
                return false; // empty
            }
            else
            {
                pos = dequeue_pos_.load(std::memory_order_relaxed);
            }
        }

        out = std::move(cell->data);
        cell->data = T{};
        cell->sequence.store(pos + mask_ + 1, std::memory_order_release);
        size_.fetch_sub(1, std::memory_order_relaxed);
        return true;
    }

    /// A gauge, not a synchronisation primitive: by the time a caller reads it,
    /// another thread may have pushed or popped.  It exists because 2.5 asks for
    /// `publish_queue_depth` and `delivery_queue_depth` as counters.
    std::size_t size() const noexcept
    {
        return size_.load(std::memory_order_relaxed);
    }

    bool empty() const noexcept
    {
        return size() == 0;
    }

    std::size_t capacity() const noexcept
    {
        return buffer_.size();
    }

  private:
    struct Cell
    {
        std::atomic<std::size_t> sequence{0};
        T data{};
    };

    static std::size_t round_up_pow2(std::size_t n) noexcept
    {
        std::size_t p = 1;
        while(p < n)
        {
            p <<= 1;
        }
        return p;
    }

    std::vector<Cell> buffer_;
    std::size_t mask_;

    // Producer and consumer cursors are deliberately not adjacent.  They are
    // written by different threads on every operation, and sharing a cache line
    // would turn an uncontended queue into a ping-pong.
    alignas(64) std::atomic<std::size_t> enqueue_pos_{0};
    alignas(64) std::atomic<std::size_t> dequeue_pos_{0};
    alignas(64) std::atomic<std::size_t> size_{0};
};

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_CORE_BOUNDED_QUEUE_H
