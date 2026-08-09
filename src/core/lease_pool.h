// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_CORE_LEASE_POOL_H
#define TANGO_BULK_SRC_CORE_LEASE_POOL_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

/// Fixed-size block pool for `shared_ptr` control blocks on the receive path.
///
/// IMPLEMENTATION_SPEC.md 5.5 makes the `shared_ptr<ReceiveSlotLease>` refcount
/// the credit interlock, and 2.1 forbids the data path from allocating.  Taken
/// together those would be contradictory -- a control block is an allocation,
/// and one is needed per delivered frame -- were it not for the fact that the
/// number of live leases is bounded by construction: a slot cannot be reused
/// until its credit returns, so at most `ring_depth` leases exist at once.
///
/// So the control blocks come from here, `ring_depth` of them plus slack,
/// reserved once at subscriber construction.  `std::allocate_shared` puts the
/// lease and its control block in one of these blocks and the malloc disappears
/// from the frame path.
///
/// Threading: `allocate()` is called only by the engine thread, which is the
/// only thing that hands out views.  `deallocate()` is called by whichever
/// thread destroys the last `FrameView`, which is any thread at all.  That
/// asymmetry is what makes the free list safe as a plain lock-free stack: ABA on
/// pop needs two concurrent poppers, and there is only ever one.
namespace TangoBulk::detail
{

class LeasePool
{
  public:
    /// Generous enough for a `_Sp_counted_ptr_inplace` around a two-word lease
    /// on every libstdc++/libc++ layout we build against.  A request that does
    /// not fit is not a corruption risk -- it falls back to the global allocator
    /// and is counted -- but it would mean this reasoning had gone stale, which
    /// is why `overflows()` is visible rather than silent.
    static constexpr std::size_t k_block_bytes = 128;

    explicit LeasePool(std::size_t block_count);

    LeasePool(const LeasePool &) = delete;
    LeasePool &operator=(const LeasePool &) = delete;

    /// Engine thread only.  Returns nullptr when the block is too large or the
    /// pool is exhausted; the caller falls back to the global allocator.
    void *allocate(std::size_t bytes) noexcept;

    /// Any thread.  `p` must have come from allocate() on this pool.
    void deallocate(void *p) noexcept;

    bool owns(const void *p) const noexcept;

    /// Requests that did not fit or found the pool empty.  Expected to stay at
    /// zero; a nonzero value means the "at most ring_depth live leases"
    /// invariant is not holding, which is worth knowing.
    std::uint64_t overflows() const noexcept
    {
        return overflows_.load(std::memory_order_relaxed);
    }

    std::size_t block_count() const noexcept
    {
        return block_count_;
    }

  private:
    struct alignas(alignof(std::max_align_t)) Block
    {
        unsigned char bytes[k_block_bytes];
    };

    std::vector<Block> storage_;
    std::size_t block_count_;
    std::atomic<void *> free_head_{nullptr};
    std::atomic<std::uint64_t> overflows_{0};
};

/// Allocator adapter so `std::allocate_shared` can be pointed at a LeasePool.
///
/// Deliberately minimal: it is used for exactly one type, in exactly one place,
/// and everything beyond what allocate_shared touches is left out rather than
/// written speculatively.
///
/// It owns the pool by `shared_ptr`, and that is not a convenience.
/// `allocate_shared` stores a copy of this allocator *inside* the control block
/// it allocates, so the pool is the storage its own owner lives in.  On the
/// receive path the lease additionally holds the arena alive, and the arena is
/// what would otherwise own the pool -- so releasing the last `FrameView` ran
/// `_M_dispose()`, which destroyed the lease, which released the arena, which
/// destroyed the pool, and then `_M_release()` carried on reading the control
/// block it had just pulled the ground out from under.  ASan caught it as a
/// heap-use-after-free in `_M_release()`.
///
/// Holding the pool here breaks that: libstdc++ copies the allocator to the
/// stack before destroying the control block and deallocates through the copy,
/// so the pool necessarily outlives the last block returned to it.
template <typename T>
class PoolAllocator
{
  public:
    using value_type = T;

    explicit PoolAllocator(std::shared_ptr<LeasePool> pool) noexcept :
        pool_(std::move(pool))
    {
    }

    template <typename U>
    PoolAllocator(const PoolAllocator<U> &other) noexcept :
        pool_(other.pool())
    {
    }

    T *allocate(std::size_t n)
    {
        if(pool_ != nullptr && n == 1)
        {
            if(void *p = pool_->allocate(sizeof(T)))
            {
                return static_cast<T *>(p);
            }
        }
        return static_cast<T *>(::operator new(n * sizeof(T)));
    }

    void deallocate(T *p, std::size_t) noexcept
    {
        if(pool_ != nullptr && pool_->owns(p))
        {
            pool_->deallocate(p);
            return;
        }
        ::operator delete(p);
    }

    const std::shared_ptr<LeasePool> &pool() const noexcept
    {
        return pool_;
    }

    template <typename U>
    bool operator==(const PoolAllocator<U> &other) const noexcept
    {
        return pool_ == other.pool();
    }

    template <typename U>
    bool operator!=(const PoolAllocator<U> &other) const noexcept
    {
        return !(*this == other);
    }

  private:
    std::shared_ptr<LeasePool> pool_;
};

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_CORE_LEASE_POOL_H
