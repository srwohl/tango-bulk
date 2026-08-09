// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <core/lease_pool.h>

#include <cstring>

namespace TangoBulk::detail
{

LeasePool::LeasePool(std::size_t block_count) :
    storage_(block_count),
    block_count_(block_count)
{
    // Thread the free list through the blocks themselves, newest first.  Built
    // once here, so the frame path only ever pops and pushes.
    void *head = nullptr;
    for(std::size_t i = 0; i < block_count_; ++i)
    {
        void *block = static_cast<void *>(storage_[i].bytes);
        std::memcpy(block, &head, sizeof(void *));
        head = block;
    }
    free_head_.store(head, std::memory_order_relaxed);
}

void *LeasePool::allocate(std::size_t bytes) noexcept
{
    if(bytes > k_block_bytes)
    {
        overflows_.fetch_add(1, std::memory_order_relaxed);
        return nullptr;
    }

    // Single-popper stack: only the engine thread calls this, so the classic ABA
    // hazard -- another popper recycling the head between our load and our CAS
    // -- cannot arise.  Pushers only ever make the list longer.
    void *head = free_head_.load(std::memory_order_acquire);
    while(head != nullptr)
    {
        void *next = nullptr;
        std::memcpy(&next, head, sizeof(void *));

        if(free_head_.compare_exchange_weak(
               head, next, std::memory_order_acq_rel, std::memory_order_acquire))
        {
            return head;
        }
    }

    overflows_.fetch_add(1, std::memory_order_relaxed);
    return nullptr;
}

void LeasePool::deallocate(void *p) noexcept
{
    if(p == nullptr)
    {
        return;
    }

    void *head = free_head_.load(std::memory_order_relaxed);
    for(;;)
    {
        std::memcpy(p, &head, sizeof(void *));
        if(free_head_.compare_exchange_weak(
               head, p, std::memory_order_release, std::memory_order_relaxed))
        {
            return;
        }
    }
}

bool LeasePool::owns(const void *p) const noexcept
{
    if(storage_.empty() || p == nullptr)
    {
        return false;
    }

    const auto *first = reinterpret_cast<const unsigned char *>(storage_.data());
    const unsigned char *last = first + storage_.size() * sizeof(Block);
    const auto *q = static_cast<const unsigned char *>(p);

    return q >= first && q < last;
}

} // namespace TangoBulk::detail
