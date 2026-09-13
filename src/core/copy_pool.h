// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_CORE_COPY_POOL_H
#define TANGO_BULK_SRC_CORE_COPY_POOL_H

#include <core/bounded_queue.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace TangoBulk::detail
{

/// Payload buffers for copied delivery.
///
/// `count` buffers of `buffer_bytes` are allocated once, up front. `acquire()`
/// hands one out, or a heap buffer when none is free, and counts the fallback.
/// The returned pointer's deleter gives the buffer back from any thread
/// without blocking, allocating or throwing, and keeps the pool alive, so a
/// buffer may outlive the transport that filled it.
class CopyPool : public std::enable_shared_from_this<CopyPool>
{
  public:
    CopyPool(std::size_t count, std::size_t buffer_bytes);
    ~CopyPool();

    CopyPool(const CopyPool &) = delete;
    CopyPool &operator=(const CopyPool &) = delete;

    /// One buffer of `buffer_bytes()`. Throws std::bad_alloc only when the
    /// pool is empty and the heap refuses too.
    std::shared_ptr<std::byte> acquire();

    std::size_t buffer_bytes() const noexcept
    {
        return buffer_bytes_;
    }

    std::size_t count() const noexcept
    {
        return count_;
    }

    /// Pool buffers not currently issued.
    std::size_t available() const noexcept
    {
        return free_.size();
    }

    /// Acquisitions served from the heap because the pool was empty.
    std::uint64_t exhausted() const noexcept
    {
        return exhausted_.load(std::memory_order_relaxed);
    }

    /// Whether `p` points into the pool's own storage.
    bool owns(const void *p) const noexcept;

  private:
    void release(std::byte *buffer) noexcept;

    std::byte *storage_{nullptr};
    std::size_t count_;
    std::size_t buffer_bytes_;
    std::size_t stride_;
    BoundedQueue<std::byte *> free_;
    std::atomic<std::uint64_t> exhausted_{0};
};

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_CORE_COPY_POOL_H
