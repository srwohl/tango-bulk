// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_UCX_REGISTERED_RING_H
#define TANGO_BULK_SRC_UCX_REGISTERED_RING_H

#include <ucx/ucx_context.h>

#include <cstddef>
#include <cstdint>

/// A ring of frame slots inside **one** registered region.
///
/// One `ucp_mem_map` covers the whole ring and slots are offsets into it, so the
/// memory-region count is 1 regardless of depth (IMPLEMENTATION_SPEC.md 2.3).
/// That matters on real hardware: registration is expensive, MR table entries
/// are finite, and a per-slot registration would put a per-frame lookup on the
/// data path for no benefit.
namespace TangoBulk::detail
{

class RegisteredRing
{
  public:
    /// Reserves against the process-wide pinned budget *before* mapping, then
    /// maps.  Throws BulkException{ResourceExhausted} if the budget is exceeded
    /// and BulkException{TransportFailure} if UCX cannot register.
    RegisteredRing(UcxContext &context,
                   std::uint64_t slot_bytes,
                   std::uint32_t depth,
                   bool pad_stride,
                   std::uint64_t pinned_limit);

    ~RegisteredRing();

    RegisteredRing(const RegisteredRing &) = delete;
    RegisteredRing &operator=(const RegisteredRing &) = delete;

    std::byte *slot(std::size_t index) const noexcept
    {
        return base_ + index * stride_;
    }

    std::size_t slot_bytes() const noexcept
    {
        return slot_bytes_;
    }

    std::size_t stride() const noexcept
    {
        return stride_;
    }

    std::size_t depth() const noexcept
    {
        return depth_;
    }

    std::uint64_t mapped_bytes() const noexcept
    {
        return mapped_bytes_;
    }

    /// Whether `p` points inside this registered region.
    ///
    /// This is what the M2 zero-copy exit criterion checks: the payload address
    /// handed to the application must lie inside the registered receive ring,
    /// not in some staging buffer the library copied out of.
    bool contains(const void *p) const noexcept;

    /// 6.2's stride rule, exposed for testing.
    ///
    /// stride = align_up(slot_bytes, 4096), and then one extra page if the
    /// result is a power of two at or above 256 KiB.  That last clause is not
    /// superstition: with an exactly-1 MiB stride every slot aliases into the
    /// same cache sets and the receive-side copy measured ~5.9 GiB/s against
    /// ~16 GiB/s once a 200-byte prefix broke the alignment.
    static std::size_t compute_stride(std::uint64_t slot_bytes, bool pad) noexcept;

  private:
    ucp_context_h context_{nullptr};
    ucp_mem_h memh_{nullptr};
    std::byte *base_{nullptr};
    std::size_t slot_bytes_{0};
    std::size_t stride_{0};
    std::size_t depth_{0};
    std::uint64_t mapped_bytes_{0};
    bool reserved_{false};
};

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_UCX_REGISTERED_RING_H
