// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <ucx/registered_ring.h>

#include <tango-bulk/errors.h>

namespace TangoBulk::detail
{
namespace
{

constexpr std::size_t k_page_bytes = 4096;
constexpr std::size_t k_alias_threshold = 256u << 10;

} // namespace

std::size_t RegisteredRing::compute_stride(std::uint64_t slot_bytes, bool pad) noexcept
{
    std::size_t stride =
        (static_cast<std::size_t>(slot_bytes) + k_page_bytes - 1) / k_page_bytes * k_page_bytes;

    if(pad && stride >= k_alias_threshold && (stride & (stride - 1)) == 0)
    {
        stride += k_page_bytes;
    }

    return stride;
}

RegisteredRing::RegisteredRing(UcxContext &context,
                               std::uint64_t slot_bytes,
                               std::uint32_t depth,
                               bool pad_stride,
                               std::uint64_t pinned_limit) :
    memory_(RegisteredMemory::ucx_allocated(
        context, compute_stride(slot_bytes, pad_stride) * depth, pinned_limit)),
    base_(memory_.base()),
    slot_bytes_(static_cast<std::size_t>(slot_bytes)),
    stride_(compute_stride(slot_bytes, pad_stride)),
    depth_(depth)
{
    // Nothing left to do: acquiring, budgeting and unmapping the region are
    // RegisteredMemory's, and this class is the arithmetic over it.
}

RegisteredRing::RegisteredRing(UcxContext &context,
                               std::uint64_t slot_bytes,
                               std::uint32_t depth,
                               std::shared_ptr<void> receive_buffer,
                               std::uint64_t receive_buffer_bytes,
                               MemoryKind memory_kind) :
    memory_(RegisteredMemory::adopted(
        context, std::move(receive_buffer), receive_buffer_bytes, memory_kind)),
    base_(memory_.base()),
    slot_bytes_(static_cast<std::size_t>(slot_bytes)),
    // The public receive-buffer contract requires exactly slot_bytes * depth
    // bytes. Caller-owned arenas are already allocated and cannot absorb the
    // hidden page-rounding used for UCX-owned host rings. Pack their slots with
    // no gaps; CUDA allocations do not require each destination to be page
    // aligned.
    stride_(static_cast<std::size_t>(slot_bytes)),
    depth_(depth)
{
}

bool RegisteredRing::contains(const void *p) const noexcept
{
    return memory_.contains(p);
}

} // namespace TangoBulk::detail
