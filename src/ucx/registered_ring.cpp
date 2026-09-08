// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <ucx/registered_ring.h>

#include <tango-bulk/errors.h>

#include <limits>

namespace TangoBulk::detail
{
namespace
{

constexpr std::size_t k_page_bytes = 4096;
constexpr std::size_t k_alias_threshold = 256u << 10;

std::uint64_t checked_region_bytes(std::size_t stride,
                                   std::uint32_t depth,
                                   const char *origin)
{
    if(depth != 0 && stride > std::numeric_limits<std::uint64_t>::max() / depth)
    {
        throw BulkException(BulkError{Status::ResourceExhausted,
                                      "receive ring byte count overflows",
                                      origin});
    }

    return static_cast<std::uint64_t>(stride) * depth;
}

std::uint64_t checked_caller_region(std::uint64_t slot_bytes,
                                    std::uint32_t depth,
                                    std::uint64_t supplied,
                                    const char *origin)
{
    if(depth != 0 && slot_bytes > std::numeric_limits<std::uint64_t>::max() / depth)
    {
        throw BulkException(BulkError{Status::ResourceExhausted,
                                      "caller-owned receive ring byte count overflows",
                                      origin});
    }

    const std::uint64_t required = slot_bytes * depth;
    if(supplied < required)
    {
        throw BulkException(BulkError{Status::ResourceExhausted,
                                      "caller-owned receive ring is smaller than its plan",
                                      origin});
    }
    return supplied;
}

} // namespace

std::size_t RegisteredRing::compute_stride(std::uint64_t slot_bytes, bool pad) noexcept
{
    // Receive rings use the exact logical slot size so the ReceivePlan's
    // pinned-byte value matches the registered region. Publisher rings may
    // opt into page alignment and the extra alias-breaking page because their
    // registration policy is independent of the subscriber's prepared plan.
    std::size_t stride = static_cast<std::size_t>(slot_bytes);
    if(pad)
    {
        stride = (stride + k_page_bytes - 1) / k_page_bytes * k_page_bytes;
        if(stride >= k_alias_threshold && (stride & (stride - 1)) == 0)
        {
            stride += k_page_bytes;
        }
    }

    return stride;
}

RegisteredRing::RegisteredRing(UcxContext &context,
                               std::uint64_t slot_bytes,
                               std::uint32_t depth,
                               bool pad_stride,
                               std::uint64_t pinned_limit,
                               const char *origin) :
    memory_(RegisteredMemory::ucx_allocated(
        context,
        checked_region_bytes(compute_stride(slot_bytes, pad_stride), depth, origin),
        pinned_limit,
        origin)),
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
                               MemoryKind memory_kind,
                               std::uint64_t pinned_limit) :
    memory_(RegisteredMemory::adopted(
        context,
        std::move(receive_buffer),
        checked_caller_region(slot_bytes, depth, receive_buffer_bytes, "subscriber"),
        memory_kind,
        pinned_limit)),
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
