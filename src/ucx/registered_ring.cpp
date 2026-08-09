// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <ucx/registered_ring.h>

#include <core/pinned_budget.h>

#include <tango-bulk/errors.h>

#include <cstring>
#include <string>

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
    context_(context.get()),
    slot_bytes_(static_cast<std::size_t>(slot_bytes)),
    stride_(compute_stride(slot_bytes, pad_stride)),
    depth_(depth)
{
    mapped_bytes_ = static_cast<std::uint64_t>(stride_) * depth;

    PinnedBudget::check_memlock(pinned_limit);

    // 6.1: reserve before mapping.  A rejection here costs nothing; a rejection
    // after ucp_mem_map would mean unwinding a registration.
    if(!PinnedBudget::try_reserve(mapped_bytes_, pinned_limit))
    {
        throw BulkException(BulkError{
            Status::ResourceExhausted,
            "pinned memory budget exceeded: this ring needs " + std::to_string(mapped_bytes_) +
                " bytes, the process already holds " + std::to_string(PinnedBudget::current()) +
                ", limit is " + std::to_string(pinned_limit),
            "publisher"});
    }
    reserved_ = true;

    ucp_mem_map_params_t params;
    std::memset(&params, 0, sizeof(params));
    // 6.3: UCP_MEM_MAP_ALLOCATE where the ring is library-owned.  Adopting a
    // vendor-supplied ring (the normal detector case) uses the ADDRESS field
    // instead and is M5.
    params.field_mask = UCP_MEM_MAP_PARAM_FIELD_LENGTH | UCP_MEM_MAP_PARAM_FIELD_FLAGS;
    params.length = mapped_bytes_;
    params.flags = UCP_MEM_MAP_ALLOCATE;

    ucs_status_t status = ucp_mem_map(context_, &params, &memh_);
    if(status != UCS_OK)
    {
        PinnedBudget::release(mapped_bytes_);
        reserved_ = false;
        // RLIMIT_MEMLOCK is the usual cause and UCX does not say so; the warning
        // from check_memlock() above is what connects the two.
        throw_ucx_error("ucp_mem_map", status, "publisher");
    }

    ucp_mem_attr_t attr;
    std::memset(&attr, 0, sizeof(attr));
    attr.field_mask = UCP_MEM_ATTR_FIELD_ADDRESS | UCP_MEM_ATTR_FIELD_LENGTH;

    status = ucp_mem_query(memh_, &attr);
    if(status != UCS_OK)
    {
        ucp_mem_unmap(context_, memh_);
        memh_ = nullptr;
        PinnedBudget::release(mapped_bytes_);
        reserved_ = false;
        throw_ucx_error("ucp_mem_query", status, "publisher");
    }

    base_ = static_cast<std::byte *>(attr.address);

    // UCX may hand back more than was asked for; it must never hand back less,
    // because every slot offset below is computed from the requested size.
    if(attr.length < mapped_bytes_)
    {
        ucp_mem_unmap(context_, memh_);
        memh_ = nullptr;
        PinnedBudget::release(mapped_bytes_);
        reserved_ = false;
        throw BulkException(BulkError{Status::Internal,
                                      "ucp_mem_map returned a shorter region than requested",
                                      "publisher"});
    }
}

RegisteredRing::~RegisteredRing()
{
    if(memh_ != nullptr)
    {
        ucp_mem_unmap(context_, memh_);
    }

    if(reserved_)
    {
        PinnedBudget::release(mapped_bytes_);
    }
}

bool RegisteredRing::contains(const void *p) const noexcept
{
    if(base_ == nullptr || p == nullptr)
    {
        return false;
    }

    const auto *q = static_cast<const std::byte *>(p);
    return q >= base_ && q < base_ + mapped_bytes_;
}

} // namespace TangoBulk::detail
