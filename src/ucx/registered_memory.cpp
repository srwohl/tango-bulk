// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <ucx/registered_memory.h>

#include <core/pinned_budget.h>

#include <tango-bulk/errors.h>

#include <cstring>
#include <string>
#include <utility>

namespace TangoBulk::detail
{

RegisteredMemory RegisteredMemory::ucx_allocated(UcxContext &context,
                                                 std::uint64_t bytes,
                                                 std::uint64_t pinned_limit)
{
    RegisteredMemory memory;
    memory.context_ = context.get();
    memory.bytes_ = bytes;

    PinnedBudget::check_memlock(pinned_limit);

    // 6.1: reserve before mapping.  A rejection here costs nothing; a rejection
    // after ucp_mem_map would mean unwinding a registration.
    if(!PinnedBudget::try_reserve(bytes, pinned_limit))
    {
        throw BulkException(
            BulkError{Status::ResourceExhausted,
                      "pinned memory budget exceeded: this region needs " + std::to_string(bytes) +
                          " bytes, the process already holds " +
                          std::to_string(PinnedBudget::current()) + ", limit is " +
                          std::to_string(pinned_limit),
                      "publisher"});
    }
    memory.reserved_ = true;

    ucp_mem_map_params_t params;
    std::memset(&params, 0, sizeof(params));
    params.field_mask = UCP_MEM_MAP_PARAM_FIELD_LENGTH | UCP_MEM_MAP_PARAM_FIELD_FLAGS;
    params.length = bytes;
    params.flags = UCP_MEM_MAP_ALLOCATE;

    // ALLOCATE and ADDRESS are mutually exclusive in intent: with ALLOCATE set,
    // UCX picks the address and any address supplied is a hint it may ignore.
    // Passing both -- allocating with numa_alloc_onnode and then setting ALLOCATE
    // -- registers UCX's memory and leaks the caller's, which is the trap the
    // `adopted()` factory will have to avoid by *not* setting this flag.
    ucs_status_t status = ucp_mem_map(memory.context_, &params, &memory.memh_);
    if(status != UCS_OK)
    {
        memory.release();
        // RLIMIT_MEMLOCK is the usual cause and UCX does not say so; the warning
        // from check_memlock() above is what connects the two.
        throw_ucx_error("ucp_mem_map", status, "publisher");
    }

    ucp_mem_attr_t attr;
    std::memset(&attr, 0, sizeof(attr));
    attr.field_mask = UCP_MEM_ATTR_FIELD_ADDRESS | UCP_MEM_ATTR_FIELD_LENGTH;

    status = ucp_mem_query(memory.memh_, &attr);
    if(status != UCS_OK)
    {
        memory.release();
        throw_ucx_error("ucp_mem_query", status, "publisher");
    }

    memory.base_ = static_cast<std::byte *>(attr.address);

    // UCX may hand back more than was asked for; it must never hand back less,
    // because every slot offset is computed from the requested size.
    if(attr.length < bytes)
    {
        memory.release();
        throw BulkException(BulkError{Status::Internal,
                                      "ucp_mem_map returned a shorter region than requested",
                                      "publisher"});
    }

    return memory;
}

RegisteredMemory RegisteredMemory::adopted(UcxContext &context,
                                           std::shared_ptr<void> owner,
                                           std::uint64_t bytes,
                                           MemoryKind memory_kind)
{
    RegisteredMemory memory;
    memory.context_ = context.get();
    memory.bytes_ = bytes;
    memory.owner_ = std::move(owner);

    ucp_mem_map_params_t params;
    std::memset(&params, 0, sizeof(params));
    params.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS | UCP_MEM_MAP_PARAM_FIELD_LENGTH |
                        UCP_MEM_MAP_PARAM_FIELD_MEMORY_TYPE;
    params.address = memory.owner_.get();
    params.length = bytes;
    params.memory_type = to_ucs_memory_type(memory_kind);

    const ucs_status_t status = ucp_mem_map(memory.context_, &params, &memory.memh_);
    if(status != UCS_OK)
    {
        memory.release();
        throw_ucx_error("ucp_mem_map(adopted receive buffer)", status, "subscriber");
    }

    memory.base_ = static_cast<std::byte *>(memory.owner_.get());
    return memory;
}

void RegisteredMemory::release() noexcept
{
    if(memh_ != nullptr)
    {
        ucp_mem_unmap(context_, memh_);
        memh_ = nullptr;
    }

    if(reserved_)
    {
        PinnedBudget::release(bytes_);
        reserved_ = false;
    }

    base_ = nullptr;
    owner_.reset();
}

RegisteredMemory::~RegisteredMemory()
{
    release();
}

RegisteredMemory::RegisteredMemory(RegisteredMemory &&other) noexcept :
    context_(other.context_),
    memh_(other.memh_),
    base_(other.base_),
    bytes_(other.bytes_),
    reserved_(other.reserved_),
    owner_(std::move(other.owner_))
{
    other.memh_ = nullptr;
    other.base_ = nullptr;
    other.reserved_ = false;
}

RegisteredMemory &RegisteredMemory::operator=(RegisteredMemory &&other) noexcept
{
    if(this != &other)
    {
        release();

        context_ = other.context_;
        memh_ = other.memh_;
        base_ = other.base_;
        bytes_ = other.bytes_;
        reserved_ = other.reserved_;
        owner_ = std::move(other.owner_);

        other.memh_ = nullptr;
        other.base_ = nullptr;
        other.reserved_ = false;
    }
    return *this;
}

bool RegisteredMemory::contains(const void *p) const noexcept
{
    if(base_ == nullptr || p == nullptr)
    {
        return false;
    }

    const auto *q = static_cast<const std::byte *>(p);
    return q >= base_ && q < base_ + bytes_;
}

} // namespace TangoBulk::detail
