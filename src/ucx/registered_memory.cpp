// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <ucx/registered_memory.h>

#include <tango-bulk/errors.h>

#include <sys/resource.h>

#include <atomic>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <string>
#include <utility>

namespace TangoBulk::detail
{
namespace
{

std::atomic<std::uint64_t> &pinned_bytes() noexcept
{
    static std::atomic<std::uint64_t> value{0};
    return value;
}

bool try_reserve(std::uint64_t bytes, std::uint64_t limit) noexcept
{
    std::atomic<std::uint64_t> &counter = pinned_bytes();
    std::uint64_t observed = counter.load(std::memory_order_relaxed);

    for(;;)
    {
        if(bytes > limit || observed > limit - bytes)
        {
            return false;
        }

        if(counter.compare_exchange_weak(observed,
                                         observed + bytes,
                                         std::memory_order_acq_rel,
                                         std::memory_order_relaxed))
        {
            return true;
        }
    }
}

void release_reservation(std::uint64_t bytes) noexcept
{
    pinned_bytes().fetch_sub(bytes, std::memory_order_acq_rel);
}

void check_memlock(std::uint64_t limit) noexcept
{
    static std::once_flag once;

    std::call_once(
        once,
        [](std::uint64_t configured)
        {
            struct rlimit rl
            {
            };

            if(getrlimit(RLIMIT_MEMLOCK, &rl) != 0 || rl.rlim_cur == RLIM_INFINITY)
            {
                return;
            }

            const auto allowed = static_cast<std::uint64_t>(rl.rlim_cur);
            if(configured > allowed)
            {
                std::fprintf(stderr,
                             "tango-bulk: warning: pinned_memory_limit_bytes is %llu but "
                             "RLIMIT_MEMLOCK is %llu. Registration will fail with an opaque "
                             "UCX error once the ring exceeds the kernel limit; raise "
                             "'ulimit -l' or lower the configured limit.\n",
                             static_cast<unsigned long long>(configured),
                             static_cast<unsigned long long>(allowed));
            }
        },
        limit);
}

} // namespace

class RegisteredMemory::Reservation
{
  public:
    static std::unique_ptr<Reservation> acquire(std::uint64_t bytes,
                                                std::uint64_t limit,
                                                const char *origin)
    {
        check_memlock(limit);
        if(!try_reserve(bytes, limit))
        {
            throw BulkException(
                BulkError{Status::ResourceExhausted,
                          "pinned memory budget exceeded: this region needs " +
                              std::to_string(bytes) + " bytes, the process already holds " +
                              std::to_string(pinned_bytes().load(std::memory_order_relaxed)) +
                              ", limit is " + std::to_string(limit),
                          origin});
        }

        try
        {
            return std::unique_ptr<Reservation>(new Reservation(bytes));
        }
        catch(...)
        {
            release_reservation(bytes);
            throw;
        }
    }

    ~Reservation()
    {
        release_reservation(bytes_);
    }

    Reservation(const Reservation &) = delete;
    Reservation &operator=(const Reservation &) = delete;

  private:
    explicit Reservation(std::uint64_t bytes) noexcept : bytes_(bytes) {}

    std::uint64_t bytes_;
};

RegisteredMemory RegisteredMemory::ucx_allocated(UcxContext &context,
                                                 std::uint64_t bytes,
                                                 std::uint64_t pinned_limit,
                                                 const char *origin)
{
    RegisteredMemory memory;
    memory.context_ = context.get();
    memory.bytes_ = bytes;
    memory.reservation_ = Reservation::acquire(bytes, pinned_limit, origin);

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
        throw_ucx_error("ucp_mem_map", status, origin);
    }

    ucp_mem_attr_t attr;
    std::memset(&attr, 0, sizeof(attr));
    attr.field_mask = UCP_MEM_ATTR_FIELD_ADDRESS | UCP_MEM_ATTR_FIELD_LENGTH;

    status = ucp_mem_query(memory.memh_, &attr);
    if(status != UCS_OK)
    {
        memory.release();
        throw_ucx_error("ucp_mem_query", status, origin);
    }

    memory.base_ = static_cast<std::byte *>(attr.address);

    // UCX may hand back more than was asked for; it must never hand back less,
    // because every slot offset is computed from the requested size.
    if(attr.length < bytes)
    {
        memory.release();
        throw BulkException(BulkError{Status::Internal,
                                      "ucp_mem_map returned a shorter region than requested",
                                      origin});
    }

    return memory;
}

RegisteredMemory RegisteredMemory::adopted(UcxContext &context,
                                           std::shared_ptr<void> owner,
                                           std::uint64_t bytes,
                                           MemoryKind memory_kind,
                                           std::uint64_t pinned_limit)
{
    RegisteredMemory memory;
    memory.context_ = context.get();
    memory.bytes_ = bytes;
    memory.owner_ = std::move(owner);
    memory.reservation_ = Reservation::acquire(bytes, pinned_limit, "subscriber");

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

    reservation_.reset();

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
    reservation_(std::move(other.reservation_)),
    owner_(std::move(other.owner_))
{
    other.memh_ = nullptr;
    other.base_ = nullptr;
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
        reservation_ = std::move(other.reservation_);
        owner_ = std::move(other.owner_);

        other.memh_ = nullptr;
        other.base_ = nullptr;
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
