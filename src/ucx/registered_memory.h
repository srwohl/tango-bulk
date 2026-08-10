// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_UCX_REGISTERED_MEMORY_H
#define TANGO_BULK_SRC_UCX_REGISTERED_MEMORY_H

#include <ucx/ucx_context.h>

#include <tango-bulk/frame.h>

#include <cstddef>
#include <cstdint>
#include <memory>

/// One registered region: where the bytes are, who owns them, and what UCX
/// calls them.
///
/// Split out of `RegisteredRing` on purpose, and the purpose is the only
/// interesting thing about this file. A ring does two unrelated jobs -- *acquire
/// registered memory* and *carve it into slots* -- and every future change is to
/// the first. 6.3 already names two acquisition modes:
///
///   - `UCP_MEM_MAP_ALLOCATE` where the ring is library-owned
///   - `UCP_MEM_MAP_PARAM_FIELD_ADDRESS` when adopting a vendor ring, which 6.3
///     calls "the normal detector case, not an optional convenience"
///
/// and dual-socket placement adds a third: allocate on a chosen NUMA node, then
/// register what we allocated. All three differ only in how `base_` and `memh_`
/// come to exist and who frees them. None of them changes slot arithmetic,
/// `contains()`, or a single call site in the publisher or subscriber.
///
/// So each new mode is a `static` factory here plus nothing else. That is the
/// whole design: the seam is a value type with private construction, not a
/// hierarchy, not a policy parameter threaded through five signatures, and not
/// an allocator concept. Today only `ucx_allocated()` exists, because building
/// the other two before there is hardware to measure them would be inventing the
/// rail-assignment policy 10 explicitly defers to M5.
namespace TangoBulk::detail
{

class RegisteredMemory
{
  public:
    /// 6.3, library-owned: UCX allocates the pages and owns them.
    ///
    /// Reserves against the process-wide pinned budget *before* mapping, so a
    /// rejection costs nothing instead of unwinding a registration. Throws
    /// `BulkException{ResourceExhausted}` if the budget is exceeded and
    /// `{TransportFailure}` if UCX cannot register.
    ///
    /// The pages land on whichever NUMA node the *calling* thread touches them
    /// from, because that is what first-touch means. On a dual-socket host that
    /// makes the constructing thread's placement the ring's placement -- see
    /// `src/ucx/locality.h` for how that is reported, and why reporting it
    /// matters more right now than controlling it.
    static RegisteredMemory ucx_allocated(UcxContext &context,
                                          std::uint64_t bytes,
                                          std::uint64_t pinned_limit);

    /// Register caller-owned memory, including CUDA and ROCm device memory.
    /// The shared owner is retained for as long as UCX or any FrameView can
    /// refer to the region.
    static RegisteredMemory adopted(UcxContext &context,
                                    std::shared_ptr<void> owner,
                                    std::uint64_t bytes,
                                    MemoryKind memory_kind);

    // The two modes this shape exists to make additive rather than invasive:
    //
    //   static RegisteredMemory on_node(UcxContext &, std::uint64_t bytes,
    //                                   int numa_node, std::uint64_t pinned_limit);
    //   static RegisteredMemory adopted(UcxContext &, void *base, std::uint64_t bytes);
    //
    // Deliberately absent rather than declared-and-throwing. A declared function
    // that always fails is the mistake `engine_cpu_affinity` made in reverse: it
    // advertises a capability that is not there. A missing factory is a compile
    // error at the call site, which is the loudest failure available.

    ~RegisteredMemory();

    RegisteredMemory(RegisteredMemory &&other) noexcept;
    RegisteredMemory &operator=(RegisteredMemory &&other) noexcept;

    RegisteredMemory(const RegisteredMemory &) = delete;
    RegisteredMemory &operator=(const RegisteredMemory &) = delete;

    std::byte *base() const noexcept
    {
        return base_;
    }

    std::uint64_t bytes() const noexcept
    {
        return bytes_;
    }

    ucp_mem_h handle() const noexcept
    {
        return memh_;
    }

    bool contains(const void *p) const noexcept;

  private:
    RegisteredMemory() = default;

    void release() noexcept;

    ucp_context_h context_{nullptr};
    ucp_mem_h memh_{nullptr};
    std::byte *base_{nullptr};
    std::uint64_t bytes_{0};

    /// Whether this object owes the pinned budget a release. Tracked separately
    /// from `memh_` because a failed map must give the reservation back without
    /// there being anything to unmap.
    bool reserved_{false};
    std::shared_ptr<void> owner_;
};

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_UCX_REGISTERED_MEMORY_H
