// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_CORE_CPU_TOPOLOGY_H
#define TANGO_BULK_SRC_CORE_CPU_TOPOLOGY_H

#include <tango-bulk/errors.h>

#include <cstddef>

/// The three OS-level questions placement needs answered, and nothing else.
///
/// Deliberately not a topology library. IMPLEMENTATION_SPEC.md 10 defers how a
/// stream is assigned to a rail to M5 "with hardware in hand", so building a
/// topology model now would be inventing the thing the spec says not to invent
/// yet. What is here is the minimum needed to *observe* where a stream landed,
/// which is what turns "affinity did not help" from a guess into a measurement.
///
/// No libnuma and no hwloc. `numa_node_of()` is one `get_mempolicy` syscall, and
/// UCX -- which links neither library -- answers the device-side questions from
/// its own sysfs walk (see src/ucx/locality.h). Adding either dependency for
/// this would cost the project's "stock packages only" property for nothing.
namespace TangoBulk::detail
{

/// The NUMA node backing the page `p` sits on, or -1 if it cannot be determined.
///
/// -1 is a normal answer, not an error: a kernel without NUMA support, a
/// container that hides it, or a page not yet faulted all produce it. Callers
/// report it and carry on.
int numa_node_of(const void *p) noexcept;

/// The CPU the calling thread is running on right now, or -1.
///
/// "Right now" is the whole caveat. On an unpinned thread this is a sample, and
/// the reason a single sample is still worth reporting is that a *stable* value
/// across a run is evidence the scheduler left the engine alone, and a varying
/// one is evidence it did not.
int current_cpu() noexcept;

/// Pin the calling thread to one CPU. `cpu < 0` is a no-op that returns Ok.
///
/// Returns `Status::Ok` on success, `Status::Internal` if the kernel refused.
/// Refusal is legitimate and expected under a restrictive cpuset or in a
/// container, which is why this reports rather than throws: a device server must
/// not fail to start because it could not optimise itself.
Status bind_thread_to_cpu(int cpu) noexcept;

/// How many CPUs the calling thread is *allowed* to run on, or 0 if unknown.
///
/// Against `std::thread::hardware_concurrency()` this is the honest number under
/// a cgroup cpuset: a container pinned to 4 of 128 CPUs reports 4 here and 128
/// there. Used to bound-check a configured affinity against reality rather than
/// against the hardware.
std::size_t allowed_cpu_count() noexcept;

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_CORE_CPU_TOPOLOGY_H
