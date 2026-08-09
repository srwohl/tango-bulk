// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

// _GNU_SOURCE for sched_getcpu, CPU_SET and the affinity calls. Set before any
// include, because a later <sched.h> from another header would win.
#ifndef _GNU_SOURCE
    #define _GNU_SOURCE
#endif

#include <core/cpu_topology.h>

#include <sched.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace TangoBulk::detail
{
namespace
{

// From <linux/mempolicy.h>, which is not worth including for two constants that
// are part of the kernel ABI and therefore cannot change.
constexpr int k_mpol_f_node = 1 << 0; ///< return the node, not the policy
constexpr int k_mpol_f_addr = 1 << 1; ///< interpret the pointer argument

} // namespace

int numa_node_of(const void *p) noexcept
{
    if(p == nullptr)
    {
        return -1;
    }

    int node = -1;

    // get_mempolicy(MPOL_F_NODE | MPOL_F_ADDR) is what libnuma's
    // numa_move_pages/get_mempolicy wrappers do; calling it directly avoids the
    // dependency. Note it reports where the page *is*, which for a first-touch
    // allocation means "whichever node the thread that touched it ran on" -- so
    // asking before the ring is faulted is meaningless, and asking after
    // registration is not, because ucp_mem_map has to pin the pages.
    const long rc =
        ::syscall(SYS_get_mempolicy, &node, nullptr, 0UL, const_cast<void *>(p),
                  static_cast<unsigned long>(k_mpol_f_node | k_mpol_f_addr));

    return rc == 0 ? node : -1;
}

int current_cpu() noexcept
{
    const int cpu = ::sched_getcpu();
    return cpu < 0 ? -1 : cpu;
}

Status bind_thread_to_cpu(int cpu) noexcept
{
    // Negative means "leave it alone", and that is the default for a reason.
    // numactl, taskset, systemd's CPUAffinity= and a Slurm cpu-bind can all place
    // this process already, and a library that pins itself anyway fights whatever
    // the deployment decided. NCCL learned this and ships
    // NCCL_IGNORE_CPU_AFFINITY as the escape hatch; defaulting to "do nothing" is
    // the same lesson applied one step earlier.
    if(cpu < 0)
    {
        return Status::Ok;
    }

    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(static_cast<unsigned>(cpu), &set);

    if(::sched_setaffinity(0, sizeof(set), &set) != 0)
    {
        return Status::Internal;
    }

    return Status::Ok;
}

std::size_t allowed_cpu_count() noexcept
{
    cpu_set_t set;
    CPU_ZERO(&set);

    if(::sched_getaffinity(0, sizeof(set), &set) != 0)
    {
        return 0;
    }

    return static_cast<std::size_t>(CPU_COUNT(&set));
}

} // namespace TangoBulk::detail
