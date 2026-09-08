// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_UCX_LOCALITY_H
#define TANGO_BULK_SRC_UCX_LOCALITY_H

#include <ucx/ucx_context.h>

#include <cstddef>
#include <string>

/// Where a stream actually landed: the ring, the NIC, and the engine thread.
///
/// **Observation, not control.** Nothing here places anything. That ordering is
/// deliberate and is the lesson of `engine_cpu_affinity`, which promised to place
/// the engine, did nothing, and could not be caught doing nothing. Pinning a
/// thread while leaving 256 MiB of registered ring on the far socket produces a
/// plausible-looking number and a wrong conclusion; a report that says
/// `coherent=no` makes that visible the first time anyone runs it.
///
/// It is also the cheapest useful step. Most detector DAQ hosts are
/// single-socket, and on those this reports one node for everything and the whole
/// placement question is answered for free. Building placement first would have
/// meant building it without knowing whether it was needed.
///
/// Everything comes from UCX, which links neither hwloc nor libnuma and answers
/// these questions from its own sysfs walk:
///
///   - `ucp_ep_query` + `UCP_EP_ATTR_FIELD_TRANSPORTS` -> the device this session
///     is really using, e.g. "mlx5_0:1"
///   - `ucs_numa_node_of_device` -> that device's NUMA node
///   - `ucs_numa_node_of_cpu` -> the engine thread's node
///   - `ucs_numa_distance` -> how far apart two nodes are
///
/// plus one `get_mempolicy` syscall for the ring, via `core/cpu_topology.h`.
namespace TangoBulk::detail
{

/// What the observed placement amounts to.
///
/// Three states rather than a bool, because "cannot tell" and "nothing to tell"
/// are different answers and neither is a problem. A two-state version reported
/// `coherent=no` on every single-socket host -- true only in the sense that a
/// question with no answer has no affirmative one, and exactly the kind of false
/// alarm that teaches an operator to filter the report out.
enum class Placement
{
    SingleNode, ///< the host has one NUMA node; there is nothing to place
    Local,      ///< ring, NIC and engine agree on a node
    Split,      ///< they are known and they disagree -- the actionable case
    Unknown     ///< at least one node could not be determined
};

const char *to_string(Placement placement) noexcept;

struct Locality
{
    /// Number of NUMA nodes the host has, or 0 if unknown.
    unsigned host_nodes{0};

    /// UCX's names for what this endpoint chose. Empty if the endpoint could not
    /// be queried, which is normal before a session is armed.
    ///
    /// `transport` and `device` describe the **first** lane. `devices` lists every
    /// lane, comma-separated, and `rails` counts them.
    ///
    /// Reporting only the first lane was a real defect, not a tidying-up: on a host
    /// with two active 25 GbE ports this printed `device=mlx5_2:1` while rendezvous
    /// was striping each frame across `mlx5_2` and `mlx5_3`. Measured against one
    /// port the library looked like it was leaving half the fabric unused; it was
    /// in fact at 96% of the aggregate. A placement report that cannot show the
    /// second rail turns a correct result into a bug hunt.
    std::string transport;
    std::string device;
    std::string devices;
    unsigned rails{0};

    int memory_node{-1}; ///< NUMA node the registered ring's pages sit on
    int device_node{-1}; ///< NUMA node the NIC sits on
    int engine_cpu{-1};  ///< CPU the engine thread was on when sampled
    int engine_node{-1}; ///< NUMA node of `engine_cpu`
    int node_distance{-1};

    /// What this placement amounts to.  `Split` is the one worth acting on.
    Placement placement() const noexcept;

    /// One line, safe to log and safe to put in `QueryReply.counters`.
    ///
    /// 3.8 forbids that blob from carrying addresses, memory keys or session
    /// identifiers; a device name and a node number are none of those.
    std::string to_string() const;
};

/// Sample the placement of one stream. Call from the **engine thread**, because
/// `engine_cpu` is meaningless anywhere else.
///
/// `ring` should be the base of the registered region. Never throws, and reports
/// -1 for anything it cannot determine rather than guessing: a container that
/// hides NUMA, a transport with no device, and a kernel without NUMA support all
/// end up here and none of them is an error.
Locality observe(ucp_ep_h endpoint, const void *ring) noexcept;

/// Emit `locality` on stderr, once per distinct (device, verdict) pair.
///
/// Rate-limited because it is called per session and a fan-out publisher would
/// otherwise repeat itself; the incoherent case is what matters and it is
/// reported the first time it appears. Matches how RegisteredMemory's pinned
/// ledger check warns, for want of a logger this library is not going to invent.
void report(const Locality &locality, const char *origin) noexcept;

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_UCX_LOCALITY_H
