// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <ucx/locality.h>

#include <core/cpu_topology.h>

#include <ucs/memory/numa.h>

#include <algorithm>
#include <array>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <set>
#include <string>

namespace TangoBulk::detail
{
namespace
{

/// UCX reports IB devices as "mlx5_0:1" -- device, colon, port. `numa_node` is a
/// property of the device, so the port has to come off before the sysfs lookup.
std::string device_without_port(const std::string &name)
{
    const std::size_t colon = name.find(':');
    return colon == std::string::npos ? name : name.substr(0, colon);
}

/// Ask UCX for the node a device sits on.
///
/// `ucs_numa_node_of_device` wants a sysfs path, not a name. Only IB-class
/// devices are tried: a shared-memory or TCP "device" has no NUMA node to find,
/// and guessing a path for one would produce a confident wrong answer where -1 is
/// the correct one.
int node_of_device(const std::string &device) noexcept
{
    if(device.empty())
    {
        return -1;
    }

    const std::string path = "/sys/class/infiniband/" + device_without_port(device);
    const ucs_numa_node_t node = ucs_numa_node_of_device(path.c_str());

    return node == UCS_NUMA_NODE_UNDEFINED ? -1 : static_cast<int>(node);
}

int node_of_cpu(int cpu) noexcept
{
    if(cpu < 0)
    {
        return -1;
    }

    const ucs_numa_node_t node = ucs_numa_node_of_cpu(cpu);
    return node == UCS_NUMA_NODE_UNDEFINED ? -1 : static_cast<int>(node);
}

} // namespace

const char *to_string(Placement placement) noexcept
{
    switch(placement)
    {
    case Placement::SingleNode:
        return "single-node";
    case Placement::Local:
        return "local";
    case Placement::Split:
        return "split";
    case Placement::Unknown:
        break;
    }
    return "unknown";
}

Placement Locality::placement() const noexcept
{
    // One node means every allocation and every device is on it by construction.
    // Checked first because it makes the rest of the question moot, and saying so
    // is more useful than reporting an unanswerable comparison.
    if(host_nodes == 1)
    {
        return Placement::SingleNode;
    }

    // A software device (rxe) or a shared-memory transport has no NUMA node to
    // find, and neither does a container that hides the topology.  Unknown is the
    // honest answer; it is not Split, because nothing has been shown to disagree.
    if(memory_node < 0 || device_node < 0 || engine_node < 0)
    {
        return Placement::Unknown;
    }

    return (memory_node == device_node && memory_node == engine_node) ? Placement::Local
                                                                     : Placement::Split;
}

std::string Locality::to_string() const
{
    const auto number = [](int value)
    { return value < 0 ? std::string("?") : std::to_string(value); };

    std::string out;
    out.reserve(160);
    out += "transport=" + (transport.empty() ? std::string("?") : transport) + ";";
    out += "device=" + (device.empty() ? std::string("?") : device) + ";";
    // `nic_node` describes `device`, the first lane, so the rail count sits next
    // to it: a reader who sees rails=2 knows the node number covers one of two,
    // and a reader who sees rails=1 knows the report is complete.
    out += "rails=" + (rails == 0 ? std::string("?") : std::to_string(rails)) + ";";
    if(rails > 1)
    {
        out += "devices=" + devices + ";";
    }
    out += "ring_node=" + number(memory_node) + ";";
    out += "nic_node=" + number(device_node) + ";";
    out += "engine_cpu=" + number(engine_cpu) + ";";
    out += "engine_node=" + number(engine_node) + ";";
    out += "node_distance=" + number(node_distance) + ";";
    out += "host_nodes=" + (host_nodes == 0 ? std::string("?") : std::to_string(host_nodes)) + ";";
    // Qualified: the member to_string() hides the free one at class scope.
    out += std::string("placement=") + detail::to_string(placement()) + ";";
    return out;
}

Locality observe(ucp_ep_h endpoint, const void *ring) noexcept
{
    Locality locality;

    locality.host_nodes = ucs_numa_num_configured_nodes();
    locality.memory_node = numa_node_of(ring);
    locality.engine_cpu = current_cpu();
    locality.engine_node = node_of_cpu(locality.engine_cpu);

    if(endpoint != nullptr)
    {
        // Every lane, not just the first. UCX stripes a rendezvous transfer over
        // up to UCX_MAX_RNDV_RAILS devices (2 by default), so on a multi-port host
        // the first entry names one of several NICs actually carrying the frame.
        // 8 is past any rail count this library expects to meet; UCX writes back
        // how many it filled.
        std::array<ucp_transport_entry_t, 8> entries{};

        ucp_ep_attr_t attr;
        std::memset(&attr, 0, sizeof(attr));
        attr.field_mask = UCP_EP_ATTR_FIELD_TRANSPORTS;
        attr.transports.entries = entries.data();
        attr.transports.num_entries = static_cast<unsigned>(entries.size());
        attr.transports.entry_size = sizeof(ucp_transport_entry_t);

        if(ucp_ep_query(endpoint, &attr) == UCS_OK)
        {
            locality.rails = std::min(attr.transports.num_entries,
                                      static_cast<unsigned>(entries.size()));

            for(unsigned i = 0; i < locality.rails; ++i)
            {
                const ucp_transport_entry_t &entry = entries[i];
                if(entry.device_name == nullptr)
                {
                    continue;
                }
                if(!locality.devices.empty())
                {
                    locality.devices += ",";
                }
                locality.devices += entry.device_name;
            }

            if(locality.rails > 0)
            {
                const ucp_transport_entry_t &first = entries[0];
                if(first.transport_name != nullptr)
                {
                    locality.transport = first.transport_name;
                }
                if(first.device_name != nullptr)
                {
                    locality.device = first.device_name;
                }
            }
        }
    }

    locality.device_node = node_of_device(locality.device);

    if(locality.memory_node >= 0 && locality.device_node >= 0)
    {
        locality.node_distance = static_cast<int>(
            ucs_numa_distance(static_cast<ucs_numa_node_t>(locality.memory_node),
                              static_cast<ucs_numa_node_t>(locality.device_node)));
    }

    return locality;
}

void report(const Locality &locality, const char *origin) noexcept
{
    static std::mutex mutex;
    static std::set<std::string> seen;

    const Placement verdict = locality.placement();

    // Keyed on device plus verdict, so a fan-out publisher opening four sessions
    // over one NIC says this once, and a genuinely different placement still gets
    // its own line.
    // Keyed on the whole rail set rather than the first lane, so a session that
    // lands on a different set of NICs still gets its own line.
    const std::string key =
        std::string(origin) + "/" + locality.devices + "/" + to_string(verdict);

    {
        const std::lock_guard<std::mutex> lock(mutex);
        if(!seen.insert(key).second)
        {
            return;
        }
    }

    // Only Split is a warning.  SingleNode and Local are good news, and Unknown is
    // the honest answer on a software device or under a container -- warning about
    // either would train an operator to filter this out, which would cost the one
    // case that matters.
    if(verdict != Placement::Split)
    {
        std::fprintf(stderr, "tango-bulk: %s placement: %s\n", origin,
                     locality.to_string().c_str());
        return;
    }

    std::fprintf(stderr,
                 "tango-bulk: warning: %s ring is on NUMA node %d but its NIC is on node %d, "
                 "so every frame crosses the interconnect. The ring's node is fixed at "
                 "construction by first touch and cannot be changed afterwards -- place the "
                 "process, e.g. 'numactl --cpunodebind=%d --membind=%d'. %s\n",
                 origin, locality.memory_node, locality.device_node, locality.device_node,
                 locality.device_node, locality.to_string().c_str());
}

} // namespace TangoBulk::detail
