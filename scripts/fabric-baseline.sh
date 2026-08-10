#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
#
# SPDX-License-Identifier: LGPL-3.0-or-later
#
# What the fabric can do, before asking what this library does with it.
#
#   ./scripts/fabric-baseline.sh describe                 # both hosts, first
#   ./scripts/fabric-baseline.sh server                   # on the publisher host
#   ./scripts/fabric-baseline.sh client <publisher-host>  # on the subscriber host
#
# `describe` answers the question that has to be settled before any GiB/s figure
# means anything: what the link rate is, and whether the NIC shares a NUMA node
# with the cores the engine lands on. Everything else is a ceiling measurement --
# ib_write_bw is the NIC with no software above it, ucx_perftest is the NIC with
# UCX above it, and this library has to be read against both, not against a round
# number someone remembers about the fabric.

set -u

DEV=${DEV:-mlx5_2}
PORT=${PORT:-1}
SIZE=${SIZE:-1048576}
ITERS=${ITERS:-5000}

hr() { printf '\n=== %s\n' "$1"; }

describe() {
    hr "link rate -- the ceiling every later number is a fraction of"
    # active_speed x active_width is the real rate; the board's model number is
    # not. A 100G-capable card negotiated down to 4x FDR10 looks identical in
    # lspci and costs half the bandwidth.
    if command -v ibstatus >/dev/null 2>&1; then
        ibstatus "$DEV" 2>/dev/null || ibstatus
    fi
    for f in rate phys_state state link_layer; do
        p="/sys/class/infiniband/$DEV/ports/$PORT/$f"
        [ -r "$p" ] && printf '%-12s %s\n' "$f" "$(cat "$p")"
    done

    # For a port in Ethernet mode (RoCE) the IB `rate` attribute above is not the
    # link speed -- it reports a nominal IB rate that can be off by 4x. The netdev
    # carries the real one, and when the two disagree the netdev is right.
    for n in /sys/class/infiniband/"$DEV"/device/net/*; do
        [ -d "$n" ] || continue
        printf '%-12s %s\n' "netdev" "$(basename "$n")"
        printf '%-12s %s Mb/s  <- authoritative for RoCE\n' "eth speed" \
            "$(cat "$n/speed" 2>/dev/null || echo '?')"
    done

    hr "NUMA -- placement of the NIC against the cores the engine lands on"
    # The library prints host_nodes=1 on this host. If that is wrong, its
    # placement report short-circuits to "single-node" and stops looking, so
    # confirm it from sysfs rather than from UCX.
    printf 'sysfs nodes:  %s\n' "$(ls -d /sys/devices/system/node/node[0-9]* 2>/dev/null | wc -l)"
    printf 'nic numa:     %s\n' "$(cat "/sys/class/infiniband/$DEV/device/numa_node" 2>/dev/null || echo '(unreadable)')"
    printf 'nic pci:      %s\n' "$(basename "$(readlink -f "/sys/class/infiniband/$DEV/device" 2>/dev/null)" 2>/dev/null)"
    printf 'local cpus:   %s\n' "$(cat "/sys/class/infiniband/$DEV/device/local_cpulist" 2>/dev/null || echo '(unreadable)')"
    command -v numactl >/dev/null 2>&1 && numactl --hardware | head -20

    hr "PCIe -- a gen3 x8 slot caps a 100G NIC at about 6.4 GiB/s of payload"
    # sysfs rather than `lspci -vv`, which needs root to report LnkSta and is not
    # installed everywhere. current_* is what the slot actually negotiated;
    # max_* is what the card would accept in a better slot.
    d="/sys/class/infiniband/$DEV/device"
    for f in current_link_speed current_link_width max_link_speed max_link_width; do
        [ -r "$d/$f" ] && printf '%-20s %s\n' "$f" "$(cat "$d/$f")"
    done

    hr "UCX build -- whether its NUMA queries can answer at all"
    # ucs_numa_node_of_device returning UNDEFINED is what prints nic_node=? in
    # the library's placement line, and a UCX built without libnuma returns that
    # for everything while also reporting one configured node.
    command -v ucx_info >/dev/null 2>&1 && ucx_info -v
    command -v ucx_info >/dev/null 2>&1 && ucx_info -b 2>/dev/null | grep -iE 'numa|HAVE_NUMA'
}

# Long options throughout. perftest and ucx_perftest have both moved short
# flags between releases, and a mis-spelled short flag on ucx_perftest exits
# through a segfault rather than a usage error.
#
# QP depth matters as much as size here: -q 1 measures one queue pair with no
# pipelining, which is not the ceiling. This library keeps credit_window frames
# outstanding, so -q 16 is the comparable shape.
QPS=${QPS:-16}

ib_bw() {
    ib_write_bw --ib-dev="$DEV" --ib-port="$PORT" --size="$SIZE" --iters="$ITERS" \
        --qp="$QPS" --report_gbits "$@"
}

ucx_bw() {
    UCX_TLS=rc_verbs,ud_verbs UCX_NET_DEVICES="$DEV:$PORT" \
        ucx_perftest -t tag_bw -s "$SIZE" -n "$ITERS" "$@"
}

server() {
    hr "ib_write_bw -- NIC ceiling, no software in the path"
    ib_bw
    hr "ucx_perftest tag_bw -- same NIC, UCX above it, single reused buffer"
    ucx_bw
}

client() {
    peer=$1
    hr "ib_write_bw -> $peer"
    ib_bw "$peer"
    hr "ucx_perftest tag_bw -> $peer"
    ucx_bw "$peer"
}

case ${1:-describe} in
    describe) describe ;;
    server)   server ;;
    client)   client "${2:?client needs the publisher host}" ;;
    *) printf 'usage: %s describe|server|client <host>\n' "$0" >&2; exit 2 ;;
esac
