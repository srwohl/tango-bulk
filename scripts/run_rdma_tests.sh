#!/bin/sh
# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
#
# SPDX-License-Identifier: LGPL-3.0-or-later
#
# Run the UCX test suite over a real verbs transport.
#
# Every other configuration in this repository runs over shared memory or CMA,
# where a send completes inside the call and the transport never has anything
# outstanding. rc_verbs is the first one where a send genuinely stays posted to a
# queue pair, and that difference alone found a use-after-free in publisher
# teardown that -Werror, ASan, TSan and a 20-run soak all passed over
# (docs/EXTRACTION.md deviation 22). That is the argument for this being a
# routine target rather than something someone remembers to try.
#
# Exits 0 with a message when no verbs device is present, so it is safe in CI
# before the hardware is. To provide one without hardware:
#
#     sudo modprobe rdma_rxe
#     sudo rdma link add rxe0 type rxe netdev <iface>
#
# Soft-RoCE is correct but slow: use it for semantics, never for throughput.

set -u

BINARY=${1:-build/tests/tango-bulk-ucx-tests}

if [ ! -x "$BINARY" ]; then
    echo "run_rdma_tests: $BINARY not built" >&2
    exit 1
fi

if [ ! -d /sys/class/infiniband ] || [ -z "$(ls -A /sys/class/infiniband 2>/dev/null)" ]; then
    echo "run_rdma_tests: SKIP -- no verbs device (see the header of this script)"
    exit 0
fi

echo "run_rdma_tests: devices: $(ls /sys/class/infiniband | tr '\n' ' ')"

# ud_verbs is not optional. rc_verbs is connection-oriented ("connection: to ep")
# and has no way to complete the address-based wireup handshake by itself; alone
# it fails with "no auxiliary transport".
UCX_TLS=rc_verbs,ud_verbs
export UCX_TLS
echo "run_rdma_tests: UCX_TLS=$UCX_TLS"

"$BINARY"
