#!/bin/sh
# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
#
# SPDX-License-Identifier: LGPL-3.0-or-later
#
# Run both benchmark roles as a local pair across a sweep of frame sizes.
#
# This is the convenience path, for a single machine. The measurement that
# matters is two machines with a NIC between them, which means running the two
# roles by hand -- see benchmarks/README.md. A sweep on one host measures the
# loopback or shared-memory path and is honest only about correctness and about
# the library's own overhead.
#
# Output is meant to be pasted into docs/THROUGHPUT.md together with the machine
# and UCX configuration.

set -u

BENCH=${BENCH:-build/benchmarks/tango-bulk-bench}
SIZES=${SIZES:-"262144 1048576 4194304 8388608"}
ITERS=${ITERS:-400}
WARMUP=${WARMUP:-50}
RING_DEPTH=${RING_DEPTH:-32}
CREDIT_WINDOW=${CREDIT_WINDOW:-16}
TLS=${TLS:-}
EXTRA=${EXTRA:-}

if [ ! -x "$BENCH" ]; then
    echo "sweep: $BENCH not built (configure with -DTANGO_BULK_BUILD_BENCHMARKS=ON)" >&2
    exit 1
fi

TLS_ARG=""
if [ -n "$TLS" ]; then
    TLS_ARG="--tls $TLS"
fi

echo "# $(date -u +%Y-%m-%dT%H:%M:%SZ)  $(uname -srm)"
echo "# ring_depth=$RING_DEPTH credit_window=$CREDIT_WINDOW iters=$ITERS warmup=$WARMUP"
echo "# UCX_TLS=${TLS:-<unset, UCX chooses>} extra='$EXTRA'"
echo

port=$((24000 + $$ % 3000))

for size in $SIZES; do
    port=$((port + 1))
    echo "--- ${size} B ---"

    # shellcheck disable=SC2086
    "$BENCH" --role publisher --port "$port" --size "$size" --iters "$ITERS" \
        --warmup "$WARMUP" --ring-depth "$RING_DEPTH" --credit-window "$CREDIT_WINDOW" \
        --stream bulk.sweep $TLS_ARG $EXTRA > "/tmp/tbsweep.$port" 2>&1 &
    publisher=$!

    i=0
    while [ $i -lt 300 ]; do
        if ss -ltn 2>/dev/null | grep -q ":$port "; then break; fi
        if ! kill -0 "$publisher" 2>/dev/null; then break; fi
        sleep 0.1
        i=$((i + 1))
    done

    # shellcheck disable=SC2086
    "$BENCH" --role subscriber --port "$port" --size "$size" --iters "$ITERS" \
        --warmup "$WARMUP" --ring-depth "$RING_DEPTH" --credit-window "$CREDIT_WINDOW" \
        --stream bulk.sweep $TLS_ARG $EXTRA

    wait "$publisher" || echo "  (publisher exited non-zero)"
    grep -E "^publisher +[0-9]|INCOMPLETE|^ +submitted" "/tmp/tbsweep.$port"
    rm -f "/tmp/tbsweep.$port"
    echo
done
