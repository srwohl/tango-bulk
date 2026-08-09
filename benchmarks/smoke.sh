#!/bin/sh
# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
#
# SPDX-License-Identifier: LGPL-3.0-or-later
#
# Run both roles of the benchmark against each other on loopback with
# verification on, and fail if either exits non-zero.
#
# Deliberately tiny -- 64 frames of 256 KiB. This asserts that the benchmark
# still runs a session end to end and that every payload arrives intact. It
# asserts nothing about speed: a throughput number from a busy CI machine is not
# a result, and treating it as one is how a benchmark becomes a flaky test.
#
# Note there is no `set -e`. The readiness loop below tests a command that is
# expected to fail repeatedly, and errexit turns that into an early exit that
# strands the publisher in accept() holding the pipe open.

set -u

BENCH=${1:?usage: smoke.sh <path-to-tango-bulk-bench>}

# Derived from the shell's pid so two concurrent ctest runs on one machine do
# not fight over a port.
PORT=$((19000 + $$ % 4000))

COMMON="--size 262144 --iters 64 --warmup 8 --ring-depth 8 --credit-window 4
        --port $PORT --stream bulk.smoke --verify"

PUBLISHER=""
cleanup() {
    if [ -n "$PUBLISHER" ]; then
        kill "$PUBLISHER" 2>/dev/null
    fi
}
trap cleanup EXIT HUP INT TERM

# shellcheck disable=SC2086
"$BENCH" --role publisher $COMMON &
PUBLISHER=$!

# The publisher must reach accept() before the subscriber connects, and creating
# a UCX context takes a couple of seconds on a machine with several transports
# to probe. Wait for the listening socket rather than guessing an interval.
ready=no
i=0
while [ "$i" -lt 300 ]; do
    if ss -ltn 2>/dev/null | grep -q ":$PORT "; then
        ready=yes
        break
    fi
    if ! kill -0 "$PUBLISHER" 2>/dev/null; then
        echo "benchmark smoke: publisher exited before listening" >&2
        wait "$PUBLISHER"
        exit 1
    fi
    sleep 0.1
    i=$((i + 1))
done

if [ "$ready" != yes ]; then
    echo "benchmark smoke: publisher never listened on port $PORT" >&2
    exit 1
fi

# shellcheck disable=SC2086
"$BENCH" --role subscriber $COMMON
SUBSCRIBER_RC=$?

wait "$PUBLISHER"
PUBLISHER_RC=$?
PUBLISHER=""

if [ "$PUBLISHER_RC" -ne 0 ] || [ "$SUBSCRIBER_RC" -ne 0 ]; then
    echo "benchmark smoke failed: publisher=$PUBLISHER_RC subscriber=$SUBSCRIBER_RC" >&2
    exit 1
fi

echo "benchmark smoke OK"
