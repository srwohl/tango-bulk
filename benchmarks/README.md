<!--
SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project

SPDX-License-Identifier: LGPL-3.0-or-later
-->

# benchmarks

`tango-bulk-bench` moves frames between two processes using the same
`BulkPublisher` and `SubscriberEngine` a device server links. Nothing in the data
path is reimplemented for the benchmark's convenience, which is the property that
makes a number from it worth recording.

This replaces `experiments/ucx-bulk-spike/` from the cppTango prototype tree. The
spike hand-rolled a registered ring, a negotiation protocol and both data paths,
because when it was written there was no library to measure. There is now.

## Two machines

The measurement that matters. Publisher first — it blocks in `accept()`:

```sh
# on the server
./tango-bulk-bench --role publisher --port 18515 \
    --size 8388608 --iters 2000 --ring-depth 32 --credit-window 16

# on the client
./tango-bulk-bench --role subscriber --host server.example --port 18515 \
    --size 8388608 --iters 2000 --ring-depth 32 --credit-window 16
```

Both roles must agree on `--size`, `--ring-depth`, `--credit-window` and
`--stream`; a mismatch is either a clamped grant or a refused `Open`, both of
which the output names.

## One machine

`pixi run bench` runs both roles locally across a sweep of frame sizes. Useful
for correctness and for the library's own overhead; it measures shared memory or
loopback, so it says nothing about a network.

```sh
pixi run bench
SIZES="1048576 8388608" ITERS=2000 pixi run bench
TLS=rc_verbs,ud_verbs pixi run bench
```

Results, with the machine and UCX configuration that produced them, belong in
[../docs/THROUGHPUT.md](../docs/THROUGHPUT.md).

## Verification, and its negative control

`--verify` recomputes every payload against a pattern keyed on the frame index
and the byte offset, so a frame delivered from the wrong slot, torn across a slot
reuse, or truncated mid-transfer does not accidentally match. It costs real
bandwidth: leave it off for throughput runs and report which mode ran.

`--corrupt-every N` damages one byte of every *N*th frame before publishing. It
exists because a checker that always passes is indistinguishable from one that
works:

```sh
--verify                      # expect: 0 mismatched
--verify --corrupt-every 10   # expect: exactly iters/10 mismatched
```

`FINDINGS.md` in the prototype tree made this a blocking prerequisite for the RMA
path, on the grounds that a missing flush shows up as a *faster* result — the one
failure mode that argues for shipping the broken thing. The same argument applies
to any verification claim, so it is wired in here from the start.

## Coordination

`oob.h` is a length-prefixed blob channel over TCP, standing in for a Tango
command. `handle_coordination()` takes encoded bytes and returns encoded bytes,
which is the whole of §7.1's contract, so this is what the M4 adapter will do over
a `DevVarCharArray`. It is deliberately not a transport: no framing beyond a u32
length, no retries, no concurrency.

## Knobs the spike had and this does not

Absent rather than stubbed, because a flag that silently does nothing is worse
than a missing one.

| Spike flag | Why it is gone |
|---|---|
| `--transport rma` | §3.2's Path B is not implemented in this library |
| `--flush-every` | The K of §9.4's P0-10, and an RMA-only question — see docs/THROUGHPUT.md |
| `--no-register` | §5.4 requires pre-registered slots; unregistered send is not a mode |
| `--progress-thread` | §5.1 settles this by measurement: one submit+progress loop per rail |
| `--err-handling` | §6.3 fixes it to `UCP_ERR_HANDLING_MODE_NONE`; leases provide liveness |
| `--prefix-bytes` | Scatter-gather is not in the MVP data path |

## Notes

- Creating a UCX context takes a couple of seconds on a host with several
  transports to probe. The publisher prints its geometry and then waits; the
  scripts poll for the listening socket rather than sleeping a guessed interval.
- The publisher's lease TTL is set to the §6.1 maximum, because a benchmark never
  renews and an expiry mid-measurement would be noise. Lifecycle behaviour is
  `tests/ucx/test_session_lifecycle.cpp`.
- The clock stops when the last frame is **credited**, not when `publish()`
  accepted it. §5.4 holds the slot until the consumer releases its view, so
  stopping at acceptance would measure how fast the loop can talk to a queue.
- Pinned memory is `ring_depth × size`. At 8 MiB × 32 that is 256 MiB, which a
  default `RLIMIT_MEMLOCK` may refuse — `ulimit -l` before blaming UCX, since
  `ucp_mem_map` failure for this reason returns a status that does not name it.
