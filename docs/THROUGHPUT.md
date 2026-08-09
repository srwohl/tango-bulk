<!--
SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project

SPDX-License-Identifier: LGPL-3.0-or-later
-->

# Throughput results

Numbers produced by `benchmarks/` in this repository, against the same
`BulkPublisher` and `SubscriberEngine` a device server links. Nothing here is
measured through a benchmark-only reimplementation of the data path, which is the
one property that makes a number worth recording.

The prototype's `THROUGHPUT.md` was deliberately **not** carried over. Its
figures came from `experiments/ucx-bulk-spike/`, which hand-rolled its own ring
and its own protocol, so they describe code this repository does not contain.

## How to reproduce

```sh
pixi run bench                       # local pair, default sweep
SIZES="1048576" ITERS=2000 pixi run bench
TLS=rc_verbs,ud_verbs pixi run bench # pin the transport
```

Every row must record the machine, the UCX version, `UCX_TLS`, and the ring and
window geometry. A GiB/s figure without those is not a result.

Two roles on two machines is the measurement that matters; `pixi run bench` runs
both roles on one host and therefore measures a shared-memory or loopback path.
See [../benchmarks/README.md](../benchmarks/README.md) for the two-machine
invocation.

## Single host, shared memory / CMA

The library's own overhead and the correctness of the zero-copy path. **Not** a
network result: both processes are on one machine, so UCX selects `cma` and the
payload moves by `process_vm_readv`. Treat these as a floor on what the library
costs, not as a transport measurement.

Raw output: [`../benchmarks/sweeps/2026-08-09-shm-cma.txt`](../benchmarks/sweeps/2026-08-09-shm-cma.txt)

| Frame | Frames | Subscriber | Publisher | Credit coalescing | Staged-copy bytes |
|---|---|---|---|---|---|
| 256 KiB | 400 | 3.285 GiB/s | 3.225 GiB/s | 1.2× | 0 |
| 1 MiB | 400 | 3.142 GiB/s | 3.129 GiB/s | 1.0× | 0 |
| 4 MiB | 400 | 3.123 GiB/s | 3.116 GiB/s | 1.0× | 0 |
| 8 MiB | 400 | 3.277 GiB/s | 3.269 GiB/s | 1.0× | 0 |

```text
Date            2026-08-09
Host            Linux 7.0.0-28-generic x86_64, Intel Core Ultra 5 135H
UCX             1.22.0 (conda-forge)
UCX_TLS         unset -- UCX selected self/sysv/posix/cma
Geometry        ring_depth 32, credit_window 16, iters 400, warmup 50
RLIMIT_MEMLOCK  1 975 696 KiB (~1.9 GiB), above the 256 MiB pinned at 8 MiB × 32
Build           RelWithDebInfo, -Werror
```

`credit-stalled 0`, `queue-full 0`, `transport-errors 0` and no dropped frames of
any kind at every size.

Two things worth reading off this table rather than the headline number:

- **Staged-copy bytes are zero at every size.** `bytes_copied()` counts payload
  that went through an eager staging copy, so a rendezvous-sized frame must leave
  it at zero. This is §9.3's zero-copy criterion asserted on a two-process
  session rather than in a unit test.
- **Coalescing is 1.0× above 256 KiB**, and only rises when frames get small
  enough that several are released between progress iterations. §3.12's
  cumulative credit is what makes that ratio free when it happens; it is not a
  tuning knob and a value of 1.0 is not a problem.

## Single host, `rc_verbs` over Soft-RoCE

A **correctness** configuration, never a performance one. `rdma_rxe` is kernel
software doing the copies a NIC would do, so its timings describe the emulation.
It earns its place by being the only local configuration where a send genuinely
stays posted to a queue pair — which is what found deviation 22.

Raw output: [`../benchmarks/sweeps/2026-08-09-rc_verbs-soft-roce.txt`](../benchmarks/sweeps/2026-08-09-rc_verbs-soft-roce.txt)

| Frame | Frames | Subscriber | Publisher | Verified | Staged-copy bytes |
|---|---|---|---|---|---|
| 256 KiB | 200 | 0.847 GiB/s | 0.831 GiB/s | 220, 0 mismatched | 0 |

```text
UCX_TLS         rc_verbs,ud_verbs
Device          rxe0 -- Soft-RoCE (rdma_rxe) over wlp0s20f3, not RDMA hardware
Geometry        ring_depth 16, credit_window 8, --verify
```

Negative control, same configuration with `--corrupt-every 10`: **22 of 220
frames reported as mismatched**, which is exactly the number damaged. A verifier
that has never been observed to fail is not a verifier.

One thing to read off this run rather than ignore: `credit-stalled 145` against
220 frames submitted, where the CMA sweep stalls zero times. The benchmark waits
on the `credits_outstanding` gauge before filling a slot, and that gauge is
refreshed by the engine — so on a transport this much slower it is stale more
often, and `publish()` refuses a frame the loop thought it had room for. Nothing
is lost (the frame was never assigned a sequence, and the next pass re-sends the
same index; 220 of 220 were submitted and credited), but each refusal wastes a
`memset` of the slot. It is a property of the benchmark's pacing, not of the
library, and it is the reason the loop retries rather than counting a refusal as
progress.

## Not measured here

- **RDMA hardware.** Every figure above is either shared memory or software
  emulation. Nothing on this page is evidence about a real NIC, and no scaling
  claim should be made from it.
- **§9.4 P0-10, flush amortization.** The K sweep asks what a `ucp_ep_flush_nbx`
  costs when amortized over K frames, which is a question about §3.2's Path B —
  the RMA path this library does not implement. It cannot be answered by this
  benchmark at any transport, and §10 still lists the §5.4 ownership contract
  among the things it could move. See `docs/EXTRACTION.md` §8.
- **Two rails, CPU affinity, NUMA placement** (P0-11) and **`ucp_worker_fence`
  versus flush ordering** (P0-12). Both need hardware.
- **Latency distribution.** The benchmark reports aggregate rate only. A
  percentile-level answer needs per-frame timestamps and a different harness.
