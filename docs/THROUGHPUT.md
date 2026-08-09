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

**Read the section after this one before quoting any number here.** Over CMA the
transfer is a single-threaded CPU copy, so these figures are bounded by one core's
copy bandwidth and by cache residency — not by anything in this library, and not
in a way that predicts RDMA.

`--fill once`, so no payload generation is on the critical path. Median of three
runs; a laptop under sustained load drifts downward as the package heats, so the
spread is reported rather than hidden.

| Frame | Ring set / side | Median | Best | Worst |
|---|---|---|---|---|
| 256 KiB | 8 MiB | 5.90 GiB/s | 7.25 | 5.19 |
| 1 MiB | 32 MiB | 5.05 GiB/s | 5.13 | 4.69 |
| 4 MiB | 128 MiB | 5.31 GiB/s | 5.42 | 5.12 |
| 8 MiB | 256 MiB | 5.24 GiB/s | 5.31 | 5.13 |

```text
Date            2026-08-09
Host            Linux 7.0.0-28-generic x86_64, Intel Core Ultra 5 135H (14C/18T, 18 MB L3)
UCX             1.22.0 (conda-forge)
UCX_TLS         unset -- UCX selected self/sysv/posix/cma
Geometry        ring_depth 32, credit_window 16, iters 300, warmup 50, --fill once
RLIMIT_MEMLOCK  1 975 696 KiB (~1.9 GiB), above the 256 MiB pinned at 8 MiB × 32
Build           RelWithDebInfo, -Werror
```

`credit-stalled` aside (a property of the benchmark's pacing, see below), no
dropped frames of any kind and no transport errors at any size.

Two results that do hold regardless of transport:

- **Staged-copy bytes are zero at every size.** `bytes_copied()` counts payload
  that went through an eager staging copy, so a rendezvous-sized frame must leave
  it at zero. This is §9.3's zero-copy criterion asserted on a two-process session
  rather than in a unit test.
- **Per-frame overhead is negligible across a 32× size range.** The rate is
  effectively flat from 256 KiB to 8 MiB, which means the credit round trip, the
  header encode, the session-table walk and the engine loop cost nothing
  measurable per frame. The window is not the limiter either: throughput is flat
  from `credit_window` 16 through 128.

### The number is bounded by cache residency, not by this library

Raw output: [`../benchmarks/sweeps/2026-08-09-ring-depth-cache.txt`](../benchmarks/sweeps/2026-08-09-ring-depth-cache.txt)

At a fixed 1 MiB frame, throughput decays monotonically with ring depth, and the
knee is exactly where the two rings stop fitting in this host's 18 MB L3. Five
repetitions per point:

| Ring depth | Working set / side | Median | Best | Worst |
|---|---|---|---|---|
| 2 | 2 MiB | 10.91 GiB/s | 13.40 | 8.63 |
| 4 | 4 MiB | 11.50 GiB/s | 12.15 | 10.66 |
| 8 | 8 MiB | 8.18 GiB/s | 8.90 | 5.17 |
| 16 | 16 MiB | 4.10 GiB/s | 6.11 | 3.27 |
| 32 | 32 MiB | 2.95 GiB/s | 3.10 | 2.70 |
| 64 | 64 MiB | 3.08 GiB/s | 3.28 | 2.86 |

`process_vm_readv` moves every payload byte twice — one read, one write — on a
single thread, so the ceiling is one core's copy bandwidth against whichever level
of the hierarchy the data lives in:

```text
DRAM-resident    3 GiB/s payload =  6.4 GB/s traffic   single-core memcpy range
cache-resident  11 GiB/s payload = 23.6 GB/s traffic   L3 bandwidth range
```

Both ends land where a single-threaded copy should. Nothing is unexplained.

### Why `ucx_perftest` looks three times faster

On this host, same transport:

| | `ucx_perftest -t tag_bw` | this library, ring 32 |
|---|---|---|
| 1 MiB | 19 347 MB/s = **18.0 GiB/s** | 5.05 GiB/s |
| 8 MiB | 11 258 MB/s = **10.5 GiB/s** | 5.24 GiB/s |

`ucx_perftest` reuses a **single** buffer, so its working set is cache-resident
and it reports the top of the curve above. At ring depth 2 — a comparable working
set — this library reaches 10.9 GiB/s median. Comparing perftest against a
realistic pinned ring is a category error, and the 8 MiB row shows perftest
falling toward the same place once its own working set reaches L3.

A deep ring is not overhead to be optimised away: it is what lets a detector
absorb a burst without dropping frames, and it is the reason §6.1 defaults
`ring_depth` to 32.

### Ruled out, each by measurement rather than argument

| Hypothesis | Test | Result |
|---|---|---|
| Credit window too small to hide the round trip | window 16 → 32 → 64 → 128 | flat, 4.9–5.7 GiB/s |
| `poll()`'s 50 µs idle sleep stalls the consumer | replaced with `yield()` | no change |
| §6.2 slot-stride cache-set aliasing | power-of-two sizes vs +4 KiB | no effect at 4 or 8 MiB |
| Benchmark's own payload generation | timed separately, then `--fill once` | **was** 54% of the measurement; fixed |

The last one was real: `fill_pattern` began as a scalar byte loop at 5.9 GiB/s,
so the first committed figures were mostly a statement about the harness. The
flatness across frame size was the clue — a transport-bound curve rises with size
as fixed overhead amortises.

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
progress. `--fill once` makes it more pronounced still, because the loop then has
no per-frame work at all to slow it down.

## Not measured here

- **RDMA hardware.** Every figure above is either shared memory or software
  emulation. Nothing on this page is evidence about a real NIC, and no scaling
  claim should be made from it.
- **§9.4 P0-10, flush amortization.** The K sweep asks what a `ucp_ep_flush_nbx`
  costs when amortized over K frames, which is a question about §3.2's Path B —
  the RMA path this library does not implement. It cannot be answered by this
  benchmark at any transport, and §10 still lists the §5.4 ownership contract
  among the things it could move. See `docs/EXTRACTION.md` §8.

  One thing the sweeps above *do* bear on it: credit-based release shows no
  measurable per-frame cost, and widening `credit_window` from 16 to 128 changes
  nothing. So if batched flush later turns out to be required, the sizing headroom
  to absorb it appears to be there. That is an encouraging sign, not a result.
- **Two rails, CPU affinity, NUMA placement** (P0-11) and **`ucp_worker_fence`
  versus flush ordering** (P0-12). Both need hardware.
- **Latency distribution.** The benchmark reports aggregate rate only. A
  percentile-level answer needs per-frame timestamps and a different harness.
