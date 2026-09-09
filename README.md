<!--
SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project

SPDX-License-Identifier: LGPL-3.0-or-later
-->

# tango-bulk

A UCX bulk-data plane for Tango device servers, built as an out-of-tree extension against a
**stock, unmodified** cppTango installation. Tango stays the control plane; UCX carries the
frames.

A device server opts in by linking a library and registering three ordinary Tango commands.
No cppTango ABI, event implementation, public class, IDL, build option, or upstream source
file changes.

> **Status: M4 — the stock-Tango adapter.** It is a Tango extension now, not a library with a
> Tango-shaped plan. `BulkOpen` / `BulkRenew` / `BulkClose` are ordinary commands
> on an ordinary device class; `TangoBulk::subscribe()` takes a stock `Tango::DeviceProxy`, opens a
> session over it, renews on a timer, reconnects with backoff, and delivers frames on a
> library-owned dispatch thread that never touches Tango or UCX. A device server integrates in
> three lines and links one target.
>
> The tests are the claim: `tests/tango/` starts a **real device server in a separate process**
> with `-nodb` and drives it through a real `DeviceProxy`. Green under a normal `-Werror`
> build, under `-fsanitize=address,undefined`, and under `-fsanitize=thread`.
>
> Deliberately not here yet: geometry epochs (§4.3), the relay, RMA, GPU memory, and M5's
> external acceptance matrix — including the event-count proof that a bulk-only run emits no
> full-rate Tango event, which the preview example demonstrates structurally but nothing yet
> counts. See [docs/EXTRACTION.md](docs/EXTRACTION.md) for exactly what exists, which
> deviations from the spec were taken deliberately, and what each test is worth.
>
> The repository is self-contained: the normative spec, the plan, the benchmark and the
> results all live here, and no cppTango source tree is needed to build, test or measure it.

## Specification

The normative contract is [docs/IMPLEMENTATION_SPEC.md](docs/IMPLEMENTATION_SPEC.md): exact
API, byte-level wire protocol, state machines, threading model, resource limits, and the
extraction map. [docs/MVP_PLAN.md](docs/MVP_PLAN.md) says in what order to build it. The spec
supersedes the plan on any point of detail.

Both were vendored from the cppTango bulk-stream prototype and are **canonical here** from
that point on; the `REV` they came from is recorded in their headers and in
[docs/EXTRACTION.md](docs/EXTRACTION.md). This repository owns the contract, so a change to
the spec is a deliberate commit to be reviewed as a contract change — not, as it was through
M3, an error in this repository to be corrected. Nothing in this repository requires a
cppTango source tree to be present.

## Building

```sh
pixi install          # cppTango, UCX >= 1.21, Catch2 >= 3.1.1, toolchain
pixi run build
pixi run test
```

Or against your own toolchain:

```sh
cmake -S . -B build -GNinja -DCMAKE_PREFIX_PATH=/path/to/prefixes
cmake --build build
ctest --test-dir build --output-on-failure
```

Linux only, deliberately. Configuration fails on other platforms rather than emitting stub
APIs that would compile and never carry data.

### cppTango must be installed

`find_package(Tango)` runs in config mode and configuration **fails** if the package resolves
inside a cppTango source or build tree. That check is the mechanical form of "out of tree":
without it, the property survives only as long as everyone remembers it in review. The pixi
environment satisfies it by taking cppTango from conda-forge.

If you need to develop against an unreleased cppTango, install it to a prefix first and point
`CMAKE_PREFIX_PATH` at the prefix.

## Layout

```text
docs/                   the normative spec, the plan, results, provenance
include/tango-bulk/     public API; the only headers a consumer sees
src/core/               protocol, credit arithmetic, geometry.  libstdc++ only
src/ucx/                the engine and registered memory.  ucp/* allowed
src/tango/              commands and DeviceProxy glue.  tango/* allowed
tests/unit/             core only; no UCX device, no Tango database
tests/ucx/              loopback UCX; no Tango process
tests/tango/            a real -nodb device server, driven through a DeviceProxy
examples/               device, client, and the decimated preview attribute
benchmarks/             two-process throughput and payload verification
scripts/                layering check, verbs test runner
```

### The layering is enforced, not advisory

| Target | May include | MUST NOT include |
|---|---|---|
| `tango-bulk-core` | libstdc++ only | `ucp/*`, `tango/*` |
| `tango-bulk-ucx` | core, `ucp/*` | `tango/*` |
| `tango-bulk-tango` | core, `tango/*` | `ucp/*` |
| `tango-bulk` (umbrella) | all three | — |

The third row is the load-bearing one. The Tango adapter deals in encoded byte vectors and
opaque handles, so a device server that links it never inherits UCX headers.

It survives M4 intact, which was the interesting question: `Subscription` is the client-facing
object and its Tango adapter lives in the Tango
layer and has to *construct* a UCX transport. It does so through an abstract interface declared
in `src/core/` whose factory is defined in the UCX library, and `tango-bulk-tango` links
`tango-bulk::ucx` **PRIVATE** — so the symbols resolve and no include directory travels. The
fixture device server and all three examples link `tango-bulk::tango` alone, which is what
keeps that a tested property rather than a claim.

`scripts/check_layering.py` greps each layer's translation units and fails on violation. It
runs as a test (`ctest -R layering`) and is meant to run in CI as a standalone step, with no
configured build tree required:

```sh
python3 scripts/check_layering.py
```

Both it and the installed-Tango guard have negative controls in the test suite. A check that
has never been observed to fail is not a check.

## Testing

```sh
pixi run test        # 143 cases
pixi run test-asan   # the same, under -fsanitize=address,undefined
```

Run both. The truncation and bit-flip sweeps in `tests/unit/test_protocol_malformed.cpp`
decode every message at every length from 0 upward and with every single bit of the fixed
part flipped; their assertion is that no input walks a decoder off the end of a buffer, and
no return value can express that. Without a sanitizer they are mostly wasted runtime.

`tests/unit/golden_vectors.h` is generated. A diff of it is a protocol change and should be
reviewed as one; `tests/unit/test_protocol_golden.cpp` documents how to regenerate it.

`tests/tango/` needs no Tango database: it starts the fixture device server with `-nodb` on a
port the kernel picks, waits for it to report readiness on an inherited descriptor, and drives
it through an ordinary `DeviceProxy`. Nothing to configure, and nothing to clean up if a case
fails.

Both sides are worth a race detector. The server state machine is a handshake between a Tango
command thread and the engine thread; the client has three threads and passes a transport
between them, so the control thread may rebuild the engine while the dispatch thread is inside
`poll()`. There is no pixi task because TSan needs ASLR disabled on recent kernels — including
at *build* time, since `catch_discover_tests` runs the binary to enumerate cases:

```sh
cmake -S . -B build-tsan -GNinja -DBUILD_TESTING=ON -DTANGO_BULK_SANITIZERS=thread
setarch -R cmake --build build-tsan
setarch -R ./build-tsan/tests/tango-bulk-ucx-tests
setarch -R ./build-tsan/tests/tango-bulk-tango-tests
```

### Run it over a verbs transport too

```sh
pixi run test-rdma   # UCX_TLS=rc_verbs,ud_verbs; skips if no device
```

Everything above runs over shared memory or CMA, where a send completes inside the call and
the transport never has anything outstanding. `rc_verbs` is the first configuration where a
send genuinely stays posted to a queue pair, and that difference alone found a use-after-free
in publisher teardown that `-Werror`, ASan, TSan and a 20-run soak all passed over — see
deviation 22. A configuration that finds a bug nothing else can should not depend on someone
remembering to try it.

It needs a verbs device, which does **not** mean it needs RDMA hardware. Soft-RoCE is enough
for the semantics, and any recent kernel has it:

```sh
sudo modprobe rdma_rxe
sudo rdma link add rxe0 type rxe netdev <iface>
```

Correct but slow: use it for correctness and never for throughput. `pixi run ucx-info` shows
what a host actually offers.

## Benchmarks

```sh
pixi run bench   # both roles locally, sweeping frame size
```

`benchmarks/tango-bulk-bench` moves frames between two processes through the same
`BulkPublisher` and `SubscriberEngine` a device server links — nothing in the data path is
reimplemented for the benchmark's convenience, which is what makes a number from it worth
recording. `--verify` checks every payload against a position-dependent pattern, and
`--corrupt-every N` is its negative control, because a checker that always passes is
indistinguishable from one that works.

See [benchmarks/README.md](benchmarks/README.md) for the two-machine invocation and for the
spike flags this deliberately does not have. Results go in
[docs/THROUGHPUT.md](docs/THROUGHPUT.md).

### What the numbers say so far

On one host over shared memory / CMA, at `ring_depth` 32: **~5 GiB/s**, flat from 256 KiB to
8 MiB frames, with **zero staged-copy bytes** at every size and no dropped frames.

Two results there are worth more than the headline figure. Staged-copy bytes staying at zero
is §9.3's zero-copy criterion holding on a real two-process session rather than in a unit
test. And the rate being *flat* across a 32× size range means per-frame overhead — the credit
round trip, the header encode, the session-table walk, the engine loop — costs nothing
measurable; throughput is also flat as `credit_window` goes from 16 to 128, so the window is
not the limiter either.

**The absolute number is bounded by cache residency, not by this library, and it does not
predict RDMA.** Over CMA the transfer is `process_vm_readv` — a single-threaded CPU copy that
moves every payload byte twice. Throughput therefore decays monotonically with ring depth,
with the knee exactly where the two rings stop fitting in this host's 18 MB L3:

| Ring depth | Working set / side | Median (1 MiB frames) |
|---|---|---|
| 2 | 2 MiB | 10.91 GiB/s |
| 8 | 8 MiB | 8.18 GiB/s |
| 16 | 16 MiB | 4.10 GiB/s |
| 32 | 32 MiB | 2.95 GiB/s |

This is also why `ucx_perftest -t tag_bw` reports 18.0 GiB/s at 1 MiB on the same host and
transport: it reuses a **single** buffer, so it measures the cache-resident top of that curve.
At ring depth 2 — comparable working set — this library reaches 10.9 GiB/s. Comparing perftest
against a realistic pinned ring is a category error. A deep ring is not overhead to optimise
away; it is what lets a detector absorb a burst, and it is why §6.1 defaults to 32.

On real hardware the DMA engine reads the publisher's ring and writes the subscriber's ring
without a CPU copy and without occupying either cache, so the whole effect above is an
artefact of the transport. `docs/THROUGHPUT.md` records the arithmetic, the four hypotheses
ruled out by measurement, and the one bug this hunt found in the benchmark itself.

### What is still missing for a real two-machine test

Nothing in the library or the benchmark blocks it — `--role publisher` / `--role subscriber`
with `--host` already works across hosts, and the coordination bytes travel over ordinary TCP.
Two things that could have blocked it turn out not to: `RLIMIT_MEMLOCK` is read at startup and
warned about before UCX can fail opaquely (`src/core/pinned_budget.cpp`), and a `rc_verbs`
worker address is 72 bytes against §3.9's 4096-byte cap, so even a multi-rail host has room.

What you have to supply:

- **Two hosts with RDMA NICs and a link between them** — RoCE or InfiniBand. Without that UCX
  falls back to `tcp` and the result describes TCP, not the design.
- **The coordination port reachable** through any firewall (`--port`, default 18515).
- **`RLIMIT_MEMLOCK` above `ring_depth × max_frame_bytes`** on both hosts — 256 MiB at the
  8 MiB × 32 default. `ulimit -l` before blaming UCX.
- **`UCX_NET_DEVICES`** if a host has more than one NIC, and `--tls` to pin the transport, so
  a run cannot silently select a different path than the one being measured.

What is missing in *this* repository, and what each gap costs:

| Gap | Consequence |
|---|---|
| No remote orchestration — `pixi run bench` is a local pair only | Run the two roles by hand in two shells; there is no ssh runner |
| Each role reports its own numbers | No combined report; collect from both terminals |
| `--iters` only, no `--duration` | Cannot express "stream for 60 s", which is what a hardware soak wants |
| No latency measurement at all | §9.4's **P0-7** (latency) cannot be answered; only aggregate rate is reported |
| No CPU-time reporting | MVP_PLAN's M5 release gate asks for a throughput **and CPU** budget; the CPU half is unmeasured |
| No rail-assignment policy | `engine_cpu_affinity` works and the placement it produces is reported, but **P0-11**'s "one engine per rail" needs a policy §10 defers to M5 with hardware in hand |
| No endpoint-error-mode knob | §6.3 allows `UCP_ERR_HANDLING_MODE_PEER` by configuration; **P0-14** asks whether it changes `rc`/`dc` selection, and there is no way to ask |
| No RMA path (§3.2 Path B) | **P0-10** flush amortization remains unanswerable at any transport — see docs/THROUGHPUT.md |

The first three are benchmark ergonomics and cheap. The last is architectural and the spec
places it past the MVP.

## Using it

A device server. Three lines, and they are the whole integration:

```cpp
void MyClass::command_factory()       { TangoBulk::install_bulk_commands(*this); }
void MyDevice::init_device()          { TangoBulk::attach_publisher(*this, *publisher_); }
void MyDevice::delete_device()        { TangoBulk::detach_publisher(*this); }
```

The acquisition path then never touches Tango:

```cpp
TangoBulk::BulkSource::Lease lease = publisher_->source().try_acquire();
if(lease)                             // never blocks; a full ring means drop, not wait
{
    fill(lease.data());               // the DMA target, filled in place
    publisher_->publish(std::move(lease), meta);
}
```

A client. Construct a Subscription and provide its frame callback:

```cpp
Tango::DeviceProxy proxy("bulk/example/1");   // BORROWED; you keep it alive

TangoBulk::SubscriberConfig config;
config.stream_name = "image";

TangoBulk::SubscriptionCallbacks callbacks;
callbacks.on_frame = [](TangoBulk::FrameView f) { process(f); };
auto subscription = TangoBulk::subscribe(proxy, config, std::move(callbacks));
```

The frame callback runs on a library-owned dispatch thread — never the UCX engine thread, so a
slow consumer cannot stall the transport, and never a Tango thread, so a stuck `command_inout`
cannot stall delivery. Select `DeliveryMode::Pull` and use `read_for()` when the application
should own frame reads.

Working versions of all of the above, including the decimated preview attribute for legacy
visibility, are in [examples/](examples/) with instructions for running them with or without a
Tango database.

## Design in one paragraph

A publisher owns a registered ring of frame slots. The producer takes a slot with a
non-blocking `try_acquire()`, fills it, and publishes it with self-contained metadata — no
`AttributeValue_5`, no CDR, nothing that requires a Tango round trip to interpret. A
dedicated engine thread owns the `ucp_worker` exclusively and does one tight
submit-and-progress loop; user callbacks never run on it. The subscriber receives into its own
registered ring and hands the application a reference-counted `FrameView` whose destruction
*is* the credit return, so slow consumers apply backpressure by construction rather than by
convention. Session leases over ordinary Tango commands (`BulkOpen` / `BulkRenew` /
`BulkClose`) are the cleanup authority. `BulkStreams` supplies a conservative discovery offer;
`OpenReply` and `Subscription::plan()` remain authoritative for the established session.

## Licence

LGPL-3.0-or-later. See [LICENSE](LICENSE).
