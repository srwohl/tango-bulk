<!--
SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project

SPDX-License-Identifier: LGPL-3.0-or-later
-->

# tango-bulk

A UCX bulk-data plane for Tango device servers, built as an out-of-tree extension against a
**stock, unmodified** cppTango installation. Tango stays the control plane; UCX carries the
frames.

A device server opts in by linking a library and registering four ordinary Tango commands.
No cppTango ABI, event implementation, public class, IDL, build option, or upstream source
file changes.

> **Status: M3 — session lifecycle and fault containment.** Bytes move over UCX and sessions
> now have a lifetime. A bounded table of clients each holds an expiring lease with its own
> credit window and sequence space; a frame fans out to all of them and its producer slot
> comes back only when every one has credited it. `Probe`/`ProbeAck` arms a session before it
> can be sent to, `Renew` extends it, `Close` is idempotent, and a client that simply vanishes
> has its pinned slots reclaimed on the lease deadline — with the stream still serving
> everybody else, and a restarted client able to open a new session without restarting the
> device server. The M2 vertical slice passes unchanged on top of all of it. Green under a
> normal `-Werror` build, under `-fsanitize=address,undefined`, and under
> `-fsanitize=thread`.
>
> Deliberately not here yet: no Tango integration. No commands, no `DeviceProxy`, no
> `BulkQuery`, geometry epochs, reconnect, relay, RMA, dispatch thread, or renew timer —
> `DeliveryMode::Manual` and `poll()` only, and the coordination plane is driven as encoded
> bytes by the caller. `BulkSubscriber` is still a declaration; the transport engine behind it
> is `detail::SubscriberEngine`. The stock-Tango command adapter is M4. See
> [docs/EXTRACTION.md](docs/EXTRACTION.md) for exactly what exists, which deviations from the
> spec were taken deliberately, and what each test is worth.
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
tests/tango/            stock-cppTango device fixture
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
pixi run test        # 127 cases
pixi run test-asan   # the same, under -fsanitize=address,undefined
```

Run both. The truncation and bit-flip sweeps in `tests/unit/test_protocol_malformed.cpp`
decode every message at every length from 0 upward and with every single bit of the fixed
part flipped; their assertion is that no input walks a decoder off the end of a buffer, and
no return value can express that. Without a sanitizer they are mostly wasted runtime.

`tests/unit/golden_vectors.h` is generated. A diff of it is a protocol change and should be
reviewed as one; `tests/unit/test_protocol_golden.cpp` documents how to regenerate it.

The session table is worth a race detector too, since every transition in the server state
machine is a handshake between a Tango command thread and the engine thread. There is no pixi
task because it needs ASLR disabled on recent kernels:

```sh
cmake -S . -B build-tsan -GNinja -DBUILD_TESTING=ON -DTANGO_BULK_SANITIZERS=thread
cmake --build build-tsan --target tango-bulk-ucx-tests
setarch -R ./build-tsan/tests/tango-bulk-ucx-tests
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

## Design in one paragraph

A publisher owns a registered ring of frame slots. The producer takes a slot with a
non-blocking `try_acquire()`, fills it, and publishes it with self-contained metadata — no
`AttributeValue_5`, no CDR, nothing that requires a Tango round trip to interpret. A
dedicated engine thread owns the `ucp_worker` exclusively and does one tight
submit-and-progress loop; user callbacks never run on it. The subscriber receives into its own
registered ring and hands the application a reference-counted `FrameView` whose destruction
*is* the credit return, so slow consumers apply backpressure by construction rather than by
convention. Session leases over ordinary Tango commands (`BulkOpen` / `BulkRenew` /
`BulkClose` / `BulkQuery`) are the cleanup authority: a client that crashes releases pinned
memory when its lease expires, which is something Tango's server→client ZMQ heartbeat cannot
tell the supplier.

## Licence

LGPL-3.0-or-later. See [LICENSE](LICENSE).
