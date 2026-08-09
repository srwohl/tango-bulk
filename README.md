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

> **Status: M2 — the minimal vertical slice.** Bytes now move over UCX. A publisher and a
> subscriber in one process carry a frame end to end — registered producer lease → engine
> thread → self-contained frame → registered consumer slot → `FrameView` → credit on release
> — with the payload landing in the registered receive ring without a copy. All eight of
> §9.3's exit criteria pass, under a normal `-Werror` build and under
> `-fsanitize=address,undefined`.
>
> Deliberately not here yet: no Tango integration and no session lifecycle. No commands, no
> `DeviceProxy`, no session manager, lease timers, expiry, geometry epochs, probe, reconnect,
> relay, RMA, or dispatch thread — `DeliveryMode::Manual` and `poll()` only. `BulkSubscriber`
> is still a declaration; the transport engine behind it is `detail::SubscriberEngine`.
> Session leases land in M3, the stock-Tango command adapter in M4. See
> [docs/EXTRACTION.md](docs/EXTRACTION.md) for exactly what exists, which deviations from the
> spec were taken deliberately, and what each test is worth.

## Specification

The normative contract is `IMPLEMENTATION_SPEC.md` in the cppTango repository under
`docs/bulk-stream/`: exact API, byte-level wire protocol, state machines, threading model,
resource limits, and the extraction map. It supersedes `DESIGN.md` and `MVP_PLAN.md` on any
point of detail. Where this repository disagrees with the spec, this repository is wrong.

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
include/tango-bulk/     public API; the only headers a consumer sees
src/core/               protocol, credit arithmetic, geometry.  libstdc++ only
src/ucx/                the engine and registered memory.  ucp/* allowed
src/tango/              commands and DeviceProxy glue.  tango/* allowed
tests/unit/             core only; no UCX device, no Tango database
tests/ucx/              loopback UCX; no Tango process
tests/tango/            stock-cppTango device fixture
scripts/check_layering.py
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
pixi run test        # 110 cases
pixi run test-asan   # the same, under -fsanitize=address,undefined
```

Run both. The truncation and bit-flip sweeps in `tests/unit/test_protocol_malformed.cpp`
decode every message at every length from 0 upward and with every single bit of the fixed
part flipped; their assertion is that no input walks a decoder off the end of a buffer, and
no return value can express that. Without a sanitizer they are mostly wasted runtime.

`tests/unit/golden_vectors.h` is generated. A diff of it is a protocol change and should be
reviewed as one; `tests/unit/test_protocol_golden.cpp` documents how to regenerate it.

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
