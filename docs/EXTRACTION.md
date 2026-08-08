<!--
SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project

SPDX-License-Identifier: LGPL-3.0-or-later
-->

# Extraction record

This file records where `tango-bulk` came from, so that provenance is a property of
the repository rather than of someone's memory. It satisfies M0 exit criteria 4 and 5 of
`IMPLEMENTATION_SPEC.md` §9.1.

## 1. Source revision (`REV`)

Everything extracted comes from the cppTango prototype worktree at:

| | |
|---|---|
| Repository | `cppTango` (out-of-tree bulk-stream prototype, uncommitted working tree) |
| `REV` | `c1a3bc4a619379b4e55d563827596d200a9877ff` |
| `git describe` | `10.4.0-dev-520-gc1a3bc4a6` |
| Recorded | 2026-08-08 |

`REV` is the *committed* base. The prototype itself was never committed: it existed as
~2 020 lines of modification across 36 tracked files plus ~6 000 untracked lines. All spec
references of the form `src/...` or `experiments/...` are relative to that worktree at `REV`.

The normative contract is `cppTango/docs/bulk-stream/IMPLEMENTATION_SPEC.md`, with
`DESIGN.md`, `MVP_PLAN.md`, `FINDINGS.md`, `THROUGHPUT.md`, and `M5_TEST_GUIDE.md` as
companions. Where this repository and those documents disagree, the spec wins and this
repository is wrong.

## 2. Extraction map status

From `IMPLEMENTATION_SPEC.md` §8. "Extract" means the logic moves and gets cleaned;
"reimplement" means the contract survives and the code does not.

| Prototype component | Action | Target | Status |
|---|---|---|---|
| `experiments/ucx-bulk-spike/bulk_source.hpp` | extract and clean | `src/ucx/registered_ring.cpp`, `include/tango-bulk/publisher.h` | API shape landed (M0); implementation M2 |
| `src/include/tango/server/BulkSource.h` (in-tree) | extract API shape only | `include/tango-bulk/publisher.h` | done — move-only pimpl `Lease`, non-blocking `try_acquire()`; `DeviceImpl` friendship dropped |
| `src/include/tango/client/FrameView.h` | extract, remove Tango dependency | `include/tango-bulk/frame.h` | done — see §3 |
| `src/include/tango/internal/ucx/UcxWorker.h` | extract behind the engine | `src/ucx/context.cpp`, `src/ucx/engine.cpp` | M2 |
| `src/ucx/BulkStreamSupplier.cpp` (`SeqWindow`) | extract, reshape to a bitmap | `src/core/credit_window.cpp` | M1 |
| geometry epochs (`rearm_stream`, `draining_generation_`) | extract | `src/core/geometry.cpp`, `src/ucx/*_impl.cpp` | M1 (validation), M3+ (epoch machinery) |
| `src/include/tango/internal/ucx/bulk_wire.h` | redesign as a standalone protocol | `include/tango-bulk/protocol.h`, `src/core/protocol.cpp` | M1 |
| `src/include/tango/internal/server/BulkStreamManager.h` | reimplement as a session manager | `src/core/session_manager.cpp`, `src/ucx/publisher_impl.cpp` | M3 |
| Tango event integration (`EventData::bulk_frame`, `ZmqEvent*` hooks, `DeviceProxy` overloads) | **retire** | — | not extracted, not referenced |
| `src/idl_bulk/`, `BulkStreamCorrelator`, `BulkTopicCutover`, `bulk_topics.h`, `bulk_negotiation.*` | **retire** | — | not extracted, not referenced |
| `TANGO_USE_UCX`, `configure/ucx.cmake` | **retire** | — | not extracted; cppTango gains no build option |
| `tests/catch2_unit_bulk_*.cpp` (12 files) | mine for cases, do not port | `tests/unit/`, `tests/ucx/` | M1/M2 |
| `experiments/ucx-bulk-spike/*` | extract as the benchmark | `benchmarks/` | M2+; the spike keeps running unchanged on hardware in parallel |

Two things the map deliberately does not do: it does not preserve the prototype's
`AttributeValue_5` metadata derivation (metadata now comes from `FrameMetadata`), and it
does not preserve wire compatibility with anything. The prototype protocol was never
deployed.

## 3. Per-file provenance

Files with a prototype ancestor. LGPL-3.0-or-later notices are preserved on every file in
this repository, extracted or not.

| This repository | Derived from (`REV`) | What changed |
|---|---|---|
| `include/tango-bulk/frame.h` (`FrameView`) | `src/include/tango/client/FrameView.h` | Namespace `Tango` → `TangoBulk`; `EventData` relationship removed; `TANGO_USE_UCX` stub comment removed; added `generation()` and `endian()`; `send_time_us` → `timestamp_ns`; `dropped_before` widened to `u64`; `unsigned char` → `std::byte` |
| `include/tango-bulk/publisher.h` (`BulkSource`, `Lease`) | `src/include/tango/server/BulkSource.h`, `experiments/ucx-bulk-spike/bulk_source.hpp` | Kept the move-only pimpl `Lease` and non-blocking `try_acquire()`; dropped the blocking `acquire()` (§5.3 forbids blocking on the data path), the `Slot*` raw-pointer API, `std::vector<bool>`, and the `DeviceImpl` friendship |
| `cmake/FindUCX.cmake` | `experiments/ucx-bulk-spike/CMakeLists.txt` | Same pkg-config-then-manual-search strategy, promoted to a find module with an imported target and a version from `ucp_version.h` |

Files with no prototype ancestor (new in this repository): `include/tango-bulk/errors.h`,
`counters.h`, `limits.h`, `subscriber.h`, `tango.h`; `src/core/enums.cpp`, `errors.cpp`;
`src/ucx/ucx_support.*`; `src/tango/registration.cpp`, `tango_support.*`; the build system;
`scripts/check_layering.py`.

## 4. Deviations from IMPLEMENTATION_SPEC.md

Recorded rather than silently taken. None changes a signature in §2.

1. **`include/tango-bulk/limits.h` added.** §1 does not list it, but §6.1's hard caps are
   needed by both the wire decoders (rejecting an oversize grant from a peer) and the config
   validators (rejecting an oversize request from the application). Neither should have to
   include the other, so the constants live in their own header.

2. **`subscriber.h` forward-declares `Tango::DeviceProxy` instead of including `tango/*`.**
   §2.4 gives `BulkSubscriber` a `Tango::DeviceProxy &` constructor parameter, while §1
   places `subscriber_impl.cpp` in `src/ucx/`, which §1.1 forbids from seeing `tango/*`.
   A forward declaration is sufficient for a reference parameter and is what makes both
   requirements simultaneously satisfiable.

   The consequence, to be settled when the subscriber is implemented (M3/M4): `BulkSubscriber`'s
   own member functions compile in the Tango layer, the transport engine lives in the UCX
   layer, and they meet at an abstract interface declared in `src/core/`. The alternative —
   putting `BulkSubscriber` in the UCX layer — would require that layer to see `DeviceProxy`,
   which the dependency table forbids.

3. **`FrameView::Fields` is public, not private.** §2.2 shows it private with
   `detail::ReceiveSlotLease` as the only friend. It is public here because the receive path
   populates one per slot and would otherwise need a friend declaration per implementation
   file. It remains an implementation detail by documentation; no accessor exposes it.

4. **Internal headers inside `src/tango/` are included by bare name**, not as
   `<tango/...>`. With `src/` on the include path, `<tango/...>` is ambiguous between this
   layer's directory and cppTango's installed headers. `src/core/` and `src/ucx/` have no
   such collision and use `<core/...>` and `<ucx/...>`.

5. **`install_bulk_commands()`, `attach_publisher()` and `detach_publisher()` are declared
   but not defined** until M4. Declaring them keeps §7.2's normative API visible; leaving
   them undefined means an attempt to use them fails at link time rather than at runtime in
   a device server. They are deliberately not stubbed to throw.

## 5. M0 exit criteria

| Criterion | Status | Evidence |
|---|---|---|
| 1. Configures against an **installed** `find_package(Tango)`; configure fails if it resolves into a source or build tree | met | `cmake/TangoBulkLayering.cmake`; tests `installed-tango-guard-{clean,build_tree,source_tree}` |
| 2. Four library targets with §1.1 dependency rules enforced by a CI include-grep | met | `tango-bulk-core`, `-ucx`, `-tango`, `tango-bulk`; `scripts/check_layering.py`; tests `layering`, `layering-detects-violations` |
| 3. An empty Catch2 executable runs | met | three executables, one per layer; `ctest` green |
| 4. `REV` recorded together with the extraction map | met | §1 and §2 of this file |
| 5. LGPL notices preserved on every extracted file; provenance recorded per file | met | §3 of this file; `LICENSE` |

Both enforcement mechanisms have negative controls, because a guard that has never been
observed to fire is not a guard. `installed-tango-guard-build_tree` asserts on the directory
named in the rejection, not merely on a non-zero exit, so it cannot pass by failing for an
unrelated reason.

## 6. What M0 deliberately does not contain

No behaviour. No protocol encoder, no UCX engine, no session manager, no Tango commands.
The milestone is about the boundary being real, and the boundary is now checked three ways:
the include-grep, the installed-Tango guard, and the fact that `tango-bulk-tango` links and
tests without ever compiling a UCX header.
