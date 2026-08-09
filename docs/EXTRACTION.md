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

### The contract was vendored at M3

`IMPLEMENTATION_SPEC.md` and `MVP_PLAN.md` were copied into `docs/` from
`cppTango/docs/bulk-stream/` at `REV` above and are **canonical here** from that point on.
Their bodies are byte-identical to what was vendored, under a provenance header, so a diff
against the source of record stays meaningful.

The precedence rule inverted when that happened. Through M3 it read "where this repository and
the spec disagree, the spec wins and this repository is wrong". It now reads: this repository
owns the contract, and a change to `docs/IMPLEMENTATION_SPEC.md` is a deliberate commit to be
reviewed as a contract change. The spec still supersedes `MVP_PLAN.md` on any point of detail.

Not vendored, and still in the cppTango tree: `DESIGN.md` (architectural rationale, settled),
`FINDINGS.md` (prototype-era hardware notes) and `M5_TEST_GUIDE.md` (needed at M5, not before).
Links to them from the vendored documents do not resolve here. `THROUGHPUT.md` was deliberately
**not** carried over — its figures came from `experiments/ucx-bulk-spike/`, which hand-rolled
its own ring and protocol, so they describe code this repository does not contain.
`docs/THROUGHPUT.md` is new and holds numbers produced by `benchmarks/` against the shipping
library.

Nothing in this repository requires a cppTango source tree to build, test, or measure.

## 2. Extraction map status

From `IMPLEMENTATION_SPEC.md` §8. "Extract" means the logic moves and gets cleaned;
"reimplement" means the contract survives and the code does not.

| Prototype component | Action | Target | Status |
|---|---|---|---|
| `experiments/ucx-bulk-spike/bulk_source.hpp` | extract and clean | `src/ucx/registered_ring.cpp`, `include/tango-bulk/publisher.h` | **done (M2)** — one `ucp_mem_map`, §6.2 stride padding |
| `src/include/tango/server/BulkSource.h` (in-tree) | extract API shape only | `include/tango-bulk/publisher.h` | done — move-only pimpl `Lease`, non-blocking `try_acquire()`; `DeviceImpl` friendship dropped |
| `src/include/tango/client/FrameView.h` | extract, remove Tango dependency | `include/tango-bulk/frame.h` | done — see §3 |
| `src/include/tango/internal/ucx/UcxWorker.h` | extract behind the engine | `src/ucx/ucx_context.{h,cpp}`, `publisher_impl.cpp`, `subscriber_engine.{h,cpp}` | **done (M2)** — context/worker RAII, `max_am_header` checked at construction |
| `src/ucx/BulkStreamSupplier.cpp` (`SeqWindow`) | extract, reshape to a bitmap | `src/core/credit_window.cpp` | **done (M1)** |
| geometry epochs (`rearm_stream`, `draining_generation_`) | extract | `src/core/geometry.cpp`, `src/ucx/*_impl.cpp` | validation done (M1); §4.3 epoch machinery M5 |
| `src/include/tango/internal/ucx/bulk_wire.h` | redesign as a standalone protocol | `include/tango-bulk/protocol.h`, `src/core/protocol.cpp` | **done (M1)** |
| `src/include/tango/internal/server/BulkStreamManager.h` | reimplement as a session manager | `src/ucx/publisher_impl.cpp` (`Impl::Session`) | **done (M3)** — bounded table, per-session credit window and lease; no separate `src/core/session_manager.cpp` (deviation 17) |
| Tango event integration (`EventData::bulk_frame`, `ZmqEvent*` hooks, `DeviceProxy` overloads) | **retire** | — | not extracted, not referenced |
| `src/idl_bulk/`, `BulkStreamCorrelator`, `BulkTopicCutover`, `bulk_topics.h`, `bulk_negotiation.*` | **retire** | — | not extracted, not referenced |
| `TANGO_USE_UCX`, `configure/ucx.cmake` | **retire** | — | not extracted; cppTango gains no build option |
| `tests/catch2_unit_bulk_*.cpp` (12 files) | mine for cases, do not port | `tests/unit/`, `tests/ucx/` | **done (M1/M2/M3)** — `test_vertical_slice.cpp` holds §9.3's eight criteria, `test_session_lifecycle.cpp` M3's |
| `experiments/ucx-bulk-spike/*` | extract as the benchmark | `benchmarks/` | **done (M3)** — rewritten on the shipping API, not ported (deviation 23) |

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
| `src/core/byte_order.h` | `bulk_wire.h` (`namespace bulk_wire`) | Same byte-at-a-time `put*`/`get*` shape; `unsigned char` → `std::byte`; added `put8`/`get8` and the overflow-checked `mul_overflow`/`add_overflow` the bounds checks need |
| `include/tango-bulk/protocol.h`, `src/core/protocol.cpp` | `bulk_wire.h` | **Redesigned, not ported.** Kept: explicit offsets, little-endian accessors, "never memcpy a struct onto the wire", magic + version + header-size rejection. Changed: 152 → 160 byte frame header; separate AM ids for `Credit` and `ProbeAck`; **`k_probe_seq` deleted**; `header_bytes` trailing-extension rule added; `dropped_before` widened to `u64`; `send_time_us` → `timestamp_ns`; the entire coordination plane is new |
| `src/core/credit_window.cpp` | `BulkStreamSupplier::SeqWindow` | Kept the base-plus-out-of-order-ahead invariant. `std::set<std::uint64_t> credited_ahead` → a fixed `ring_depth`-bit bitmap, so there is no allocation on the data path, and moved to the subscriber, which is the side that knows which views were released |

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

   **M2 executed the first half of that plan.** The transport engine is
   `detail::SubscriberEngine` in `src/ucx/subscriber_engine.{h,cpp}`, and `BulkSubscriber`
   remains a declaration. The M2 tests drive the coordination plane by hand against the
   engine, which is exactly what §9.3 asks for ("no Tango process, no `DeviceProxy`, no
   commands"). `BulkSubscriber` becomes a shell over the engine in M4, when there is a
   `DeviceProxy` to carry coordination.

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

6. **`DropPolicy` is declared in `frame.h`, not `publisher.h`.** §2.3 puts it in
   `publisher.h`, but §3.5 puts it on the wire in `Open`, so `protocol.h` needs it — and
   `protocol.h` including the publisher API would invert the dependency. It sits with the
   other vocabulary types (`ElementType`, `MemoryKind`, `Endian`). No consumer-visible
   change: `publisher.h` and `subscriber.h` both include `frame.h`.

7. **Minor-version trailing tolerance is split by message shape.** §3.1 says a minor
   version may append fields and that a receiver reads `min(header_bytes, its own layout)`
   and ignores the excess; §3.9 says the envelope, fixed body, and variable fields must sum
   to *exactly* the delivered length. For coordination messages these conflict, because only
   the envelope carries `header_bytes` — a body has no size field of its own. Resolved as:

   - Bodies with **no variable tail** (`Renew`, `RenewReply`, `Close`, `CloseReply`,
     `Query`) tolerate trailing bytes, but only when the sender claims a minor above the one
     implemented. At our own minor the length is exact, because there a longer body is a bug.
   - Bodies **with** a variable tail (`Open`, `OpenReply`, `QueryReply`, `Error`) require
     exactness always. Appending a fixed field to one of those would move the tail, so
     reading the tail at its old offset would yield garbage. Refusing fails safe.

   The data plane has no such ambiguity: every message carries its own `header_bytes`, so
   the trailing-extension rule applies directly.

8. **§3.0.2's "every message length is a multiple of 8" is read as applying to fixed
   parts.** Every fixed part defined in §3 is in fact a multiple of 8, and a test asserts it.
   Applying the rule to whole messages would require padding after variable-length fields,
   which would contradict §3.9's rule that trailing unexplained bytes are malformed.

9. **An unrecognised `Status` on the wire is preserved, not rejected.** Refusing to parse a
   reply because its failure code is from a newer minor would convert "something failed,
   code 19" into "the message was garbage". The raw value is kept and `to_string()` reports
   `"Unknown"`. Enum *fields that describe the payload* — `ElementType`, and the geometry
   bounds generally — are the opposite: those are validated strictly, because a receiver
   that cannot name the element type cannot interpret the bytes.

The following arrived with M2.

10. **A session is armed on `Open`, not on `ProbeAck`.** §4.2 says arm-before-send is
    mandatory and that the publisher **MUST NOT** submit a frame to a session in `Open` —
    but §9.3 puts the probe explicitly out of scope, so there is no `ProbeAck` to arm on.
    M2 therefore armed at `Open`, which was a deliberate weakening of a MUST.

    **Closed in M3.** The publisher sends `Probe` when the endpoint is created and arms on
    `ProbeAck`; the subscriber routes `Opening → Probing → Active` as §4.1 draws it. A probe
    is sent once and never retried: a session that is never acknowledged stays in `Open`,
    never eligible for a frame, until its lease expires — the same reclaim path a client that
    died just after `Open` takes, so a retry policy would add a mechanism the lease already
    provides. Under the `UCP_ERR_HANDLING_MODE_NONE` §6.3 mandates, `ProbeAck` is the only
    evidence this side has that the endpoint reaches anyone at all.

11. **`Query` returns `Error{Internal}`; everything else it said is closed in M3.** M2 had
    one implicit session, no `Renew`, and a `Close` that stopped the whole publisher engine —
    a client's clean shutdown permanently killed the device server's stream. M3 replaces all
    of it with the §6.1 session table and the §4.2 per-session `Expiring` transition.

    `Query` remains unimplemented and answers `"not implemented before M4"`, because
    MVP_PLAN.md puts `BulkQuery` with the Tango adapter. The handler exists and gives a
    protocol-legal reply rather than dropping the message.

12. **The consumer has a teardown order, and §4.2 does not give it one.** §4.2 fixes the
    publisher's `Expiring` order and says it MUST NOT be reordered. Nothing states the
    mirror obligation for the subscriber, which is harder: its outstanding work is
    *receives*, and a rendezvous receive is a get issued against the peer's buffer, so it
    sits on an endpoint UCX created internally and that this code can neither name nor
    close. `ucp_worker_destroy` with such an operation still queued is not an error code but
    `ucs_fatal_error` — the process aborts inside UCX.

    `SubscriberEngine::quiesce()` therefore runs, on the engine thread after the loop stops:
    bounded drain of in-flight receives → best-effort `ucp_request_cancel` of the remainder
    → close our own endpoint, bounded then forced → `ucp_worker_flush_nbx`. The bounded
    drain is the load-bearing step; the original code progressed the worker only while the
    *close* request was in progress, which with `UCP_EP_CLOSE_FLAG_FORCE` is barely at all.

    Two consequences worth stating plainly. The cancel is genuinely best-effort: UCX 1.22
    documents `ucp_request_cancel`'s peer-independence for send and tag-receive requests and
    says nothing about an AM receive, and the M2 suite never reaches that path, so it is
    unverified. And if `quiesce()` cannot prove the worker safe, the destructor deliberately
    **leaks** it rather than destroying it — a stranded worker is a far better outcome for a
    device server than a fatal error raised by a client's disappearance. The publisher gained
    the symmetric fix: it now progresses after its force-close to collect the send
    completions that force produced, without which those requests are never freed.

    This is the local half of "either end may hang up abruptly". §6.3 mandates
    `UCP_ERR_HANDLING_MODE_NONE` and says session leases provide liveness, so none of it may
    rely on UCX detecting peer failure — it is all local reclaim. M3 supplies the missing
    half: the lease is what *detects* a peer that stopped answering, and `quiesce()` is what
    makes the resulting teardown survivable.

13. **The lease control block owns its pool.** `PoolAllocator` holds a
    `shared_ptr<LeasePool>` rather than a raw pointer. `allocate_shared` stores a copy of the
    allocator *inside* the control block, so the pool is the storage its own owner lives in;
    and on the receive path the lease also holds the arena alive, and the arena owned the
    pool. Releasing the last `FrameView` therefore ran `_M_dispose()` → destroy lease →
    release arena → destroy pool, and then `_M_release()` carried on reading the control
    block whose ground had just gone. ASan reported it as a heap-use-after-free in
    `_M_release()`; it is invisible without one, which is the argument for §9.3's last
    criterion existing at all.

14. **`publish()` does admission control on the application thread.** It compares
    `admitted - credited` against `credit_window` itself, so it can answer `CreditStalled`
    to its caller synchronously — §2.3 requires that answer from `publish()`, and the engine
    thread is not there to give it. `CreditWindow` in `src/core/` remains the authority for
    what is actually on the wire; the app-thread counter is an admission gate in front of it,
    never a second source of truth.

    Under M3's fan-out `credited` is the **maximum** over armed sessions of the publish
    ordinal that session is finished with, not the minimum. A frame only has to be
    deliverable to *someone*, so a session stalled behind a retained view — or a dead one
    waiting out its lease — must not stop the publisher serving everybody else. Its own
    window still holds its own slots, and §5.4 still holds those until it credits or expires,
    which is exactly the bound M3's exit condition asks for.

15. **Lease and control-block pools exist because §2.1 forbids data-path allocation.**
    A `shared_ptr` control block per delivered frame is an allocation, and §5.5 makes that
    refcount the credit interlock — the two are only compatible because the number of live
    leases is bounded by construction at `ring_depth`. Hence `LeasePool` plus a class-level
    `operator new` on `Lease::Impl`. `LeasePool::overflows()` is public so the bound can be
    observed rather than assumed.

16. **The expiry sweep runs on the engine thread, not on a control thread.** §5.1's thread
    table gives the publisher a control thread whose listed jobs are lease timers and expiry
    sweeps — on the publisher side that is its *only* job, since `Renew` commands and
    `DeviceProxy` calls are the subscriber's. Such a thread would have had to hand every
    `ucp_*` call in §4.2's teardown back to the engine anyway.

    What §4.2 actually requires is that expiry is "independent of frame traffic", and the
    engine loop runs whether or not frames flow: it sweeps every 256 iterations, which against
    a TTL floor of 1 000 ms is several orders of magnitude more often than can matter. Folding
    the sweep in removes a thread, a request queue, and a shutdown ordering problem, and it
    makes `Close` collapse to a claim on the session — so a close, a lease deadline and a
    transport error all reach §4.2's teardown down one code path rather than three.

17. **The session table lives in `src/ucx/publisher_impl.cpp`, not `src/core/session_manager.cpp`.**
    §8's extraction map names the latter. A session is an endpoint, a credit window, a
    sequence space and a lease deadline, and three of those four are only meaningful next to
    the `ucp_worker` — teardown steps 2 and 3 of §4.2 are `ucp_*` calls. A `src/core/` module
    would have held the deadline arithmetic and handed everything else back through an
    interface. The arithmetic is four lines; the interface would not have been.

18. **`PublisherConfig::max_renewals_per_ttl` added.** §6.1 lists "renewals per TTL" among the
    configurable quantities with a default of 10 and a range of 2..100, and §2.3's struct has
    no field for it. Adding one is the only way to satisfy both. It is an addition, not a
    rename: no existing signature changed.

19. **A retired session keeps its identifier until its seat is needed.** §4.2 says a `Closed`
    session's `session_id` is "retired permanently", and §3.7 wants a late `Renew` told which
    of two things happened — `UnknownSession` after a `Close`, `SessionExpired` after a lease
    ran out. A seat that reset itself to empty on teardown could answer neither. So teardown
    leaves the seat in `Closed` holding its identifier and how it ended, and `handle_open`
    takes a never-used seat first and otherwise the seat retired longest ago. An identifier is
    therefore remembered for at least `max_sessions` further opens and never reused.

    Which of the three possible enders won is settled by a single write-once CAS on
    `Session::end_reason` rather than by a CAS on the state. §4.4 requires teardown to run
    exactly once, and the state CAS alone gives that — but it leaves a window in which a loser
    can overwrite the winner's reason before teardown reads it, which is a real race and not a
    theoretical one: it was reproduced by `Close` being called twice in a row.

20. **The subscriber's engine thread starts in `adopt_open_reply()`, not in the constructor.**
    §5.1 says the `ucp_worker` "is touched **only** by its engine thread". The endpoint to the
    server is created from the address in the `OpenReply`, on whatever thread ran the Tango
    command — so M2 called `ucp_ep_create` concurrently with the engine's
    `ucp_worker_progress` on a `UCS_THREAD_MODE_SINGLE` worker. That is undefined behaviour;
    the publisher already marshalled the same call to its engine and the subscriber did not.

    Deferring the thread start is the fix and it costs nothing: before a session opens there
    is no endpoint, no credit and no frame, so the loop had nothing to progress. It also
    avoids the request queue and condition variable the publisher needs for the same job. The
    destructor gained the matching case — if the engine never started, it quiesces the worker
    on its own thread instead of quarantining it, which otherwise leaked a worker on every
    failed open.

21. **No renew timer on the subscriber, and no reconnect.** §5.1 gives the subscriber a
    control thread for the renew timer and `DeviceProxy` calls. There is no `DeviceProxy`
    until M4, so M3 exposes `make_renew_request()` / `adopt_renew_reply()` and lets the caller
    drive them — the same shape `Open` and `Close` already have, and the same shape the M4
    adapter will wrap. M3 adds no thread to either side.

    §4.1's `Reconnecting` state and `ReconnectPolicy` are likewise unimplemented:
    `SessionExpired` and `UnknownSession` move the subscriber to `Failed`. MVP_PLAN.md puts
    "negotiate, renew, close, and reopen sessions" in M4's client.

22. **The publisher destroys its `ucp_worker` explicitly, before any other member.**
    `ucp_worker_destroy` does not merely abandon outstanding sends — it purges them by
    *invoking their completion callbacks* (`uct_rc_txqp_purge_outstanding`, status
    `UCS_ERR_CANCELED`). M3 gave `on_send_complete` a `Session *` as its `user_data` so each
    session could count its own in-flight sends, and members are destroyed in reverse
    declaration order, so the implicit destructor freed the session table and *then* destroyed
    the worker. The purge called back into freed memory.

    `worker` is therefore a `unique_ptr` reset at the top of `~Impl`, after the engine join.
    The general rule it encodes: a UCX handle must be destroyed before anything its callbacks
    can touch, and relying on declaration order to arrange that is not good enough for an
    invariant this sharp.

    **This is not reachable over `cma` or `sm`**, where every send completes synchronously and
    the purge list is always empty — which is why M2, M3, ASan, TSan and a 20-run soak all
    passed over it. It took running the suite over `rc_verbs` to surface, and it is the
    ordinary RDMA case rather than a Soft-RoCE artefact.


23. **The benchmark was rewritten on the shipping API, not ported.** §8's map says "extract as
    the benchmark", and `experiments/ucx-bulk-spike/` is 2 455 lines of which almost none
    survives: `bulk_source.hpp` was already extracted into `registered_ring.cpp` at M2, the
    negotiation is now `handle_coordination()`, and both data paths are now the library's. What
    remains is `benchmarks/bulk_bench.cpp` driving `BulkPublisher` and `SubscriberEngine`
    directly, plus `oob.h` — a length-prefixed TCP blob channel standing in for the Tango
    command that carries coordination bytes in M4.

    The point of the rewrite is that a number from it is a number about the shipping code.
    Extracting the spike would have kept a second implementation of the data path alive, and a
    benchmark measuring code nobody runs is worse than no benchmark.

    Six spike flags are therefore absent rather than stubbed, because a flag that silently does
    nothing is worse than a missing one: `--transport rma` and `--flush-every` (§3.2 Path B,
    unimplemented), `--no-register`, `--progress-thread`, `--err-handling` and
    `--prefix-bytes`. `benchmarks/README.md` tabulates why each is gone.

    `zmq_baseline.cpp` was dropped. It was the comparison that justified starting the project,
    not one that guides it, §8 retires the Tango event integration wholesale, and
    `M5_TEST_GUIDE.md` does not ask for it. It remains in cppTango's history if a number is
    ever wanted.

24. **`--corrupt-every` is the verifier's negative control.** `FINDINGS.md` made a
    receiver-side checksum plus "flush removed ⇒ checksums fail" a blocking prerequisite for
    the RMA path, on the grounds that a missing flush shows up as a *faster* result — the one
    failure mode that argues for shipping the broken thing. That specific control is an RMA
    question this library cannot yet ask, but the general argument applies to any verification
    claim, so `--verify` ships with a way to make it fail: `--corrupt-every N` damages one byte
    of every *n*th frame, and the run is expected to report exactly `iters / n` mismatches.

    Observed: 9 of 72 at `N=8` over CMA, 22 of 220 at `N=10` over `rc_verbs`. A verifier that
    has never been observed to fail is not a verifier.


25. **`engine_cpu_affinity` was a public field that nothing read; it works now, and the
    placement it produces is observable.** §2.3 and §2.4 both declare
    `int engine_cpu_affinity{-1}`, and through M3 no code referenced either. Setting it
    returned `Status::Ok`, constructed cleanly, ran cleanly, and did nothing.

    That is a worse failure than an unimplemented feature, and the difference is worth
    stating: every other unimplemented thing here refuses in the caller's face —
    `DeliveryMode::DispatchThread` throws, `declare_geometry()` returns `Internal`, `Query`
    answers "not implemented before M4". A silent no-op on a field whose *only* purpose is to
    influence measurements means a benchmark can be attributed to a configuration that was
    never in effect. It is the same shape as `FINDINGS.md`'s warning that a missing flush
    "shows up as a *faster* result".

    Both engines now bind their own thread in `engine_loop()`, `-1` still means "leave it
    alone", and a refusal is a warning rather than a failure — a restrictive cpuset or a
    container can legitimately say no, and a device server must not fail to start because it
    could not optimise itself. `validate()` rejects anything below `-1`, so a typo stops
    reading as success.

    **Placement is where the real risk was.** Pinning the thread does not move the ring:
    `ucp_mem_map` faults and NUMA-places the pages during construction, on whatever thread
    constructed the publisher, by first touch. Implementing only the thread half would have
    produced thread affinity with the memory still on the far socket — measurable, plausible,
    and wrong. So `src/ucx/locality.h` reports what actually happened: the transport and
    device from `ucp_ep_query`, the NIC's node from `ucs_numa_node_of_device`, the ring's node
    from one `get_mempolicy` syscall, the engine's CPU and node, and a verdict.

    The verdict has three states, not two, and that was a correction made after the first
    version reported `coherent=no` on every single-socket host: `SingleNode` (one node, nothing
    to place), `Local`, `Split`, `Unknown`. Only `Split` warns. `Unknown` is the honest answer
    for a software device such as Soft-RoCE's `rxe0`, which has no PCI parent and therefore no
    NUMA node at all, and warning about it would train an operator to filter out the one
    message that matters.

    No `libnuma` and no `hwloc`: UCX links neither and answers the device-side questions from
    its own sysfs walk, so adding either would cost the "stock packages only" property for
    nothing.

26. **`RegisteredMemory` split out of `RegisteredRing`, ahead of needing it.** A ring does two
    unrelated jobs — acquire registered memory, and carve it into slots — and every foreseeable
    change is to the first. §6.3 already names two acquisition modes (`UCP_MEM_MAP_ALLOCATE`
    for a library-owned ring, `UCP_MEM_MAP_PARAM_FIELD_ADDRESS` for "the normal detector case"
    of adopting a vendor ring), and dual-socket placement adds a third: allocate on a chosen
    node, then register what we allocated.

    All three differ only in how the base pointer and `ucp_mem_h` come to exist and who frees
    them. None changes slot arithmetic, `contains()`, or a single call site in either engine.
    So each is a `static` factory on a value type and nothing else — which is the whole point
    of doing the split before the second mode exists rather than during it.

    Only `ucx_allocated()` is implemented. The other two are named in a comment and
    deliberately **not** declared: a declared factory that always throws advertises a
    capability that is not there, which is the mistake in deviation 25 wearing different
    clothes. A missing factory is a compile error at the call site, which is the loudest
    failure available.

    The mutual exclusion worth recording, because it is an easy trap: `UCP_MEM_MAP_ALLOCATE`
    and a supplied address are not composable. With `ALLOCATE` set, UCX picks the address and
    any address passed is a hint it may ignore — so allocating with `numa_alloc_onnode` and
    *then* setting `ALLOCATE` registers UCX's memory and leaks the caller's. The `on_node()`
    and `adopted()` factories must not set that flag.

    What is deliberately absent: any rail-assignment policy. §10 defers "how a stream is
    assigned to a rail (static, round-robin, NUMA-derived)" to M5 "with hardware in hand", so
    building one now would be inventing exactly what the spec says not to invent yet. This
    milestone observes; M5 decides.


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

## 6. M1 exit criteria

Pure `src/core/`: no UCX, no Tango, no threads. Everything below runs in milliseconds with
no external resource.

| Requirement (§9.2) | Status | Where |
|---|---|---|
| Every message in §3, encode and decode, exact byte layouts | met | `protocol.h`, `src/core/protocol.cpp` |
| Bounds validation per §3.4 and §3.9 | met | `src/core/geometry.cpp`, `geometry_rules.h`, `protocol.cpp` |
| CSPRNG session/stream identifiers | met | `src/core/session_id.cpp` |
| Credit window with the bitmap | met | `src/core/credit_window.{h,cpp}` |
| Geometry validation | met | `src/core/geometry.cpp` |

| Test (§9.2) | Status | Where |
|---|---|---|
| Round trip, every message type | met | `test_protocol_roundtrip.cpp` |
| Golden vectors, one committed blob per message type | met | `golden_vectors.h`, `test_protocol_golden.cpp` |
| Truncation sweep, every length from 0 to `len-1`, under ASan | met | `test_protocol_malformed.cpp` |
| Corruption sweep, every bit of the fixed part | met | `test_protocol_malformed.cpp` |
| Version rules | met | `test_protocol_malformed.cpp` |
| Bounds | met | `test_protocol_malformed.cpp`, `test_geometry.cpp` |
| Endianness asserted against hand-written expected bytes | met | `test_protocol_layout.cpp`, `wire_helpers.h` |
| Credit window | met | `test_credit_window.cpp` |

**Exit condition — "the golden vectors and the implementation agree": met.** 110 ctest cases,
3 914 assertions, green under both a normal build and `-fsanitize=address,undefined`.

Two notes on what these tests are worth:

- The golden vectors are *change detectors*. They are generated from the encoder, so they
  cannot prove conformance on their own. `test_protocol_layout.cpp` is the conformance
  check: every offset there was transcribed from the spec's tables, and every integer is
  compared against a little-endian pattern computed independently in
  `tests/unit/wire_helpers.h` rather than through the library's own accessors. The `Open`
  vector was additionally decoded field-by-field against §3.3 and §3.5 by hand before being
  committed.
- The truncation and bit-flip sweeps assert "never reads past the supplied length", which no
  return value can express. They are only worth their runtime under a sanitizer, which is why
  `TANGO_BULK_SANITIZERS` exists and why `pixi run test-asan` is part of the workflow rather
  than a debugging aid. Each truncation is copied into a fresh, exactly-sized buffer so a
  one-byte overread is a heap overflow ASan can see, not a byte still inside the original
  allocation.

## 7. M2 exit criteria

One publisher and one subscriber in the same test process over a UCX loopback: registered
producer lease → engine thread → self-contained frame → registered consumer slot →
`FrameView` → credit on release. All eight criteria live in `tests/ucx/test_vertical_slice.cpp`.

| Criterion (§9.3) | Status | Test |
|---|---|---|
| **Single frame** — bytes arrive intact; `FrameView` fields match the published `FrameMetadata` | met | `A published frame arrives intact with its metadata` |
| **Zero copy** — payload address lies inside the registered receive ring, no copy of payload size, by instrumentation not inspection | met | `The delivered payload lives in the registered receive ring` |
| **Wraparound** — `4 × ring_depth` frames reuse every slot; slot `i` holds sequence `s` iff `s % ring_depth == i` | met | `Slots recycle: sequence s lands in slot s % ring_depth` |
| **Lifetime under retention** — holding a view withholds exactly one credit; publisher stalls at `credit_window` and resumes on release | met | `A retained view withholds exactly one credit` |
| **Out-of-order release** — `ack_sequence` advances only across the contiguous prefix | met | `Out-of-order release advances the ack only across the contiguous prefix` |
| **Credit exhaustion** — `publish()` returns `CreditStalled`, `dropped_credit_stalled` increments, nothing blocks, no slot reused | met | `With every view retained, publish reports CreditStalled and never blocks` |
| **Lease return** — `QueueFull` on a full publish queue **and the caller still holds a usable lease** | met | `A full publish queue returns QueueFull and leaves the lease usable` |
| **ASan/UBSan clean** — whole suite under sanitizers, including teardown with views still outstanding | met | `Views outlive the subscriber that delivered them`; `pixi run test-asan` |

**Exit condition met**, and still met at M3: all eight criteria pass unchanged against the
session table and the restored probe interlock.

The zero-copy criterion is asserted two ways, because a pointer inside the ring does not by
itself prove nothing was staged: `ring_contains()` is the address half and `bytes_copied()`
the registration half, counting bytes that went through the eager path. For a
rendezvous-sized frame it must stay at zero.

Three defects found by running the criteria rather than by reading the code, all recorded
here because each failure mode was reported a long way from its cause:

- A **double free** of the `ucp_am_send_nbx` request — freed at the call site *and* in the
  completion callback. UCX surfaced it as `arbiter.c:36 Assertion
  'ucs_arbiter_group_is_empty(group)' failed` during endpoint teardown.
- The **consumer never drained its in-flight rendezvous receives** before destroying its
  worker, which produced the same arbiter assertion from a different direction (deviation 12).
- A **heap-use-after-free** in the lease control block, found only under ASan (deviation 13).

**One assumption carried forward, not a settled result.** The producer lease rule is built
to §5.4 exactly as written: release is credit-driven, and local send completion frees the
header context only — never the slot. That is what makes the rule hold uniformly across AM,
RMA and the batched-flush variants. But §9.4's **P0-10** flush-amortization track has not
run on hardware, and §10 lists this contract among the things that could still move. Treat
`on_send_complete` not releasing the slot as an assumption under test, not as decided.

## 8. M3 exit criteria

MVP_PLAN.md states M3's exit condition in one sentence: **"killing a client cannot exhaust
producer slots beyond the configured lease deadline; recovery does not require restarting the
device server."** IMPLEMENTATION_SPEC.md §9 stops at M2, so the normative detail is §4.2
(server state machine and the fixed `Expiring` teardown order), §4.4 (edge cases), §3.7
(`Renew`), §3.8 (`Close`) and §6.1 (bounds). Tests are in `tests/ucx/test_session_lifecycle.cpp`.

| Requirement (MVP_PLAN M3) | Status | Test |
|---|---|---|
| Unpredictable session identifiers and negotiated TTL / renew interval | met | `Open grants unpredictable identifiers and the negotiated lease terms` |
| Probe-before-ready and arm-before-send interlocks | met | `A granted session carries no frame until ProbeAck arms it` |
| `BulkRenew` state exchange | met | `Renewal keeps a session alive past its lease`; `Close is idempotent and Renew tells a closed session from an unknown one` |
| Idempotent `BulkClose` | met | `Close is idempotent and Renew tells a closed session from an unknown one` |
| Expire crashed/unrenewed clients **independent of frame traffic** | met | `An unrenewed session expires on schedule with no frames in flight` |
| On expiry: stop submission, close endpoint, release per-session leases, update counters | met | `A client that vanishes releases its slots on the lease, and the stream recovers` |
| Malformed and late renewals, close races, restart | met | rate limit and `UnknownSession`/`SessionExpired` cases above; restart is the second half of the vanishing-client test |
| Bound pinned memory, queue capacity, sessions, frame size, depth | met | `A publisher admits no more sessions than it was configured for`; `PublisherConfig::validate()` (already complete at M0/M1) |

Two further cases carry weight beyond the checklist:

- `Two sessions coexist and a slot returns only when both have credited it` is the direct test
  of §5.4's plural — "retained until **every** session it was successfully submitted to has
  credited its sequence". It is also §4.4's crash-and-restart case, which requires two
  sessions to be live at once and is why §6.1 defaults `max_sessions` above one.
- `Renewing faster than the rate limit is refused without shortening the lease` asserts the
  half of §3.7 that is easy to get wrong: the reply is refused, the lease is **not** shortened
  as a penalty, and the session keeps carrying frames. Punishing a client for a misconfigured
  renew interval would turn a configuration mistake into data loss.

**Exit condition met.** 127 ctest cases green under a normal `-Werror` build, under
`-fsanitize=address,undefined`, and under `-fsanitize=thread`: 3 914 assertions in
`tango-bulk-unit-tests`, 523 in `tango-bulk-ucx-tests`, 10 in `tango-bulk-tango-tests`. The
UCX suite additionally ran 20 consecutive times with no failure and no UCX request-pool
warning.

ThreadSanitizer is new at this milestone and is the reason to trust the session table: every
transition in §4.2 is a handshake between a Tango command thread and the engine thread, and
the write-once claim in deviation 19 is the kind of mistake only a race detector or a very
unlucky user finds. It is not yet a `pixi` task — it needs `setarch -R` on this kernel — so it
is run deliberately rather than by default.

**The suite also runs over `rc_verbs`**, on a Soft-RoCE device (`modprobe rdma_rxe`) rather
than on RDMA hardware. This is a correctness configuration, never a performance one: rxe is
kernel software and its timings mean nothing. What it supplies that `cma` and `sm` cannot is
*asynchrony* — a send genuinely stays posted to a queue pair instead of completing inside the
call — and that alone found deviation 22, a use-after-free that every other configuration in
this repository passed cleanly. Run it with:

```sh
UCX_TLS=rc_verbs,ud_verbs ./build/tests/tango-bulk-ucx-tests
```

`ud_verbs` is not optional: `rc_verbs` is connection-oriented and has no way to complete the
address-based wireup handshake on its own.

Cases that wait on a lease use the shortest TTL §6.1 permits, 1 000 ms, so the timing is real
rather than mocked. That costs the suite about five seconds and is the price of testing a
timer at all.

**Two things M3 did not resolve.**

- The §5.4 assumption carried forward from M2 is unchanged: release is credit-driven and local
  send completion frees the header context only. §9.4's **P0-10** flush-amortization track has
  still not run on hardware, and §10 lists this contract among the things that could move.
  M3 multiplied the number of places that rule is applied without settling it.
- `ucp_request_cancel` on an AM receive (deviation 12) is still unverified and still
  unreachable from the suite.

## 9. What M0–M3 deliberately do not contain

No Tango integration. No commands, no `DeviceProxy` client, no `BulkQuery`, no geometry
epochs, no reconnect, no relay, no RMA, no dispatch thread, no renew timer.
`BulkSubscriber` is a declaration only; the transport engine behind it is
`detail::SubscriberEngine` (deviation 2).

The next slice is M4, the stock-Tango adapter: `BulkOpen` / `BulkRenew` / `BulkClose` /
`BulkQuery` as ordinary commands over `handle_coordination`, a `BulkSubscriber` shell over
`SubscriberEngine` taking a `Tango::DeviceProxy &`, and the renew timer and reconnect that
need one.
