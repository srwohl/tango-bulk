<!--
SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project

SPDX-License-Identifier: LGPL-3.0-or-later
-->

# tango-bulk

A generic bulk data plane for Tango devices: Tango carries control, UCX carries frames,
and the control plane owns the data plane's lifecycle. This glossary is the language the
RFC, the C++ reference implementation and the Python client are all held to.

## Language

### The stream and what flows through it

**Bulk stream**:
A high-rate data path a Tango device offers alongside its ordinary attributes. One per device.
_Avoid_: channel, feed, topic, pipe

**Frame**:
One self-contained unit of published data — payload bytes plus everything needed to interpret
them. Interpreting a frame never requires a Tango round-trip.
_Avoid_: message, image, event, sample

**Delivered frame**:
A frame the subscriber has handed to the application, either copied or borrowed.
_Avoid_: received frame, buffer

**Borrowed frame**:
A delivered frame that still refers to its receive slot. Its last retained view holds the
credit that prevents that slot from being reused.
_Avoid_: zero-copy buffer, borrowed lease

**Copied frame**:
A delivered frame in application-owned memory. Retaining it does not withhold credit for
the receive slot from which it was copied.
_Avoid_: detached buffer, fallback frame

**Array terms**:
Element type, element size, rank, shape and strides: what one payload is. A Geometry, a
producer's frame metadata and a delivered frame all carry the same array terms, and
"describes the same array" compares only these.
_Avoid_: array hint, layout, shape tuple

**Geometry**:
The negotiated description of a stream — its array terms, its epoch, and the sizing terms
(maximum frame size, ring depth, credit window). It is what the session contract carries.
_Avoid_: layout, format, schema (reserved for the Python client's view of a geometry)

**Geometry epoch**:
The generation stamped on a geometry. Changing any term of a session contract means closing
and reopening, so an epoch spans a session rather than changing inside one.
_Avoid_: version, revision, generation counter

### The session

**Publisher**:
The device-server side of a bulk stream: the half that owns the data and grants sessions.
_Avoid_: server, producer, source

**Subscriber**:
The client side of a bulk stream: the half that pins memory and receives frames.
_Avoid_: consumer, client, sink

**Session**:
One subscriber's attachment to one bulk stream. Granted by the publisher, and expiring unless
renewed.
_Avoid_: connection, stream instance, subscription (that is the client-side act of asking for one)

**Lease**:
The expiring right a session holds. When it runs out the publisher reclaims everything the
session held — which is why a client that dies cannot exhaust a publisher.
_Avoid_: TTL, keepalive, heartbeat

**Grant**:
What the publisher answers an open request with: the geometry, ring depth, credit window and
lease terms it will actually honour. Only ever clamped downward from what was asked for.
_Avoid_: negotiation result, agreement, settings

**Flow policy**:
Whether one Session is lossy or lossless when it lacks credit. A lossy Session may miss frames;
a lossless Session contributes backpressure and preserves a frame until application handoff or
until its Lease requires eviction. For a copied frame, handoff is when the application takes it;
for a borrowed frame, slot ownership continues until the last view is released.
_Avoid_: loss policy, fan-out mode (that is the current publisher-wide implementation)

**Recovery policy**:
Whether a Subscription tries to establish a replacement Session after a transient coordination
or transport failure, or fails immediately. It does not change whether frames may be lost.
_Avoid_: loss policy, reconnect policy

**Delivery ownership**:
Whether a Subscription delivers borrowed frames or copied frames. `Borrow` is the C++ default;
`Copy` makes the copy on the engine thread before the frame is queued, from a pool that falls
back to the heap, and is refused with a device receive region.
_Avoid_: copy mode, zero-copy mode

**Queue policy**:
Which delivered frame a Subscription preserves when its local delivery queue is full.
`PreserveOrder` keeps frames already queued; `PreferFresh` replaces the oldest queued copied frame
and is invalid for borrowed or lossless delivery. It does not change a Session's flow policy.
_Avoid_: drop policy, flow policy

**Subscription**:
One client's continuing attachment to one bulk stream: the act of asking for sessions, the
memory pinned for receiving them, and the client-visible state and delivery lifetime. A
subscription may span successive sessions when reconnect replaces one that was lost or expired.
A session is what the publisher granted; a subscription is what the client holds. Their states
are named separately for that reason — the publisher's wire-level view of a session is not the
client's view of its subscription.
_Avoid_: subscriber (that is the party, not the thing it holds)

**Frame event**:
What push delivery hands to the application's callback: one delivered frame, or, exactly once
and last, the terminal outcome that ended the subscription. Orderly close and interruption end
push delivery without an event.
_Avoid_: callback argument, message, notification

**Probe**:
The publisher's first message to a newly granted session, proving it can reach that
subscriber's UCX endpoint. No frame flows until it is answered.
_Avoid_: handshake, ping, hello

### Memory and flow control

**Receive ring**:
The registered memory a subscriber pins before opening a session, and into which the publisher
writes frames directly.
_Avoid_: buffer pool, queue, arena

**Slot**:
One frame-sized division of a receive ring. Which slot a frame lands in is its sequence modulo
the ring depth.
_Avoid_: cell, entry, bucket

**Publisher slot**:
One frame-sized region the publisher makes available for an application to fill and publish.
It remains occupied until every session that received its frame has relinquished it.
_Avoid_: producer buffer, source buffer

**Slot handle**:
An application's exclusive hold on a publisher slot before publication. It keeps that
storage valid even after the publisher closes; releasing it gives up the hold.
_Avoid_: producer lease, buffer token

**Credit**:
A subscriber's permission for the publisher to reuse one slot, returned when the last view of
that frame is released. Withholding credit is backpressure, not a leak.
_Avoid_: ack, token, permit

**Pinned budget**:
How much registered memory one subscription may hold, including receive rings retained
from earlier sessions. Ring depth and maximum frame size are derived from it.
_Avoid_: memory limit, quota

**Receive allocator**:
The application's one chance to supply the receive ring's memory instead of letting the
subscription allocate it. Asked once, during initial establishment, for the bytes the receive
plan needs; a replacement session reuses that memory only while nothing else refers to it, and
never asks again.
_Avoid_: buffer callback, custom allocator, memory hook

**Geometry expectation**:
What a subscribing application says the granted geometry must describe — any of element
type, rank, shape and strides, each optional. A grant that differs fails establishment; a
replacement session that differs ends the subscription.
_Avoid_: schema check, require, assertion

### The control plane's structure

**Coordination channel**:
How a session supervisor asks the publisher a question and gets an answer, without knowing what
carries it. Tango commands carry it today; a Python callable carries it in the binding.
_Avoid_: command callback, RPC, proxy, transport (that is the data plane)

**Session supervisor**:
The part of a subscription that keeps one session alive — opening it, renewing its lease,
reconnecting when it is lost, and closing it when asked. It owns the policy and the current
transport, taking a fresh transport from the transport factory on every reconnect; it owns no
control system. It is a role within a subscription, not a thing an application holds.
_Avoid_: session manager, controller, runner, client

**Transport**:
The receiving half of the data plane: what holds the receive ring and the UCX worker, and turns
arriving bytes into delivered frames.
_Avoid_: engine (that names one concrete transport), connection, link

**Transport factory**:
What a session supervisor obtains a transport from. Every reconnect gets a fresh one; a retired
transport is never reused.
_Avoid_: provider, builder, allocator
