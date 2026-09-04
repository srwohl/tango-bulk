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
A frame the subscriber has handed to the application, still occupying the slot it landed in.
_Avoid_: received frame, buffer

**Geometry**:
The negotiated description of a frame's array shape — element type, rank, shape, strides,
maximum size.
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

**Subscription**:
One client's side of a session: the act of asking for one, the memory pinned for it, and the
state it is in. A session is what the publisher granted; a subscription is what the client
holds. Their states are named separately for that reason — the publisher's wire-level view of a
session is not the client's view of its subscription.
_Avoid_: subscriber (that is the party, not the thing it holds)

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

**Credit**:
A subscriber's permission for the publisher to reuse one slot, returned when the last view of
that frame is released. Withholding credit is backpressure, not a leak.
_Avoid_: ack, token, permit

**Pinned budget**:
How much registered memory one subscription may hold. Ring depth and maximum frame size are
derived from it, not asked for separately.
_Avoid_: memory limit, quota

### The control plane's structure

**Coordination channel**:
How a session supervisor asks the publisher a question and gets an answer, without knowing what
carries it. Tango commands carry it today; a Python callable carries it in the binding.
_Avoid_: command callback, RPC, proxy, transport (that is the data plane)

**Session supervisor**:
The module that keeps one session alive — opening it, renewing its lease, reconnecting when it
is lost, and closing it when asked. It owns the policy and owns neither a transport nor a
control system.
_Avoid_: session manager, controller, runner, client

**Transport**:
The receiving half of the data plane: what holds the receive ring and the UCX worker, and turns
arriving bytes into delivered frames.
_Avoid_: engine (that names one concrete transport), connection, link

**Transport factory**:
What a session supervisor obtains a transport from. Every reconnect gets a fresh one; a retired
transport is never reused.
_Avoid_: provider, builder, allocator
