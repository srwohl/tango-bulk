// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_UNSTABLE_SUBSCRIBER_TRANSPORT_H
#define TANGO_BULK_UNSTABLE_SUBSCRIBER_TRANSPORT_H

#include <tango-bulk/counters.h>
#include <tango-bulk/errors.h>
#include <tango-bulk/protocol.h>
#include <tango-bulk/subscriber.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

/// UNSTABLE. Installed so that bindings and out-of-tree clients can reach the
/// data-plane seam, and free to change without a version bump until the RFC
/// settles. Nothing under `unstable/` is covered by the package's version
/// compatibility promise; move a header out of here to promise otherwise.
///
/// The interface where the two halves of `BulkSubscriber` meet.
///
/// 2.4 gives `BulkSubscriber` a `Tango::DeviceProxy &` constructor parameter and
/// 1.1 forbids `src/ucx/` from seeing `tango/*`, so the class cannot live in the
/// layer that owns its `ucp_worker`.  docs/EXTRACTION.md deviation 2 resolves
/// that by splitting it: the transport engine is `detail::SubscriberEngine` in
/// `src/ucx/`, `BulkSubscriber` is the shell in `src/tango/` that carries the
/// coordination bytes over Tango commands, and this header is the seam.
///
/// It lives in `src/core/` because that is the one layer both sides may include.
/// No UCX type appears on it, so the Tango layer links the transport without
/// ever compiling against a `ucp/*` header.
///
/// Five operations: say where you are, adopt a validated grant, and report
/// state, failure and counts. It carried sixteen once, mixing the protocol
/// codec, the lease terms, the wakeup primitives, delivery and observation --
/// five unrelated concerns, of which the codec varied between implementations
/// not at all. What is left is what actually differs between a UCX transport
/// and a fake.
namespace TangoBulk::detail
{

/// The subscription's delivery queue, defined in `src/core/delivery_queue.h`.
///
/// Forward-declared rather than included, because that header is internal and
/// this one is installed. A `shared_ptr` parameter needs no complete type, and
/// a transport implementation -- which does, since it pushes -- is compiled
/// inside this project. An out-of-tree transport therefore cannot be written
/// against the installed headers alone; section 4.10 rank 8 is where that seam
/// stops being installed at all.
class DeliveryQueue;

class SubscriberTransport
{
  public:
    virtual ~SubscriberTransport() = default;

    SubscriberTransport(const SubscriberTransport &) = delete;
    SubscriberTransport &operator=(const SubscriberTransport &) = delete;

    /// This subscriber's own address, for the peer to create an endpoint from.
    ///
    /// Valid from construction: the receive ring is allocated, registered and
    /// armed before `Open` goes out, because the publisher may send the first
    /// frame the instant it replies.
    virtual const std::vector<std::byte> &local_address() const noexcept = 0;

    /// Adopt a grant that has already been validated, and start receiving.
    ///
    /// Everything above the line -- what to ask for, whether the answer is
    /// acceptable, what the lease now says -- belongs to the subscription and
    /// happens before this is called. What arrives here is the settled contract:
    /// the data-plane handle, the array this session will carry, and where to
    /// reach the publisher.
    ///
    /// The ordering is the point. A transport that has not been activated has
    /// no endpoint and no progress thread, so it cannot put a frame into the
    /// subscription's delivery queue -- which is what lets a refused grant be
    /// refused with nothing to clean up. Creating the endpoint precedes
    /// starting worker progress, for the same reason: one thread owns the
    /// worker, and `ucp_ep_create` must not race `ucp_worker_progress`.
    ///
    /// Leaves the state in `Probing`; the publisher's `Probe` moves it to
    /// `Active`. Returns the reason it could not, having called nothing.
    virtual Status activate(Protocol::StreamId stream_id,
                            const Protocol::GeometryBlock &granted,
                            const std::vector<std::byte> &server_address) = 0;

    /// Why the transport reached `Failed`, valid once it has.
    ///
    /// Six different conditions retire a session -- a refused grant, an
    /// unreachable endpoint, a lost lease, a rendezvous that could not start or
    /// could not finish, a peer that contradicted the contract it granted -- and
    /// without this they all reach the application as one sentence. Which of
    /// them happened decides whether reconnecting can possibly help, so it is
    /// the application's to know.
    ///
    /// `Status::Ok` with an empty message while the transport is healthy.
    virtual BulkError last_error() const noexcept = 0;

    virtual SubscriberState state() const noexcept = 0;
    virtual SubscriberCounters counters() const noexcept = 0;

  protected:
    SubscriberTransport() = default;
};

/// Declared here, defined in `src/ucx/subscriber_engine.cpp`.
///
/// This is the whole reason the interface exists: the Tango layer needs to
/// *construct* a transport, and a constructor call would require the concrete
/// type, which would require the UCX headers.  A factory returning a base
/// pointer does not.
///
/// `delivery` is where received frames go, and it belongs to the subscription
/// rather than to this transport: it is created once, before the first session,
/// and outlives every transport a reconnect builds. Required, not optional --
/// a transport with nowhere to put frames is a stream that silently goes
/// nowhere, and construction is the cheapest moment to say so.
///
/// Throws `BulkException` if the configuration is invalid, the queue is null,
/// or the transport cannot be brought up.
std::unique_ptr<SubscriberTransport> make_subscriber_transport(
    SubscriberConfig config, std::shared_ptr<DeliveryQueue> delivery);

} // namespace TangoBulk::detail

#endif // TANGO_BULK_UNSTABLE_SUBSCRIBER_TRANSPORT_H
