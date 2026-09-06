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
/// Every method deals in encoded bytes, plain enums and `FrameView` -- no UCX
/// type appears, so the Tango layer links the transport without ever compiling
/// against a `ucp/*` header.
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

    /// Encoded `Open`.  By the time this returns the receive ring is allocated,
    /// registered and armed (4.1's `Opening` entry action), because the
    /// publisher may send the first frame the instant it replies.
    virtual std::vector<std::byte> make_open_request(std::uint64_t correlation_id) const = 0;

    /// Adopt an `OpenReply`: stream id, granted geometry, endpoint to the
    /// server.  Leaves the state in `Probing`; the publisher's `Probe` moves it
    /// to `Active`.
    virtual Status adopt_open_reply(const std::byte *data, std::size_t size) = 0;

    virtual std::vector<std::byte> make_renew_request(std::uint64_t correlation_id) = 0;

    /// Adopt a `RenewReply`.  `SessionExpired` and `UnknownSession` are terminal
    /// for this session (3.7: "There is no resurrection").
    virtual Status adopt_renew_reply(const std::byte *data, std::size_t size) = 0;

    virtual std::vector<std::byte> make_close_request(std::uint64_t correlation_id) const = 0;

    /// The lease terms as they stand after the last reply.  3.7 lets the server
    /// change them at any renewal and requires the client to adopt the new
    /// values, so the renew timer reads these rather than the configuration.
    virtual std::uint32_t lease_ttl_ms() const noexcept = 0;
    virtual std::uint32_t renew_interval_ms() const noexcept = 0;

    /// The geometry this session was granted: element type, rank, shape,
    /// strides, maximum frame size, ring depth and credit window.
    ///
    /// All-zero before a grant is adopted; `generation` is the field to test,
    /// since 0 is never a legal epoch on the wire.
    ///
    /// By value rather than by reference: a reconnect replaces the whole
    /// transport, so a reference handed to an application thread would outlive
    /// what it points at. The block is 96 bytes of POD.
    virtual Protocol::GeometryBlock granted_geometry() const noexcept = 0;

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
    virtual std::uint32_t generation() const noexcept = 0;
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
