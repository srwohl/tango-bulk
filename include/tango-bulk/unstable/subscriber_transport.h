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

    /// Invoke `cb` on the CALLING thread for each frame available within
    /// `timeout`; returns how many it dispatched.  Never the engine thread --
    /// that is 5.2's guarantee, and it is structural: the engine holds no
    /// `std::function` at all.
    /// A descriptor that becomes readable when a frame arrives, or -1 if this
    /// transport has none.
    ///
    /// **It is only signalled while armed**, and that is not an implementation
    /// detail to be discovered: the engine writes the descriptor only on the
    /// arm-to-disarmed transition, which is exactly what makes a consumer that
    /// is keeping up cost zero syscalls. Adding an unarmed descriptor to an
    /// event loop produces something that never fires.
    ///
    /// The protocol, and all three steps are load-bearing:
    ///
    ///     transport.arm_wakeup();               // 1. declare the intent
    ///     if (transport.poll(0ms, cb, 1) == 0)  // 2. re-check: a frame may
    ///     {                                     //    have landed before (1)
    ///         ::poll(&pfd, 1, timeout);         // 3. now it is safe to block
    ///         transport.drain_wakeup();
    ///     }
    ///
    /// Step 2 is what closes the lost-wakeup race. Without it the engine can
    /// push a frame between the arm and the block, find nobody armed yet, write
    /// nothing, and leave the consumer asleep with work waiting.
    ///
    /// `poll()` does all of this internally; this is for a caller that owns its
    /// own event loop -- `epoll`, `select`, `loop.add_reader()` -- and needs the
    /// waiting to happen somewhere else.
    virtual int fd() const noexcept = 0;

    /// Declare that this consumer is about to wait on `fd()`. Idempotent.
    /// See `fd()` for why a re-check must follow it.
    virtual void arm_wakeup() noexcept = 0;

    /// Consume a signal after `fd()` becomes readable, so it does not persist.
    /// A no-op when there is no descriptor or nothing pending.
    virtual void drain_wakeup() noexcept = 0;

    /// `max_frames` of 0 means "everything queued", which is what this did
    /// before the parameter existed and remains the default.
    ///
    /// It is the parameter that lets one frame be taken at a time. Draining
    /// unconditionally means a single call can withhold up to `queue_depth`
    /// credits at once, and it is why a consumer that wants exactly one frame
    /// -- a `read()`, a `try_read()`, an iterator step -- could not be built on
    /// top of this without a second delivery path of its own.
    virtual std::size_t poll(std::chrono::milliseconds timeout,
                             const FrameCallback &cb,
                             std::size_t max_frames = 0) = 0;

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
/// Throws `BulkException` if the configuration is invalid or the transport
/// cannot be brought up.
std::unique_ptr<SubscriberTransport> make_subscriber_transport(SubscriberConfig config);

} // namespace TangoBulk::detail

#endif // TANGO_BULK_UNSTABLE_SUBSCRIBER_TRANSPORT_H
