// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_UNSTABLE_SESSION_SUPERVISOR_H
#define TANGO_BULK_UNSTABLE_SESSION_SUPERVISOR_H

#include <tango-bulk/counters.h>
#include <tango-bulk/errors.h>
#include <tango-bulk/protocol.h>
#include <tango-bulk/subscriber.h>
#include <tango-bulk/unstable/subscriber_transport.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

/// UNSTABLE. Installed so that bindings and out-of-tree clients can drive a
/// session without a `Tango::DeviceProxy`, and free to change without a version
/// bump until the RFC settles. Nothing under `unstable/` is covered by the
/// package's version compatibility promise.
///
/// Everything `BulkSubscriber` does that is not Tango: open a session, keep its
/// lease renewed, reconnect when it is lost, deliver frames, close on the way
/// out. `BulkSubscriber` is now a thin adapter over this, and the Python binding
/// is a second one -- which is the point. This class names no Tango type and no
/// UCX type, so `tango-bulk-core` compiles it with neither dependency, and
/// `tests/unit` proves that mechanically by linking core alone.
namespace TangoBulk::detail
{

/// Carries one coordination message to the publisher and brings back the reply.
///
/// **Called only from the supervisor's control thread, never concurrently.**
/// That is a promise this class makes to its callers, not an accident of the
/// current implementation: a Python adapter acquires the GIL inside this call,
/// and bounding when that can happen is what makes the renewal-latency
/// question analysable at all.
///
/// Returns the encoded reply. MUST throw `BulkException` if it cannot deliver
/// the message or obtain a reply -- an unreachable device, a wrong reply type,
/// a timeout. The supervisor treats a throw as a hint to reconnect, never as
/// authority to release anything: only the lease decides that.
///
/// Typed by protocol message rather than by name, so that what the command is
/// called -- and on a device that already owns `BulkOpen`, it is called
/// something else -- stays entirely with the adapter.
using CoordinationChannel =
    std::function<std::vector<std::byte>(Protocol::CoordType, const std::vector<std::byte> &)>;

/// Builds a transport for one session.
///
/// Required rather than defaulted, which is worth explaining because the
/// omission looks like one: a default argument here would name
/// `make_subscriber_transport`, whose definition lives in `tango-bulk-ucx`, and
/// that would put an unresolved UCX symbol into `libtango-bulk-core.so`. Call
/// `open_session()` below to get the shipping transport without naming it.
///
/// Called once per session, so once more on every reconnect. A retired
/// transport is never reused: a failed UCX endpoint is not a usable data path
/// again, which is why this is a factory and not an instance.
using TransportFactory =
    std::function<std::unique_ptr<SubscriberTransport>(const SubscriberConfig &)>;

/// Where delivered frames and state changes go.
///
/// Both are required. A supervisor with no frame callback is a stream that
/// silently goes nowhere, and the only moment that omission is cheap to report
/// is before anything has been opened.
struct SessionCallbacks
{
    FrameCallback on_frame;
    StateCallback on_state;
};

/// The reconnect backoff schedule: exponential from `backoff_ms`, capped at the
/// last known lease TTL.
///
/// Free and pure so that the doubling and the cap are testable without a
/// supervisor, a transport, or a clock -- they are the part of the reconnect
/// policy hardest to provoke and easiest to get subtly wrong.
///
/// Capped at the TTL because backing off longer than a lease cannot help: by
/// then the publisher has reclaimed everything this client held, so the reopen
/// is a fresh session either way.
std::chrono::milliseconds backoff_delay(std::uint32_t attempt,
                                        std::uint32_t backoff_ms,
                                        std::uint32_t lease_ttl_ms) noexcept;

class SessionSupervisor
{
  public:
    /// Construct and open. There is no unstarted supervisor.
    ///
    /// Deliberately a factory rather than a constructor plus `start()`. The
    /// two-phase shape it replaces had an object that was illegal to use until
    /// two setters had been called, and a `started` flag that everything else
    /// had to consult; a lifetime that *is* the session has neither. Destroying
    /// it closes the session and joins both threads.
    ///
    /// Throws `BulkException` if the configuration is invalid, and if the first
    /// open fails under `FailFast` or `Manual`. Under `BoundedRetry` a failed
    /// first open returns a supervisor already in `Reconnecting`, which is what
    /// today's `start()` does.
    ///
    /// `channel` and `factory` must both be callable; `callbacks` must have both
    /// members set.
    static std::unique_ptr<SessionSupervisor> open(SubscriberConfig config,
                                                   CoordinationChannel channel,
                                                   TransportFactory factory,
                                                   SessionCallbacks callbacks);

    ~SessionSupervisor();

    SessionSupervisor(const SessionSupervisor &) = delete;
    SessionSupervisor &operator=(const SessionSupervisor &) = delete;

    /// `DeliveryMode::Manual` only. Invokes the frame callback on the CALLING
    /// thread and returns how many frames it dispatched. Throws if the
    /// configuration asked for a dispatch thread, which is already delivering.
    std::size_t poll(std::chrono::milliseconds timeout);

    SubscriberState state() const noexcept;
    std::uint32_t generation() const noexcept;
    SubscriberCounters counters() const noexcept;

  private:
    struct Impl;
    explicit SessionSupervisor(std::unique_ptr<Impl> impl) noexcept;

    std::unique_ptr<Impl> impl_;
};

/// `SessionSupervisor::open()` with the shipping UCX transport.
///
/// Declared here, defined in `src/ucx/session.cpp` -- the same arrangement as
/// `make_subscriber_transport()`, and for the same reason: naming the factory
/// is a UCX-layer privilege, so this header can offer it without core
/// referencing it.
///
/// This is the front door. The four-argument form exists so that a test can
/// substitute a transport it can make misbehave on demand; every other caller
/// wants this one, which asks only for the two things it genuinely has to
/// supply.
std::unique_ptr<SessionSupervisor> open_session(SubscriberConfig config,
                                                CoordinationChannel channel,
                                                SessionCallbacks callbacks);

} // namespace TangoBulk::detail

#endif // TANGO_BULK_UNSTABLE_SESSION_SUPERVISOR_H
