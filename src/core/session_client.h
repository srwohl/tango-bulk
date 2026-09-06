// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_CORE_SESSION_CLIENT_H
#define TANGO_BULK_SRC_CORE_SESSION_CLIENT_H

#include <tango-bulk/errors.h>
#include <tango-bulk/protocol.h>
#include <tango-bulk/subscriber.h>

#include <cstddef>
#include <cstdint>
#include <vector>

/// The client half of a session, with no transport in it.
///
/// `Open`, `Renew` and `Close` are protocol and policy: what to ask for, whether
/// the answer is acceptable, what the lease now says, and whether a refusal is
/// recoverable.  None of that needs a `ucp_worker`, and none of it changes when
/// the transport does -- so keeping it inside the transport engine only made the
/// engine bigger than the thing it is for.
///
/// It lives in `core/` for the same reason the protocol codec does: everything
/// here is testable in milliseconds with no NIC, no publisher and no Tango
/// database.  `SubscriberEngine` composes one and delegates to it, which is why
/// the seam in `subscriber_transport.h` is unchanged -- this is a split inside
/// the engine, not a new interface for its callers to learn.
namespace TangoBulk::detail
{

class SessionClient
{
  public:
    /// 3.5's request: everything this client wants, clamped by nobody yet.
    ///
    /// `client_address` is the subscriber's own UCX worker address, which is the
    /// one thing here the transport has to supply.
    std::vector<std::byte> make_open_request(const SubscriberConfig &config,
                                             const std::vector<std::byte> &client_addr,
                                             std::uint64_t correlation_id) const;

    /// Decode an `OpenReply` and adopt the grant, or say why it was refused.
    ///
    /// A client must accept `Error` in place of any expected reply, so the
    /// server's status is returned as-is when it sends one.  On anything but
    /// `Ok` nothing is stored and the session has not started.
    Status adopt_open_reply(const std::byte *data, std::size_t size, const SubscriberConfig &config);

    /// 3.7's renewal.  The counters are the client's own progress report; they
    /// are the caller's to supply because this class does not observe the data
    /// path.
    std::vector<std::byte> make_renew_request(std::uint64_t correlation_id,
                                              std::uint64_t frames_delivered,
                                              std::uint64_t credits_returned,
                                              std::uint32_t client_state) const;

    struct RenewOutcome
    {
        Status status{Status::Ok};

        /// 3.7: `SessionExpired` and `UnknownSession` both mean this session is
        /// over and MUST NOT be treated as recoverable -- "There is no
        /// resurrection".  `RenewTooFrequent` is the opposite: the lease is
        /// untouched and the only correct response is to renew less often.
        bool session_lost{false};
    };

    RenewOutcome adopt_renew_reply(const std::byte *data, std::size_t size);

    std::vector<std::byte> make_close_request(std::uint64_t correlation_id) const;

    /// The publisher's address, valid once `adopt_open_reply` returned `Ok`.
    /// The transport creates its endpoint from this and nothing else needs it.
    const std::vector<std::byte> &server_address() const noexcept
    {
        return server_address_;
    }

    Protocol::StreamId stream_id() const noexcept
    {
        return stream_id_;
    }

    const Protocol::GeometryBlock &granted_geometry() const noexcept
    {
        return granted_;
    }

    std::uint32_t generation() const noexcept
    {
        return granted_.generation;
    }

    std::uint32_t granted_ring_depth() const noexcept
    {
        return granted_.ring_depth;
    }

    std::uint64_t granted_frame_bytes() const noexcept
    {
        return granted_.max_frame_bytes;
    }

    /// 3.7 lets the server change either term at any renewal and requires the
    /// client to adopt the new value, so a renew timer reads these rather than
    /// the configuration it started from.
    std::uint32_t lease_ttl_ms() const noexcept
    {
        return lease_ttl_ms_;
    }

    std::uint32_t renew_interval_ms() const noexcept
    {
        return renew_interval_ms_;
    }

  private:
    Protocol::SessionId session_id_{};
    Protocol::StreamId stream_id_{0};
    std::vector<std::byte> server_address_;

    Protocol::GeometryBlock granted_{};

    std::uint32_t lease_ttl_ms_{0};
    std::uint32_t renew_interval_ms_{0};
};

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_CORE_SESSION_CLIENT_H
