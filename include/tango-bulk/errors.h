// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_ERRORS_H
#define TANGO_BULK_ERRORS_H

#include <cstdint>
#include <stdexcept>
#include <string>

namespace TangoBulk
{

/// Every failure this library can report, on either plane.
///
/// The numeric values are on the wire (coordination replies carry a `u16
/// status`), so they are fixed for protocol major 1 and MUST NOT be renumbered.
enum class Status : std::uint16_t
{
    Ok = 0,
    UnsupportedVersion = 1,
    UnknownStream = 2,
    UnknownSession = 3,
    SessionExpired = 4,
    TooManySessions = 5,
    FrameTooLarge = 6,
    DepthTooLarge = 7,
    ResourceExhausted = 8, ///< pinned memory, RLIMIT_MEMLOCK, ring registration
    MalformedMessage = 9,
    NotAuthorized = 10,
    TransportFailure = 11,
    GeometryMismatch = 12,
    RenewTooFrequent = 13,
    Shutdown = 14,
    Internal = 15,
};

const char *to_string(Status status) noexcept;

/// Which half of the system reported a failure.
enum class Origin : std::uint8_t
{
    Publisher = 0,
    Subscriber = 1,
    Protocol = 2,
    Tango = 3,
    Transport = 4,
};

const char *to_string(Origin origin) noexcept;

struct BulkError
{
    Status status{Status::Ok};
    std::string message; ///< human-readable; never contains addresses or keys
    Origin origin{Origin::Subscriber};
};

/// The base of every exception this library throws. Catch a subclass when the
/// caller's action differs; catch this when it does not.
class BulkException : public std::runtime_error
{
  public:
    explicit BulkException(BulkError err);

    const BulkError &error() const noexcept;

  private:
    BulkError err_;
};

/// The options or metadata supplied by the caller are not acceptable.
class ConfigurationError : public BulkException
{
  public:
    using BulkException::BulkException;
};

/// Initial establishment did not produce an active Subscription. The error
/// carries the last failure; for a deadline expiry that is the last transient
/// one.
class EstablishmentError : public BulkException
{
  public:
    using BulkException::BulkException;
};

/// A delivery operation on a Subscription that was closed in an orderly way.
class StreamClosed : public BulkException
{
  public:
    using BulkException::BulkException;
};

/// A delivery operation after `interrupt()`. Sticky: every later operation
/// reports it again.
class Interrupted : public BulkException
{
  public:
    using BulkException::BulkException;
};

/// The Session ended and the recovery policy did not replace it.
class SessionLost : public BulkException
{
  public:
    using BulkException::BulkException;
};

/// A replacement Session described a different array than the one the
/// application accepted. The Subscription is closed; open a new one with the
/// new contract in hand.
class GeometryChanged : public BulkException
{
  public:
    using BulkException::BulkException;
};

/// Registered memory could not be obtained within the pinned budget.
class ResourceExhausted : public BulkException
{
  public:
    using BulkException::BulkException;
};

/// A pull operation on a push Subscription: caller misuse, not a failure of
/// the stream.
class DeliveryModeError : public BulkException
{
  public:
    using BulkException::BulkException;
};

} // namespace TangoBulk

#endif // TANGO_BULK_ERRORS_H
