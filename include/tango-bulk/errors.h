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

struct BulkError
{
    Status status{Status::Ok};
    std::string message; ///< human-readable; never contains addresses or keys
    std::string origin;  ///< "publisher" | "subscriber" | "protocol" | "tango"
};

class BulkException : public std::runtime_error
{
  public:
    explicit BulkException(BulkError err);

    const BulkError &error() const noexcept;

  private:
    BulkError err_;
};

} // namespace TangoBulk

#endif // TANGO_BULK_ERRORS_H
