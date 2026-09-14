// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_GEOMETRY_H
#define TANGO_BULK_GEOMETRY_H

#include <tango-bulk/frame.h>

#include <array>
#include <cstdint>
#include <string>

namespace TangoBulk
{

/// The immutable array contract negotiated for a stream: the array terms, the
/// epoch and the sizing terms. It is what OpenReply and RenewReply carry, what
/// a BulkStreams row offers, and what a Subscription reports.
struct Geometry : ArrayTerms
{
    std::uint32_t generation{0}; ///< epoch; starts at 1, never zero on the wire
    std::uint64_t max_frame_bytes{0};
    std::uint32_t ring_depth{0};
    std::uint32_t credit_window{0};

    /// Applied on every receipt, not only on Open. A geometry that fails this
    /// is never partially adopted. generation == 0 is rejected: zero means
    /// "never armed" and is legal only as a local sentinel.
    Status validate() const noexcept;

    /// The array terms alone; see describes_same_array(ArrayTerms, ArrayTerms).
    bool describes_same_array(const Geometry &other) const noexcept;
};

bool operator==(const Geometry &left, const Geometry &right) noexcept;
bool operator!=(const Geometry &left, const Geometry &right) noexcept;

/// The conservative, pre-Open description of one bulk stream.
///
/// How an offer travels is not its business: the `BulkStreams` row codec lives
/// in the Tango adapter, because the row exists only because that attribute is
/// a string spectrum.
///
/// A valid available offer is an upper bound, not a grant: the publisher may
/// clamp every sizing field in OpenReply, but it must never require more than
/// this value.  The generation and Geometry are discovery observations and
/// therefore never replace OpenReply as the session authority.
struct StreamOffer
{
    static constexpr std::uint32_t k_version = 1;
    static constexpr std::uint64_t k_max_age_ms = 5'000;

    /// The typed result of inspecting a BulkStreams row before Open.
    enum class Outcome : std::uint8_t
    {
        Available,
        Missing,
        Unavailable,
        Malformed,
        Stale,
        Unsafe,
    };

    std::uint32_t version{k_version};
    std::string stream_name;
    Geometry geometry{};
    /// Age reported by the publisher when this row was produced.  It is a
    /// relative age, not a cross-process clock value; an old row is unusable.
    std::uint64_t age_ms{0};
    Status status{Status::UnknownStream};
    std::string message;

    Outcome outcome() const noexcept;
    bool available() const noexcept;
    Status validate() const noexcept;
};

bool operator==(const StreamOffer &left, const StreamOffer &right) noexcept;
bool operator!=(const StreamOffer &left, const StreamOffer &right) noexcept;

} // namespace TangoBulk

#endif // TANGO_BULK_GEOMETRY_H
