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

/// The immutable array contract negotiated for a stream.
///
/// This is the application-facing projection of the wire geometry.  Keeping
/// it independent of the protocol codec lets bindings expose schema without
/// making the wire vocabulary part of their ABI.
struct Geometry
{
    std::uint32_t generation{0};
    ElementType element_type{ElementType::Unknown};
    std::uint32_t element_size{0};
    std::uint32_t rank{0};
    std::uint64_t max_frame_bytes{0};
    std::uint32_t ring_depth{0};
    std::uint32_t credit_window{0};
    std::array<std::uint64_t, k_max_rank> shape{};
    std::array<std::uint64_t, k_max_rank> strides{};

    Status validate() const noexcept;
    std::uint64_t reachable_span() const noexcept;
    bool describes_same_array(const Geometry &other) const noexcept;
};

bool operator==(const Geometry &left, const Geometry &right) noexcept;
bool operator!=(const Geometry &left, const Geometry &right) noexcept;

/// The conservative, pre-Open description of one bulk stream.
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

    /// Decode the fixed BulkStreams spectrum row:
    /// version|name|generation|element_type|element_size|rank|max_frame_bytes|
    /// ring_depth|credit_window|shape[0..3]|strides[0..3]|age_ms.
    static StreamOffer from_bulk_stream_row(const std::string &row);
    /// Encode one available offer using the same fixed field order.  An
    /// unavailable offer has no row and therefore encodes as an empty string.
    std::string to_bulk_stream_row() const;

    Outcome outcome() const noexcept;
    bool available() const noexcept;
    Status validate() const noexcept;
};

bool operator==(const StreamOffer &left, const StreamOffer &right) noexcept;
bool operator!=(const StreamOffer &left, const StreamOffer &right) noexcept;

} // namespace TangoBulk

#endif // TANGO_BULK_GEOMETRY_H
