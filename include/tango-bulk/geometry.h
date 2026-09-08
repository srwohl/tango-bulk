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

    std::uint32_t version{k_version};
    std::string stream_name;
    Geometry geometry{};
    Status status{Status::UnknownStream};
    std::string message;

    bool available() const noexcept;
    Status validate() const noexcept;
};

bool operator==(const StreamOffer &left, const StreamOffer &right) noexcept;
bool operator!=(const StreamOffer &left, const StreamOffer &right) noexcept;

} // namespace TangoBulk

#endif // TANGO_BULK_GEOMETRY_H
