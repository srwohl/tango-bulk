// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_GEOMETRY_H
#define TANGO_BULK_GEOMETRY_H

#include <tango-bulk/frame.h>

#include <array>
#include <cstdint>

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

} // namespace TangoBulk

#endif // TANGO_BULK_GEOMETRY_H
