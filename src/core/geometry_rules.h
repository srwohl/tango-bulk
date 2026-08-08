// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_CORE_GEOMETRY_RULES_H
#define TANGO_BULK_SRC_CORE_GEOMETRY_RULES_H

#include <tango-bulk/errors.h>
#include <tango-bulk/frame.h>

#include <array>
#include <cstdint>

/// The array-description rules, in one place.
///
/// Three call sites need them: a GeometryBlock arriving off the coordination
/// plane, a FrameMetadata supplied by a producer, and a Frame header arriving in
/// an AM callback.  One implementation means a bound tightened in review is
/// tightened for all three, which is the entire reason the spec specifies a
/// single validator rather than three checks that happen to agree.
namespace TangoBulk::detail
{

/// `limit` is max_frame_bytes for a geometry and payload_bytes for one frame: in
/// both cases the number of bytes the described array must fit inside.
///
/// Every product is overflow-checked.  These values come off the wire, and an
/// unchecked product is how a bounds check gets satisfied by a number that
/// wrapped.
Status validate_shape_and_strides(std::uint32_t rank,
                                  const std::array<std::uint64_t, k_max_rank> &shape,
                                  const std::array<std::uint64_t, k_max_rank> &strides,
                                  std::uint32_t element_size,
                                  std::uint64_t limit) noexcept;

/// element_size in 1..256, and consistent with element_type when the type is
/// named.
///
/// An unrecognised element_type fails, because element_size_of() reports 0 for
/// one: a receiver that cannot name the element type cannot interpret the
/// payload, and adopting the description anyway only defers the problem to the
/// application.
Status validate_element_size(ElementType type, std::uint32_t element_size) noexcept;

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_CORE_GEOMETRY_RULES_H
