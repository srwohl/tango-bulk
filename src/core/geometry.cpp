// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/frame.h>
#include <tango-bulk/limits.h>
#include <tango-bulk/protocol.h>
#include <tango-bulk/geometry.h>

#include "core/byte_order.h"
#include "core/geometry_rules.h"

namespace TangoBulk
{

namespace detail
{

Status validate_shape_and_strides(std::uint32_t rank,
                                  const std::array<std::uint64_t, k_max_rank> &shape,
                                  const std::array<std::uint64_t, k_max_rank> &strides,
                                  std::uint32_t element_size,
                                  std::uint64_t limit) noexcept
{
    if(rank > k_max_rank)
    {
        return Status::GeometryMismatch;
    }

    // Entries at or beyond the rank must be zero.  Tolerating junk there would
    // make two geometries that compare unequal describe the same array, which
    // matters because the epoch interlock compares them.
    for(std::size_t i = rank; i < k_max_rank; ++i)
    {
        if(shape[i] != 0 || strides[i] != 0)
        {
            return Status::GeometryMismatch;
        }
    }

    std::uint64_t elements = 1;
    for(std::uint32_t i = 0; i < rank; ++i)
    {
        if(wire::mul_overflow(elements, shape[i], elements))
        {
            return Status::GeometryMismatch;
        }
    }

    std::uint64_t described_bytes = 0;
    if(wire::mul_overflow(elements, element_size, described_bytes))
    {
        return Status::GeometryMismatch;
    }

    if(described_bytes > limit)
    {
        return Status::GeometryMismatch;
    }

    bool empty = false;
    for(std::uint32_t i = 0; i < rank; ++i)
    {
        if(shape[i] == 0)
        {
            empty = true;
            break;
        }
    }

    if(!empty)
    {
        std::uint64_t span = 0;

        for(std::uint32_t i = 0; i < rank; ++i)
        {
            std::uint64_t reach = 0;
            if(wire::mul_overflow(shape[i] - 1, strides[i], reach))
            {
                return Status::GeometryMismatch;
            }

            if(wire::add_overflow(span, reach, span))
            {
                return Status::GeometryMismatch;
            }
        }

        if(wire::add_overflow(span, element_size, span))
        {
            return Status::GeometryMismatch;
        }

        if(span > limit)
        {
            return Status::GeometryMismatch;
        }
    }

    return Status::Ok;
}

Status validate_element_size(ElementType type, std::uint32_t element_size) noexcept
{
    if(element_size < k_min_element_size || element_size > k_max_element_size)
    {
        return Status::GeometryMismatch;
    }

    if(type != ElementType::Unknown && element_size_of(type) != element_size)
    {
        return Status::GeometryMismatch;
    }

    return Status::Ok;
}

} // namespace detail

Status Geometry::validate() const noexcept
{
    Protocol::GeometryBlock wire;
    wire.generation = generation;
    wire.element_type = element_type;
    wire.element_size = element_size;
    wire.rank = rank;
    wire.max_frame_bytes = max_frame_bytes;
    wire.ring_depth = ring_depth;
    wire.credit_window = credit_window;
    wire.shape = shape;
    wire.strides = strides;
    return wire.validate();
}

std::uint64_t Geometry::reachable_span() const noexcept
{
    if(validate() != Status::Ok)
    {
        return 0;
    }

    bool empty = false;
    for(std::uint32_t i = 0; i < rank; ++i)
    {
        if(shape[i] == 0)
        {
            empty = true;
            break;
        }
    }

    if(empty)
    {
        return 0;
    }

    std::uint64_t span = element_size;
    for(std::uint32_t i = 0; i < rank; ++i)
    {
        const std::uint64_t reach = (shape[i] - 1) * strides[i];
        if(span > UINT64_MAX - reach)
        {
            return 0;
        }
        span += reach;
    }
    return span;
}

bool Geometry::describes_same_array(const Geometry &other) const noexcept
{
    return element_type == other.element_type && element_size == other.element_size &&
           rank == other.rank && shape == other.shape && strides == other.strides;
}

namespace detail
{

Geometry to_geometry(const Protocol::GeometryBlock &wire) noexcept
{
    Geometry out;
    out.generation = wire.generation;
    out.element_type = wire.element_type;
    out.element_size = wire.element_size;
    out.rank = wire.rank;
    out.max_frame_bytes = wire.max_frame_bytes;
    out.ring_depth = wire.ring_depth;
    out.credit_window = wire.credit_window;
    out.shape = wire.shape;
    out.strides = wire.strides;
    return out;
}

} // namespace detail

namespace
{

/// C-contiguous strides in bytes, innermost dimension last.
void fill_contiguous_strides(std::uint32_t rank,
                             const std::array<std::uint64_t, k_max_rank> &shape,
                             std::uint32_t element_size,
                             std::array<std::uint64_t, k_max_rank> &strides) noexcept
{
    strides = {};

    std::uint64_t stride = element_size;
    for(std::uint32_t i = rank; i > 0; --i)
    {
        strides[i - 1] = stride;
        if(wire::mul_overflow(stride, shape[i - 1], stride))
        {
            // The caller's validate() rejects this; leaving the partially filled
            // strides is harmless because resolve() discards them on failure.
            return;
        }
    }
}

} // namespace

Status Protocol::GeometryBlock::validate() const noexcept
{
    // generation == 0 means "never armed" and is legal only as a local sentinel.
    // On the wire it is always a real epoch.
    if(generation == 0)
    {
        return Status::GeometryMismatch;
    }

    if(const Status status = detail::validate_element_size(element_type, element_size);
       status != Status::Ok)
    {
        return status;
    }

    if(max_frame_bytes < k_min_frame_bytes || max_frame_bytes > k_max_frame_bytes_hard_cap)
    {
        return Status::FrameTooLarge;
    }

    if(ring_depth < k_min_ring_depth || ring_depth > k_max_ring_depth)
    {
        return Status::DepthTooLarge;
    }

    if(credit_window == 0 || credit_window > ring_depth)
    {
        return Status::DepthTooLarge;
    }

    return detail::validate_shape_and_strides(rank, shape, strides, element_size,
                                              max_frame_bytes);
}

namespace Protocol
{

bool operator==(const GeometryBlock &a, const GeometryBlock &b) noexcept
{
    return a.generation == b.generation && a.element_type == b.element_type &&
           a.element_size == b.element_size && a.rank == b.rank &&
           a.max_frame_bytes == b.max_frame_bytes && a.ring_depth == b.ring_depth &&
           a.credit_window == b.credit_window && a.shape == b.shape &&
           a.strides == b.strides;
}

bool operator!=(const GeometryBlock &a, const GeometryBlock &b) noexcept
{
    return !(a == b);
}

bool describes_same_array(const GeometryBlock &a, const GeometryBlock &b) noexcept
{
    return a.element_type == b.element_type && a.element_size == b.element_size &&
           a.rank == b.rank && a.shape == b.shape && a.strides == b.strides;
}

} // namespace Protocol

bool describes_same_array(const FrameMetadata &a, const FrameMetadata &b) noexcept
{
    return a.element_type == b.element_type && a.element_size == b.element_size &&
           a.rank == b.rank && a.shape == b.shape && a.strides == b.strides;
}

Status FrameMetadata::resolve(std::uint64_t max_frame_bytes) noexcept
{
    FrameMetadata resolved = *this;

    if(resolved.rank > k_max_rank)
    {
        return Status::GeometryMismatch;
    }

    if(resolved.element_size == 0)
    {
        resolved.element_size = element_size_of(resolved.element_type);
    }

    if(resolved.element_size < k_min_element_size ||
       resolved.element_size > k_max_element_size)
    {
        return Status::GeometryMismatch;
    }

    if(resolved.payload_bytes == 0)
    {
        std::uint64_t elements = 1;
        for(std::uint32_t i = 0; i < resolved.rank; ++i)
        {
            if(wire::mul_overflow(elements, resolved.shape[i], elements))
            {
                return Status::GeometryMismatch;
            }
        }

        if(wire::mul_overflow(elements, resolved.element_size, resolved.payload_bytes))
        {
            return Status::GeometryMismatch;
        }
    }

    bool strides_unset = true;
    for(std::uint32_t i = 0; i < resolved.rank; ++i)
    {
        if(resolved.strides[i] != 0)
        {
            strides_unset = false;
            break;
        }
    }

    if(strides_unset && resolved.rank > 0)
    {
        fill_contiguous_strides(resolved.rank, resolved.shape, resolved.element_size,
                                resolved.strides);
    }

    if(const Status status = resolved.validate(max_frame_bytes); status != Status::Ok)
    {
        return status;
    }

    *this = resolved;
    return Status::Ok;
}

Status FrameMetadata::validate(std::uint64_t max_frame_bytes) const noexcept
{
    if(rank > k_max_rank)
    {
        return Status::GeometryMismatch;
    }

    // Unlike a GeometryBlock, a producer-supplied metadata with rank > 0 must
    // name its element type: the rank claims the payload is an array of
    // something, and "something" is not a description a consumer can act on.
    if(element_type == ElementType::Unknown && rank > 0)
    {
        return Status::GeometryMismatch;
    }

    if(const Status status = detail::validate_element_size(element_type, element_size);
       status != Status::Ok)
    {
        return status;
    }

    if(payload_bytes == 0)
    {
        return Status::GeometryMismatch;
    }

    if(payload_bytes > max_frame_bytes || payload_bytes > k_max_frame_bytes_hard_cap)
    {
        return Status::FrameTooLarge;
    }

    return detail::validate_shape_and_strides(rank, shape, strides, element_size,
                                              payload_bytes);
}

} // namespace TangoBulk
