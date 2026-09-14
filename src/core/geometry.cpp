// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/frame.h>
#include <tango-bulk/geometry.h>
#include <tango-bulk/limits.h>

#include "core/byte_order.h"
#include "core/geometry_rules.h"

#include <charconv>
#include <limits>
#include <string>
#include <vector>

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

Status validate_endian(Endian endian) noexcept
{
    if(endian != Endian::Little && endian != Endian::Big)
    {
        return Status::GeometryMismatch;
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

bool describes_same_array(const ArrayTerms &a, const ArrayTerms &b) noexcept
{
    return a.element_type == b.element_type && a.element_size == b.element_size &&
           a.endian == b.endian && a.rank == b.rank && a.shape == b.shape &&
           a.strides == b.strides;
}

Status Geometry::validate() const noexcept
{
    if(generation == 0)
    {
        return Status::GeometryMismatch;
    }

    if(const Status status = detail::validate_endian(endian); status != Status::Ok)
    {
        return status;
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

bool Geometry::describes_same_array(const Geometry &other) const noexcept
{
    return TangoBulk::describes_same_array(*this, other);
}

bool operator==(const Geometry &left, const Geometry &right) noexcept
{
    return describes_same_array(left, right) && left.generation == right.generation &&
           left.max_frame_bytes == right.max_frame_bytes &&
           left.ring_depth == right.ring_depth && left.credit_window == right.credit_window;
}

bool operator!=(const Geometry &left, const Geometry &right) noexcept
{
    return !(left == right);
}

namespace
{

bool valid_stream_name(const std::string &name) noexcept
{
    if(name.size() < k_min_stream_name_bytes || name.size() > k_max_stream_name_bytes)
    {
        return false;
    }

    for(const char c : name)
    {
        const bool valid = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                           (c >= '0' && c <= '9') || c == '_' || c == '.' || c == '-';
        if(!valid)
        {
            return false;
        }
    }
    return true;
}

} // namespace

bool StreamOffer::available() const noexcept
{
    return outcome() == Outcome::Available;
}

StreamOffer::Outcome StreamOffer::outcome() const noexcept
{
    if(status == Status::UnknownStream)
    {
        return Outcome::Missing;
    }

    if(status == Status::ResourceExhausted)
    {
        return Outcome::Unsafe;
    }

    if(status == Status::MalformedMessage)
    {
        return Outcome::Malformed;
    }

    if(status != Status::Ok)
    {
        return Outcome::Unavailable;
    }

    if(version != k_version || !valid_stream_name(stream_name) || geometry.validate() != Status::Ok)
    {
        return Outcome::Malformed;
    }

    return age_ms > k_max_age_ms ? Outcome::Stale : Outcome::Available;
}

Status StreamOffer::validate() const noexcept
{
    if(version != k_version)
    {
        return Status::UnsupportedVersion;
    }

    if(status != Status::Ok)
    {
        return status;
    }

    if(!valid_stream_name(stream_name))
    {
        return Status::MalformedMessage;
    }

    if(age_ms > k_max_age_ms)
    {
        // Status has no discovery-specific stale value.  TransportFailure is
        // the existing establishment status for an observation that cannot be
        // trusted, while outcome() preserves the more precise classification.
        return Status::TransportFailure;
    }

    return geometry.validate();
}

bool operator==(const StreamOffer &left, const StreamOffer &right) noexcept
{
    return left.version == right.version && left.stream_name == right.stream_name &&
           left.geometry == right.geometry && left.age_ms == right.age_ms &&
           left.status == right.status && left.message == right.message;
}

bool operator!=(const StreamOffer &left, const StreamOffer &right) noexcept
{
    return !(left == right);
}

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

    // Unlike a Geometry, a producer-supplied metadata with rank > 0 must
    // name its element type: the rank claims the payload is an array of
    // something, and "something" is not a description a consumer can act on.
    if(element_type == ElementType::Unknown && rank > 0)
    {
        return Status::GeometryMismatch;
    }

    if(const Status status = detail::validate_endian(endian); status != Status::Ok)
    {
        return status;
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
