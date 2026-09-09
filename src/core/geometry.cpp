// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/frame.h>
#include <tango-bulk/limits.h>
#include <tango-bulk/protocol.h>
#include <tango-bulk/geometry.h>

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

bool operator==(const Geometry &left, const Geometry &right) noexcept
{
    return left.generation == right.generation && left.element_type == right.element_type &&
           left.element_size == right.element_size && left.rank == right.rank &&
           left.max_frame_bytes == right.max_frame_bytes &&
           left.ring_depth == right.ring_depth &&
           left.credit_window == right.credit_window && left.shape == right.shape &&
           left.strides == right.strides;
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

constexpr char k_bulk_stream_field_separator = '|';
constexpr char k_bulk_stream_vector_separator = ',';
constexpr std::size_t k_bulk_stream_field_count = 12;

std::vector<std::string> split_bulk_stream_row(const std::string &row)
{
    std::vector<std::string> fields;
    std::size_t begin = 0;
    for(;;)
    {
        const std::size_t end = row.find(k_bulk_stream_field_separator, begin);
        fields.emplace_back(row.substr(begin, end == std::string::npos ? end : end - begin));
        if(end == std::string::npos)
        {
            return fields;
        }
        begin = end + 1;
    }
}

template <typename Integer>
bool parse_bulk_stream_integer(const std::string &text, Integer &value) noexcept
{
    if(text.empty())
    {
        return false;
    }

    Integer parsed{};
    const auto result = std::from_chars(text.data(), text.data() + text.size(), parsed, 10);
    if(result.ec != std::errc{} || result.ptr != text.data() + text.size())
    {
        return false;
    }

    value = parsed;
    return true;
}

template <typename Integer>
bool parse_bulk_stream_vector(const std::string &text,
                              std::array<Integer, k_max_rank> &values) noexcept
{
    std::size_t begin = 0;
    for(std::size_t i = 0; i < k_max_rank; ++i)
    {
        const std::size_t end = text.find(k_bulk_stream_vector_separator, begin);
        const std::string item =
            text.substr(begin, end == std::string::npos ? end : end - begin);
        if(!parse_bulk_stream_integer(item, values[i]))
        {
            return false;
        }

        if(end == std::string::npos)
        {
            return i + 1 == k_max_rank;
        }
        begin = end + 1;
    }

    return false;
}

StreamOffer malformed_bulk_stream_row(const char *message) noexcept
{
    StreamOffer offer;
    offer.status = Status::MalformedMessage;
    offer.message = message;
    return offer;
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

StreamOffer StreamOffer::from_bulk_stream_row(const std::string &row)
{
    const std::vector<std::string> fields = split_bulk_stream_row(row);
    if(fields.size() != k_bulk_stream_field_count)
    {
        return malformed_bulk_stream_row("BulkStreams row has the wrong field count");
    }

    StreamOffer offer;
    offer.status = Status::Ok;
    std::uint32_t element_type = 0;
    if(!parse_bulk_stream_integer(fields[0], offer.version) ||
       !parse_bulk_stream_integer(fields[2], offer.geometry.generation) ||
       !parse_bulk_stream_integer(fields[3], element_type) ||
       !parse_bulk_stream_integer(fields[4], offer.geometry.element_size) ||
       !parse_bulk_stream_integer(fields[5], offer.geometry.rank) ||
       !parse_bulk_stream_integer(fields[6], offer.geometry.max_frame_bytes) ||
       !parse_bulk_stream_integer(fields[7], offer.geometry.ring_depth) ||
       !parse_bulk_stream_integer(fields[8], offer.geometry.credit_window) ||
       !parse_bulk_stream_vector(fields[9], offer.geometry.shape) ||
       !parse_bulk_stream_vector(fields[10], offer.geometry.strides) ||
       !parse_bulk_stream_integer(fields[11], offer.age_ms))
    {
        return malformed_bulk_stream_row("BulkStreams row contains a non-decimal field");
    }

    offer.geometry.element_type = static_cast<ElementType>(element_type);
    offer.stream_name = fields[1];
    if(offer.validate() != Status::Ok)
    {
        if(offer.version != k_version)
        {
            offer.message = "BulkStreams row version is not supported";
        }
        else if(offer.age_ms > k_max_age_ms)
        {
            offer.message = "BulkStreams row is stale";
        }
        else
        {
            offer.message = "BulkStreams row failed validation";
        }
    }
    return offer;
}

std::string StreamOffer::to_bulk_stream_row() const
{
    if(status != Status::Ok)
    {
        return {};
    }

    std::string row;
    row.reserve(256);
    row += std::to_string(version);
    row += '|';
    row += stream_name;
    row += '|';
    row += std::to_string(geometry.generation);
    row += '|';
    row += std::to_string(static_cast<std::uint32_t>(geometry.element_type));
    row += '|';
    row += std::to_string(geometry.element_size);
    row += '|';
    row += std::to_string(geometry.rank);
    row += '|';
    row += std::to_string(geometry.max_frame_bytes);
    row += '|';
    row += std::to_string(geometry.ring_depth);
    row += '|';
    row += std::to_string(geometry.credit_window);
    row += '|';
    for(std::size_t i = 0; i < k_max_rank; ++i)
    {
        if(i != 0)
        {
            row += ',';
        }
        row += std::to_string(geometry.shape[i]);
    }
    row += '|';
    for(std::size_t i = 0; i < k_max_rank; ++i)
    {
        if(i != 0)
        {
            row += ',';
        }
        row += std::to_string(geometry.strides[i]);
    }
    row += '|';
    row += std::to_string(age_ms);
    return row;
}

bool operator==(const StreamOffer &left, const StreamOffer &right) noexcept
{
    return left.version == right.version && left.stream_name == right.stream_name &&
           left.geometry.generation == right.geometry.generation &&
           left.geometry.element_type == right.geometry.element_type &&
           left.geometry.element_size == right.geometry.element_size &&
           left.geometry.rank == right.geometry.rank &&
           left.geometry.max_frame_bytes == right.geometry.max_frame_bytes &&
           left.geometry.ring_depth == right.geometry.ring_depth &&
           left.geometry.credit_window == right.geometry.credit_window &&
           left.geometry.shape == right.geometry.shape &&
           left.geometry.strides == right.geometry.strides && left.age_ms == right.age_ms &&
           left.status == right.status &&
           left.message == right.message;
}

bool operator!=(const StreamOffer &left, const StreamOffer &right) noexcept
{
    return !(left == right);
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
