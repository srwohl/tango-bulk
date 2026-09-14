// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include "bulk_stream_row.h"

#include <tango-bulk/limits.h>

#include <array>
#include <charconv>
#include <cstdint>
#include <vector>

namespace TangoBulk::detail
{

namespace
{

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

StreamOffer stream_offer_from_row(const std::string &row)
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
        if(offer.version != StreamOffer::k_version)
        {
            offer.message = "BulkStreams row version is not supported";
        }
        else if(offer.age_ms > StreamOffer::k_max_age_ms)
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

std::string stream_offer_to_row(const StreamOffer &offer)
{
    if(offer.status != Status::Ok)
    {
        return {};
    }

    std::string row;
    row.reserve(256);
    row += std::to_string(offer.version);
    row += '|';
    row += offer.stream_name;
    row += '|';
    row += std::to_string(offer.geometry.generation);
    row += '|';
    row += std::to_string(static_cast<std::uint32_t>(offer.geometry.element_type));
    row += '|';
    row += std::to_string(offer.geometry.element_size);
    row += '|';
    row += std::to_string(offer.geometry.rank);
    row += '|';
    row += std::to_string(offer.geometry.max_frame_bytes);
    row += '|';
    row += std::to_string(offer.geometry.ring_depth);
    row += '|';
    row += std::to_string(offer.geometry.credit_window);
    row += '|';
    for(std::size_t i = 0; i < k_max_rank; ++i)
    {
        if(i != 0)
        {
            row += ',';
        }
        row += std::to_string(offer.geometry.shape[i]);
    }
    row += '|';
    for(std::size_t i = 0; i < k_max_rank; ++i)
    {
        if(i != 0)
        {
            row += ',';
        }
        row += std::to_string(offer.geometry.strides[i]);
    }
    row += '|';
    row += std::to_string(offer.age_ms);
    return row;
}

} // namespace TangoBulk::detail
