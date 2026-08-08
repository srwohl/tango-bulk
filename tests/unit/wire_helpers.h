// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_TESTS_UNIT_WIRE_HELPERS_H
#define TANGO_BULK_TESTS_UNIT_WIRE_HELPERS_H

#include <tango-bulk/protocol.h>

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

/// Test-side byte handling, written independently of src/core/byte_order.h.
///
/// The duplication is deliberate.  If the tests used the library's own accessors
/// they could only prove that encode and decode agree with each other, which
/// they would do just as happily with the bytes in the wrong order.
namespace TangoBulk::test
{

/// Expected little-endian representation of `value` in `width` bytes.
inline std::vector<std::byte> le(std::uint64_t value, std::size_t width)
{
    std::vector<std::byte> out;
    out.reserve(width);

    for(std::size_t i = 0; i < width; ++i)
    {
        out.push_back(static_cast<std::byte>((value >> (i * 8)) & 0xFFu));
    }

    return out;
}

inline std::string to_hex(const std::byte *data, std::size_t size)
{
    static const char *digits = "0123456789abcdef";

    std::string out;
    out.reserve(size * 2);

    for(std::size_t i = 0; i < size; ++i)
    {
        const auto value = static_cast<unsigned>(data[i]);
        out.push_back(digits[value >> 4]);
        out.push_back(digits[value & 0xFu]);
    }

    return out;
}

inline std::string to_hex(const std::vector<std::byte> &bytes)
{
    return to_hex(bytes.data(), bytes.size());
}

template <std::size_t N>
std::string to_hex(const std::array<std::byte, N> &bytes)
{
    return to_hex(bytes.data(), bytes.size());
}

inline std::vector<std::byte> from_hex(const std::string &hex)
{
    REQUIRE(hex.size() % 2 == 0);

    const auto nibble = [](char c) -> unsigned
    {
        if(c >= '0' && c <= '9')
        {
            return static_cast<unsigned>(c - '0');
        }
        REQUIRE(c >= 'a');
        REQUIRE(c <= 'f');
        return static_cast<unsigned>(c - 'a') + 10;
    };

    std::vector<std::byte> out;
    out.reserve(hex.size() / 2);

    for(std::size_t i = 0; i < hex.size(); i += 2)
    {
        out.push_back(
            static_cast<std::byte>((nibble(hex[i]) << 4) | nibble(hex[i + 1])));
    }

    return out;
}

/// Assert that `width` bytes at `offset` are the little-endian encoding of
/// `value`, and say which field failed when they are not.
inline void check_le_field(const std::vector<std::byte> &bytes, std::size_t offset,
                           std::size_t width, std::uint64_t value, const char *field)
{
    INFO("field '" << field << "' at offset " << offset << ", width " << width);
    REQUIRE(bytes.size() >= offset + width);

    const std::vector<std::byte> expected = le(value, width);
    const std::vector<std::byte> actual(bytes.begin() + static_cast<std::ptrdiff_t>(offset),
                                        bytes.begin() +
                                            static_cast<std::ptrdiff_t>(offset + width));

    INFO("expected " << to_hex(expected) << ", got " << to_hex(actual));
    CHECK(actual == expected);
}

template <std::size_t N>
void check_le_field(const std::array<std::byte, N> &bytes, std::size_t offset,
                    std::size_t width, std::uint64_t value, const char *field)
{
    check_le_field(std::vector<std::byte>(bytes.begin(), bytes.end()), offset, width,
                   value, field);
}

/// A distinctive value per width, so a field read at the wrong offset produces a
/// different number rather than an accidentally plausible one.
inline constexpr std::uint64_t k_probe_u64 = 0x0102030405060708ull;
inline constexpr std::uint32_t k_probe_u32 = 0x0A0B0C0Du;
inline constexpr std::uint16_t k_probe_u16 = 0x1122u;

/// Sixteen distinguishable octets for an identifier field.
inline std::array<std::byte, 16> pattern_id16(unsigned seed = 0)
{
    std::array<std::byte, 16> out{};
    for(std::size_t i = 0; i < out.size(); ++i)
    {
        out[i] = static_cast<std::byte>((seed + i * 17 + 1) & 0xFFu);
    }
    return out;
}

} // namespace TangoBulk::test

#endif // TANGO_BULK_TESTS_UNIT_WIRE_HELPERS_H
