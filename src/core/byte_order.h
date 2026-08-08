// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_CORE_BYTE_ORDER_H
#define TANGO_BULK_SRC_CORE_BYTE_ORDER_H

#include <cstddef>
#include <cstdint>

/// Byte-at-a-time little-endian accessors.
///
/// Every multi-byte integer field in every message is little-endian regardless
/// of host, and a native struct is never memcpy'd onto the wire.  Doing it a
/// byte at a time is what makes that true on a big-endian host as well as on a
/// little-endian one, and it costs nothing: the shift-and-or sequence compiles
/// to a single unaligned load on x86-64 and aarch64.
namespace TangoBulk::wire
{

inline void put8(std::byte *p, std::uint8_t value) noexcept
{
    p[0] = static_cast<std::byte>(value);
}

inline void put16(std::byte *p, std::uint16_t value) noexcept
{
    p[0] = static_cast<std::byte>(value & 0xFFu);
    p[1] = static_cast<std::byte>((value >> 8) & 0xFFu);
}

inline void put32(std::byte *p, std::uint32_t value) noexcept
{
    for(unsigned i = 0; i < 4; ++i)
    {
        p[i] = static_cast<std::byte>((value >> (i * 8)) & 0xFFu);
    }
}

inline void put64(std::byte *p, std::uint64_t value) noexcept
{
    for(unsigned i = 0; i < 8; ++i)
    {
        p[i] = static_cast<std::byte>((value >> (i * 8)) & 0xFFu);
    }
}

inline std::uint8_t get8(const std::byte *p) noexcept
{
    return static_cast<std::uint8_t>(p[0]);
}

inline std::uint16_t get16(const std::byte *p) noexcept
{
    return static_cast<std::uint16_t>(static_cast<unsigned>(get8(p)) |
                                     (static_cast<unsigned>(get8(p + 1)) << 8));
}

inline std::uint32_t get32(const std::byte *p) noexcept
{
    std::uint32_t value = 0;
    for(unsigned i = 0; i < 4; ++i)
    {
        value |= static_cast<std::uint32_t>(get8(p + i)) << (i * 8);
    }
    return value;
}

inline std::uint64_t get64(const std::byte *p) noexcept
{
    std::uint64_t value = 0;
    for(unsigned i = 0; i < 8; ++i)
    {
        value |= static_cast<std::uint64_t>(get8(p + i)) << (i * 8);
    }
    return value;
}

/// Overflow-checked multiply, used by every shape/stride product.
///
/// The products are attacker-influenced -- they come off the wire -- and an
/// unchecked one is how a bounds check gets satisfied by a value that wrapped.
inline bool mul_overflow(std::uint64_t a, std::uint64_t b, std::uint64_t &out) noexcept
{
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_mul_overflow(a, b, &out);
#else
    if(a != 0 && b > (~std::uint64_t{0}) / a)
    {
        return true;
    }
    out = a * b;
    return false;
#endif
}

inline bool add_overflow(std::uint64_t a, std::uint64_t b, std::uint64_t &out) noexcept
{
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_add_overflow(a, b, &out);
#else
    if(b > (~std::uint64_t{0}) - a)
    {
        return true;
    }
    out = a + b;
    return false;
#endif
}

} // namespace TangoBulk::wire

#endif // TANGO_BULK_SRC_CORE_BYTE_ORDER_H
