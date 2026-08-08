// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_LIMITS_H
#define TANGO_BULK_LIMITS_H

#include <cstddef>
#include <cstdint>

/// Hard caps from IMPLEMENTATION_SPEC.md 6.1.
///
/// Everything here is a ceiling that *configuration cannot exceed*, as distinct
/// from a default, which lives next to the option it defaults.  They are in
/// their own header because both the wire decoders (which must reject an
/// oversize grant arriving from a peer) and the config validators (which must
/// reject an oversize request from the application) need them, and neither
/// should have to include the other.
namespace TangoBulk
{

inline constexpr std::uint64_t k_max_frame_bytes_hard_cap = 256ull << 20; // 256 MiB
inline constexpr std::uint64_t k_min_frame_bytes = 4ull << 10;            // 4 KiB

inline constexpr std::uint32_t k_max_ring_depth = 1024;
inline constexpr std::uint32_t k_min_ring_depth = 2;

inline constexpr std::uint32_t k_max_sessions = 32;

inline constexpr std::uint32_t k_max_publish_queue = 8192;
inline constexpr std::uint32_t k_min_publish_queue = 8;

inline constexpr std::uint32_t k_max_delivery_queue = 4096;
inline constexpr std::uint32_t k_min_delivery_queue = 1;

inline constexpr std::uint32_t k_max_lease_ttl_ms = 600'000;
inline constexpr std::uint32_t k_min_lease_ttl_ms = 1'000;

inline constexpr std::uint32_t k_max_renewals_per_ttl = 100;
inline constexpr std::uint32_t k_min_renewals_per_ttl = 2;

inline constexpr std::uint64_t k_max_pinned_bytes = 64ull << 30; // 64 GiB
inline constexpr std::uint64_t k_min_pinned_bytes = 16ull << 20; // 16 MiB

inline constexpr std::size_t k_max_ucx_address_bytes = 4096;

/// Element size bounds, shared by GeometryBlock validation and FrameMetadata.
inline constexpr std::uint32_t k_max_element_size = 256;
inline constexpr std::uint32_t k_min_element_size = 1;

/// Stream names are bounded so a name can never make a coordination message
/// exceed its own limit, and are restricted to [A-Za-z0-9_.-] so they can be
/// used unescaped in logs and in `key=value;` counter blobs.
inline constexpr std::size_t k_max_stream_name_bytes = 64;
inline constexpr std::size_t k_min_stream_name_bytes = 1;

} // namespace TangoBulk

#endif // TANGO_BULK_LIMITS_H
