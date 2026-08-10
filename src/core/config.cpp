// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/publisher.h>
#include <tango-bulk/subscriber.h>

#include <tango-bulk/limits.h>

/// Configuration validation, per IMPLEMENTATION_SPEC.md 6.1.
///
/// Every quantity there has a default, a range, and a hard cap that
/// configuration cannot exceed.  These two functions are the only place the
/// ranges are enforced, and they run before anything is allocated or mapped --
/// a rejected configuration must never leave a partially registered ring
/// behind.
///
/// `Status` has no general-purpose "bad argument" code, so the mapping from a
/// rejected field to a status is fixed here and documented rather than invented
/// per call site:
///
///   stream_name, lease TTL, DropOldest-on-producer  -> MalformedMessage
///   max_frame_bytes                                 -> FrameTooLarge
///   ring_depth, credit_window, queue depths         -> DepthTooLarge
///   pinned_memory_limit_bytes                       -> ResourceExhausted
///   renew_interval_ms                               -> RenewTooFrequent
///
/// The same codes appear on the wire when a peer asks for something out of
/// range, which is the reason to reuse them here rather than add a config-only
/// enum: one vocabulary for "that number is not allowed", whichever side of the
/// wire said it.
namespace TangoBulk
{
namespace
{

/// 1..64 bytes of [A-Za-z0-9_.-].
///
/// Bounded so a name can never push a coordination message past its own limit,
/// and restricted so it can appear unescaped in a log line or in a `key=value;`
/// counter blob without quoting rules.
bool is_valid_stream_name(const std::string &name) noexcept
{
    if(name.size() < k_min_stream_name_bytes || name.size() > k_max_stream_name_bytes)
    {
        return false;
    }

    for(const char c : name)
    {
        const bool ok = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                        (c >= '0' && c <= '9') || c == '_' || c == '.' || c == '-';
        if(!ok)
        {
            return false;
        }
    }

    return true;
}

Status validate_common(const std::string &stream_name,
                       std::uint64_t max_frame_bytes,
                       std::uint32_t ring_depth,
                       std::uint32_t credit_window,
                       std::uint64_t pinned_memory_limit_bytes) noexcept
{
    if(!is_valid_stream_name(stream_name))
    {
        return Status::MalformedMessage;
    }

    if(max_frame_bytes < k_min_frame_bytes || max_frame_bytes > k_max_frame_bytes_hard_cap)
    {
        return Status::FrameTooLarge;
    }

    if(ring_depth < k_min_ring_depth || ring_depth > k_max_ring_depth)
    {
        return Status::DepthTooLarge;
    }

    // The credit window is what bounds outstanding frames; a window wider than
    // the ring would let the publisher be told to recycle a slot the peer has
    // not released.
    if(credit_window < 1 || credit_window > ring_depth)
    {
        return Status::DepthTooLarge;
    }

    if(pinned_memory_limit_bytes < k_min_pinned_bytes ||
       pinned_memory_limit_bytes > k_max_pinned_bytes)
    {
        return Status::ResourceExhausted;
    }

    return Status::Ok;
}

} // namespace

Status PublisherConfig::validate() const noexcept
{
    const Status common = validate_common(
        stream_name, max_frame_bytes, ring_depth, credit_window, pinned_memory_limit_bytes);
    if(common != Status::Ok)
    {
        return common;
    }

    if(max_sessions < 1 || max_sessions > k_max_sessions)
    {
        return Status::DepthTooLarge;
    }

    if(publish_queue_depth < k_min_publish_queue || publish_queue_depth > k_max_publish_queue)
    {
        return Status::DepthTooLarge;
    }

    if(lease_ttl_ms < k_min_lease_ttl_ms || lease_ttl_ms > k_max_lease_ttl_ms)
    {
        return Status::MalformedMessage;
    }

    // Renewing more often than TTL/10 is pointless load on the Tango control
    // plane; less often than TTL/2 leaves no room for one lost round trip.
    if(renew_interval_ms < lease_ttl_ms / 10 || renew_interval_ms > lease_ttl_ms / 2)
    {
        return Status::RenewTooFrequent;
    }

    // 3.7's rate limit.  Below two, a client that renews on schedule and once
    // more after a lost reply would be refused; above the cap it stops being a
    // limit.
    if(max_renewals_per_ttl < k_min_renewals_per_ttl ||
       max_renewals_per_ttl > k_max_renewals_per_ttl)
    {
        return Status::RenewTooFrequent;
    }


    // A negative value other than -1 is not "unpinned", it is a typo. Rejecting
    // it is the difference between a field that is off and a field that is
    // ignored -- see docs/EXTRACTION.md on engine_cpu_affinity.
    if(engine_cpu_affinity < -1)
    {
        return Status::MalformedMessage;
    }

    // 6.1: DropOldest is illegal on the producer ring.  A slot already handed to
    // UCX cannot be reclaimed, so offering the option would be offering
    // corruption -- silently, and only under load.
    if(drop_policy == DropPolicy::DropOldest)
    {
        return Status::MalformedMessage;
    }

    return Status::Ok;
}

Status SubscriberConfig::validate() const noexcept
{
    const Status common = validate_common(
        stream_name, max_frame_bytes, ring_depth, credit_window, pinned_memory_limit_bytes);
    if(common != Status::Ok)
    {
        return common;
    }

    if(delivery_queue_depth < k_min_delivery_queue || delivery_queue_depth > k_max_delivery_queue)
    {
        return Status::DepthTooLarge;
    }


    // A negative value other than -1 is not "unpinned", it is a typo. Rejecting
    // it is the difference between a field that is off and a field that is
    // ignored -- see docs/EXTRACTION.md on engine_cpu_affinity.
    if(engine_cpu_affinity < -1)
    {
        return Status::MalformedMessage;
    }

    const bool has_receive_buffer = static_cast<bool>(receive_buffer);
    if(has_receive_buffer != (receive_buffer_bytes != 0))
    {
        return Status::MalformedMessage;
    }

    if(!has_receive_buffer && receive_memory_kind != MemoryKind::Host)
    {
        return Status::MalformedMessage;
    }

    if(receive_memory_kind != MemoryKind::Host && receive_memory_kind != MemoryKind::Cuda &&
       receive_memory_kind != MemoryKind::Rocm)
    {
        return Status::MalformedMessage;
    }

    if(has_receive_buffer &&
       receive_buffer_bytes < max_frame_bytes * static_cast<std::uint64_t>(ring_depth))
    {
        return Status::ResourceExhausted;
    }

    // Both drop policies are legal here: the delivery queue holds views the
    // application has not taken yet, and dropping the oldest of those is a
    // choice about which frames matter, not a memory-safety question.

    return Status::Ok;
}

} // namespace TangoBulk
