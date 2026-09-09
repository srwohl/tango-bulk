// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/publisher.h>
#include <tango-bulk/subscriber.h>

#include <tango-bulk/limits.h>

#include <core/pinned_ledger.h>

#include <cstdint>
#include <algorithm>
#include <limits>

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

    if(frame_metadata.element_type != ElementType::Unknown || frame_metadata.rank != 0)
    {
        FrameMetadata resolved = frame_metadata;
        if(const Status status = resolved.resolve(max_frame_bytes); status != Status::Ok)
            return status;
    }

    if(max_sessions < 1 || max_sessions > k_max_sessions)
    {
        return Status::DepthTooLarge;
    }

    if(fanout_mode != FanoutMode::BestEffort && fanout_mode != FanoutMode::AllActive)
    {
        return Status::MalformedMessage;
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

    return Status::Ok;
}

Status SubscriberConfig::validate() const noexcept
{
    if(receive_plan)
    {
        const Status plan_status = receive_plan->validate();
        if(plan_status != Status::Ok)
        {
            return plan_status;
        }
    }

    if(discovery_offer)
    {
        if(discovery_offer->stream_name != stream_name)
        {
            return Status::MalformedMessage;
        }

        if(const Status offer_status = discovery_offer->validate();
           offer_status != Status::Ok)
        {
            return offer_status;
        }
    }

    const ReceivePlan upper = upper_receive_plan();
    const ReceivePlan requested = receive_plan.value_or(
        ReceivePlan::from_limits(max_frame_bytes, ring_depth, credit_window));
    if(requested.validate() == Status::Ok && upper.max_frame_bytes == 0)
    {
        // An otherwise valid request that cannot retain even the minimum ring
        // is a budget failure, not a malformed geometry. Keeping that
        // distinction here prevents adapters from turning it into a generic
        // frame-size error later.
        return Status::ResourceExhausted;
    }

    const Status common = validate_common(stream_name,
                                          upper.max_frame_bytes,
                                          upper.ring_depth,
                                          upper.credit_window,
                                          pinned_memory_limit_bytes);
    if(common != Status::Ok)
    {
        return common;
    }

    if(delivery_queue_depth < k_min_delivery_queue || delivery_queue_depth > k_max_delivery_queue)
    {
        return Status::DepthTooLarge;
    }

    if(establishment_timeout_ms == 0)
    {
        return Status::MalformedMessage;
    }

    if(command_timeout_ms == 0 || probe_timeout_ms == 0)
    {
        return Status::MalformedMessage;
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

    if(upper.pinned_bytes > pinned_memory_limit_bytes)
    {
        return Status::ResourceExhausted;
    }

    if(has_receive_buffer && receive_buffer_bytes < upper.pinned_bytes)
    {
        return Status::ResourceExhausted;
    }

    if(has_receive_buffer && receive_buffer_bytes > pinned_memory_limit_bytes)
    {
        return Status::ResourceExhausted;
    }

    // Both drop policies are legal here: the delivery queue holds views the
    // application has not taken yet, and dropping the oldest of those is a
    // choice about which frames matter, not a memory-safety question.

    return Status::Ok;
}

ReceivePlan SubscriberConfig::upper_receive_plan() const noexcept
{
    const bool has_explicit_plan = receive_plan.has_value();
    ReceivePlan upper = receive_plan.value_or(
        ReceivePlan::from_limits(max_frame_bytes, ring_depth, credit_window));

    if(discovery_offer)
    {
        const ReceivePlan discovered =
            ReceivePlan::derive(*discovery_offer, pinned_memory_limit_bytes);
        return ReceivePlan::intersect(upper, discovered);
    }

    // With no discovery source, the configured frame size is the only safe
    // pre-Open upper bound. Reduce its depth to the budget rather than
    // allowing every caller to repeat this division and floor rule.
    if(!has_explicit_plan && upper.max_frame_bytes != 0 &&
       upper.pinned_bytes > pinned_memory_limit_bytes)
    {
        const std::uint64_t budget_depth =
            pinned_memory_limit_bytes / upper.max_frame_bytes;
        if(budget_depth < k_min_ring_depth)
        {
            return {};
        }

        upper = ReceivePlan::from_limits(
            upper.max_frame_bytes,
            static_cast<std::uint32_t>(std::min<std::uint64_t>(upper.ring_depth, budget_depth)),
            upper.credit_window);
        upper.credit_window = std::min(upper.credit_window, upper.ring_depth);
    }

    return upper;
}

Status ReceivePlan::validate() const noexcept
{
    std::uint64_t required_bytes = 0;
    if(!detail::PinnedLedger::checked_bytes(max_frame_bytes, ring_depth, required_bytes))
    {
        return Status::ResourceExhausted;
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

    if(pinned_bytes != required_bytes)
    {
        return Status::ResourceExhausted;
    }

    return Status::Ok;
}

ReceivePlan ReceivePlan::from_limits(std::uint64_t max_frame_bytes,
                                     std::uint32_t ring_depth,
                                     std::uint32_t credit_window) noexcept
{
    ReceivePlan out;
    out.max_frame_bytes = max_frame_bytes;
    out.ring_depth = ring_depth;
    out.credit_window = credit_window;

    if(!detail::PinnedLedger::checked_bytes(max_frame_bytes, ring_depth, out.pinned_bytes))
    {
        // Keep the value observably invalid.  Returning a wrapped byte count
        // would allow a caller to mistake an overflowing upper plan for a
        // valid, small allocation.
        out.pinned_bytes = std::numeric_limits<std::uint64_t>::max();
    }

    return out;
}

ReceivePlan ReceivePlan::intersect(const ReceivePlan &upper, const Geometry &grant) noexcept
{
    if(upper.validate() != Status::Ok || grant.validate() != Status::Ok)
    {
        return {};
    }

    return from_limits(std::min(upper.max_frame_bytes, grant.max_frame_bytes),
                       std::min(upper.ring_depth, grant.ring_depth),
                       std::min(upper.credit_window, grant.credit_window));
}

ReceivePlan ReceivePlan::intersect(const ReceivePlan &upper,
                                   const ReceivePlan &grant) noexcept
{
    if(upper.validate() != Status::Ok || grant.validate() != Status::Ok)
    {
        return {};
    }

    return from_limits(std::min(upper.max_frame_bytes, grant.max_frame_bytes),
                       std::min(upper.ring_depth, grant.ring_depth),
                       std::min(upper.credit_window, grant.credit_window));
}

ReceivePlan ReceivePlan::intersect(const ReceivePlan &upper, const StreamOffer &offer) noexcept
{
    if(offer.validate() != Status::Ok)
    {
        return {};
    }

    return intersect(upper, offer.geometry);
}

ReceivePlan ReceivePlan::derive(const Geometry &geometry,
                                std::uint64_t pinned_memory_limit_bytes) noexcept
{
    if(geometry.validate() != Status::Ok)
    {
        return {};
    }

    ReceivePlan out = from_limits(
        geometry.max_frame_bytes, geometry.ring_depth, geometry.credit_window);

    if(out.max_frame_bytes == 0)
    {
        return out;
    }

    const std::uint64_t budget_depth = pinned_memory_limit_bytes / out.max_frame_bytes;
    out.ring_depth = static_cast<std::uint32_t>(std::min<std::uint64_t>(
        out.ring_depth, std::numeric_limits<std::uint32_t>::max()));
    out.ring_depth = static_cast<std::uint32_t>(std::min<std::uint64_t>(out.ring_depth,
                                                                         budget_depth));
    if(out.ring_depth < k_min_ring_depth)
    {
        return {};
    }
    out.credit_window = std::min(out.credit_window, out.ring_depth);
    return from_limits(out.max_frame_bytes, out.ring_depth, out.credit_window);
}

ReceivePlan ReceivePlan::derive(const StreamOffer &offer,
                                std::uint64_t pinned_memory_limit_bytes) noexcept
{
    if(offer.validate() != Status::Ok)
    {
        return {};
    }

    return derive(offer.geometry, pinned_memory_limit_bytes);
}

bool operator==(const ReceivePlan &left, const ReceivePlan &right) noexcept
{
    return left.max_frame_bytes == right.max_frame_bytes &&
           left.ring_depth == right.ring_depth && left.credit_window == right.credit_window &&
           left.pinned_bytes == right.pinned_bytes;
}

bool operator!=(const ReceivePlan &left, const ReceivePlan &right) noexcept
{
    return !(left == right);
}

} // namespace TangoBulk
