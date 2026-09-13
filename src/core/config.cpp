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
///   stream_name, lease TTL, queue policy, expectation shape -> MalformedMessage / GeometryMismatch
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

Status GeometryExpectation::validate() const noexcept
{
    if(rank && *rank > k_max_rank)
    {
        return Status::GeometryMismatch;
    }

    if((shape || strides) && !rank)
    {
        return Status::GeometryMismatch;
    }

    if(rank)
    {
        for(std::size_t i = *rank; i < k_max_rank; ++i)
        {
            if((shape && (*shape)[i] != 0) || (strides && (*strides)[i] != 0))
            {
                return Status::GeometryMismatch;
            }
        }
    }

    return Status::Ok;
}

Status GeometryExpectation::check(const Geometry &granted) const noexcept
{
    if(element_type && *element_type != granted.element_type)
    {
        return Status::GeometryMismatch;
    }
    if(rank && *rank != granted.rank)
    {
        return Status::GeometryMismatch;
    }
    if(shape && *shape != granted.shape)
    {
        return Status::GeometryMismatch;
    }
    if(strides && *strides != granted.strides)
    {
        return Status::GeometryMismatch;
    }
    return Status::Ok;
}

Status SubscriptionOptions::validate() const noexcept
{
    if(receive_plan)
    {
        const Status plan_status = receive_plan->validate();
        if(plan_status != Status::Ok)
        {
            return plan_status;
        }
    }

    if(!is_valid_stream_name(stream_name))
    {
        return Status::MalformedMessage;
    }

    if(pinned_budget_bytes < k_min_pinned_bytes || pinned_budget_bytes > k_max_pinned_bytes)
    {
        return Status::ResourceExhausted;
    }

    if(establishment_timeout_ms == 0)
    {
        return Status::MalformedMessage;
    }

    if(recovery_policy != RecoveryPolicy::Fail && recovery_policy != RecoveryPolicy::Reconnect)
    {
        return Status::MalformedMessage;
    }

    if(ownership != DeliveryOwnership::Borrow && ownership != DeliveryOwnership::Copy)
    {
        return Status::MalformedMessage;
    }

    if(queue_policy != QueuePolicy::PreserveOrder && queue_policy != QueuePolicy::PreferFresh)
    {
        return Status::MalformedMessage;
    }

    // PreferFresh evicts a queued frame the application never saw. Evicting a
    // borrowed frame would return its credit behind the application's back, so
    // the policy needs copied delivery.
    if(queue_policy == QueuePolicy::PreferFresh && ownership != DeliveryOwnership::Copy)
    {
        return Status::MalformedMessage;
    }

    if(const Status expectation = expect.validate(); expectation != Status::Ok)
    {
        return expectation;
    }

    if(receive_plan && receive_plan->pinned_bytes() > pinned_budget_bytes)
    {
        return Status::ResourceExhausted;
    }

    return Status::Ok;
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

    return Status::Ok;
}

std::uint64_t ReceivePlan::pinned_bytes() const noexcept
{
    std::uint64_t bytes = 0;
    if(!detail::PinnedLedger::checked_bytes(max_frame_bytes, ring_depth, bytes))
    {
        return std::numeric_limits<std::uint64_t>::max();
    }
    return bytes;
}

bool operator==(const ReceivePlan &left, const ReceivePlan &right) noexcept
{
    return left.max_frame_bytes == right.max_frame_bytes &&
           left.ring_depth == right.ring_depth && left.credit_window == right.credit_window;
}

bool operator!=(const ReceivePlan &left, const ReceivePlan &right) noexcept
{
    return !(left == right);
}

} // namespace TangoBulk
