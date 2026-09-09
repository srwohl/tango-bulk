// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SUBSCRIBER_H
#define TANGO_BULK_SUBSCRIBER_H

#include <tango-bulk/counters.h>
#include <tango-bulk/errors.h>
#include <tango-bulk/frame.h>
#include <tango-bulk/geometry.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>

namespace TangoBulk
{

enum class SubscriberState : std::uint32_t
{
    Closed = 0,
    Opening = 1,
    Probing = 2,
    Active = 3,
    Reconnecting = 4,
    Failed = 5,
};

const char *to_string(SubscriberState state) noexcept;

/// The receive dimensions selected for a subscription.
///
/// A value supplied in SubscriberConfig is a complete upper limit. A value
/// returned by Subscription::plan() is the actual grant-backed plan. In both
/// cases the byte count is derived here so every storage adapter shares one
/// interpretation of the ring.
struct ReceivePlan
{
    std::uint64_t max_frame_bytes{0};
    std::uint32_t ring_depth{0};
    std::uint32_t credit_window{0};
    std::uint64_t pinned_bytes{0};

    Status validate() const noexcept;

    static ReceivePlan from_limits(std::uint64_t max_frame_bytes,
                                   std::uint32_t ring_depth,
                                   std::uint32_t credit_window) noexcept;

    static ReceivePlan intersect(const ReceivePlan &upper,
                                 const Geometry &grant) noexcept;

    static ReceivePlan intersect(const ReceivePlan &upper,
                                 const ReceivePlan &grant) noexcept;

    static ReceivePlan intersect(const ReceivePlan &upper,
                                 const StreamOffer &offer) noexcept;

    static ReceivePlan derive(const Geometry &geometry,
                              std::uint64_t pinned_memory_limit_bytes) noexcept;

    static ReceivePlan derive(const StreamOffer &offer,
                              std::uint64_t pinned_memory_limit_bytes) noexcept;
};

bool operator==(const ReceivePlan &left, const ReceivePlan &right) noexcept;
bool operator!=(const ReceivePlan &left, const ReceivePlan &right) noexcept;

enum class DeliveryMode : std::uint32_t
{
    Push = 0, ///< a dedicated library thread invokes the callback (default)
    Pull = 1, ///< the application claims frames with Subscription::read_for()
};

enum class ReconnectPolicy : std::uint32_t
{
    FailFast = 0,
    BoundedRetry = 1,
};

struct SubscriberConfig
{
    std::string stream_name;
    std::uint64_t max_frame_bytes{8ull << 20};
    std::uint32_t ring_depth{32};
    std::uint32_t credit_window{16};
    std::uint32_t delivery_queue_depth{64};
    DeliveryMode delivery_mode{DeliveryMode::Push};
    DropPolicy drop_policy{DropPolicy::DropNewest};
    ReconnectPolicy reconnect_policy{ReconnectPolicy::BoundedRetry};
    std::uint32_t reconnect_max_attempts{10};
    std::uint32_t reconnect_backoff_ms{500}; ///< exponential, capped at lease TTL
    std::uint32_t command_timeout_ms{5'000}; ///< one Tango command round trip
    std::uint32_t establishment_timeout_ms{30'000}; ///< total initial subscribe budget

    /// How long to wait for the publisher's `Probe` after `Open` is granted.
    ///
    /// Split from `command_timeout_ms`, which it used to share: one bounds a
    /// Tango call, the other bounds a UCX round trip that the publisher
    /// initiates, and they are only alike in having had the same default. A
    /// client whose UCX endpoint the publisher cannot reach spends this budget
    /// once per reconnect attempt before it is told, so lowering it is what
    /// makes an unreachable fabric quick to diagnose rather than slow.
    std::uint32_t probe_timeout_ms{5'000};
    std::uint64_t pinned_memory_limit_bytes{1ull << 30};
    std::string ucx_tls;
    int engine_cpu_affinity{-1};

    /// Optional caller-owned receive ring. When null, the subscriber allocates
    /// its usual host ring. For CUDA/ROCm, supply a shared_ptr whose get() is the
    /// device pointer and whose deleter releases it. The allocation must contain
    /// at least max_frame_bytes * ring_depth bytes. Shared ownership keeps it
    /// alive until every FrameView has been released, even after stop().
    std::shared_ptr<void> receive_buffer;
    std::uint64_t receive_buffer_bytes{0};
    MemoryKind receive_memory_kind{MemoryKind::Host};

    /// Optional complete upper plan. When present it replaces the individual
    /// sizing fields for preparation and Open, and is still intersected with
    /// discovery and the pinned budget.
    std::optional<ReceivePlan> receive_plan;

    /// Optional conservative pre-Open discovery. The offer is only used to
    /// choose a safe upper request; OpenReply remains authoritative for the
    /// actual geometry and lease.
    std::optional<StreamOffer> discovery_offer;

    ReceivePlan upper_receive_plan() const noexcept;
    Status validate() const noexcept;
};

using FrameCallback = std::function<void(FrameView)>;

} // namespace TangoBulk

#endif // TANGO_BULK_SUBSCRIBER_H
