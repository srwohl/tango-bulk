// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SUBSCRIBER_H
#define TANGO_BULK_SUBSCRIBER_H

#include <tango-bulk/errors.h>
#include <tango-bulk/frame.h>
#include <tango-bulk/geometry.h>

#include <array>
#include <cstdint>
#include <exception>
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
/// A value supplied in SubscriptionOptions is a complete upper limit. A value
/// returned by Subscription::plan() is the actual grant-backed plan. The byte
/// count is derived so it cannot disagree with the dimensions.
struct ReceivePlan
{
    std::uint64_t max_frame_bytes{0};
    std::uint32_t ring_depth{0};
    std::uint32_t credit_window{0};

    Status validate() const noexcept;
    std::uint64_t pinned_bytes() const noexcept;
};

bool operator==(const ReceivePlan &left, const ReceivePlan &right) noexcept;
bool operator!=(const ReceivePlan &left, const ReceivePlan &right) noexcept;

enum class RecoveryPolicy : std::uint32_t
{
    Fail = 0,
    Reconnect = 1,
};

/// Which delivered frame a Subscription preserves when its delivery queue is
/// full. Local to the Subscription; it never changes the Session's flow.
enum class QueuePolicy : std::uint32_t
{
    PreserveOrder = 0, ///< keep the frames already queued; refuse the new one
    PreferFresh = 1,   ///< evict the oldest queued frame; requires Copy
};

/// Who owns the bytes of a delivered frame.
enum class DeliveryOwnership : std::uint32_t
{
    /// The frame is its receive slot. The slot's credit returns when the last
    /// view of the frame is released, so every retained frame withholds one.
    Borrow = 0,
    /// The frame is a copy made on the engine thread before it is queued. The
    /// slot's credit returns at delivery, so retention never reaches the
    /// publisher; the copy pool falls back to the heap, counted, when the
    /// application retains more frames than it holds.
    Copy = 1,
};

/// What the caller expects the granted Geometry to describe. Every term is
/// optional and only the supplied terms are checked; a mismatch fails
/// establishment, and a mismatch on reopen ends the Subscription.
struct GeometryExpectation
{
    std::optional<ElementType> element_type;
    std::optional<std::uint32_t> rank;
    std::optional<std::array<std::uint64_t, k_max_rank>> shape;   ///< requires rank
    std::optional<std::array<std::uint64_t, k_max_rank>> strides; ///< requires rank

    Status validate() const noexcept;

    /// Ok, or GeometryMismatch naming the first term that differs.
    Status check(const Geometry &granted) const noexcept;
};

/// Caller-owned receive storage, as the receive allocator returns it.
///
/// `owner` keeps the memory alive: the library holds a copy for as long as UCX
/// or any delivered frame can refer to the region, so releasing the caller's
/// copy never frees memory still in use. For CUDA or ROCm, `owner.get()` is the
/// device pointer.
struct ReceiveRegion
{
    std::shared_ptr<void> owner;
    std::uint64_t bytes{0};
    MemoryKind memory_kind{MemoryKind::Host};
};

/// Asked for at most once, during initial establishment, with the number of
/// bytes the receive plan needs. A region smaller than that fails establishment
/// with ResourceExhausted. Reconnect reuses the region only while nothing else
/// refers to it, and never asks again.
using ReceiveAllocator = std::function<ReceiveRegion(std::uint64_t bytes)>;

/// What a push callback receives: one delivered frame, or the terminal outcome
/// of the Subscription. `error` is set exactly once, on the last event, and
/// holds the same typed exception a pull reader would have caught.
struct FrameEvent
{
    FrameView frame;
    std::exception_ptr error;

    bool terminal() const noexcept
    {
        return error != nullptr;
    }
};

using FrameCallback = std::function<void(FrameEvent)>;

/// Everything a caller decides about a Subscription. Supplying `on_frame`
/// selects push delivery; leaving it empty selects pull.
struct SubscriptionOptions
{
    std::string stream_name;

    /// How much registered memory this Subscription may hold, including rings
    /// retained from earlier Sessions. Ring depth and frame size derive from it.
    std::uint64_t pinned_budget_bytes{1ull << 30};

    RecoveryPolicy recovery_policy{RecoveryPolicy::Reconnect};
    QueuePolicy queue_policy{QueuePolicy::PreserveOrder};
    /// Copy is refused with a device receive region: the copy destination
    /// would have to be host memory the caller has no way to supply.
    DeliveryOwnership ownership{DeliveryOwnership::Borrow};
    GeometryExpectation expect;

    /// Optional complete upper plan, intersected with discovery and the pinned
    /// budget before Open. When absent those dimensions derive entirely from
    /// discovery and the budget.
    std::optional<ReceivePlan> receive_plan;

    ReceiveAllocator receive_allocator;
    FrameCallback on_frame;

    /// One deadline for discovery, allocation, Open, Probe and transient retries.
    std::uint32_t establishment_timeout_ms{30'000};

    Status validate() const noexcept;
};

} // namespace TangoBulk

#endif // TANGO_BULK_SUBSCRIBER_H
