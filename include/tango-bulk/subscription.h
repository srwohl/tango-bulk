// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SUBSCRIPTION_H
#define TANGO_BULK_SUBSCRIPTION_H

#include <tango-bulk/counters.h>
#include <tango-bulk/errors.h>
#include <tango-bulk/subscriber.h>

#include <chrono>
#include <memory>
#include <optional>
#include <string>

namespace TangoBulk
{
namespace detail
{
class SubscriptionFactory;
} // namespace detail

/// One observation of a Subscription. Sampled together and stamped with the
/// steady-clock time it was taken; never authority for anything.
struct SubscriptionSnapshot
{
    SubscriberState state{SubscriberState::Closed};
    bool interrupted{false};
    BulkError error{};
    SubscriberCounters counters{};

    std::string session_id; ///< log form of the current Session, empty when none
    std::uint32_t lease_ttl_ms{0};
    std::uint32_t renew_interval_ms{0};
    std::uint64_t last_renewal_steady_ns{0};   ///< 0 until a renewal succeeds
    std::uint64_t max_renewal_lateness_ms{0};  ///< worst delay past the scheduled renewal
    std::uint64_t sampled_at_steady_ns{0};
};

class Subscription
{
  public:
    ~Subscription();

    Subscription(const Subscription &) = delete;
    Subscription &operator=(const Subscription &) = delete;

    void close() noexcept;

    void interrupt() noexcept;

    /// Claim one frame without waiting. Empty only when no frame is ready;
    /// terminal outcomes throw their typed exception. Pull delivery only.
    std::optional<FrameView> try_read();

    /// Claim one frame, waiting up to `timeout`. Empty only on expiry, which is
    /// distinct from close, interruption and failure. Pull delivery only.
    std::optional<FrameView> read_for(std::chrono::milliseconds timeout);

    /// Advisory readiness descriptor for pull delivery, stable across
    /// reconnects; -1 for push delivery.
    int fd() const noexcept;

    Geometry geometry() const noexcept;

    ReceivePlan plan() const noexcept;

    SubscriptionSnapshot snapshot() const noexcept;

  private:
    friend class detail::SubscriptionFactory;

    struct Impl;
    explicit Subscription(std::shared_ptr<Impl> impl) noexcept;

    std::shared_ptr<Impl> impl_;
};

} // namespace TangoBulk

#endif // TANGO_BULK_SUBSCRIPTION_H
