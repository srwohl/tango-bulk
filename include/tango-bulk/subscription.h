// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SUBSCRIPTION_H
#define TANGO_BULK_SUBSCRIPTION_H

#include <tango-bulk/counters.h>
#include <tango-bulk/errors.h>
#include <tango-bulk/subscriber.h>

#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>

namespace TangoBulk
{
namespace detail
{
class SubscriptionFactory;
} // namespace detail

struct SubscriptionCallbacks
{
    FrameCallback on_frame;
};

struct SubscriptionSnapshot
{
    SubscriberState state{SubscriberState::Closed};
    BulkError error{};
    SubscriberCounters counters{};
    bool interrupted{false};
};

class Subscription
{
  public:
    ~Subscription();

    Subscription(const Subscription &) = delete;
    Subscription &operator=(const Subscription &) = delete;

    void close() noexcept;

    void interrupt() noexcept;

    /// Claim one frame without registering a callback. Returns no value only
    /// when no frame is ready; terminal lifecycle outcomes are reported as a
    /// BulkException. Only available for DeliveryMode::Pull.
    std::optional<FrameView> try_read();

    /// Claim one frame, waiting up to `timeout`. A timeout is represented by
    /// an empty optional and is distinct from close, interruption, and failure.
    /// Only available for DeliveryMode::Pull.
    std::optional<FrameView> read_for(std::chrono::milliseconds timeout);

    SubscriberState state() const noexcept;

    int fd() const noexcept;

    Geometry geometry() const noexcept;

    ReceivePlan plan() const noexcept;

    SubscriptionSnapshot snapshot() const noexcept;

    SubscriberCounters counters() const noexcept;

  private:
    friend class detail::SubscriptionFactory;

    struct Impl;
    explicit Subscription(std::shared_ptr<Impl> impl) noexcept;

    std::shared_ptr<Impl> impl_;
};

} // namespace TangoBulk

#endif // TANGO_BULK_SUBSCRIPTION_H
