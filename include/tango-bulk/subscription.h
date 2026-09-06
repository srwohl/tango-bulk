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

    std::size_t poll(std::chrono::milliseconds timeout, std::size_t max_frames);

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
