// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SUBSCRIPTION_H
#define TANGO_BULK_SUBSCRIPTION_H

#include <tango-bulk/counters.h>
#include <tango-bulk/errors.h>
#include <tango-bulk/protocol.h>
#include <tango-bulk/subscriber.h>
#include <tango-bulk/unstable/subscriber_transport.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

namespace TangoBulk
{
namespace detail
{

using CoordinationChannel =
    std::function<std::vector<std::byte>(Protocol::CoordType, const std::vector<std::byte> &)>;

using TransportFactory = std::function<std::unique_ptr<SubscriberTransport>(
    const SubscriberConfig &, std::shared_ptr<DeliveryQueue>)>;

} // namespace detail

struct SubscriptionCallbacks
{
    FrameCallback on_frame;
    StateCallback on_state;
};

std::chrono::milliseconds backoff_delay(std::uint32_t attempt,
                                        std::uint32_t backoff_ms,
                                        std::uint32_t lease_ttl_ms) noexcept;

class Subscription
{
  public:
    static std::unique_ptr<Subscription> open(SubscriberConfig config,
                                              detail::CoordinationChannel channel,
                                              detail::TransportFactory factory,
                                              SubscriptionCallbacks callbacks);

    ~Subscription();

    Subscription(const Subscription &) = delete;
    Subscription &operator=(const Subscription &) = delete;

    std::size_t poll(std::chrono::milliseconds timeout, std::size_t max_frames = 0);

    SubscriberState state() const noexcept;

    int fd() const noexcept;

    Protocol::GeometryBlock granted_geometry() const noexcept;

    std::uint32_t generation() const noexcept;
    SubscriberCounters counters() const noexcept;

  private:
    struct Impl;
    explicit Subscription(std::unique_ptr<Impl> impl) noexcept;

    std::unique_ptr<Impl> impl_;
};

std::unique_ptr<Subscription> open_subscription(SubscriberConfig config,
                                                detail::CoordinationChannel channel,
                                                SubscriptionCallbacks callbacks);

} // namespace TangoBulk

#endif // TANGO_BULK_SUBSCRIPTION_H
