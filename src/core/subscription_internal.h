// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_CORE_SUBSCRIPTION_INTERNAL_H
#define TANGO_BULK_SRC_CORE_SUBSCRIPTION_INTERNAL_H

#include <core/delivery_queue.h>
#include <core/subscriber_transport.h>

#include <tango-bulk/protocol.h>
#include <tango-bulk/subscription.h>

#include <chrono>
#include <functional>
#include <vector>

namespace TangoBulk::detail
{

using CoordinationChannel =
    std::function<std::vector<std::byte>(Protocol::CoordType, const std::vector<std::byte> &)>;

using TransportFactory = std::function<std::unique_ptr<SubscriberTransport>(
    const SubscriberConfig &, std::shared_ptr<DeliveryQueue>)>;

class SubscriptionFactory
{
  public:
    static std::unique_ptr<Subscription> open(SubscriberConfig config,
                                              CoordinationChannel channel,
                                              TransportFactory factory,
                                              SubscriptionCallbacks callbacks);

    static std::unique_ptr<Subscription> open_default(SubscriberConfig config,
                                                      CoordinationChannel channel,
                                                      SubscriptionCallbacks callbacks);
};

std::chrono::milliseconds backoff_delay(std::uint32_t attempt,
                                        std::uint32_t backoff_ms,
                                        std::uint32_t lease_ttl_ms) noexcept;

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_CORE_SUBSCRIPTION_INTERNAL_H
