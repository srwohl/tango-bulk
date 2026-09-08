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
#include <string>
#include <utility>
#include <vector>

namespace TangoBulk::detail
{

using CoordinationChannel =
    std::function<std::vector<std::byte>(Protocol::CoordType,
                                         const std::vector<std::byte> &,
                                         std::chrono::steady_clock::time_point)>;

using TransportFactory = std::function<std::unique_ptr<SubscriberTransport>(
    const SubscriberConfig &, std::shared_ptr<DeliveryQueue>)>;

/// A small test and embedding adapter for the same seam a real discovery
/// source uses.  It owns copies of offers and never contacts coordination,
/// making it useful to Subscription callers and lifecycle tests without
/// introducing a second establishment module.
class InMemoryStreamDiscovery
{
  public:
    explicit InMemoryStreamDiscovery(std::vector<StreamOffer> offers) :
        offers_(std::move(offers))
    {
    }

    StreamOffer discover(const std::string &stream_name) const
    {
        for(const StreamOffer &offer : offers_)
        {
            if(offer.stream_name == stream_name)
            {
                return offer;
            }
        }

        StreamOffer unavailable;
        unavailable.stream_name = stream_name;
        unavailable.status = Status::UnknownStream;
        unavailable.message = "stream was not present in discovery";
        return unavailable;
    }

  private:
    std::vector<StreamOffer> offers_;
};

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
