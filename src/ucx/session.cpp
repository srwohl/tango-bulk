// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/subscription.h>

#include <memory>
#include <utility>

namespace TangoBulk
{

std::unique_ptr<Subscription> open_subscription(SubscriberConfig config,
                                                detail::CoordinationChannel channel,
                                                SubscriptionCallbacks callbacks)
{
    return Subscription::open(
        std::move(config),
        std::move(channel),
        [](const SubscriberConfig &for_session, std::shared_ptr<detail::DeliveryQueue> delivery)
        { return detail::make_subscriber_transport(for_session, std::move(delivery)); },
        std::move(callbacks));
}

} // namespace TangoBulk
