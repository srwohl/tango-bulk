// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <core/subscription_internal.h>

#include <utility>

namespace TangoBulk::detail
{

std::unique_ptr<Subscription> SubscriptionFactory::open_default(
    SubscriberConfig config,
    CoordinationChannel channel,
    SubscriptionCallbacks callbacks,
    std::chrono::steady_clock::time_point establishment_deadline)
{
    return open(std::move(config),
                std::move(channel),
                [](const SubscriberConfig &for_session,
                   std::shared_ptr<DeliveryIngress> delivery)
                { return make_subscriber_transport(for_session, std::move(delivery)); },
                std::move(callbacks),
                establishment_deadline);
}

} // namespace TangoBulk::detail
