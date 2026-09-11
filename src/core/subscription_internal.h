// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_CORE_SUBSCRIPTION_INTERNAL_H
#define TANGO_BULK_SRC_CORE_SUBSCRIPTION_INTERNAL_H

#include <core/subscriber_transport.h>

#include <tango-bulk/protocol.h>
#include <tango-bulk/subscription.h>

#include <chrono>
#include <functional>
#include <memory>
#include <vector>

namespace TangoBulk::detail
{

/// The private coordination seam shared by the C++ Tango adapter, the Python
/// adapter, and tests. Subscription owns message construction, lifecycle, and
/// recovery; an adapter only exchanges one encoded command before its deadline.
using CoordinationChannel =
    std::function<std::vector<std::byte>(Protocol::CoordType,
                                         const std::vector<std::byte> &,
                                         std::chrono::steady_clock::time_point)>;

/// Private construction access for the deep Subscription module.
///
/// The class is forward-declared in the installed header solely as a friendship
/// key. Its interface stays here so callers cannot construct a Subscription by
/// supplying coordination or transport implementation details.
class SubscriptionFactory
{
  public:
    static std::unique_ptr<Subscription> open(
        SubscriberConfig config,
        StreamOffer offer,
        CoordinationChannel channel,
        TransportFactory transport_factory,
        SubscriptionCallbacks callbacks,
        std::chrono::steady_clock::time_point establishment_deadline);
};

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_CORE_SUBSCRIPTION_INTERNAL_H
