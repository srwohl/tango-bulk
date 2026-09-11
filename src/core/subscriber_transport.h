// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_CORE_SUBSCRIBER_TRANSPORT_H
#define TANGO_BULK_SRC_CORE_SUBSCRIBER_TRANSPORT_H

#include <tango-bulk/counters.h>
#include <tango-bulk/errors.h>
#include <tango-bulk/protocol.h>
#include <tango-bulk/subscription.h>

#include <cstddef>
#include <functional>
#include <memory>
#include <vector>

namespace TangoBulk::detail
{

class DeliveryIngress;

class SubscriberTransport
{
  public:
    virtual ~SubscriberTransport() = default;

    SubscriberTransport(const SubscriberTransport &) = delete;
    SubscriberTransport &operator=(const SubscriberTransport &) = delete;

    virtual const std::vector<std::byte> &local_address() const noexcept = 0;

    virtual Status activate(Protocol::StreamId stream_id,
                            const Protocol::GeometryBlock &granted,
                            const std::vector<std::byte> &server_address) = 0;

    virtual BulkError last_error() const noexcept = 0;

    virtual SubscriberState state() const noexcept = 0;
    virtual SubscriberCounters counters() const noexcept = 0;

  protected:
    SubscriberTransport() = default;
};

using TransportFactory = std::function<std::unique_ptr<SubscriberTransport>(
    const ReceivePlan &, std::shared_ptr<DeliveryIngress>)>;

TransportFactory make_subscriber_transport_factory(SubscriberConfig config);

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_CORE_SUBSCRIBER_TRANSPORT_H
