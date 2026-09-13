// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_CORE_SUBSCRIBER_TRANSPORT_H
#define TANGO_BULK_SRC_CORE_SUBSCRIBER_TRANSPORT_H

#include <tango-bulk/counters.h>
#include <tango-bulk/errors.h>
#include <core/protocol.h>
#include <tango-bulk/subscription.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
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
                            const Geometry &granted,
                            const std::vector<std::byte> &server_address) = 0;

    virtual BulkError last_error() const noexcept = 0;

    virtual SubscriberState state() const noexcept = 0;
    virtual SubscriberCounters counters() const noexcept = 0;

  protected:
    SubscriberTransport() = default;
};

/// Expert transport settings. Not part of the stable interface: tests and the
/// benchmark reach them through this seam, applications do not.
struct TransportOptions
{
    std::string ucx_tls;        ///< UCX_TLS; empty lets UCX choose
    int engine_cpu_affinity{-1}; ///< -1 leaves the engine thread unpinned
};

/// Builds one transport per Session. An empty `ReceiveRegion::owner` asks the
/// transport to allocate its own host ring; otherwise it registers the caller's
/// region and shares its ownership. The ownership decides whether the
/// transport delivers its slots or copies of them.
using TransportFactory = std::function<std::unique_ptr<SubscriberTransport>(
    const ReceivePlan &, DeliveryOwnership, ReceiveRegion, std::shared_ptr<DeliveryIngress>)>;

TransportFactory make_subscriber_transport_factory(std::uint64_t pinned_budget_bytes,
                                                   TransportOptions options = {});

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_CORE_SUBSCRIBER_TRANSPORT_H
