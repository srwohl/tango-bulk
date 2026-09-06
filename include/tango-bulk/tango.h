// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_TANGO_H
#define TANGO_BULK_TANGO_H

#include <tango-bulk/publisher.h>
#include <tango-bulk/subscriber.h>
#include <tango-bulk/subscription.h>

#include <array>
#include <cstdint>
#include <string>

// The one public header allowed to name Tango types, and the reason the
// layering check has an explicit exception for it.  Note what is *not* here:
// no ucp/*, and no UCX type in any signature.  A device server that links the
// adapter never inherits UCX headers.
#include <memory>

namespace Tango
{
class DeviceClass;
class DeviceImpl;
class DeviceProxy;
} // namespace Tango

namespace TangoBulk
{

/// Command names, overridable for devices that already own one of these names.
///
/// Unprefixed is the normal path: the names are discoverable and match the
/// documentation.  The override exists for collisions, not as a style choice.
struct CommandNames
{
    std::string open{"BulkOpen"};
    std::string renew{"BulkRenew"};
    std::string close{"BulkClose"};
    std::string query{"BulkQuery"};

    /// "Xyz" -> "XyzBulkOpen", and so on.
    static CommandNames with_prefix(const std::string &prefix);
};

/// Call from DeviceClass::command_factory().  Appends four commands that
/// dispatch to the BulkPublisher attached to the receiving device.
///
/// BulkOpen and BulkClose are registered at Tango::EXPERT level: BulkOpen
/// causes pinned-memory allocation and BulkClose terminates a data stream, so
/// both are write-level operations under any access control that distinguishes
/// read from write.  BulkQuery is read-only and is registered at OPERATOR.
void install_bulk_commands(Tango::DeviceClass &device_class,
                           const CommandNames &names = {});

/// Call from DeviceImpl::init_device(), so the installed commands can find the
/// publisher.  Detach in delete_device().
///
/// A command invoked on a device with no attached publisher returns an encoded
/// Error{UnknownStream} rather than throwing: a device server that has not yet
/// finished init_device() is a normal transient state, not a fault.
void attach_publisher(Tango::DeviceImpl &device, BulkPublisher &publisher);
void detach_publisher(Tango::DeviceImpl &device) noexcept;

///
///
std::unique_ptr<Subscription> subscribe(Tango::DeviceProxy &proxy,
                                        SubscriberConfig config,
                                        SubscriptionCallbacks callbacks,
                                        const CommandNames &names = {});

/// What BulkQuery reports about a publisher.
struct BulkQueryResult
{
    Status status{Status::Ok};
    std::uint32_t active_sessions{0};
    std::uint32_t generation{0};
    std::uint64_t max_frame_bytes{0};
    std::uint32_t ring_depth{0};
    std::uint32_t credit_window{0};

    /// The declared frame layout.  These fields come from the publisher's
    /// current geometry epoch; clients should not have to wait for a first
    /// frame before sizing or laying out their destination.
    ElementType element_type{ElementType::Unknown};
    std::uint32_t element_size{0};
    std::uint32_t rank{0};
    std::array<std::uint64_t, k_max_rank> shape{};
    std::array<std::uint64_t, k_max_rank> strides{};

    /// `key=value;` pairs.  Free-form by design: an operator reads it, and the
    /// set of counters may grow within a minor version.  It carries no UCX
    /// address, no memory key, and no untruncated session identifier.
    std::string counters;
};

/// Server-wide status of a device's bulk publisher, over the ordinary BulkQuery
/// command.
///
/// Server-wide because that is the question an operator has.  3.8 also allows a
/// Query naming one session, which needs a `session_id` no public API hands out;
/// that path is driven through `handle_coordination` and is covered by the UCX
/// tests, which decode the `OpenReply` themselves.
///
/// Throws BulkException on a malformed reply, and lets Tango::DevFailed out of
/// the command call itself -- a device that cannot be reached is the caller's
/// problem to handle, not something to flatten into a status code.
BulkQueryResult bulk_query(Tango::DeviceProxy &proxy, const CommandNames &names = {});

} // namespace TangoBulk

#endif // TANGO_BULK_TANGO_H
