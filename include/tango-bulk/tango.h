// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_TANGO_H
#define TANGO_BULK_TANGO_H

#include <tango-bulk/publisher.h>
#include <tango-bulk/subscriber.h>
#include <tango-bulk/subscription.h>

#include <cstdint>
#include <string>
#include <vector>

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
class Attr;
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

    /// "Xyz" -> "XyzBulkOpen", and so on.
    static CommandNames with_prefix(const std::string &prefix);
};

/// Call from DeviceClass::command_factory().  Appends three commands that
/// dispatch to the BulkPublisher attached to the receiving device.
///
/// BulkOpen and BulkClose are registered at Tango::EXPERT level: BulkOpen
/// causes pinned-memory allocation and BulkClose terminates a data stream, so
/// both are write-level operations under any access control that distinguishes
/// read from write.  BulkRenew is registered at the same level.
void install_bulk_commands(Tango::DeviceClass &device_class,
                           const CommandNames &names = {});

/// Call from DeviceClass::attribute_factory(). Appends fixed, read-only
/// BulkStreams, BulkSessions, BulkFramesPublished, BulkFramesDropped,
/// BulkTransport, and BulkWorstLagFrames attributes. BulkStreams remains the
/// pre-Open discovery marker; every other value is copied from one publisher
/// snapshot. The registry protects publisher lifetime but is never held across I/O.
void install_bulk_attributes(std::vector<Tango::Attr *> &attributes);

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

} // namespace TangoBulk

#endif // TANGO_BULK_TANGO_H
