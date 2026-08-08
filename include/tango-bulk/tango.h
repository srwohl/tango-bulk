// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_TANGO_H
#define TANGO_BULK_TANGO_H

#include <tango-bulk/publisher.h>

#include <string>

// The one public header allowed to name Tango types, and the reason the
// layering check has an explicit exception for it.  Note what is *not* here:
// no ucp/*, and no UCX type in any signature.  A device server that links the
// adapter never inherits UCX headers.
namespace Tango
{
class DeviceClass;
class DeviceImpl;
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

} // namespace TangoBulk

#endif // TANGO_BULK_TANGO_H
