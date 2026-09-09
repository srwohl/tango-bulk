// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_TANGO_BULK_COMMANDS_H
#define TANGO_BULK_SRC_TANGO_BULK_COMMANDS_H

#include <core/publisher_internal.h>

#include <tango-bulk/tango.h>

#include <cstddef>
#include <vector>

// Internal header of the Tango layer: the seam between the command classes
// (commands.cpp) and the registration/attachment they need (registration.cpp).
//
// It includes no tango/* header of its own. The command classes name Tango
// types in their own translation unit; everything crossing this header is a
// byte vector or a plain handle.

namespace Tango
{
class Command;
} // namespace Tango

namespace TangoBulk::detail
{

/// Run one coordination request against whatever is attached to `device`.
///
/// Never throws: this is called from a Tango command implementation, where an
/// escaping C++ exception is a device server crash rather than a `DevFailed`.
/// A protocol-level failure comes back as an encoded `Error` (7.1).
std::vector<std::byte> dispatch_coordination(Tango::DeviceImpl *device,
                                             const std::byte *data,
                                             std::size_t size,
                                             std::optional<Protocol::CoordType> expected =
                                                 std::nullopt) noexcept;

/// One command object per name, ready for `DeviceClass::get_command_list()`.
///
/// Ownership passes to the caller, and from there to the command list, which
/// cppTango deletes with the class.
std::vector<Tango::Command *> make_bulk_commands(const CommandNames &names);

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_TANGO_BULK_COMMANDS_H
