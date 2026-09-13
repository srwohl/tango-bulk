// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_TANGO_COMMAND_DISCOVERY_H
#define TANGO_BULK_SRC_TANGO_COMMAND_DISCOVERY_H

#include <tango-bulk/tango.h>

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

// How a client recognises the bulk commands whatever a device named them.
// Includes no tango/* header and nothing from src/core, so the Tango test
// binary can name it with only the public headers on its path.

namespace TangoBulk::detail
{

/// The argument descriptions the three commands are installed with. They are
/// how a client recognises the commands whatever the device named them, so
/// they are part of the adapter's contract with itself and never change.
inline constexpr const char *k_open_request_description = "Encoded tango-bulk Open request";
inline constexpr const char *k_renew_request_description = "Encoded tango-bulk Renew request";
inline constexpr const char *k_close_request_description = "Encoded tango-bulk Close request";
inline constexpr const char *k_reply_description = "Encoded tango-bulk coordination reply";

/// One command as `DeviceProxy::command_list_query()` reports it, reduced to
/// the facts the matcher needs.
struct CommandDescriptor
{
    std::string name;
    bool char_array_in{false};  ///< argument type is DevVarCharArray
    bool char_array_out{false}; ///< result type is DevVarCharArray
    std::string in_description;
    std::string out_description;
};

/// Recover the three bulk command names from a device's command list.
///
/// Ok when each role is claimed by exactly one command; UnknownStream when a
/// role is missing, which is what a device with no bulk stream looks like;
/// MalformedMessage when two commands claim one role.
Status match_bulk_commands(const std::vector<CommandDescriptor> &commands,
                           CommandNames &names) noexcept;

/// Recognise the three bulk commands on a live device by the descriptions
/// above. One `command_list_query()` bounded by `deadline` (or
/// `fallback_timeout_ms` when there is none). Throws EstablishmentError:
/// UnknownStream when the device exposes no bulk commands.
CommandNames discover_command_names(Tango::DeviceProxy &proxy,
                                    std::chrono::steady_clock::time_point deadline,
                                    std::uint32_t fallback_timeout_ms);

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_TANGO_COMMAND_DISCOVERY_H
