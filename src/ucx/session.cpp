// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/unstable/session_supervisor.h>

#include <utility>

/// The one translation unit that knows both halves.
///
/// `SessionSupervisor` lives in core and must not name a UCX symbol;
/// `make_subscriber_transport()` is defined in this library. Joining them is a
/// three-line function, and it belongs here because here is the only place
/// where naming both is legal.
namespace TangoBulk::detail
{

std::unique_ptr<SessionSupervisor> open_session(SubscriberConfig config,
                                                CoordinationChannel channel,
                                                SessionCallbacks callbacks)
{
    return SessionSupervisor::open(
        std::move(config),
        std::move(channel),
        [](const SubscriberConfig &for_session) { return make_subscriber_transport(for_session); },
        std::move(callbacks));
}

} // namespace TangoBulk::detail
