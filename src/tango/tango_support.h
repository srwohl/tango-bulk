// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_TANGO_TANGO_SUPPORT_H
#define TANGO_BULK_SRC_TANGO_TANGO_SUPPORT_H

#include <string>

// Internal header of the Tango layer.  Like its UCX counterpart it includes no
// tango/* header of its own, so a test can check the adapter without inheriting
// cppTango's include surface.

namespace TangoBulk::detail
{

/// Version of the cppTango headers this layer was compiled against.
///
/// tango-bulk builds against an installed cppTango on purpose; recording which
/// one is the cheapest way to make a mismatch legible in a bug report.
std::string tango_headers_version();

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_TANGO_TANGO_SUPPORT_H
