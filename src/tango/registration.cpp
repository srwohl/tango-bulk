// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/tango.h>

// M0 defines only CommandNames.  install_bulk_commands(), attach_publisher()
// and detach_publisher() are declared in <tango-bulk/tango.h> because the API
// is normative (IMPLEMENTATION_SPEC.md 7.2), and are deliberately left
// undefined until M4 rather than stubbed: an unresolved symbol says
// "not implemented yet" at build time, where a throwing stub would say it at
// 3 a.m. in a device server.

namespace TangoBulk
{

CommandNames CommandNames::with_prefix(const std::string &prefix)
{
    CommandNames names;
    names.open = prefix + names.open;
    names.renew = prefix + names.renew;
    names.close = prefix + names.close;
    names.query = prefix + names.query;
    return names;
}

} // namespace TangoBulk
