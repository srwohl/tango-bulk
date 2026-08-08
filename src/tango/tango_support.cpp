// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

// Quoted and unqualified on purpose.  ${PROJECT_SOURCE_DIR}/src is on the
// include path, so <tango/...> would be ambiguous between this layer's own
// directory and cppTango's installed headers.  Inside src/tango/ we therefore
// include siblings by bare name and leave the <tango/...> spelling to cppTango.
#include "tango_support.h"

#include <tango/common/versions.h>

namespace TangoBulk::detail
{

std::string tango_headers_version()
{
    return Tango::TgLibVers;
}

} // namespace TangoBulk::detail
