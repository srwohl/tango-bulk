// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_UCX_UCX_SUPPORT_H
#define TANGO_BULK_SRC_UCX_UCX_SUPPORT_H

#include <cstdint>
#include <string>

// Internal header of the UCX layer.  Note that it does not include ucp/api/ucp.h:
// nothing outside src/ucx/ may see a UCX type, and that includes this header's
// own consumers in tests/ucx.

namespace TangoBulk::detail
{

/// UCX release the library is *running* against, as reported by libucp itself.
std::string ucx_runtime_version();

/// UCX API version the library was *compiled* against, packed as
/// major * 1'000'000 + minor * 1'000 + release.
std::uint32_t ucx_compiled_api_version() noexcept;

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_UCX_UCX_SUPPORT_H
