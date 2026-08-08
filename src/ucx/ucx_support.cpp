// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include "ucx_support.h"

#include <ucp/api/ucp.h>

namespace TangoBulk::detail
{

std::string ucx_runtime_version()
{
    unsigned major = 0;
    unsigned minor = 0;
    unsigned release = 0;

    ucp_get_version(&major, &minor, &release);

    return std::to_string(major) + "." + std::to_string(minor) + "." +
           std::to_string(release);
}

std::uint32_t ucx_compiled_api_version() noexcept
{
    // UCP_API_MAJOR/MINOR come from the headers this file was compiled with.
    // Comparing them against ucx_runtime_version() is how a "works on my
    // machine" transport mismatch surfaces as a message instead of as a
    // segfault inside a progress callback.
    return static_cast<std::uint32_t>(UCP_API_MAJOR) * 1'000'000u +
           static_cast<std::uint32_t>(UCP_API_MINOR) * 1'000u;
}

} // namespace TangoBulk::detail
