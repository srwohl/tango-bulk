// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <ucx/ucx_support.h>

#include <catch2/catch_test_macros.hpp>

#include <string>

using namespace TangoBulk::detail;

// Note what this file does NOT include: ucp/api/ucp.h.  A UCX-layer test links
// the transport but sees no UCX type, which is the same boundary the library
// itself keeps.

TEST_CASE("UCX runtime is at or above the supported floor", "[ucx][support]")
{
    const std::string version = ucx_runtime_version();
    INFO("UCX runtime version: " << version);

    REQUIRE_FALSE(version.empty());

    // IMPLEMENTATION_SPEC.md section 0 sets the UCX floor at 1.21.0.  Checking
    // the runtime rather than the headers is deliberate: the headers are
    // whatever CMake found, but the .so is whatever the loader picks, and it is
    // the second one that decides whether a transport exists.
    const auto dot = version.find('.');
    REQUIRE(dot != std::string::npos);

    const int major = std::stoi(version.substr(0, dot));
    const int minor = std::stoi(version.substr(dot + 1));

    CHECK(major >= 1);
    if(major == 1)
    {
        CHECK(minor >= 21);
    }
}

TEST_CASE("compiled UCX API version is reported", "[ucx][support]")
{
    CHECK(ucx_compiled_api_version() >= 1'021'000u);
}
