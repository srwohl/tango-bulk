// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/tango.h>

#include "tango_support.h"

#include <catch2/catch_test_macros.hpp>

using namespace TangoBulk;

TEST_CASE("default command names are unprefixed", "[tango][commands]")
{
    const CommandNames names;

    CHECK(names.open == "BulkOpen");
    CHECK(names.renew == "BulkRenew");
    CHECK(names.close == "BulkClose");
    CHECK(names.query == "BulkQuery");
}

TEST_CASE("with_prefix renames all four commands", "[tango][commands]")
{
    // The override exists for devices with a name collision, not as the normal
    // path -- which is why it prefixes rather than letting each name be set
    // independently.
    const CommandNames names = CommandNames::with_prefix("Xyz");

    CHECK(names.open == "XyzBulkOpen");
    CHECK(names.renew == "XyzBulkRenew");
    CHECK(names.close == "XyzBulkClose");
    CHECK(names.query == "XyzBulkQuery");
}

TEST_CASE("the adapter is built against an installed cppTango", "[tango][build]")
{
    const std::string version = detail::tango_headers_version();
    INFO("cppTango headers: " << version);

    REQUIRE_FALSE(version.empty());
    CHECK(version.find('.') != std::string::npos);
}
