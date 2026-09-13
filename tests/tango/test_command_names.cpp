// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/tango.h>

#include "command_discovery.h"
#include "tango_support.h"

#include <catch2/catch_test_macros.hpp>

using namespace TangoBulk;

TEST_CASE("default command names are unprefixed", "[tango][commands]")
{
    const CommandNames names;

    CHECK(names.open == "BulkOpen");
    CHECK(names.renew == "BulkRenew");
    CHECK(names.close == "BulkClose");
}

TEST_CASE("with_prefix renames all three commands", "[tango][commands]")
{
    // The override exists for devices with a name collision, not as the normal
    // path -- which is why it prefixes rather than letting each name be set
    // independently.
    const CommandNames names = CommandNames::with_prefix("Xyz");

    CHECK(names.open == "XyzBulkOpen");
    CHECK(names.renew == "XyzBulkRenew");
    CHECK(names.close == "XyzBulkClose");
}

TEST_CASE("the matcher recovers prefixed command names from their descriptions",
          "[tango][commands]")
{
    const auto bulk = [](const std::string &name, const char *request)
    {
        return detail::CommandDescriptor{
            name, true, true, request, detail::k_reply_description};
    };

    std::vector<detail::CommandDescriptor> commands{
        detail::CommandDescriptor{"State", false, false, "Uninitialised", "Device state"},
        bulk("XyzBulkClose", detail::k_close_request_description),
        detail::CommandDescriptor{"Init", false, false, "Uninitialised", "Uninitialised"},
        bulk("XyzBulkOpen", detail::k_open_request_description),
        bulk("XyzBulkRenew", detail::k_renew_request_description),
        // Same argument types, different descriptions: not a bulk command.
        detail::CommandDescriptor{"Blob", true, true, "bytes in", "bytes out"},
    };

    CommandNames names;
    REQUIRE(detail::match_bulk_commands(commands, names) == Status::Ok);
    CHECK(names.open == "XyzBulkOpen");
    CHECK(names.renew == "XyzBulkRenew");
    CHECK(names.close == "XyzBulkClose");

    SECTION("a missing role is UnknownStream")
    {
        commands.erase(commands.begin() + 1);
        CHECK(detail::match_bulk_commands(commands, names) == Status::UnknownStream);
    }

    SECTION("a role claimed twice is MalformedMessage")
    {
        commands.push_back(bulk("OtherBulkOpen", detail::k_open_request_description));
        CHECK(detail::match_bulk_commands(commands, names) == Status::MalformedMessage);
    }
}

TEST_CASE("the adapter is built against an installed cppTango", "[tango][build]")
{
    const std::string version = detail::tango_headers_version();
    INFO("cppTango headers: " << version);

    REQUIRE_FALSE(version.empty());
    CHECK(version.find('.') != std::string::npos);
}
