// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/errors.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <string>

using namespace TangoBulk;
using Catch::Matchers::ContainsSubstring;

TEST_CASE("BulkException carries its error and renders it", "[core][errors]")
{
    const BulkError err{Status::ResourceExhausted, "ring registration failed",
                        "publisher"};

    const BulkException ex{err};

    CHECK(ex.error().status == Status::ResourceExhausted);
    CHECK(ex.error().origin == "publisher");

    const std::string what = ex.what();
    CHECK_THAT(what, ContainsSubstring("publisher"));
    CHECK_THAT(what, ContainsSubstring("ResourceExhausted"));
    CHECK_THAT(what, ContainsSubstring("ring registration failed"));
}

TEST_CASE("BulkException renders a bare status", "[core][errors]")
{
    const BulkException ex{BulkError{Status::Shutdown, {}, {}}};

    CHECK(std::string{ex.what()} == "Shutdown");
}
