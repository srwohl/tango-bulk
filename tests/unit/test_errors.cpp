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
                        Origin::Publisher};

    const BulkException ex{err};

    CHECK(ex.error().status == Status::ResourceExhausted);
    CHECK(ex.error().origin == Origin::Publisher);

    const std::string what = ex.what();
    CHECK_THAT(what, ContainsSubstring("publisher"));
    CHECK_THAT(what, ContainsSubstring("ResourceExhausted"));
    CHECK_THAT(what, ContainsSubstring("ring registration failed"));
}

TEST_CASE("BulkException renders a bare status", "[core][errors]")
{
    const BulkException ex{BulkError{Status::Shutdown, {}, Origin::Subscriber}};

    CHECK(std::string{ex.what()} == "subscriber: Shutdown");
}

TEST_CASE("every typed exception is a BulkException carrying the shared error", "[core][errors]")
{
    const BulkError err{Status::GeometryMismatch, "shape", Origin::Subscriber};

    const auto check = [&err](const BulkException &ex)
    {
        CHECK(ex.error().status == Status::GeometryMismatch);
        CHECK(ex.error().message == "shape");
    };

    check(ConfigurationError{err});
    check(EstablishmentError{err});
    check(StreamClosed{err});
    check(Interrupted{err});
    check(SessionLost{err});
    check(GeometryChanged{err});
    check(ResourceExhausted{err});
    check(DeliveryModeError{err});

    // A caller that does not care which one it is catches the base.
    try
    {
        throw GeometryChanged{err};
    }
    catch(const BulkException &ex)
    {
        CHECK(ex.error().status == Status::GeometryMismatch);
    }
}

TEST_CASE("every origin has a name", "[core][errors]")
{
    CHECK(std::string{to_string(Origin::Publisher)} == "publisher");
    CHECK(std::string{to_string(Origin::Subscriber)} == "subscriber");
    CHECK(std::string{to_string(Origin::Protocol)} == "protocol");
    CHECK(std::string{to_string(Origin::Tango)} == "tango");
    CHECK(std::string{to_string(Origin::Transport)} == "transport");
}
