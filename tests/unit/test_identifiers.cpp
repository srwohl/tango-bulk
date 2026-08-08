// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

// Identifier generation and rendering (spec 3.2).

#include <tango-bulk/protocol.h>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <set>
#include <string>

using namespace TangoBulk;
using namespace TangoBulk::Protocol;
using Catch::Matchers::ContainsSubstring;

TEST_CASE("generated identifiers do not repeat", "[core][identifiers]")
{
    // A weak generator is caught here, but the real reason identifiers must be
    // unpredictable is elsewhere: reuse is what makes a late frame from a dead
    // session indistinguishable from a live one.
    constexpr int k_samples = 512;

    std::set<std::string> sessions;
    std::set<StreamId> streams;

    for(int i = 0; i < k_samples; ++i)
    {
        sessions.insert(to_hex(generate_session_id()));
        streams.insert(generate_stream_id());
    }

    CHECK(sessions.size() == k_samples);
    CHECK(streams.size() == k_samples);
}

TEST_CASE("identifiers are not sequential", "[core][identifiers]")
{
    // A counter would pass the uniqueness test above.  Consecutive draws
    // differing by one, repeatedly, would not happen by chance.
    int consecutive = 0;

    StreamId previous = generate_stream_id();
    for(int i = 0; i < 64; ++i)
    {
        const StreamId current = generate_stream_id();
        if(current == previous + 1)
        {
            ++consecutive;
        }
        previous = current;
    }

    CHECK(consecutive == 0);
}

TEST_CASE("reserved sentinel values are never generated", "[core][identifiers]")
{
    // An all-zero session_id selects server-wide status in Query, and a zero
    // stream_id is the local "no stream" sentinel.  Issuing either as a real
    // identifier would make a one-in-2^128 case that is impossible to test.
    for(int i = 0; i < 256; ++i)
    {
        CHECK_FALSE(generate_session_id().is_zero());
        CHECK_FALSE(generate_client_instance_id().is_zero());
        CHECK(generate_stream_id() != 0);
        CHECK(generate_server_epoch_id() != 0);
        CHECK(generate_probe_token() != 0);
    }
}

TEST_CASE("identifiers compare by value", "[core][identifiers]")
{
    const SessionId a = generate_session_id();
    SessionId b = a;

    CHECK(a == b);
    CHECK_FALSE(a != b);

    b.bytes[15] = static_cast<std::byte>(static_cast<unsigned>(b.bytes[15]) ^ 1u);

    CHECK(a != b);
    CHECK_FALSE(a == b);
}

TEST_CASE("hex rendering is lowercase and full width", "[core][identifiers]")
{
    SessionId id;
    for(std::size_t i = 0; i < id.bytes.size(); ++i)
    {
        id.bytes[i] = static_cast<std::byte>(0xAB);
    }

    const std::string hex = to_hex(id);

    CHECK(hex == std::string(32, 'a').replace(0, 32, "abababababababababababababababab"));
    CHECK(hex.size() == 32);
}

TEST_CASE("log rendering truncates to eight characters", "[core][identifiers]")
{
    // A full session id in a log is a credential for the coordination plane, and
    // a stream id is one for the data plane.  Truncation is not cosmetic.
    SessionId id;
    for(std::size_t i = 0; i < id.bytes.size(); ++i)
    {
        id.bytes[i] = static_cast<std::byte>(i);
    }

    const std::string logged = to_log_string(id);

    CHECK_THAT(logged, ContainsSubstring("00010203"));
    CHECK(logged.find("0405") == std::string::npos);

    // Eight hex characters plus the ellipsis, which is three bytes of UTF-8.
    CHECK(logged.size() == 8 + 3);

    const std::string stream = to_log_string(StreamId{0x0123'4567'89AB'CDEFull});
    CHECK_THAT(stream, ContainsSubstring("efcdab89"));
    CHECK(stream.find("67452301") == std::string::npos);
}
