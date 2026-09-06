// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

// Truncation, corruption, version rules, and bounds.
//
// The sweeps are the reason this file exists.  A decoder that handles the inputs
// someone thought to write down by hand is not the same as one that cannot be
// walked off the end of a buffer, and the difference only shows up when every
// length and every bit gets tried.  Run under ASan the sweeps also prove the
// "MUST NOT read past the supplied length" rule, which no return value can.

#include "wire_helpers.h"

#include <tango-bulk/protocol.h>

#include <catch2/catch_test_macros.hpp>

#include <functional>
#include <vector>

using namespace TangoBulk;
using namespace TangoBulk::Protocol;
using namespace TangoBulk::test;

namespace
{

GeometryBlock valid_geometry()
{
    GeometryBlock g;
    g.generation = 3;
    g.element_type = ElementType::UInt16;
    g.element_size = 2;
    g.rank = 2;
    g.max_frame_bytes = 8ull << 20;
    g.ring_depth = 32;
    g.credit_window = 16;
    g.shape = {512, 512, 0, 0};
    g.strides = {1024, 2, 0, 0};
    return g;
}

OpenRequest valid_open()
{
    OpenRequest msg;
    msg.client_instance_id.bytes = pattern_id16();
    msg.requested_max_frame_bytes = 8ull << 20;
    msg.requested_ring_depth = 32;
    msg.requested_credit_window = 16;
    msg.stream_name = "image";
    msg.client_ucx_address.assign(64, std::byte{0x11});
    return msg;
}

OpenReply valid_open_reply()
{
    OpenReply msg;
    msg.session_id.bytes = pattern_id16(3);
    msg.stream_id = 42;
    msg.lease_ttl_ms = 10'000;
    msg.renew_interval_ms = 3'333;
    msg.geometry = valid_geometry();
    msg.server_ucx_address.assign(32, std::byte{0x22});
    return msg;
}

FrameHeader valid_frame()
{
    FrameHeader msg;
    msg.generation = 3;
    msg.stream_id = 42;
    msg.sequence = 7;
    msg.payload_bytes = 512 * 512 * 2;
    msg.element_type = ElementType::UInt16;
    msg.element_size = 2;
    msg.rank = 2;
    msg.shape = {512, 512, 0, 0};
    msg.strides = {1024, 2, 0, 0};
    return msg;
}

/// Erased decoder, so the sweeps can be written once per plane instead of once
/// per message type.
using Decoder = std::function<Status(const std::byte *, std::size_t)>;

template <typename Message>
Decoder coord_decoder()
{
    return [](const std::byte *data, std::size_t size)
    {
        Message out;
        return decode(data, size, out);
    };
}

template <typename Message>
Decoder data_decoder()
{
    return [](const std::byte *data, std::size_t size)
    {
        Message out;
        return decode(data, size, out);
    };
}

/// Decoding at every length below the real one must fail, and must not read a
/// byte that is not there.
void sweep_truncations(const std::vector<std::byte> &message, const Decoder &decode_fn,
                       const char *what)
{
    for(std::size_t len = 0; len < message.size(); ++len)
    {
        INFO(what << " truncated to " << len << " of " << message.size() << " bytes");

        // A fresh, exactly-sized buffer per length, so a read one byte past the
        // end is a heap overflow ASan can see rather than a byte that happens to
        // still be in the original allocation.
        const std::vector<std::byte> truncated(message.begin(),
                                               message.begin() +
                                                   static_cast<std::ptrdiff_t>(len));

        const Status status = decode_fn(truncated.data(), truncated.size());
        CHECK(status != Status::Ok);
    }
}

/// Flipping any single bit of the message must leave the decoder in a defined
/// state: either a clean rejection, or a successful decode of a different but
/// self-consistent message.  What it must never do is read out of bounds.
void sweep_bit_flips(const std::vector<std::byte> &message, const Decoder &decode_fn,
                     const char *what)
{
    for(std::size_t byte = 0; byte < message.size(); ++byte)
    {
        for(unsigned bit = 0; bit < 8; ++bit)
        {
            std::vector<std::byte> corrupted = message;
            corrupted[byte] ^= static_cast<std::byte>(1u << bit);

            INFO(what << ": bit " << bit << " of byte " << byte << " flipped");

            // The value is deliberately unused: the assertion is that this
            // returns at all, without a sanitizer report.
            const Status status = decode_fn(corrupted.data(), corrupted.size());
            (void) status;
        }
    }
}

} // namespace

// ---------------------------------------------------------------------------
// Truncation
// ---------------------------------------------------------------------------

TEST_CASE("truncation sweep, coordination plane", "[protocol][malformed]")
{
    sweep_truncations(encode(valid_open(), 1), coord_decoder<OpenRequest>(), "Open");
    sweep_truncations(encode(valid_open_reply(), 1), coord_decoder<OpenReply>(),
                      "OpenReply");
    sweep_truncations(encode(RenewRequest{}, 1), coord_decoder<RenewRequest>(), "Renew");
    sweep_truncations(encode(RenewReply{}, 1), coord_decoder<RenewReply>(), "RenewReply");
    sweep_truncations(encode(CloseRequest{}, 1), coord_decoder<CloseRequest>(), "Close");
    sweep_truncations(encode(CloseReply{}, 1), coord_decoder<CloseReply>(), "CloseReply");
    sweep_truncations(encode(QueryRequest{}, 1), coord_decoder<QueryRequest>(), "Query");
    sweep_truncations(encode(QueryReply{}, 1), coord_decoder<QueryReply>(), "QueryReply");
    sweep_truncations(encode(ErrorMessage{Status::Internal, "boom"}, 1),
                      coord_decoder<ErrorMessage>(), "Error");
}

TEST_CASE("truncation sweep, data plane", "[protocol][malformed]")
{
    const auto to_vector = [](const auto &array)
    { return std::vector<std::byte>(array.begin(), array.end()); };

    sweep_truncations(to_vector(encode(valid_frame())), data_decoder<FrameHeader>(),
                      "Frame");
    sweep_truncations(to_vector(encode(CreditMessage{1, 2, 3})),
                      data_decoder<CreditMessage>(), "Credit");
    sweep_truncations(to_vector(encode(ProbeMessage{1, 2, 3})),
                      data_decoder<ProbeMessage>(), "Probe");
    sweep_truncations(to_vector(encode(ProbeAckMessage{1, 2, 3})),
                      data_decoder<ProbeAckMessage>(), "ProbeAck");
}

TEST_CASE("a null pointer is rejected, not dereferenced", "[protocol][malformed]")
{
    OpenRequest out;
    CHECK(decode(nullptr, 0, out) == Status::MalformedMessage);
    CHECK(decode(nullptr, 1024, out) == Status::MalformedMessage);

    FrameHeader frame;
    CHECK(decode(nullptr, 160, frame) == Status::MalformedMessage);

    Envelope env;
    CHECK(decode_envelope(nullptr, 32, env) == Status::MalformedMessage);
}

// ---------------------------------------------------------------------------
// Corruption
// ---------------------------------------------------------------------------

TEST_CASE("bit-flip sweep, coordination plane", "[protocol][malformed]")
{
    sweep_bit_flips(encode(valid_open(), 1), coord_decoder<OpenRequest>(), "Open");
    sweep_bit_flips(encode(valid_open_reply(), 1), coord_decoder<OpenReply>(),
                    "OpenReply");
    sweep_bit_flips(encode(RenewRequest{}, 1), coord_decoder<RenewRequest>(), "Renew");
    sweep_bit_flips(encode(RenewReply{}, 1), coord_decoder<RenewReply>(), "RenewReply");
    sweep_bit_flips(encode(CloseRequest{}, 1), coord_decoder<CloseRequest>(), "Close");
    sweep_bit_flips(encode(QueryReply{}, 1), coord_decoder<QueryReply>(), "QueryReply");
    sweep_bit_flips(encode(ErrorMessage{Status::Internal, "boom"}, 1),
                    coord_decoder<ErrorMessage>(), "Error");
}

TEST_CASE("bit-flip sweep, data plane", "[protocol][malformed]")
{
    const auto to_vector = [](const auto &array)
    { return std::vector<std::byte>(array.begin(), array.end()); };

    sweep_bit_flips(to_vector(encode(valid_frame())), data_decoder<FrameHeader>(),
                    "Frame");
    sweep_bit_flips(to_vector(encode(CreditMessage{1, 2, 3})),
                    data_decoder<CreditMessage>(), "Credit");

}

TEST_CASE("a corrupted magic is rejected", "[protocol][malformed]")
{
    auto bytes = encode(valid_open(), 1);
    bytes[0] = std::byte{'X'};

    OpenRequest out;
    CHECK(decode(bytes.data(), bytes.size(), out) == Status::MalformedMessage);

    auto frame = encode(valid_frame());
    frame[3] = std::byte{'2'};

    FrameHeader frame_out;
    CHECK(decode(frame.data(), frame.size(), frame_out) == Status::MalformedMessage);
}

// ---------------------------------------------------------------------------
// Version rules (3.1)
// ---------------------------------------------------------------------------

TEST_CASE("a foreign major is rejected as UnsupportedVersion", "[protocol][version]")
{
    auto bytes = encode(valid_open(), 1);
    bytes[4] = std::byte{2}; // version_major

    OpenRequest out;
    CHECK(decode(bytes.data(), bytes.size(), out) == Status::UnsupportedVersion);

    // Never silently downgraded on the data plane either.
    auto frame = encode(valid_frame());
    frame[4] = std::byte{0};

    FrameHeader frame_out;
    CHECK(decode(frame.data(), frame.size(), frame_out) == Status::UnsupportedVersion);
}

TEST_CASE("a newer minor may append to a fixed body", "[protocol][version]")
{
    // Renew has no variable tail, so a v1.1 sender appending a field must still
    // be understood by a v1.0 receiver reading the fields it knows.
    auto bytes = encode(RenewRequest{}, 1);

    bytes[5] = std::byte{1};                  // version_minor = 1
    bytes[12] = std::byte{k_renew_bytes + 8}; // body_bytes += 8
    bytes.insert(bytes.end(), 8, std::byte{0xEE});

    RenewRequest out;
    Envelope env;
    CHECK(decode(bytes.data(), bytes.size(), out, &env) == Status::Ok);
    CHECK(env.version_minor == 1);
}

TEST_CASE("trailing bytes at our own minor are malformed", "[protocol][version]")
{
    // At minor 0 a longer body is a bug, not an extension.
    auto bytes = encode(RenewRequest{}, 1);

    bytes[12] = std::byte{k_renew_bytes + 8};
    bytes.insert(bytes.end(), 8, std::byte{0xEE});

    RenewRequest out;
    CHECK(decode(bytes.data(), bytes.size(), out) == Status::MalformedMessage);
}

TEST_CASE("a variable-tail body gets no trailing tolerance", "[protocol][version]")
{
    // Appending a fixed field to Open would move stream_name, so reading the tail
    // at its old offset would yield garbage.  Refusing is the safe answer even
    // from a newer minor.
    auto bytes = encode(valid_open(), 1);
    const std::size_t body = bytes.size() - k_coord_envelope_bytes;

    bytes[5] = std::byte{1}; // version_minor = 1
    bytes[12] = static_cast<std::byte>(body + 8);
    bytes.insert(bytes.end(), 8, std::byte{0});

    OpenRequest out;
    CHECK(decode(bytes.data(), bytes.size(), out) == Status::MalformedMessage);
}

TEST_CASE("a header_bytes below the minimum is malformed", "[protocol][version]")
{
    auto bytes = encode(CloseRequest{}, 1);
    bytes[6] = std::byte{24}; // envelope header_bytes, minimum is 32

    CloseRequest out;
    CHECK(decode(bytes.data(), bytes.size(), out) == Status::MalformedMessage);

    auto frame = encode(valid_frame());
    frame[6] = std::byte{152}; // the prototype's size; the minimum is now 160
    frame[7] = std::byte{0};

    FrameHeader frame_out;
    CHECK(decode(frame.data(), frame.size(), frame_out) == Status::MalformedMessage);
}

TEST_CASE("a larger header_bytes is accepted with the excess ignored",
          "[protocol][version]")
{
    // The trailing-extension rule: read min(header_bytes, our layout) and ignore
    // the rest.  This is what makes minor-version compatibility mechanical.
    auto frame = encode(valid_frame());

    frame[6] = std::byte{168};
    frame[7] = std::byte{0};

    std::vector<std::byte> extended(frame.begin(), frame.end());
    extended.insert(extended.end(), 8, std::byte{0xAB});

    FrameHeader out;
    REQUIRE(decode(extended.data(), extended.size(), out) == Status::Ok);
    CHECK(out.sequence == 7);
}

TEST_CASE("a header_bytes claiming more than was delivered is malformed",
          "[protocol][version]")
{
    // The explicit case from 3.0.8: the decoder must not read past the supplied
    // length even when the header says it may.
    auto frame = encode(valid_frame());

    frame[6] = std::byte{200};
    frame[7] = std::byte{0};

    FrameHeader out;
    CHECK(decode(frame.data(), frame.size(), out) == Status::MalformedMessage);
}

TEST_CASE("an unknown message type is rejected", "[protocol][version]")
{
    auto bytes = encode(CloseRequest{}, 1);
    bytes[8] = std::byte{0x77};

    Envelope env;
    CHECK(decode_envelope(bytes.data(), bytes.size(), env) == Status::MalformedMessage);

    auto frame = encode(valid_frame());
    frame[8] = std::byte{9};

    DataPrefix prefix;
    CHECK(decode_data_prefix(frame.data(), frame.size(), prefix) ==
          Status::MalformedMessage);
}

// ---------------------------------------------------------------------------
// Bounds (3.4, 3.9)
// ---------------------------------------------------------------------------

TEST_CASE("body_bytes must account for the whole message", "[protocol][bounds]")
{
    auto bytes = encode(CloseRequest{}, 1);

    SECTION("too small")
    {
        bytes[12] = std::byte{16};
        CloseRequest out;
        CHECK(decode(bytes.data(), bytes.size(), out) == Status::MalformedMessage);
    }

    SECTION("too large")
    {
        bytes[12] = std::byte{200};
        CloseRequest out;
        CHECK(decode(bytes.data(), bytes.size(), out) == Status::MalformedMessage);
    }

    SECTION("beyond the maximum")
    {
        bytes[12] = std::byte{0xFF};
        bytes[13] = std::byte{0xFF};
        bytes[14] = std::byte{0xFF};
        bytes[15] = std::byte{0xFF};
        CloseRequest out;
        CHECK(decode(bytes.data(), bytes.size(), out) == Status::MalformedMessage);
    }
}

TEST_CASE("a declared string length past the buffer is malformed", "[protocol][bounds]")
{
    auto bytes = encode(valid_open(), 1);

    // stream_name length lives at body offset 56.
    const std::size_t len_offset = k_coord_envelope_bytes + 56;
    bytes[len_offset] = std::byte{0xFF};
    bytes[len_offset + 1] = std::byte{0xFF};

    OpenRequest out;
    CHECK(decode(bytes.data(), bytes.size(), out) == Status::MalformedMessage);
}

TEST_CASE("a declared blob length past the buffer is malformed", "[protocol][bounds]")
{
    auto bytes = encode(valid_open_reply(), 1);

    // server_ucx_address length lives at body offset 152.
    const std::size_t len_offset = k_coord_envelope_bytes + k_open_reply_fixed_bytes;
    bytes[len_offset] = std::byte{0x00};
    bytes[len_offset + 1] = std::byte{0x10}; // 4096, well past the buffer

    OpenReply out;
    CHECK(decode(bytes.data(), bytes.size(), out) == Status::MalformedMessage);
}

TEST_CASE("encode rejects an out-of-range stream name", "[protocol][bounds]")
{
    OpenRequest msg = valid_open();

    SECTION("empty")
    {
        msg.stream_name.clear();
        CHECK_THROWS_AS(encode(msg, 0), BulkException);
    }

    SECTION("too long")
    {
        msg.stream_name.assign(65, 'a');
        CHECK_THROWS_AS(encode(msg, 0), BulkException);
    }

    SECTION("illegal character")
    {
        msg.stream_name = "image/raw";
        CHECK_THROWS_AS(encode(msg, 0), BulkException);
    }

    SECTION("at the limit")
    {
        msg.stream_name.assign(64, 'a');
        CHECK_NOTHROW(encode(msg, 0));
    }
}

TEST_CASE("decode rejects an illegal stream name", "[protocol][bounds]")
{
    // A name that encode would refuse must not be accepted from the wire either:
    // the name reaches a device server's stream table, and the character class is
    // what lets it be logged and put in a counter blob unescaped.
    auto bytes = encode(valid_open(), 1);

    bytes[k_coord_envelope_bytes + 58] = std::byte{'/'};

    OpenRequest out;
    CHECK(decode(bytes.data(), bytes.size(), out) == Status::MalformedMessage);
}

TEST_CASE("a NUL inside a string field is malformed", "[protocol][bounds]")
{
    auto bytes = encode(ErrorMessage{Status::Internal, "abcd"}, 1);

    bytes[k_coord_envelope_bytes + k_error_fixed_bytes + 2 + 1] = std::byte{0};

    ErrorMessage out;
    CHECK(decode(bytes.data(), bytes.size(), out) == Status::MalformedMessage);
}

TEST_CASE("encode rejects an out-of-range address blob", "[protocol][bounds]")
{
    OpenRequest msg = valid_open();

    SECTION("empty")
    {
        msg.client_ucx_address.clear();
        CHECK_THROWS_AS(encode(msg, 0), BulkException);
    }

    SECTION("too long")
    {
        msg.client_ucx_address.assign(k_max_ucx_address_bytes + 1, std::byte{0});
        CHECK_THROWS_AS(encode(msg, 0), BulkException);
    }

    SECTION("at the limit")
    {
        msg.client_ucx_address.assign(k_max_ucx_address_bytes, std::byte{0});
        CHECK_NOTHROW(encode(msg, 0));
    }
}

TEST_CASE("over-long bounded text is truncated on encode", "[protocol][bounds]")
{
    // The two fields the spec truncates rather than rejects.  An error path must
    // not fail because its explanation was too long.
    const ErrorMessage error{Status::Internal, std::string(600, 'x')};

    const auto error_bytes = encode(error, 0);

    ErrorMessage error_out;
    REQUIRE(decode(error_bytes.data(), error_bytes.size(), error_out) == Status::Ok);
    CHECK(error_out.message.size() == k_max_error_message_bytes);

    QueryReply reply;
    reply.counters = std::string(k_max_counters_bytes + 100, 'y');

    const auto reply_bytes = encode(reply, 0);

    QueryReply reply_out;
    REQUIRE(decode(reply_bytes.data(), reply_bytes.size(), reply_out) == Status::Ok);
    CHECK(reply_out.counters.size() == k_max_counters_bytes);
}

// ---------------------------------------------------------------------------
// Frame-header field validation (3.11 step 7)
// ---------------------------------------------------------------------------

TEST_CASE("a frame header with a bad description is dropped", "[protocol][bounds]")
{
    const auto round_trip = [](const FrameHeader &msg)
    {
        const auto bytes = encode(msg);
        FrameHeader out;
        return decode(bytes.data(), bytes.size(), out);
    };

    SECTION("rank above the maximum")
    {
        FrameHeader msg = valid_frame();
        msg.rank = 5;
        CHECK(round_trip(msg) == Status::GeometryMismatch);
    }

    SECTION("nonzero shape beyond the rank")
    {
        FrameHeader msg = valid_frame();
        msg.shape[2] = 1;
        CHECK(round_trip(msg) == Status::GeometryMismatch);
    }

    SECTION("nonzero stride beyond the rank")
    {
        FrameHeader msg = valid_frame();
        msg.strides[3] = 8;
        CHECK(round_trip(msg) == Status::GeometryMismatch);
    }

    SECTION("element_size disagreeing with element_type")
    {
        FrameHeader msg = valid_frame();
        msg.element_size = 4; // UInt16 is 2
        CHECK(round_trip(msg) == Status::GeometryMismatch);
    }

    SECTION("element_size zero")
    {
        FrameHeader msg = valid_frame();
        msg.element_type = ElementType::Unknown;
        msg.element_size = 0;
        CHECK(round_trip(msg) == Status::GeometryMismatch);
    }

    SECTION("shape product exceeding payload_bytes")
    {
        FrameHeader msg = valid_frame();
        msg.shape = {512, 1024, 0, 0}; // needs 1 MiB, claims 512 KiB
        CHECK(round_trip(msg) == Status::GeometryMismatch);
    }

    SECTION("shape product overflowing")
    {
        FrameHeader msg = valid_frame();
        msg.shape = {0xFFFF'FFFF'FFFFull, 0xFFFF'FFFFull, 0, 0};
        CHECK(round_trip(msg) == Status::GeometryMismatch);
    }

    SECTION("stride times shape overflowing")
    {
        FrameHeader msg = valid_frame();
        msg.strides = {0xFFFF'FFFF'FFFF'FFFFull, 2, 0, 0};
        CHECK(round_trip(msg) == Status::GeometryMismatch);
    }

    SECTION("payload_bytes zero")
    {
        FrameHeader msg = valid_frame();
        msg.payload_bytes = 0;
        CHECK(round_trip(msg) == Status::FrameTooLarge);
    }

    SECTION("payload_bytes above the hard cap")
    {
        FrameHeader msg = valid_frame();
        msg.payload_bytes = k_max_frame_bytes_hard_cap + 1;
        CHECK(round_trip(msg) == Status::FrameTooLarge);
    }

    SECTION("an unnamed element type")
    {
        FrameHeader msg = valid_frame();
        msg.element_type = static_cast<ElementType>(99);
        CHECK(round_trip(msg) == Status::GeometryMismatch);
    }
}
