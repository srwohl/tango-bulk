// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

// Round trip for every message type: encode, decode, field-by-field equality.
//
// On its own this proves only that encode and decode agree; the layout tests are
// what tie them to the specification.  Both are needed, and neither substitutes
// for the other.

#include "wire_helpers.h"

#include <core/protocol.h>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>

using namespace TangoBulk;
using namespace TangoBulk::Protocol;
using namespace TangoBulk::test;

namespace
{

Geometry filled_geometry()
{
    Geometry g;
    g.generation = 12;
    g.element_type = ElementType::Float32;
    g.element_size = 4;
    g.rank = 3;
    g.max_frame_bytes = 16ull << 20;
    g.ring_depth = 64;
    g.credit_window = 32;
    g.shape = {8, 256, 512, 0};
    g.strides = {524'288, 2'048, 4, 0};
    g.endian = Endian::Big;
    return g;
}

} // namespace

TEST_CASE("Open round trip", "[protocol][roundtrip]")
{
    OpenRequest sent;
    sent.version_min = 2;
    sent.version_max = 2;
    sent.flow = FlowPolicy::Lossless;
    sent.client_instance_id.bytes = pattern_id16(0x55);
    sent.requested_max_frame_bytes = 4ull << 20;
    sent.requested_ring_depth = 16;
    sent.requested_credit_window = 8;
    sent.requested_memory_kind = MemoryKind::Cuda;
    sent.stream_name = "detector.image-0_9";
    sent.client_label = "hdf5-writer-2";
    sent.client_ucx_address.assign(273, std::byte{0xAB});

    const auto bytes = encode(sent, 0xC0FFEEull);

    OpenRequest got;
    Envelope env;
    REQUIRE(decode(bytes.data(), bytes.size(), got, &env) == Status::Ok);

    CHECK(env.correlation_id == 0xC0FFEEull);
    CHECK(env.msg_type == CoordType::Open);
    CHECK(env.version_major == 2);
    CHECK(env.version_minor == 0);

    CHECK(got.version_min == sent.version_min);
    CHECK(got.version_max == sent.version_max);
    CHECK(got.flow == sent.flow);
    CHECK(got.client_instance_id == sent.client_instance_id);
    CHECK(got.requested_max_frame_bytes == sent.requested_max_frame_bytes);
    CHECK(got.requested_ring_depth == sent.requested_ring_depth);
    CHECK(got.requested_credit_window == sent.requested_credit_window);
    CHECK(got.requested_memory_kind == sent.requested_memory_kind);
    CHECK(got.stream_name == sent.stream_name);
    CHECK(got.client_label == sent.client_label);
    CHECK(got.client_ucx_address == sent.client_ucx_address);
}

TEST_CASE("an Open with no client label round trips", "[protocol][roundtrip]")
{
    // Empty is the one length a stream name may not have and a label may, so it
    // is the case where the two tail fields could most easily be confused.
    OpenRequest sent;
    sent.stream_name = "image";
    sent.client_ucx_address.assign(8, std::byte{0x11});

    const auto bytes = encode(sent, 0);

    OpenRequest got;
    REQUIRE(decode(bytes.data(), bytes.size(), got) == Status::Ok);

    CHECK(got.stream_name == "image");
    CHECK(got.client_label.empty());
    CHECK(got.client_ucx_address == sent.client_ucx_address);
}

TEST_CASE("a label outside the charset is refused in both directions",
          "[protocol][roundtrip]")
{
    OpenRequest sent;
    sent.stream_name = "image";
    sent.client_label = "live viewer"; // a space is not in [A-Za-z0-9_.-]
    sent.client_ucx_address.assign(8, std::byte{0x11});

    CHECK_THROWS_AS(encode(sent, 0), BulkException);

    // A peer that skipped its own check must not get the label through: build
    // the same message with a legal label of equal length and patch one byte.
    sent.client_label = "live-viewer";
    auto bytes = encode(sent, 0);

    const auto space = std::find(bytes.begin(), bytes.end(), std::byte{'-'});
    REQUIRE(space != bytes.end());
    *space = std::byte{' '};

    OpenRequest got;
    CHECK(decode(bytes.data(), bytes.size(), got) == Status::MalformedMessage);
}

TEST_CASE("an unknown flow policy is refused rather than defaulted",
          "[protocol][roundtrip]")
{
    OpenRequest sent;
    sent.stream_name = "image";
    sent.client_ucx_address.assign(8, std::byte{0x11});

    auto bytes = encode(sent, 0);
    // flow sits at body offset 4, so absolute offset 36.
    bytes[k_coord_envelope_bytes + 4] = std::byte{0x07};

    OpenRequest got;
    CHECK(decode(bytes.data(), bytes.size(), got) == Status::MalformedMessage);
}

TEST_CASE("OpenReply round trip", "[protocol][roundtrip]")
{
    OpenReply sent;
    sent.status = Status::Ok;
    sent.session_id.bytes = pattern_id16(0x66);
    sent.stream_id = 0x1234'5678'9ABC'DEF0ull;
    sent.lease_ttl_ms = 10'000;
    sent.renew_interval_ms = 3'333;
    sent.geometry = filled_geometry();
    sent.server_ucx_address.assign(4096, std::byte{0x5A});

    const auto bytes = encode(sent, 7);

    OpenReply got;
    REQUIRE(decode(bytes.data(), bytes.size(), got) == Status::Ok);

    CHECK(got.status == sent.status);
    CHECK(got.session_id == sent.session_id);
    CHECK(got.stream_id == sent.stream_id);
    CHECK(got.lease_ttl_ms == sent.lease_ttl_ms);
    CHECK(got.renew_interval_ms == sent.renew_interval_ms);
    CHECK(got.geometry == sent.geometry);
    CHECK(got.geometry.endian == sent.geometry.endian);
    CHECK(got.server_ucx_address == sent.server_ucx_address);
}

TEST_CASE("a failing OpenReply needs no address", "[protocol][roundtrip]")
{
    // Status is nonzero, so the spec leaves the remaining fields undefined.
    // Requiring an address here would force every error path to invent one.
    OpenReply sent;
    sent.status = Status::TooManySessions;

    const auto bytes = encode(sent, 0);

    OpenReply got;
    REQUIRE(decode(bytes.data(), bytes.size(), got) == Status::Ok);

    CHECK(got.status == Status::TooManySessions);
    CHECK(got.server_ucx_address.empty());
}

TEST_CASE("Renew round trip", "[protocol][roundtrip]")
{
    RenewRequest sent;
    sent.session_id.bytes = pattern_id16(0x77);

    const auto bytes = encode(sent, 99);

    RenewRequest got;
    Envelope env;
    REQUIRE(decode(bytes.data(), bytes.size(), got, &env) == Status::Ok);

    CHECK(env.correlation_id == 99);
    CHECK(got.session_id == sent.session_id);
}

TEST_CASE("RenewReply round trip", "[protocol][roundtrip]")
{
    RenewReply sent;
    sent.session_id.bytes = pattern_id16(0x88);
    sent.status = Status::Ok;
    sent.lease_ttl_ms = 20'000;
    sent.renew_interval_ms = 6'666;

    const auto bytes = encode(sent, 0);

    RenewReply got;
    REQUIRE(decode(bytes.data(), bytes.size(), got) == Status::Ok);

    CHECK(got.session_id == sent.session_id);
    CHECK(got.status == sent.status);
    CHECK(got.lease_ttl_ms == sent.lease_ttl_ms);
    CHECK(got.renew_interval_ms == sent.renew_interval_ms);
}

TEST_CASE("Close and CloseReply round trip", "[protocol][roundtrip]")
{
    CloseRequest close;
    close.session_id.bytes = pattern_id16(0x99);
    close.reason = CloseReason::ClientShutdown;

    const auto close_bytes = encode(close, 1);

    CloseRequest got_close;
    REQUIRE(decode(close_bytes.data(), close_bytes.size(), got_close) == Status::Ok);
    CHECK(got_close.session_id == close.session_id);
    CHECK(got_close.reason == close.reason);

    CloseReply reply;
    reply.session_id = close.session_id;
    reply.status = Status::UnknownSession;

    const auto reply_bytes = encode(reply, 1);

    CloseReply got_reply;
    REQUIRE(decode(reply_bytes.data(), reply_bytes.size(), got_reply) == Status::Ok);
    CHECK(got_reply.session_id == reply.session_id);
    CHECK(got_reply.status == reply.status);
}

TEST_CASE("Error round trip", "[protocol][roundtrip]")
{
    const ErrorMessage sent{Status::ResourceExhausted,
                            "pinned memory budget exhausted: 1 GiB configured"};

    const auto bytes = encode(sent, 0xDEAD);

    ErrorMessage got;
    Envelope env;
    REQUIRE(decode(bytes.data(), bytes.size(), got, &env) == Status::Ok);

    CHECK(env.correlation_id == 0xDEAD);
    CHECK(got.status == sent.status);
    CHECK(got.message == sent.message);
}

TEST_CASE("an empty error message round trips", "[protocol][roundtrip]")
{
    const ErrorMessage sent{Status::Shutdown, ""};

    const auto bytes = encode(sent, 0);

    ErrorMessage got;
    REQUIRE(decode(bytes.data(), bytes.size(), got) == Status::Ok);
    CHECK(got.status == Status::Shutdown);
    CHECK(got.message.empty());
}

TEST_CASE("decode_envelope classifies without committing to a type",
          "[protocol][roundtrip]")
{
    // A client must accept Error in place of any expected reply, so it needs the
    // type before it can choose a decoder.
    const auto bytes = encode(ErrorMessage{Status::UnknownStream, "no such stream"}, 5);

    Envelope env;
    REQUIRE(decode_envelope(bytes.data(), bytes.size(), env) == Status::Ok);

    CHECK(env.msg_type == CoordType::Error);
    CHECK(env.correlation_id == 5);

    // And the wrong decoder must refuse it rather than reinterpret its bytes.
    OpenReply wrong;
    CHECK(decode(bytes.data(), bytes.size(), wrong) == Status::MalformedMessage);
}

// ---------------------------------------------------------------------------
// Data plane
// ---------------------------------------------------------------------------

TEST_CASE("Frame header round trip", "[protocol][roundtrip]")
{
    FrameHeader sent;
    sent.generation = 12;
    sent.stream_id = 0x0F1E'2D3C'4B5A'6978ull;
    sent.sequence = 987'654'321;
    sent.payload_bytes = 8ull * 256 * 512 * 4;
    sent.timestamp_ns = 1'700'000'000'123'456'789ull;
    sent.event_counter = 42;
    sent.dropped_before = 17;
    sent.element_type = ElementType::Float32;
    sent.element_size = 4;
    sent.rank = 3;
    sent.quality = 1;
    sent.memory_kind = MemoryKind::Host;
    sent.endian = Endian::Little;
    sent.shape = {8, 256, 512, 0};
    sent.strides = {524'288, 2'048, 4, 0};

    const auto bytes = encode(sent);

    FrameHeader got;
    REQUIRE(decode(bytes.data(), bytes.size(), got) == Status::Ok);

    CHECK(got.generation == sent.generation);
    CHECK(got.stream_id == sent.stream_id);
    CHECK(got.sequence == sent.sequence);
    CHECK(got.payload_bytes == sent.payload_bytes);
    CHECK(got.timestamp_ns == sent.timestamp_ns);
    CHECK(got.event_counter == sent.event_counter);
    CHECK(got.dropped_before == sent.dropped_before);
    CHECK(got.element_type == sent.element_type);
    CHECK(got.element_size == sent.element_size);
    CHECK(got.rank == sent.rank);
    CHECK(got.quality == sent.quality);
    CHECK(got.memory_kind == sent.memory_kind);
    CHECK(got.endian == sent.endian);
    CHECK(got.shape == sent.shape);
    CHECK(got.strides == sent.strides);
}

TEST_CASE("a rank-0 opaque frame round trips", "[protocol][roundtrip]")
{
    // The degenerate case the spec allows: an opaque byte blob with no shape.
    FrameHeader sent;
    sent.generation = 1;
    sent.stream_id = 1;
    sent.sequence = 0;
    sent.payload_bytes = 4096;
    sent.element_type = ElementType::Byte;
    sent.element_size = 1;
    sent.rank = 0;

    const auto bytes = encode(sent);

    FrameHeader got;
    REQUIRE(decode(bytes.data(), bytes.size(), got) == Status::Ok);

    CHECK(got.rank == 0);
    CHECK(got.payload_bytes == 4096);
    CHECK(got.element_type == ElementType::Byte);
}

TEST_CASE("Credit round trip", "[protocol][roundtrip]")
{
    CreditMessage sent;
    sent.generation = 3;
    sent.stream_id = 0xABCD'EF01'2345'6789ull;
    sent.ack_sequence = 18'446'744'073'709'551'614ull;

    const auto bytes = encode(sent);

    CreditMessage got;
    REQUIRE(decode(bytes.data(), bytes.size(), got) == Status::Ok);

    CHECK(got.generation == sent.generation);
    CHECK(got.stream_id == sent.stream_id);
    CHECK(got.ack_sequence == sent.ack_sequence);
}

TEST_CASE("Probe and ProbeAck round trip and do not alias", "[protocol][roundtrip]")
{
    ProbeMessage probe;
    probe.generation = 1;
    probe.stream_id = 9;
    probe.probe_token = 0x1122'3344'5566'7788ull;

    const auto probe_bytes = encode(probe);

    ProbeMessage got_probe;
    REQUIRE(decode(probe_bytes.data(), probe_bytes.size(), got_probe) == Status::Ok);
    CHECK(got_probe.probe_token == probe.probe_token);

    // A ProbeAck decoder must refuse a Probe, and vice versa.  With a shared AM
    // id this distinction did not exist, which is exactly why it needed a
    // sentinel sequence to compensate.
    ProbeAckMessage as_ack;
    CHECK(decode(probe_bytes.data(), probe_bytes.size(), as_ack) ==
          Status::MalformedMessage);

    ProbeAckMessage ack;
    ack.generation = 1;
    ack.stream_id = 9;
    ack.probe_token = probe.probe_token;

    const auto ack_bytes = encode(ack);

    CreditMessage as_credit;
    CHECK(decode(ack_bytes.data(), ack_bytes.size(), as_credit) ==
          Status::MalformedMessage);

    ProbeAckMessage got_ack;
    REQUIRE(decode(ack_bytes.data(), ack_bytes.size(), got_ack) == Status::Ok);
    CHECK(got_ack.probe_token == probe.probe_token);
}

TEST_CASE("decode_data_prefix classifies every data message", "[protocol][roundtrip]")
{
    const auto check = [](const auto &bytes, DataType expected, std::uint32_t generation)
    {
        DataPrefix prefix;
        REQUIRE(decode_data_prefix(bytes.data(), bytes.size(), prefix) == Status::Ok);
        CHECK(prefix.msg_type == expected);
        CHECK(prefix.generation == generation);
        CHECK(prefix.version_major == k_version_major);
    };

    FrameHeader frame;
    frame.generation = 4;
    frame.payload_bytes = 64;
    frame.element_type = ElementType::Byte;
    frame.element_size = 1;

    check(encode(frame), DataType::Frame, 4);
    check(encode(CreditMessage{5, 1, 0}), DataType::Credit, 5);
    check(encode(ProbeMessage{6, 1, 2}), DataType::Probe, 6);
    check(encode(ProbeAckMessage{7, 1, 2}), DataType::ProbeAck, 7);
}
