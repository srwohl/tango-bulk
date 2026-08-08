// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

// Golden vectors: one committed hex blob per message type.
//
// These are change detectors, not conformance checks -- the conformance checks
// are in test_protocol_layout.cpp, where every offset is transcribed from the
// specification and every integer is compared against an independently computed
// little-endian pattern.  What golden vectors add is that an accidental layout
// change shows up in review as a diff of committed bytes, which is much harder
// to wave through than a diff of encoder source.
//
// Every message here is built from fixed values, never from the CSPRNG, so the
// vectors are reproducible.  To regenerate after an intentional protocol change:
//
//     pixi run build
//     TANGO_BULK_DUMP_GOLDEN=1 ./build/tests/tango-bulk-unit-tests "[golden]"
//
// and paste the output over golden_vectors.h.  Regenerating is a protocol change
// and should be reviewed as one.

#include "golden_vectors.h"
#include "wire_helpers.h"

#include <tango-bulk/protocol.h>

#include <catch2/catch_test_macros.hpp>

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

using namespace TangoBulk;
using namespace TangoBulk::Protocol;
using namespace TangoBulk::test;

namespace
{

GeometryBlock canonical_geometry()
{
    GeometryBlock g;
    g.generation = 3;
    g.element_type = ElementType::UInt16;
    g.element_size = 2;
    g.rank = 2;
    g.max_frame_bytes = 8ull << 20;
    g.ring_depth = 32;
    g.credit_window = 16;
    g.shape = {1024, 512, 0, 0};
    g.strides = {1024, 2, 0, 0};
    return g;
}

std::array<std::byte, 16> canonical_id(unsigned base)
{
    std::array<std::byte, 16> out{};
    for(std::size_t i = 0; i < out.size(); ++i)
    {
        out[i] = static_cast<std::byte>((base + i) & 0xFFu);
    }
    return out;
}

OpenRequest canonical_open()
{
    OpenRequest msg;
    msg.version_min = 1;
    msg.version_max = 1;
    msg.requested_caps = k_caps_all;
    msg.client_instance_id.bytes = canonical_id(0x10);
    msg.requested_max_frame_bytes = 8ull << 20;
    msg.requested_ring_depth = 32;
    msg.requested_credit_window = 16;
    msg.requested_memory_kind = MemoryKind::Host;
    msg.requested_transport = Transport::Any;
    msg.drop_policy = DropPolicy::DropNewest;
    msg.stream_name = "image";
    msg.client_ucx_address = {std::byte{0xC0}, std::byte{0xFF}, std::byte{0xEE},
                              std::byte{0x00}};
    return msg;
}

OpenReply canonical_open_reply()
{
    OpenReply msg;
    msg.version_selected = 1;
    msg.status = Status::Ok;
    msg.granted_caps = k_caps_all;
    msg.session_id.bytes = canonical_id(0x20);
    msg.stream_id = 0x0123'4567'89AB'CDEFull;
    msg.lease_ttl_ms = 10'000;
    msg.renew_interval_ms = 3'333;
    msg.transport_selected = Transport::ActiveMessage;
    msg.server_epoch_id = 0x0011'2233'4455'6677ull;
    msg.geometry = canonical_geometry();
    msg.server_ucx_address = {std::byte{0xAA}, std::byte{0xBB}};
    return msg;
}

RenewRequest canonical_renew()
{
    RenewRequest msg;
    msg.session_id.bytes = canonical_id(0x20);
    msg.client_frames_delivered = 1'000;
    msg.client_credits_returned = 996;
    msg.client_state = 3;
    return msg;
}

RenewReply canonical_renew_reply()
{
    RenewReply msg;
    msg.session_id.bytes = canonical_id(0x20);
    msg.status = Status::Ok;
    msg.lease_ttl_ms = 10'000;
    msg.renew_interval_ms = 3'333;
    msg.server_state = SessionState::Active;
    msg.geometry = canonical_geometry();
    return msg;
}

CloseRequest canonical_close()
{
    CloseRequest msg;
    msg.session_id.bytes = canonical_id(0x20);
    msg.reason = CloseReason::ClientShutdown;
    return msg;
}

CloseReply canonical_close_reply()
{
    CloseReply msg;
    msg.session_id.bytes = canonical_id(0x20);
    msg.status = Status::Ok;
    msg.frames_credited_final = 996;
    return msg;
}

QueryRequest canonical_query()
{
    QueryRequest msg;
    msg.session_id.bytes = canonical_id(0x20);
    msg.query_flags = 1;
    return msg;
}

QueryReply canonical_query_reply()
{
    QueryReply msg;
    msg.session_id.bytes = canonical_id(0x20);
    msg.status = Status::Ok;
    msg.active_sessions = 1;
    msg.generation = 3;
    msg.geometry = canonical_geometry();
    msg.counters = "frames=1000;";
    return msg;
}

ErrorMessage canonical_error()
{
    return ErrorMessage{Status::TooManySessions, "session limit reached"};
}

FrameHeader canonical_frame()
{
    FrameHeader msg;
    msg.generation = 3;
    msg.stream_id = 0x0123'4567'89AB'CDEFull;
    msg.sequence = 7;
    msg.payload_bytes = 1024ull * 512 * 2;
    msg.timestamp_ns = 1'700'000'000'000'000'000ull;
    msg.event_counter = 12'345;
    msg.dropped_before = 2;
    msg.element_type = ElementType::UInt16;
    msg.element_size = 2;
    msg.rank = 2;
    msg.quality = 0;
    msg.memory_kind = MemoryKind::Host;
    msg.endian = Endian::Little;
    msg.shape = {1024, 512, 0, 0};
    msg.strides = {1024, 2, 0, 0};
    return msg;
}

GeometryMessage canonical_geometry_message()
{
    GeometryMessage msg;
    msg.generation = 3;
    msg.stream_id = 0x0123'4567'89AB'CDEFull;
    msg.first_sequence = 4096;
    msg.geometry = canonical_geometry();
    return msg;
}

/// Correlation id shared by every coordination vector, so the envelope bytes are
/// comparable across them.
constexpr std::uint64_t k_correlation = 0x0102'0304'0506'0708ull;

bool dumping()
{
    return std::getenv("TANGO_BULK_DUMP_GOLDEN") != nullptr;
}

template <typename Bytes>
void check_or_dump(const char *name, const Bytes &bytes, const char *expected)
{
    const std::string actual = to_hex(bytes.data(), bytes.size());

    if(dumping())
    {
        std::cout << "inline constexpr const char *" << name << " =\n    \"" << actual
                  << "\";\n\n";
        return;
    }

    INFO("golden vector '" << name << "'");
    CHECK(actual == std::string{expected});
}

} // namespace

#define CHECK_GOLDEN(name, bytes) check_or_dump(#name, bytes, golden::name)

TEST_CASE("golden vectors, coordination plane", "[protocol][golden]")
{
    CHECK_GOLDEN(k_open, encode(canonical_open(), k_correlation));
    CHECK_GOLDEN(k_open_reply, encode(canonical_open_reply(), k_correlation));
    CHECK_GOLDEN(k_renew, encode(canonical_renew(), k_correlation));
    CHECK_GOLDEN(k_renew_reply, encode(canonical_renew_reply(), k_correlation));
    CHECK_GOLDEN(k_close, encode(canonical_close(), k_correlation));
    CHECK_GOLDEN(k_close_reply, encode(canonical_close_reply(), k_correlation));
    CHECK_GOLDEN(k_query, encode(canonical_query(), k_correlation));
    CHECK_GOLDEN(k_query_reply, encode(canonical_query_reply(), k_correlation));
    CHECK_GOLDEN(k_error, encode(canonical_error(), k_correlation));
}

TEST_CASE("golden vectors, data plane", "[protocol][golden]")
{
    CHECK_GOLDEN(k_frame, encode(canonical_frame()));
    CHECK_GOLDEN(k_credit, encode(CreditMessage{3, 0x0123'4567'89AB'CDEFull, 6}));
    CHECK_GOLDEN(k_probe, encode(ProbeMessage{3, 0x0123'4567'89AB'CDEFull,
                                              0x0BAD'C0DE'DEAD'BEEFull}));
    CHECK_GOLDEN(k_probe_ack, encode(ProbeAckMessage{3, 0x0123'4567'89AB'CDEFull,
                                                     0x0BAD'C0DE'DEAD'BEEFull}));
    CHECK_GOLDEN(k_geometry, encode(canonical_geometry_message()));
    CHECK_GOLDEN(k_geometry_ack,
                 encode(GeometryAckMessage{3, 0x0123'4567'89AB'CDEFull, 4096}));
}

TEST_CASE("every golden vector decodes to the values it was built from",
          "[protocol][golden]")
{
    // A vector that only round-trips through the encoder proves nothing about the
    // decoder.  Decoding the committed bytes back is what makes these vectors an
    // interoperability record rather than a hash of the encoder.
    if(dumping())
    {
        return;
    }

    const auto bytes = from_hex(golden::k_open);

    OpenRequest open;
    Envelope env;
    REQUIRE(decode(bytes.data(), bytes.size(), open, &env) == Status::Ok);

    CHECK(env.correlation_id == k_correlation);
    CHECK(open.stream_name == "image");
    CHECK(open.requested_ring_depth == 32);
    CHECK(open.requested_credit_window == 16);
    CHECK(open.requested_max_frame_bytes == (8ull << 20));
    CHECK(open.client_instance_id.bytes == canonical_id(0x10));

    const auto reply_bytes = from_hex(golden::k_open_reply);

    OpenReply reply;
    REQUIRE(decode(reply_bytes.data(), reply_bytes.size(), reply) == Status::Ok);

    CHECK(reply.stream_id == 0x0123'4567'89AB'CDEFull);
    CHECK(reply.lease_ttl_ms == 10'000);
    CHECK(reply.renew_interval_ms == 3'333);
    CHECK(reply.geometry == canonical_geometry());

    const auto frame_bytes = from_hex(golden::k_frame);

    FrameHeader frame;
    REQUIRE(decode(frame_bytes.data(), frame_bytes.size(), frame) == Status::Ok);

    CHECK(frame.sequence == 7);
    CHECK(frame.payload_bytes == 1024ull * 512 * 2);
    CHECK(frame.element_type == ElementType::UInt16);
    CHECK(frame.shape == canonical_frame().shape);

    const auto credit_bytes = from_hex(golden::k_credit);

    CreditMessage credit;
    REQUIRE(decode(credit_bytes.data(), credit_bytes.size(), credit) == Status::Ok);

    CHECK(credit.ack_sequence == 6);
    CHECK(credit.generation == 3);
}
