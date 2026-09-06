// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

// Byte-level conformance to IMPLEMENTATION_SPEC.md section 3.
//
// Every offset asserted here was transcribed from the specification's tables, not
// read out of the implementation, and every integer is checked against a
// little-endian byte pattern computed in the test.  That is what makes these
// tests fail on a big-endian host instead of passing vacuously, and what makes
// them catch a field that moved rather than merely a field that changed.

#include "wire_helpers.h"

#include <tango-bulk/protocol.h>

#include <catch2/catch_test_macros.hpp>

using namespace TangoBulk;
using namespace TangoBulk::Protocol;
using namespace TangoBulk::test;

#define CHECK_FIELD(bytes, offset, width, value) \
    check_le_field((bytes), (offset), (width), (value), #offset " -> " #value)

namespace
{

OpenRequest sample_open()
{
    OpenRequest msg;
    msg.version_min = 1;
    msg.version_max = 1;
    msg.requested_caps = k_probe_u32;
    msg.client_instance_id.bytes = pattern_id16(0x40);
    msg.requested_max_frame_bytes = 8ull << 20;
    msg.requested_ring_depth = 32;
    msg.requested_credit_window = 16;
    msg.requested_memory_kind = MemoryKind::Cuda;
    msg.requested_transport = Transport::Rma;
    msg.drop_policy = DropPolicy::DropOldest;
    msg.stream_name = "image";
    msg.client_ucx_address = {std::byte{0xDE}, std::byte{0xAD}, std::byte{0xBE},
                              std::byte{0xEF}};
    return msg;
}

GeometryBlock sample_geometry()
{
    GeometryBlock g;
    g.generation = 7;
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

} // namespace

// ---------------------------------------------------------------------------
// 3.3 Coordination envelope
// ---------------------------------------------------------------------------

TEST_CASE("coordination envelope matches spec 3.3 byte for byte", "[protocol][layout]")
{
    const auto bytes = encode(CloseRequest{}, k_probe_u64);

    REQUIRE(bytes.size() == k_coord_envelope_bytes + k_close_bytes);

    // magic 0x4B4C4254, which is "TBLK" when written little-endian.  Asserting
    // the characters rather than the number is the whole point: it pins the byte
    // order without restating the constant.
    CHECK(bytes[0] == std::byte{'T'});
    CHECK(bytes[1] == std::byte{'B'});
    CHECK(bytes[2] == std::byte{'L'});
    CHECK(bytes[3] == std::byte{'K'});

    CHECK_FIELD(bytes, 4, 1, 1);                        // version_major
    CHECK_FIELD(bytes, 5, 1, 0);                        // version_minor
    CHECK_FIELD(bytes, 6, 2, 32);                       // header_bytes
    CHECK_FIELD(bytes, 8, 2, 0x0005);                   // msg_type = Close
    CHECK_FIELD(bytes, 10, 2, 0);                       // flags
    CHECK_FIELD(bytes, 12, 4, k_close_bytes);           // body_bytes
    CHECK_FIELD(bytes, 16, 8, k_probe_u64);             // correlation_id
    CHECK_FIELD(bytes, 24, 8, 0);                       // reserved
}

TEST_CASE("every message type carries its spec msg_type", "[protocol][layout]")
{
    const auto type_of = [](const std::vector<std::byte> &bytes)
    {
        return static_cast<unsigned>(bytes[8]) |
               (static_cast<unsigned>(bytes[9]) << 8);
    };

    OpenReply open_reply;
    open_reply.server_ucx_address = {std::byte{1}};
    open_reply.geometry = sample_geometry();

    CHECK(type_of(encode(sample_open(), 0)) == 0x0001);
    CHECK(type_of(encode(open_reply, 0)) == 0x0002);
    CHECK(type_of(encode(RenewRequest{}, 0)) == 0x0003);
    CHECK(type_of(encode(RenewReply{}, 0)) == 0x0004);
    CHECK(type_of(encode(CloseRequest{}, 0)) == 0x0005);
    CHECK(type_of(encode(CloseReply{}, 0)) == 0x0006);
    CHECK(type_of(encode(QueryRequest{}, 0)) == 0x0007);
    CHECK(type_of(encode(QueryReply{}, 0)) == 0x0008);
    CHECK(type_of(encode(ErrorMessage{Status::Internal, ""}, 0)) == 0x00FF);
}

// ---------------------------------------------------------------------------
// 3.4 GeometryBlock
// ---------------------------------------------------------------------------

TEST_CASE("GeometryBlock matches spec 3.4 at its embedded offset", "[protocol][layout]")
{
    RenewReply msg;
    msg.geometry = sample_geometry();

    const auto bytes = encode(msg, 0);

    // RenewReply embeds the block at body offset 32, so absolute offset 64.
    constexpr std::size_t g = k_coord_envelope_bytes + 32;

    REQUIRE(bytes.size() == k_coord_envelope_bytes + k_renew_reply_bytes);

    CHECK_FIELD(bytes, g + 0, 4, 7);            // generation
    CHECK_FIELD(bytes, g + 4, 4, 3);            // element_type = UInt16
    CHECK_FIELD(bytes, g + 8, 4, 2);            // element_size
    CHECK_FIELD(bytes, g + 12, 4, 2);           // rank
    CHECK_FIELD(bytes, g + 16, 8, 8ull << 20);  // max_frame_bytes
    CHECK_FIELD(bytes, g + 24, 4, 32);          // ring_depth
    CHECK_FIELD(bytes, g + 28, 4, 16);          // credit_window
    CHECK_FIELD(bytes, g + 32, 8, 1024);        // shape[0]
    CHECK_FIELD(bytes, g + 40, 8, 512);         // shape[1]
    CHECK_FIELD(bytes, g + 48, 8, 0);           // shape[2]
    CHECK_FIELD(bytes, g + 56, 8, 0);           // shape[3]
    CHECK_FIELD(bytes, g + 64, 8, 1024);        // strides[0]
    CHECK_FIELD(bytes, g + 72, 8, 2);           // strides[1]
    CHECK_FIELD(bytes, g + 80, 8, 0);           // strides[2]
    CHECK_FIELD(bytes, g + 88, 8, 0);           // strides[3]

    // The block is exactly 96 bytes and ends the RenewReply body.
    CHECK(g + k_geometry_block_bytes == bytes.size());
}

// ---------------------------------------------------------------------------
// 3.5 Open
// ---------------------------------------------------------------------------

TEST_CASE("Open body matches spec 3.5", "[protocol][layout]")
{
    const OpenRequest msg = sample_open();
    const auto bytes = encode(msg, 0);

    constexpr std::size_t b = k_coord_envelope_bytes;

    CHECK_FIELD(bytes, b + 0, 1, 1);                 // version_min
    CHECK_FIELD(bytes, b + 1, 1, 1);                 // version_max
    CHECK_FIELD(bytes, b + 2, 2, 0);                 // reserved
    CHECK_FIELD(bytes, b + 4, 4, k_probe_u32);       // requested_caps
    CHECK_FIELD(bytes, b + 24, 8, 8ull << 20);       // requested_max_frame_bytes
    CHECK_FIELD(bytes, b + 32, 4, 32);               // requested_ring_depth
    CHECK_FIELD(bytes, b + 36, 4, 16);               // requested_credit_window
    CHECK_FIELD(bytes, b + 40, 4, 1);                // requested_memory_kind = Cuda
    CHECK_FIELD(bytes, b + 44, 4, 2);                // requested_transport = Rma
    CHECK_FIELD(bytes, b + 48, 4, 1);                // drop_policy = DropOldest
    CHECK_FIELD(bytes, b + 52, 4, 0);                // reserved

    // client_instance_id is 16 opaque octets with no endianness, so it appears in
    // the order it was given.
    for(std::size_t i = 0; i < 16; ++i)
    {
        INFO("client_instance_id octet " << i);
        CHECK(bytes[b + 8 + i] == msg.client_instance_id.bytes[i]);
    }

    // stream_name: u16 length then exactly that many bytes, no NUL.
    CHECK_FIELD(bytes, b + 56, 2, 5);
    CHECK(bytes[b + 58] == std::byte{'i'});
    CHECK(bytes[b + 59] == std::byte{'m'});
    CHECK(bytes[b + 60] == std::byte{'a'});
    CHECK(bytes[b + 61] == std::byte{'g'});
    CHECK(bytes[b + 62] == std::byte{'e'});

    // client_ucx_address: u32 length then the blob.
    CHECK_FIELD(bytes, b + 63, 4, 4);
    CHECK(bytes[b + 67] == std::byte{0xDE});
    CHECK(bytes[b + 70] == std::byte{0xEF});

    CHECK(bytes.size() == b + 56 + 2 + 5 + 4 + 4);
}

// ---------------------------------------------------------------------------
// 3.6 OpenReply
// ---------------------------------------------------------------------------

TEST_CASE("OpenReply body matches spec 3.6", "[protocol][layout]")
{
    OpenReply msg;
    msg.version_selected = 1;
    msg.status = Status::Ok;
    msg.granted_caps = k_caps_all;
    msg.session_id.bytes = pattern_id16(0x10);
    msg.stream_id = k_probe_u64;
    msg.lease_ttl_ms = 10'000;
    msg.renew_interval_ms = 3'333;
    msg.transport_selected = Transport::ActiveMessage;
    msg.server_epoch_id = 0x1122334455667788ull;
    msg.geometry = sample_geometry();
    msg.server_ucx_address = {std::byte{0x01}, std::byte{0x02}};

    const auto bytes = encode(msg, 0);

    constexpr std::size_t b = k_coord_envelope_bytes;

    CHECK_FIELD(bytes, b + 0, 1, 1);                          // version_selected
    CHECK_FIELD(bytes, b + 1, 1, 0);                          // reserved
    CHECK_FIELD(bytes, b + 2, 2, 0);                          // status = Ok
    CHECK_FIELD(bytes, b + 4, 4, k_caps_all);                 // granted_caps
    CHECK_FIELD(bytes, b + 24, 8, k_probe_u64);               // stream_id
    CHECK_FIELD(bytes, b + 32, 4, 10'000);                    // lease_ttl_ms
    CHECK_FIELD(bytes, b + 36, 4, 3'333);                     // renew_interval_ms
    CHECK_FIELD(bytes, b + 40, 4, 1);                         // transport_selected
    CHECK_FIELD(bytes, b + 44, 4, 0);                         // reserved
    CHECK_FIELD(bytes, b + 48, 8, 0x1122334455667788ull);     // server_epoch_id
    CHECK_FIELD(bytes, b + 56, 4, 7);                         // geometry.generation
    CHECK_FIELD(bytes, b + 152, 4, 2);                        // address length

    CHECK(bytes.size() == b + k_open_reply_fixed_bytes + 4 + 2);
}

// ---------------------------------------------------------------------------
// 3.7 / 3.8 fixed-body messages
// ---------------------------------------------------------------------------

TEST_CASE("Renew body matches spec 3.7", "[protocol][layout]")
{
    RenewRequest msg;
    msg.session_id.bytes = pattern_id16(0x20);
    msg.client_frames_delivered = k_probe_u64;
    msg.client_credits_returned = 0x1111222233334444ull;
    msg.client_state = 3; // SubscriberState::Active

    const auto bytes = encode(msg, 0);
    constexpr std::size_t b = k_coord_envelope_bytes;

    REQUIRE(bytes.size() == b + 40);

    CHECK_FIELD(bytes, b + 16, 8, k_probe_u64);
    CHECK_FIELD(bytes, b + 24, 8, 0x1111222233334444ull);
    CHECK_FIELD(bytes, b + 32, 4, 3);
    CHECK_FIELD(bytes, b + 36, 4, 0); // reserved
}

TEST_CASE("RenewReply body matches spec 3.7", "[protocol][layout]")
{
    RenewReply msg;
    msg.status = Status::SessionExpired;
    msg.lease_ttl_ms = 10'000;
    msg.renew_interval_ms = 3'333;
    msg.server_state = SessionState::Expiring;
    msg.geometry = sample_geometry();

    const auto bytes = encode(msg, 0);
    constexpr std::size_t b = k_coord_envelope_bytes;

    REQUIRE(bytes.size() == b + 128);

    CHECK_FIELD(bytes, b + 16, 2, 4); // status = SessionExpired
    CHECK_FIELD(bytes, b + 18, 2, 0); // reserved
    CHECK_FIELD(bytes, b + 20, 4, 10'000);
    CHECK_FIELD(bytes, b + 24, 4, 3'333);
    CHECK_FIELD(bytes, b + 28, 4, 4); // server_state = Expiring
}

TEST_CASE("Close and CloseReply bodies match spec 3.8", "[protocol][layout]")
{
    CloseRequest close;
    close.session_id.bytes = pattern_id16(0x30);
    close.reason = CloseReason::ClientError;

    const auto close_bytes = encode(close, 0);
    constexpr std::size_t b = k_coord_envelope_bytes;

    REQUIRE(close_bytes.size() == b + 24);
    CHECK_FIELD(close_bytes, b + 16, 4, 2); // reason = ClientError
    CHECK_FIELD(close_bytes, b + 20, 4, 0); // reserved

    CloseReply reply;
    reply.status = Status::UnknownSession;
    reply.frames_credited_final = k_probe_u32;

    const auto reply_bytes = encode(reply, 0);

    REQUIRE(reply_bytes.size() == b + 24);
    CHECK_FIELD(reply_bytes, b + 16, 2, 3); // status = UnknownSession
    CHECK_FIELD(reply_bytes, b + 18, 2, 0); // reserved
    CHECK_FIELD(reply_bytes, b + 20, 4, k_probe_u32);
}

TEST_CASE("Query and QueryReply bodies match spec 3.8", "[protocol][layout]")
{
    QueryRequest query;
    query.query_flags = k_probe_u32;

    const auto query_bytes = encode(query, 0);
    constexpr std::size_t b = k_coord_envelope_bytes;

    REQUIRE(query_bytes.size() == b + 24);
    CHECK_FIELD(query_bytes, b + 16, 4, k_probe_u32);
    CHECK_FIELD(query_bytes, b + 20, 4, 0); // reserved

    QueryReply reply;
    reply.active_sessions = 3;
    reply.generation = 7;
    reply.geometry = sample_geometry();
    reply.counters = "frames=10;drops=0;";

    const auto reply_bytes = encode(reply, 0);

    CHECK_FIELD(reply_bytes, b + 16, 2, 0);  // status
    CHECK_FIELD(reply_bytes, b + 18, 2, 0);  // reserved
    CHECK_FIELD(reply_bytes, b + 20, 4, 3);  // active_sessions
    CHECK_FIELD(reply_bytes, b + 24, 4, 7);  // generation
    CHECK_FIELD(reply_bytes, b + 28, 4, 0);  // reserved
    CHECK_FIELD(reply_bytes, b + 32, 4, 7);  // geometry.generation
    CHECK_FIELD(reply_bytes, b + 128, 4, reply.counters.size());

    CHECK(reply_bytes.size() == b + 128 + 4 + reply.counters.size());
}

TEST_CASE("Error body matches spec 3.8", "[protocol][layout]")
{
    const ErrorMessage msg{Status::TooManySessions, "no"};

    const auto bytes = encode(msg, 0);
    constexpr std::size_t b = k_coord_envelope_bytes;

    CHECK_FIELD(bytes, b + 0, 2, 5); // status = TooManySessions
    CHECK_FIELD(bytes, b + 2, 2, 0); // reserved
    CHECK_FIELD(bytes, b + 4, 4, 0); // reserved
    CHECK_FIELD(bytes, b + 8, 2, 2); // message length
    CHECK(bytes[b + 10] == std::byte{'n'});
    CHECK(bytes[b + 11] == std::byte{'o'});

    CHECK(bytes.size() == b + 8 + 2 + 2);
}

// ---------------------------------------------------------------------------
// 3.10 Data-plane common prefix
// ---------------------------------------------------------------------------

TEST_CASE("data-plane prefix matches spec 3.10 byte for byte", "[protocol][layout]")
{
    CreditMessage msg;
    msg.generation = k_probe_u32;
    msg.stream_id = k_probe_u64;
    msg.ack_sequence = 41;

    const auto bytes = encode(msg);

    // magic 0x314B4254, which is "TBK1" written little-endian.
    CHECK(bytes[0] == std::byte{'T'});
    CHECK(bytes[1] == std::byte{'B'});
    CHECK(bytes[2] == std::byte{'K'});
    CHECK(bytes[3] == std::byte{'1'});

    CHECK_FIELD(bytes, 4, 1, 1);              // version_major
    CHECK_FIELD(bytes, 5, 1, 0);              // version_minor
    CHECK_FIELD(bytes, 6, 2, 32);             // header_bytes
    CHECK_FIELD(bytes, 8, 2, 2);              // msg_type = Credit
    CHECK_FIELD(bytes, 10, 2, 0);             // flags
    CHECK_FIELD(bytes, 12, 4, k_probe_u32);   // generation
    CHECK_FIELD(bytes, 16, 8, k_probe_u64);   // stream_id
    CHECK_FIELD(bytes, 24, 8, 41);            // ack_sequence
}

TEST_CASE("data-plane header sizes and AM ids match spec 3.10", "[protocol][layout]")
{
    CHECK(k_frame_header_bytes == 160);
    CHECK(k_credit_bytes == 32);
    CHECK(k_probe_bytes == 32);
    CHECK(k_probe_ack_bytes == 32);

    // Credit and ProbeAck have distinct ids.  The prototype shared one, which is
    // what forced a reserved sequence sentinel to keep a probe ack from sliding
    // the credit window; separate ids are what delete the sentinel.
    CHECK(k_am_id_credit != k_am_id_probe_ack);

    CHECK(k_am_id_frame == 0);
    CHECK(k_am_id_credit == 1);
    CHECK(k_am_id_probe == 2);
    CHECK(k_am_id_probe_ack == 3);
}

// ---------------------------------------------------------------------------
// 3.11 Frame header
// ---------------------------------------------------------------------------

TEST_CASE("Frame header matches spec 3.11 byte for byte", "[protocol][layout]")
{
    FrameHeader msg;
    msg.generation = 7;
    msg.stream_id = k_probe_u64;
    msg.sequence = 0x00000000DEADBEEFull;
    msg.payload_bytes = 1024 * 512 * 2;
    msg.timestamp_ns = 0x1122334455667788ull;
    msg.event_counter = 0x8877665544332211ull;
    msg.dropped_before = 0x0000000100000002ull;
    msg.element_type = ElementType::UInt16;
    msg.element_size = 2;
    msg.rank = 2;
    msg.quality = k_probe_u32;
    msg.memory_kind = MemoryKind::Rocm;
    msg.endian = Endian::Big;
    msg.shape = {1024, 512, 0, 0};
    msg.strides = {1024, 2, 0, 0};

    const auto bytes = encode(msg);

    REQUIRE(bytes.size() == 160);

    CHECK_FIELD(bytes, 8, 2, 1);                          // msg_type = Frame
    CHECK_FIELD(bytes, 6, 2, 160);                        // header_bytes
    CHECK_FIELD(bytes, 12, 4, 7);                         // generation
    CHECK_FIELD(bytes, 16, 8, k_probe_u64);               // stream_id
    CHECK_FIELD(bytes, 24, 8, 0xDEADBEEFull);             // sequence
    CHECK_FIELD(bytes, 32, 8, 1024 * 512 * 2);            // payload_bytes
    CHECK_FIELD(bytes, 40, 8, 0x1122334455667788ull);     // timestamp_ns
    CHECK_FIELD(bytes, 48, 8, 0x8877665544332211ull);     // event_counter
    CHECK_FIELD(bytes, 56, 8, 0x0000000100000002ull);     // dropped_before
    CHECK_FIELD(bytes, 64, 4, 3);                         // element_type = UInt16
    CHECK_FIELD(bytes, 68, 4, 2);                         // element_size
    CHECK_FIELD(bytes, 72, 4, 2);                         // rank
    CHECK_FIELD(bytes, 76, 4, k_probe_u32);               // quality
    CHECK_FIELD(bytes, 80, 4, 2);                         // memory_kind = Rocm
    CHECK_FIELD(bytes, 84, 4, 1);                         // endian = Big
    CHECK_FIELD(bytes, 88, 8, 1024);                      // shape[0]
    CHECK_FIELD(bytes, 96, 8, 512);                       // shape[1]
    CHECK_FIELD(bytes, 104, 8, 0);                        // shape[2]
    CHECK_FIELD(bytes, 112, 8, 0);                        // shape[3]
    CHECK_FIELD(bytes, 120, 8, 1024);                     // strides[0]
    CHECK_FIELD(bytes, 128, 8, 2);                        // strides[1]
    CHECK_FIELD(bytes, 136, 8, 0);                        // strides[2]
    CHECK_FIELD(bytes, 144, 8, 0);                        // strides[3]
    CHECK_FIELD(bytes, 152, 8, 0);                        // reserved

    // `sequence` spelled out literally, as an independent check on the whole
    // little-endian scheme rather than on one helper.
    CHECK(bytes[24] == std::byte{0xEF});
    CHECK(bytes[25] == std::byte{0xBE});
    CHECK(bytes[26] == std::byte{0xAD});
    CHECK(bytes[27] == std::byte{0xDE});
    CHECK(bytes[28] == std::byte{0x00});
    CHECK(bytes[31] == std::byte{0x00});
}

// ---------------------------------------------------------------------------
// 3.13 / 3.14
// ---------------------------------------------------------------------------

TEST_CASE("Probe and ProbeAck match spec 3.13", "[protocol][layout]")
{
    ProbeMessage probe;
    probe.generation = 1;
    probe.stream_id = k_probe_u64;
    probe.probe_token = 0xFEEDFACECAFEBEEFull;

    const auto probe_bytes = encode(probe);

    REQUIRE(probe_bytes.size() == 32);
    CHECK_FIELD(probe_bytes, 8, 2, 3); // msg_type = Probe
    CHECK_FIELD(probe_bytes, 16, 8, k_probe_u64);
    CHECK_FIELD(probe_bytes, 24, 8, 0xFEEDFACECAFEBEEFull);

    ProbeAckMessage ack;
    ack.generation = 1;
    ack.stream_id = k_probe_u64;
    ack.probe_token = 0xFEEDFACECAFEBEEFull;

    const auto ack_bytes = encode(ack);

    REQUIRE(ack_bytes.size() == 32);
    CHECK_FIELD(ack_bytes, 8, 2, 4); // msg_type = ProbeAck
    CHECK_FIELD(ack_bytes, 24, 8, 0xFEEDFACECAFEBEEFull);
}

// ---------------------------------------------------------------------------
// 3.0.2 -- every fixed part is a multiple of 8
// ---------------------------------------------------------------------------

TEST_CASE("every fixed part is a multiple of eight bytes", "[protocol][layout]")
{
    // This is what keeps every integer field on its natural offset without any
    // implicit padding, and it is why no message needs an alignment pad.
    CHECK(k_coord_envelope_bytes % 8 == 0);
    CHECK(k_geometry_block_bytes % 8 == 0);
    CHECK(k_open_fixed_bytes % 8 == 0);
    CHECK(k_open_reply_fixed_bytes % 8 == 0);
    CHECK(k_renew_bytes % 8 == 0);
    CHECK(k_renew_reply_bytes % 8 == 0);
    CHECK(k_close_bytes % 8 == 0);
    CHECK(k_close_reply_bytes % 8 == 0);
    CHECK(k_query_bytes % 8 == 0);
    CHECK(k_query_reply_fixed_bytes % 8 == 0);
    CHECK(k_error_fixed_bytes % 8 == 0);
    CHECK(k_data_prefix_bytes % 8 == 0);
    CHECK(k_frame_header_bytes % 8 == 0);
    CHECK(k_credit_bytes % 8 == 0);
}
