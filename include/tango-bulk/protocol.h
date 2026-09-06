// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_PROTOCOL_H
#define TANGO_BULK_PROTOCOL_H

#include <tango-bulk/errors.h>
#include <tango-bulk/frame.h>
#include <tango-bulk/limits.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

/// The tango-bulk wire protocol, major 1 minor 0.
///
/// Two planes, deliberately different in shape:
///
///   - The **coordination plane** is request/reply over ordinary Tango commands,
///     DevVarCharArray in both directions.  Low rate; allowed to allocate.
///   - The **data plane** is one-way messages over UCX active messages.
///     Per-frame rate, fixed size, decoded inside an AM callback with no
///     allocation at all.
///
/// Every multi-byte integer is little-endian regardless of host, every field has
/// an explicit offset, and no native struct is ever memcpy'd onto the wire.  The
/// `Endian` field inside a frame header describes the payload *elements*; the
/// header itself is always little-endian.
namespace TangoBulk::Protocol
{

// ---------------------------------------------------------------------------
// Versions
// ---------------------------------------------------------------------------

inline constexpr std::uint8_t k_version_major = 1;
inline constexpr std::uint8_t k_version_minor = 0;

// ---------------------------------------------------------------------------
// Identifiers
// ---------------------------------------------------------------------------

/// 128-bit opaque octet string.  No endianness: it is never interpreted as a
/// number, which is also why it cannot accidentally be a counter.
struct SessionId
{
    std::array<std::byte, 16> bytes{};

    bool is_zero() const noexcept;

    friend bool operator==(const SessionId &a, const SessionId &b) noexcept;
    friend bool operator!=(const SessionId &a, const SessionId &b) noexcept;
};

/// Client-chosen 128-bit identity, stable across reopen.
struct ClientInstanceId
{
    std::array<std::byte, 16> bytes{};

    bool is_zero() const noexcept;

    friend bool operator==(const ClientInstanceId &a, const ClientInstanceId &b) noexcept;
    friend bool operator!=(const ClientInstanceId &a, const ClientInstanceId &b) noexcept;
};

/// Separate 64-bit data-plane handle, issued in OpenReply.
///
/// It exists because the data plane cannot afford 16 bytes of identity in a
/// per-frame header.  It is valid only on the endpoint it was issued to.
using StreamId = std::uint64_t;

/// Lowercase hex, truncated to the first 8 characters followed by an ellipsis.
///
/// Logs MUST use this form.  Full identifiers, UCX worker addresses, and memory
/// keys must not be logged at any level below debug.
std::string to_log_string(const SessionId &id);
std::string to_log_string(const ClientInstanceId &id);
std::string to_log_string(StreamId id);

/// Full lowercase hex.  For BulkQuery output and tests, not for logs.
std::string to_hex(const SessionId &id);
std::string to_hex(const ClientInstanceId &id);

/// Cryptographically random identifiers.
///
/// Drawn from getrandom(2).  Never a counter, a hash of the client address, a
/// PID, or a timestamp: reuse is what makes a late frame from a dead session
/// indistinguishable from a live one.  Never returns an all-zero SessionId or a
/// zero StreamId, both of which are reserved as local sentinels.
///
/// Throws BulkException{Status::Internal} if the system RNG is unavailable.
/// These are control-path calls only.
SessionId generate_session_id();
ClientInstanceId generate_client_instance_id();
StreamId generate_stream_id();
std::uint64_t generate_server_epoch_id();
std::uint64_t generate_probe_token();

// ---------------------------------------------------------------------------
// Coordination plane
// ---------------------------------------------------------------------------

inline constexpr std::uint32_t k_coord_magic = 0x4B4C4254u; ///< "TBLK" as LE bytes
inline constexpr std::size_t k_coord_envelope_bytes = 32;
inline constexpr std::size_t k_max_coord_message_bytes = 65'536;
inline constexpr std::uint32_t k_max_coord_body_bytes = 65'504;
inline constexpr std::size_t k_max_error_message_bytes = 512;
inline constexpr std::size_t k_max_counters_bytes = 8'192;

enum class CoordType : std::uint16_t
{
    Open = 0x0001,
    OpenReply = 0x0002,
    Renew = 0x0003,
    RenewReply = 0x0004,
    Close = 0x0005,
    CloseReply = 0x0006,
    Query = 0x0007,
    QueryReply = 0x0008,
    Error = 0x00FF,
};

const char *to_string(CoordType type) noexcept;

/// Capability bits in Open::requested_caps / OpenReply::granted_caps.
///
/// Unknown bits MUST be ignored rather than rejected: that is what lets a minor
/// version add a capability without breaking an older server.
inline constexpr std::uint32_t k_caps_credit_coalescing = 1u << 0;
// 1u << 1 was geometry rearm, cut with geometry epochs. Reserved, not reused.
inline constexpr std::uint32_t k_caps_probe = 1u << 2;
inline constexpr std::uint32_t k_caps_all = k_caps_credit_coalescing | k_caps_probe;

enum class Transport : std::uint32_t
{
    Any = 0,
    ActiveMessage = 1, ///< AM rendezvous
    Rma = 2,
};

enum class CloseReason : std::uint32_t
{
    Normal = 0,
    ClientShutdown = 1,
    ClientError = 2,
};

/// Server-side session state, as reported in RenewReply.
enum class SessionState : std::uint32_t
{
    Unknown = 0,
    Open = 1,
    Armed = 2,
    Active = 3,
    Expiring = 4,
    Closed = 5,
};

const char *to_string(SessionState state) noexcept;

/// The 32-byte envelope every coordination message begins with, in both
/// directions.
struct Envelope
{
    std::uint8_t version_major{k_version_major};
    std::uint8_t version_minor{k_version_minor};
    std::uint16_t header_bytes{k_coord_envelope_bytes};
    CoordType msg_type{CoordType::Error};
    std::uint16_t flags{0}; ///< 0 in major 1; reserved
    std::uint32_t body_bytes{0};
    std::uint64_t correlation_id{0}; ///< client-chosen; echoed unchanged
};

/// Read the envelope without committing to a message type.
///
/// A dispatcher needs `msg_type` before it can choose a decoder, and a client
/// must accept `Error` in place of any expected reply.
Status decode_envelope(const std::byte *data, std::size_t size, Envelope &out) noexcept;

// ---------------------------------------------------------------------------
// GeometryBlock -- 96 bytes, embedded in four different messages
// ---------------------------------------------------------------------------

/// One layout, one validator, one set of bounds checks, reused verbatim by
/// OpenReply, RenewReply, QueryReply, and the data-plane Geometry message.
struct GeometryBlock
{
    std::uint32_t generation{0}; ///< epoch; starts at 1, increments, never wraps
    ElementType element_type{ElementType::Unknown};
    std::uint32_t element_size{0};
    std::uint32_t rank{0};
    std::uint64_t max_frame_bytes{0};
    std::uint32_t ring_depth{0};
    std::uint32_t credit_window{0};
    std::array<std::uint64_t, k_max_rank> shape{};
    std::array<std::uint64_t, k_max_rank> strides{};

    /// Applied on every receipt, not only on Open.  A geometry that fails this
    /// is never partially adopted.
    ///
    /// generation == 0 is rejected: zero means "never armed" and is legal only
    /// as a local sentinel, never on the wire.
    Status validate() const noexcept;

    friend bool operator==(const GeometryBlock &a, const GeometryBlock &b) noexcept;
    friend bool operator!=(const GeometryBlock &a, const GeometryBlock &b) noexcept;

    /// Whether two grants describe the same array: element type, element size,
    /// rank, shape and strides.
    ///
    /// Deliberately narrower than operator==, which also compares the epoch and
    /// the sizing terms. A reopened session may legitimately be granted a
    /// smaller ring or a new generation without any consumer's layout becoming
    /// wrong; a different shape or element type is exactly the case where it
    /// does.
    friend bool describes_same_array(const GeometryBlock &a, const GeometryBlock &b) noexcept;
};

inline constexpr std::size_t k_geometry_block_bytes = 96;

// ---------------------------------------------------------------------------
// Coordination messages
// ---------------------------------------------------------------------------

struct OpenRequest
{
    std::uint8_t version_min{k_version_major};
    std::uint8_t version_max{k_version_major};
    std::uint32_t requested_caps{k_caps_all};
    ClientInstanceId client_instance_id{};
    std::uint64_t requested_max_frame_bytes{0};
    std::uint32_t requested_ring_depth{0};
    std::uint32_t requested_credit_window{0};
    MemoryKind requested_memory_kind{MemoryKind::Host};
    Transport requested_transport{Transport::Any};
    DropPolicy drop_policy{DropPolicy::DropNewest};
    std::string stream_name;
    std::vector<std::byte> client_ucx_address;
};

struct OpenReply
{
    std::uint8_t version_selected{k_version_major};
    Status status{Status::Ok};
    std::uint32_t granted_caps{0};
    SessionId session_id{};
    StreamId stream_id{0};
    std::uint32_t lease_ttl_ms{0};
    std::uint32_t renew_interval_ms{0};
    Transport transport_selected{Transport::ActiveMessage};

    /// Changes on server restart.  A client that sees a different value on
    /// reopen knows the server restarted rather than merely dropped the session,
    /// and MUST discard all cached geometry and sequence state.
    std::uint64_t server_epoch_id{0};

    GeometryBlock geometry{};
    std::vector<std::byte> server_ucx_address;
};

struct RenewRequest
{
    SessionId session_id{};
    std::uint64_t client_frames_delivered{0}; ///< cumulative; diagnostics only
    std::uint64_t client_credits_returned{0}; ///< cumulative; diagnostics only
    std::uint32_t client_state{0};            ///< SubscriberState
};

struct RenewReply
{
    SessionId session_id{};
    Status status{Status::Ok};
    std::uint32_t lease_ttl_ms{0};      ///< MAY change; client MUST adopt
    std::uint32_t renew_interval_ms{0}; ///< MAY change; client MUST adopt
    SessionState server_state{SessionState::Unknown};

    /// Always the CURRENT epoch.  This is the only mandatory way a client learns
    /// about a geometry change, because the data-plane Geometry message may be
    /// lost or may race a reconnect.
    GeometryBlock geometry{};
};

struct CloseRequest
{
    SessionId session_id{};
    CloseReason reason{CloseReason::Normal};
};

struct CloseReply
{
    SessionId session_id{};
    Status status{Status::Ok}; ///< Ok, or UnknownSession for an already-closed id
    std::uint32_t frames_credited_final{0}; ///< low 32 bits; diagnostics
};

struct QueryRequest
{
    SessionId session_id{}; ///< all-zero requests server-wide status
    std::uint32_t query_flags{0};
};

struct QueryReply
{
    SessionId session_id{};
    Status status{Status::Ok};
    std::uint32_t active_sessions{0};
    std::uint32_t generation{0};
    GeometryBlock geometry{};

    /// `key=value;` pairs.  MUST NOT contain UCX addresses, memory keys, session
    /// identifiers beyond the truncated form, or hostnames not already known to
    /// the caller.
    std::string counters;
};

struct ErrorMessage
{
    Status status{Status::Internal}; ///< never Ok
    std::string message;             ///< human-readable, 0..512 bytes
};

/// Fixed body sizes, exclusive of the envelope and of any variable tail.
inline constexpr std::size_t k_open_fixed_bytes = 56;
inline constexpr std::size_t k_open_reply_fixed_bytes = 152;
inline constexpr std::size_t k_renew_bytes = 40;
inline constexpr std::size_t k_renew_reply_bytes = 128;
inline constexpr std::size_t k_close_bytes = 24;
inline constexpr std::size_t k_close_reply_bytes = 24;
inline constexpr std::size_t k_query_bytes = 24;
inline constexpr std::size_t k_query_reply_fixed_bytes = 128;
inline constexpr std::size_t k_error_fixed_bytes = 8;

/// Encode, envelope included.  Control path: allowed to allocate, allowed to
/// throw BulkException if the message cannot be represented (an over-long
/// stream name, an address blob past k_max_ucx_address_bytes).
///
/// Fields the spec bounds by truncation on encode -- ErrorMessage::message and
/// QueryReply::counters -- are truncated rather than rejected.
std::vector<std::byte> encode(const OpenRequest &msg, std::uint64_t correlation_id);
std::vector<std::byte> encode(const OpenReply &msg, std::uint64_t correlation_id);
std::vector<std::byte> encode(const RenewRequest &msg, std::uint64_t correlation_id);
std::vector<std::byte> encode(const RenewReply &msg, std::uint64_t correlation_id);
std::vector<std::byte> encode(const CloseRequest &msg, std::uint64_t correlation_id);
std::vector<std::byte> encode(const CloseReply &msg, std::uint64_t correlation_id);
std::vector<std::byte> encode(const QueryRequest &msg, std::uint64_t correlation_id);
std::vector<std::byte> encode(const QueryReply &msg, std::uint64_t correlation_id);
std::vector<std::byte> encode(const ErrorMessage &msg, std::uint64_t correlation_id);

/// Decode, envelope included.  Returns Status::MalformedMessage for anything
/// that does not decode exactly, Status::UnsupportedVersion for a foreign major.
///
/// Never reads past `size`, under any circumstance, including when a declared
/// length claims more than was delivered.  `out` is left unspecified on failure;
/// callers must not read it.
///
/// The optional `envelope` out-parameter receives the decoded envelope, which is
/// how a caller recovers `correlation_id`.
Status decode(const std::byte *data, std::size_t size, OpenRequest &out,
              Envelope *envelope = nullptr) noexcept;
Status decode(const std::byte *data, std::size_t size, OpenReply &out,
              Envelope *envelope = nullptr) noexcept;
Status decode(const std::byte *data, std::size_t size, RenewRequest &out,
              Envelope *envelope = nullptr) noexcept;
Status decode(const std::byte *data, std::size_t size, RenewReply &out,
              Envelope *envelope = nullptr) noexcept;
Status decode(const std::byte *data, std::size_t size, CloseRequest &out,
              Envelope *envelope = nullptr) noexcept;
Status decode(const std::byte *data, std::size_t size, CloseReply &out,
              Envelope *envelope = nullptr) noexcept;
Status decode(const std::byte *data, std::size_t size, QueryRequest &out,
              Envelope *envelope = nullptr) noexcept;
Status decode(const std::byte *data, std::size_t size, QueryReply &out,
              Envelope *envelope = nullptr) noexcept;
Status decode(const std::byte *data, std::size_t size, ErrorMessage &out,
              Envelope *envelope = nullptr) noexcept;

/// Stream names are 1..64 bytes of [A-Za-z0-9_.-].
Status validate_stream_name(const std::string &name) noexcept;

// ---------------------------------------------------------------------------
// Data plane
// ---------------------------------------------------------------------------

inline constexpr std::uint32_t k_data_magic = 0x314B4254u; ///< "TBK1" as LE bytes
inline constexpr std::size_t k_data_prefix_bytes = 16;

enum class DataType : std::uint16_t
{
    Frame = 1,
    Credit = 2,
    Probe = 3,
    ProbeAck = 4,
    // 5 and 6 were Geometry and GeometryAck. RFC 3 and 6.2 cut geometry epochs:
    // a session contract is fixed at Open and changing it is close-and-reopen.
    // Reserved rather than reused, so an old peer's message is unknown here
    // rather than something else.
};

const char *to_string(DataType type) noexcept;

/// Active-message ids.
///
/// Credit and ProbeAck have *separate* ids, unlike the prototype, which shared
/// one and therefore needed a `k_probe_seq = ~0ull` sentinel to stop a probe ack
/// from sliding the credit window past a frame that was never sent.  Separate
/// ids delete the sentinel and the class of bug it guarded against; no such
/// sentinel appears anywhere in this protocol.
inline constexpr unsigned k_am_id_frame = 0;
inline constexpr unsigned k_am_id_credit = 1;
inline constexpr unsigned k_am_id_probe = 2;
inline constexpr unsigned k_am_id_probe_ack = 3;

inline constexpr std::size_t k_frame_header_bytes = 160;
inline constexpr std::size_t k_credit_bytes = 32;
inline constexpr std::size_t k_probe_bytes = 32;
inline constexpr std::size_t k_probe_ack_bytes = 32;

/// The largest data-plane header, which is what the engine must confirm the
/// transport can carry.  The engine queries ucp_worker_attr_t.max_am_header at
/// startup and fails construction if it is below this; discovering the limit at
/// the first frame is not acceptable.

/// Common 16-byte prefix, so an AM callback can classify and version-check
/// before touching anything type-specific.
struct DataPrefix
{
    std::uint8_t version_major{k_version_major};
    std::uint8_t version_minor{k_version_minor};
    std::uint16_t header_bytes{0};
    DataType msg_type{DataType::Frame};
    std::uint16_t flags{0};
    std::uint32_t generation{0}; ///< the geometry epoch this message belongs to
};

Status decode_data_prefix(const std::byte *data, std::size_t size,
                          DataPrefix &out) noexcept;

struct FrameHeader
{
    std::uint32_t generation{0};
    StreamId stream_id{0};
    std::uint64_t sequence{0}; ///< monotonic within a session, from 0, never wraps
    std::uint64_t payload_bytes{0}; ///< == the AM data length; receiver cross-checks
    std::uint64_t timestamp_ns{0};
    std::uint64_t event_counter{0};
    std::uint64_t dropped_before{0};
    ElementType element_type{ElementType::Unknown};
    std::uint32_t element_size{0};
    std::uint32_t rank{0};
    std::uint32_t quality{0};
    MemoryKind memory_kind{MemoryKind::Host};
    Endian endian{Endian::Little}; ///< byte order of payload ELEMENTS
    std::array<std::uint64_t, k_max_rank> shape{};
    std::array<std::uint64_t, k_max_rank> strides{};
};

struct CreditMessage
{
    std::uint32_t generation{0};
    StreamId stream_id{0};

    /// CUMULATIVE: every sequence <= ack_sequence is released.
    ///
    /// Cumulative rather than per-sequence makes credit idempotent (a reordered
    /// lower ack is ignored), loss-tolerant (the next one repairs a lost one, so
    /// there is no retransmit logic), and free to coalesce.
    std::uint64_t ack_sequence{0};
};

struct ProbeMessage
{
    std::uint32_t generation{0};
    StreamId stream_id{0};
    std::uint64_t probe_token{0}; ///< fresh CSPRNG value per probe
};

struct ProbeAckMessage
{
    std::uint32_t generation{0};
    StreamId stream_id{0};
    std::uint64_t probe_token{0}; ///< echoed exactly
};

/// Data-plane encode is allocation-free by construction: the result is a
/// fixed-size array returned by value, ready to hand to UCX as an AM header.
std::array<std::byte, k_frame_header_bytes> encode(const FrameHeader &msg) noexcept;
std::array<std::byte, k_credit_bytes> encode(const CreditMessage &msg) noexcept;
std::array<std::byte, k_probe_bytes> encode(const ProbeMessage &msg) noexcept;
std::array<std::byte, k_probe_ack_bytes> encode(const ProbeAckMessage &msg) noexcept;

/// Data-plane decode runs inside an AM callback: no allocation, no throw, and it
/// must never read past `size` even when `header_bytes` claims more.
Status decode(const std::byte *data, std::size_t size, FrameHeader &out) noexcept;
Status decode(const std::byte *data, std::size_t size, CreditMessage &out) noexcept;
Status decode(const std::byte *data, std::size_t size, ProbeMessage &out) noexcept;
Status decode(const std::byte *data, std::size_t size, ProbeAckMessage &out) noexcept;

} // namespace TangoBulk::Protocol

#endif // TANGO_BULK_PROTOCOL_H
