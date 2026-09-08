// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/protocol.h>

#include "core/byte_order.h"
#include "core/geometry_rules.h"

#include <cstring>
#include <utility>

namespace TangoBulk::Protocol
{

namespace
{

// ---------------------------------------------------------------------------
// Envelope
// ---------------------------------------------------------------------------

void put_envelope(std::byte *p, CoordType type, std::uint64_t correlation_id,
                  std::uint32_t body_bytes) noexcept
{
    wire::put32(p + 0, k_coord_magic);
    wire::put8(p + 4, k_version_major);
    wire::put8(p + 5, k_version_minor);
    wire::put16(p + 6, static_cast<std::uint16_t>(k_coord_envelope_bytes));
    wire::put16(p + 8, static_cast<std::uint16_t>(type));
    wire::put16(p + 10, 0); // flags
    wire::put32(p + 12, body_bytes);
    wire::put64(p + 16, correlation_id);
    wire::put64(p + 24, 0); // reserved
}

bool is_known_coord_type(std::uint16_t raw) noexcept
{
    switch(static_cast<CoordType>(raw))
    {
    case CoordType::Open:
    case CoordType::OpenReply:
    case CoordType::Renew:
    case CoordType::RenewReply:
    case CoordType::Close:
    case CoordType::CloseReply:
    case CoordType::Query:
    case CoordType::QueryReply:
    case CoordType::Error:
        return true;
    }

    return false;
}

[[noreturn]] void throw_encode_failure(std::string message)
{
    BulkError error;
    error.status = Status::MalformedMessage;
    error.origin = "protocol";
    error.message = std::move(message);
    throw BulkException{std::move(error)};
}

/// Envelope + zero-filled body, ready for the caller to fill by offset.
std::vector<std::byte> make_message(CoordType type, std::uint64_t correlation_id,
                                   std::size_t body_bytes)
{
    if(body_bytes > k_max_coord_body_bytes)
    {
        throw_encode_failure("coordination body exceeds " +
                             std::to_string(k_max_coord_body_bytes) + " bytes");
    }

    std::vector<std::byte> out(k_coord_envelope_bytes + body_bytes, std::byte{0});
    put_envelope(out.data(), type, correlation_id,
                 static_cast<std::uint32_t>(body_bytes));

    return out;
}

struct Body
{
    const std::byte *data{nullptr};
    std::size_t size{0};
};

/// Validate the envelope of an incoming message and locate its body.
///
/// The exact-length rule is here, once, rather than in nine decoders: the
/// envelope, the fixed body, and every declared variable-length field must sum
/// to exactly the delivered length.  Trailing unexplained bytes are malformed --
/// the trailing-extension rule applies to `header_bytes` within the *fixed* part
/// only, never to the variable tail.
Status open_coord(const std::byte *data, std::size_t size, CoordType expected,
                  Envelope &env, Body &body) noexcept
{
    if(data == nullptr || size < k_coord_envelope_bytes)
    {
        return Status::MalformedMessage;
    }

    if(size > k_max_coord_message_bytes)
    {
        return Status::MalformedMessage;
    }

    // Recover the correlation before validating the version and body. An
    // unsupported peer version can still be answered with an Error carrying
    // the request's correlation; adapters use this field for failure replies.
    env.correlation_id = wire::get64(data + 16);

    if(wire::get32(data + 0) != k_coord_magic)
    {
        return Status::MalformedMessage;
    }

    const std::uint8_t major = wire::get8(data + 4);
    if(major != k_version_major)
    {
        return Status::UnsupportedVersion;
    }

    const std::uint16_t header_bytes = wire::get16(data + 6);
    if(header_bytes < k_coord_envelope_bytes)
    {
        return Status::MalformedMessage;
    }

    if(size < header_bytes)
    {
        return Status::MalformedMessage;
    }

    const std::uint32_t body_bytes = wire::get32(data + 12);
    if(body_bytes > k_max_coord_body_bytes)
    {
        return Status::MalformedMessage;
    }

    if(size != static_cast<std::size_t>(header_bytes) + body_bytes)
    {
        return Status::MalformedMessage;
    }

    const std::uint16_t type_raw = wire::get16(data + 8);
    if(!is_known_coord_type(type_raw))
    {
        return Status::MalformedMessage;
    }

    env.version_major = major;
    env.version_minor = wire::get8(data + 5);
    env.header_bytes = header_bytes;
    env.msg_type = static_cast<CoordType>(type_raw);
    env.flags = wire::get16(data + 10); // reserved in major 1; ignored, not rejected
    env.body_bytes = body_bytes;
    env.correlation_id = wire::get64(data + 16);

    if(env.msg_type != expected)
    {
        return Status::MalformedMessage;
    }

    body.data = data + header_bytes;
    body.size = body_bytes;

    return Status::Ok;
}

/// Length rule for a message whose body is entirely fixed.
///
/// A newer minor may append fields to such a body, so trailing bytes are
/// tolerated when the sender claims a minor above the one we implement.  At our
/// own minor the length is exact, because at our own minor a longer body means a
/// bug rather than an extension.
///
/// Messages with a variable tail get no such tolerance: appending a fixed field
/// to one of those would move the tail, and reading the tail at its old offset
/// would yield garbage.  Requiring exactness there fails safe.
Status check_fixed_body(std::size_t body_size, std::size_t fixed,
                        std::uint8_t minor) noexcept
{
    if(body_size < fixed)
    {
        return Status::MalformedMessage;
    }

    if(body_size > fixed && minor <= k_version_minor)
    {
        return Status::MalformedMessage;
    }

    return Status::Ok;
}

// ---------------------------------------------------------------------------
// GeometryBlock on the wire
// ---------------------------------------------------------------------------

void put_geometry(std::byte *p, const GeometryBlock &g) noexcept
{
    wire::put32(p + 0, g.generation);
    wire::put32(p + 4, static_cast<std::uint32_t>(g.element_type));
    wire::put32(p + 8, g.element_size);
    wire::put32(p + 12, g.rank);
    wire::put64(p + 16, g.max_frame_bytes);
    wire::put32(p + 24, g.ring_depth);
    wire::put32(p + 28, g.credit_window);

    for(std::size_t i = 0; i < k_max_rank; ++i)
    {
        wire::put64(p + 32 + i * 8, g.shape[i]);
        wire::put64(p + 64 + i * 8, g.strides[i]);
    }
}

void get_geometry(const std::byte *p, GeometryBlock &g) noexcept
{
    g.generation = wire::get32(p + 0);
    g.element_type = static_cast<ElementType>(wire::get32(p + 4));
    g.element_size = wire::get32(p + 8);
    g.rank = wire::get32(p + 12);
    g.max_frame_bytes = wire::get64(p + 16);
    g.ring_depth = wire::get32(p + 24);
    g.credit_window = wire::get32(p + 28);

    for(std::size_t i = 0; i < k_max_rank; ++i)
    {
        g.shape[i] = wire::get64(p + 32 + i * 8);
        g.strides[i] = wire::get64(p + 64 + i * 8);
    }
}

// ---------------------------------------------------------------------------
// Identifier fields
// ---------------------------------------------------------------------------

void put_id16(std::byte *p, const std::array<std::byte, 16> &bytes) noexcept
{
    for(std::size_t i = 0; i < bytes.size(); ++i)
    {
        p[i] = bytes[i];
    }
}

void get_id16(const std::byte *p, std::array<std::byte, 16> &bytes) noexcept
{
    for(std::size_t i = 0; i < bytes.size(); ++i)
    {
        bytes[i] = p[i];
    }
}

// ---------------------------------------------------------------------------
// Variable-length tails
// ---------------------------------------------------------------------------

std::size_t put_string_field(std::byte *p, const std::string &value) noexcept
{
    wire::put16(p, static_cast<std::uint16_t>(value.size()));
    std::memcpy(p + 2, value.data(), value.size());
    return 2 + value.size();
}

std::size_t put_blob_field(std::byte *p, const std::vector<std::byte> &value) noexcept
{
    wire::put32(p, static_cast<std::uint32_t>(value.size()));
    if(!value.empty())
    {
        std::memcpy(p + 4, value.data(), value.size());
    }
    return 4 + value.size();
}

/// Bounds-checked cursor over the variable tail.
///
/// Each read checks the declared length against both the field's own bound and
/// the bytes actually remaining *before* allocating anything, so a bogus length
/// cannot turn a 32-byte message into a 4 GiB allocation.
class TailReader
{
  public:
    TailReader(const std::byte *base, std::size_t size, std::size_t offset) noexcept :
        base_(base),
        size_(size),
        offset_(offset)
    {
    }

    Status read_string(std::size_t min_len, std::size_t max_len,
                       std::string &out) noexcept
    {
        if(offset_ + 2 > size_)
        {
            return Status::MalformedMessage;
        }

        const std::size_t len = wire::get16(base_ + offset_);
        if(len < min_len || len > max_len || offset_ + 2 + len > size_)
        {
            return Status::MalformedMessage;
        }

        const char *first = reinterpret_cast<const char *>(base_ + offset_ + 2);

        // No embedded NUL: strings are length-prefixed UTF-8, and a NUL inside
        // one is either a truncation attempt or a C string that leaked onto the
        // wire.  Either way it is not what the sender meant.
        if(std::memchr(first, 0, len) != nullptr)
        {
            return Status::MalformedMessage;
        }

        out.assign(first, len);
        offset_ += 2 + len;

        return Status::Ok;
    }

    Status read_blob(std::size_t min_len, std::size_t max_len,
                     std::vector<std::byte> &out) noexcept
    {
        if(offset_ + 4 > size_)
        {
            return Status::MalformedMessage;
        }

        const std::size_t len = wire::get32(base_ + offset_);
        if(len < min_len || len > max_len || offset_ + 4 + len > size_)
        {
            return Status::MalformedMessage;
        }

        out.assign(base_ + offset_ + 4, base_ + offset_ + 4 + len);
        offset_ += 4 + len;

        return Status::Ok;
    }

    /// Every byte of the message must be accounted for.
    bool at_end() const noexcept
    {
        return offset_ == size_;
    }

  private:
    const std::byte *base_;
    std::size_t size_;
    std::size_t offset_;
};

/// Bounded fields the spec truncates on encode rather than rejecting.
std::string truncated(std::string value, std::size_t max_len)
{
    if(value.size() > max_len)
    {
        value.resize(max_len);
    }

    return value;
}

} // namespace

// ---------------------------------------------------------------------------
// to_string
// ---------------------------------------------------------------------------

const char *to_string(CoordType type) noexcept
{
    switch(type)
    {
    case CoordType::Open:
        return "Open";
    case CoordType::OpenReply:
        return "OpenReply";
    case CoordType::Renew:
        return "Renew";
    case CoordType::RenewReply:
        return "RenewReply";
    case CoordType::Close:
        return "Close";
    case CoordType::CloseReply:
        return "CloseReply";
    case CoordType::Query:
        return "Query";
    case CoordType::QueryReply:
        return "QueryReply";
    case CoordType::Error:
        return "Error";
    }

    return "Unknown";
}

const char *to_string(DataType type) noexcept
{
    switch(type)
    {
    case DataType::Frame:
        return "Frame";
    case DataType::Credit:
        return "Credit";
    case DataType::Probe:
        return "Probe";
    case DataType::ProbeAck:
        return "ProbeAck";
    }

    return "Unknown";
}

const char *to_string(SessionState state) noexcept
{
    switch(state)
    {
    case SessionState::Unknown:
        return "Unknown";
    case SessionState::Open:
        return "Open";
    case SessionState::Armed:
        return "Armed";
    case SessionState::Active:
        return "Active";
    case SessionState::Expiring:
        return "Expiring";
    case SessionState::Closed:
        return "Closed";
    }

    return "Unknown";
}

Status validate_stream_name(const std::string &name) noexcept
{
    if(name.size() < k_min_stream_name_bytes || name.size() > k_max_stream_name_bytes)
    {
        return Status::MalformedMessage;
    }

    for(const char c : name)
    {
        const bool allowed = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                             (c >= '0' && c <= '9') || c == '_' || c == '.' || c == '-';
        if(!allowed)
        {
            return Status::MalformedMessage;
        }
    }

    return Status::Ok;
}

Status decode_envelope(const std::byte *data, std::size_t size, Envelope &out) noexcept
{
    Body ignored;

    // open_coord() checks the type against an expected value; here we accept
    // whatever known type is present, so pass the one that is actually there.
    if(data == nullptr || size < k_coord_envelope_bytes)
    {
        return Status::MalformedMessage;
    }

    const std::uint16_t type_raw = wire::get16(data + 8);
    if(!is_known_coord_type(type_raw))
    {
        // Still validate the rest, so a caller gets MalformedMessage rather than
        // a half-filled envelope with a type it cannot name.
        return Status::MalformedMessage;
    }

    return open_coord(data, size, static_cast<CoordType>(type_raw), out, ignored);
}

// ---------------------------------------------------------------------------
// Open / OpenReply
// ---------------------------------------------------------------------------

std::vector<std::byte> encode(const OpenRequest &msg, std::uint64_t correlation_id)
{
    if(validate_stream_name(msg.stream_name) != Status::Ok)
    {
        throw_encode_failure("stream_name must be 1..64 bytes of [A-Za-z0-9_.-]");
    }

    if(msg.client_ucx_address.empty() ||
       msg.client_ucx_address.size() > k_max_ucx_address_bytes)
    {
        throw_encode_failure("client_ucx_address must be 1.." +
                             std::to_string(k_max_ucx_address_bytes) + " bytes");
    }

    const std::size_t body_bytes = k_open_fixed_bytes + 2 + msg.stream_name.size() + 4 +
                                   msg.client_ucx_address.size();

    std::vector<std::byte> out = make_message(CoordType::Open, correlation_id, body_bytes);
    std::byte *b = out.data() + k_coord_envelope_bytes;

    wire::put8(b + 0, msg.version_min);
    wire::put8(b + 1, msg.version_max);
    wire::put16(b + 2, 0); // reserved
    wire::put32(b + 4, msg.requested_caps);
    put_id16(b + 8, msg.client_instance_id.bytes);
    wire::put64(b + 24, msg.requested_max_frame_bytes);
    wire::put32(b + 32, msg.requested_ring_depth);
    wire::put32(b + 36, msg.requested_credit_window);
    wire::put32(b + 40, static_cast<std::uint32_t>(msg.requested_memory_kind));
    wire::put32(b + 44, static_cast<std::uint32_t>(msg.requested_transport));
    wire::put32(b + 48, static_cast<std::uint32_t>(msg.drop_policy));
    wire::put32(b + 52, 0); // reserved

    std::size_t offset = k_open_fixed_bytes;
    offset += put_string_field(b + offset, msg.stream_name);
    put_blob_field(b + offset, msg.client_ucx_address);

    return out;
}

Status decode(const std::byte *data, std::size_t size, OpenRequest &out,
              Envelope *envelope) noexcept
{
    Envelope env;
    Body body;

    if(const Status status = open_coord(data, size, CoordType::Open, env, body);
       status != Status::Ok)
    {
        return status;
    }

    if(body.size < k_open_fixed_bytes)
    {
        return Status::MalformedMessage;
    }

    OpenRequest msg;
    msg.version_min = wire::get8(body.data + 0);
    msg.version_max = wire::get8(body.data + 1);
    msg.requested_caps = wire::get32(body.data + 4);
    get_id16(body.data + 8, msg.client_instance_id.bytes);
    msg.requested_max_frame_bytes = wire::get64(body.data + 24);
    msg.requested_ring_depth = wire::get32(body.data + 32);
    msg.requested_credit_window = wire::get32(body.data + 36);
    msg.requested_memory_kind = static_cast<MemoryKind>(wire::get32(body.data + 40));
    msg.requested_transport = static_cast<Transport>(wire::get32(body.data + 44));
    msg.drop_policy = static_cast<DropPolicy>(wire::get32(body.data + 48));

    TailReader tail{body.data, body.size, k_open_fixed_bytes};

    if(const Status status = tail.read_string(k_min_stream_name_bytes,
                                             k_max_stream_name_bytes, msg.stream_name);
       status != Status::Ok)
    {
        return status;
    }

    if(const Status status = tail.read_blob(1, k_max_ucx_address_bytes,
                                            msg.client_ucx_address);
       status != Status::Ok)
    {
        return status;
    }

    if(!tail.at_end())
    {
        return Status::MalformedMessage;
    }

    if(const Status status = validate_stream_name(msg.stream_name); status != Status::Ok)
    {
        return status;
    }

    out = std::move(msg);
    if(envelope != nullptr)
    {
        *envelope = env;
    }

    return Status::Ok;
}

std::vector<std::byte> encode(const OpenReply &msg, std::uint64_t correlation_id)
{
    if(msg.server_ucx_address.size() > k_max_ucx_address_bytes)
    {
        throw_encode_failure("server_ucx_address exceeds " +
                             std::to_string(k_max_ucx_address_bytes) + " bytes");
    }

    // A non-Ok reply carries no address, and the spec says the remaining fields
    // are undefined in that case.  Requiring one would force an error path to
    // invent a value.
    if(msg.status == Status::Ok && msg.server_ucx_address.empty())
    {
        throw_encode_failure("a successful OpenReply must carry server_ucx_address");
    }

    const std::size_t body_bytes =
        k_open_reply_fixed_bytes + 4 + msg.server_ucx_address.size();

    std::vector<std::byte> out =
        make_message(CoordType::OpenReply, correlation_id, body_bytes);
    std::byte *b = out.data() + k_coord_envelope_bytes;

    wire::put8(b + 0, msg.version_selected);
    wire::put8(b + 1, 0); // reserved
    wire::put16(b + 2, static_cast<std::uint16_t>(msg.status));
    wire::put32(b + 4, msg.granted_caps);
    put_id16(b + 8, msg.session_id.bytes);
    wire::put64(b + 24, msg.stream_id);
    wire::put32(b + 32, msg.lease_ttl_ms);
    wire::put32(b + 36, msg.renew_interval_ms);
    wire::put32(b + 40, static_cast<std::uint32_t>(msg.transport_selected));
    wire::put32(b + 44, 0); // reserved
    wire::put64(b + 48, msg.server_epoch_id);
    put_geometry(b + 56, msg.geometry);

    put_blob_field(b + k_open_reply_fixed_bytes, msg.server_ucx_address);

    return out;
}

Status decode(const std::byte *data, std::size_t size, OpenReply &out,
              Envelope *envelope) noexcept
{
    Envelope env;
    Body body;

    if(const Status status = open_coord(data, size, CoordType::OpenReply, env, body);
       status != Status::Ok)
    {
        return status;
    }

    if(body.size < k_open_reply_fixed_bytes)
    {
        return Status::MalformedMessage;
    }

    OpenReply msg;
    msg.version_selected = wire::get8(body.data + 0);
    msg.status = static_cast<Status>(wire::get16(body.data + 2));
    msg.granted_caps = wire::get32(body.data + 4);
    get_id16(body.data + 8, msg.session_id.bytes);
    msg.stream_id = wire::get64(body.data + 24);
    msg.lease_ttl_ms = wire::get32(body.data + 32);
    msg.renew_interval_ms = wire::get32(body.data + 36);
    msg.transport_selected = static_cast<Transport>(wire::get32(body.data + 40));
    msg.server_epoch_id = wire::get64(body.data + 48);
    get_geometry(body.data + 56, msg.geometry);

    TailReader tail{body.data, body.size, k_open_reply_fixed_bytes};

    if(const Status status =
           tail.read_blob(0, k_max_ucx_address_bytes, msg.server_ucx_address);
       status != Status::Ok)
    {
        return status;
    }

    if(!tail.at_end())
    {
        return Status::MalformedMessage;
    }

    out = std::move(msg);
    if(envelope != nullptr)
    {
        *envelope = env;
    }

    return Status::Ok;
}

// ---------------------------------------------------------------------------
// Renew / RenewReply
// ---------------------------------------------------------------------------

std::vector<std::byte> encode(const RenewRequest &msg, std::uint64_t correlation_id)
{
    std::vector<std::byte> out =
        make_message(CoordType::Renew, correlation_id, k_renew_bytes);
    std::byte *b = out.data() + k_coord_envelope_bytes;

    put_id16(b + 0, msg.session_id.bytes);
    wire::put64(b + 16, msg.client_frames_delivered);
    wire::put64(b + 24, msg.client_credits_returned);
    wire::put32(b + 32, msg.client_state);
    wire::put32(b + 36, 0); // reserved

    return out;
}

Status decode(const std::byte *data, std::size_t size, RenewRequest &out,
              Envelope *envelope) noexcept
{
    Envelope env;
    Body body;

    if(const Status status = open_coord(data, size, CoordType::Renew, env, body);
       status != Status::Ok)
    {
        return status;
    }

    if(const Status status =
           check_fixed_body(body.size, k_renew_bytes, env.version_minor);
       status != Status::Ok)
    {
        return status;
    }

    RenewRequest msg;
    get_id16(body.data + 0, msg.session_id.bytes);
    msg.client_frames_delivered = wire::get64(body.data + 16);
    msg.client_credits_returned = wire::get64(body.data + 24);
    msg.client_state = wire::get32(body.data + 32);

    out = msg;
    if(envelope != nullptr)
    {
        *envelope = env;
    }

    return Status::Ok;
}

std::vector<std::byte> encode(const RenewReply &msg, std::uint64_t correlation_id)
{
    std::vector<std::byte> out =
        make_message(CoordType::RenewReply, correlation_id, k_renew_reply_bytes);
    std::byte *b = out.data() + k_coord_envelope_bytes;

    put_id16(b + 0, msg.session_id.bytes);
    wire::put16(b + 16, static_cast<std::uint16_t>(msg.status));
    wire::put16(b + 18, 0); // reserved
    wire::put32(b + 20, msg.lease_ttl_ms);
    wire::put32(b + 24, msg.renew_interval_ms);
    wire::put32(b + 28, static_cast<std::uint32_t>(msg.server_state));
    put_geometry(b + 32, msg.geometry);

    return out;
}

Status decode(const std::byte *data, std::size_t size, RenewReply &out,
              Envelope *envelope) noexcept
{
    Envelope env;
    Body body;

    if(const Status status = open_coord(data, size, CoordType::RenewReply, env, body);
       status != Status::Ok)
    {
        return status;
    }

    if(const Status status =
           check_fixed_body(body.size, k_renew_reply_bytes, env.version_minor);
       status != Status::Ok)
    {
        return status;
    }

    RenewReply msg;
    get_id16(body.data + 0, msg.session_id.bytes);
    msg.status = static_cast<Status>(wire::get16(body.data + 16));
    msg.lease_ttl_ms = wire::get32(body.data + 20);
    msg.renew_interval_ms = wire::get32(body.data + 24);
    msg.server_state = static_cast<SessionState>(wire::get32(body.data + 28));
    get_geometry(body.data + 32, msg.geometry);

    out = msg;
    if(envelope != nullptr)
    {
        *envelope = env;
    }

    return Status::Ok;
}

// ---------------------------------------------------------------------------
// Close / CloseReply
// ---------------------------------------------------------------------------

std::vector<std::byte> encode(const CloseRequest &msg, std::uint64_t correlation_id)
{
    std::vector<std::byte> out =
        make_message(CoordType::Close, correlation_id, k_close_bytes);
    std::byte *b = out.data() + k_coord_envelope_bytes;

    put_id16(b + 0, msg.session_id.bytes);
    wire::put32(b + 16, static_cast<std::uint32_t>(msg.reason));
    wire::put32(b + 20, 0); // reserved

    return out;
}

Status decode(const std::byte *data, std::size_t size, CloseRequest &out,
              Envelope *envelope) noexcept
{
    Envelope env;
    Body body;

    if(const Status status = open_coord(data, size, CoordType::Close, env, body);
       status != Status::Ok)
    {
        return status;
    }

    if(const Status status =
           check_fixed_body(body.size, k_close_bytes, env.version_minor);
       status != Status::Ok)
    {
        return status;
    }

    CloseRequest msg;
    get_id16(body.data + 0, msg.session_id.bytes);
    msg.reason = static_cast<CloseReason>(wire::get32(body.data + 16));

    out = msg;
    if(envelope != nullptr)
    {
        *envelope = env;
    }

    return Status::Ok;
}

std::vector<std::byte> encode(const CloseReply &msg, std::uint64_t correlation_id)
{
    std::vector<std::byte> out =
        make_message(CoordType::CloseReply, correlation_id, k_close_reply_bytes);
    std::byte *b = out.data() + k_coord_envelope_bytes;

    put_id16(b + 0, msg.session_id.bytes);
    wire::put16(b + 16, static_cast<std::uint16_t>(msg.status));
    wire::put16(b + 18, 0); // reserved
    wire::put32(b + 20, msg.frames_credited_final);

    return out;
}

Status decode(const std::byte *data, std::size_t size, CloseReply &out,
              Envelope *envelope) noexcept
{
    Envelope env;
    Body body;

    if(const Status status = open_coord(data, size, CoordType::CloseReply, env, body);
       status != Status::Ok)
    {
        return status;
    }

    if(const Status status =
           check_fixed_body(body.size, k_close_reply_bytes, env.version_minor);
       status != Status::Ok)
    {
        return status;
    }

    CloseReply msg;
    get_id16(body.data + 0, msg.session_id.bytes);
    msg.status = static_cast<Status>(wire::get16(body.data + 16));
    msg.frames_credited_final = wire::get32(body.data + 20);

    out = msg;
    if(envelope != nullptr)
    {
        *envelope = env;
    }

    return Status::Ok;
}

// ---------------------------------------------------------------------------
// Query / QueryReply
// ---------------------------------------------------------------------------

std::vector<std::byte> encode(const QueryRequest &msg, std::uint64_t correlation_id)
{
    std::vector<std::byte> out =
        make_message(CoordType::Query, correlation_id, k_query_bytes);
    std::byte *b = out.data() + k_coord_envelope_bytes;

    put_id16(b + 0, msg.session_id.bytes);
    wire::put32(b + 16, msg.query_flags);
    wire::put32(b + 20, 0); // reserved

    return out;
}

Status decode(const std::byte *data, std::size_t size, QueryRequest &out,
              Envelope *envelope) noexcept
{
    Envelope env;
    Body body;

    if(const Status status = open_coord(data, size, CoordType::Query, env, body);
       status != Status::Ok)
    {
        return status;
    }

    if(const Status status =
           check_fixed_body(body.size, k_query_bytes, env.version_minor);
       status != Status::Ok)
    {
        return status;
    }

    QueryRequest msg;
    get_id16(body.data + 0, msg.session_id.bytes);
    msg.query_flags = wire::get32(body.data + 16);

    out = msg;
    if(envelope != nullptr)
    {
        *envelope = env;
    }

    return Status::Ok;
}

std::vector<std::byte> encode(const QueryReply &msg, std::uint64_t correlation_id)
{
    const std::string counters = truncated(msg.counters, k_max_counters_bytes);

    const std::size_t body_bytes = k_query_reply_fixed_bytes + 4 + counters.size();

    std::vector<std::byte> out =
        make_message(CoordType::QueryReply, correlation_id, body_bytes);
    std::byte *b = out.data() + k_coord_envelope_bytes;

    put_id16(b + 0, msg.session_id.bytes);
    wire::put16(b + 16, static_cast<std::uint16_t>(msg.status));
    wire::put16(b + 18, 0); // reserved
    wire::put32(b + 20, msg.active_sessions);
    wire::put32(b + 24, msg.generation);
    wire::put32(b + 28, 0); // reserved
    put_geometry(b + 32, msg.geometry);

    // The counter blob is length-prefixed with a u32, like a blob rather than a
    // string, because the spec sizes it that way.
    std::byte *tail = b + k_query_reply_fixed_bytes;
    wire::put32(tail, static_cast<std::uint32_t>(counters.size()));
    if(!counters.empty())
    {
        std::memcpy(tail + 4, counters.data(), counters.size());
    }

    return out;
}

Status decode(const std::byte *data, std::size_t size, QueryReply &out,
              Envelope *envelope) noexcept
{
    Envelope env;
    Body body;

    if(const Status status = open_coord(data, size, CoordType::QueryReply, env, body);
       status != Status::Ok)
    {
        return status;
    }

    if(body.size < k_query_reply_fixed_bytes)
    {
        return Status::MalformedMessage;
    }

    QueryReply msg;
    get_id16(body.data + 0, msg.session_id.bytes);
    msg.status = static_cast<Status>(wire::get16(body.data + 16));
    msg.active_sessions = wire::get32(body.data + 20);
    msg.generation = wire::get32(body.data + 24);
    get_geometry(body.data + 32, msg.geometry);

    std::size_t offset = k_query_reply_fixed_bytes;
    if(offset + 4 > body.size)
    {
        return Status::MalformedMessage;
    }

    const std::size_t len = wire::get32(body.data + offset);
    if(len > k_max_counters_bytes || offset + 4 + len != body.size)
    {
        return Status::MalformedMessage;
    }

    const char *first = reinterpret_cast<const char *>(body.data + offset + 4);
    if(std::memchr(first, 0, len) != nullptr)
    {
        return Status::MalformedMessage;
    }

    msg.counters.assign(first, len);

    out = std::move(msg);
    if(envelope != nullptr)
    {
        *envelope = env;
    }

    return Status::Ok;
}

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

std::vector<std::byte> encode(const ErrorMessage &msg, std::uint64_t correlation_id)
{
    const std::string message = truncated(msg.message, k_max_error_message_bytes);

    const std::size_t body_bytes = k_error_fixed_bytes + 2 + message.size();

    std::vector<std::byte> out =
        make_message(CoordType::Error, correlation_id, body_bytes);
    std::byte *b = out.data() + k_coord_envelope_bytes;

    wire::put16(b + 0, static_cast<std::uint16_t>(msg.status));
    wire::put16(b + 2, 0); // reserved
    wire::put32(b + 4, 0); // reserved

    put_string_field(b + k_error_fixed_bytes, message);

    return out;
}

Status decode(const std::byte *data, std::size_t size, ErrorMessage &out,
              Envelope *envelope) noexcept
{
    Envelope env;
    Body body;

    if(const Status status = open_coord(data, size, CoordType::Error, env, body);
       status != Status::Ok)
    {
        return status;
    }

    if(body.size < k_error_fixed_bytes)
    {
        return Status::MalformedMessage;
    }

    ErrorMessage msg;
    msg.status = static_cast<Status>(wire::get16(body.data + 0));

    TailReader tail{body.data, body.size, k_error_fixed_bytes};

    if(const Status status = tail.read_string(0, k_max_error_message_bytes, msg.message);
       status != Status::Ok)
    {
        return status;
    }

    if(!tail.at_end())
    {
        return Status::MalformedMessage;
    }

    out = std::move(msg);
    if(envelope != nullptr)
    {
        *envelope = env;
    }

    return Status::Ok;
}

// ---------------------------------------------------------------------------
// Data plane
// ---------------------------------------------------------------------------

namespace
{

void put_data_prefix(std::byte *p, DataType type, std::size_t header_bytes,
                     std::uint32_t generation) noexcept
{
    wire::put32(p + 0, k_data_magic);
    wire::put8(p + 4, k_version_major);
    wire::put8(p + 5, k_version_minor);
    wire::put16(p + 6, static_cast<std::uint16_t>(header_bytes));
    wire::put16(p + 8, static_cast<std::uint16_t>(type));
    wire::put16(p + 10, 0); // flags
    wire::put32(p + 12, generation);
}

std::size_t min_header_bytes(DataType type) noexcept
{
    switch(type)
    {
    case DataType::Frame:
        return k_frame_header_bytes;
    case DataType::Credit:
        return k_credit_bytes;
    case DataType::Probe:
        return k_probe_bytes;
    case DataType::ProbeAck:
        return k_probe_ack_bytes;
    }

    return 0;
}

bool is_known_data_type(std::uint16_t raw) noexcept
{
    return min_header_bytes(static_cast<DataType>(raw)) != 0;
}

/// Classify and version-check a data-plane message, then confirm the delivered
/// length can hold everything the header claims.
///
/// The order matters.  `header_bytes` is read before it is trusted, and the
/// bound checks are `header_bytes >= minimum for this (major, msg_type)` and
/// `size >= header_bytes` -- so a header claiming more than was delivered is
/// rejected rather than read.  Anything beyond our known layout is ignored,
/// which is what makes minor-version forward compatibility mechanical.
Status open_data(const std::byte *data, std::size_t size, DataType expected,
                 DataPrefix &out) noexcept
{
    if(data == nullptr || size < k_data_prefix_bytes)
    {
        return Status::MalformedMessage;
    }

    if(wire::get32(data + 0) != k_data_magic)
    {
        return Status::MalformedMessage;
    }

    const std::uint8_t major = wire::get8(data + 4);
    if(major != k_version_major)
    {
        return Status::UnsupportedVersion;
    }

    const std::uint16_t type_raw = wire::get16(data + 8);
    if(type_raw != static_cast<std::uint16_t>(expected))
    {
        return Status::MalformedMessage;
    }

    const std::uint16_t header_bytes = wire::get16(data + 6);
    if(header_bytes < min_header_bytes(expected) || size < header_bytes)
    {
        return Status::MalformedMessage;
    }

    out.version_major = major;
    out.version_minor = wire::get8(data + 5);
    out.header_bytes = header_bytes;
    out.msg_type = expected;
    out.flags = wire::get16(data + 10);
    out.generation = wire::get32(data + 12);

    return Status::Ok;
}

} // namespace

Status decode_data_prefix(const std::byte *data, std::size_t size,
                          DataPrefix &out) noexcept
{
    if(data == nullptr || size < k_data_prefix_bytes)
    {
        return Status::MalformedMessage;
    }

    if(wire::get32(data + 0) != k_data_magic)
    {
        return Status::MalformedMessage;
    }

    const std::uint16_t type_raw = wire::get16(data + 8);
    if(!is_known_data_type(type_raw))
    {
        return Status::MalformedMessage;
    }

    return open_data(data, size, static_cast<DataType>(type_raw), out);
}

std::array<std::byte, k_frame_header_bytes> encode(const FrameHeader &msg) noexcept
{
    std::array<std::byte, k_frame_header_bytes> out{};
    std::byte *p = out.data();

    put_data_prefix(p, DataType::Frame, k_frame_header_bytes, msg.generation);

    wire::put64(p + 16, msg.stream_id);
    wire::put64(p + 24, msg.sequence);
    wire::put64(p + 32, msg.payload_bytes);
    wire::put64(p + 40, msg.timestamp_ns);
    wire::put64(p + 48, msg.event_counter);
    wire::put64(p + 56, msg.dropped_before);
    wire::put32(p + 64, static_cast<std::uint32_t>(msg.element_type));
    wire::put32(p + 68, msg.element_size);
    wire::put32(p + 72, msg.rank);
    wire::put32(p + 76, msg.quality);
    wire::put32(p + 80, static_cast<std::uint32_t>(msg.memory_kind));
    wire::put32(p + 84, static_cast<std::uint32_t>(msg.endian));

    for(std::size_t i = 0; i < k_max_rank; ++i)
    {
        wire::put64(p + 88 + i * 8, msg.shape[i]);
        wire::put64(p + 120 + i * 8, msg.strides[i]);
    }

    wire::put64(p + 152, 0); // reserved

    return out;
}

Status decode(const std::byte *data, std::size_t size, FrameHeader &out) noexcept
{
    DataPrefix prefix;

    if(const Status status = open_data(data, size, DataType::Frame, prefix);
       status != Status::Ok)
    {
        return status;
    }

    FrameHeader msg;
    msg.generation = prefix.generation;
    msg.stream_id = wire::get64(data + 16);
    msg.sequence = wire::get64(data + 24);
    msg.payload_bytes = wire::get64(data + 32);
    msg.timestamp_ns = wire::get64(data + 40);
    msg.event_counter = wire::get64(data + 48);
    msg.dropped_before = wire::get64(data + 56);
    msg.element_type = static_cast<ElementType>(wire::get32(data + 64));
    msg.element_size = wire::get32(data + 68);
    msg.rank = wire::get32(data + 72);
    msg.quality = wire::get32(data + 76);
    msg.memory_kind = static_cast<MemoryKind>(wire::get32(data + 80));
    msg.endian = static_cast<Endian>(wire::get32(data + 84));

    for(std::size_t i = 0; i < k_max_rank; ++i)
    {
        msg.shape[i] = wire::get64(data + 88 + i * 8);
        msg.strides[i] = wire::get64(data + 120 + i * 8);
    }

    // The geometry-equivalent field checks, applied here so that no receive path
    // can forget them.  The session-scoped checks -- stream_id, payload_bytes
    // against the AM data length, generation against the armed epoch, sequence
    // against the credit window -- need session state and stay with the receiver.
    if(msg.payload_bytes == 0 || msg.payload_bytes > k_max_frame_bytes_hard_cap)
    {
        return Status::FrameTooLarge;
    }

    if(const Status status = detail::validate_element_size(msg.element_type,
                                                          msg.element_size);
       status != Status::Ok)
    {
        return status;
    }

    // The frame's own payload_bytes is the limit here, not a granted
    // max_frame_bytes: the described array has to fit in the bytes that actually
    // arrived.  Checking against the grant as well is the receiver's step, since
    // only it knows the grant.
    if(const Status status = detail::validate_shape_and_strides(
           msg.rank, msg.shape, msg.strides, msg.element_size, msg.payload_bytes);
       status != Status::Ok)
    {
        return status;
    }

    out = msg;

    return Status::Ok;
}

std::array<std::byte, k_credit_bytes> encode(const CreditMessage &msg) noexcept
{
    std::array<std::byte, k_credit_bytes> out{};
    std::byte *p = out.data();

    put_data_prefix(p, DataType::Credit, k_credit_bytes, msg.generation);
    wire::put64(p + 16, msg.stream_id);
    wire::put64(p + 24, msg.ack_sequence);

    return out;
}

Status decode(const std::byte *data, std::size_t size, CreditMessage &out) noexcept
{
    DataPrefix prefix;

    if(const Status status = open_data(data, size, DataType::Credit, prefix);
       status != Status::Ok)
    {
        return status;
    }

    out.generation = prefix.generation;
    out.stream_id = wire::get64(data + 16);
    out.ack_sequence = wire::get64(data + 24);

    return Status::Ok;
}

std::array<std::byte, k_probe_bytes> encode(const ProbeMessage &msg) noexcept
{
    std::array<std::byte, k_probe_bytes> out{};
    std::byte *p = out.data();

    put_data_prefix(p, DataType::Probe, k_probe_bytes, msg.generation);
    wire::put64(p + 16, msg.stream_id);
    wire::put64(p + 24, msg.probe_token);

    return out;
}

Status decode(const std::byte *data, std::size_t size, ProbeMessage &out) noexcept
{
    DataPrefix prefix;

    if(const Status status = open_data(data, size, DataType::Probe, prefix);
       status != Status::Ok)
    {
        return status;
    }

    out.generation = prefix.generation;
    out.stream_id = wire::get64(data + 16);
    out.probe_token = wire::get64(data + 24);

    return Status::Ok;
}

std::array<std::byte, k_probe_ack_bytes> encode(const ProbeAckMessage &msg) noexcept
{
    std::array<std::byte, k_probe_ack_bytes> out{};
    std::byte *p = out.data();

    put_data_prefix(p, DataType::ProbeAck, k_probe_ack_bytes, msg.generation);
    wire::put64(p + 16, msg.stream_id);
    wire::put64(p + 24, msg.probe_token);

    return out;
}

Status decode(const std::byte *data, std::size_t size, ProbeAckMessage &out) noexcept
{
    DataPrefix prefix;

    if(const Status status = open_data(data, size, DataType::ProbeAck, prefix);
       status != Status::Ok)
    {
        return status;
    }

    out.generation = prefix.generation;
    out.stream_id = wire::get64(data + 16);
    out.probe_token = wire::get64(data + 24);

    return Status::Ok;
}


} // namespace TangoBulk::Protocol
