// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <core/session_client.h>

namespace TangoBulk::detail
{

std::vector<std::byte> SessionClient::make_open_request(
    const SubscriberConfig &config,
    const std::vector<std::byte> &client_address,
    std::uint64_t correlation_id) const
{
    Protocol::OpenRequest request;
    request.version_min = Protocol::k_version_major;
    request.version_max = Protocol::k_version_major;
    // Coalescing and probe.  Geometry re-arm is left out because 4.3's two-ring
    // interlock is not implemented, and claiming a capability this side cannot
    // honour is worse than not having it.
    request.requested_caps = Protocol::k_caps_credit_coalescing | Protocol::k_caps_probe;
    request.client_instance_id = Protocol::generate_client_instance_id();
    request.requested_max_frame_bytes = config.max_frame_bytes;
    request.requested_ring_depth = config.ring_depth;
    request.requested_credit_window = config.credit_window;
    request.requested_memory_kind = config.receive_memory_kind;
    request.requested_transport = Protocol::Transport::ActiveMessage;
    request.drop_policy = config.drop_policy;
    request.stream_name = config.stream_name;
    request.client_ucx_address = client_address;

    return Protocol::encode(request, correlation_id);
}

Status SessionClient::adopt_open_reply(const std::byte *data,
                                       std::size_t size,
                                       const SubscriberConfig &config)
{
    Protocol::Envelope envelope;
    if(Protocol::decode_envelope(data, size, envelope) != Status::Ok)
    {
        return Status::MalformedMessage;
    }

    // A client must accept Error in place of any expected reply.
    if(envelope.msg_type == Protocol::CoordType::Error)
    {
        Protocol::ErrorMessage error;
        const Status decoded = Protocol::decode(data, size, error);
        return decoded == Status::Ok ? error.status : Status::MalformedMessage;
    }

    Protocol::OpenReply reply;
    if(Protocol::decode(data, size, reply) != Status::Ok)
    {
        return Status::MalformedMessage;
    }

    if(reply.status != Status::Ok)
    {
        return reply.status;
    }

    if(reply.geometry.validate() != Status::Ok)
    {
        return Status::GeometryMismatch;
    }

    // A grant is only ever clamped downward (3.5 step 5), so a ring registered
    // at the requested geometry is always large enough for the granted one.
    // Checking anyway, because "always" here depends on the peer behaving.
    if(reply.geometry.max_frame_bytes > config.max_frame_bytes ||
       reply.geometry.ring_depth > config.ring_depth)
    {
        return Status::GeometryMismatch;
    }

    session_id_ = reply.session_id;
    stream_id_ = reply.stream_id;
    server_address_ = reply.server_ucx_address;

    // The whole block, not four fields out of it. Shape, strides and element
    // type are the description the application needs and the receive path has
    // to check each frame against; copying out the three numbers the transport
    // happened to need was what made both impossible.
    granted_ = reply.geometry;

    lease_ttl_ms_ = reply.lease_ttl_ms;
    renew_interval_ms_ = reply.renew_interval_ms;

    return Status::Ok;
}

std::vector<std::byte> SessionClient::make_renew_request(std::uint64_t correlation_id,
                                                        std::uint64_t frames_delivered,
                                                        std::uint64_t credits_returned,
                                                        std::uint32_t client_state) const
{
    Protocol::RenewRequest request;
    request.session_id = session_id_;
    request.client_frames_delivered = frames_delivered;
    request.client_credits_returned = credits_returned;
    request.client_state = client_state;

    return Protocol::encode(request, correlation_id);
}

SessionClient::RenewOutcome SessionClient::adopt_renew_reply(const std::byte *data, std::size_t size)
{
    Protocol::Envelope envelope;
    if(Protocol::decode_envelope(data, size, envelope) != Status::Ok)
    {
        return {Status::MalformedMessage, false};
    }

    // 7.4: a transport-level failure of the command is a recovery hint, and so
    // is an Error in place of the reply.  Neither is terminal on its own -- only
    // the server saying the session is gone is.
    if(envelope.msg_type == Protocol::CoordType::Error)
    {
        Protocol::ErrorMessage error;
        const Status decoded = Protocol::decode(data, size, error);
        return {decoded == Status::Ok ? error.status : Status::MalformedMessage, false};
    }

    Protocol::RenewReply reply;
    if(Protocol::decode(data, size, reply) != Status::Ok)
    {
        return {Status::MalformedMessage, false};
    }

    if(reply.status != Status::Ok)
    {
        return {reply.status, reply.status != Status::RenewTooFrequent};
    }

    // 3.7: the server MAY change either term at any renewal and the client MUST
    // adopt the new value.
    lease_ttl_ms_ = reply.lease_ttl_ms;
    renew_interval_ms_ = reply.renew_interval_ms;

    return {Status::Ok, false};
}

std::vector<std::byte> SessionClient::make_close_request(std::uint64_t correlation_id) const
{
    Protocol::CloseRequest request;
    request.session_id = session_id_;
    request.reason = Protocol::CloseReason::ClientShutdown;
    return Protocol::encode(request, correlation_id);
}

} // namespace TangoBulk::detail
