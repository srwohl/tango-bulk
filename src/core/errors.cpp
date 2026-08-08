// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/errors.h>

namespace TangoBulk
{

const char *to_string(Status status) noexcept
{
    switch(status)
    {
    case Status::Ok:
        return "Ok";
    case Status::UnsupportedVersion:
        return "UnsupportedVersion";
    case Status::UnknownStream:
        return "UnknownStream";
    case Status::UnknownSession:
        return "UnknownSession";
    case Status::SessionExpired:
        return "SessionExpired";
    case Status::TooManySessions:
        return "TooManySessions";
    case Status::FrameTooLarge:
        return "FrameTooLarge";
    case Status::DepthTooLarge:
        return "DepthTooLarge";
    case Status::ResourceExhausted:
        return "ResourceExhausted";
    case Status::MalformedMessage:
        return "MalformedMessage";
    case Status::NotAuthorized:
        return "NotAuthorized";
    case Status::TransportFailure:
        return "TransportFailure";
    case Status::GeometryMismatch:
        return "GeometryMismatch";
    case Status::RenewTooFrequent:
        return "RenewTooFrequent";
    case Status::Shutdown:
        return "Shutdown";
    case Status::Internal:
        return "Internal";
    }

    // Reachable only for a value that came off the wire and was not validated.
    // Decoders reject unknown status codes; this exists so a logging path
    // cannot be the thing that crashes.
    return "Unknown";
}

namespace
{

std::string describe(const BulkError &err)
{
    std::string text;
    text.reserve(err.origin.size() + err.message.size() + 24);

    if(!err.origin.empty())
    {
        text += err.origin;
        text += ": ";
    }

    text += to_string(err.status);

    if(!err.message.empty())
    {
        text += ": ";
        text += err.message;
    }

    return text;
}

} // namespace

BulkException::BulkException(BulkError err) :
    std::runtime_error(describe(err)),
    err_(std::move(err))
{
}

const BulkError &BulkException::error() const noexcept
{
    return err_;
}

} // namespace TangoBulk
