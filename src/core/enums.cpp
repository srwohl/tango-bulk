// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/frame.h>
#include <tango-bulk/publisher.h>
#include <tango-bulk/subscriber.h>

namespace TangoBulk
{

std::uint32_t element_size_of(ElementType type) noexcept
{
    switch(type)
    {
    case ElementType::Unknown:
        return 0;
    case ElementType::UInt8:
    case ElementType::Int8:
    case ElementType::Byte:
        return 1;
    case ElementType::UInt16:
    case ElementType::Int16:
        return 2;
    case ElementType::UInt32:
    case ElementType::Int32:
    case ElementType::Float32:
        return 4;
    case ElementType::UInt64:
    case ElementType::Int64:
    case ElementType::Float64:
        return 8;
    }

    return 0;
}

const char *to_string(ElementType type) noexcept
{
    switch(type)
    {
    case ElementType::Unknown:
        return "Unknown";
    case ElementType::UInt8:
        return "UInt8";
    case ElementType::Int8:
        return "Int8";
    case ElementType::UInt16:
        return "UInt16";
    case ElementType::Int16:
        return "Int16";
    case ElementType::UInt32:
        return "UInt32";
    case ElementType::Int32:
        return "Int32";
    case ElementType::UInt64:
        return "UInt64";
    case ElementType::Int64:
        return "Int64";
    case ElementType::Float32:
        return "Float32";
    case ElementType::Float64:
        return "Float64";
    case ElementType::Byte:
        return "Byte";
    }

    return "Unknown";
}

const char *to_string(MemoryKind kind) noexcept
{
    switch(kind)
    {
    case MemoryKind::Host:
        return "Host";
    case MemoryKind::Cuda:
        return "Cuda";
    case MemoryKind::Rocm:
        return "Rocm";
    }

    return "Unknown";
}

const char *to_string(PublishResult result) noexcept
{
    switch(result)
    {
    case PublishResult::Accepted:
        return "Accepted";
    case PublishResult::NoSession:
        return "NoSession";
    case PublishResult::QueueFull:
        return "QueueFull";
    case PublishResult::CreditStalled:
        return "CreditStalled";
    case PublishResult::BadMetadata:
        return "BadMetadata";
    case PublishResult::Shutdown:
        return "Shutdown";
    }

    return "Unknown";
}

const char *to_string(SubscriberState state) noexcept
{
    switch(state)
    {
    case SubscriberState::Closed:
        return "Closed";
    case SubscriberState::Opening:
        return "Opening";
    case SubscriberState::Probing:
        return "Probing";
    case SubscriberState::Active:
        return "Active";
    case SubscriberState::Reconnecting:
        return "Reconnecting";
    case SubscriberState::Failed:
        return "Failed";
    }

    return "Unknown";
}

} // namespace TangoBulk
