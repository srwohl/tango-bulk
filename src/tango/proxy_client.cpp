// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include "bulk_commands.h"

#include <core/subscription_internal.h>

#include <core/protocol.h>
#include <tango-bulk/tango.h>

#include <tango/tango.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

///
///
///
namespace TangoBulk
{
namespace
{

/// Scope a Tango timeout and put back what the application had.
///
/// 7.4 requires the adapter not to mutate the caller's proxy configuration.  The
/// destructor restores unconditionally, including when `command_inout` throws,
/// which is the case that would otherwise leave a borrowed proxy with our
/// timeout on it forever.
class ScopedTimeout
{
  public:
    ScopedTimeout(Tango::DeviceProxy &proxy, std::uint32_t timeout_ms) :
        proxy_(proxy),
        previous_(proxy.get_timeout_millis())
    {
        proxy_.set_timeout_millis(static_cast<int>(timeout_ms));
    }

    ~ScopedTimeout()
    {
        try
        {
            proxy_.set_timeout_millis(previous_);
        }
        catch(...)
        {
            // Restoring a timeout on a proxy whose connection has just died is
            // allowed to fail, and there is nothing useful to do about it here.
        }
    }

    ScopedTimeout(const ScopedTimeout &) = delete;
    ScopedTimeout &operator=(const ScopedTimeout &) = delete;

  private:
    Tango::DeviceProxy &proxy_;
    int previous_;
};

std::string describe(const Tango::DevFailed &failure)
{
    if(failure.errors.length() == 0)
    {
        return "DevFailed with no error stack";
    }

    // The first element is the most specific; the rest is a call stack that
    // belongs in the device server's log, not in a state callback.
    return std::string(failure.errors[0].reason.in()) + ": " +
           std::string(failure.errors[0].desc.in());
}

std::string describe(const Tango::DevErrorList &errors)
{
    if(errors.length() == 0)
    {
        return "BulkStreams read failed with no error stack";
    }

    return std::string(errors[0].reason.in()) + ": " + std::string(errors[0].desc.in());
}

std::uint32_t remaining_timeout_ms(std::chrono::steady_clock::time_point deadline,
                                   std::uint32_t fallback) noexcept
{
    if(deadline == std::chrono::steady_clock::time_point::max())
    {
        return fallback;
    }

    const auto remaining = deadline - std::chrono::steady_clock::now();
    if(remaining <= std::chrono::steady_clock::duration::zero())
    {
        return 0;
    }

    auto remaining_ms = std::chrono::duration_cast<std::chrono::milliseconds>(remaining);
    if(remaining_ms < remaining)
    {
        ++remaining_ms;
    }
    return static_cast<std::uint32_t>(std::min<std::uint64_t>(
        fallback, static_cast<std::uint64_t>(std::max<std::int64_t>(1, remaining_ms.count()))));
}

class TangoStreamDiscovery
{
  public:
    TangoStreamDiscovery(Tango::DeviceProxy &proxy, std::uint32_t timeout_ms) :
        proxy_(proxy),
        timeout_ms_(timeout_ms)
    {
    }

    StreamOffer discover(const std::string &stream_name,
                         std::chrono::steady_clock::time_point deadline) const
    {
        StreamOffer unavailable;
        unavailable.stream_name = stream_name;

        const std::uint32_t timeout_ms = remaining_timeout_ms(deadline, timeout_ms_);
        if(timeout_ms == 0)
        {
            unavailable.status = Status::TransportFailure;
            unavailable.message = "BulkStreams discovery deadline expired";
            return unavailable;
        }

        try
        {
            const ScopedTimeout guard(proxy_, timeout_ms);
            Tango::DeviceAttribute attribute = proxy_.read_attribute("BulkStreams");
            attribute.reset_exceptions(Tango::DeviceAttribute::failed_flag);
            attribute.reset_exceptions(Tango::DeviceAttribute::isempty_flag);

            if(attribute.has_failed())
            {
                unavailable.status = Status::TransportFailure;
                unavailable.message = describe(attribute.get_err_stack());
                return unavailable;
            }

            if(attribute.get_type() != Tango::DEV_STRING ||
               attribute.get_data_format() != Tango::SPECTRUM)
            {
                unavailable.status = Status::MalformedMessage;
                unavailable.message = "BulkStreams is not a string spectrum";
                return unavailable;
            }

            std::vector<std::string> rows;
            if(!attribute.extract_read(rows))
            {
                unavailable.status = attribute.is_empty() ? Status::UnknownStream
                                                          : Status::MalformedMessage;
                unavailable.message = attribute.is_empty()
                                          ? "BulkStreams is empty"
                                          : "BulkStreams could not be decoded";
                return unavailable;
            }

            if(rows.empty())
            {
                unavailable.status = Status::UnknownStream;
                unavailable.message = "BulkStreams is empty";
                return unavailable;
            }

            std::optional<StreamOffer> found;
            for(const std::string &row : rows)
            {
                StreamOffer offer = StreamOffer::from_bulk_stream_row(row);
                if(offer.outcome() == StreamOffer::Outcome::Malformed ||
                   offer.outcome() == StreamOffer::Outcome::Unavailable)
                {
                    return offer;
                }

                if(offer.stream_name == stream_name)
                {
                    if(found)
                    {
                        StreamOffer duplicate = offer;
                        duplicate.status = Status::MalformedMessage;
                        duplicate.message = "BulkStreams contains duplicate stream rows";
                        return duplicate;
                    }
                    found = std::move(offer);
                }
            }

            if(found)
            {
                return *found;
            }

            unavailable.status = Status::UnknownStream;
            unavailable.message = "stream was not present in BulkStreams";
            return unavailable;
        }
        catch(const Tango::DevFailed &failure)
        {
            unavailable.status = Status::TransportFailure;
            unavailable.message = describe(failure);
            return unavailable;
        }
    }

  private:
    Tango::DeviceProxy &proxy_;
    std::uint32_t timeout_ms_;
};

Status discovery_failure_status(const StreamOffer &offer) noexcept
{
    switch(offer.outcome())
    {
    case StreamOffer::Outcome::Missing:
        return Status::UnknownStream;
    case StreamOffer::Outcome::Stale:
        return Status::TransportFailure;
    case StreamOffer::Outcome::Unsafe:
        return Status::ResourceExhausted;
    case StreamOffer::Outcome::Malformed:
        return offer.validate() == Status::Ok ? Status::MalformedMessage : offer.validate();
    case StreamOffer::Outcome::Unavailable:
        return offer.status == Status::Ok ? Status::TransportFailure : offer.status;
    case StreamOffer::Outcome::Available:
        return Status::Ok;
    }
    return Status::MalformedMessage;
}

} // namespace

// ---------------------------------------------------------------------------

/// Recognise the three bulk commands by the argument descriptions they were
/// installed with, whatever the device named them. One `command_list_query`
/// within the establishment deadline; a device with no bulk stream reports
/// UnknownStream, which is what a missing publisher looks like from outside.
CommandNames detail::discover_command_names(Tango::DeviceProxy &proxy,
                                            std::chrono::steady_clock::time_point deadline,
                                            std::uint32_t fallback_timeout_ms)
{
    const std::uint32_t timeout_ms = remaining_timeout_ms(deadline, fallback_timeout_ms);
    if(timeout_ms == 0)
    {
        throw EstablishmentError(BulkError{Status::TransportFailure,
                                           "command discovery deadline expired",
                                           Origin::Tango});
    }

    std::vector<detail::CommandDescriptor> descriptors;
    try
    {
        const ScopedTimeout guard(proxy, timeout_ms);
        std::unique_ptr<Tango::CommandInfoList> commands(proxy.command_list_query());
        if(commands)
        {
            descriptors.reserve(commands->size());
            for(const Tango::CommandInfo &info : *commands)
            {
                descriptors.push_back(
                    detail::CommandDescriptor{info.cmd_name,
                                              info.in_type == Tango::DEVVAR_CHARARRAY,
                                              info.out_type == Tango::DEVVAR_CHARARRAY,
                                              info.in_type_desc,
                                              info.out_type_desc});
            }
        }
    }
    catch(const Tango::DevFailed &failure)
    {
        throw EstablishmentError(
            BulkError{Status::TransportFailure, describe(failure), Origin::Tango});
    }

    CommandNames names;
    const Status status = detail::match_bulk_commands(descriptors, names);
    if(status == Status::UnknownStream)
    {
        throw EstablishmentError(BulkError{status,
                                           "the device does not expose the bulk commands",
                                           Origin::Tango});
    }
    if(status != Status::Ok)
    {
        throw EstablishmentError(
            BulkError{status, "the device exposes more than one set of bulk commands", Origin::Tango});
    }
    return names;
}

class CommandChannel
{
  public:
    CommandChannel(Tango::DeviceProxy &device_proxy, CommandNames command_names) :
        proxy(device_proxy),
        names(std::move(command_names))
    {
    }

    std::vector<std::byte> command(Protocol::CoordType kind,
                                   const std::vector<std::byte> &request,
                                   std::chrono::steady_clock::time_point deadline);

  private:
    const std::string &name_for(Protocol::CoordType kind) const
    {
        switch(kind)
        {
        case Protocol::CoordType::Open:
            return names.open;
        case Protocol::CoordType::Renew:
            return names.renew;
        case Protocol::CoordType::Close:
            return names.close;
        default:
            break;
        }

        throw BulkException(BulkError{
            Status::Internal, "no bulk command carries this coordination message", Origin::Tango});
    }

    Tango::DeviceProxy &proxy; ///< BORROWED; the caller keeps it alive (7.4)
    CommandNames names;
};

std::vector<std::byte> CommandChannel::command(Protocol::CoordType kind,
                                               const std::vector<std::byte> &request,
                                               std::chrono::steady_clock::time_point deadline)
{
    const std::string &name = name_for(kind);

    try
    {
        // The Subscription bounds every call by its deadline; the proxy's own
        // timeout is the bound only when it gives none.
        std::optional<ScopedTimeout> guard;
        if(deadline != std::chrono::steady_clock::time_point::max())
        {
            const auto remaining = deadline - std::chrono::steady_clock::now();
            if(remaining <= std::chrono::steady_clock::duration::zero())
            {
                throw BulkException(BulkError{Status::TransportFailure,
                                              "coordination deadline expired",
                                              Origin::Tango});
            }

            auto remaining_ms = std::chrono::duration_cast<std::chrono::milliseconds>(remaining);
            if(remaining_ms < remaining)
            {
                ++remaining_ms;
            }
            guard.emplace(proxy,
                          static_cast<std::uint32_t>(std::min<std::int64_t>(
                              std::max<std::int64_t>(1, remaining_ms.count()),
                              std::numeric_limits<int>::max())));
        }

        std::vector<unsigned char> in(request.size());
        if(!request.empty())
        {
            std::memcpy(in.data(), request.data(), request.size());
        }

        Tango::DeviceData argument;
        argument << in;

        Tango::DeviceData reply = proxy.command_inout(name, argument);

        std::vector<unsigned char> out;
        if(!(reply >> out))
        {
            throw BulkException(BulkError{
                Status::MalformedMessage, name + " did not return a DevVarCharArray", Origin::Tango});
        }

        std::vector<std::byte> bytes(out.size());
        if(!out.empty())
        {
            std::memcpy(bytes.data(), out.data(), out.size());
        }
        return bytes;
    }
    catch(const Tango::DevFailed &failure)
    {
        throw BulkException(BulkError{Status::TransportFailure, describe(failure), Origin::Tango});
    }
}

// ---------------------------------------------------------------------------

StreamOffer discover(Tango::DeviceProxy &proxy,
                     const std::string &stream_name,
                     std::uint32_t timeout_ms)
{
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    TangoStreamDiscovery discovery(proxy, timeout_ms);
    const StreamOffer offer = discovery.discover(stream_name, deadline);
    if(!offer.available())
    {
        const Status status = discovery_failure_status(offer);
        throw EstablishmentError(BulkError{status,
                                           offer.message.empty() ? "BulkStreams discovery failed"
                                                                 : offer.message,
                                           Origin::Tango});
    }
    return offer;
}

// ---------------------------------------------------------------------------

std::unique_ptr<Subscription> subscribe(Tango::DeviceProxy &proxy, SubscriptionOptions options)
{
    if(const Status status = options.validate(); status != Status::Ok)
    {
        throw ConfigurationError(BulkError{status,
                                           std::string("invalid SubscriptionOptions: ") +
                                               to_string(status),
                                           Origin::Tango});
    }

    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(options.establishment_timeout_ms);

    TangoStreamDiscovery discovery(proxy, options.establishment_timeout_ms);
    StreamOffer offer = discovery.discover(options.stream_name, deadline);
    if(!offer.available())
    {
        const Status status = discovery_failure_status(offer);
        throw EstablishmentError(BulkError{status,
                                           offer.message.empty() ? "BulkStreams discovery failed"
                                                                 : offer.message,
                                           Origin::Tango});
    }

    const auto adapter = std::make_shared<CommandChannel>(
        proxy, detail::discover_command_names(proxy, deadline, options.establishment_timeout_ms));
    detail::TransportFactory transport_factory =
        detail::make_subscriber_transport_factory(options.pinned_budget_bytes);

    return detail::SubscriptionFactory::open(
        std::move(options),
        std::move(offer),
        [adapter](Protocol::CoordType kind,
                  const std::vector<std::byte> &request,
                  std::chrono::steady_clock::time_point call_deadline)
        { return adapter->command(kind, request, call_deadline); },
        std::move(transport_factory),
        deadline);
}

} // namespace TangoBulk
