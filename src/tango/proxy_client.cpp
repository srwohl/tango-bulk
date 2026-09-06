// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/subscription.h>

#include <tango-bulk/protocol.h>
#include <tango-bulk/tango.h>

#include <tango/tango.h>

#include <atomic>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

/// `BulkSubscriber`: the Tango adapter, and nothing else.
///
/// Everything that was session policy -- the control loop, the renew timer, the
/// reconnect policy, the transport slot, the three threads -- is now
/// `detail::Subscription` in `core/`, which knows nothing about Tango. What
/// is left here is the part that could never move: turning a coordination
/// message into a `command_inout` on a borrowed `DeviceProxy`, and turning what
/// comes back (or what is thrown) into bytes and a `BulkError`.
///
/// The public interface is unchanged. Two things about it are worth stating,
/// because they are the reason this class still exists rather than being
/// replaced by the subscription:
///
///   * 2.4 documents that constructing a subscriber that is never started must
///     cost nothing but memory, so a client can build one per stream and start
///     a subset. A `Subscription` *is* its session and has no unstarted
///     form -- so the two-phase shape lives here, as a `unique_ptr` that is null
///     until `start()`.
///   * That pointer is also the answer to "have you started?", which
///     `set_command_names()` needs. There is no second flag to disagree with it.
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

} // namespace

// ---------------------------------------------------------------------------

/// The coordination adapter: Tango commands carrying encoded messages.
///
/// All that is left of what used to be a class wrapped around a Subscription.
/// It holds no session state, no callbacks and no lifecycle -- only which
/// command carries which message, and the proxy to call it on.
class CommandChannel
{
  public:
    CommandChannel(Tango::DeviceProxy &device_proxy,
                   CommandNames command_names,
                   std::uint32_t timeout_ms) :
        proxy(device_proxy),
        names(std::move(command_names)),
        command_timeout_ms(timeout_ms)
    {
    }

    /// The coordination channel, and the only Tango in the data path's lifetime.
    ///
    /// Called on the subscription's control thread and never concurrently, which
    /// lets it scope the caller's proxy timeout without racing anyone for it.
    ///
    /// 7.4: a `DevFailed` is a recovery hint, never cleanup authority, and it is
    /// translated here so the subscription never names a Tango type.
    std::vector<std::byte> command(Protocol::CoordType kind,
                                   const std::vector<std::byte> &request);

  private:
    /// Which command carries which coordination message. Below this function
    /// the message is a `CoordType`, and what a device calls it is not a fact
    /// the session policy could use.
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
        case Protocol::CoordType::Query:
            return names.query;
        default:
            break;
        }

        throw BulkException(BulkError{
            Status::Internal, "no bulk command carries this coordination message", "tango"});
    }

    Tango::DeviceProxy &proxy; ///< BORROWED; the caller keeps it alive (7.4)
    CommandNames names;
    std::uint32_t command_timeout_ms;
};

std::vector<std::byte> CommandChannel::command(Protocol::CoordType kind,
                                               const std::vector<std::byte> &request)
{
    {
        const std::string &name = name_for(kind);

        try
        {
            std::vector<unsigned char> in(request.size());
            if(!request.empty())
            {
                std::memcpy(in.data(), request.data(), request.size());
            }

            Tango::DeviceData argument;
            argument << in;

            // 7.4: only command_inout, set_timeout_millis, get_timeout_millis,
            // name and status.  No subscribe_event, no callback registration, no
            // second connection.
            const ScopedTimeout guard(proxy, command_timeout_ms);
            Tango::DeviceData reply = proxy.command_inout(name, argument);

            std::vector<unsigned char> out;
            if(!(reply >> out))
            {
                throw BulkException(BulkError{
                    Status::MalformedMessage, name + " did not return a DevVarCharArray", "tango"});
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
            throw BulkException(BulkError{Status::TransportFailure, describe(failure), "tango"});
        }
    }
}

// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------

std::unique_ptr<Subscription> subscribe(Tango::DeviceProxy &proxy,
                                        SubscriberConfig config,
                                        SubscriptionCallbacks callbacks,
                                        const CommandNames &names)
{
    // The adapter owns its own state and outlives the call, because the control
    // thread keeps calling it. It holds the proxy by reference, which is why the
    // caller must keep the proxy alive.
    const auto adapter = std::make_shared<CommandChannel>(proxy, names, config.command_timeout_ms);

    return open_subscription(
        std::move(config),
        [adapter](Protocol::CoordType kind, const std::vector<std::byte> &request)
        { return adapter->command(kind, request); },
        std::move(callbacks));
}


BulkQueryResult bulk_query(Tango::DeviceProxy &proxy, const CommandNames &names)
{
    Protocol::QueryRequest request; ///< all-zero session_id: server-wide status
    const std::vector<std::byte> encoded = Protocol::encode(request, 1);

    std::vector<unsigned char> in(encoded.size());
    std::memcpy(in.data(), encoded.data(), encoded.size());

    Tango::DeviceData argument;
    argument << in;

    Tango::DeviceData reply = proxy.command_inout(names.query, argument);

    std::vector<unsigned char> out;
    if(!(reply >> out))
    {
        throw BulkException(BulkError{
            Status::MalformedMessage, names.query + " did not return a DevVarCharArray", "tango"});
    }

    const auto *data = reinterpret_cast<const std::byte *>(out.data());

    Protocol::Envelope envelope;
    if(Protocol::decode_envelope(data, out.size(), envelope) != Status::Ok)
    {
        throw BulkException(
            BulkError{Status::MalformedMessage, "undecodable BulkQuery reply", "tango"});
    }

    // A client must accept Error in place of any expected reply (3.3).
    if(envelope.msg_type == Protocol::CoordType::Error)
    {
        Protocol::ErrorMessage error;
        if(Protocol::decode(data, out.size(), error) != Status::Ok)
        {
            throw BulkException(
                BulkError{Status::MalformedMessage, "undecodable BulkQuery error", "tango"});
        }

        BulkQueryResult result;
        result.status = error.status;
        result.counters = error.message;
        return result;
    }

    Protocol::QueryReply decoded;
    if(Protocol::decode(data, out.size(), decoded) != Status::Ok)
    {
        throw BulkException(
            BulkError{Status::MalformedMessage, "undecodable BulkQuery reply", "tango"});
    }

    BulkQueryResult result;
    result.status = decoded.status;
    result.active_sessions = decoded.active_sessions;
    result.generation = decoded.generation;
    result.max_frame_bytes = decoded.geometry.max_frame_bytes;
    result.ring_depth = decoded.geometry.ring_depth;
    result.credit_window = decoded.geometry.credit_window;
    result.element_type = decoded.geometry.element_type;
    result.element_size = decoded.geometry.element_size;
    result.rank = decoded.geometry.rank;
    result.shape = decoded.geometry.shape;
    result.strides = decoded.geometry.strides;
    result.counters = std::move(decoded.counters);
    return result;
}

} // namespace TangoBulk
