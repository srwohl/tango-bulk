// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/unstable/session_supervisor.h>

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
/// `detail::SessionSupervisor` in `core/`, which knows nothing about Tango. What
/// is left here is the part that could never move: turning a coordination
/// message into a `command_inout` on a borrowed `DeviceProxy`, and turning what
/// comes back (or what is thrown) into bytes and a `BulkError`.
///
/// The public interface is unchanged. Two things about it are worth stating,
/// because they are the reason this class still exists rather than being
/// replaced by the supervisor:
///
///   * 2.4 documents that constructing a subscriber that is never started must
///     cost nothing but memory, so a client can build one per stream and start
///     a subset. A `SessionSupervisor` *is* its session and has no unstarted
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

struct BulkSubscriber::Impl
{
    Impl(Tango::DeviceProxy &device_proxy, SubscriberConfig cfg) :
        proxy(device_proxy),
        config(std::move(cfg))
    {
    }

    /// Which command carries which coordination message.
    ///
    /// The whole of what `CommandNames` is for, and the whole of why it never
    /// crosses the seam: below this function the message is a `CoordType`, and
    /// what a particular device happens to call it is not a fact the session
    /// policy could use.
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

    /// The coordination channel, and the only Tango in the data path's lifetime.
    ///
    /// Called on the supervisor's control thread and never concurrently, which
    /// is what lets it scope the caller's proxy timeout without racing anyone
    /// for it.
    ///
    /// 7.4: a `DevFailed` is a recovery hint, never cleanup authority.  It is
    /// translated here rather than passed upward, so that the supervisor's
    /// interface deals in `BulkException` and its implementation never names a
    /// Tango type.
    std::vector<std::byte> command(Protocol::CoordType kind,
                                   const std::vector<std::byte> &request)
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
            const ScopedTimeout guard(proxy, config.command_timeout_ms);
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

    Tango::DeviceProxy &proxy; ///< BORROWED; the caller keeps it alive (7.4)
    SubscriberConfig config;
    CommandNames names;

    FrameCallback frame_callback;
    StateCallback state_callback;

    /// What `state()` and `counters()` answer once the supervisor is gone.
    ///
    /// Destroying the supervisor is how a session stops, so both would otherwise
    /// read zero after `stop()` -- and 2.4 says `state()` reports `Closed` there,
    /// while an operator watching counters should not see them reset by a
    /// shutdown any more than by a reconnect.
    std::atomic<SubscriberState> last_state{SubscriberState::Closed};
    SubscriberCounters retired{};

    /// Null until `start()`, null again after `stop()`.  Also the answer to
    /// "have you started?".
    std::unique_ptr<detail::SessionSupervisor> supervisor;
};

// ---------------------------------------------------------------------------

BulkSubscriber::BulkSubscriber(Tango::DeviceProxy &proxy, SubscriberConfig config)
{
    const Status status = config.validate();
    if(status != Status::Ok)
    {
        throw BulkException(BulkError{
            status, std::string("invalid SubscriberConfig: ") + to_string(status), "subscriber"});
    }

    // Nothing is registered, mapped or connected here.  Construction of a
    // subscriber that is never started must cost nothing but memory, because a
    // device client may build one per stream and start a subset.
    impl_ = std::make_unique<Impl>(proxy, std::move(config));
}

BulkSubscriber::~BulkSubscriber()
{
    stop();
}

void BulkSubscriber::set_frame_callback(FrameCallback cb)
{
    if(impl_->supervisor)
    {
        throw BulkException(BulkError{
            Status::Internal, "set_frame_callback() must be called before start()", "subscriber"});
    }
    impl_->frame_callback = std::move(cb);
}

void BulkSubscriber::set_state_callback(StateCallback cb)
{
    if(impl_->supervisor)
    {
        throw BulkException(BulkError{
            Status::Internal, "set_state_callback() must be called before start()", "subscriber"});
    }
    impl_->state_callback = std::move(cb);
}

void BulkSubscriber::start()
{
    Impl &impl = *impl_;

    if(impl.supervisor)
    {
        throw BulkException(
            BulkError{Status::Internal, "the subscriber is already started", "subscriber"});
    }

    // 2.4: both callbacks MUST be set before start().  The supervisor refuses an
    // empty one too, but refusing here keeps the message about *this* class.
    if(!impl.frame_callback || !impl.state_callback)
    {
        throw BulkException(BulkError{Status::Internal,
                                      "set_frame_callback() and set_state_callback() must both "
                                      "be called before start()",
                                      "subscriber"});
    }

    detail::SessionCallbacks callbacks;
    callbacks.on_frame = impl.frame_callback;

    // Wrapped so that `state()` keeps answering after the supervisor is gone.
    // The supervisor is the authority while it exists; this only records what it
    // last said, for the window afterwards.
    callbacks.on_state = [&impl](SubscriberState next, const BulkError &error) {
        impl.last_state.store(next, std::memory_order_release);
        if(impl.state_callback)
        {
            impl.state_callback(next, error);
        }
    };

    // Throws under FailFast and Manual, which is what 2.4 asks of start().  The
    // wrapped callback has already recorded `Failed` by then, so `state()`
    // reports the diagnosis rather than the absence.
    impl.supervisor = detail::open_session(
        impl.config,
        [&impl](Protocol::CoordType kind, const std::vector<std::byte> &request) {
            return impl.command(kind, request);
        },
        std::move(callbacks));
}

void BulkSubscriber::stop() noexcept
{
    Impl &impl = *impl_;

    if(!impl.supervisor)
    {
        return; // 2.4: idempotent.
    }

    // Read before the destructor runs, because the destructor is what takes the
    // counters away with it.  Nothing arrives between here and there: the
    // session is still open, and stopping it is the next statement.
    impl.retired = impl.supervisor->counters();

    // Closes the session, joins both threads, and reports `Closed` through the
    // wrapped state callback on the way out.
    impl.supervisor.reset();
}

std::size_t BulkSubscriber::poll(std::chrono::milliseconds timeout)
{
    Impl &impl = *impl_;

    if(impl.config.delivery_mode != DeliveryMode::Manual)
    {
        throw BulkException(BulkError{Status::Internal,
                                      "poll() requires DeliveryMode::Manual; a dispatch thread "
                                      "is already delivering frames",
                                      "subscriber"});
    }

    if(!impl.supervisor)
    {
        return 0;
    }

    return impl.supervisor->poll(timeout);
}

SubscriberState BulkSubscriber::state() const noexcept
{
    const Impl &impl = *impl_;

    // The supervisor is the authority while it exists: it stores its state when
    // it changes, rather than when the change is delivered.
    return impl.supervisor ? impl.supervisor->state()
                           : impl.last_state.load(std::memory_order_acquire);
}

std::uint32_t BulkSubscriber::generation() const noexcept
{
    return impl_->supervisor ? impl_->supervisor->generation() : 0;
}

Protocol::GeometryBlock BulkSubscriber::granted_geometry() const noexcept
{
    return impl_->supervisor ? impl_->supervisor->granted_geometry()
                             : Protocol::GeometryBlock{};
}

SubscriberCounters BulkSubscriber::counters() const noexcept
{
    return impl_->supervisor ? impl_->supervisor->counters() : impl_->retired;
}

// ---------------------------------------------------------------------------

void set_command_names(BulkSubscriber &subscriber, const CommandNames &names)
{
    // A friend of BulkSubscriber, declared in <tango-bulk/subscriber.h> against
    // a forward-declared CommandNames.  A forward declaration is enough there
    // and pulls in no Tango header, which is what lets the knob be Tango-only
    // while the class it configures stays in a header the UCX layer compiles.
    BulkSubscriber::Impl &impl = *subscriber.impl_;

    if(impl.supervisor)
    {
        throw BulkException(BulkError{Status::Internal,
                                      "set_command_names() must be called before start(): the "
                                      "session was opened with the previous names",
                                      "tango"});
    }

    impl.names = names;
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
