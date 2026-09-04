// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/unstable/subscriber_transport.h>

#include <tango-bulk/protocol.h>
#include <tango-bulk/tango.h>

#include <tango/tango.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

/// `BulkSubscriber`: the client half of the Tango adapter.
///
/// This class owns no UCX: the transport is `detail::SubscriberEngine` behind
/// `detail::SubscriberTransport`, one layer down and one library over.  What
/// lives here is everything that needs a `DeviceProxy` -- the `Open`/`Renew`/
/// `Close` round trips, the renew timer, the reconnect policy -- plus the
/// dispatch thread that keeps user callbacks off the engine (5.2).
///
/// Three threads, and which one may touch what is the whole design (5.1):
///
///   * **control** -- the only thread that calls the proxy.  Opens, renews,
///     closes, reconnects.  Never touches UCX and never invokes a user callback.
///   * **dispatch** -- the only thread that invokes `FrameCallback` and
///     `StateCallback`.  Never touches Tango and never touches UCX.
///   * **engine** -- inside the transport, invisible from here.
///
/// The consequence worth stating: a stuck Tango call cannot stall frame
/// delivery, because delivery does not run on the thread that makes it.  It can
/// only, eventually, expire the lease -- which is exactly the failure mode 7.4
/// asks for.
namespace TangoBulk
{
namespace
{

using namespace std::chrono_literals;

/// The longest the control thread sleeps without looking around.
///
/// A renew interval is seconds; a transport failure should be noticed sooner
/// than that, so the wait is chopped into quanta and the renewal is scheduled
/// against a deadline rather than against the sleep.
constexpr auto k_control_quantum = 100ms;

/// How long a dispatch-thread poll blocks before looking at the run flag.
constexpr auto k_dispatch_quantum = 20ms;

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

void accumulate(SubscriberCounters &total, const SubscriberCounters &part) noexcept
{
    total.frames_received += part.frames_received;
    total.frames_delivered += part.frames_delivered;
    total.frames_dropped_queue_full += part.frames_dropped_queue_full;
    total.frames_dropped_stale_epoch += part.frames_dropped_stale_epoch;
    total.frames_dropped_bad_header += part.frames_dropped_bad_header;
    total.frames_dropped_oversize += part.frames_dropped_oversize;
    total.frames_dropped_duplicate_seq += part.frames_dropped_duplicate_seq;
    total.credits_returned += part.credits_returned;
    total.credit_messages_sent += part.credit_messages_sent;
    total.sessions_opened += part.sessions_opened;
    total.renewals_sent += part.renewals_sent;
    total.renewals_failed += part.renewals_failed;
    total.geometry_changes += part.geometry_changes;
    total.transport_errors += part.transport_errors;

    // Gauges, not counters: they describe the transport that is live now, so
    // the newest value wins rather than the sum.
    total.views_outstanding = part.views_outstanding;
    total.delivery_queue_depth = part.delivery_queue_depth;
    total.delivery_queue_high_water =
        std::max(total.delivery_queue_high_water, part.delivery_queue_high_water);
    total.pinned_bytes = part.pinned_bytes;
}

} // namespace

// ---------------------------------------------------------------------------

struct BulkSubscriber::Impl
{
    struct Transition
    {
        SubscriberState state{SubscriberState::Closed};
        BulkError error;
    };

    /// A transport borrowed for the duration of one call.
    ///
    /// The dispatch thread polls the transport while the control thread may be
    /// replacing it, so a `shared_ptr` alone will not do: dropping the last
    /// reference *is* the destruction, and the engine must not be destroyed on
    /// the dispatch thread from inside its own `poll()`.  A borrow keeps the
    /// slot's own reference alive too -- `publish_transport()` waits for every
    /// borrow to be given back before it swaps -- so the destructor always runs
    /// on the control thread.
    class Ref
    {
      public:
        Ref() noexcept = default;

        Ref(Impl &owner, std::shared_ptr<detail::SubscriberTransport> transport) noexcept :
            owner_(&owner),
            transport_(std::move(transport))
        {
        }

        ~Ref()
        {
            reset();
        }

        Ref(Ref &&other) noexcept :
            owner_(other.owner_),
            transport_(std::move(other.transport_))
        {
            other.owner_ = nullptr;
        }

        Ref &operator=(Ref &&other) noexcept
        {
            if(this != &other)
            {
                reset();
                owner_ = other.owner_;
                transport_ = std::move(other.transport_);
                other.owner_ = nullptr;
            }
            return *this;
        }

        Ref(const Ref &) = delete;
        Ref &operator=(const Ref &) = delete;

        void reset() noexcept
        {
            if(owner_ == nullptr)
            {
                return;
            }

            // The reference goes first, the borrow count second: after the
            // count reaches zero no borrower may still hold a pointer, which is
            // precisely what publish_transport() waits on.
            transport_.reset();
            owner_->end_borrow();
            owner_ = nullptr;
        }

        explicit operator bool() const noexcept
        {
            return static_cast<bool>(transport_);
        }

        detail::SubscriberTransport *operator->() const noexcept
        {
            return transport_.get();
        }

      private:
        Impl *owner_{nullptr};
        std::shared_ptr<detail::SubscriberTransport> transport_;
    };

    Impl(Tango::DeviceProxy &device_proxy, SubscriberConfig cfg) :
        proxy(device_proxy),
        config(std::move(cfg))
    {
    }

    // -- coordination, control thread only ----------------------------------

    std::vector<std::byte> command(const std::string &name, const std::vector<std::byte> &request)
    {
        std::vector<unsigned char> in(request.size());
        if(!request.empty())
        {
            std::memcpy(in.data(), request.data(), request.size());
        }

        Tango::DeviceData argument;
        argument << in;

        // 7.4: only command_inout, set_timeout_millis, get_timeout_millis, name
        // and status.  No subscribe_event, no callback registration, no second
        // connection.
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

    /// Open a session, from `make_open_request` through to `Active`.
    ///
    /// Every failure path leaves no transport behind: a half-open subscriber
    /// with a registered ring and no session is exactly the resource leak the
    /// lease exists to prevent on the other side.
    bool open_session(BulkError &error) noexcept
    {
        std::shared_ptr<detail::SubscriberTransport> fresh;

        try
        {
            fresh = detail::make_subscriber_transport(config);
        }
        catch(const BulkException &e)
        {
            error = e.error();
            return false;
        }
        catch(const std::exception &e)
        {
            error = BulkError{Status::Internal, e.what(), "subscriber"};
            return false;
        }

        try
        {
            const std::vector<std::byte> request =
                fresh->make_open_request(next_correlation_id());
            const std::vector<std::byte> reply = command(names.open, request);

            const Status status = fresh->adopt_open_reply(reply.data(), reply.size());
            if(status != Status::Ok)
            {
                error = BulkError{status,
                                  std::string("BulkOpen was refused: ") + to_string(status),
                                  "subscriber"};
                return false;
            }
        }
        catch(const Tango::DevFailed &failure)
        {
            // 7.4: a DevFailed is a recovery hint, never cleanup authority.  It
            // says this client should reconnect; it says nothing about what the
            // publisher should release, which only the lease decides.
            error = BulkError{Status::TransportFailure, describe(failure), "tango"};
            return false;
        }
        catch(const BulkException &e)
        {
            error = e.error();
            return false;
        }

        publish_transport(std::move(fresh));
        transition(SubscriberState::Probing, BulkError{});

        // 4.1: `Probing` until the publisher's `Probe` is answered.  Waiting for
        // it here rather than reporting `Active` optimistically is what makes
        // the state mean "frames can flow", which is the only reading that is
        // useful to an application.
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(config.command_timeout_ms);

        for(;;)
        {
            const Ref live = borrow();
            const SubscriberState observed = live ? live->state() : SubscriberState::Failed;

            if(observed == SubscriberState::Active)
            {
                transition(SubscriberState::Active, BulkError{});
                return true;
            }

            if(observed == SubscriberState::Failed || !running.load(std::memory_order_acquire))
            {
                error = BulkError{Status::TransportFailure,
                                  "the transport failed before the probe was answered",
                                  "subscriber"};
                break;
            }

            if(std::chrono::steady_clock::now() >= deadline)
            {
                error = BulkError{Status::TransportFailure,
                                  "no Probe arrived within command_timeout_ms; the publisher "
                                  "cannot reach this client's UCX endpoint",
                                  "subscriber"};
                break;
            }

            std::this_thread::sleep_for(1ms);
        }

        close_session();
        return false;
    }

    /// Best-effort `Close`, then drop the transport.
    ///
    /// Best-effort because 3.8 makes `Close` idempotent and the lease is the
    /// backstop: a close that cannot be delivered costs the publisher one lease
    /// TTL, not a leaked ring.  Sending it anyway is what turns the common case
    /// -- an orderly client shutdown -- into an immediate release.
    void close_session() noexcept
    {
        {
            const Ref live = borrow();
            if(live)
            {
                try
                {
                    const std::vector<std::byte> request =
                        live->make_close_request(next_correlation_id());
                    command(names.close, request);
                }
                catch(const Tango::DevFailed &)
                {
                    // The device is gone or unreachable.  The lease covers it.
                }
                catch(const std::exception &)
                {
                }
            }
        }

        publish_transport(nullptr);
    }

    /// Renew on schedule until something ends the session.
    ///
    /// Returns true if `stop()` ended it, false if it was lost -- which is the
    /// difference between "we are done" and "reconnect if the policy allows".
    bool run_session(BulkError &error) noexcept
    {
        auto next_renew = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(current_renew_interval());

        while(running.load(std::memory_order_acquire))
        {
            {
                std::unique_lock<std::mutex> lock(mutex);
                wake.wait_for(lock, k_control_quantum, [this] {
                    return !running.load(std::memory_order_acquire);
                });
            }

            if(!running.load(std::memory_order_acquire))
            {
                return true;
            }

            SubscriberState observed = SubscriberState::Failed;
            {
                const Ref live = borrow();
                if(!live)
                {
                    error = BulkError{Status::Internal, "the transport disappeared", "subscriber"};
                    return false;
                }
                observed = live->state();
            }

            if(observed == SubscriberState::Failed)
            {
                error = BulkError{
                    Status::TransportFailure, "the transport reported a failure", "subscriber"};
                return false;
            }

            if(std::chrono::steady_clock::now() < next_renew)
            {
                continue;
            }

            const Status status = renew_once(error);
            if(status == Status::Ok)
            {
                next_renew = std::chrono::steady_clock::now() +
                             std::chrono::milliseconds(current_renew_interval());
                continue;
            }

            if(status == Status::RenewTooFrequent)
            {
                // 3.7: the lease is not shortened as a penalty, so the session is
                // healthy and the only correct response is to renew less often.
                // Backing off by one full interval is the smallest change that
                // cannot loop.
                next_renew = std::chrono::steady_clock::now() +
                             std::chrono::milliseconds(current_renew_interval());
                continue;
            }

            return false;
        }

        return true;
    }

    Status renew_once(BulkError &error) noexcept
    {
        const Ref live = borrow();
        if(!live)
        {
            error = BulkError{Status::Internal, "the transport disappeared", "subscriber"};
            return Status::Internal;
        }

        Status status = Status::Ok;

        try
        {
            const std::vector<std::byte> request = live->make_renew_request(next_correlation_id());
            const std::vector<std::byte> reply = command(names.renew, request);
            status = live->adopt_renew_reply(reply.data(), reply.size());

            if(status != Status::Ok)
            {
                error = BulkError{status,
                                  std::string("BulkRenew was refused: ") + to_string(status),
                                  "subscriber"};
            }
        }
        catch(const Tango::DevFailed &failure)
        {
            error = BulkError{Status::TransportFailure, describe(failure), "tango"};
            status = Status::TransportFailure;
        }
        catch(const BulkException &e)
        {
            error = e.error();
            status = e.error().status;
        }

        return status;
    }

    std::uint32_t current_renew_interval() noexcept
    {
        // 3.7 lets the server change the interval at any renewal and requires
        // the client to adopt the new value, so it is read from the transport
        // rather than from the configuration.
        const Ref live = borrow();
        const std::uint32_t granted = live ? live->renew_interval_ms() : 0;
        return granted == 0 ? 1'000u : granted;
    }

    // -- control thread -----------------------------------------------------

    void control_loop() noexcept
    {
        std::uint32_t attempt = 0;

        for(;;)
        {
            if(!running.load(std::memory_order_acquire))
            {
                break;
            }

            BulkError error;
            if(run_session(error))
            {
                break; // stop() ended it.
            }

            close_session();

            if(!running.load(std::memory_order_acquire))
            {
                break;
            }

            if(!reconnect(attempt, error))
            {
                break;
            }

            attempt = 0;
        }

        // Whatever ended the loop, the session does not outlive it.
        close_session();
    }

    /// Reopen under the configured policy.  Returns false if the subscriber is
    /// finished -- `Failed`, or stopped while backing off.
    bool reconnect(std::uint32_t &attempt, BulkError error) noexcept
    {
        if(config.reconnect_policy != ReconnectPolicy::BoundedRetry)
        {
            // 4.1: FailFast makes any transition to `Reconnecting` a `Failed`,
            // and Manual enters `Failed` and waits for the application to call
            // start() again.  They differ in what the application does next, not
            // in what happens here.
            transition(SubscriberState::Failed, error);
            return false;
        }

        while(running.load(std::memory_order_acquire))
        {
            if(attempt >= config.reconnect_max_attempts)
            {
                transition(SubscriberState::Failed,
                           BulkError{error.status,
                                     "reconnect attempts exhausted: " + error.message,
                                     error.origin});
                return false;
            }

            transition(SubscriberState::Reconnecting, error);
            reconnects.fetch_add(1, std::memory_order_relaxed);

            if(!backoff(attempt))
            {
                return false;
            }

            ++attempt;

            transition(SubscriberState::Opening, BulkError{});
            if(open_session(error))
            {
                return true;
            }
        }

        return false;
    }

    /// Exponential from `reconnect_backoff_ms`, capped at the last known lease
    /// TTL.  Returns false if `stop()` arrived during the wait.
    ///
    /// Capped at the TTL because backing off longer than a lease cannot help: by
    /// then the publisher has reclaimed everything this client held, and the
    /// reopen would be a fresh session either way.
    bool backoff(std::uint32_t attempt) noexcept
    {
        std::uint64_t delay = config.reconnect_backoff_ms;
        for(std::uint32_t i = 0; i < attempt && delay < last_lease_ttl_ms; ++i)
        {
            delay *= 2;
        }
        delay = std::min<std::uint64_t>(delay, std::max<std::uint32_t>(last_lease_ttl_ms, 1));

        std::unique_lock<std::mutex> lock(mutex);
        wake.wait_for(lock,
                      std::chrono::milliseconds(delay),
                      [this] { return !running.load(std::memory_order_acquire); });
        return running.load(std::memory_order_acquire);
    }

    // -- the transport slot -------------------------------------------------

    Ref borrow() noexcept
    {
        std::lock_guard<std::mutex> lock(mutex);

        // `swapping` is what makes this a handshake rather than a race.  Without
        // it the dispatch thread re-borrows the instant it gives a borrow back
        // -- it is a loop whose whole body is one poll -- and the control thread
        // waiting for the count to reach zero has to win a scheduling coin flip
        // against a thread that is already running and already holds the lock a
        // microsecond later.  It loses that flip often enough to hang: observed
        // as a subscriber that stayed `Active` after its stream was taken away,
        // because the control thread never got past `close_session()` to say
        // `Reconnecting`.
        if(!transport || swapping)
        {
            return Ref{};
        }

        ++borrowed;
        return Ref{*this, transport};
    }

    void end_borrow() noexcept
    {
        {
            std::lock_guard<std::mutex> lock(mutex);
            --borrowed;
        }
        wake.notify_all();
    }

    void publish_transport(std::shared_ptr<detail::SubscriberTransport> next) noexcept
    {
        std::shared_ptr<detail::SubscriberTransport> previous;

        {
            std::unique_lock<std::mutex> lock(mutex);

            // Closes the door first, then waits for the room to empty.
            swapping = true;
            wake.wait(lock, [this] { return borrowed == 0; });

            if(transport)
            {
                // Fold the retiring transport's totals in, so a reconnect does
                // not reset the counters an operator is watching.
                accumulate(retired, transport->counters());
                last_lease_ttl_ms = std::max(last_lease_ttl_ms, transport->lease_ttl_ms());
            }

            previous = std::move(transport);
            transport = std::move(next);
            swapping = false;
        }

        wake.notify_all();
        // `previous` dies here, on the control thread, outside the lock.
    }

    // -- delivery -----------------------------------------------------------

    void transition(SubscriberState next, BulkError error) noexcept
    {
        state.store(next, std::memory_order_release);

        {
            std::lock_guard<std::mutex> lock(queue_mutex);
            transitions.push_back(Transition{next, std::move(error)});
        }
        wake.notify_all();
    }

    /// Deliver queued state transitions on the calling thread.
    std::size_t drain_transitions() noexcept
    {
        std::size_t delivered = 0;

        for(;;)
        {
            Transition item;
            {
                std::lock_guard<std::mutex> lock(queue_mutex);
                if(transitions.empty())
                {
                    break;
                }
                item = std::move(transitions.front());
                transitions.pop_front();
            }

            if(state_callback)
            {
                try
                {
                    state_callback(item.state, item.error);
                }
                catch(...)
                {
                    // A throwing user callback must not take the dispatch thread
                    // with it; there is nobody above it to catch anything.
                }
            }
            ++delivered;
        }

        return delivered;
    }

    std::size_t deliver_frames(std::chrono::milliseconds timeout) noexcept
    {
        const Ref live = borrow();
        if(!live)
        {
            std::this_thread::sleep_for(std::min(timeout, k_dispatch_quantum));
            return 0;
        }

        std::size_t delivered = 0;
        try
        {
            delivered = live->poll(timeout, frame_callback);
        }
        catch(...)
        {
            // Same reasoning as above: a user callback that throws is contained
            // here rather than unwinding through the transport.
        }

        return delivered;
    }

    void dispatch_loop() noexcept
    {
        while(running.load(std::memory_order_acquire))
        {
            drain_transitions();
            deliver_frames(k_dispatch_quantum);
        }

        drain_transitions();
    }

    std::uint64_t next_correlation_id() noexcept
    {
        return correlation.fetch_add(1, std::memory_order_relaxed) + 1;
    }

    // -----------------------------------------------------------------------

    Tango::DeviceProxy &proxy; ///< BORROWED; the caller keeps it alive (7.4)
    SubscriberConfig config;
    CommandNames names;

    FrameCallback frame_callback;
    StateCallback state_callback;

    std::atomic<SubscriberState> state{SubscriberState::Closed};
    std::atomic<bool> running{false};
    std::atomic<bool> started{false};

    std::mutex mutex;
    std::condition_variable wake;
    std::shared_ptr<detail::SubscriberTransport> transport;
    std::size_t borrowed{0};
    bool swapping{false}; ///< a swap is waiting for the borrows to come back
    std::uint32_t last_lease_ttl_ms{0};
    SubscriberCounters retired{};

    std::mutex queue_mutex;
    std::deque<Transition> transitions;

    std::thread control;
    std::thread dispatch;

    std::atomic<std::uint64_t> correlation{0};
    std::atomic<std::uint64_t> reconnects{0};
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
    if(impl_->started.load(std::memory_order_acquire))
    {
        throw BulkException(BulkError{
            Status::Internal, "set_frame_callback() must be called before start()", "subscriber"});
    }
    impl_->frame_callback = std::move(cb);
}

void BulkSubscriber::set_state_callback(StateCallback cb)
{
    if(impl_->started.load(std::memory_order_acquire))
    {
        throw BulkException(BulkError{
            Status::Internal, "set_state_callback() must be called before start()", "subscriber"});
    }
    impl_->state_callback = std::move(cb);
}

void BulkSubscriber::start()
{
    Impl &impl = *impl_;

    if(impl.started.exchange(true, std::memory_order_acq_rel))
    {
        throw BulkException(
            BulkError{Status::Internal, "the subscriber is already started", "subscriber"});
    }

    // 2.4: both callbacks MUST be set before start().  Refusing here is the only
    // moment at which the omission is cheap to report -- afterwards it is a
    // stream that silently goes nowhere.
    if(!impl.frame_callback || !impl.state_callback)
    {
        impl.started.store(false, std::memory_order_release);
        throw BulkException(BulkError{Status::Internal,
                                      "set_frame_callback() and set_state_callback() must both "
                                      "be called before start()",
                                      "subscriber"});
    }

    impl.running.store(true, std::memory_order_release);

    if(impl.config.delivery_mode == DeliveryMode::DispatchThread)
    {
        impl.dispatch = std::thread([&impl] { impl.dispatch_loop(); });
    }

    impl.transition(SubscriberState::Opening, BulkError{});

    BulkError error;
    if(impl.open_session(error))
    {
        impl.control = std::thread([&impl] { impl.control_loop(); });
        return;
    }

    // The first open failed.  Under BoundedRetry that is the control thread's
    // problem and start() returns; under the other two policies the caller is
    // right here and gets told, which is what 2.4 means by "throws on open
    // failure under FailFast".
    if(impl.config.reconnect_policy == ReconnectPolicy::BoundedRetry)
    {
        impl.transition(SubscriberState::Reconnecting, error);
        impl.control = std::thread([&impl] { impl.control_loop(); });
        return;
    }

    impl.transition(SubscriberState::Failed, error);
    impl.running.store(false, std::memory_order_release);
    impl.wake.notify_all();

    if(impl.dispatch.joinable())
    {
        impl.dispatch.join();
    }

    impl.started.store(false, std::memory_order_release);
    throw BulkException(error);
}

void BulkSubscriber::stop() noexcept
{
    Impl &impl = *impl_;

    if(!impl.started.load(std::memory_order_acquire))
    {
        return; // 2.4: idempotent.
    }

    impl.running.store(false, std::memory_order_release);
    impl.wake.notify_all();

    if(impl.control.joinable())
    {
        impl.control.join();
    }
    if(impl.dispatch.joinable())
    {
        impl.dispatch.join();
    }

    // The control thread closes the session on its way out; this covers the case
    // where there never was one.
    impl.close_session();

    impl.transition(SubscriberState::Closed, BulkError{});

    // Both delivery threads are joined, so the final transitions are delivered
    // on the caller's thread.  That is a deliberate choice: a state callback
    // that never reports `Closed` is worse than one that reports it from
    // stop(), and the guarantee 5.2 actually makes is about the *engine*
    // thread, which this is not.
    impl.drain_transitions();

    impl.started.store(false, std::memory_order_release);
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

    impl.drain_transitions();
    const std::size_t frames = impl.deliver_frames(timeout);
    impl.drain_transitions();
    return frames;
}

SubscriberState BulkSubscriber::state() const noexcept
{
    return impl_->state.load(std::memory_order_acquire);
}

std::uint32_t BulkSubscriber::generation() const noexcept
{
    const Impl::Ref live = impl_->borrow();
    return live ? live->generation() : 0;
}

SubscriberCounters BulkSubscriber::counters() const noexcept
{
    SubscriberCounters total = impl_->retired;

    {
        const Impl::Ref live = impl_->borrow();
        if(live)
        {
            accumulate(total, live->counters());
        }
    }

    total.reconnects = impl_->reconnects.load(std::memory_order_relaxed);
    return total;
}

// ---------------------------------------------------------------------------

void set_command_names(BulkSubscriber &subscriber, const CommandNames &names)
{
    // A friend of BulkSubscriber, declared in <tango-bulk/subscriber.h> against
    // a forward-declared CommandNames.  A forward declaration is enough there
    // and pulls in no Tango header, which is what lets the knob be Tango-only
    // while the class it configures stays in a header the UCX layer compiles.
    BulkSubscriber::Impl &impl = *subscriber.impl_;

    if(impl.started.load(std::memory_order_acquire))
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
