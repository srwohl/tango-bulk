// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/subscription.h>

#include <core/delivery_queue.h>
#include <core/session_client.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>
#include <utility>

/// `Subscription`: everything `BulkSubscriber` used to do that was not
/// Tango.
///
/// This code came out of `src/tango/proxy_client.cpp` with two substitutions and
/// no change of behaviour: a `Tango::DeviceProxy &` became a
/// `CoordinationChannel`, and a direct call to `make_subscriber_transport()`
/// became a `TransportFactory`. Everything else -- the control loop, the renew
/// schedule, the reconnect policy, the transport slot, the transition queue --
/// is what it was, and the existing Tango adapter tests are what says so.
///
/// Three threads, and which one may touch what is the whole design (5.1):
///
///   * **control** -- the only thread that calls the coordination channel.
///     Opens, renews, closes, reconnects. Never touches the transport's data
///     path and never invokes a user callback.
///   * **dispatch** -- the only thread that invokes `FrameCallback` and
///     `StateCallback`. Never calls the channel.
///   * **engine** -- inside the transport, invisible from here.
///
/// The two consequences: a stuck coordination call cannot stall delivery, and a
/// stuck application cannot stall coordination. Neither runs on the other's
/// thread, and delivery goes through a queue this object owns rather than
/// through the transport, so replacing a transport waits for nobody.
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

/// Fold a retiring transport's per-session totals into the subscription's.
///
/// Delivery and renewal counts are absent: they belong to the subscription,
/// which spans every session, so `counters()` reads them once from their owner.
/// Summing a transport's copy would count the same frame per session.
void accumulate(SubscriberCounters &total, const SubscriberCounters &part) noexcept
{
    total.frames_received += part.frames_received;
    total.frames_dropped_stale_epoch += part.frames_dropped_stale_epoch;
    total.frames_dropped_bad_header += part.frames_dropped_bad_header;
    total.frames_dropped_oversize += part.frames_dropped_oversize;
    total.frames_dropped_duplicate_seq += part.frames_dropped_duplicate_seq;
    total.frames_dropped_geometry_mismatch += part.frames_dropped_geometry_mismatch;
    total.credits_returned += part.credits_returned;
    total.credit_messages_sent += part.credit_messages_sent;
    total.sessions_opened += part.sessions_opened;
    total.geometry_changes += part.geometry_changes;
    total.transport_errors += part.transport_errors;

    // Gauges, not counters: they describe the transport that is live now, so
    // the newest value wins rather than the sum.
    total.views_outstanding = part.views_outstanding;
    total.pinned_bytes = part.pinned_bytes;
}

} // namespace

// ---------------------------------------------------------------------------

std::chrono::milliseconds backoff_delay(std::uint32_t attempt,
                                        std::uint32_t backoff_ms,
                                        std::uint32_t lease_ttl_ms) noexcept
{
    std::uint64_t delay = backoff_ms;
    for(std::uint32_t i = 0; i < attempt && delay < lease_ttl_ms; ++i)
    {
        delay *= 2;
    }

    delay = std::min<std::uint64_t>(delay, std::max<std::uint32_t>(lease_ttl_ms, 1));
    return std::chrono::milliseconds(delay);
}

// ---------------------------------------------------------------------------

struct Subscription::Impl
{
    struct Transition
    {
        SubscriberState state{SubscriberState::Closed};
        BulkError error;
    };

    Impl(SubscriberConfig cfg,
         detail::CoordinationChannel coordination,
         detail::TransportFactory transport_factory,
         SubscriptionCallbacks cbs) :
        config(std::move(cfg)),
        channel(std::move(coordination)),
        factory(std::move(transport_factory)),
        frame_callback(std::move(cbs.on_frame)),
        state_callback(std::move(cbs.on_state)),
        delivery(std::make_shared<detail::DeliveryQueue>(config.delivery_queue_depth,
                                                 config.drop_policy))
    {
    }

    // -- coordination, control thread only ----------------------------------

    /// Open a session, from `make_open_request` through to `Active`.
    ///
    /// Every failure path leaves no transport behind: a half-open subscriber
    /// with a registered ring and no session is exactly the resource leak the
    /// lease exists to prevent on the other side.
    bool open_subscription(BulkError &error) noexcept
    {
        std::unique_ptr<detail::SubscriberTransport> fresh;

        try
        {
            fresh = factory(config, delivery);
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

        if(!fresh)
        {
            error = BulkError{
                Status::Internal, "the transport factory returned nothing", "subscriber"};
            return false;
        }

        // A candidate, not the live session: a grant about to be refused must
        // not become the answer granted_geometry() gives, and must not start a
        // transport. The check below sits between the two.
        detail::SessionClient candidate;

        try
        {
            const std::vector<std::byte> request = candidate.make_open_request(
                config, fresh->local_address(), next_correlation_id());
            const std::vector<std::byte> reply =
                channel(Protocol::CoordType::Open, request);

            const Status status =
                candidate.adopt_open_reply(reply.data(), reply.size(), config);
            if(status != Status::Ok)
            {
                error = BulkError{status,
                                  std::string("BulkOpen was refused: ") + to_string(status),
                                  "subscriber"};
                return false;
            }
        }
        catch(const BulkException &e)
        {
            // 7.4: a channel failure is a recovery hint, never cleanup
            // authority.  It says this client should reconnect; it says nothing
            // about what the publisher should release, which only the lease
            // decides.
            error = e.error();
            return false;
        }
        catch(const std::exception &e)
        {
            error = BulkError{Status::TransportFailure, e.what(), "subscriber"};
            return false;
        }

        // A reopened session may be granted a different array than the one the
        // application laid its buffers out for. Adopting it silently is how a
        // client ends up interpreting 1024x1024 frames as 2048x2048; the epoch
        // and the sizing terms are allowed to move, the description is not.
        if(retired_geometry.generation != 0 &&
           !describes_same_array(retired_geometry, candidate.granted_geometry()))
        {
            geometry_changes.fetch_add(1, std::memory_order_relaxed);
            error = BulkError{Status::GeometryMismatch,
                              "the reopened session describes a different array than the one "
                              "that was retired; the application must open a new subscription "
                              "with the new contract in hand",
                              "subscriber"};

            // An unactivated transport has no endpoint and no progress thread,
            // so it cannot have queued anything. Refusing costs a destructor.
            fresh.reset();
            return false;
        }

        {
            // The grant is settled. Under the lock because granted_geometry()
            // reads it from an application thread.
            std::lock_guard<std::mutex> lock(observation);
            session = candidate;
        }

        // Renew reports this session's progress, not the subscription's.
        delivered_at_open = delivery->stats().taken;

        if(const Status status = fresh->activate(session.stream_id(),
                                                 session.granted_geometry(),
                                                 session.server_address());
           status != Status::Ok)
        {
            const BulkError reported = fresh->last_error();
            error = reported.status != Status::Ok
                        ? reported
                        : BulkError{status, "the transport could not adopt the grant",
                                    "subscriber"};
            fresh.reset();
            return false;
        }

        publish_transport(std::move(fresh));
        transition(SubscriberState::Probing, BulkError{});

        // 4.1: `Probing` until the publisher's `Probe` is answered.  Waiting for
        // it here rather than reporting `Active` optimistically is what makes
        // the state mean "frames can flow", which is the only reading that is
        // useful to an application.
        //
        // Budgeted by probe_timeout_ms, not by the channel's own timeout: this
        // waits for a UCX round trip the publisher initiates, which is a
        // different question from how long one coordination call may take.
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(config.probe_timeout_ms);

        for(;;)
        {
            const SubscriberState observed =
                transport ? transport->state() : SubscriberState::Failed;

            if(observed == SubscriberState::Active)
            {
                transition(SubscriberState::Active, BulkError{});
                return true;
            }

            if(observed == SubscriberState::Failed || !running.load(std::memory_order_acquire))
            {
                error = transport ? transport->last_error() : BulkError{};
                if(error.status == Status::Ok)
                {
                    error = BulkError{Status::TransportFailure,
                                      "the transport failed before the probe was answered",
                                      "subscriber"};
                }
                break;
            }

            if(std::chrono::steady_clock::now() >= deadline)
            {
                error = BulkError{Status::TransportFailure,
                                  "no Probe arrived within probe_timeout_ms; the publisher "
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
            if(transport)
            {
                try
                {
                    const std::vector<std::byte> request =
                        session.make_close_request(next_correlation_id());
                    channel(Protocol::CoordType::Close, request);
                }
                catch(const std::exception &)
                {
                    // The device is gone or unreachable.  The lease covers it.
                }
            }
        }

        publish_transport(nullptr);
    }

    /// Renew on schedule until something ends the session.
    ///
    /// Returns true if teardown ended it, false if it was lost -- which is the
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

            if(!transport)
            {
                error = BulkError{Status::Internal, "the transport disappeared", "subscriber"};
                return false;
            }

            const SubscriberState observed = transport->state();
            const BulkError reported =
                observed == SubscriberState::Failed ? transport->last_error() : BulkError{};

            publish_observation();

            if(observed == SubscriberState::Failed)
            {
                // Say what happened rather than that something did. Which
                // condition retired the session decides whether reconnecting
                // can help, and that is the application's to know.
                error = reported.status != Status::Ok
                            ? reported
                            : BulkError{Status::TransportFailure,
                                        "the transport reported a failure",
                                        "subscriber"};
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
        std::uint64_t credits_returned = 0;
        std::uint32_t client_state = static_cast<std::uint32_t>(SubscriberState::Failed);

        if(!transport)
        {
            error = BulkError{Status::Internal, "the transport disappeared", "subscriber"};
            return Status::Internal;
        }

        // The only thing the renewal needs from the transport.
        {
            const SubscriberCounters sampled = transport->counters();
            credits_returned = sampled.credits_returned;
            client_state = static_cast<std::uint32_t>(transport->state());
        }

        Status status = Status::Ok;

        try
        {
            const std::vector<std::byte> request =
                session.make_renew_request(next_correlation_id(),
                                           delivery->stats().taken - delivered_at_open,
                                           credits_returned,
                                           client_state);

            renewals_sent.fetch_add(1, std::memory_order_relaxed);
            const std::vector<std::byte> reply =
                channel(Protocol::CoordType::Renew, request);
            status = session.adopt_renew_reply(reply.data(), reply.size()).status;

            if(status != Status::Ok)
            {
                error = BulkError{status,
                                  std::string("BulkRenew was refused: ") + to_string(status),
                                  "subscriber"};
            }
        }
        catch(const BulkException &e)
        {
            error = e.error();
            status = e.error().status;
        }
        catch(const std::exception &e)
        {
            error = BulkError{Status::TransportFailure, e.what(), "subscriber"};
            status = Status::TransportFailure;
        }

        // Anything that is not a granted renewal, a channel that threw included.
        if(status != Status::Ok)
        {
            renewals_failed.fetch_add(1, std::memory_order_relaxed);
        }

        return status;
    }

    std::uint32_t current_renew_interval() noexcept
    {
        // 3.7 lets the server change the interval at any renewal and requires
        // the client to adopt the new value, so it is read from the adopted
        // lease rather than from the configuration. Control thread, which is
        // also the only writer -- no borrow, and nothing to borrow it from.
        const std::uint32_t granted = session.renew_interval_ms();
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
                break; // teardown ended it.
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

    /// Reopen under the configured policy.  Returns false if the subscription is
    /// finished -- `Failed`, or torn down while backing off.
    bool reconnect(std::uint32_t &attempt, BulkError error) noexcept
    {
        if(config.reconnect_policy != ReconnectPolicy::BoundedRetry)
        {
            // 4.1: FailFast makes any transition to `Reconnecting` a `Failed`,
            // and Manual enters `Failed` and waits for the application to open a
            // new subscription.  They differ in what the application does next,
            // not in what happens here.
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
            if(open_subscription(error))
            {
                return true;
            }

            if(error.status == Status::GeometryMismatch)
            {
                // The reopened session describes a different array. Retrying
                // cannot help -- neither side changes between attempts, so the
                // next grant is the same different array -- and BoundedRetry
                // would spend every remaining attempt and a backoff apiece
                // discovering that.
                //
                // This is also where a frame-level contradiction ends up. That
                // retires its session and reconnects once, which is right: the
                // publisher may have genuinely reopened with a new contract,
                // and reopening is how the client finds out what it now is.
                // Then this stops, and says so in the more accurate of the two
                // messages.
                transition(SubscriberState::Failed, error);
                return false;
            }
        }

        return false;
    }

    /// Wait out `backoff_delay()`.  Returns false if teardown arrived during it.
    bool backoff(std::uint32_t attempt) noexcept
    {
        const auto delay =
            backoff_delay(attempt, config.reconnect_backoff_ms, last_lease_ttl_ms);

        std::unique_lock<std::mutex> lock(mutex);
        wake.wait_for(lock, delay, [this] { return !running.load(std::memory_order_acquire); });
        return running.load(std::memory_order_acquire);
    }

    // -- the transport slot -------------------------------------------------

    /// Retire the current transport and install `next`. Control thread, like
    /// every other use of `transport`.
    void publish_transport(std::unique_ptr<detail::SubscriberTransport> next) noexcept
    {
        if(transport)
        {
            // Fold in the retiring totals so a reconnect does not reset what an
            // operator is watching, and keep the geometry so the next grant can
            // be compared against it.
            std::lock_guard<std::mutex> lock(observation);

            accumulate(retired, transport->counters());
            live = SubscriberCounters{};
            last_lease_ttl_ms = std::max(last_lease_ttl_ms, session.lease_ttl_ms());

            if(session.granted_geometry().generation != 0)
            {
                retired_geometry = session.granted_geometry();
            }

            session = detail::SessionClient{};
        }

        const bool retired_one = transport != nullptr;

        // Destroyed before the replacement goes in: that is what stops the old
        // producer pushing.
        transport = nullptr;

        if(retired_one)
        {
            // Its queued frames were granted under a contract that has ended.
            delivery->discard();
        }

        transport = std::move(next);
        publish_observation();
    }

    /// Copy the transport's counters for readers on other threads. Control
    /// thread. They lag by up to one control quantum (ADR 0004); reading them
    /// live would mean keeping the transport alive to read.
    void publish_observation() noexcept
    {
        SubscriberCounters sampled;
        if(transport)
        {
            sampled = transport->counters();
        }

        std::lock_guard<std::mutex> lock(observation);
        live = sampled;
    }

    // -- delivery -----------------------------------------------------------

    /// The longest run of undelivered state changes kept.
    ///
    /// Bounded because in `Manual` delivery nothing drains this until the
    /// application polls, and a reconnecting subscriber produces two entries per
    /// attempt. The oldest goes: the newest says where the subscription is.
    static constexpr std::size_t k_max_transitions = 64;

    void transition(SubscriberState next, BulkError error) noexcept
    {
        state.store(next, std::memory_order_release);

        {
            std::lock_guard<std::mutex> lock(queue_mutex);
            if(transitions.size() >= k_max_transitions)
            {
                transitions.pop_front();
                transitions_dropped.fetch_add(1, std::memory_order_relaxed);
            }
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

    /// Hand queued frames to the application, on the CALLING thread.
    ///
    /// Touches no transport, so a slow callback delays nothing but its own
    /// caller.
    std::size_t deliver_frames(std::chrono::milliseconds timeout,
                               std::size_t max_frames = 0) noexcept
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        std::size_t delivered = 0;

        while(max_frames == 0 || delivered < max_frames)
        {
            FrameView view;

            // Only the first frame waits; the rest of a burst is taken without
            // blocking again.
            const bool got =
                delivered == 0 ? delivery->take(view, deadline) : delivery->try_take(view);

            if(!got)
            {
                break;
            }

            ++delivered;

            try
            {
                frame_callback(std::move(view));
            }
            catch(...)
            {
                // A throwing callback must not take the dispatch thread with it,
                // and must not cost the rest of the burst.
            }

            view.reset();
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

    /// Stop everything.  Idempotent, and safe to call on a subscription whose
    /// first open never succeeded.
    ///
    /// `announce_closed` is false on exactly one path: a first open that failed
    /// under a policy that reports rather than retries.  There the caller is
    /// about to receive an exception and the last state it should observe is
    /// `Failed`; following that with `Closed` would overwrite the diagnosis
    /// with the fact that the failed thing is also no longer running, which the
    /// caller can see for itself from the throw.
    void shutdown(bool announce_closed) noexcept
    {
        running.store(false, std::memory_order_release);
        wake.notify_all();

        // Sticky: a consumer blocked in take() comes back and stays back.
        delivery->stop();

        if(control.joinable())
        {
            control.join();
        }
        if(dispatch.joinable())
        {
            dispatch.join();
        }

        // The control thread closes the session on its way out; this covers the
        // case where there never was one.
        close_session();

        if(announce_closed)
        {
            transition(SubscriberState::Closed, BulkError{});
        }

        // Both delivery threads are joined, so the final transitions are
        // delivered on the caller's thread.  That is a deliberate choice: a
        // state callback that never reports `Closed` is worse than one that
        // reports it from teardown, and the guarantee 5.2 actually makes is
        // about the *engine* thread, which this is not.
        drain_transitions();
    }

    // -----------------------------------------------------------------------

    SubscriberConfig config;
    detail::CoordinationChannel channel;
    detail::TransportFactory factory;

    FrameCallback frame_callback;
    StateCallback state_callback;

    /// Who this session is, what it was granted, what its lease says. Written
    /// by the control thread under `observation`, read by `granted_geometry()`
    /// from an application thread.
    detail::SessionClient session;

    /// Frames the queue had handed over when the live session was granted, so
    /// `Renew` can report this session rather than the subscription. Control
    /// thread only.
    std::uint64_t delivered_at_open{0};

    /// Where frames go, for the life of the subscription. Outliving every
    /// transport is what makes `fd()` stable across a reconnect. `shared_ptr`
    /// because a transport that could not be quiesced is quarantined rather
    /// than destroyed, and may still hold it.
    std::shared_ptr<detail::DeliveryQueue> delivery;

    std::atomic<SubscriberState> state{SubscriberState::Closed};
    std::atomic<bool> running{false};

    /// The control thread's timed waits, and nothing else.
    std::mutex mutex;
    std::condition_variable wake;

    /// The live transport. **Control thread only** -- created, replaced and
    /// destroyed there, and read nowhere else. No lock, because there is no
    /// second thread to lock against.
    std::unique_ptr<detail::SubscriberTransport> transport;

    /// Guards everything an application thread may read while the control
    /// thread runs: the session contract above, and these.
    std::mutex observation;
    SubscriberCounters retired{};
    SubscriberCounters live{};
    std::uint32_t last_lease_ttl_ms{0};

    /// What the last session was describing. All-zero until one is retired,
    /// which is why the comparison is skipped on a first open.
    Protocol::GeometryBlock retired_geometry{};

    std::mutex queue_mutex;
    std::deque<Transition> transitions;
    std::atomic<std::uint64_t> transitions_dropped{0};

    std::thread control;
    std::thread dispatch;

    std::atomic<std::uint64_t> correlation{0};
    std::atomic<std::uint64_t> reconnects{0};
    std::atomic<std::uint64_t> geometry_changes{0};
    std::atomic<std::uint64_t> renewals_sent{0};
    std::atomic<std::uint64_t> renewals_failed{0};
};

// ---------------------------------------------------------------------------

Subscription::Subscription(std::unique_ptr<Impl> impl) noexcept :
    impl_(std::move(impl))
{
}

Subscription::~Subscription()
{
    impl_->shutdown(true);
}

std::unique_ptr<Subscription> Subscription::open(SubscriberConfig config,
                                                  detail::CoordinationChannel channel,
                                                  detail::TransportFactory factory,
                                                           SubscriptionCallbacks callbacks)
{
    const Status status = config.validate();
    if(status != Status::Ok)
    {
        throw BulkException(BulkError{
            status, std::string("invalid SubscriberConfig: ") + to_string(status), "subscriber"});
    }

    if(!channel || !factory)
    {
        throw BulkException(BulkError{Status::Internal,
                                      "a Subscription needs both a coordination channel "
                                      "and a transport factory",
                                      "subscriber"});
    }

    // 2.4 required both callbacks before start(); making them constructor
    // arguments moves that from a runtime check to a thing the caller cannot
    // omit.  The emptiness check remains, because a default-constructed
    // std::function satisfies the type and delivers nowhere.
    if(!callbacks.on_frame || !callbacks.on_state)
    {
        throw BulkException(BulkError{Status::Internal,
                                      "SubscriptionCallbacks needs both on_frame and on_state",
                                      "subscriber"});
    }

    auto impl = std::make_unique<Impl>(
        std::move(config), std::move(channel), std::move(factory), std::move(callbacks));

    impl->running.store(true, std::memory_order_release);

    if(impl->config.delivery_mode == DeliveryMode::DispatchThread)
    {
        Impl &ref = *impl;
        impl->dispatch = std::thread([&ref] { ref.dispatch_loop(); });
    }

    impl->transition(SubscriberState::Opening, BulkError{});

    BulkError error;
    if(impl->open_subscription(error))
    {
        Impl &ref = *impl;
        impl->control = std::thread([&ref] { ref.control_loop(); });
        return std::unique_ptr<Subscription>(new Subscription(std::move(impl)));
    }

    // The first open failed.  Under BoundedRetry that is the control thread's
    // problem and open() returns; under the other two policies the caller is
    // right here and gets told, which is what 2.4 means by "throws on open
    // failure under FailFast".
    if(impl->config.reconnect_policy == ReconnectPolicy::BoundedRetry)
    {
        impl->transition(SubscriberState::Reconnecting, error);
        Impl &ref = *impl;
        impl->control = std::thread([&ref] { ref.control_loop(); });
        return std::unique_ptr<Subscription>(new Subscription(std::move(impl)));
    }

    impl->transition(SubscriberState::Failed, error);
    impl->shutdown(false);
    throw BulkException(error);
}

std::size_t Subscription::poll(std::chrono::milliseconds timeout, std::size_t max_frames)
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
    const std::size_t frames = impl.deliver_frames(timeout, max_frames);
    impl.drain_transitions();
    return frames;
}

SubscriberState Subscription::state() const noexcept
{
    return impl_->state.load(std::memory_order_acquire);
}

int Subscription::fd() const noexcept
{
    return impl_->delivery->fd();
}

Protocol::GeometryBlock Subscription::granted_geometry() const noexcept
{
    const std::lock_guard<std::mutex> lock(impl_->observation);
    return impl_->session.granted_geometry();
}

std::uint32_t Subscription::generation() const noexcept
{
    const std::lock_guard<std::mutex> lock(impl_->observation);
    return impl_->session.generation();
}

SubscriberCounters Subscription::counters() const noexcept
{
    SubscriberCounters total;

    {
        // A published copy, never a live transport: these lag by up to one
        // control quantum (ADR 0004). The ones below are exact.
        const std::lock_guard<std::mutex> lock(impl_->observation);
        total = impl_->retired;
        accumulate(total, impl_->live);
    }

    total.reconnects = impl_->reconnects.load(std::memory_order_relaxed);
    total.geometry_changes += impl_->geometry_changes.load(std::memory_order_relaxed);

    // Read once from their owner: they span every session, so there is nothing
    // to fold in and nothing that resets on a reconnect.
    total.renewals_sent = impl_->renewals_sent.load(std::memory_order_relaxed);
    total.renewals_failed = impl_->renewals_failed.load(std::memory_order_relaxed);

    const detail::DeliveryQueue::Stats queue = impl_->delivery->stats();
    total.frames_delivered = queue.taken;
    total.frames_dropped_queue_full = queue.dropped;
    total.delivery_queue_depth = queue.depth;
    total.delivery_queue_high_water = queue.high_water;

    return total;
}

} // namespace TangoBulk
