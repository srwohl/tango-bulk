// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/unstable/session_supervisor.h>

#include <core/delivery_queue.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>
#include <utility>

/// `SessionSupervisor`: everything `BulkSubscriber` used to do that was not
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
/// The consequence worth stating: a stuck coordination call cannot stall frame
/// delivery, because delivery does not run on the thread that makes it. It can
/// only, eventually, expire the lease -- which is exactly the failure mode 7.4
/// asks for.
///
/// And the converse, which was *not* true until the delivery queue moved here:
/// a stuck application cannot stall coordination either. Delivery used to reach
/// the transport through a borrow held for the whole of `poll()` and the whole
/// of the user callback, so retirement waited on arbitrary application code --
/// in `Manual` mode, on whatever timeout the application passed. Frames now go
/// into a queue this object owns, the transport pushes and never lends itself
/// out, and replacing a transport waits for nobody (section 4.2).
namespace TangoBulk::detail
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
/// Delivery counts are deliberately absent. `frames_delivered`,
/// `frames_dropped_queue_full` and the two queue gauges belong to the delivery
/// queue, which spans every session rather than one -- so they are read from it
/// once in `counters()`, and summing a transport's copy would count the same
/// frame again for every session the subscription outlived.
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
    total.renewals_sent += part.renewals_sent;
    total.renewals_failed += part.renewals_failed;
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

struct SessionSupervisor::Impl
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

        Ref(Impl &owner, std::shared_ptr<SubscriberTransport> transport) noexcept :
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

        SubscriberTransport *operator->() const noexcept
        {
            return transport_.get();
        }

      private:
        Impl *owner_{nullptr};
        std::shared_ptr<SubscriberTransport> transport_;
    };

    Impl(SubscriberConfig cfg,
         CoordinationChannel coordination,
         TransportFactory transport_factory,
         SessionCallbacks cbs) :
        config(std::move(cfg)),
        channel(std::move(coordination)),
        factory(std::move(transport_factory)),
        frame_callback(std::move(cbs.on_frame)),
        state_callback(std::move(cbs.on_state)),
        delivery(std::make_shared<DeliveryQueue>(config.delivery_queue_depth,
                                                 config.drop_policy))
    {
    }

    // -- coordination, control thread only ----------------------------------

    /// Open a session, from `make_open_request` through to `Active`.
    ///
    /// Every failure path leaves no transport behind: a half-open subscriber
    /// with a registered ring and no session is exactly the resource leak the
    /// lease exists to prevent on the other side.
    bool open_session(BulkError &error) noexcept
    {
        std::shared_ptr<SubscriberTransport> fresh;

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

        try
        {
            const std::vector<std::byte> request =
                fresh->make_open_request(next_correlation_id());
            const std::vector<std::byte> reply =
                channel(Protocol::CoordType::Open, request);

            const Status status = fresh->adopt_open_reply(reply.data(), reply.size());
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
           !describes_same_array(retired_geometry, fresh->granted_geometry()))
        {
            geometry_changes.fetch_add(1, std::memory_order_relaxed);
            error = BulkError{Status::GeometryMismatch,
                              "the reopened session describes a different array than the one "
                              "that was retired; the application must open a new subscription "
                              "with the new contract in hand",
                              "subscriber"};

            // Not adopted: this transport is closed rather than published, so
            // no frame from it can reach a callback expecting the old shape.
            //
            // Destroyed *first*, then the queue is emptied. `adopt_open_reply`
            // starts the engine before this check runs, so a frame of the new
            // shape may already be queued; discarding before the producer has
            // stopped would leave the next one behind it. Section 4.2 wants the
            // check to precede the start, which is a change to the seam rather
            // than to this ordering.
            fresh.reset();
            delivery->discard();
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
            const Ref live = borrow();
            const SubscriberState observed = live ? live->state() : SubscriberState::Failed;

            if(observed == SubscriberState::Active)
            {
                transition(SubscriberState::Active, BulkError{});
                return true;
            }

            if(observed == SubscriberState::Failed || !running.load(std::memory_order_acquire))
            {
                error = live ? live->last_error() : BulkError{};
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
            const Ref live = borrow();
            if(live)
            {
                try
                {
                    const std::vector<std::byte> request =
                        live->make_close_request(next_correlation_id());
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

            SubscriberState observed = SubscriberState::Failed;
            BulkError reported;
            {
                const Ref live = borrow();
                if(!live)
                {
                    error = BulkError{Status::Internal, "the transport disappeared", "subscriber"};
                    return false;
                }
                observed = live->state();

                // Read while the borrow is still held: the reason belongs to
                // the transport, and the transport can be swapped the moment it
                // is given back.
                if(observed == SubscriberState::Failed)
                {
                    reported = live->last_error();
                }
            }

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
            const std::vector<std::byte> reply =
                channel(Protocol::CoordType::Renew, request);
            status = live->adopt_renew_reply(reply.data(), reply.size());

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

    /// Reopen under the configured policy.  Returns false if the supervisor is
    /// finished -- `Failed`, or torn down while backing off.
    bool reconnect(std::uint32_t &attempt, BulkError error) noexcept
    {
        if(config.reconnect_policy != ReconnectPolicy::BoundedRetry)
        {
            // 4.1: FailFast makes any transition to `Reconnecting` a `Failed`,
            // and Manual enters `Failed` and waits for the application to open a
            // new supervisor.  They differ in what the application does next,
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
            if(open_session(error))
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

    void publish_transport(std::shared_ptr<SubscriberTransport> next) noexcept
    {
        std::shared_ptr<SubscriberTransport> previous;

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

                // And keep what it was describing, so the next grant can be
                // compared against it rather than silently replacing it.
                const Protocol::GeometryBlock going = transport->granted_geometry();
                if(going.generation != 0)
                {
                    retired_geometry = going;
                }
            }

            previous = std::move(transport);
            transport = std::move(next);
            swapping = false;
        }

        wake.notify_all();

        // `previous` dies here, on the control thread, outside the lock, and
        // that is what stops it producing. Only then is it safe to empty the
        // queue: the frames left in it were granted under a contract that has
        // ended, and the next session may describe a different array entirely.
        //
        // The ordering holds because a transport is always retired before the
        // next one is built -- `control_loop` closes the session before it
        // reconnects -- so there is never a live producer on the other side of
        // this call.
        const bool retired_one = previous != nullptr;
        previous.reset();

        if(retired_one)
        {
            delivery->discard();
        }
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

    /// Hand queued frames to the application, on the CALLING thread.
    ///
    /// No borrow. This is the whole point of the move: the queue is this
    /// object's, so delivery never touches the transport, and a callback that
    /// takes thirty seconds delays nothing but its own caller. It used to hold
    /// the transport for the length of the call, which is why an application
    /// polling with a long timeout could stop a reconnect for that long -- and
    /// under a Python binding it held the GIL while doing it.
    std::size_t deliver_frames(std::chrono::milliseconds timeout,
                               std::size_t max_frames = 0) noexcept
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        std::size_t delivered = 0;

        while(max_frames == 0 || delivered < max_frames)
        {
            FrameView view;

            // Only the first frame waits; the rest of a burst is taken without
            // blocking again. Both forms leave this consumer armed when they
            // come up empty, so a caller watching fd() needs no protocol.
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
                // A user callback that throws must not take the dispatch thread
                // with it; there is nobody above it to catch anything. Contained
                // per frame rather than per call, so one bad frame does not
                // discard the rest of the burst.
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

    /// Stop everything.  Idempotent, and safe to call on a supervisor whose
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

        // Sticky, so a consumer blocked in `take()` comes back now and stays
        // back, rather than waking, finding nothing and blocking again for the
        // rest of its timeout. Queued frames are still claimable until the
        // queue itself goes; what happens to them is settled below.
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
    CoordinationChannel channel;
    TransportFactory factory;

    FrameCallback frame_callback;
    StateCallback state_callback;

    /// Where frames go, for the life of the subscription rather than the life
    /// of a session.
    ///
    /// Created before the first transport and outliving the last, which is what
    /// makes `fd()` stable across a reconnect and what lets delivery run
    /// without touching the transport at all. Shared with whichever transport
    /// is current; `shared_ptr` because a transport that could not be quiesced
    /// is quarantined rather than destroyed, and nothing else would prove it
    /// has stopped pushing.
    std::shared_ptr<DeliveryQueue> delivery;

    std::atomic<SubscriberState> state{SubscriberState::Closed};
    std::atomic<bool> running{false};

    std::mutex mutex;
    std::condition_variable wake;
    std::shared_ptr<SubscriberTransport> transport;
    std::size_t borrowed{0};
    bool swapping{false}; ///< a swap is waiting for the borrows to come back
    std::uint32_t last_lease_ttl_ms{0};
    SubscriberCounters retired{};

    /// What the last session was describing. All-zero until one is retired,
    /// which is why the comparison is skipped on a first open.
    Protocol::GeometryBlock retired_geometry{};

    std::mutex queue_mutex;
    std::deque<Transition> transitions;

    std::thread control;
    std::thread dispatch;

    std::atomic<std::uint64_t> correlation{0};
    std::atomic<std::uint64_t> reconnects{0};
    std::atomic<std::uint64_t> geometry_changes{0};
};

// ---------------------------------------------------------------------------

SessionSupervisor::SessionSupervisor(std::unique_ptr<Impl> impl) noexcept :
    impl_(std::move(impl))
{
}

SessionSupervisor::~SessionSupervisor()
{
    impl_->shutdown(true);
}

std::unique_ptr<SessionSupervisor> SessionSupervisor::open(SubscriberConfig config,
                                                           CoordinationChannel channel,
                                                           TransportFactory factory,
                                                           SessionCallbacks callbacks)
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
                                      "a SessionSupervisor needs both a coordination channel "
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
                                      "SessionCallbacks needs both on_frame and on_state",
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
    if(impl->open_session(error))
    {
        Impl &ref = *impl;
        impl->control = std::thread([&ref] { ref.control_loop(); });
        return std::unique_ptr<SessionSupervisor>(new SessionSupervisor(std::move(impl)));
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
        return std::unique_ptr<SessionSupervisor>(new SessionSupervisor(std::move(impl)));
    }

    impl->transition(SubscriberState::Failed, error);
    impl->shutdown(false);
    throw BulkException(error);
}

std::size_t SessionSupervisor::poll(std::chrono::milliseconds timeout, std::size_t max_frames)
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

SubscriberState SessionSupervisor::state() const noexcept
{
    return impl_->state.load(std::memory_order_acquire);
}

int SessionSupervisor::fd() const noexcept
{
    return impl_->delivery->fd();
}

Protocol::GeometryBlock SessionSupervisor::granted_geometry() const noexcept
{
    const Impl::Ref live = impl_->borrow();
    return live ? live->granted_geometry() : Protocol::GeometryBlock{};
}

std::uint32_t SessionSupervisor::generation() const noexcept
{
    const Impl::Ref live = impl_->borrow();
    return live ? live->generation() : 0;
}

SubscriberCounters SessionSupervisor::counters() const noexcept
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
    total.geometry_changes += impl_->geometry_changes.load(std::memory_order_relaxed);

    // Read once from the owner rather than accumulated per session. These four
    // describe the subscription's queue, which spans every session it has had,
    // so there is nothing to fold in and nothing that resets on a reconnect.
    total.frames_delivered = impl_->delivery->taken();
    total.frames_dropped_queue_full = impl_->delivery->dropped();
    total.delivery_queue_depth = impl_->delivery->size();
    total.delivery_queue_high_water = impl_->delivery->high_water();

    return total;
}

} // namespace TangoBulk::detail
