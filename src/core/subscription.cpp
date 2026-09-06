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

namespace TangoBulk
{
namespace
{

using namespace std::chrono_literals;

constexpr auto k_control_quantum = 100ms;

constexpr auto k_dispatch_quantum = 20ms;

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

    total.views_outstanding = part.views_outstanding;
    total.pinned_bytes = part.pinned_bytes;
}

} // namespace


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
            error = e.error();
            return false;
        }
        catch(const std::exception &e)
        {
            error = BulkError{Status::TransportFailure, e.what(), "subscriber"};
            return false;
        }

        if(retired_geometry.generation != 0 &&
           !describes_same_array(retired_geometry, candidate.granted_geometry()))
        {
            geometry_changes.fetch_add(1, std::memory_order_relaxed);
            error = BulkError{Status::GeometryMismatch,
                              "the reopened session describes a different array than the one "
                              "that was retired; the application must open a new subscription "
                              "with the new contract in hand",
                              "subscriber"};

            fresh.reset();
            return false;
        }

        {
            std::lock_guard<std::mutex> lock(observation);
            session = candidate;
        }

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
                }
            }
        }

        publish_transport(nullptr);
    }

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

        if(status != Status::Ok)
        {
            renewals_failed.fetch_add(1, std::memory_order_relaxed);
        }

        return status;
    }

    std::uint32_t current_renew_interval() noexcept
    {
        const std::uint32_t granted = session.renew_interval_ms();
        return granted == 0 ? 1'000u : granted;
    }


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

        close_session();
    }

    bool reconnect(std::uint32_t &attempt, BulkError error) noexcept
    {
        if(config.reconnect_policy != ReconnectPolicy::BoundedRetry)
        {
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
                transition(SubscriberState::Failed, error);
                return false;
            }
        }

        return false;
    }

    bool backoff(std::uint32_t attempt) noexcept
    {
        const auto delay =
            backoff_delay(attempt, config.reconnect_backoff_ms, last_lease_ttl_ms);

        std::unique_lock<std::mutex> lock(mutex);
        wake.wait_for(lock, delay, [this] { return !running.load(std::memory_order_acquire); });
        return running.load(std::memory_order_acquire);
    }


    void publish_transport(std::unique_ptr<detail::SubscriberTransport> next) noexcept
    {
        if(transport)
        {
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

        transport = nullptr;

        if(retired_one)
        {
            delivery->discard();
        }

        transport = std::move(next);
        publish_observation();
    }

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
                }
            }
            ++delivered;
        }

        return delivered;
    }

    std::size_t deliver_frames(std::chrono::milliseconds timeout,
                               std::size_t max_frames = 0) noexcept
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        std::size_t delivered = 0;

        while(max_frames == 0 || delivered < max_frames)
        {
            FrameView view;

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

    void shutdown(bool announce_closed) noexcept
    {
        running.store(false, std::memory_order_release);
        wake.notify_all();

        delivery->stop();

        if(control.joinable())
        {
            control.join();
        }
        if(dispatch.joinable())
        {
            dispatch.join();
        }

        close_session();

        if(announce_closed)
        {
            transition(SubscriberState::Closed, BulkError{});
        }

        drain_transitions();
    }


    SubscriberConfig config;
    detail::CoordinationChannel channel;
    detail::TransportFactory factory;

    FrameCallback frame_callback;
    StateCallback state_callback;

    detail::SessionClient session;

    std::uint64_t delivered_at_open{0};

    std::shared_ptr<detail::DeliveryQueue> delivery;

    std::atomic<SubscriberState> state{SubscriberState::Closed};
    std::atomic<bool> running{false};

    std::mutex mutex;
    std::condition_variable wake;

    std::unique_ptr<detail::SubscriberTransport> transport;

    std::mutex observation;
    SubscriberCounters retired{};
    SubscriberCounters live{};
    std::uint32_t last_lease_ttl_ms{0};

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
        const std::lock_guard<std::mutex> lock(impl_->observation);
        total = impl_->retired;
        accumulate(total, impl_->live);
    }

    total.reconnects = impl_->reconnects.load(std::memory_order_relaxed);
    total.geometry_changes += impl_->geometry_changes.load(std::memory_order_relaxed);

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
