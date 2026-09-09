// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <core/subscription_internal.h>

#include <core/geometry_conversion.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
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

[[noreturn]] void throw_delivery_terminal(const detail::DeliveryRead &result)
{
    switch(result.kind)
    {
    case detail::DeliveryRead::Kind::Closed:
        throw BulkException(
            BulkError{Status::Shutdown, "the subscription is closed", "subscriber"});
    case detail::DeliveryRead::Kind::Interrupted:
        throw BulkException(
            BulkError{Status::Shutdown, "the subscription was interrupted", "subscriber"});
    case detail::DeliveryRead::Kind::SessionFailed:
        throw BulkException(result.error.status == Status::Ok
                                ? BulkError{Status::TransportFailure,
                                            "the subscription failed",
                                            "subscriber"}
                                : result.error);
    case detail::DeliveryRead::Kind::CallbackFailed:
        throw BulkException(result.error.status == Status::Ok
                                ? BulkError{Status::Internal,
                                            "the frame callback failed",
                                            "subscriber"}
                                : result.error);
    case detail::DeliveryRead::Kind::Frame:
    case detail::DeliveryRead::Kind::Empty:
    case detail::DeliveryRead::Kind::Timeout:
        break;
    }

    throw BulkException(BulkError{Status::Internal,
                                  "the delivery result was not terminal",
                                  "subscriber"});
}

} // namespace


std::chrono::milliseconds detail::backoff_delay(std::uint32_t attempt,
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
    /// The wire-level session is implementation state of a Subscription.  It
    /// deliberately has no header of its own: callers own a Subscription, not
    /// a second session object, and a replacement transport must not become a
    /// second lifecycle owner.
    struct SessionState
    {
        std::vector<std::byte> make_open_request(const SubscriberConfig &config,
                                                 const std::vector<std::byte> &client_address,
                                                 std::uint64_t correlation_id) const
        {
            Protocol::OpenRequest request;
            request.version_min = Protocol::k_version_major;
            request.version_max = Protocol::k_version_major;
            // Coalescing and probe.  Geometry re-arm is left out because the
            // two-ring interlock is not implemented, and claiming a capability
            // this side cannot honour is worse than not having it.
            request.requested_caps = Protocol::k_caps_credit_coalescing | Protocol::k_caps_probe;
            request.client_instance_id = Protocol::generate_client_instance_id();
            const ReceivePlan upper = ReceivePlan::from_limits(
                config.max_frame_bytes, config.ring_depth, config.credit_window);
            request.requested_max_frame_bytes = upper.max_frame_bytes;
            request.requested_ring_depth = upper.ring_depth;
            request.requested_credit_window = upper.credit_window;
            request.requested_memory_kind = config.receive_memory_kind;
            request.requested_transport = Protocol::Transport::ActiveMessage;
            request.drop_policy = config.drop_policy;
            request.stream_name = config.stream_name;
            request.client_ucx_address = client_address;

            return Protocol::encode(request, correlation_id);
        }

        Status adopt_open_reply(const std::byte *data,
                                std::size_t size,
                                const SubscriberConfig &config,
                                std::uint64_t expected_correlation)
        {
            Protocol::Envelope envelope;
            if(Protocol::decode_envelope(data, size, envelope) != Status::Ok)
            {
                return Status::MalformedMessage;
            }

            if(envelope.correlation_id != expected_correlation)
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

            // A grant is only ever clamped downward, so a ring registered at
            // the requested geometry is large enough for the granted one.
            // Check anyway because that depends on peer behaviour.
            if(reply.geometry.max_frame_bytes > config.max_frame_bytes ||
               reply.geometry.ring_depth > config.ring_depth ||
               reply.geometry.credit_window > config.credit_window)
            {
                return Status::GeometryMismatch;
            }

            session_id = reply.session_id;
            stream_id = reply.stream_id;
            server_address = reply.server_ucx_address;
            granted = reply.geometry;
            lease_ttl_ms = reply.lease_ttl_ms;
            renew_interval_ms = reply.renew_interval_ms;
            return Status::Ok;
        }

        std::vector<std::byte> make_renew_request(std::uint64_t correlation_id,
                                                  std::uint64_t frames_delivered,
                                                  std::uint64_t credits_returned,
                                                  std::uint32_t client_state) const
        {
            Protocol::RenewRequest request;
            request.session_id = session_id;
            request.client_frames_delivered = frames_delivered;
            request.client_credits_returned = credits_returned;
            request.client_state = client_state;
            return Protocol::encode(request, correlation_id);
        }

        struct RenewOutcome
        {
            Status status{Status::Ok};
            bool session_lost{false};
        };

        RenewOutcome adopt_renew_reply(const std::byte *data,
                                       std::size_t size,
                                       std::uint64_t expected_correlation)
        {
            Protocol::Envelope envelope;
            if(Protocol::decode_envelope(data, size, envelope) != Status::Ok)
            {
                return {Status::MalformedMessage, false};
            }

            if(envelope.correlation_id != expected_correlation)
            {
                return {Status::MalformedMessage, false};
            }

            // Transport errors and Error replies are recovery hints.  A
            // session is terminal only when the publisher says it is gone.
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

            if(reply.session_id != session_id)
            {
                return {Status::MalformedMessage, false};
            }

            if(reply.status != Status::Ok)
            {
                return {reply.status, reply.status != Status::RenewTooFrequent};
            }

            lease_ttl_ms = reply.lease_ttl_ms;
            renew_interval_ms = reply.renew_interval_ms;
            return {Status::Ok, false};
        }

        std::vector<std::byte> make_close_request(std::uint64_t correlation_id) const
        {
            Protocol::CloseRequest request;
            request.session_id = session_id;
            request.reason = Protocol::CloseReason::ClientShutdown;
            return Protocol::encode(request, correlation_id);
        }

        Status adopt_close_reply(const std::byte *data,
                                 std::size_t size,
                                 std::uint64_t expected_correlation) const noexcept
        {
            Protocol::Envelope envelope;
            if(Protocol::decode_envelope(data, size, envelope) != Status::Ok ||
               envelope.correlation_id != expected_correlation)
            {
                return Status::MalformedMessage;
            }

            if(envelope.msg_type == Protocol::CoordType::Error)
            {
                Protocol::ErrorMessage error;
                return Protocol::decode(data, size, error) == Status::Ok
                           ? error.status
                           : Status::MalformedMessage;
            }

            Protocol::CloseReply reply;
            if(Protocol::decode(data, size, reply) != Status::Ok)
            {
                return Status::MalformedMessage;
            }
            if(reply.session_id != session_id)
            {
                return Status::MalformedMessage;
            }
            return reply.status;
        }

        const std::vector<std::byte> &server_address_value() const noexcept
        {
            return server_address;
        }

        Protocol::StreamId stream_id_value() const noexcept
        {
            return stream_id;
        }

        const Protocol::GeometryBlock &granted_geometry() const noexcept
        {
            return granted;
        }

        std::uint32_t lease_ttl() const noexcept
        {
            return lease_ttl_ms;
        }

        std::uint32_t renew_interval() const noexcept
        {
            return renew_interval_ms;
        }

      private:
        Protocol::SessionId session_id{};
        Protocol::StreamId stream_id{0};
        std::vector<std::byte> server_address;
        Protocol::GeometryBlock granted{};
        std::uint32_t lease_ttl_ms{0};
        std::uint32_t renew_interval_ms{0};
    };

    Impl(SubscriberConfig cfg,
         detail::CoordinationChannel coordination,
         detail::TransportFactory transport_factory,
         SubscriptionCallbacks cbs) :
        config(std::move(cfg)),
        channel(std::move(coordination)),
        factory(std::move(transport_factory)),
        frame_callback(std::move(cbs.on_frame)),
        delivery(std::make_shared<detail::DeliveryQueue>(config.delivery_queue_depth,
                                                 config.drop_policy))
    {
    }

    void close_candidate(const SessionState &candidate,
                         std::chrono::steady_clock::time_point deadline) noexcept
    {
        try
        {
            const std::uint64_t correlation_id = next_correlation_id();
            const std::vector<std::byte> request = candidate.make_close_request(correlation_id);
            const std::vector<std::byte> reply =
                channel(Protocol::CoordType::Close, request, deadline);
            (void)candidate.adopt_close_reply(reply.data(), reply.size(), correlation_id);
        }
        catch(const std::exception &)
        {
            // BulkClose is best effort while abandoning a session that never
            // became active; the original establishment error is authoritative.
        }
    }


    bool open_subscription(
        BulkError &error,
        std::chrono::steady_clock::time_point establishment_deadline =
            std::chrono::steady_clock::time_point::max()) noexcept
    {
        std::unique_ptr<detail::SubscriberTransport> fresh;

        try
        {
            fresh = factory(config, delivery->make_ingress());
        }
        catch(const BulkException &e)
        {
            delivery->retire_ingress();
            error = e.error();
            return false;
        }
        catch(const std::exception &e)
        {
            delivery->retire_ingress();
            error = BulkError{Status::Internal, e.what(), "subscriber"};
            return false;
        }

        if(!fresh)
        {
            delivery->retire_ingress();
            error = BulkError{
                Status::Internal, "the transport factory returned nothing", "subscriber"};
            return false;
        }

        SessionState candidate;

        try
        {
            const std::uint64_t correlation_id = next_correlation_id();
            const std::vector<std::byte> request = candidate.make_open_request(
                config, fresh->local_address(), correlation_id);
            const std::vector<std::byte> reply =
                channel(Protocol::CoordType::Open, request, establishment_deadline);

            const Status status =
                candidate.adopt_open_reply(reply.data(), reply.size(), config, correlation_id);
            if(status != Status::Ok)
            {
                delivery->retire_ingress();
                error = BulkError{status,
                                  std::string("BulkOpen was refused: ") + to_string(status),
                                  "subscriber"};
                return false;
            }
        }
        catch(const BulkException &e)
        {
            delivery->retire_ingress();
            error = e.error();
            return false;
        }
        catch(const std::exception &e)
        {
            delivery->retire_ingress();
            error = BulkError{Status::TransportFailure, e.what(), "subscriber"};
            return false;
        }

        const Geometry granted = detail::to_geometry(candidate.granted_geometry());
        if(retired_geometry.generation != 0 &&
           !retired_geometry.describes_same_array(granted))
        {
            geometry_changes.fetch_add(1, std::memory_order_relaxed);
            error = BulkError{Status::GeometryMismatch,
                              "the reopened session describes a different array than the one "
                              "that was retired; the application must open a new subscription "
                              "with the new contract in hand",
                              "subscriber"};

            close_candidate(candidate, establishment_deadline);
            delivery->retire_ingress();
            fresh.reset();
            return false;
        }

        {
            std::lock_guard<std::mutex> lock(observation);
            session = candidate;
            current_geometry = granted;
            actual_plan = ReceivePlan::from_limits(granted.max_frame_bytes,
                                                    granted.ring_depth,
                                                    granted.credit_window);
        }

        delivered_at_open = delivery->stats().taken;

        if(const Status status = fresh->activate(session.stream_id_value(),
                                                 session.granted_geometry(),
                                                 session.server_address_value());
           status != Status::Ok)
        {
            const BulkError reported = fresh->last_error();
            error = reported.status != Status::Ok
                        ? reported
                        : BulkError{status, "the transport could not adopt the grant",
                                    "subscriber"};
            close_candidate(candidate, establishment_deadline);
            delivery->retire_ingress();
            fresh.reset();
            {
                const std::lock_guard<std::mutex> lock(observation);
                session = SessionState{};
                current_geometry = Geometry{};
                actual_plan = ReceivePlan{};
            }
            return false;
        }

        publish_transport(std::move(fresh));
        transition(SubscriberState::Probing, BulkError{});

        const auto probe_deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(config.probe_timeout_ms);
        const auto deadline = std::min(probe_deadline, establishment_deadline);

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

        close_session(establishment_deadline);
        return false;
    }

    void close_session(
        std::chrono::steady_clock::time_point deadline =
            std::chrono::steady_clock::time_point::max(),
        bool discard_queued = true) noexcept
    {
        if(deadline == std::chrono::steady_clock::time_point::max())
        {
            deadline = coordination_deadline();
        }

        {
            if(transport)
            {
                try
                {
                    const std::uint64_t correlation_id = next_correlation_id();
                    const std::vector<std::byte> request =
                        session.make_close_request(correlation_id);
                    const std::vector<std::byte> reply =
                        channel(Protocol::CoordType::Close, request, deadline);
                    (void)session.adopt_close_reply(reply.data(), reply.size(), correlation_id);
                }
                catch(const std::exception &)
                {
                }
            }
        }

        publish_transport(nullptr, discard_queued);
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
            const std::uint64_t correlation_id = next_correlation_id();
            const std::vector<std::byte> request = session.make_renew_request(
                correlation_id,
                delivery->stats().taken - delivered_at_open,
                credits_returned,
                client_state);

            renewals_sent.fetch_add(1, std::memory_order_relaxed);
            const std::vector<std::byte> reply =
                channel(Protocol::CoordType::Renew, request, coordination_deadline());
            status = session.adopt_renew_reply(reply.data(), reply.size(), correlation_id).status;

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
        const std::uint32_t granted = session.renew_interval();
        return granted == 0 ? 1'000u : granted;
    }

    std::chrono::steady_clock::time_point coordination_deadline() noexcept
    {
        {
            std::lock_guard<std::mutex> lock(shutdown_mutex);
            if(shutdown_started)
            {
                return shutdown_deadline;
            }
        }
        return std::chrono::steady_clock::now() +
               std::chrono::milliseconds(config.command_timeout_ms);
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

            // Keep accepted frames until we know whether this is a terminal
            // failure. A reconnect attempt discards them before it allocates
            // or activates its replacement transport.
            close_session(std::chrono::steady_clock::time_point::max(), false);

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

        if(!interrupted.load(std::memory_order_acquire))
        {
            close_session();
        }
        if(state.load(std::memory_order_acquire) == SubscriberState::Failed)
        {
            running.store(false, std::memory_order_release);
            wake.notify_all();
        }

        {
            std::lock_guard<std::mutex> lock(shutdown_mutex);
            control_finished = true;
        }
        shutdown_wake.notify_all();
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

            delivery->discard();
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

    bool backoff(
        std::uint32_t attempt,
        std::chrono::steady_clock::time_point deadline =
            std::chrono::steady_clock::time_point::max()) noexcept
    {
        auto delay =
            detail::backoff_delay(attempt, config.reconnect_backoff_ms, last_lease_ttl_ms);
        const auto remaining = deadline - std::chrono::steady_clock::now();
        if(remaining < delay)
        {
            delay = std::chrono::duration_cast<std::chrono::milliseconds>(remaining);
        }
        if(delay <= std::chrono::milliseconds::zero())
        {
            return false;
        }

        std::unique_lock<std::mutex> lock(mutex);
        wake.wait_for(lock, delay, [this] { return !running.load(std::memory_order_acquire); });
        return running.load(std::memory_order_acquire) &&
               std::chrono::steady_clock::now() < deadline;
    }

    static bool retryable_establishment(Status status) noexcept
    {
        return status == Status::TransportFailure || status == Status::UnknownStream ||
               status == Status::UnknownSession || status == Status::SessionExpired;
    }


    void publish_transport(std::unique_ptr<detail::SubscriberTransport> next,
                           bool discard_queued = true) noexcept
    {
        if(transport)
        {
            // The transport gets a generation-scoped ingress. Retire it
            // before destroying the object so delayed progress cannot publish
            // into the replacement session's queue.
            delivery->retire_ingress();
            std::lock_guard<std::mutex> lock(observation);

            accumulate(retired, transport->counters());
            live = SubscriberCounters{};
            last_lease_ttl_ms = std::max(last_lease_ttl_ms, session.lease_ttl());

            if(current_geometry.generation != 0)
            {
                retired_geometry = current_geometry;
            }

            session = SessionState{};
            current_geometry = Geometry{};
            actual_plan = ReceivePlan{};
        }

        const bool retired_one = transport != nullptr;

        transport = nullptr;

        if(retired_one && discard_queued)
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


    void transition(SubscriberState next, BulkError error) noexcept
    {
        if(next == SubscriberState::Failed)
        {
            // Delivery publishes the terminal boundary before state is made
            // visible. Readers therefore drain accepted frames first and only
            // then observe this failure.
            delivery->fail(error);
        }
        state.store(next, std::memory_order_release);
        if(next == SubscriberState::Failed)
        {
            std::lock_guard<std::mutex> lock(observation);
            last_error = std::move(error);
        }
        else if(next == SubscriberState::Active)
        {
            std::lock_guard<std::mutex> lock(observation);
            last_error = BulkError{};
        }
        wake.notify_all();
    }

    std::size_t deliver_frames(std::chrono::milliseconds timeout,
                               std::size_t max_frames)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        std::size_t delivered = 0;

        while(delivered < max_frames)
        {
            detail::DeliveryRead result = delivered == 0
                                              ? delivery->read_result(deadline)
                                              : delivery->try_read_result();
            if(result.kind != detail::DeliveryRead::Kind::Frame)
            {
                break;
            }

            ++delivered;

            try
            {
                frame_callback(std::move(result.frame));
            }
            catch(...)
            {
                fail_delivery();
                break;
            }

            result.frame.reset();
        }

        return delivered;
    }

    void fail_delivery() noexcept
    {
        const BulkError error{Status::Internal, "the frame callback threw", "subscriber"};
        delivery->callback_failed(error);
        transition(SubscriberState::Failed, error);
        running.store(false, std::memory_order_release);
        wake.notify_all();
    }

    void dispatch_loop() noexcept
    {
        for(;;)
        {
            const bool active = running.load(std::memory_order_acquire) &&
                                !interrupted.load(std::memory_order_acquire) &&
                                state.load(std::memory_order_acquire) != SubscriberState::Failed;
            if(!active &&
               (interrupted.load(std::memory_order_acquire) ||
                state.load(std::memory_order_acquire) == SubscriberState::Closed ||
                delivery->stats().depth == 0))
            {
                {
                    std::lock_guard<std::mutex> lock(shutdown_mutex);
                    dispatch_finished = true;
                }
                shutdown_wake.notify_all();
                return;
            }

            // A terminal Session failure stops ingress but deliberately leaves
            // accepted frames in the queue. Drain those frames before the
            // dispatcher exits; orderly close and interrupt discard first, so
            // they still stop immediately.
            deliver_frames(active ? k_dispatch_quantum : std::chrono::milliseconds::zero(), 32);
        }
    }

    std::uint64_t next_correlation_id() noexcept
    {
        return correlation.fetch_add(1, std::memory_order_relaxed) + 1;
    }

    void shutdown(bool announce_closed) noexcept
    {
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(config.command_timeout_ms);
        {
            std::unique_lock<std::mutex> lock(shutdown_mutex);
            if(shutdown_complete)
            {
                return;
            }

            const std::thread::id self = std::this_thread::get_id();
            if(shutdown_started)
            {
                if(shutdown_owner == self)
                {
                    return;
                }

                shutdown_wake.wait(lock, [this] { return shutdown_complete; });
                return;
            }

            shutdown_started = true;
            shutdown_owner = self;
            shutdown_deadline = deadline;
        }

        running.store(false, std::memory_order_release);
        wake.notify_all();

        delivery->close();

        const std::thread::id self = std::this_thread::get_id();
        bool control_detached = false;
        if(control.joinable())
        {
            if(control.get_id() == self)
            {
                control.detach();
                control_detached = true;
            }
            else
            {
                std::unique_lock<std::mutex> lock(shutdown_mutex);
                const bool finished = control_finished ||
                                      shutdown_wake.wait_until(
                                          lock, deadline, [this] { return control_finished; });
                lock.unlock();
                if(finished)
                {
                    control.join();
                }
                else
                {
                    control.detach();
                    control_detached = true;
                }
            }
        }
        if(dispatch.joinable())
        {
            if(dispatch.get_id() == self)
            {
                dispatch.detach();
            }
            else
            {
                std::unique_lock<std::mutex> lock(shutdown_mutex);
                const bool finished = dispatch_finished ||
                                      shutdown_wake.wait_until(
                                          lock, deadline, [this] { return dispatch_finished; });
                lock.unlock();
                if(finished)
                {
                    dispatch.join();
                }
                else
                {
                    dispatch.detach();
                }
            }
        }

        if(!control_started || (interrupted.load(std::memory_order_acquire) && !control_detached))
        {
            close_session(deadline);
        }

        if(announce_closed)
        {
            transition(SubscriberState::Closed, BulkError{});
        }

        {
            std::lock_guard<std::mutex> lock(shutdown_mutex);
            shutdown_complete = true;
        }
        shutdown_wake.notify_all();
    }


    SubscriberConfig config;
    detail::CoordinationChannel channel;
    detail::TransportFactory factory;

    FrameCallback frame_callback;

    SessionState session;

    std::uint64_t delivered_at_open{0};

    std::shared_ptr<detail::DeliveryQueue> delivery;

    std::atomic<SubscriberState> state{SubscriberState::Closed};
    std::atomic<bool> running{false};
    std::atomic<bool> interrupted{false};

    std::mutex mutex;
    std::condition_variable wake;

    std::unique_ptr<detail::SubscriberTransport> transport;

    std::mutex observation;
    SubscriberCounters retired{};
    SubscriberCounters live{};
    std::uint32_t last_lease_ttl_ms{0};

    Geometry current_geometry{};
    ReceivePlan actual_plan{};
    Geometry retired_geometry{};
    BulkError last_error{};

    std::thread control;
    std::thread dispatch;

    std::mutex shutdown_mutex;
    std::condition_variable shutdown_wake;
    std::thread::id shutdown_owner;
    std::chrono::steady_clock::time_point shutdown_deadline{
        std::chrono::steady_clock::time_point::max()};
    bool shutdown_started{false};
    bool shutdown_complete{false};
    bool control_started{false};
    bool control_finished{false};
    bool dispatch_finished{false};

    std::atomic<std::uint64_t> correlation{0};
    std::atomic<std::uint64_t> reconnects{0};
    std::atomic<std::uint64_t> geometry_changes{0};
    std::atomic<std::uint64_t> renewals_sent{0};
    std::atomic<std::uint64_t> renewals_failed{0};
};


Subscription::Subscription(std::shared_ptr<Impl> impl) noexcept :
    impl_(std::move(impl))
{
}

Subscription::~Subscription()
{
    close();
}

void Subscription::close() noexcept
{
    impl_->shutdown(true);
}

void Subscription::interrupt() noexcept
{
    impl_->interrupted.store(true, std::memory_order_release);
    impl_->running.store(false, std::memory_order_release);
    impl_->delivery->interrupt();
    impl_->wake.notify_all();
}

std::optional<FrameView> Subscription::try_read()
{
    Impl &impl = *impl_;

    if(impl.config.delivery_mode != DeliveryMode::Pull)
    {
        throw BulkException(BulkError{Status::Internal,
                                      "try_read() requires DeliveryMode::Pull",
                                      "subscriber"});
    }

    detail::DeliveryRead result = impl.delivery->try_read_result();
    if(result.kind == detail::DeliveryRead::Kind::Frame)
    {
        return std::move(result.frame);
    }
    if(result.kind == detail::DeliveryRead::Kind::Empty)
    {
        return std::nullopt;
    }
    throw_delivery_terminal(result);
}

std::optional<FrameView> Subscription::read_for(std::chrono::milliseconds timeout)
{
    Impl &impl = *impl_;

    if(impl.config.delivery_mode != DeliveryMode::Pull)
    {
        throw BulkException(BulkError{Status::Internal,
                                      "read_for() requires DeliveryMode::Pull",
                                      "subscriber"});
    }

    const auto deadline = std::chrono::steady_clock::now() + timeout;
    detail::DeliveryRead result = impl.delivery->read_result(deadline);
    if(result.kind == detail::DeliveryRead::Kind::Frame)
    {
        return std::move(result.frame);
    }
    if(result.kind == detail::DeliveryRead::Kind::Empty ||
       result.kind == detail::DeliveryRead::Kind::Timeout)
    {
        return std::nullopt;
    }
    throw_delivery_terminal(result);
}

std::unique_ptr<Subscription> detail::SubscriptionFactory::open(
    SubscriberConfig config,
    detail::CoordinationChannel channel,
    detail::TransportFactory factory,
    SubscriptionCallbacks callbacks,
    std::chrono::steady_clock::time_point establishment_deadline)
{
    const Status status = config.validate();
    if(status != Status::Ok)
    {
        throw BulkException(BulkError{
            status, std::string("invalid SubscriberConfig: ") + to_string(status), "subscriber"});
    }

    // Normalize all caller intent before constructing a transport. The
    // transport allocates the receive ring before Open, so it must see the
    // exact same upper plan that the coordination request advertises.
    const ReceivePlan upper = config.upper_receive_plan();
    config.max_frame_bytes = upper.max_frame_bytes;
    config.ring_depth = upper.ring_depth;
    config.credit_window = upper.credit_window;

    if(!channel || !factory)
    {
        throw BulkException(BulkError{Status::Internal,
                                      "a Subscription needs both a coordination channel "
                                      "and a transport factory",
                                      "subscriber"});
    }

    if(config.delivery_mode == DeliveryMode::Push && !callbacks.on_frame)
    {
        throw BulkException(BulkError{Status::Internal,
                                      "SubscriptionCallbacks needs on_frame",
                                      "subscriber"});
    }

    auto impl = std::make_shared<Subscription::Impl>(
        std::move(config), std::move(channel), std::move(factory), std::move(callbacks));

    impl->running.store(true, std::memory_order_release);

    impl->transition(SubscriberState::Opening, BulkError{});

    BulkError error;
    if(establishment_deadline == std::chrono::steady_clock::time_point::max())
    {
        establishment_deadline =
            std::chrono::steady_clock::now() +
            std::chrono::milliseconds(impl->config.establishment_timeout_ms);
    }
    if(impl->open_subscription(error, establishment_deadline))
    {
        if(impl->config.delivery_mode == DeliveryMode::Push)
        {
            impl->dispatch = std::thread([worker = impl] { worker->dispatch_loop(); });
        }
        impl->control_started = true;
        impl->control = std::thread([worker = impl] { worker->control_loop(); });
        return std::unique_ptr<Subscription>(new Subscription(std::move(impl)));
    }

    if(impl->config.reconnect_policy == ReconnectPolicy::BoundedRetry)
    {
        bool opened = false;
        for(std::uint32_t attempt = 0; attempt < impl->config.reconnect_max_attempts;
            ++attempt)
        {
            if(!Subscription::Impl::retryable_establishment(error.status) ||
               std::chrono::steady_clock::now() >= establishment_deadline)
            {
                break;
            }
            impl->transition(SubscriberState::Reconnecting, error);
            if(!impl->backoff(attempt, establishment_deadline))
            {
                break;
            }
            impl->transition(SubscriberState::Opening, BulkError{});
            if(impl->open_subscription(error, establishment_deadline))
            {
                opened = true;
                break;
            }
            if(!Subscription::Impl::retryable_establishment(error.status))
            {
                break;
            }
        }

        if(opened)
        {
            if(impl->config.delivery_mode == DeliveryMode::Push)
            {
                impl->dispatch = std::thread([worker = impl] { worker->dispatch_loop(); });
            }
            impl->control_started = true;
            impl->control = std::thread([worker = impl] { worker->control_loop(); });
            return std::unique_ptr<Subscription>(new Subscription(std::move(impl)));
        }
    }

    impl->transition(SubscriberState::Failed, error);
    impl->shutdown(false);
    throw BulkException(error);
}

SubscriberState Subscription::state() const noexcept
{
    return impl_->state.load(std::memory_order_acquire);
}

int Subscription::fd() const noexcept
{
    return impl_->config.delivery_mode == DeliveryMode::Pull ? impl_->delivery->fd() : -1;
}

Geometry Subscription::geometry() const noexcept
{
    const std::lock_guard<std::mutex> lock(impl_->observation);
    return impl_->current_geometry;
}

ReceivePlan Subscription::plan() const noexcept
{
    const std::lock_guard<std::mutex> lock(impl_->observation);
    return impl_->actual_plan;
}

SubscriptionSnapshot Subscription::snapshot() const noexcept
{
    SubscriptionSnapshot out;
    out.state = state();
    out.counters = counters();
    out.interrupted = impl_->interrupted.load(std::memory_order_acquire);
    {
        const std::lock_guard<std::mutex> lock(impl_->observation);
        out.error = impl_->last_error;
    }
    return out;
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
