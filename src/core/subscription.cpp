// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <core/delivery_queue.h>
#include <core/subscription_internal.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <exception>
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
constexpr auto k_reconnect_backoff = 50ms;
constexpr std::uint32_t k_reconnect_max_attempts = 10;

/// ADR 0007: one total budget for close() and destruction, not one per
/// request. A performance characteristic, not an option; its measurement is
/// still owed.
constexpr auto k_shutdown_budget = 5s;

/// Bound on one coordination call before any lease has been granted, and the
/// floor once one has: a call that outlasts the lease has failed by definition.
constexpr std::uint32_t k_min_coordination_ms = 1'000;

ReceivePlan plan_receive(const SubscriptionOptions &options, const StreamOffer &offer) noexcept
{
    if(offer.stream_name != options.stream_name || offer.validate() != Status::Ok)
    {
        return {};
    }

    ReceivePlan planned{offer.geometry.max_frame_bytes,
                        offer.geometry.ring_depth,
                        offer.geometry.credit_window};
    if(options.receive_plan)
    {
        const ReceivePlan &upper = *options.receive_plan;
        planned.max_frame_bytes = std::min(planned.max_frame_bytes, upper.max_frame_bytes);
        planned.ring_depth = std::min(planned.ring_depth, upper.ring_depth);
        planned.credit_window = std::min(planned.credit_window, upper.credit_window);
    }

    if(planned.max_frame_bytes == 0)
    {
        return {};
    }

    const std::uint64_t budget_depth = options.pinned_budget_bytes / planned.max_frame_bytes;
    planned.ring_depth =
        static_cast<std::uint32_t>(std::min<std::uint64_t>(planned.ring_depth, budget_depth));
    planned.credit_window = std::min(planned.credit_window, planned.ring_depth);

    if(planned.validate() != Status::Ok || planned.pinned_bytes() > options.pinned_budget_bytes)
    {
        return {};
    }
    return planned;
}

std::chrono::milliseconds backoff_delay(std::uint32_t attempt,
                                        std::uint32_t lease_ttl_ms) noexcept
{
    std::uint64_t delay = static_cast<std::uint64_t>(k_reconnect_backoff.count());
    for(std::uint32_t i = 0; i < attempt && delay < lease_ttl_ms; ++i)
    {
        delay *= 2;
    }

    delay = std::min<std::uint64_t>(delay, std::max<std::uint32_t>(lease_ttl_ms, 1));
    return std::chrono::milliseconds(delay);
}

void accumulate(SubscriberCounters &total, const SubscriberCounters &part) noexcept
{
    total.frames_received += part.frames_received;
    total.frames_dropped_stale_epoch += part.frames_dropped_stale_epoch;
    total.frames_dropped_bad_header += part.frames_dropped_bad_header;
    total.frames_dropped_oversize += part.frames_dropped_oversize;
    total.frames_dropped_duplicate_seq += part.frames_dropped_duplicate_seq;
    total.frames_dropped_geometry_mismatch += part.frames_dropped_geometry_mismatch;
    total.frames_copied += part.frames_copied;
    total.bytes_copied += part.bytes_copied;
    total.copy_pool_exhausted += part.copy_pool_exhausted;
    total.credits_returned += part.credits_returned;
    total.credit_messages_sent += part.credit_messages_sent;
    total.sessions_opened += part.sessions_opened;
    total.geometry_changes += part.geometry_changes;
    total.transport_errors += part.transport_errors;

    total.views_outstanding = part.views_outstanding;
    total.pinned_bytes = part.pinned_bytes;
}

std::uint64_t steady_now_ns() noexcept
{
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                          std::chrono::steady_clock::now().time_since_epoch())
                                          .count());
}

/// The typed exception for a Session that ended and was not replaced.
std::exception_ptr session_failure(BulkError error)
{
    switch(error.status)
    {
    case Status::GeometryMismatch:
        return std::make_exception_ptr(GeometryChanged(std::move(error)));
    case Status::ResourceExhausted:
        return std::make_exception_ptr(ResourceExhausted(std::move(error)));
    default:
        return std::make_exception_ptr(SessionLost(std::move(error)));
    }
}

/// The typed exception a delivery operation reports for a terminal read.
std::exception_ptr terminal_exception(const detail::DeliveryRead &result)
{
    switch(result.kind)
    {
    case detail::DeliveryRead::Kind::Closed:
        return std::make_exception_ptr(StreamClosed(
            BulkError{Status::Shutdown, "the subscription is closed", Origin::Subscriber}));
    case detail::DeliveryRead::Kind::Interrupted:
        return std::make_exception_ptr(Interrupted(BulkError{
            Status::Shutdown, "the subscription was interrupted", Origin::Subscriber}));
    case detail::DeliveryRead::Kind::SessionFailed:
        return session_failure(result.error.status == Status::Ok
                                   ? BulkError{Status::TransportFailure,
                                               "the subscription failed",
                                               Origin::Subscriber}
                                   : result.error);
    case detail::DeliveryRead::Kind::CallbackFailed:
        return std::make_exception_ptr(SessionLost(
            result.error.status == Status::Ok
                ? BulkError{Status::Internal, "the frame callback failed", Origin::Subscriber}
                : result.error));
    case detail::DeliveryRead::Kind::Frame:
    case detail::DeliveryRead::Kind::Empty:
    case detail::DeliveryRead::Kind::Timeout:
        break;
    }

    return std::make_exception_ptr(SessionLost(BulkError{
        Status::Internal, "the delivery result was not terminal", Origin::Subscriber}));
}

/// The typed exception subscribe() throws when establishment fails.
std::exception_ptr establishment_failure(BulkError error)
{
    if(error.status == Status::ResourceExhausted)
    {
        return std::make_exception_ptr(ResourceExhausted(std::move(error)));
    }
    return std::make_exception_ptr(EstablishmentError(std::move(error)));
}

} // namespace

struct Subscription::Impl
{
    /// The wire-level session is implementation state of a Subscription.  It
    /// deliberately has no header of its own: callers own a Subscription, not
    /// a second session object, and a replacement transport must not become a
    /// second lifecycle owner.
    struct SessionState
    {
        std::vector<std::byte> make_open_request(const std::string &name,
                                                 const std::string &label,
                                                 FlowPolicy session_flow,
                                                 const ReceivePlan &plan,
                                                 MemoryKind memory_kind,
                                                 Protocol::ClientInstanceId id,
                                                 const std::vector<std::byte> &client_address,
                                                 std::uint64_t correlation_id) const
        {
            Protocol::OpenRequest request;
            request.version_min = Protocol::k_version_major;
            request.version_max = Protocol::k_version_major;
            // The queue policy stays local and never crosses the wire; the
            // flow policy is precisely what does.
            request.flow = session_flow == FlowPolicy::Lossless
                               ? Protocol::FlowPolicy::Lossless
                               : Protocol::FlowPolicy::Lossy;
            request.client_instance_id = id;
            request.requested_max_frame_bytes = plan.max_frame_bytes;
            request.requested_ring_depth = plan.ring_depth;
            request.requested_credit_window = plan.credit_window;
            request.requested_memory_kind = memory_kind;
            request.stream_name = name;
            request.client_label = label;
            request.client_ucx_address = client_address;

            return Protocol::encode(request, correlation_id);
        }

        Status adopt_open_reply(const std::byte *data,
                                std::size_t size,
                                const ReceivePlan &plan,
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
            if(reply.geometry.max_frame_bytes > plan.max_frame_bytes ||
               reply.geometry.ring_depth > plan.ring_depth ||
               reply.geometry.credit_window > plan.credit_window)
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

        std::vector<std::byte> make_renew_request(std::uint64_t correlation_id) const
        {
            Protocol::RenewRequest request;
            request.session_id = session_id;
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

        const Protocol::SessionId &session_id_value() const noexcept
        {
            return session_id;
        }

        const Geometry &granted_geometry() const noexcept
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
        Geometry granted{};
        std::uint32_t lease_ttl_ms{0};
        std::uint32_t renew_interval_ms{0};
    };

    Impl(SubscriptionOptions opts,
         ReceivePlan plan,
         detail::CoordinationChannel coordination,
         detail::TransportFactory transport_factory) :
        stream_name(std::move(opts.stream_name)),
        client_label(std::move(opts.client_label)),
        upper_plan(plan),
        recovery_policy(opts.recovery_policy),
        flow(opts.flow),
        ownership(opts.ownership),
        expect(opts.expect),
        establishment_timeout_ms(opts.establishment_timeout_ms),
        allocator(std::move(opts.receive_allocator)),
        channel(std::move(coordination)),
        factory(std::move(transport_factory)),
        frame_callback(std::move(opts.on_frame)),
        // Borrowed delivery can hold at most ring_depth frames unreleased, so a
        // queue of that capacity never refuses a frame: ADR 0008's derived
        // queue capacity is the plan's ring depth.
        delivery(std::make_shared<detail::DeliveryQueue>(std::max<std::uint32_t>(plan.ring_depth, 1),
                                                         opts.queue_policy)),
        client_id(Protocol::generate_client_instance_id())
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

    /// The receive region for the next transport: the allocator's, obtained
    /// once, or empty so the transport allocates its own host ring.
    ///
    /// The allocator's region is reusable only while nothing else holds it.
    /// `owner` is shared with every transport that registered it, with a
    /// quarantined worker, and with every borrowed frame, so a use count above
    /// one is exactly "an earlier session or a delivered frame still refers to
    /// it" (ADR 0003 and 0005).
    bool prepare_region(BulkError &error) noexcept
    {
        if(!allocator)
        {
            return true;
        }

        if(!allocator_used)
        {
            allocator_used = true;
            try
            {
                region = allocator(upper_plan.pinned_bytes());
            }
            catch(const BulkException &e)
            {
                error = e.error();
                return false;
            }
            catch(const std::exception &e)
            {
                error = BulkError{Status::ResourceExhausted, e.what(), Origin::Subscriber};
                return false;
            }

            if(!region.owner)
            {
                error = BulkError{Status::ResourceExhausted,
                                  "the receive allocator returned no storage",
                                  Origin::Subscriber};
                return false;
            }
            if(region.bytes < upper_plan.pinned_bytes())
            {
                error = BulkError{Status::ResourceExhausted,
                                  "the receive allocator returned less than the receive plan "
                                  "needs",
                                  Origin::Subscriber};
                region = ReceiveRegion{};
                return false;
            }
            if(ownership == DeliveryOwnership::Copy && region.memory_kind != MemoryKind::Host)
            {
                error = BulkError{Status::MalformedMessage,
                                  "copied delivery needs a host receive region",
                                  Origin::Subscriber};
                region = ReceiveRegion{};
                return false;
            }
        }

        if(!region.owner)
        {
            error = BulkError{Status::ResourceExhausted,
                              "the receive allocator did not supply usable storage",
                              Origin::Subscriber};
            return false;
        }

        if(region.owner.use_count() > 1)
        {
            error = BulkError{Status::ResourceExhausted,
                              "the receive storage is still held by an earlier session or a "
                              "delivered frame",
                              Origin::Subscriber};
            return false;
        }

        return true;
    }

    bool open_subscription(
        BulkError &error,
        std::chrono::steady_clock::time_point establishment_deadline =
            std::chrono::steady_clock::time_point::max()) noexcept
    {
        if(!prepare_region(error))
        {
            return false;
        }

        std::unique_ptr<detail::SubscriberTransport> fresh;

        try
        {
            fresh = factory(upper_plan, ownership, flow, region, delivery->make_ingress());
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
            error = BulkError{Status::Internal, e.what(), Origin::Subscriber};
            return false;
        }

        if(!fresh)
        {
            delivery->retire_ingress();
            error = BulkError{
                Status::Internal, "the transport factory returned nothing", Origin::Subscriber};
            return false;
        }

        SessionState candidate;

        try
        {
            const std::uint64_t correlation_id = next_correlation_id();
            const std::vector<std::byte> request = candidate.make_open_request(
                stream_name,
                client_label,
                flow,
                upper_plan,
                region.owner ? region.memory_kind : MemoryKind::Host,
                client_id,
                fresh->local_address(),
                correlation_id);
            const std::vector<std::byte> reply =
                channel(Protocol::CoordType::Open, request, establishment_deadline);

            const Status status =
                candidate.adopt_open_reply(
                    reply.data(), reply.size(), upper_plan, correlation_id);
            if(status != Status::Ok)
            {
                delivery->retire_ingress();
                error = BulkError{status,
                                  std::string("BulkOpen was refused: ") + to_string(status),
                                  Origin::Subscriber};
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
            error = BulkError{Status::TransportFailure, e.what(), Origin::Subscriber};
            return false;
        }

        const Geometry &granted = candidate.granted_geometry();

        if(expect.check(granted) != Status::Ok)
        {
            error = BulkError{Status::GeometryMismatch,
                              "the granted geometry does not match the expectation given at "
                              "subscribe",
                              Origin::Subscriber};
            close_candidate(candidate, establishment_deadline);
            delivery->retire_ingress();
            fresh.reset();
            return false;
        }

        if(retired_geometry.generation != 0 &&
           !retired_geometry.describes_same_array(granted))
        {
            geometry_changes.fetch_add(1, std::memory_order_relaxed);
            error = BulkError{Status::GeometryMismatch,
                              "the reopened session describes a different array than the one "
                              "that was retired; the application must open a new subscription "
                              "with the new contract in hand",
                              Origin::Subscriber};

            close_candidate(candidate, establishment_deadline);
            delivery->retire_ingress();
            fresh.reset();
            return false;
        }

        {
            std::lock_guard<std::mutex> lock(observation);
            session = candidate;
            current_geometry = granted;
            actual_plan = ReceivePlan{granted.max_frame_bytes,
                                      granted.ring_depth,
                                      granted.credit_window};
        }

        if(const Status status = fresh->activate(session.stream_id_value(),
                                                 session.granted_geometry(),
                                                 session.server_address_value());
           status != Status::Ok)
        {
            const BulkError reported = fresh->last_error();
            error = reported.status != Status::Ok
                        ? reported
                        : BulkError{status, "the transport could not adopt the grant",
                                    Origin::Subscriber};
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

        // The probe budget is the lease: a session not probed within its own
        // lease is dead by the publisher's definition too.
        const auto probe_deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(lease_budget_ms());
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
                                      Origin::Subscriber};
                }
                break;
            }

            if(std::chrono::steady_clock::now() >= deadline)
            {
                error = BulkError{Status::TransportFailure,
                                  "no Probe arrived within the lease; the publisher cannot "
                                  "reach this client's UCX endpoint",
                                  Origin::Subscriber};
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
                error = BulkError{Status::Internal, "the transport disappeared", Origin::Subscriber};
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
                                        Origin::Subscriber};
                return false;
            }

            const auto now = std::chrono::steady_clock::now();
            if(now < next_renew)
            {
                continue;
            }

            const Status status = renew_once(error);
            if(status == Status::Ok)
            {
                note_renewal(now, next_renew);
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

    void note_renewal(std::chrono::steady_clock::time_point started,
                      std::chrono::steady_clock::time_point scheduled) noexcept
    {
        const auto late = started - scheduled;
        const std::uint64_t late_ms =
            late > std::chrono::steady_clock::duration::zero()
                ? static_cast<std::uint64_t>(
                      std::chrono::duration_cast<std::chrono::milliseconds>(late).count())
                : 0;

        const std::lock_guard<std::mutex> lock(observation);
        last_renewal_steady_ns = steady_now_ns();
        max_renewal_lateness_ms = std::max(max_renewal_lateness_ms, late_ms);
    }

    Status renew_once(BulkError &error) noexcept
    {
        if(!transport)
        {
            error = BulkError{Status::Internal, "the transport disappeared", Origin::Subscriber};
            return Status::Internal;
        }

        Status status = Status::Ok;

        try
        {
            const std::uint64_t correlation_id = next_correlation_id();
            const std::vector<std::byte> request =
                session.make_renew_request(correlation_id);

            renewals_sent.fetch_add(1, std::memory_order_relaxed);
            const std::vector<std::byte> reply =
                channel(Protocol::CoordType::Renew, request, coordination_deadline());
            status = session.adopt_renew_reply(reply.data(), reply.size(), correlation_id).status;

            if(status != Status::Ok)
            {
                error = BulkError{status,
                                  std::string("BulkRenew was refused: ") + to_string(status),
                                  Origin::Subscriber};
            }
        }
        catch(const BulkException &e)
        {
            error = e.error();
            status = e.error().status;
        }
        catch(const std::exception &e)
        {
            error = BulkError{Status::TransportFailure, e.what(), Origin::Subscriber};
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
        return granted == 0 ? k_min_coordination_ms : granted;
    }

    /// The granted lease, else the last one seen, else k_min_coordination_ms.
    std::uint32_t lease_budget_ms() noexcept
    {
        if(session.lease_ttl() != 0)
        {
            return session.lease_ttl();
        }
        return last_lease_ttl_ms != 0 ? last_lease_ttl_ms : k_min_coordination_ms;
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
        return std::chrono::steady_clock::now() + std::chrono::milliseconds(lease_budget_ms());
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
        if(recovery_policy != RecoveryPolicy::Reconnect)
        {
            transition(SubscriberState::Failed, error);
            return false;
        }

        while(running.load(std::memory_order_acquire))
        {
            if(attempt >= k_reconnect_max_attempts)
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
        auto delay = backoff_delay(attempt, last_lease_ttl_ms);
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

    /// Invoke the push callback with one event. False when the callback threw,
    /// which ends delivery.
    bool invoke(FrameEvent event) noexcept
    {
        try
        {
            frame_callback(std::move(event));
            return true;
        }
        catch(...)
        {
            fail_delivery();
            return false;
        }
    }

    void fail_delivery() noexcept
    {
        const BulkError error{Status::Internal, "the frame callback threw", Origin::Subscriber};
        delivery->callback_failed(error);
        transition(SubscriberState::Failed, error);
        running.store(false, std::memory_order_release);
        wake.notify_all();
    }

    /// ADR 0008: one dispatcher drains the authoritative queue and invokes the
    /// callback sequentially. A terminal Session failure follows the frames the
    /// queue accepted and reaches the callback exactly once as the last event;
    /// close, interruption and a callback exception end dispatch silently.
    void dispatch_loop() noexcept
    {
        for(;;)
        {
            const bool active = running.load(std::memory_order_acquire) &&
                                !interrupted.load(std::memory_order_acquire) &&
                                state.load(std::memory_order_acquire) != SubscriberState::Failed;
            const auto deadline =
                std::chrono::steady_clock::now() + (active ? k_dispatch_quantum : 1ms);

            detail::DeliveryRead result = delivery->read_result(deadline);
            switch(result.kind)
            {
            case detail::DeliveryRead::Kind::Frame:
                if(!invoke(FrameEvent{std::move(result.frame), nullptr}))
                {
                    break;
                }
                continue;
            case detail::DeliveryRead::Kind::Empty:
            case detail::DeliveryRead::Kind::Timeout:
                continue;
            case detail::DeliveryRead::Kind::SessionFailed:
                (void)invoke(FrameEvent{FrameView{}, terminal_exception(result)});
                break;
            case detail::DeliveryRead::Kind::Closed:
            case detail::DeliveryRead::Kind::Interrupted:
            case detail::DeliveryRead::Kind::CallbackFailed:
                break;
            }
            break;
        }

        {
            std::lock_guard<std::mutex> lock(shutdown_mutex);
            dispatch_finished = true;
        }
        shutdown_wake.notify_all();
    }

    std::uint64_t next_correlation_id() noexcept
    {
        return correlation.fetch_add(1, std::memory_order_relaxed) + 1;
    }

    void shutdown(bool announce_closed) noexcept
    {
        const auto deadline = std::chrono::steady_clock::now() + k_shutdown_budget;
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

    SubscriberCounters sample_counters() const noexcept
    {
        SubscriberCounters total;

        {
            const std::lock_guard<std::mutex> lock(observation);
            total = retired;
            accumulate(total, live);
        }

        total.reconnects = reconnects.load(std::memory_order_relaxed);
        total.geometry_changes += geometry_changes.load(std::memory_order_relaxed);

        total.renewals_sent = renewals_sent.load(std::memory_order_relaxed);
        total.renewals_failed = renewals_failed.load(std::memory_order_relaxed);

        const detail::DeliveryQueue::Stats queue = delivery->stats();
        total.frames_delivered = queue.taken;
        total.frames_dropped_queue_full = queue.dropped;
        total.delivery_queue_depth = queue.depth;
        total.delivery_queue_high_water = queue.high_water;

        return total;
    }

    std::string stream_name;
    std::string client_label;
    ReceivePlan upper_plan;
    RecoveryPolicy recovery_policy;
    FlowPolicy flow;
    DeliveryOwnership ownership;
    GeometryExpectation expect;
    std::uint32_t establishment_timeout_ms;
    ReceiveAllocator allocator;
    detail::CoordinationChannel channel;
    detail::TransportFactory factory;

    FrameCallback frame_callback;

    SessionState session;

    std::shared_ptr<detail::DeliveryQueue> delivery;

    /// Stable across every Open this Subscription makes, as the wire promises.
    Protocol::ClientInstanceId client_id;

    ReceiveRegion region;
    bool allocator_used{false};

    std::atomic<SubscriberState> state{SubscriberState::Closed};
    std::atomic<bool> running{false};
    std::atomic<bool> interrupted{false};

    std::mutex mutex;
    std::condition_variable wake;

    std::unique_ptr<detail::SubscriberTransport> transport;

    mutable std::mutex observation;
    SubscriberCounters retired{};
    SubscriberCounters live{};
    std::uint32_t last_lease_ttl_ms{0};
    std::uint64_t last_renewal_steady_ns{0};
    std::uint64_t max_renewal_lateness_ms{0};

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

namespace
{

[[noreturn]] void throw_pull_on_push(const char *operation)
{
    throw DeliveryModeError(BulkError{Status::MalformedMessage,
                                      std::string(operation) +
                                          " is a pull operation; this Subscription delivers "
                                          "through its callback",
                                      Origin::Subscriber});
}

} // namespace

std::optional<FrameView> Subscription::try_read()
{
    Impl &impl = *impl_;

    if(impl.frame_callback)
    {
        throw_pull_on_push("try_read()");
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
    std::rethrow_exception(terminal_exception(result));
}

std::optional<FrameView> Subscription::read_for(std::chrono::milliseconds timeout)
{
    Impl &impl = *impl_;

    if(impl.frame_callback)
    {
        throw_pull_on_push("read_for()");
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
    std::rethrow_exception(terminal_exception(result));
}

std::unique_ptr<Subscription> detail::SubscriptionFactory::open(
    SubscriptionOptions options,
    StreamOffer offer,
    detail::CoordinationChannel channel,
    detail::TransportFactory factory,
    std::chrono::steady_clock::time_point establishment_deadline)
{
    const Status status = options.validate();
    if(status != Status::Ok)
    {
        throw ConfigurationError(BulkError{status,
                                           std::string("invalid SubscriptionOptions: ") +
                                               to_string(status),
                                           Origin::Subscriber});
    }

    if(offer.stream_name != options.stream_name)
    {
        throw EstablishmentError(BulkError{Status::UnknownStream,
                                           "discovery returned a different stream",
                                           Origin::Subscriber});
    }
    if(const Status offer_status = offer.validate(); offer_status != Status::Ok)
    {
        throw EstablishmentError(BulkError{offer_status,
                                           offer.message.empty() ? "stream discovery failed"
                                                                 : std::move(offer.message),
                                           Origin::Subscriber});
    }

    const ReceivePlan upper = plan_receive(options, offer);
    if(upper.validate() != Status::Ok)
    {
        throw ResourceExhausted(BulkError{Status::ResourceExhausted,
                                          "the pinned budget cannot hold a valid ReceivePlan",
                                          Origin::Subscriber});
    }

    if(!channel || !factory)
    {
        throw ConfigurationError(BulkError{Status::Internal,
                                           "a Subscription needs both a coordination channel "
                                           "and a transport factory",
                                           Origin::Subscriber});
    }

    auto impl = std::make_shared<Subscription::Impl>(
        std::move(options), upper, std::move(channel), std::move(factory));

    impl->running.store(true, std::memory_order_release);

    impl->transition(SubscriberState::Opening, BulkError{});

    BulkError error;
    if(establishment_deadline == std::chrono::steady_clock::time_point::max())
    {
        establishment_deadline =
            std::chrono::steady_clock::now() +
            std::chrono::milliseconds(impl->establishment_timeout_ms);
    }

    const auto start_threads = [&impl]
    {
        if(impl->frame_callback)
        {
            impl->dispatch = std::thread([worker = impl] { worker->dispatch_loop(); });
        }
        impl->control_started = true;
        impl->control = std::thread([worker = impl] { worker->control_loop(); });
        return std::unique_ptr<Subscription>(new Subscription(impl));
    };

    if(impl->open_subscription(error, establishment_deadline))
    {
        return start_threads();
    }

    if(impl->recovery_policy == RecoveryPolicy::Reconnect)
    {
        for(std::uint32_t attempt = 0; attempt < k_reconnect_max_attempts; ++attempt)
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
                return start_threads();
            }
            if(!Subscription::Impl::retryable_establishment(error.status))
            {
                break;
            }
        }
    }

    impl->transition(SubscriberState::Failed, error);
    impl->shutdown(false);
    std::rethrow_exception(establishment_failure(std::move(error)));
}

int Subscription::fd() const noexcept
{
    return impl_->frame_callback ? -1 : impl_->delivery->fd();
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
    const Impl &impl = *impl_;

    SubscriptionSnapshot out;
    out.state = impl.state.load(std::memory_order_acquire);
    out.interrupted = impl.interrupted.load(std::memory_order_acquire);
    out.counters = impl.sample_counters();
    {
        const std::lock_guard<std::mutex> lock(impl.observation);
        out.error = impl.last_error;
        if(!impl.session.session_id_value().is_zero())
        {
            out.session_id = Protocol::to_log_string(impl.session.session_id_value());
            out.lease_ttl_ms = impl.session.lease_ttl();
            out.renew_interval_ms = impl.session.renew_interval();
        }
        out.last_renewal_steady_ns = impl.last_renewal_steady_ns;
        out.max_renewal_lateness_ms = impl.max_renewal_lateness_ms;
    }
    out.sampled_at_steady_ns = steady_now_ns();
    return out;
}

} // namespace TangoBulk
