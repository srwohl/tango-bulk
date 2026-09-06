// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_TESTS_UNIT_FAKE_TRANSPORT_H
#define TANGO_BULK_TESTS_UNIT_FAKE_TRANSPORT_H

#include <core/delivery_queue.h>

#include <tango-bulk/unstable/session_supervisor.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

/// A transport and a coordination channel that can be told to misbehave.
///
/// This exists for one reason: a real publisher cannot be made to refuse a
/// renewal, grant a session and then never probe, or fail an open four times in
/// a row -- and those are the paths the control loop exists to handle. The
/// integration suite in tests/tango covers the happy paths against a real
/// device server and keeps covering them; nothing here replaces it.
///
/// Everything scripted lives in `Script`, not in the transport, because the
/// supervisor builds a *new* transport per session and a script that died with
/// the transport could not describe a reconnect.
namespace TangoBulkTests
{

using namespace TangoBulk;
using namespace std::chrono_literals;

/// One frame the fake has been told to receive, and the storage behind it.
///
/// The payload is a real allocation held by `shared_ptr` because
/// `FrameView::detached()` keeps that owner alive for as long as any copy of
/// the view does. That is what lets this fake be asked ADR 0003's question --
/// does a frame the application kept outlive the session that delivered it --
/// and not only the delivery question.
struct ScriptedFrame
{
    std::shared_ptr<std::vector<std::uint16_t>> payload;
    FrameView::Fields fields;

    const std::byte *bytes() const noexcept
    {
        return reinterpret_cast<const std::byte *>(payload->data());
    }
};

struct Script
{
    std::mutex mutex;

    /// What each successive `adopt_open_reply` returns. Exhausted entries mean
    /// `Ok` -- a script says what is unusual and stays quiet otherwise.
    std::deque<Status> open_results;

    /// What each successive `adopt_renew_reply` returns.
    std::deque<Status> renew_results;

    /// Whether a granted session ever reaches `Active`. False models a
    /// publisher that answered `Open` but cannot reach this client's endpoint.
    bool probe_arrives{true};

    /// Set to make every grant after the first describe a different array, as a
    /// detector reconfigured between sessions would.
    bool reshape_after_first{false};

    /// Set to make the coordination channel throw, as an unreachable device
    /// does. Counted down; zero means "answer normally".
    int channel_throws{0};

    /// Send a refusal as an `Error` message rather than as a reply carrying a
    /// non-Ok status.
    ///
    /// Both are legal -- a client must accept `Error` in place of any expected
    /// reply -- and they take different paths through the decoder, so which one
    /// a test provokes is a thing to choose rather than to inherit.
    bool refuse_with_error_message{false};

    /// Return bytes that decode as nothing, as a peer that is not this library
    /// -- or a truncating transport -- would. Counted down.
    int garble_replies{0};

    /// What the grant says. The identifiers and lease terms a real publisher
    /// would mint, fixed here so a test can assert on them.
    Protocol::SessionId session_id{{std::byte{0xA1}, std::byte{0xB2}, std::byte{0xC3}}};
    Protocol::StreamId stream_id{0x5EED};
    std::uint32_t lease_ttl_ms{200};
    std::uint32_t renew_interval_ms{20};
    std::vector<std::byte> server_address{std::byte{9}, std::byte{8}, std::byte{7}};

    std::atomic<int> transports_built{0};

    /// How many transports were actually started on a grant.
    ///
    /// Distinct from `transports_built`, and the gap between them is the point:
    /// a transport whose grant the subscription refuses is built and destroyed
    /// without ever being activated, so it never had an endpoint and never
    /// could have queued a frame.
    std::atomic<int> activations{0};
    std::atomic<int> opens{0};
    std::atomic<int> renews{0};
    std::atomic<int> closes{0};

    /// Which threads have called the channel, and whether two were ever inside
    /// it at once. The supervisor promises the calls never overlap -- which is
    /// what the Python binding's GIL story rests on -- so it is asserted rather
    /// than assumed. It does NOT promise a single thread: the first open runs
    /// on the caller's.
    std::set<std::thread::id> channel_threads;
    std::atomic<int> channel_inside{0};
    std::atomic<int> channel_max_concurrent{0};

    /// Queue one renewal outcome on a supervisor that is already running.
    ///
    /// The plain `renew_results = {...}` the other cases use is safe only
    /// because they all write it before `open()`, when no control thread
    /// exists. Making a *live* session fail is a write the control thread races
    /// with, and ThreadSanitizer says so.
    void refuse_next_renew(Status status)
    {
        std::lock_guard<std::mutex> lock(mutex);
        renew_results.push_back(status);
    }

    bool probes()
    {
        std::lock_guard<std::mutex> lock(mutex);
        return probe_arrives;
    }

    int grants_made{0};

    // -- the coordination plane, answered on the wire ------------------------

    /// The `OpenReply` the publisher sends, encoded.
    ///
    /// Real protocol bytes rather than a sentinel, so that everything between
    /// the request and the adopted grant -- the encoder, the envelope, the
    /// decoder, the geometry validation, the downward-clamp check -- is on the
    /// path a unit test exercises. None of it was, while this returned one byte
    /// and the transport consulted a flag.
    std::vector<std::byte> open_reply(std::uint64_t correlation_id)
    {
        std::lock_guard<std::mutex> lock(mutex);

        if(garble_replies > 0)
        {
            --garble_replies;
            return {std::byte{0xDE}, std::byte{0xAD}};
        }

        const Status status = next_status(open_results);
        if(status != Status::Ok && refuse_with_error_message)
        {
            Protocol::ErrorMessage error;
            error.status = status;
            error.message = "the fake refused the open";
            return Protocol::encode(error, correlation_id);
        }

        Protocol::OpenReply reply;
        reply.status = status;
        reply.session_id = session_id;
        reply.stream_id = stream_id;
        reply.lease_ttl_ms = lease_ttl_ms;
        reply.renew_interval_ms = renew_interval_ms;
        reply.geometry = grant_locked();
        reply.server_ucx_address = server_address;
        return Protocol::encode(reply, correlation_id);
    }

    std::vector<std::byte> renew_reply(std::uint64_t correlation_id)
    {
        std::lock_guard<std::mutex> lock(mutex);

        const Status status = next_status(renew_results);
        if(status != Status::Ok && refuse_with_error_message)
        {
            Protocol::ErrorMessage error;
            error.status = status;
            error.message = "the fake refused the renewal";
            return Protocol::encode(error, correlation_id);
        }

        Protocol::RenewReply reply;
        reply.session_id = session_id;
        reply.status = status;

        // 3.7 lets the server change either term at any renewal. Repeating the
        // current ones is what an unchanged lease looks like on the wire, and
        // sending zeros instead would silently reset the client's renew timer
        // to its fallback.
        reply.lease_ttl_ms = lease_ttl_ms;
        reply.renew_interval_ms = renew_interval_ms;
        reply.geometry = last_grant;
        return Protocol::encode(reply, correlation_id);
    }

    std::vector<std::byte> close_reply(std::uint64_t correlation_id)
    {
        std::lock_guard<std::mutex> lock(mutex);

        Protocol::CloseReply reply;
        reply.session_id = session_id;
        reply.status = Status::Ok;
        return Protocol::encode(reply, correlation_id);
    }

    /// The grant, reshaped from the second one onward if asked.
    Protocol::GeometryBlock grant_locked()
    {
        const bool reshaped = reshape_after_first && grants_made > 0;
        ++grants_made;

        Protocol::GeometryBlock granted;
        granted.generation = 1;
        granted.element_type = ElementType::UInt16;
        granted.element_size = 2;
        granted.rank = 2;
        granted.max_frame_bytes = 64u << 10;
        granted.ring_depth = 4;
        granted.credit_window = 2;
        granted.shape = reshaped ? std::array<std::uint64_t, k_max_rank>{4, 16, 0, 0}
                                 : std::array<std::uint64_t, k_max_rank>{8, 8, 0, 0};
        granted.strides = reshaped ? std::array<std::uint64_t, k_max_rank>{32, 2, 0, 0}
                                   : std::array<std::uint64_t, k_max_rank>{16, 2, 0, 0};

        last_grant = granted;
        return granted;
    }

    /// The grant as it stands, so a renewal can echo the current epoch.
    Protocol::GeometryBlock last_grant{};

    static Status next_status(std::deque<Status> &scripted)
    {
        if(scripted.empty())
        {
            return Status::Ok;
        }
        const Status status = scripted.front();
        scripted.pop_front();
        return status;
    }

    // -- the data path -------------------------------------------------------

    /// The subscription's delivery queue, once a transport has been built over
    /// it. The same queue for every session, which is the ownership under test.
    std::shared_ptr<detail::DeliveryQueue> delivery;

    /// Frames sent before any transport existed.
    ///
    /// A test arranges arrivals before it opens a supervisor, and until then
    /// there is nowhere to put them -- the queue is created by the subscription,
    /// which does not exist yet. They go in on the first attach, which is what a
    /// publisher that had frames waiting would produce anyway.
    std::deque<ScriptedFrame> staged;

    /// Called by the factory for every transport it builds.
    void attach(std::shared_ptr<detail::DeliveryQueue> queue)
    {
        std::lock_guard<std::mutex> lock(mutex);
        delivery = std::move(queue);

        for(ScriptedFrame &frame : staged)
        {
            deliver_locked(frame);
        }
        staged.clear();
    }

    /// Hand the transport one more frame to deliver, shaped like the grant.
    ///
    /// 8x8 UInt16, matching the geometry `adopt_open_reply` grants when it has
    /// not been told to reshape, with `sequence` written into every element --
    /// so a test can tell delivered frames apart by their contents and not
    /// merely count them.
    void receive(std::uint64_t sequence)
    {
        ScriptedFrame frame;
        frame.payload = std::make_shared<std::vector<std::uint16_t>>(
            8 * 8, static_cast<std::uint16_t>(sequence));

        frame.fields.element_type = ElementType::UInt16;
        frame.fields.element_size = 2;
        frame.fields.rank = 2;
        frame.fields.shape = {8, 8, 0, 0};
        frame.fields.strides = {16, 2, 0, 0};
        frame.fields.payload_bytes = 8 * 8 * sizeof(std::uint16_t);
        frame.fields.sequence = sequence;
        frame.fields.event_counter = sequence;
        frame.fields.generation = 1;

        std::lock_guard<std::mutex> lock(mutex);
        if(delivery)
        {
            deliver_locked(frame);
        }
        else
        {
            staged.push_back(std::move(frame));
        }
    }

  private:
    /// Push straight into the subscription's queue, exactly as the engine's AM
    /// callback does, and then wake anyone waiting -- which the engine does from
    /// its loop rather than from the callback.
    void deliver_locked(ScriptedFrame &frame)
    {
        delivery->push(FrameView::detached(frame.payload, frame.bytes(), frame.fields));
        delivery->notify();
    }
};

class FakeTransport final : public detail::SubscriberTransport
{
  public:
    /// Takes the subscription's queue and hands it to the script, which is what
    /// gives a test somewhere to send frames from.
    ///
    /// There is no `poll()` here any more, and that is the point: a transport
    /// is given somewhere to put frames rather than asked for them, so nothing
    /// above has to hold it alive across an application callback.
    FakeTransport(Script &script, std::shared_ptr<detail::DeliveryQueue> delivery) :
        script_(script)
    {
        script_.transports_built.fetch_add(1, std::memory_order_relaxed);
        script_.attach(std::move(delivery));
    }

    // -- what is left of the seam --------------------------------------------
    //
    // No codec, no lease terms, no grant. All of that is the subscription's
    // now, so a fake transport has nothing to fake about it: it says where it
    // is, takes a settled contract, and reports what happened.

    const std::vector<std::byte> &local_address() const noexcept override
    {
        return k_client_address;
    }

    Status activate(Protocol::StreamId,
                    const Protocol::GeometryBlock &,
                    const std::vector<std::byte> &) override
    {
        script_.activations.fetch_add(1, std::memory_order_relaxed);

        // A real engine reaches Active when the publisher's Probe is answered.
        // This is the knob a real one does not have.
        state_.store(script_.probes() ? SubscriberState::Active : SubscriberState::Probing,
                     std::memory_order_release);
        return Status::Ok;
    }

    BulkError last_error() const noexcept override
    {
        return failed_ ? BulkError{Status::TransportFailure, "the fake was told to fail", "transport"}
                       : BulkError{};
    }

    SubscriberState state() const noexcept override
    {
        return state_.load(std::memory_order_acquire);
    }

    SubscriberCounters counters() const noexcept override
    {
        SubscriberCounters out;
        out.sessions_opened = 1;
        return out;
    }

    void fail() noexcept
    {
        failed_ = true;
        state_.store(SubscriberState::Failed, std::memory_order_release);
    }

  private:
    /// This client's own UCX address, as far as the coordination plane is
    /// concerned: an opaque blob that goes out in `Open` and comes back to
    /// nobody. There is no data path here to give it meaning.
    static inline const std::vector<std::byte> k_client_address{
        std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};

    Script &script_;
    std::atomic<SubscriberState> state_{SubscriberState::Opening};
    std::atomic<bool> failed_{false};
};

/// A factory over a script, and the last transport it built -- so a test can
/// reach in and fail the live one.
inline detail::TransportFactory fake_factory(Script &script,
                                             std::shared_ptr<FakeTransport *> latest = nullptr)
{
    return [&script, latest](const SubscriberConfig &,
                             std::shared_ptr<detail::DeliveryQueue> delivery)
        -> std::unique_ptr<detail::SubscriberTransport>
    {
        auto transport = std::make_unique<FakeTransport>(script, std::move(delivery));
        if(latest)
        {
            *latest = transport.get();
        }
        return transport;
    };
}

inline detail::CoordinationChannel fake_channel(Script &script)
{
    return [&script](Protocol::CoordType kind,
                     const std::vector<std::byte> &request) -> std::vector<std::byte>
    {
        const int inside = script.channel_inside.fetch_add(1, std::memory_order_acq_rel) + 1;
        int observed = script.channel_max_concurrent.load(std::memory_order_relaxed);
        while(inside > observed &&
              !script.channel_max_concurrent.compare_exchange_weak(observed, inside))
        {
        }
        // Held for the whole body, so an overlap anywhere in it is recorded.
        struct Leave
        {
            Script &script;
            ~Leave()
            {
                script.channel_inside.fetch_sub(1, std::memory_order_acq_rel);
            }
        } leave{script};

        {
            std::lock_guard<std::mutex> lock(script.mutex);
            script.channel_threads.insert(std::this_thread::get_id());
            if(script.channel_throws > 0)
            {
                --script.channel_throws;
                throw BulkException(
                    BulkError{Status::TransportFailure, "the device is unreachable", "tango"});
            }
        }

        // Wide enough that an overlapping caller would be caught.
        std::this_thread::sleep_for(std::chrono::microseconds{200});

        // The request is decoded rather than ignored, and the reply is encoded
        // rather than invented. A device carries opaque bytes and answers with
        // opaque bytes; a channel that answered `{0}` to anything could not tell
        // a correlated reply from an uncorrelated one, and never had to produce
        // a grant that survived validation.
        Protocol::Envelope envelope;
        if(Protocol::decode_envelope(request.data(), request.size(), envelope) != Status::Ok)
        {
            throw BulkException(BulkError{
                Status::MalformedMessage, "the client sent something undecodable", "tango"});
        }

        switch(kind)
        {
        case Protocol::CoordType::Open:
            script.opens.fetch_add(1, std::memory_order_relaxed);
            return script.open_reply(envelope.correlation_id);
        case Protocol::CoordType::Renew:
            script.renews.fetch_add(1, std::memory_order_relaxed);
            return script.renew_reply(envelope.correlation_id);
        case Protocol::CoordType::Close:
            script.closes.fetch_add(1, std::memory_order_relaxed);
            return script.close_reply(envelope.correlation_id);
        default:
            break;
        }

        Protocol::ErrorMessage unexpected;
        unexpected.status = Status::Internal;
        unexpected.message = "the fake was asked for a message it does not serve";
        return Protocol::encode(unexpected, envelope.correlation_id);
    };
}

inline SubscriberConfig supervisor_config()
{
    SubscriberConfig config;
    config.stream_name = "bulk.unit";
    config.max_frame_bytes = 64u << 10;
    config.ring_depth = 4;
    config.credit_window = 2;
    config.delivery_queue_depth = 8;
    config.delivery_mode = DeliveryMode::Manual;
    config.reconnect_backoff_ms = 5;
    config.reconnect_max_attempts = 3;
    config.command_timeout_ms = 500;
    config.probe_timeout_ms = 60;
    return config;
}

inline detail::SessionCallbacks noop_callbacks()
{
    detail::SessionCallbacks callbacks;
    callbacks.on_frame = [](FrameView) {};
    callbacks.on_state = [](SubscriberState, const BulkError &) {};
    return callbacks;
}

template <typename Predicate>
bool eventually(Predicate predicate, std::chrono::milliseconds budget = 4s)
{
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while(std::chrono::steady_clock::now() < deadline)
    {
        if(predicate())
        {
            return true;
        }
        std::this_thread::sleep_for(1ms);
    }
    return predicate();
}

} // namespace TangoBulkTests

#endif // TANGO_BULK_TESTS_UNIT_FAKE_TRANSPORT_H
