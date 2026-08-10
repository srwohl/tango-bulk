// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <ucx/locality.h>
#include <ucx/registered_ring.h>
#include <ucx/ucx_context.h>

#include <core/bounded_queue.h>
#include <core/cpu_topology.h>
#include <core/credit_window.h>
#include <core/lease_pool.h>

#include <tango-bulk/protocol.h>
#include <tango-bulk/publisher.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

/// The publisher: registered producer ring, engine thread, and the session
/// table that owns every client's lease.
///
/// M3 turns the single implicit session of the vertical slice into a bounded
/// table of `max_sessions` (6.1), each with its own credit window, sequence
/// space, endpoint and expiring lease.  A producer slot is now retained until
/// *every* session it was submitted to has credited it (5.4), which is why slot
/// release is refcounted rather than a straight hand-back.
///
/// Still deliberately absent, per MVP_PLAN M5 and later: no geometry re-arm
/// (4.3), no RMA, and no relay.
namespace TangoBulk
{
namespace
{

using detail::allowed_cpu_count;
using detail::bind_thread_to_cpu;
using detail::BoundedQueue;
using detail::CreditWindow;
using detail::Locality;
using detail::observe;
using detail::LeasePool;
using detail::RegisteredRing;
using detail::UcxContext;
using detail::UcxWorker;
using Protocol::SessionState;

/// Blocks for `Lease::Impl`, so `try_acquire()` does not malloc.
///
/// 2.3 fixes `Lease` as a pimpl over `std::unique_ptr<Impl>` and 2.1 forbids the
/// data path from allocating.  A class-level `operator new` is the language's
/// answer to exactly that: the declared type stays what the spec says it is, and
/// the storage comes from somewhere bounded.
///
/// Sized for many rings at the maximum depth; the fallback to the global
/// allocator is a safety net that is counted, not a design assumption.
LeasePool &lease_impl_pool()
{
    static LeasePool pool{4096};
    return pool;
}

std::uint64_t now_realtime_ns() noexcept
{
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

/// Monotonic milliseconds.  Lease deadlines are compared against this and never
/// against the wall clock, so a step in system time cannot expire a live client
/// or resurrect a dead one.
std::uint64_t now_steady_ms() noexcept
{
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
}

/// How long teardown waits on the peer before reclaiming unilaterally.  4.2
/// gives the shape -- bounded wait, then force -- and not the number.
constexpr std::chrono::seconds k_teardown_budget{2};

/// Why a session left the live states.  See `Session::end_reason`.
enum class EndReason : std::uint32_t
{
    None = 0,
    Close = 1,   ///< the client asked (3.8)
    Expire = 2,  ///< the lease deadline passed (4.2)
    Transport = 3 ///< a send failed on an armed session (4.4)
};

/// Counters as relaxed atomics, handed out as a snapshot copy (2.5).
struct AtomicPublisherCounters
{
    std::atomic<std::uint64_t> frames_published{0};
    std::atomic<std::uint64_t> frames_submitted{0};
    std::atomic<std::uint64_t> frames_completed{0};
    std::atomic<std::uint64_t> frames_credited{0};
    std::atomic<std::uint64_t> dropped_no_session{0};
    std::atomic<std::uint64_t> dropped_queue_full{0};
    std::atomic<std::uint64_t> dropped_credit_stalled{0};
    std::atomic<std::uint64_t> dropped_bad_metadata{0};
    std::atomic<std::uint64_t> acquire_failed{0};
    std::atomic<std::uint64_t> leases_retained{0};
    std::atomic<std::uint64_t> credits_outstanding{0};
    std::atomic<std::uint64_t> publish_queue_depth{0};
    std::atomic<std::uint64_t> publish_queue_high_water{0};
    std::atomic<std::uint64_t> sessions_opened{0};
    std::atomic<std::uint64_t> sessions_closed{0};
    std::atomic<std::uint64_t> sessions_expired{0};
    std::atomic<std::uint64_t> sessions_rejected{0};
    std::atomic<std::uint64_t> renewals_accepted{0};
    std::atomic<std::uint64_t> renewals_late{0};
    std::atomic<std::uint64_t> renewals_rejected{0};
    std::atomic<std::uint64_t> malformed_messages{0};
    std::atomic<std::uint64_t> transport_errors{0};
    std::atomic<std::uint64_t> pinned_bytes{0};
};

/// Spin a bounded wait on a UCX request, progressing the worker meanwhile.
void await_request(ucp_worker_h worker, void *request) noexcept
{
    if(!UCS_PTR_IS_PTR(request))
    {
        return;
    }

    const auto deadline = std::chrono::steady_clock::now() + k_teardown_budget;
    while(ucp_request_check_status(request) == UCS_INPROGRESS &&
          std::chrono::steady_clock::now() < deadline)
    {
        ucp_worker_progress(worker);
    }
    ucp_request_free(request);
}

} // namespace

// ---------------------------------------------------------------------------
// BulkSource
// ---------------------------------------------------------------------------

struct BulkSource::Lease::Impl
{
    static void *operator new(std::size_t bytes)
    {
        if(void *p = lease_impl_pool().allocate(bytes))
        {
            return p;
        }
        return ::operator new(bytes);
    }

    static void operator delete(void *p) noexcept
    {
        if(lease_impl_pool().owns(p))
        {
            lease_impl_pool().deallocate(p);
            return;
        }
        ::operator delete(p);
    }

    BulkSource::Impl *owner{nullptr};
    std::size_t index{0};
    std::byte *data{nullptr};
    std::size_t capacity{0};
};

struct BulkSource::Impl
{
    Impl(RegisteredRing &ring_in, AtomicPublisherCounters &counters_in) :
        ring(&ring_in),
        counters(&counters_in),
        free_slots(ring_in.depth())
    {
        for(std::size_t i = 0; i < ring_in.depth(); ++i)
        {
            auto index = static_cast<std::uint32_t>(i);
            free_slots.try_push(std::move(index));
        }
    }

    /// Return a slot to the free list.
    ///
    /// Called from three places, and it matters which: an unpublished lease
    /// being destroyed, a publish() that dropped the frame, and the engine
    /// dropping the last session reference to a slot.  All three are the same
    /// operation, and 5.4 is the rule about *when* each is allowed to happen --
    /// not about doing anything different once it does.
    void release(std::size_t index) noexcept
    {
        auto value = static_cast<std::uint32_t>(index);
        const bool pushed = free_slots.try_push(std::move(value));
        // The free list is exactly ring_depth deep and a slot is in it or in a
        // lease, never both.  A failure here means a slot was released twice,
        // which would go on to corrupt a frame in flight.
        (void) pushed;
        assert(pushed);
        retained.fetch_sub(1, std::memory_order_relaxed);
        counters->leases_retained.store(retained.load(std::memory_order_relaxed),
                                        std::memory_order_relaxed);
    }

    RegisteredRing *ring{nullptr};
    AtomicPublisherCounters *counters{nullptr};
    BoundedQueue<std::uint32_t> free_slots;
    std::atomic<std::uint64_t> retained{0};
};

BulkSource::BulkSource() = default;
BulkSource::~BulkSource() = default;

BulkSource::Lease::Lease() noexcept = default;
BulkSource::Lease::Lease(Lease &&) noexcept = default;
BulkSource::Lease &BulkSource::Lease::operator=(Lease &&other) noexcept
{
    if(this != &other)
    {
        reset();
        impl_ = std::move(other.impl_);
    }
    return *this;
}

BulkSource::Lease::~Lease()
{
    reset();
}

void BulkSource::Lease::reset() noexcept
{
    // 5.4: "A lease that is destroyed without being published returns its slot
    // immediately."  Publishing moves the Impl out, so by the time this runs on
    // a published lease there is nothing left to return.
    if(impl_ && impl_->owner != nullptr)
    {
        impl_->owner->release(impl_->index);
    }
    impl_.reset();
}

BulkSource::Lease::operator bool() const noexcept
{
    return impl_ != nullptr;
}

void *BulkSource::Lease::data() const noexcept
{
    return impl_ ? static_cast<void *>(impl_->data) : nullptr;
}

std::size_t BulkSource::Lease::capacity() const noexcept
{
    return impl_ ? impl_->capacity : 0;
}

std::size_t BulkSource::Lease::index() const noexcept
{
    return impl_ ? impl_->index : 0;
}

MemoryKind BulkSource::Lease::memory_kind() const noexcept
{
    // Host only in the MVP.  Cuda/Rocm are in the protocol so the field does not
    // have to be retrofitted later (spec section 10), not because they work.
    return MemoryKind::Host;
}

BulkSource::Lease BulkSource::try_acquire() noexcept
{
    Lease lease;

    std::uint32_t index = 0;
    if(!impl_->free_slots.try_pop(index))
    {
        // 5.3: never blocks.  An acquisition thread must be able to drop rather
        // than wait while slots are retained by a slow or dead consumer -- a
        // detector does not stop producing because a client stopped reading.
        impl_->counters->acquire_failed.fetch_add(1, std::memory_order_relaxed);
        return lease;
    }

    lease.impl_ = std::unique_ptr<Lease::Impl>(new Lease::Impl);
    lease.impl_->owner = impl_.get();
    lease.impl_->index = index;
    lease.impl_->data = impl_->ring->slot(index);
    lease.impl_->capacity = impl_->ring->slot_bytes();

    const std::uint64_t held = impl_->retained.fetch_add(1, std::memory_order_relaxed) + 1;
    impl_->counters->leases_retained.store(held, std::memory_order_relaxed);

    return lease;
}

std::size_t BulkSource::slot_count() const noexcept
{
    return impl_->ring->depth();
}

std::size_t BulkSource::slot_bytes() const noexcept
{
    return impl_->ring->slot_bytes();
}

std::size_t BulkSource::slot_stride() const noexcept
{
    return impl_->ring->stride();
}

std::size_t BulkSource::retained() const noexcept
{
    return static_cast<std::size_t>(impl_->retained.load(std::memory_order_relaxed));
}

// ---------------------------------------------------------------------------
// BulkPublisher
// ---------------------------------------------------------------------------

struct BulkPublisher::Impl
{
    /// One frame handed from an application thread to the engine.
    ///
    /// It carries a slot index, not a lease: publish() has already taken
    /// ownership by this point.  `ordinal` is the publisher-wide publish count,
    /// which is what admission control reasons in -- sequences are per session
    /// and cannot be compared across them.
    struct PublishItem
    {
        std::size_t slot_index{0};
        std::uint64_t payload_bytes{0};
        std::uint64_t ordinal{0};
        Protocol::FrameHeader header{};
    };

    /// A send in flight to one session: its header context and what it reads.
    ///
    /// Indexed by `sequence % ring_depth` within that session.  At most
    /// `credit_window` sequences are outstanding per session and
    /// `credit_window <= ring_depth`, so the index is unique across everything
    /// that session has in flight.
    struct InFlight
    {
        std::array<std::byte, Protocol::k_frame_header_bytes> header{};
        std::size_t slot_index{0};
        std::uint64_t ordinal{0};
    };

    /// One client's lease over this publisher's stream.
    ///
    /// The split between "control thread under `session_mutex`" and "engine
    /// thread only" is the whole reason this is a struct rather than more loose
    /// members: 4.2's teardown is a sequence of `ucp_*` calls that must run on
    /// the engine, while `Open`/`Renew`/`Close` arrive on Tango command threads.
    /// `state` is the atomic both sides meet at, and the CAS into `Expiring` is
    /// what makes teardown run exactly once however a session ends (4.4).
    struct Session
    {
        Session(Impl &owner_in, std::uint32_t depth, std::uint32_t width) :
            owner(&owner_in),
            window(depth, width),
            inflight(depth)
        {
        }

        Impl *owner{nullptr};

        // Written under session_mutex before `state` leaves `Unknown`, and read
        // by the engine only after it observes a state that is not `Unknown`.
        // That release/acquire pair is what publishes them across the threads.
        Protocol::SessionId id{};
        Protocol::StreamId stream_id{0};
        std::uint64_t probe_token{0};
        Protocol::GeometryBlock geometry{};

        std::atomic<SessionState> state{SessionState::Unknown};
        std::atomic<std::uint64_t> deadline_ms{0};

        /// Why this session ended, and the claim that says who gets to end it.
        ///
        /// 4.4 requires teardown to run exactly once however a session ends, and
        /// three things can end one: a `Close`, a lease deadline, and a
        /// transport error.  The write-once move off `None` is what arbitrates
        /// between them -- a single CAS that both elects the winner and records
        /// the reason, so a loser can never overwrite the winner's answer.  It
        /// also survives teardown, because 3.7 wants a late `Renew` told which
        /// of the two things happened.
        std::atomic<EndReason> end_reason{EndReason::None};
        std::atomic<std::uint64_t> retired_seq{0};

        /// Excluded from reuse: a completion callback may still name this
        /// Session.  See teardown().
        std::atomic<bool> poisoned{false};

        // Renew bookkeeping; control thread only, under session_mutex.
        std::uint64_t rate_window_start_ms{0};
        std::uint32_t renewals_in_window{0};
        std::uint64_t last_activity_ms{0};

        // Engine thread only.
        ucp_ep_h ep{nullptr};
        bool probe_sent{false};
        CreditWindow window;
        std::vector<InFlight> inflight;
        std::uint64_t dropped_for_session{0};
        std::uint64_t processed_ordinal{0};
        std::size_t sends_inflight{0};
    };

    /// Endpoint creation is a `ucp_*` call, so it belongs to the engine thread
    /// (5.1).  `handle_coordination` runs on the caller's thread -- a Tango
    /// command thread in production -- and therefore asks rather than acts.
    struct ConnectTask
    {
        const std::byte *address{nullptr};
        std::size_t address_size{0};
        ucp_ep_h ep{nullptr};
        bool ok{false};
        bool done{false};
    };

    explicit Impl(PublisherConfig cfg) :
        config(std::move(cfg)),
        context(config.ucx_tls),
        worker(std::make_unique<UcxWorker>(context)),
        ring(context,
             config.max_frame_bytes,
             config.ring_depth,
             config.pad_slot_stride,
             config.pinned_memory_limit_bytes),
        publish_queue(config.publish_queue_depth),
        slot_refs(config.ring_depth, 0)
    {
        counters.pinned_bytes.store(ring.mapped_bytes(), std::memory_order_relaxed);

        source.impl_ = std::make_unique<BulkSource::Impl>(ring, counters);

        // 6.1 bounds sessions per publisher, so the table is allocated once here
        // and never grows.  `unique_ptr` because a Session holds atomics and so
        // cannot be relocated by a vector that reallocates.
        sessions.reserve(config.max_sessions);
        for(std::uint32_t i = 0; i < config.max_sessions; ++i)
        {
            sessions.push_back(
                std::make_unique<Session>(*this, config.ring_depth, config.credit_window));
        }

        server_epoch_id = Protocol::generate_server_epoch_id();

        register_am_handlers();

        engine = std::thread([this] { engine_loop(); });
    }

    ~Impl()
    {
        running.store(false, std::memory_order_release);
        if(engine.joinable())
        {
            engine.join();
        }

        // Destroy the worker here, explicitly, while everything its completion
        // callbacks touch is still alive.
        //
        // `ucp_worker_destroy` does not merely drop outstanding sends: it purges
        // them by *invoking their completion callbacks*
        // (`uct_rc_txqp_purge_outstanding`, with `UCS_ERR_CANCELED`).  Ours
        // reaches into the session table and the counters, and members are
        // destroyed in reverse declaration order -- so leaving this to the
        // implicit destructor calls back into a freed `Session`.
        //
        // Only reachable on a transport where a send can still be outstanding at
        // this point.  Over `cma` every send completes synchronously and the
        // purge list is always empty, which is why it took an rc_verbs run to
        // surface.  It is not an rc quirk: it is the ordinary RDMA case.
        worker.reset();
    }

    // -- engine thread ------------------------------------------------------

    void register_am_handlers()
    {
        const auto install = [this](unsigned id, ucp_am_recv_callback_t cb, const char *what)
        {
            ucp_am_handler_param_t param;
            std::memset(&param, 0, sizeof(param));
            param.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID | UCP_AM_HANDLER_PARAM_FIELD_CB |
                               UCP_AM_HANDLER_PARAM_FIELD_ARG;
            param.id = id;
            param.cb = cb;
            param.arg = this;

            const ucs_status_t status = ucp_worker_set_am_recv_handler(worker->get(), &param);
            if(status != UCS_OK)
            {
                detail::throw_ucx_error(what, status, "publisher");
            }
        };

        install(Protocol::k_am_id_credit, &Impl::on_credit_am, "set_am_recv_handler(credit)");
        install(Protocol::k_am_id_probe_ack, &Impl::on_probe_ack_am,
                "set_am_recv_handler(probe_ack)");
    }

    /// 5.1's loop, plus 4.2's expiry sweep.
    ///
    /// One thread that both submits and progresses -- not a submit thread plus a
    /// progress thread.  That is settled by measurement rather than taste:
    /// ucx_perftest saturates with a single submit+progress loop, and the
    /// spike's --progress-thread variant measured ~13 against ~16 GiB/s.
    ///
    /// The sweep lives here rather than on the separate control thread 5.1's
    /// table gives the publisher, because that thread would have had exactly one
    /// job and would still have had to hand every `ucp_*` call in 4.2's teardown
    /// back to this loop.  4.2's actual requirement is that expiry is
    /// "independent of frame traffic", and this loop runs whether or not frames
    /// flow.  Recorded in docs/EXTRACTION.md as a deliberate deviation.
    void engine_loop()
    {
        // 2.3's engine_cpu_affinity, applied here and nowhere else: the thread has
        // to bind itself, and this is the first thing it owns.  -1 means "leave it
        // alone", which is the default because numactl, taskset, systemd's
        // CPUAffinity= and a Slurm cpu-bind may all have placed this process
        // already, and a library that pins itself anyway fights the deployment.
        //
        // A refusal is reported, not fatal.  A restrictive cpuset or a container
        // can legitimately say no, and a device server must not fail to start
        // because it could not optimise itself.
        if(bind_thread_to_cpu(config.engine_cpu_affinity) != Status::Ok)
        {
            std::fprintf(stderr,
                         "tango-bulk: warning: publisher could not pin its engine thread to "
                         "CPU %d (%zu CPUs allowed); running unpinned.\n",
                         config.engine_cpu_affinity,
                         allowed_cpu_count());
        }

        unsigned idle = 0;
        unsigned until_sweep = 0;

        while(running.load(std::memory_order_acquire))
        {
            bool worked = false;

            worked |= drain_connect_requests();
            worked |= send_pending_probes();
            worked |= submit_ready_frames();

            if(until_sweep == 0)
            {
                // A lease deadline is measured in seconds; checking it a few
                // thousand times a second is already three orders of magnitude
                // more often than it can matter, and keeps a clock read off the
                // path a frame takes.
                until_sweep = 256;
                worked |= sweep_sessions();
            }
            --until_sweep;

            if(ucp_worker_progress(worker->get()) != 0)
            {
                worked = true;
            }

            if(worked)
            {
                idle = 0;
            }
            else if(++idle > 64)
            {
                // The design's loop is a busy loop, because at line rate it
                // never has nothing to do.  Yielding after a long idle run
                // keeps an idle publisher from owning a core, and costs
                // nothing once frames are actually flowing.
                idle = 0;
                std::this_thread::yield();
            }
        }

        shutdown_all_sessions();
    }

    bool drain_connect_requests()
    {
        std::vector<ConnectTask *> batch;
        {
            std::lock_guard<std::mutex> lock(connect_mutex);
            if(connect_queue.empty())
            {
                return false;
            }
            batch.swap(connect_queue);
        }

        for(ConnectTask *task : batch)
        {
            try
            {
                task->ep = worker->create_endpoint(task->address, task->address_size);
                task->ok = true;
            }
            catch(const BulkException &)
            {
                task->ok = false;
                counters.transport_errors.fetch_add(1, std::memory_order_relaxed);
            }
        }

        {
            std::lock_guard<std::mutex> lock(connect_mutex);
            for(ConnectTask *task : batch)
            {
                task->done = true;
            }
        }
        connect_done.notify_all();

        return true;
    }

    /// 4.2: a session in `Open` has an endpoint but no proof it reaches anyone.
    ///
    /// Sent once.  A `Probe` that is never answered leaves the session in `Open`
    /// -- never eligible for a frame -- until its lease expires, which is the
    /// same reclaim path a client that died after `Open` takes.  Retrying would
    /// buy nothing that the lease does not already provide.
    bool send_pending_probes()
    {
        bool worked = false;

        for(const std::unique_ptr<Session> &held : sessions)
        {
            Session &session = *held;
            if(session.state.load(std::memory_order_acquire) != SessionState::Open ||
               session.ep == nullptr || session.probe_sent)
            {
                continue;
            }

            Protocol::ProbeMessage probe;
            probe.generation = generation;
            probe.stream_id = session.stream_id;
            probe.probe_token = session.probe_token;

            const std::array<std::byte, Protocol::k_probe_bytes> bytes = Protocol::encode(probe);

            ucp_request_param_t param;
            std::memset(&param, 0, sizeof(param));
            param.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS;
            // 32 bytes: letting UCX copy the header is cheaper than keeping a
            // context alive to completion, and there is nothing to retire after.
            param.flags = UCP_AM_SEND_FLAG_COPY_HEADER;

            void *request = ucp_am_send_nbx(
                session.ep, Protocol::k_am_id_probe, bytes.data(), bytes.size(), nullptr, 0, &param);

            if(UCS_PTR_IS_ERR(request))
            {
                counters.transport_errors.fetch_add(1, std::memory_order_relaxed);
            }
            else if(UCS_PTR_IS_PTR(request))
            {
                ucp_request_free(request);
            }

            session.probe_sent = true;
            worked = true;
        }

        return worked;
    }

    bool submit_ready_frames()
    {
        bool worked = false;

        // Bounded per iteration (5.1 step 1): a burst on the publish queue must
        // not starve ucp_worker_progress(), or completions and credits stop
        // arriving and the burst becomes a stall.
        for(unsigned n = 0; n < 32; ++n)
        {
            PublishItem item;
            if(!publish_queue.try_pop(item))
            {
                break;
            }

            counters.publish_queue_depth.store(publish_queue.size(), std::memory_order_relaxed);
            fan_out(item);
            worked = true;
        }

        return worked;
    }

    /// Submit one frame to every eligible session and account for the slot.
    ///
    /// 5.4: the slot is retained until every session it was submitted to has
    /// credited it, so the reference count -- not any single send -- is what
    /// hands it back.  It starts at one for the duration of this function, so a
    /// credit that arrives while the fan-out is still running cannot drop the
    /// count to zero and recycle a slot the next send is about to read.
    void fan_out(PublishItem &item)
    {
        slot_refs[item.slot_index] = 1;

        for(const std::unique_ptr<Session> &held : sessions)
        {
            Session &session = *held;
            const SessionState state = session.state.load(std::memory_order_acquire);
            if(state != SessionState::Armed && state != SessionState::Active)
            {
                continue;
            }

            session.processed_ordinal = item.ordinal + 1;

            if(!session.window.can_submit())
            {
                // This session alone is behind; the others still get the frame.
                // Its own `dropped_before` records the gap, which is what lets a
                // client tell "I was slow" from "the detector dropped it".
                ++session.dropped_for_session;
                continue;
            }

            ++slot_refs[item.slot_index];
            if(!submit_to(session, item))
            {
                --slot_refs[item.slot_index];
            }
        }

        if(slot_refs[item.slot_index] == 1 && armed_sessions.load(std::memory_order_acquire) == 0)
        {
            // publish() saw an armed session and the last one left before the
            // engine got here.  Counted on this side because publish() already
            // returned Accepted and cannot take it back.
            counters.dropped_no_session.fetch_add(1, std::memory_order_relaxed);
            dropped_before_all.fetch_add(1, std::memory_order_relaxed);
        }

        release_slot_ref(item.slot_index);
        refresh_gauges();
    }

    bool submit_to(Session &session, PublishItem &item)
    {
        // 5.4: sequence assignment and window advancement are atomic with the
        // engine's acceptance of the frame.  A frame that fails to submit is
        // never assigned a sequence, so an uncreditable hole in the window is
        // impossible rather than merely unlikely.
        const std::uint64_t sequence = session.window.next_sequence();
        const auto index = static_cast<std::size_t>(sequence % ring.depth());

        InFlight &slot = session.inflight[index];
        Protocol::FrameHeader header = item.header;
        header.sequence = sequence;
        header.stream_id = session.stream_id;
        header.dropped_before =
            dropped_before_all.load(std::memory_order_relaxed) + session.dropped_for_session;

        slot.header = Protocol::encode(header);
        slot.slot_index = item.slot_index;
        slot.ordinal = item.ordinal;

        ucp_request_param_t param;
        std::memset(&param, 0, sizeof(param));
        param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
        param.cb.send = &Impl::on_send_complete;
        param.user_data = &session;

        void *request = ucp_am_send_nbx(session.ep,
                                        Protocol::k_am_id_frame,
                                        slot.header.data(),
                                        slot.header.size(),
                                        ring.slot(item.slot_index),
                                        static_cast<std::size_t>(item.payload_bytes),
                                        &param);

        if(UCS_PTR_IS_ERR(request))
        {
            // Never assigned a sequence, so nothing to credit and no hole.  4.4:
            // a transport error on an armed session expires it immediately,
            // without waiting for the lease.
            counters.transport_errors.fetch_add(1, std::memory_order_relaxed);
            begin_expiry(session, EndReason::Transport);
            return false;
        }

        session.window.note_submitted();
        counters.frames_submitted.fetch_add(1, std::memory_order_relaxed);

        // 4.2's `Armed → Active` on the first frame, by CAS and not by a store.
        // A `Close` arriving between fan_out()'s eligibility check and here has
        // already moved this session to `Expiring`; storing `Active` over it
        // would resurrect a session whose end has been claimed, and since
        // begin_expiry() can never claim it twice, nothing would ever tear it
        // down.  A failed CAS here means exactly that happened, and is correct.
        SessionState expected = SessionState::Armed;
        session.state.compare_exchange_strong(
            expected, SessionState::Active, std::memory_order_acq_rel);

        if(request == nullptr)
        {
            // Completed inline; no callback will fire, so the header context is
            // free again immediately.
            counters.frames_completed.fetch_add(1, std::memory_order_relaxed);
        }
        else
        {
            // Outstanding until on_send_complete says otherwise.  Counted so
            // teardown knows whether it has collected every completion; a
            // request whose callback never runs is a request never freed.
            ++session.sends_inflight;
        }

        // A non-null request means the send is in flight and on_send_complete
        // will be called with it.  Releasing the handle is that callback's job:
        // freeing it here as well is a double free, and UCX reports it far from
        // its cause -- as an arbiter assertion during endpoint teardown.
        return true;
    }

    /// Drop one session's claim on a producer slot; hand it back at zero.
    void release_slot_ref(std::size_t slot_index) noexcept
    {
        assert(slot_refs[slot_index] > 0);
        if(--slot_refs[slot_index] == 0)
        {
            source.impl_->release(slot_index);
        }
    }

    static void on_send_complete(void *request, ucs_status_t status, void *user_data) noexcept
    {
        auto *session = static_cast<Session *>(user_data);
        Impl *self = session->owner;

        if(status != UCS_OK)
        {
            self->counters.transport_errors.fetch_add(1, std::memory_order_relaxed);
        }

        // 5.4, and this is the load-bearing line of the whole ownership rule:
        // local send completion does NOT release the slot.  It says the local
        // operation finished, not that the peer is done reading, so the release
        // stays credit-driven -- which is what makes the rule hold uniformly
        // across AM, RMA, and the batched-flush variants P0-10 is measuring.
        // Completion frees the header context and nothing else.
        self->counters.frames_completed.fetch_add(1, std::memory_order_relaxed);

        if(request != nullptr)
        {
            --session->sends_inflight;
            ucp_request_free(request);
        }
    }

    /// Find the session a data-plane message belongs to.  Engine thread.
    Session *session_for_stream(Protocol::StreamId stream_id) noexcept
    {
        if(stream_id == 0)
        {
            return nullptr;
        }

        for(const std::unique_ptr<Session> &held : sessions)
        {
            const SessionState state = held->state.load(std::memory_order_acquire);
            const bool live = state == SessionState::Open || state == SessionState::Armed ||
                              state == SessionState::Active;
            if(live && held->stream_id == stream_id)
            {
                return held.get();
            }
        }
        return nullptr;
    }

    static ucs_status_t on_credit_am(void *arg,
                                     const void *header,
                                     std::size_t header_length,
                                     void *data,
                                     std::size_t length,
                                     const ucp_am_recv_param_t *param) noexcept
    {
        (void) data;
        (void) length;
        (void) param;

        auto *self = static_cast<Impl *>(arg);
        self->handle_credit(static_cast<const std::byte *>(header), header_length);
        return UCS_OK;
    }

    void handle_credit(const std::byte *header, std::size_t length) noexcept
    {
        Protocol::CreditMessage credit;
        if(Protocol::decode(header, length, credit) != Status::Ok)
        {
            counters.malformed_messages.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        // 4.4: a Credit for an unknown stream_id is dropped and counted, never
        // treated as anything else.
        Session *session = session_for_stream(credit.stream_id);
        if(session == nullptr)
        {
            counters.malformed_messages.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        const std::uint64_t first = session->window.credited_end();
        const CreditWindow::CreditOutcome outcome = session->window.apply_credit(credit.ack_sequence);

        if(outcome.status != Status::Ok)
        {
            // 3.12: an ack above the highest submitted sequence is a protocol
            // violation.  Refusing it is what stops a buggy or hostile
            // subscriber from talking the publisher into recycling a slot the
            // NIC is still reading.
            counters.malformed_messages.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        for(std::uint64_t seq = first; seq < first + outcome.released; ++seq)
        {
            const InFlight &slot = session->inflight[static_cast<std::size_t>(seq % ring.depth())];
            const std::size_t slot_index = slot.slot_index;
            const bool last = slot_refs[slot_index] == 1;
            release_slot_ref(slot_index);
            if(last)
            {
                // The frame is done everywhere, not merely here.  Counting the
                // last release rather than every one keeps frames_credited
                // comparable with frames_published under fan-out.
                counters.frames_credited.fetch_add(1, std::memory_order_relaxed);
            }
        }

        if(outcome.released != 0)
        {
            refresh_gauges();
        }
    }

    static ucs_status_t on_probe_ack_am(void *arg,
                                        const void *header,
                                        std::size_t header_length,
                                        void *data,
                                        std::size_t length,
                                        const ucp_am_recv_param_t *param) noexcept
    {
        (void) data;
        (void) length;
        (void) param;

        auto *self = static_cast<Impl *>(arg);
        self->handle_probe_ack(static_cast<const std::byte *>(header), header_length);
        return UCS_OK;
    }

    /// 4.2's arm transition: `Open` becomes `Armed` on `ProbeAck`, and not before.
    ///
    /// This is the interlock that stops a frame landing in a receive ring that
    /// is not registered -- and, under the `UCP_ERR_HANDLING_MODE_NONE` that 6.3
    /// mandates, it is the only proof the endpoint reaches anyone at all.
    void handle_probe_ack(const std::byte *header, std::size_t length) noexcept
    {
        Protocol::ProbeAckMessage ack;
        if(Protocol::decode(header, length, ack) != Status::Ok)
        {
            counters.malformed_messages.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        Session *session = session_for_stream(ack.stream_id);
        if(session == nullptr || ack.probe_token != session->probe_token)
        {
            counters.malformed_messages.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        SessionState expected = SessionState::Open;
        if(session->state.compare_exchange_strong(
               expected, SessionState::Armed, std::memory_order_acq_rel))
        {
            // A session that arms mid-stream has no claim on anything published
            // before it existed, so admission control starts it level with the
            // current publish count rather than at zero.
            session->processed_ordinal = admitted.load(std::memory_order_acquire);
            refresh_gauges();

            // Armed is the first moment the endpoint has settled on a transport
            // and device, so it is the earliest this can be asked.  On the engine
            // thread, which is the only thread whose CPU is worth sampling.
            locality = observe(session->ep, ring.memory().base());
            detail::report(locality, "publisher");
        }
    }

    /// Publisher-wide admission headroom, recomputed on the engine.
    ///
    /// `credited` is the highest ordinal any armed session is done with, so
    /// `admitted - credited` on the application thread is "how far ahead of the
    /// fastest consumer this publisher has run".  Taking the max rather than the
    /// min is deliberate: a frame only has to be deliverable to *someone*, so a
    /// session stalled behind a retained view -- or a dead one waiting out its
    /// lease -- must not stop the publisher serving everyone else.  Its own
    /// window still holds its own slots, and 5.4 still holds those until it
    /// credits or expires.
    void refresh_gauges() noexcept
    {
        const std::uint64_t admitted_now = admitted.load(std::memory_order_acquire);

        std::uint64_t best = 0;
        std::uint64_t worst_outstanding = 0;
        std::uint32_t armed = 0;

        for(const std::unique_ptr<Session> &held : sessions)
        {
            Session &session = *held;
            const SessionState state = session.state.load(std::memory_order_acquire);
            if(state != SessionState::Armed && state != SessionState::Active)
            {
                continue;
            }

            ++armed;
            const std::uint64_t retired =
                session.window.outstanding() == 0
                    ? session.processed_ordinal
                    : session
                          .inflight[static_cast<std::size_t>(session.window.credited_end() %
                                                             ring.depth())]
                          .ordinal;
            best = std::max(best, retired);

            // The laggard, not the last one to speak: under fan-out the useful
            // reading is how far behind the slowest consumer has fallen.
            worst_outstanding = std::max(worst_outstanding, session.window.outstanding());
        }

        credited.store(armed != 0 ? best : admitted_now, std::memory_order_release);
        armed_sessions.store(armed, std::memory_order_release);
        counters.credits_outstanding.store(worst_outstanding, std::memory_order_relaxed);
    }

    // -- expiry -------------------------------------------------------------

    /// Move a session towards teardown, from any thread, exactly once.
    ///
    /// 4.4: "Whichever reaches `Expiring` first wins ... teardown runs exactly
    /// once."  Claiming `end_reason` is what elects that winner: it moves off
    /// `None` exactly once, and only the thread that moved it publishes the
    /// `Expiring` state.  Doing it this way round rather than CAS-ing the state
    /// and recording the reason afterwards is what stops a loser from
    /// overwriting the winner's reason in the window before teardown reads it.
    bool begin_expiry(Session &session, EndReason reason) noexcept
    {
        const SessionState state = session.state.load(std::memory_order_acquire);
        if(state != SessionState::Open && state != SessionState::Armed &&
           state != SessionState::Active)
        {
            return false;
        }

        EndReason unclaimed = EndReason::None;
        if(!session.end_reason.compare_exchange_strong(
               unclaimed, reason, std::memory_order_acq_rel))
        {
            return false;
        }

        session.state.store(SessionState::Expiring, std::memory_order_release);
        return true;
    }

    bool sweep_sessions()
    {
        bool worked = false;
        const std::uint64_t now = now_steady_ms();

        for(const std::unique_ptr<Session> &held : sessions)
        {
            Session &session = *held;
            const SessionState state = session.state.load(std::memory_order_acquire);

            if(state == SessionState::Open || state == SessionState::Armed ||
               state == SessionState::Active)
            {
                if(now >= session.deadline_ms.load(std::memory_order_acquire))
                {
                    worked |= begin_expiry(session, EndReason::Expire);
                }
            }

            if(session.state.load(std::memory_order_acquire) == SessionState::Expiring)
            {
                teardown(session);
                worked = true;
            }
        }

        return worked;
    }

    /// 4.2's teardown, in the one order it is allowed to run in.
    ///
    /// Step 4 (release leases) after step 3 (close the endpoint) is the single
    /// most dangerous ordering in the design to get wrong, because handing a
    /// slot back to the producer while the NIC may still be reading it corrupts
    /// a payload silently rather than raising anything.
    void teardown(Session &session)
    {
        // 1. Already ineligible: the CAS into Expiring did that, and fan_out
        //    only ever submits to Armed or Active.

        if(session.ep != nullptr)
        {
            // 2. Let outstanding operations drain, bounded.
            ucp_request_param_t flush_param;
            std::memset(&flush_param, 0, sizeof(flush_param));
            flush_param.op_attr_mask = 0;
            await_request(worker->get(), ucp_ep_flush_nbx(session.ep, &flush_param));

            // 3. Close the endpoint, forcing after the bounded wait.
            ucp_request_param_t close_param;
            std::memset(&close_param, 0, sizeof(close_param));
            close_param.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS;
            close_param.flags = UCP_EP_CLOSE_FLAG_FORCE;
            await_request(worker->get(), ucp_ep_close_nbx(session.ep, &close_param));
            session.ep = nullptr;

            // 3b. Collect the completions the force-close just produced.
            //
            // Forcing the endpoint shut fails every send still on it, but a
            // failed send is still delivered through on_send_complete, and that
            // callback only runs inside progress.  Without this the requests are
            // never freed: UCX reports it at cleanup as "was not returned to
            // mpool ucp_requests" and LeakSanitizer as an indirect leak from
            // ucs_posix_memalign.
            //
            // It sits after the close rather than in step 2 on purpose.  The
            // sends stranded here are exactly the ones the step-2 flush could
            // not drain -- a consumer that stopped returning credit leaves them
            // unsendable -- so waiting on them before forcing would only burn
            // the budget twice.
            const auto deadline = std::chrono::steady_clock::now() + k_teardown_budget;
            while(session.sends_inflight > 0 && std::chrono::steady_clock::now() < deadline)
            {
                ucp_worker_progress(worker->get());
            }
        }

        // 4. Only now may retained slots go back to the producer.
        const std::uint64_t from = session.window.credited_end();
        const std::uint64_t to = session.window.next_sequence();
        for(std::uint64_t seq = from; seq < to; ++seq)
        {
            const InFlight &slot = session.inflight[static_cast<std::size_t>(seq % ring.depth())];
            release_slot_ref(slot.slot_index);
        }

        // 5. No receive-side state on this side of the wire; the producer ring
        //    is per publisher, not per session, and outlives every client.

        // 6. Count it, and retire the identifiers.
        if(session.end_reason.load(std::memory_order_acquire) == EndReason::Close)
        {
            counters.sessions_closed.fetch_add(1, std::memory_order_relaxed);
        }
        else
        {
            counters.sessions_expired.fetch_add(1, std::memory_order_relaxed);
        }

        session.window.reset(0);
        session.probe_sent = false;
        session.dropped_for_session = 0;
        session.processed_ordinal = 0;
        session.retired_seq.store(++retire_counter, std::memory_order_relaxed);

        if(session.sends_inflight != 0)
        {
            // A completion that has not arrived still names this Session.  Reusing
            // the seat would let a new client's session receive it, so the seat is
            // retired for good: one lost out of `max_sessions` beats a callback
            // landing in a stranger's credit window.  Reaching this needs UCX to
            // have abandoned a request across a forced close, which has not been
            // observed.
            session.poisoned.store(true, std::memory_order_relaxed);
        }

        // 4.2: `Closed` is "resources released; session_id retired permanently".
        // The seat keeps its identifier so a late Renew gets an answer that says
        // which of the two things happened (3.7), and is only reused when a new
        // Open has nowhere else to go.
        session.state.store(SessionState::Closed, std::memory_order_release);

        refresh_gauges();
    }

    void shutdown_all_sessions()
    {
        for(const std::unique_ptr<Session> &held : sessions)
        {
            begin_expiry(*held, EndReason::Expire);
            if(held->state.load(std::memory_order_acquire) == SessionState::Expiring)
            {
                teardown(*held);
            }
        }
    }

    // -- control path -------------------------------------------------------

    /// Ask the engine to create an endpoint, and wait for it.
    ///
    /// Blocking is fine here and only here: this is the control path, reached
    /// from a Tango command thread, and 2.1 draws the no-blocking line around
    /// the data path.  It stays synchronous so a client that gave an
    /// unreachable address learns it from `Open` rather than from silence.
    ucp_ep_h connect(const std::byte *address, std::size_t size)
    {
        ConnectTask task;
        task.address = address;
        task.address_size = size;

        {
            std::lock_guard<std::mutex> lock(connect_mutex);
            connect_queue.push_back(&task);
        }

        std::unique_lock<std::mutex> lock(connect_mutex);
        const bool completed = connect_done.wait_for(
            lock, std::chrono::seconds(5), [&task] { return task.done; });

        if(!completed || !task.ok)
        {
            throw BulkException(BulkError{
                Status::TransportFailure, "could not create an endpoint to the client", "publisher"});
        }

        return task.ep;
    }

    /// Locate a session by the identifier a client quoted.  Control thread,
    /// under `session_mutex`.
    Session *session_for_id(const Protocol::SessionId &id) noexcept
    {
        for(const std::unique_ptr<Session> &held : sessions)
        {
            if(held->state.load(std::memory_order_acquire) != SessionState::Unknown &&
               held->id == id)
            {
                return held.get();
            }
        }
        return nullptr;
    }

    std::vector<std::byte> handle_open(const std::byte *data,
                                       std::size_t size,
                                       std::uint64_t correlation_id);
    std::vector<std::byte> handle_renew(const std::byte *data,
                                        std::size_t size,
                                        std::uint64_t correlation_id);
    std::vector<std::byte> handle_close(const std::byte *data,
                                        std::size_t size,
                                        std::uint64_t correlation_id);
    std::vector<std::byte> handle_query(const std::byte *data,
                                        std::size_t size,
                                        std::uint64_t correlation_id);

    /// The geometry this publisher grants, at the current epoch.
    ///
    /// `Open` clamps it downward per client (3.5 step 5); `Query` reports it
    /// unclamped, because a server-wide question has no client to clamp to.
    Protocol::GeometryBlock current_geometry() const noexcept
    {
        Protocol::GeometryBlock geometry;
        geometry.generation = generation;
        geometry.element_type = ElementType::Byte;
        geometry.element_size = 1;
        geometry.rank = 0;
        geometry.max_frame_bytes = config.max_frame_bytes;
        geometry.ring_depth = config.ring_depth;
        geometry.credit_window = config.credit_window;
        return geometry;
    }

    PublisherConfig config;
    UcxContext context;

    /// By pointer so ~Impl can destroy it before any other member.
    std::unique_ptr<UcxWorker> worker;
    RegisteredRing ring;

    BulkSource source;
    BoundedQueue<PublishItem> publish_queue;

    /// How many sessions still owe a credit for each producer slot.  Engine
    /// thread only; the slot goes back to the free list at zero (5.4).
    std::vector<std::uint32_t> slot_refs;

    AtomicPublisherCounters counters;

    // Admission control, read by publish() on application threads.
    //
    // `admitted` counts frames publish() accepted; `credited` is the engine's
    // view of how far the fastest armed session has got.  Their difference is
    // "accepted but not yet released by anyone who could take it", which is what
    // the credit window bounds -- and computing it on the application thread is
    // what lets publish() answer CreditStalled itself instead of discovering it
    // later on the engine.
    std::atomic<std::uint64_t> admitted{0};
    std::atomic<std::uint64_t> credited{0};
    std::atomic<std::uint64_t> dropped_before_all{0};

    /// Sessions a frame could go to.  A gauge rather than a walk of the table,
    /// because publish() reads it once per frame on an application thread.
    std::atomic<std::uint32_t> armed_sessions{0};

    std::atomic<bool> running{true};
    std::thread engine;

    std::mutex connect_mutex;
    std::condition_variable connect_done;
    std::vector<ConnectTask *> connect_queue;

    /// Where this publisher's ring, NIC and engine actually landed.  Engine
    /// thread writes it at arm; read for diagnostics only.
    Locality locality;

    /// Serialises the coordination plane against itself.  Two Tango command
    /// threads may arrive at once; the engine never takes this lock on the frame
    /// path.
    std::mutex session_mutex;
    std::vector<std::unique_ptr<Session>> sessions;
    std::uint64_t retire_counter{0}; ///< engine thread; orders retired seats
    std::uint64_t server_epoch_id{0};
    std::uint32_t generation{1};
};

BulkPublisher::BulkPublisher(PublisherConfig config)
{
    const Status status = config.validate();
    if(status != Status::Ok)
    {
        throw BulkException(
            BulkError{status, std::string("invalid PublisherConfig: ") + to_string(status), "publisher"});
    }

    impl_ = std::make_unique<Impl>(std::move(config));
}

BulkPublisher::~BulkPublisher() = default;
BulkPublisher::BulkPublisher(BulkPublisher &&) noexcept = default;
BulkPublisher &BulkPublisher::operator=(BulkPublisher &&) noexcept = default;

BulkSource &BulkPublisher::source() noexcept
{
    return impl_->source;
}

PublishResult BulkPublisher::publish(BulkSource::Lease &&lease, const FrameMetadata &meta) noexcept
{
    // Every early return below either consumes the lease deliberately or leaves
    // it with the caller; 5.4 spells out which is which, and QueueFull is the
    // only one that gives it back.
    if(!lease)
    {
        return PublishResult::BadMetadata;
    }

    if(!impl_->running.load(std::memory_order_acquire))
    {
        lease.reset();
        return PublishResult::Shutdown;
    }

    FrameMetadata resolved = meta;
    if(resolved.resolve(impl_->config.max_frame_bytes) != Status::Ok ||
       resolved.payload_bytes > lease.capacity())
    {
        impl_->counters.dropped_bad_metadata.fetch_add(1, std::memory_order_relaxed);
        lease.reset();
        return PublishResult::BadMetadata;
    }

    if(impl_->armed_sessions.load(std::memory_order_acquire) == 0)
    {
        // 4.2: arm-before-send.  A frame submitted to a session that is not
        // Armed could land in a receive ring that is not registered yet.
        impl_->counters.dropped_no_session.fetch_add(1, std::memory_order_relaxed);
        impl_->dropped_before_all.fetch_add(1, std::memory_order_relaxed);
        lease.reset();
        return PublishResult::NoSession;
    }

    const std::uint64_t admitted = impl_->admitted.load(std::memory_order_acquire);
    const std::uint64_t credited = impl_->credited.load(std::memory_order_acquire);
    if(admitted - credited >= impl_->config.credit_window)
    {
        impl_->counters.dropped_credit_stalled.fetch_add(1, std::memory_order_relaxed);
        impl_->dropped_before_all.fetch_add(1, std::memory_order_relaxed);
        lease.reset();
        return PublishResult::CreditStalled;
    }

    Impl::PublishItem item;
    item.slot_index = lease.index();
    item.payload_bytes = resolved.payload_bytes;
    item.header.generation = impl_->generation;
    item.header.payload_bytes = resolved.payload_bytes;
    item.header.timestamp_ns =
        resolved.timestamp_ns != 0 ? resolved.timestamp_ns : now_realtime_ns();
    item.header.event_counter = resolved.event_counter;
    item.header.element_type = resolved.element_type;
    item.header.element_size = resolved.element_size;
    item.header.rank = resolved.rank;
    item.header.quality = resolved.quality;
    item.header.memory_kind = resolved.memory_kind;
    item.header.endian = resolved.endian;
    item.header.shape = resolved.shape;
    item.header.strides = resolved.strides;
    item.ordinal = admitted;

    if(!impl_->publish_queue.try_push(std::move(item)))
    {
        // 5.3: "the lease stays with the caller".  Not a courtesy -- a producer
        // that has already filled a slot must be able to retry or drop on its
        // own terms, and taking the slot away would force it to re-acquire and
        // re-fill for a queue that may drain in microseconds.
        impl_->counters.dropped_queue_full.fetch_add(1, std::memory_order_relaxed);
        return PublishResult::QueueFull;
    }

    // Past this point the publisher owns the slot.  Releasing the lease's Impl
    // without returning the slot is what transfers it.
    impl_->admitted.fetch_add(1, std::memory_order_release);
    lease.impl_->owner = nullptr;
    lease.reset();

    const std::size_t depth = impl_->publish_queue.size();
    impl_->counters.publish_queue_depth.store(depth, std::memory_order_relaxed);
    if(depth > impl_->counters.publish_queue_high_water.load(std::memory_order_relaxed))
    {
        impl_->counters.publish_queue_high_water.store(depth, std::memory_order_relaxed);
    }
    impl_->counters.frames_published.fetch_add(1, std::memory_order_relaxed);

    return PublishResult::Accepted;
}

Status BulkPublisher::declare_geometry(const FrameMetadata &, std::uint64_t, std::uint32_t)
{
    // 4.3's interlock is not something to approximate: it has to hold both rings
    // registered, route by generation, and free the old ring only when both the
    // ack has arrived and the old epoch has drained.  Returning a status beats a
    // partial version.  M5 in MVP_PLAN.md is where it lands.
    return Status::Internal;
}

std::uint32_t BulkPublisher::generation() const noexcept
{
    return impl_->generation;
}

std::size_t BulkPublisher::session_count() const noexcept
{
    // Sessions that can be sent to.  A session in `Open` has an endpoint but no
    // ProbeAck, so 4.2 makes it ineligible and it is deliberately not counted.
    return impl_->armed_sessions.load(std::memory_order_acquire);
}

PublisherCounters BulkPublisher::counters() const noexcept
{
    const AtomicPublisherCounters &c = impl_->counters;

    PublisherCounters out;
    out.frames_published = c.frames_published.load(std::memory_order_relaxed);
    out.frames_submitted = c.frames_submitted.load(std::memory_order_relaxed);
    out.frames_completed = c.frames_completed.load(std::memory_order_relaxed);
    out.frames_credited = c.frames_credited.load(std::memory_order_relaxed);
    out.dropped_no_session = c.dropped_no_session.load(std::memory_order_relaxed);
    out.dropped_queue_full = c.dropped_queue_full.load(std::memory_order_relaxed);
    out.dropped_credit_stalled = c.dropped_credit_stalled.load(std::memory_order_relaxed);
    out.dropped_bad_metadata = c.dropped_bad_metadata.load(std::memory_order_relaxed);
    out.acquire_failed = c.acquire_failed.load(std::memory_order_relaxed);
    out.leases_retained = c.leases_retained.load(std::memory_order_relaxed);
    out.credits_outstanding = c.credits_outstanding.load(std::memory_order_relaxed);
    out.publish_queue_depth = c.publish_queue_depth.load(std::memory_order_relaxed);
    out.publish_queue_high_water = c.publish_queue_high_water.load(std::memory_order_relaxed);
    out.sessions_opened = c.sessions_opened.load(std::memory_order_relaxed);
    out.sessions_closed = c.sessions_closed.load(std::memory_order_relaxed);
    out.sessions_expired = c.sessions_expired.load(std::memory_order_relaxed);
    out.sessions_rejected = c.sessions_rejected.load(std::memory_order_relaxed);
    out.renewals_accepted = c.renewals_accepted.load(std::memory_order_relaxed);
    out.renewals_late = c.renewals_late.load(std::memory_order_relaxed);
    out.renewals_rejected = c.renewals_rejected.load(std::memory_order_relaxed);
    out.malformed_messages = c.malformed_messages.load(std::memory_order_relaxed);
    out.transport_errors = c.transport_errors.load(std::memory_order_relaxed);
    out.pinned_bytes = c.pinned_bytes.load(std::memory_order_relaxed);
    return out;
}

std::vector<std::byte> BulkPublisher::handle_coordination(const std::byte *data,
                                                          std::size_t size) noexcept
{
    // The seam that keeps Tango out of the core: encoded bytes in, encoded bytes
    // out.  The Tango adapter (M4) is a DevVarCharArray wrapper over this, and
    // the tests drive the whole session lifecycle through it with no Tango
    // process at all.
    //
    // noexcept, and it means it: this is reached from a Tango command
    // implementation, where an escaping C++ exception is a device server crash
    // rather than a DevFailed.
    try
    {
        Protocol::Envelope envelope;
        if(Protocol::decode_envelope(data, size, envelope) != Status::Ok)
        {
            impl_->counters.malformed_messages.fetch_add(1, std::memory_order_relaxed);
            return Protocol::encode(
                Protocol::ErrorMessage{Status::MalformedMessage, "undecodable envelope"}, 0);
        }

        switch(envelope.msg_type)
        {
        case Protocol::CoordType::Open:
            return impl_->handle_open(data, size, envelope.correlation_id);
        case Protocol::CoordType::Renew:
            return impl_->handle_renew(data, size, envelope.correlation_id);
        case Protocol::CoordType::Close:
            return impl_->handle_close(data, size, envelope.correlation_id);
        case Protocol::CoordType::Query:
            return impl_->handle_query(data, size, envelope.correlation_id);
        default:
            // Everything left is a *reply* type, which a publisher never
            // receives.  Naming it beats a stub that answers Ok and quietly does
            // nothing.
            impl_->counters.malformed_messages.fetch_add(1, std::memory_order_relaxed);
            return Protocol::encode(
                Protocol::ErrorMessage{Status::MalformedMessage,
                                       std::string(Protocol::to_string(envelope.msg_type)) +
                                           " is not a request a publisher answers"},
                envelope.correlation_id);
        }
    }
    catch(const BulkException &e)
    {
        return Protocol::encode(Protocol::ErrorMessage{e.error().status, e.error().message},
                                0);
    }
    catch(const std::exception &e)
    {
        return Protocol::encode(Protocol::ErrorMessage{Status::Internal, e.what()}, 0);
    }
}

std::vector<std::byte> BulkPublisher::Impl::handle_open(const std::byte *data,
                                                        std::size_t size,
                                                        std::uint64_t correlation_id)
{
    Protocol::OpenRequest request;
    if(Protocol::decode(data, size, request) != Status::Ok)
    {
        counters.malformed_messages.fetch_add(1, std::memory_order_relaxed);
        return Protocol::encode(
            Protocol::ErrorMessage{Status::MalformedMessage, "undecodable Open"}, correlation_id);
    }

    // 3.5 step 2: version intersect.
    if(request.version_min > Protocol::k_version_major ||
       request.version_max < Protocol::k_version_major)
    {
        return Protocol::encode(
            Protocol::ErrorMessage{Status::UnsupportedVersion, "no common protocol major"},
            correlation_id);
    }

    // 3.5 step 3: resolve stream_name.
    if(request.stream_name != config.stream_name)
    {
        return Protocol::encode(
            Protocol::ErrorMessage{Status::UnknownStream, "no such stream on this publisher"},
            correlation_id);
    }

    // 3.5 step 5: clamp downward.  A grant is never larger than requested and
    // never larger than configured; clamping is normal and is reported in the
    // reply rather than raised as an error.
    Protocol::GeometryBlock geometry = current_geometry();
    geometry.max_frame_bytes = std::min(request.requested_max_frame_bytes, config.max_frame_bytes);
    geometry.ring_depth = std::min(request.requested_ring_depth, config.ring_depth);
    geometry.credit_window = std::min(request.requested_credit_window, config.credit_window);

    if(geometry.max_frame_bytes == 0 || geometry.ring_depth == 0 || geometry.credit_window == 0)
    {
        return Protocol::encode(
            Protocol::ErrorMessage{Status::MalformedMessage, "Open requested a zero geometry"},
            correlation_id);
    }

    // 3.5 step 4.  4.4: a duplicate Open -- same client_instance_id or not --
    // makes a *new* session and never reuses an existing one, which is why this
    // looks for a free seat rather than for the caller.  A client that leaks
    // sessions hits TooManySessions, and that is correct feedback.
    Session *session = nullptr;
    {
        std::lock_guard<std::mutex> lock(session_mutex);

        // A never-used seat first; otherwise the seat retired longest ago, so a
        // Renew for a session that has just ended still finds its identifier.
        std::uint64_t oldest = 0;
        for(const std::unique_ptr<Session> &held : sessions)
        {
            const SessionState state = held->state.load(std::memory_order_acquire);
            if(state == SessionState::Unknown)
            {
                session = held.get();
                break;
            }

            if(state != SessionState::Closed || held->poisoned.load(std::memory_order_acquire))
            {
                continue;
            }

            const std::uint64_t retired = held->retired_seq.load(std::memory_order_acquire);
            if(session == nullptr || retired < oldest)
            {
                session = held.get();
                oldest = retired;
            }
        }
    }

    if(session == nullptr)
    {
        counters.sessions_rejected.fetch_add(1, std::memory_order_relaxed);
        return Protocol::encode(
            Protocol::ErrorMessage{Status::TooManySessions, "no free session on this publisher"},
            correlation_id);
    }

    // 3.5 step 7.  The endpoint has to exist before a probe can go out, and
    // creating it is a ucp_* call, so it goes to the engine and this thread
    // waits.  Done before the seat is claimed so a failure leaves nothing to
    // unwind.
    ucp_ep_h endpoint =
        connect(request.client_ucx_address.data(), request.client_ucx_address.size());

    const std::uint64_t now = now_steady_ms();

    {
        std::lock_guard<std::mutex> lock(session_mutex);

        session->end_reason.store(EndReason::None, std::memory_order_relaxed);
        session->id = Protocol::generate_session_id();
        session->stream_id = Protocol::generate_stream_id();
        session->probe_token = Protocol::generate_probe_token();
        session->geometry = geometry;
        session->ep = endpoint;
        session->probe_sent = false;
        session->deadline_ms.store(now + config.lease_ttl_ms, std::memory_order_relaxed);
        session->rate_window_start_ms = now;
        session->renewals_in_window = 0;
        session->last_activity_ms = now;

        // Release: everything above must be visible to the engine before it can
        // observe a state other than Unknown and start touching the session.
        session->state.store(SessionState::Open, std::memory_order_release);
    }

    counters.sessions_opened.fetch_add(1, std::memory_order_relaxed);

    Protocol::OpenReply reply;
    reply.version_selected = Protocol::k_version_major;
    reply.status = Status::Ok;
    reply.granted_caps =
        request.requested_caps & (Protocol::k_caps_credit_coalescing | Protocol::k_caps_probe);
    reply.session_id = session->id;
    reply.stream_id = session->stream_id;
    reply.lease_ttl_ms = config.lease_ttl_ms;
    reply.renew_interval_ms = config.renew_interval_ms;
    reply.transport_selected = Protocol::Transport::ActiveMessage;
    reply.server_epoch_id = server_epoch_id;
    reply.geometry = geometry;
    reply.server_ucx_address = worker->address();

    return Protocol::encode(reply, correlation_id);
}

std::vector<std::byte> BulkPublisher::Impl::handle_renew(const std::byte *data,
                                                         std::size_t size,
                                                         std::uint64_t correlation_id)
{
    Protocol::RenewRequest request;
    if(Protocol::decode(data, size, request) != Status::Ok)
    {
        counters.malformed_messages.fetch_add(1, std::memory_order_relaxed);
        return Protocol::encode(
            Protocol::ErrorMessage{Status::MalformedMessage, "undecodable Renew"}, correlation_id);
    }

    Protocol::RenewReply reply;
    reply.session_id = request.session_id;
    reply.lease_ttl_ms = config.lease_ttl_ms;
    reply.renew_interval_ms = config.renew_interval_ms;

    std::lock_guard<std::mutex> lock(session_mutex);

    Session *session = session_for_id(request.session_id);
    if(session == nullptr)
    {
        // 3.7: UnknownSession rather than Error, so a client can tell "you are
        // gone, reopen" from "your message was garbage".
        reply.status = Status::UnknownSession;
        reply.server_state = SessionState::Unknown;
        return Protocol::encode(reply, correlation_id);
    }

    reply.geometry = session->geometry;

    const SessionState state = session->state.load(std::memory_order_acquire);
    if(state == SessionState::Expiring || state == SessionState::Closed)
    {
        // 3.7 splits these two: a session the client closed is `UnknownSession`
        // ("you are gone, reopen"), a lease that ran out is `SessionExpired`.
        // Either way there is no resurrection -- the difference is what the
        // client's log says about why it has to open a new one.
        reply.status =
            (state == SessionState::Closed &&
             session->end_reason.load(std::memory_order_acquire) == EndReason::Close)
                ? Status::UnknownSession
                : Status::SessionExpired;
        reply.server_state = state;
        return Protocol::encode(reply, correlation_id);
    }

    const std::uint64_t now = now_steady_ms();

    if(now - session->rate_window_start_ms >= config.lease_ttl_ms)
    {
        session->rate_window_start_ms = now;
        session->renewals_in_window = 0;
    }

    if(session->renewals_in_window >= config.max_renewals_per_ttl)
    {
        // 3.7: the lease is *not* shortened as a penalty; the reply is simply
        // refused.  Punishing a client for renewing too often would turn a
        // misconfigured renew interval into data loss.
        counters.renewals_rejected.fetch_add(1, std::memory_order_relaxed);
        reply.status = Status::RenewTooFrequent;
        reply.server_state = state;
        return Protocol::encode(reply, correlation_id);
    }

    // 4.4: a late Renew that is still inside the TTL is accepted and counted,
    // not refused -- one slow Tango round trip must not kill a healthy session.
    if(now - session->last_activity_ms > config.renew_interval_ms)
    {
        counters.renewals_late.fetch_add(1, std::memory_order_relaxed);
    }

    ++session->renewals_in_window;
    session->last_activity_ms = now;

    // 4.4: the deadline is set to now + ttl, never extended cumulatively, so a
    // burst of renewals cannot buy a client a longer lease than one TTL.
    session->deadline_ms.store(now + config.lease_ttl_ms, std::memory_order_release);
    counters.renewals_accepted.fetch_add(1, std::memory_order_relaxed);

    reply.status = Status::Ok;
    reply.server_state = state;
    return Protocol::encode(reply, correlation_id);
}

std::vector<std::byte> BulkPublisher::Impl::handle_close(const std::byte *data,
                                                         std::size_t size,
                                                         std::uint64_t correlation_id)
{
    Protocol::CloseRequest request;
    if(Protocol::decode(data, size, request) != Status::Ok)
    {
        counters.malformed_messages.fetch_add(1, std::memory_order_relaxed);
        return Protocol::encode(
            Protocol::ErrorMessage{Status::MalformedMessage, "undecodable Close"}, correlation_id);
    }

    Protocol::CloseReply reply;
    reply.session_id = request.session_id;
    reply.frames_credited_final =
        static_cast<std::uint32_t>(counters.frames_credited.load(std::memory_order_relaxed));

    std::lock_guard<std::mutex> lock(session_mutex);

    Session *session = session_for_id(request.session_id);

    // 4.4: a duplicate Close is UnknownSession, no work performed, and not an
    // error -- a client retrying a close it already made is behaving correctly.
    // The same answer covers a Close that lost the race with expiry, because
    // begin_expiry() has already claimed the session by then.
    if(session == nullptr)
    {
        reply.status = Status::UnknownSession;
        return Protocol::encode(reply, correlation_id);
    }

    if(!begin_expiry(*session, EndReason::Close))
    {
        reply.status = Status::UnknownSession;
        return Protocol::encode(reply, correlation_id);
    }

    // Teardown itself belongs to the engine: steps 2 and 3 of 4.2 are ucp_*
    // calls on the worker.  The reply does not wait for it -- the counters it
    // carries are diagnostics, and 3.8 asks for idempotence, not synchrony.
    reply.status = Status::Ok;
    return Protocol::encode(reply, correlation_id);
}

std::vector<std::byte> BulkPublisher::Impl::handle_query(const std::byte *data,
                                                         std::size_t size,
                                                         std::uint64_t correlation_id)
{
    Protocol::QueryRequest request;
    if(Protocol::decode(data, size, request) != Status::Ok)
    {
        counters.malformed_messages.fetch_add(1, std::memory_order_relaxed);
        return Protocol::encode(
            Protocol::ErrorMessage{Status::MalformedMessage, "undecodable Query"}, correlation_id);
    }

    Protocol::QueryReply reply;
    reply.session_id = request.session_id;
    reply.generation = generation;
    reply.geometry = current_geometry();

    std::string blob;
    const auto add = [&blob](const char *key, std::uint64_t value)
    {
        blob += key;
        blob += '=';
        blob += std::to_string(value);
        blob += ';';
    };
    const auto add_text = [&blob](const char *key, const std::string &value)
    {
        blob += key;
        blob += '=';
        blob += value;
        blob += ';';
    };

    // 3.8: the blob MUST NOT carry UCX addresses, memory keys, untruncated
    // session identifiers, or hostnames the caller does not already know.  Every
    // value below is either a counter, a configured bound, or an identifier in
    // the truncated form 3.2 mandates for logs.  The stream name is safe
    // unescaped because 3.9 restricts it to [A-Za-z0-9_.-].
    add_text("stream", config.stream_name);
    add_text("transport", "am");
    add_text("memory_kind", "host");

    const AtomicPublisherCounters &c = counters;
    add("frames_published", c.frames_published.load(std::memory_order_relaxed));
    add("frames_submitted", c.frames_submitted.load(std::memory_order_relaxed));
    add("frames_completed", c.frames_completed.load(std::memory_order_relaxed));
    add("frames_credited", c.frames_credited.load(std::memory_order_relaxed));
    add("dropped_no_session", c.dropped_no_session.load(std::memory_order_relaxed));
    add("dropped_queue_full", c.dropped_queue_full.load(std::memory_order_relaxed));
    add("dropped_credit_stalled", c.dropped_credit_stalled.load(std::memory_order_relaxed));
    add("dropped_bad_metadata", c.dropped_bad_metadata.load(std::memory_order_relaxed));
    add("acquire_failed", c.acquire_failed.load(std::memory_order_relaxed));
    add("leases_retained", c.leases_retained.load(std::memory_order_relaxed));
    add("credits_outstanding", c.credits_outstanding.load(std::memory_order_relaxed));
    add("publish_queue_depth", c.publish_queue_depth.load(std::memory_order_relaxed));
    add("publish_queue_high_water", c.publish_queue_high_water.load(std::memory_order_relaxed));
    add("sessions_opened", c.sessions_opened.load(std::memory_order_relaxed));
    add("sessions_closed", c.sessions_closed.load(std::memory_order_relaxed));
    add("sessions_expired", c.sessions_expired.load(std::memory_order_relaxed));
    add("sessions_rejected", c.sessions_rejected.load(std::memory_order_relaxed));
    add("renewals_accepted", c.renewals_accepted.load(std::memory_order_relaxed));
    add("renewals_late", c.renewals_late.load(std::memory_order_relaxed));
    add("renewals_rejected", c.renewals_rejected.load(std::memory_order_relaxed));
    add("malformed_messages", c.malformed_messages.load(std::memory_order_relaxed));
    add("transport_errors", c.transport_errors.load(std::memory_order_relaxed));
    add("pinned_bytes", c.pinned_bytes.load(std::memory_order_relaxed));
    add("max_sessions", config.max_sessions);
    add("lease_ttl_ms", config.lease_ttl_ms);
    add("renew_interval_ms", config.renew_interval_ms);

    const std::uint64_t now = now_steady_ms();

    {
        std::lock_guard<std::mutex> lock(session_mutex);

        std::uint32_t live = 0;
        for(const std::unique_ptr<Session> &held : sessions)
        {
            const SessionState state = held->state.load(std::memory_order_acquire);
            if(state != SessionState::Unknown && state != SessionState::Closed)
            {
                ++live;
            }
        }
        reply.active_sessions = live;
        add("sessions_live", live);
        add("sessions_armed", armed_sessions.load(std::memory_order_relaxed));

        // 3.8 allows an all-zero session_id to ask for server-wide status.  A
        // quoted identifier asks about one session, and an identifier this
        // publisher does not hold is `UnknownSession` -- the same answer 3.7
        // gives a `Renew`, so an operator's Query and a client's Renew cannot
        // disagree about whether a session still exists.
        if(!request.session_id.is_zero())
        {
            const Session *session = session_for_id(request.session_id);
            if(session == nullptr)
            {
                reply.status = Status::UnknownSession;
            }
            else
            {
                const SessionState state = session->state.load(std::memory_order_acquire);
                const std::uint64_t deadline = session->deadline_ms.load(std::memory_order_relaxed);

                add_text("session", Protocol::to_log_string(session->id));
                add_text("session_state", Protocol::to_string(state));
                add("session_lease_ms_remaining", deadline > now ? deadline - now : 0);
                add("session_geometry_generation", session->geometry.generation);
            }
        }
    }

    reply.counters = std::move(blob);
    return Protocol::encode(reply, correlation_id);
}

} // namespace TangoBulk
