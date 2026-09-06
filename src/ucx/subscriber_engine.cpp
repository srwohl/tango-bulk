// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <ucx/subscriber_engine.h>


#include <ucx/locality.h>

#include <core/cpu_topology.h>

#include <cassert>
#include <cstdio>
#include <chrono>
#include <cstring>
#include <mutex>
#include <utility>

namespace TangoBulk::detail
{
namespace
{

/// How long teardown waits on the peer before reclaiming unilaterally.
///
/// Long enough that a rendezvous transfer already on the wire lands, short
/// enough that a publisher which died mid-frame cannot hold a subscriber's
/// destructor open.  4.2 gives the publisher the same bounded-wait shape for the
/// same reason.
constexpr std::chrono::milliseconds k_quiesce_budget{2000};

/// Park a worker that could not be quiesced, for the life of the process.
///
/// `ucp_worker_destroy` on a worker whose endpoints still hold queued operations
/// is not an error code, it is `ucs_fatal_error` -- the process aborts inside
/// UCX.  A consumer whose publisher was killed mid-rendezvous must not be able to
/// take a device server down on its way out, so when the bounded teardown in
/// `quiesce()` cannot prove the worker is safe to destroy, it is deliberately
/// leaked instead.
///
/// The context comes along because unmapping the ring needs it, and because
/// `ucp_cleanup` on a context with a live worker is the same class of mistake.
///
/// So does the arena.  The receives that could not be drained are rendezvous
/// gets -- RDMA reads landing in ring slots, which the NIC completes without
/// asking the CPU.  Nothing progresses a quarantined worker again so no callback
/// can fire, but `ucp_mem_unmap` and a free underneath a live RDMA read needs no
/// callback to corrupt whatever takes those pages next, and the arena would
/// otherwise go as soon as the last `FrameView` did.
///
/// The holder is heap-allocated and never freed: a static with a destructor would
/// simply move the abort to exit time.
void quarantine(std::unique_ptr<UcxWorker> worker,
                std::shared_ptr<UcxContext> context,
                std::shared_ptr<ReceiveArena> arena)
{
    struct Held
    {
        std::unique_ptr<UcxWorker> worker;
        std::shared_ptr<UcxContext> context;
        std::shared_ptr<ReceiveArena> arena;
    };

    static std::mutex mutex;
    static std::vector<Held> *held = new std::vector<Held>();

    const std::lock_guard<std::mutex> lock(mutex);
    held->push_back(Held{std::move(worker), std::move(context), std::move(arena)});
}

} // namespace

// ---------------------------------------------------------------------------
// ReceiveArena
// ---------------------------------------------------------------------------

ReceiveArena::ReceiveArena(std::shared_ptr<UcxContext> context,
                           std::uint64_t slot_bytes,
                           std::uint32_t depth,
                           std::uint64_t pinned_limit,
                           std::shared_ptr<void> receive_buffer,
                           std::uint64_t receive_buffer_bytes,
                           MemoryKind memory_kind) :
    context_(std::move(context)),
    // The receive ring never pads its stride: 6.2's extra page exists to break
    // cache-set aliasing on the *copy* the receiver would otherwise do, and this
    // path lands the payload in the slot directly.  It is the publisher's ring
    // that pays that tax.
    ring_(receive_buffer ? RegisteredRing(*context_,
                                          slot_bytes,
                                          depth,
                                          std::move(receive_buffer),
                                          receive_buffer_bytes,
                                          memory_kind)
                         : RegisteredRing(*context_, slot_bytes, depth, false, pinned_limit)),
    slots_(depth),
    credit_returns_(static_cast<std::size_t>(depth) * 2),
    lease_pool_(std::make_shared<LeasePool>(static_cast<std::size_t>(depth) + 8))
{
    for(std::uint32_t i = 0; i < depth; ++i)
    {
        slots_[i].data = ring_.slot(i);
        slots_[i].capacity = ring_.slot_bytes();
    }
}

void ReceiveArena::return_credit(std::uint64_t sequence) noexcept
{
    credits_returned_.fetch_add(1, std::memory_order_relaxed);
    views_outstanding_.fetch_sub(1, std::memory_order_relaxed);

    const bool pushed = credit_returns_.try_push(std::move(sequence));

    // 5.3: this queue "cannot fill".  At most ring_depth credits are outstanding
    // and the capacity is 2 x ring_depth, so an overflow means the invariant is
    // broken -- and the spec requires an assert rather than handling, because a
    // full credit queue deadlocks the window and a dropped credit is
    // unrecoverable.  Failing loudly beats stalling silently.
    (void) pushed;
    assert(pushed && "credit-return queue overflow: more outstanding credits than ring slots");
}

// ---------------------------------------------------------------------------
// SubscriberEngine
// ---------------------------------------------------------------------------

/// One in-progress rendezvous receive.
///
/// The completion callback needs to know which slot it is finishing, and UCX
/// gives it one `void *`.  One of these per slot, reused, so nothing is
/// allocated to receive a frame.
struct SubscriberEngine::Pending
{
    SubscriberEngine *self{nullptr};
    std::size_t slot_index{0};

    /// The in-flight `ucp_am_recv_data_nbx` request for this slot, or null.
    ///
    /// Kept so teardown has something to cancel.  A rendezvous receive is a
    /// *get* the receiver issues against the sender's buffer, so it sits queued
    /// on a UCX-internal endpoint that this class never names and cannot close;
    /// dropping the handle would leave no way to reclaim it.  Engine thread
    /// only: it is written in the AM handler and cleared in the completion
    /// callback, both of which run inside `ucp_worker_progress`.
    void *request{nullptr};
};

SubscriberEngine::SubscriberEngine(SubscriberConfig config,
                                   std::shared_ptr<DeliveryQueue> delivery) :
    config_(std::move(config)),
    tracker_(config_.ring_depth),
    delivery_(std::move(delivery)),
    pending_(config_.ring_depth)
{
    const Status status = config_.validate();
    if(status != Status::Ok)
    {
        throw BulkException(BulkError{
            status, std::string("invalid SubscriberConfig: ") + to_string(status), "subscriber"});
    }

    // Checked here rather than at the first push, because the first push is
    // inside `ucp_worker_progress` on the engine thread, where there is nothing
    // useful to do about it.
    if(!delivery_)
    {
        throw BulkException(BulkError{
            Status::Internal, "a subscriber transport needs a delivery queue", "subscriber"});
    }

    // `delivery_mode` is deliberately not inspected here.  This class receives
    // into the queue it was given and nothing else; whether a library-owned
    // thread or the application takes frames out of it is the subscription's
    // business, one layer up, which is also the layer that owns the
    // `std::function` 5.2 keeps away from the engine.  A check here could only
    // refuse a mode this class has no opinion about.

    context_ = std::make_shared<UcxContext>(config_.ucx_tls);
    worker_ = std::make_unique<UcxWorker>(*context_);

    // 4.1, `Opening` entry action, in this order: the ring is allocated,
    // registered and armed *before* Open goes out, because the publisher may
    // send the first frame the moment it replies.
    arena_ = std::make_shared<ReceiveArena>(context_,
                                            config_.max_frame_bytes,
                                            config_.ring_depth,
                                            config_.pinned_memory_limit_bytes,
                                            config_.receive_buffer,
                                            config_.receive_buffer_bytes,
                                            config_.receive_memory_kind);
    sink_ = arena_;

    for(std::size_t i = 0; i < pending_.size(); ++i)
    {
        pending_[i].self = this;
        pending_[i].slot_index = i;
    }

    register_am_handlers();

    // The engine thread starts in adopt_open_reply(), not here.
    //
    // 5.1 gives the worker to exactly one thread, and the endpoint to the server
    // is created from the address in the `OpenReply` -- on whatever thread ran
    // the Tango command.  Starting the engine only once that endpoint exists is
    // what keeps `ucp_ep_create` from racing `ucp_worker_progress` on a
    // UCS_THREAD_MODE_SINGLE worker, and it costs nothing: before the session
    // opens there is no endpoint, no credit and no frame, so the loop would have
    // had nothing to progress.
    state_.store(SubscriberState::Opening, std::memory_order_release);
}

SubscriberEngine::~SubscriberEngine()
{
    running_.store(false, std::memory_order_release);

    // The delivery queue is deliberately NOT stopped here. It belongs to the
    // subscription, and a reconnect destroys this engine to build another over
    // the same queue -- stopping it would end delivery for the subscription on
    // the first lost session. Whoever owns the queue stops it when the
    // subscription itself ends.

    if(engine_.joinable())
    {
        engine_.join();
    }
    else
    {
        // Never opened, so the engine never ran and nobody has quiesced the
        // worker.  This thread is the only one that could touch it, so it is
        // safe to do here -- and skipping it would quarantine a worker on every
        // failed open.
        quiesced_.store(quiesce(), std::memory_order_release);
    }
    state_.store(SubscriberState::Closed, std::memory_order_release);

    // The engine thread ran quiesce() on its way out.  If it could not bring the
    // worker to a destroyable state within its budget -- a peer that died
    // mid-rendezvous is the case that does this -- then destroying the worker
    // here would abort the process from inside UCX.  Leak it instead: one
    // stranded worker per unrecoverable teardown is a far better outcome for a
    // device server than a fatal error raised by a client's disappearance.
    if(!quiesced_.load(std::memory_order_acquire))
    {
        quarantine(std::move(worker_), context_, arena_);
    }

    // Anything still in the delivery queue, and anything the application is
    // still holding, keeps the arena alive past this point through its lease.
    // That is what makes teardown with views outstanding safe rather than a
    // race: the ring is unmapped by the last lease, not by this destructor.
}

void SubscriberEngine::register_am_handlers()
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

        const ucs_status_t status = ucp_worker_set_am_recv_handler(worker_->get(), &param);
        if(status != UCS_OK)
        {
            throw_ucx_error(what, status, "subscriber");
        }
    };

    install(Protocol::k_am_id_frame, &SubscriberEngine::on_frame_am,
            "ucp_worker_set_am_recv_handler(frame)");
    install(Protocol::k_am_id_probe, &SubscriberEngine::on_probe_am,
            "ucp_worker_set_am_recv_handler(probe)");
}

// -- control plane ----------------------------------------------------------
//
// What is left here is the transport's share of it: the worker address to put
// in the request, the endpoint to create from the reply, and the state this
// class publishes to the layer above.  `SessionClient` owns the rest, and owns
// it in `core/` where it can be tested without a NIC.

std::vector<std::byte> SubscriberEngine::make_open_request(std::uint64_t correlation_id) const
{
    return session_.make_open_request(config_, worker_->address(), correlation_id);
}

Status SubscriberEngine::adopt_open_reply(const std::byte *data, std::size_t size)
{
    const Status status = session_.adopt_open_reply(data, size, config_);
    if(status != Status::Ok)
    {
        fail(status, "the publisher refused the Open, or its grant was unusable");
        return status;
    }

    tracker_.reset(0);

    // Where this session's delivery count starts. The queue spans every session
    // the subscription has had; `Renew` reports the progress of this one.
    // Written before the engine thread exists, which is what publishes it.
    delivered_at_open_ = delivery_->taken();

    // Still the only thread touching this worker: the engine has not started.
    try
    {
        endpoint_ = worker_->create_endpoint(session_.server_address().data(),
                                             session_.server_address().size());
    }
    catch(const BulkException &)
    {
        fail(Status::TransportFailure,
             "could not create a UCX endpoint to the address the publisher returned");
        return Status::TransportFailure;
    }

    // 4.1: `Probing` until the publisher's `Probe` is answered.  The ring is
    // already registered and armed -- that happened in the constructor, before
    // `Open` went out -- so what the probe adds is proof that this endpoint
    // reaches anyone, which under the UCP_ERR_HANDLING_MODE_NONE that 6.3
    // mandates nothing else on this side can tell us.
    state_.store(SubscriberState::Probing, std::memory_order_release);
    engine_ = std::thread([this] { engine_loop(); });
    return Status::Ok;
}

std::vector<std::byte> SubscriberEngine::make_renew_request(std::uint64_t correlation_id)
{
    renewals_sent_.fetch_add(1, std::memory_order_relaxed);
    return session_.make_renew_request(
        correlation_id,
        delivery_->taken() - delivered_at_open_,
        arena_ ? arena_->credits_returned() : 0,
        static_cast<std::uint32_t>(state_.load(std::memory_order_acquire)));
}

Status SubscriberEngine::adopt_renew_reply(const std::byte *data, std::size_t size)
{
    const SessionClient::RenewOutcome outcome = session_.adopt_renew_reply(data, size);

    if(outcome.status != Status::Ok)
    {
        renewals_failed_.fetch_add(1, std::memory_order_relaxed);
    }
    if(outcome.session_lost)
    {
        fail(outcome.status, "the publisher no longer recognises this session");
    }

    return outcome.status;
}

std::vector<std::byte> SubscriberEngine::make_close_request(std::uint64_t correlation_id) const
{
    return session_.make_close_request(correlation_id);
}

// -- engine thread ----------------------------------------------------------

void SubscriberEngine::engine_loop()
{
    // 2.4's engine_cpu_affinity.  Same contract as the publisher's: -1 leaves the
    // thread alone so a launcher-level placement is never fought, and a refusal is
    // reported rather than fatal.
    if(bind_thread_to_cpu(config_.engine_cpu_affinity) != Status::Ok)
    {
        std::fprintf(stderr,
                     "tango-bulk: warning: subscriber could not pin its engine thread to "
                     "CPU %d (%zu CPUs allowed); running unpinned.\n",
                     config_.engine_cpu_affinity,
                     allowed_cpu_count());
    }

    unsigned idle = 0;

    while(running_.load(std::memory_order_acquire))
    {
        bool worked = send_pending_probe_ack();
        worked |= drain_credit_returns();
        worked |= send_pending_credit();

        if(ucp_worker_progress(worker_->get()) != 0)
        {
            worked = true;
        }

        // Out of the callback and into the loop, the same way credit and
        // probe-acks are routed: a write() belongs nowhere near the inside of
        // ucp_worker_progress.
        delivery_->notify();

        if(worked)
        {
            idle = 0;
        }
        else if(++idle > 64)
        {
            idle = 0;
            std::this_thread::yield();
        }
    }

    quiesced_.store(quiesce(), std::memory_order_release);
}

bool SubscriberEngine::drain_inflight_receives() noexcept
{
    const auto deadline = std::chrono::steady_clock::now() + k_quiesce_budget;
    while(rndv_inflight_ > 0 && std::chrono::steady_clock::now() < deadline)
    {
        ucp_worker_progress(worker_->get());
    }

    return rndv_inflight_ == 0;
}

bool SubscriberEngine::quiesce() noexcept
{
    // The publisher's mirror of this is 4.2's teardown, whose order is fixed.
    // There is no equivalent order written down for the consumer, but it has the
    // same obligation and one extra difficulty: its outstanding work is
    // *receives*, which sit on an endpoint UCX created internally for the
    // rendezvous get and which this class can neither name nor close.
    //
    // The whole point is that none of this may depend on the peer.  A device
    // server restarts, a client is killed, a session is closed with frames still
    // moving -- in each case the far side stops answering mid-transfer, and
    // teardown here still has to end at a worker that is safe to destroy.

    // 0. Stop accepting new frames, before anything progresses the worker.
    //
    //    Every step below drains by calling ucp_worker_progress, and progress is
    //    what runs the AM handler.  Without this the drain competes with a
    //    publisher that is still sending -- each pass retires some receives and
    //    starts others, `rndv_inflight_` never reaches zero, and a subscriber
    //    closing under load quarantines a healthy worker.
    closing_ = true;

    // 1. Bounded wait for outstanding receives to land on their own.  When the
    //    peer is merely slower than we are, every transfer finishes here and the
    //    rest of this function has nothing left to do.
    drain_inflight_receives();

    // 2. Best-effort reclaim of whatever the peer never finished.  6.3 mandates
    //    UCP_ERR_HANDLING_MODE_NONE -- the spike traced the "10x problem" to
    //    PEER forcing a tcp/lo fallback -- so a publisher that vanished
    //    mid-rendezvous produces no error completion and no timeout, and step 1
    //    will have spent its whole budget for nothing.
    //
    //    Best-effort is meant literally.  ucp_request_cancel promises completion
    //    "regardless of the status of the target endpoint", which is exactly the
    //    property wanted here, but UCX 1.22 documents that promise only for send
    //    and tag-receive requests; it says nothing about an AM receive, and this
    //    path is not reachable from the M2 suite, so it is unverified.  It is
    //    kept because it costs one call and step 4 is what has to hold anyway.
    for(Pending &pending : pending_)
    {
        if(pending.request != nullptr)
        {
            ucp_request_cancel(worker_->get(), pending.request);
        }
    }

    // A cancellation is still delivered through on_rndv_complete, which only
    // runs inside progress -- so it needs a second drain to be observed.
    drain_inflight_receives();

    // 3. Close our own endpoint, bounded.
    //
    //    Deliberately *not* UCP_EP_CLOSE_FLAG_FORCE, which ucp.h says "requires
    //    set UCP_ERR_HANDLING_MODE_PEER for all endpoints created on both (local
    //    and remote) sides to avoid undefined behavior".  6.3 mandates NONE, and
    //    the behaviour force leaves undefined is the publisher's, not ours: a
    //    client's shutdown must not be able to fault a device server.
    //
    //    Without the flag the close flushes outstanding operations instead, so a
    //    silent peer leaves it in progress.  That is what the budget is for --
    //    ucp_request_free releases a request "regardless of its current state",
    //    after which no callback fires and the close still finishes internally.
    if(endpoint_ != nullptr)
    {
        ucp_request_param_t param;
        std::memset(&param, 0, sizeof(param));
        param.op_attr_mask = 0;

        void *close = ucp_ep_close_nbx(endpoint_, &param);
        if(UCS_PTR_IS_PTR(close))
        {
            const auto deadline = std::chrono::steady_clock::now() + k_quiesce_budget;
            while(ucp_request_check_status(close) == UCS_INPROGRESS &&
                  std::chrono::steady_clock::now() < deadline)
            {
                ucp_worker_progress(worker_->get());
            }
            ucp_request_free(close);
        }
        endpoint_ = nullptr;
    }

    // 4. Flush the worker.  Closing endpoint_ says nothing about the internal
    //    endpoint the rendezvous get ran on; this is what drains that one, and it
    //    is the last thing between here and ucp_worker_destroy.
    ucp_request_param_t flush_param;
    std::memset(&flush_param, 0, sizeof(flush_param));
    flush_param.op_attr_mask = 0;

    bool flushed = true;
    void *flush = ucp_worker_flush_nbx(worker_->get(), &flush_param);
    if(UCS_PTR_IS_ERR(flush))
    {
        flushed = false;
    }
    else if(UCS_PTR_IS_PTR(flush))
    {
        const auto deadline = std::chrono::steady_clock::now() + k_quiesce_budget;
        while(ucp_request_check_status(flush) == UCS_INPROGRESS &&
              std::chrono::steady_clock::now() < deadline)
        {
            ucp_worker_progress(worker_->get());
        }
        flushed = ucp_request_check_status(flush) != UCS_INPROGRESS;
        ucp_request_free(flush);
    }

    return flushed && rndv_inflight_ == 0;
}

bool SubscriberEngine::drain_credit_returns()
{
    bool worked = false;

    std::uint64_t sequence = 0;
    while(arena_->try_pop_credit(sequence))
    {
        // The slot is free the moment its credit is on its way back.  Clearing
        // the flag here, on the engine thread, is what keeps `occupied` a purely
        // engine-thread quantity despite releases happening anywhere.
        const std::uint32_t depth = session_.granted_ring_depth();
        if(depth != 0)
        {
            arena_->slot(static_cast<std::size_t>(sequence % depth)).occupied = false;
        }

        tracker_.release(sequence);
        worked = true;
    }

    return worked;
}

bool SubscriberEngine::send_pending_credit()
{
    if(endpoint_ == nullptr)
    {
        return false;
    }

    std::uint64_t ack = 0;
    if(!tracker_.take_pending_ack(ack))
    {
        return false;
    }

    Protocol::CreditMessage credit;
    credit.generation = session_.generation();
    credit.stream_id = session_.stream_id();
    // 3.12: cumulative.  Every sequence at or below this has been released, and
    // 5.5 advances it only across the contiguous prefix -- one message per
    // progress iteration however many views were let go, which is what makes
    // credit_messages_sent divided by credits_returned the coalescing metric.
    credit.ack_sequence = ack;

    const std::array<std::byte, Protocol::k_credit_bytes> bytes = Protocol::encode(credit);

    ucp_request_param_t param;
    std::memset(&param, 0, sizeof(param));
    param.op_attr_mask =
        UCP_OP_ATTR_FIELD_FLAGS | UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
    // 32 bytes: cheaper to let UCX copy the header than to keep a context alive
    // until completion, which is the trade the frame path makes the other way.
    param.flags = UCP_AM_SEND_FLAG_COPY_HEADER;
    param.cb.send = &SubscriberEngine::on_credit_sent;
    param.user_data = this;

    void *request =
        ucp_am_send_nbx(endpoint_, Protocol::k_am_id_credit, bytes.data(), bytes.size(), nullptr, 0, &param);

    if(UCS_PTR_IS_ERR(request))
    {
        // Hand the ack back to the tracker: nothing carries it now, and if this
        // was the last release of the stream nothing later will either.
        tracker_.mark_ack_failed();
        transport_errors_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    // A non-null request is in flight and belongs to on_credit_sent.  Freeing it
    // here too would be a double free -- the same trap the frame path sets, and
    // UCX reports it just as indirectly.

    credit_messages_sent_.fetch_add(1, std::memory_order_relaxed);
    return true;
}

ucs_status_t SubscriberEngine::on_probe_am(void *arg,
                                           const void *header,
                                           std::size_t header_length,
                                           void *data,
                                           std::size_t length,
                                           const ucp_am_recv_param_t *param) noexcept
{
    (void) data;
    (void) length;
    (void) param;

    auto *self = static_cast<SubscriberEngine *>(arg);

    Protocol::ProbeMessage probe;
    const auto *bytes = static_cast<const std::byte *>(header);
    if(Protocol::decode(bytes, header_length, probe) != Status::Ok ||
       probe.stream_id != self->session_.stream_id())
    {
        self->dropped_bad_header_.fetch_add(1, std::memory_order_relaxed);
        return UCS_OK;
    }

    // 3.13: the token is echoed exactly.  It is what lets the publisher tell
    // this session's ack from a stale one belonging to a session that has
    // already been retired.
    self->pending_probe_token_ = probe.probe_token;
    self->probe_ack_pending_ = true;
    return UCS_OK;
}

bool SubscriberEngine::send_pending_probe_ack()
{
    if(!probe_ack_pending_ || endpoint_ == nullptr)
    {
        return false;
    }

    Protocol::ProbeAckMessage ack;
    ack.generation = session_.generation();
    ack.stream_id = session_.stream_id();
    ack.probe_token = pending_probe_token_;

    const std::array<std::byte, Protocol::k_probe_ack_bytes> bytes = Protocol::encode(ack);

    ucp_request_param_t param;
    std::memset(&param, 0, sizeof(param));
    param.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS;
    param.flags = UCP_AM_SEND_FLAG_COPY_HEADER;

    void *request = ucp_am_send_nbx(
        endpoint_, Protocol::k_am_id_probe_ack, bytes.data(), bytes.size(), nullptr, 0, &param);

    if(UCS_PTR_IS_ERR(request))
    {
        // Leave it pending: the publisher will not arm, the session expires on
        // its lease, and nothing here has to invent a retry policy.
        transport_errors_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    if(UCS_PTR_IS_PTR(request))
    {
        ucp_request_free(request);
    }

    probe_ack_pending_ = false;

    // The endpoint has settled on a transport and device by now, and this runs on
    // the engine thread, so it is both the earliest and the only correct place to
    // sample where this subscriber landed.
    locality_ = observe(endpoint_, arena_->ring().memory().base());
    detail::report(locality_, "subscriber");

    // 4.1: `Probing` exits to `Active` here.  The publisher is not armed yet --
    // it arms when this ack lands -- but from this side the handshake is done.
    SubscriberState expected = SubscriberState::Probing;
    state_.compare_exchange_strong(expected, SubscriberState::Active, std::memory_order_acq_rel);
    return true;
}

void SubscriberEngine::on_credit_sent(void *request,
                                      ucs_status_t status,
                                      void *user_data) noexcept
{
    auto *self = static_cast<SubscriberEngine *>(user_data);

    if(status != UCS_OK)
    {
        // Credit is cumulative, so the next Credit message carries everything
        // this one would have (3.12) -- but only if there is a next one.  Re-arm
        // the ack so the loop sends it again, rather than betting the last
        // credit of a stream on a release that may never come.  This runs inside
        // ucp_worker_progress, so it is the engine thread touching the tracker.
        self->tracker_.mark_ack_failed();
        self->transport_errors_.fetch_add(1, std::memory_order_relaxed);
    }

    if(request != nullptr)
    {
        ucp_request_free(request);
    }
}

ucs_status_t SubscriberEngine::on_frame_am(void *arg,
                                           const void *header,
                                           std::size_t header_length,
                                           void *data,
                                           std::size_t length,
                                           const ucp_am_recv_param_t *param) noexcept
{
    auto *self = static_cast<SubscriberEngine *>(arg);
    return self->handle_frame(
        static_cast<const std::byte *>(header), header_length, data, length, param);
}

ucs_status_t SubscriberEngine::handle_frame(const std::byte *header,
                                            std::size_t header_length,
                                            void *data,
                                            std::size_t length,
                                            const ucp_am_recv_param_t *param) noexcept
{
    if(closing_)
    {
        // Teardown drains outstanding receives by calling ucp_worker_progress,
        // which is what runs this handler; starting one more here is what would
        // make that drain unbounded.  Returning UCS_OK without calling
        // ucp_am_recv_data_nbx drops the descriptor and completes the
        // publisher's send with UCS_OK (ucp.h).
        return UCS_OK;
    }

    // 3.11's seven checks.  Every failure drops the frame, bumps a counter, and
    // returns UCS_OK -- never aborts the worker, because one malformed frame
    // from one peer must not take the transport down.
    Protocol::FrameHeader frame;
    if(Protocol::decode(header, header_length, frame) != Status::Ok)
    {
        dropped_bad_header_.fetch_add(1, std::memory_order_relaxed);
        return UCS_OK;
    }

    if(frame.stream_id != session_.stream_id())
    {
        // 4.4: a frame for an unknown stream_id is dropped, never treated as a
        // request to create a session.
        dropped_bad_header_.fetch_add(1, std::memory_order_relaxed);
        return UCS_OK;
    }

    // Ahead of the size checks, deviating from 3.11's written order, because the
    // releases below depend on it: a sequence only means something inside its
    // own epoch, since sequences restart at a re-arm.
    if(frame.generation != session_.generation())
    {
        dropped_stale_epoch_.fetch_add(1, std::memory_order_relaxed);
        return UCS_OK;
    }

    // The session contract, enforced.
    //
    // 6.2 makes the grant the contract: the array description is settled at
    // Open and changing a term of it means closing and reopening. A frame that
    // decodes cleanly and describes a *different* array is therefore not a
    // malformed message, it is the publisher contradicting what it granted --
    // and it is the one thing that would make `granted_geometry()` a hint
    // rather than a guarantee, forcing every consumer to re-check the shape of
    // every frame.
    //
    // Only checked when the grant is typed. A rank-0 `Byte` grant is the opaque
    // tier -- bytes, length and ordering, with the per-frame hint free to say
    // more -- so there is no contract to contradict.
    if(const Protocol::GeometryBlock &granted = session_.granted_geometry(); granted.rank > 0)
    {
        if(frame.element_type != granted.element_type ||
           frame.element_size != granted.element_size || frame.rank != granted.rank ||
           frame.shape != granted.shape || frame.strides != granted.strides)
        {
            dropped_geometry_mismatch_.fetch_add(1, std::memory_order_relaxed);
            tracker_.release(frame.sequence);

            // Retired rather than dropped. A publisher that has started sending
            // a different array will keep doing it, so discarding frames one at
            // a time would spend the whole session on a contract that no longer
            // holds -- and would do it silently. Closing and reopening is what
            // 6.2 says a changed term means, and BoundedRetry does exactly that.
            fail(Status::GeometryMismatch,
                 "a frame described a different array from the one this session "
                 "granted; the publisher changed a contract term without reopening");
            return UCS_OK;
        }
    }

    // Past here the sequence is in *this* window, so dropping the frame has to
    // release it.  Nothing else ever will -- a dropped frame is never committed
    // and so never gets a view -- and credit advances only across a contiguous
    // prefix (5.5), so one un-released sequence freezes the ack below it for
    // good and the publisher's window fills and never reopens.
    if(frame.payload_bytes != length)
    {
        dropped_bad_header_.fetch_add(1, std::memory_order_relaxed);
        tracker_.release(frame.sequence);
        return UCS_OK;
    }

    if(frame.payload_bytes > session_.granted_frame_bytes())
    {
        dropped_oversize_.fetch_add(1, std::memory_order_relaxed);
        tracker_.release(frame.sequence);
        return UCS_OK;
    }

    const auto slot_index =
        static_cast<std::size_t>(frame.sequence % session_.granted_ring_depth());
    ReceiveSlot &slot = arena_->slot(slot_index);

    if(slot.occupied || frame.sequence < tracker_.released_end())
    {
        // 5.3: unreachable while the credit window is respected, so reaching it
        // means the peer sent a sequence it had no credit for.  Not released,
        // unlike the drops above: this sequence either belongs to a frame still
        // moving through the slot or was released once already, and releasing it
        // again would credit one arrival twice.
        dropped_duplicate_seq_.fetch_add(1, std::memory_order_relaxed);
        return UCS_OK;
    }

    slot.occupied = true;
    slot.fields.shape = frame.shape;
    slot.fields.strides = frame.strides;
    slot.fields.sequence = frame.sequence;
    slot.fields.event_counter = frame.event_counter;
    slot.fields.timestamp_ns = frame.timestamp_ns;
    slot.fields.dropped_before = frame.dropped_before;
    slot.fields.payload_bytes = frame.payload_bytes;
    slot.fields.element_type = frame.element_type;
    slot.fields.element_size = frame.element_size;
    slot.fields.rank = frame.rank;
    slot.fields.quality = frame.quality;
    slot.fields.generation = frame.generation;
    // FrameView describes the memory the application receives, not the
    // publisher's source allocation. They differ for GPUDirect receives.
    slot.fields.memory_kind = config_.receive_memory_kind;
    slot.fields.endian = frame.endian;

    if((param->recv_attr & UCP_AM_RECV_ATTR_FLAG_RNDV) != 0)
    {
        // The zero-copy path: UCX writes the payload straight into the
        // registered slot.  No staging buffer, no copy, and the address the
        // application eventually sees is this one.
        ucp_request_param_t recv_param;
        std::memset(&recv_param, 0, sizeof(recv_param));
        recv_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA |
                                  UCP_OP_ATTR_FIELD_MEMORY_TYPE | UCP_OP_ATTR_FIELD_MEMH;
        recv_param.cb.recv_am = &SubscriberEngine::on_rndv_complete;
        recv_param.user_data = &pending_[slot_index];
        // The destination was registered at construction and its kind known
        // then, so there is no reason to make UCX rediscover either: ucp.h
        // describes the handle as letting a protocol "skip the registration
        // step", and the memory type is what stops UCX probing the pointer for
        // one -- a driver query per frame on the CUDA path.
        recv_param.memory_type = to_ucs_memory_type(config_.receive_memory_kind);
        recv_param.memh = arena_->ring().memory().handle();

        void *request = ucp_am_recv_data_nbx(worker_->get(),
                                             data,
                                             slot.data,
                                             static_cast<std::size_t>(frame.payload_bytes),
                                             &recv_param);

        if(UCS_PTR_IS_ERR(request))
        {
            slot.occupied = false;
            transport_errors_.fetch_add(1, std::memory_order_relaxed);
            fail(Status::TransportFailure, "a rendezvous receive could not be started");
            return UCS_OK;
        }

        if(request == nullptr)
        {
            // Completed inline; no callback will fire for it.
            commit(slot_index);
            return UCS_OK;
        }

        // In flight, and now reclaimable: `quiesce()` needs the handle to cancel
        // it if the peer never finishes the transfer.
        pending_[slot_index].request = request;
        ++rndv_inflight_;
        return UCS_INPROGRESS;
    }

    // Eager into a device ring is not a slow path, it is an invalid one: the
    // memcpy below is a CPU store into what for a Cuda or Rocm arena is a device
    // pointer.  The publisher forces rendezvous for a non-host session (4.4), so
    // arriving here means the two sides disagree and the data path can no longer
    // be trusted -- fail the session, as a failed rendezvous start does.
    if(config_.receive_memory_kind != MemoryKind::Host)
    {
        slot.occupied = false;
        tracker_.release(frame.sequence);
        transport_errors_.fetch_add(1, std::memory_order_relaxed);
        fail(Status::GeometryMismatch,
             "the publisher sent an eager frame into a device ring, which it must "
             "not: the two sides disagree about the memory kind");
        return UCS_OK;
    }

    // Eager: UCX has already staged the bytes, so there is a copy and no way to
    // avoid one.  It is counted rather than hidden, because "was that actually
    // zero copy?" is a question a benchmark must be able to answer without a
    // debugger.
    std::memcpy(slot.data, data, length);
    bytes_copied_.fetch_add(length, std::memory_order_relaxed);
    commit(slot_index);
    return UCS_OK;
}

void SubscriberEngine::on_rndv_complete(void *request,
                                        ucs_status_t status,
                                        std::size_t length,
                                        void *user_data) noexcept
{
    (void) length;

    auto *pending = static_cast<Pending *>(user_data);
    SubscriberEngine *self = pending->self;

    // Retire the handle first: whatever this callback decides, the request is
    // finished and must not be cancelled by a teardown that runs after it.
    // UCS_ERR_CANCELED arrives here too, which is how a cancelled receive stops
    // being counted.
    if(pending->request != nullptr)
    {
        pending->request = nullptr;
        --self->rndv_inflight_;
    }

    if(status != UCS_OK)
    {
        self->arena_->slot(pending->slot_index).occupied = false;
        self->transport_errors_.fetch_add(1, std::memory_order_relaxed);
        // A failed rendezvous operation means the endpoint/QP is no longer a
        // usable data path.  Tell the Tango control loop so BoundedRetry can
        // retire this session and reconnect instead of reporting Active while
        // no further frame or credit can move.
        self->fail(Status::TransportFailure,
                   "a rendezvous receive failed; the endpoint is no longer a usable "
                   "data path");
    }
    else
    {
        self->commit(pending->slot_index);
    }

    if(request != nullptr)
    {
        ucp_request_free(request);
    }
}

void SubscriberEngine::fail(Status status, const char *reason) noexcept
{
    // First reason wins. A failing transport tends to fail again on the way
    // down, and the second reason is usually a consequence of the first.
    const char *expected = nullptr;
    if(failure_reason_.compare_exchange_strong(expected, reason, std::memory_order_acq_rel))
    {
        failure_status_.store(status, std::memory_order_release);
    }

    state_.store(SubscriberState::Failed, std::memory_order_release);
}

BulkError SubscriberEngine::last_error() const noexcept
{
    const char *reason = failure_reason_.load(std::memory_order_acquire);
    if(reason == nullptr)
    {
        return BulkError{};
    }

    // Allocates, and may only do so because this runs on the control thread --
    // never on the engine, which is why fail() takes a literal.
    return BulkError{failure_status_.load(std::memory_order_acquire), reason, "transport"};
}

void SubscriberEngine::commit(std::size_t slot_index) noexcept
{
    ReceiveSlot &slot = arena_->slot(slot_index);
    frames_received_.fetch_add(1, std::memory_order_relaxed);

    // allocate_shared from the arena's pool, so the control block that *is* the
    // credit interlock (5.5) does not cost a malloc per frame.
    PoolAllocator<ReceiveSlotLease> allocator(arena_->lease_pool());
    auto lease = std::allocate_shared<ReceiveSlotLease>(allocator, sink_, slot.fields.sequence);

    arena_->note_view_issued();
    FrameView view = ReceiveSlotLease::make_view(std::move(lease), slot.data, &slot.fields);

    // 5.3's drop policy, the high-water gauge and the credit a dropped frame
    // returns are all the queue's now. Waking a consumer is deliberately not
    // done here: this runs inside `ucp_worker_progress`, and the loop calls
    // `notify()` for the same reason it sends credit and probe acks.
    delivery_->push(std::move(view));
}

// -- application thread -----------------------------------------------------

bool SubscriberEngine::ring_contains(const void *p) const noexcept
{
    return arena_->ring().contains(p);
}

const std::byte *SubscriberEngine::slot_address(std::size_t index) const noexcept
{
    return arena_->ring().slot(index);
}

SubscriberCounters SubscriberEngine::counters() const noexcept
{
    // `frames_delivered`, `frames_dropped_queue_full` and the two queue gauges
    // are absent, and stay absent: they describe the subscription's delivery
    // queue, which outlives this transport. Reporting the shared totals from
    // here would have them counted once per session by whoever accumulates
    // retiring transports.
    SubscriberCounters out;
    out.frames_received = frames_received_.load(std::memory_order_relaxed);
    out.frames_dropped_stale_epoch = dropped_stale_epoch_.load(std::memory_order_relaxed);
    out.frames_dropped_bad_header = dropped_bad_header_.load(std::memory_order_relaxed);
    out.frames_dropped_oversize = dropped_oversize_.load(std::memory_order_relaxed);
    out.frames_dropped_duplicate_seq = dropped_duplicate_seq_.load(std::memory_order_relaxed);
    out.frames_dropped_geometry_mismatch =
        dropped_geometry_mismatch_.load(std::memory_order_relaxed);
    out.credits_returned = arena_->credits_returned();
    out.credit_messages_sent = credit_messages_sent_.load(std::memory_order_relaxed);
    out.views_outstanding = arena_->views_outstanding();
    out.sessions_opened = session_.stream_id() != 0 ? 1u : 0u;
    out.renewals_sent = renewals_sent_.load(std::memory_order_relaxed);
    out.renewals_failed = renewals_failed_.load(std::memory_order_relaxed);
    out.transport_errors = transport_errors_.load(std::memory_order_relaxed);
    out.pinned_bytes = arena_->ring().mapped_bytes();
    return out;
}

std::unique_ptr<SubscriberTransport> make_subscriber_transport(
    SubscriberConfig config, std::shared_ptr<DeliveryQueue> delivery)
{
    // Declared in `tango-bulk/unstable/subscriber_transport.h` and defined here, which is the
    // point of the seam: `BulkSubscriber` constructs a transport without naming
    // the concrete type, and therefore without compiling against `ucp/*`.
    return std::make_unique<SubscriberEngine>(std::move(config), std::move(delivery));
}

} // namespace TangoBulk::detail
