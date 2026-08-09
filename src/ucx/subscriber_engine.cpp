// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <ucx/subscriber_engine.h>

#include <cassert>
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
/// destructor open.  4.2 gives the publisher the same bounded-wait-then-force
/// shape for the same reason.
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
/// The holder is heap-allocated and never freed: a static with a destructor would
/// simply move the abort to exit time.
void quarantine(std::unique_ptr<UcxWorker> worker, std::shared_ptr<UcxContext> context)
{
    using Held = std::vector<std::pair<std::unique_ptr<UcxWorker>, std::shared_ptr<UcxContext>>>;

    static std::mutex mutex;
    static Held *held = new Held();

    const std::lock_guard<std::mutex> lock(mutex);
    held->emplace_back(std::move(worker), std::move(context));
}

} // namespace

// ---------------------------------------------------------------------------
// ReceiveArena
// ---------------------------------------------------------------------------

ReceiveArena::ReceiveArena(std::shared_ptr<UcxContext> context,
                           std::uint64_t slot_bytes,
                           std::uint32_t depth,
                           std::uint64_t pinned_limit) :
    context_(std::move(context)),
    // The receive ring never pads its stride: 6.2's extra page exists to break
    // cache-set aliasing on the *copy* the receiver would otherwise do, and this
    // path lands the payload in the slot directly.  It is the publisher's ring
    // that pays that tax.
    ring_(*context_, slot_bytes, depth, false, pinned_limit),
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

SubscriberEngine::SubscriberEngine(SubscriberConfig config) :
    config_(std::move(config)),
    tracker_(config_.ring_depth),
    delivery_(config_.delivery_queue_depth),
    pending_(config_.ring_depth)
{
    const Status status = config_.validate();
    if(status != Status::Ok)
    {
        throw BulkException(BulkError{
            status, std::string("invalid SubscriberConfig: ") + to_string(status), "subscriber"});
    }

    if(config_.delivery_mode != DeliveryMode::Manual)
    {
        // 9.3: "no user callbacks and no dispatch thread -- DeliveryMode::Manual
        // and poll() only".  Refusing beats silently creating the thread that
        // the milestone says does not exist yet.
        throw BulkException(BulkError{Status::Internal,
                                      "M2 implements DeliveryMode::Manual only; the dispatch "
                                      "thread arrives with the callback path",
                                      "subscriber"});
    }

    context_ = std::make_shared<UcxContext>(config_.ucx_tls);
    worker_ = std::make_unique<UcxWorker>(*context_);

    // 4.1, `Opening` entry action, in this order: the ring is allocated,
    // registered and armed *before* Open goes out, because the publisher may
    // send the first frame the moment it replies.
    arena_ = std::make_shared<ReceiveArena>(context_,
                                            config_.max_frame_bytes,
                                            config_.ring_depth,
                                            config_.pinned_memory_limit_bytes);
    sink_ = arena_;

    for(std::size_t i = 0; i < pending_.size(); ++i)
    {
        pending_[i].self = this;
        pending_[i].slot_index = i;
    }

    register_frame_handler();

    state_.store(SubscriberState::Opening, std::memory_order_release);
    engine_ = std::thread([this] { engine_loop(); });
}

SubscriberEngine::~SubscriberEngine()
{
    running_.store(false, std::memory_order_release);
    if(engine_.joinable())
    {
        engine_.join();
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
        quarantine(std::move(worker_), context_);
    }

    // Anything still in the delivery queue, and anything the application is
    // still holding, keeps the arena alive past this point through its lease.
    // That is what makes teardown with views outstanding safe rather than a
    // race: the ring is unmapped by the last lease, not by this destructor.
}

void SubscriberEngine::register_frame_handler()
{
    ucp_am_handler_param_t param;
    std::memset(&param, 0, sizeof(param));
    param.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID | UCP_AM_HANDLER_PARAM_FIELD_CB |
                       UCP_AM_HANDLER_PARAM_FIELD_ARG;
    param.id = Protocol::k_am_id_frame;
    param.cb = &SubscriberEngine::on_frame_am;
    param.arg = this;

    const ucs_status_t status = ucp_worker_set_am_recv_handler(worker_->get(), &param);
    if(status != UCS_OK)
    {
        throw_ucx_error("ucp_worker_set_am_recv_handler(frame)", status, "subscriber");
    }
}

std::vector<std::byte> SubscriberEngine::make_open_request(std::uint64_t correlation_id) const
{
    Protocol::OpenRequest request;
    request.version_min = Protocol::k_version_major;
    request.version_max = Protocol::k_version_major;
    // M2 asks only for coalescing.  Claiming probe or geometry-rearm would be
    // claiming capabilities this side does not implement yet.
    request.requested_caps = Protocol::k_caps_credit_coalescing;
    request.client_instance_id = Protocol::generate_client_instance_id();
    request.requested_max_frame_bytes = config_.max_frame_bytes;
    request.requested_ring_depth = config_.ring_depth;
    request.requested_credit_window = config_.credit_window;
    request.requested_memory_kind = MemoryKind::Host;
    request.requested_transport = Protocol::Transport::ActiveMessage;
    request.drop_policy = config_.drop_policy;
    request.stream_name = config_.stream_name;
    request.client_ucx_address = worker_->address();

    return Protocol::encode(request, correlation_id);
}

Status SubscriberEngine::adopt_open_reply(const std::byte *data, std::size_t size)
{
    Protocol::Envelope envelope;
    if(Protocol::decode_envelope(data, size, envelope) != Status::Ok)
    {
        state_.store(SubscriberState::Failed, std::memory_order_release);
        return Status::MalformedMessage;
    }

    // A client must accept Error in place of any expected reply.
    if(envelope.msg_type == Protocol::CoordType::Error)
    {
        Protocol::ErrorMessage error;
        const Status decoded = Protocol::decode(data, size, error);
        state_.store(SubscriberState::Failed, std::memory_order_release);
        return decoded == Status::Ok ? error.status : Status::MalformedMessage;
    }

    Protocol::OpenReply reply;
    if(Protocol::decode(data, size, reply) != Status::Ok)
    {
        state_.store(SubscriberState::Failed, std::memory_order_release);
        return Status::MalformedMessage;
    }

    if(reply.status != Status::Ok)
    {
        state_.store(SubscriberState::Failed, std::memory_order_release);
        return reply.status;
    }

    if(reply.geometry.validate() != Status::Ok)
    {
        state_.store(SubscriberState::Failed, std::memory_order_release);
        return Status::GeometryMismatch;
    }

    // A grant is only ever clamped downward (3.5 step 5), so a ring registered
    // at the requested geometry is always large enough for the granted one.
    // Checking anyway, because "always" here depends on the peer behaving.
    if(reply.geometry.max_frame_bytes > config_.max_frame_bytes ||
       reply.geometry.ring_depth > config_.ring_depth)
    {
        state_.store(SubscriberState::Failed, std::memory_order_release);
        return Status::GeometryMismatch;
    }

    session_id_ = reply.session_id;
    stream_id_ = reply.stream_id;
    generation_ = reply.geometry.generation;
    granted_depth_ = reply.geometry.ring_depth;
    granted_frame_bytes_ = reply.geometry.max_frame_bytes;

    tracker_.reset(0);

    try
    {
        endpoint_ = worker_->create_endpoint(reply.server_ucx_address.data(),
                                             reply.server_ucx_address.size());
    }
    catch(const BulkException &)
    {
        state_.store(SubscriberState::Failed, std::memory_order_release);
        return Status::TransportFailure;
    }

    // M2 goes straight to Active.  4.1 routes through Probing, but 9.3 puts the
    // probe out of scope; the transition it guards -- proof the ring is
    // reachable, not merely registered -- returns in M3.
    state_.store(SubscriberState::Active, std::memory_order_release);
    return Status::Ok;
}

std::vector<std::byte> SubscriberEngine::make_close_request(std::uint64_t correlation_id) const
{
    Protocol::CloseRequest request;
    request.session_id = session_id_;
    request.reason = Protocol::CloseReason::ClientShutdown;
    return Protocol::encode(request, correlation_id);
}

// -- engine thread ----------------------------------------------------------

void SubscriberEngine::engine_loop()
{
    unsigned idle = 0;

    while(running_.load(std::memory_order_acquire))
    {
        bool worked = drain_credit_returns();
        worked |= send_pending_credit();

        if(ucp_worker_progress(worker_->get()) != 0)
        {
            worked = true;
        }

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
    // server restarts, a client is killed, a session is force-closed with frames
    // still moving -- in each case the far side stops answering mid-transfer, and
    // teardown here still has to end at a worker that is safe to destroy.

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

    // 3. Close our own endpoint: bounded wait, then force, the same shape 4.2
    //    fixes for the publisher.
    if(endpoint_ != nullptr)
    {
        ucp_request_param_t param;
        std::memset(&param, 0, sizeof(param));
        param.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS;
        param.flags = UCP_EP_CLOSE_FLAG_FORCE;

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
        if(granted_depth_ != 0)
        {
            arena_->slot(static_cast<std::size_t>(sequence % granted_depth_)).occupied = false;
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
    credit.generation = generation_;
    credit.stream_id = stream_id_;
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
        transport_errors_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    // A non-null request is in flight and belongs to on_credit_sent.  Freeing it
    // here too would be a double free -- the same trap the frame path sets, and
    // UCX reports it just as indirectly.

    credit_messages_sent_.fetch_add(1, std::memory_order_relaxed);
    return true;
}

void SubscriberEngine::on_credit_sent(void *request,
                                      ucs_status_t status,
                                      void *user_data) noexcept
{
    auto *self = static_cast<SubscriberEngine *>(user_data);

    if(status != UCS_OK)
    {
        // Nothing to repair: credit is cumulative, so the next Credit message
        // carries everything this one would have (3.12).  That is the whole
        // reason there is no retransmit path here.
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
    // 3.11's seven checks, in order.  Every failure drops the frame, bumps a
    // counter, and returns UCS_OK -- never aborts the worker, because one
    // malformed frame from one peer must not take the transport down.
    Protocol::FrameHeader frame;
    if(Protocol::decode(header, header_length, frame) != Status::Ok)
    {
        dropped_bad_header_.fetch_add(1, std::memory_order_relaxed);
        return UCS_OK;
    }

    if(frame.stream_id != stream_id_)
    {
        // 4.4: a frame for an unknown stream_id is dropped, never treated as a
        // request to create a session.
        dropped_bad_header_.fetch_add(1, std::memory_order_relaxed);
        return UCS_OK;
    }

    if(frame.payload_bytes != length)
    {
        dropped_bad_header_.fetch_add(1, std::memory_order_relaxed);
        return UCS_OK;
    }

    if(frame.payload_bytes > granted_frame_bytes_)
    {
        dropped_oversize_.fetch_add(1, std::memory_order_relaxed);
        return UCS_OK;
    }

    if(frame.generation != generation_)
    {
        dropped_stale_epoch_.fetch_add(1, std::memory_order_relaxed);
        return UCS_OK;
    }

    const std::size_t slot_index = static_cast<std::size_t>(frame.sequence % granted_depth_);
    ReceiveSlot &slot = arena_->slot(slot_index);

    if(slot.occupied || frame.sequence < tracker_.released_end())
    {
        // 5.3: unreachable while the credit window is respected, so reaching it
        // means the peer sent a sequence it had no credit for.
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
    slot.fields.memory_kind = frame.memory_kind;
    slot.fields.endian = frame.endian;

    if((param->recv_attr & UCP_AM_RECV_ATTR_FLAG_RNDV) != 0)
    {
        // The zero-copy path: UCX writes the payload straight into the
        // registered slot.  No staging buffer, no copy, and the address the
        // application eventually sees is this one.
        ucp_request_param_t recv_param;
        std::memset(&recv_param, 0, sizeof(recv_param));
        recv_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
        recv_param.cb.recv_am = &SubscriberEngine::on_rndv_complete;
        recv_param.user_data = &pending_[slot_index];

        void *request = ucp_am_recv_data_nbx(worker_->get(),
                                             data,
                                             slot.data,
                                             static_cast<std::size_t>(frame.payload_bytes),
                                             &recv_param);

        if(UCS_PTR_IS_ERR(request))
        {
            slot.occupied = false;
            transport_errors_.fetch_add(1, std::memory_order_relaxed);
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

    if(delivery_.try_push(std::move(view)))
    {
        const std::uint64_t depth = delivery_.size();
        if(depth > delivery_high_water_.load(std::memory_order_relaxed))
        {
            delivery_high_water_.store(depth, std::memory_order_relaxed);
        }
        return;
    }

    // 5.3: apply drop_policy, and never stall UCX progress to do it.
    if(config_.drop_policy == DropPolicy::DropOldest)
    {
        FrameView oldest;
        if(delivery_.try_pop(oldest))
        {
            // Destroying the oldest view releases its slot and returns its
            // credit, which is what makes room.
            oldest.reset();
            if(delivery_.try_push(std::move(view)))
            {
                dropped_queue_full_.fetch_add(1, std::memory_order_relaxed);
                return;
            }
        }
    }

    // DropNewest, or DropOldest that lost a race for the space it just made:
    // let `view` go out of scope.  Its destructor releases the slot and returns
    // the credit immediately, which is exactly what the spec asks for and is
    // why there is nothing else to do here.
    dropped_queue_full_.fetch_add(1, std::memory_order_relaxed);
}

// -- application thread -----------------------------------------------------

std::size_t SubscriberEngine::poll(std::chrono::milliseconds timeout, const FrameCallback &cb)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    std::size_t dispatched = 0;

    for(;;)
    {
        FrameView view;
        while(delivery_.try_pop(view))
        {
            frames_delivered_.fetch_add(1, std::memory_order_relaxed);
            ++dispatched;

            // On the CALLING thread, per 2.4.  Whether the credit returns when
            // this returns is entirely up to the callback: keeping a copy
            // withholds it, which is the documented backpressure semantic and
            // not a leak.
            if(cb)
            {
                cb(std::move(view));
            }
            view.reset();
        }

        if(dispatched != 0 || std::chrono::steady_clock::now() >= deadline)
        {
            break;
        }

        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }

    return dispatched;
}

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
    SubscriberCounters out;
    out.frames_received = frames_received_.load(std::memory_order_relaxed);
    out.frames_delivered = frames_delivered_.load(std::memory_order_relaxed);
    out.frames_dropped_queue_full = dropped_queue_full_.load(std::memory_order_relaxed);
    out.frames_dropped_stale_epoch = dropped_stale_epoch_.load(std::memory_order_relaxed);
    out.frames_dropped_bad_header = dropped_bad_header_.load(std::memory_order_relaxed);
    out.frames_dropped_oversize = dropped_oversize_.load(std::memory_order_relaxed);
    out.frames_dropped_duplicate_seq = dropped_duplicate_seq_.load(std::memory_order_relaxed);
    out.credits_returned = arena_->credits_returned();
    out.credit_messages_sent = credit_messages_sent_.load(std::memory_order_relaxed);
    out.views_outstanding = arena_->views_outstanding();
    out.delivery_queue_depth = delivery_.size();
    out.delivery_queue_high_water = delivery_high_water_.load(std::memory_order_relaxed);
    out.sessions_opened = stream_id_ != 0 ? 1u : 0u;
    out.transport_errors = transport_errors_.load(std::memory_order_relaxed);
    out.pinned_bytes = arena_->ring().mapped_bytes();
    return out;
}

} // namespace TangoBulk::detail
