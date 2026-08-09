// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <ucx/registered_ring.h>
#include <ucx/ucx_context.h>

#include <core/bounded_queue.h>
#include <core/credit_window.h>
#include <core/lease_pool.h>

#include <tango-bulk/protocol.h>
#include <tango-bulk/publisher.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <thread>
#include <vector>

/// The publisher half of the M2 vertical slice.
///
/// What is here: the registered producer ring, the lease, the engine thread and
/// its submit-and-progress loop, `Frame` submission, `Credit` receipt, and
/// enough of the coordination plane (`Open` / `Close`) to establish a session
/// without a Tango process.
///
/// What is deliberately not here, per IMPLEMENTATION_SPEC.md 9.3: no lease
/// timers or expiry, no probe, no geometry re-arm, no reconnect, no RMA, and one
/// session rather than `max_sessions`.  Those are M3 and later, and adding any
/// of them to make a test pass is explicitly out of order.
namespace TangoBulk
{
namespace
{

using detail::BoundedQueue;
using detail::CreditWindow;
using detail::LeasePool;
using detail::RegisteredRing;
using detail::UcxContext;
using detail::UcxWorker;

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
    std::atomic<std::uint64_t> sessions_rejected{0};
    std::atomic<std::uint64_t> malformed_messages{0};
    std::atomic<std::uint64_t> transport_errors{0};
    std::atomic<std::uint64_t> pinned_bytes{0};
};

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
    /// releasing a slot on credit.  All three are the same operation, and 5.4
    /// is the rule about *when* each is allowed to happen -- not about doing
    /// anything different once it does.
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
    /// ownership by this point, and the engine releases the slot through the
    /// source when the credit arrives.
    struct PublishItem
    {
        std::size_t slot_index{0};
        std::uint64_t payload_bytes{0};
        Protocol::FrameHeader header{};
    };

    /// A send in flight: its header context and the slot it is reading from.
    ///
    /// Indexed by `sequence % ring_depth`.  At most `credit_window` sequences
    /// are outstanding and `credit_window <= ring_depth`, so that index is
    /// unique across everything in flight.
    struct InFlight
    {
        std::array<std::byte, Protocol::k_frame_header_bytes> header{};
        std::size_t slot_index{0};
        bool header_busy{false};
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
        worker(context),
        ring(context,
             config.max_frame_bytes,
             config.ring_depth,
             config.pad_slot_stride,
             config.pinned_memory_limit_bytes),
        publish_queue(config.publish_queue_depth),
        window(config.ring_depth, config.credit_window),
        inflight(config.ring_depth)
    {
        counters.pinned_bytes.store(ring.mapped_bytes(), std::memory_order_relaxed);

        source.impl_ = std::make_unique<BulkSource::Impl>(ring, counters);

        server_epoch_id = Protocol::generate_server_epoch_id();

        register_credit_handler();

        engine = std::thread([this] { engine_loop(); });
    }

    ~Impl()
    {
        running.store(false, std::memory_order_release);
        if(engine.joinable())
        {
            engine.join();
        }
    }

    // -- engine thread ------------------------------------------------------

    void register_credit_handler()
    {
        ucp_am_handler_param_t param;
        std::memset(&param, 0, sizeof(param));
        param.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID | UCP_AM_HANDLER_PARAM_FIELD_CB |
                           UCP_AM_HANDLER_PARAM_FIELD_ARG;
        param.id = Protocol::k_am_id_credit;
        param.cb = &Impl::on_credit_am;
        param.arg = this;

        const ucs_status_t status = ucp_worker_set_am_recv_handler(worker.get(), &param);
        if(status != UCS_OK)
        {
            detail::throw_ucx_error("ucp_worker_set_am_recv_handler(credit)", status, "publisher");
        }
    }

    /// 5.1's loop, minus the parts M2 does not have.
    ///
    /// One thread that both submits and progresses -- not a submit thread plus a
    /// progress thread.  That is settled by measurement rather than taste:
    /// ucx_perftest saturates with a single submit+progress loop, and the
    /// spike's --progress-thread variant measured ~13 against ~16 GiB/s.
    void engine_loop()
    {
        engine_thread_id = std::this_thread::get_id();

        unsigned idle = 0;
        while(running.load(std::memory_order_acquire))
        {
            bool worked = false;

            worked |= drain_connect_requests();
            worked |= submit_ready_frames();

            if(ucp_worker_progress(worker.get()) != 0)
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

        teardown_session();
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
                task->ep = worker.create_endpoint(task->address, task->address_size);
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

    bool submit_ready_frames()
    {
        if(!session_armed.load(std::memory_order_acquire))
        {
            return false;
        }

        bool worked = false;

        // Bounded per iteration (5.1 step 1): a burst on the publish queue must
        // not starve ucp_worker_progress(), or completions and credits stop
        // arriving and the burst becomes a stall.
        for(unsigned n = 0; n < 32; ++n)
        {
            if(!window.can_submit())
            {
                break;
            }

            PublishItem item;
            if(!publish_queue.try_pop(item))
            {
                break;
            }

            counters.publish_queue_depth.store(publish_queue.size(), std::memory_order_relaxed);
            submit(item);
            worked = true;
        }

        return worked;
    }

    void submit(PublishItem &item)
    {
        // 5.4: sequence assignment and window advancement are atomic with the
        // engine's acceptance of the frame.  A frame that fails to submit is
        // never assigned a sequence, so an uncreditable hole in the window is
        // impossible rather than merely unlikely.
        const std::uint64_t sequence = window.next_sequence();
        const std::size_t index = static_cast<std::size_t>(sequence % ring.depth());

        InFlight &slot = inflight[index];
        item.header.sequence = sequence;
        item.header.stream_id = stream_id;
        item.header.dropped_before = dropped_before.load(std::memory_order_relaxed);
        slot.header = Protocol::encode(item.header);
        slot.slot_index = item.slot_index;
        slot.header_busy = true;

        ucp_request_param_t param;
        std::memset(&param, 0, sizeof(param));
        param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
        param.cb.send = &Impl::on_send_complete;
        param.user_data = this;

        void *request = ucp_am_send_nbx(endpoint,
                                        Protocol::k_am_id_frame,
                                        slot.header.data(),
                                        slot.header.size(),
                                        ring.slot(item.slot_index),
                                        static_cast<std::size_t>(item.payload_bytes),
                                        &param);

        if(UCS_PTR_IS_ERR(request))
        {
            // Never assigned a sequence, so nothing to credit and no hole.  The
            // slot goes straight back to the producer: the NIC never saw it.
            slot.header_busy = false;
            source.impl_->release(item.slot_index);
            counters.transport_errors.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        window.note_submitted();
        counters.frames_submitted.fetch_add(1, std::memory_order_relaxed);
        counters.credits_outstanding.store(window.outstanding(), std::memory_order_relaxed);

        if(request == nullptr)
        {
            // Completed inline; no callback will fire, so the header context is
            // free again immediately.
            slot.header_busy = false;
            counters.frames_completed.fetch_add(1, std::memory_order_relaxed);
        }
        else
        {
            // Outstanding until on_send_complete says otherwise.  Counted so
            // teardown knows whether it has collected every completion; a
            // request whose callback never runs is a request never freed.
            ++sends_inflight;
        }

        // A non-null request means the send is in flight and on_send_complete
        // will be called with it.  Releasing the handle is that callback's job:
        // freeing it here as well is a double free, and UCX reports it far from
        // its cause -- as an arbiter assertion during endpoint teardown.
    }

    static void on_send_complete(void *request, ucs_status_t status, void *user_data) noexcept
    {
        auto *self = static_cast<Impl *>(user_data);

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
            --self->sends_inflight;
            ucp_request_free(request);
        }
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
        if(credit.stream_id != stream_id)
        {
            counters.malformed_messages.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        const std::uint64_t first = window.credited_end();
        const CreditWindow::CreditOutcome outcome = window.apply_credit(credit.ack_sequence);

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
            InFlight &slot = inflight[static_cast<std::size_t>(seq % ring.depth())];
            source.impl_->release(slot.slot_index);
        }

        if(outcome.released != 0)
        {
            counters.frames_credited.fetch_add(outcome.released, std::memory_order_relaxed);
            credited.store(window.credited_end(), std::memory_order_release);
            counters.credits_outstanding.store(window.outstanding(), std::memory_order_relaxed);
        }
    }

    /// 4.2's teardown, in the one order it is allowed to run in.
    ///
    /// Step 4 (release leases) after step 3 (close the endpoint) is the single
    /// most dangerous ordering in the design to get wrong, because handing a
    /// slot back to the producer while the NIC may still be reading it corrupts
    /// a payload silently rather than raising anything.
    void teardown_session()
    {
        if(endpoint == nullptr)
        {
            return;
        }

        // 1. no new submissions.
        session_armed.store(false, std::memory_order_release);

        // 2. let outstanding operations drain, bounded.
        ucp_request_param_t flush_param;
        std::memset(&flush_param, 0, sizeof(flush_param));
        flush_param.op_attr_mask = 0;

        void *flush = ucp_ep_flush_nbx(endpoint, &flush_param);
        if(UCS_PTR_IS_PTR(flush))
        {
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
            while(ucp_request_check_status(flush) == UCS_INPROGRESS &&
                  std::chrono::steady_clock::now() < deadline)
            {
                ucp_worker_progress(worker.get());
            }
            ucp_request_free(flush);
        }

        // 3. close the endpoint, forcing after the bounded wait.
        ucp_request_param_t close_param;
        std::memset(&close_param, 0, sizeof(close_param));
        close_param.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS;
        close_param.flags = UCP_EP_CLOSE_FLAG_FORCE;

        void *close = ucp_ep_close_nbx(endpoint, &close_param);
        if(UCS_PTR_IS_PTR(close))
        {
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
            while(ucp_request_check_status(close) == UCS_INPROGRESS &&
                  std::chrono::steady_clock::now() < deadline)
            {
                ucp_worker_progress(worker.get());
            }
            ucp_request_free(close);
        }
        endpoint = nullptr;

        // 3b. Collect the completions the force-close just produced.
        //
        // Forcing the endpoint shut fails every send still on it, but a failed
        // send is still delivered through on_send_complete, and that callback
        // only runs inside progress.  Without this the requests are never freed:
        // UCX reports it at cleanup as "was not returned to mpool ucp_requests"
        // and LeakSanitizer as an indirect leak from ucs_posix_memalign.
        //
        // It sits after the close rather than in step 2 on purpose.  The sends
        // stranded here are exactly the ones the step-2 flush could not drain --
        // a consumer that stopped returning credit leaves them unsendable -- so
        // waiting on them before forcing would only burn the budget twice.
        {
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
            while(sends_inflight > 0 && std::chrono::steady_clock::now() < deadline)
            {
                ucp_worker_progress(worker.get());
            }
        }

        // 4. only now may retained slots go back to the producer.
        const std::uint64_t from = window.credited_end();
        const std::uint64_t to = window.next_sequence();
        for(std::uint64_t seq = from; seq < to; ++seq)
        {
            InFlight &slot = inflight[static_cast<std::size_t>(seq % ring.depth())];
            source.impl_->release(slot.slot_index);
        }
        window.reset(to);
        credited.store(to, std::memory_order_release);

        counters.sessions_closed.fetch_add(1, std::memory_order_relaxed);
    }

    // -- control path -------------------------------------------------------

    /// Ask the engine to create an endpoint, and wait for it.
    ///
    /// Blocking is fine here and only here: this is the control path, reached
    /// from a Tango command thread, and 2.1 draws the no-blocking line around
    /// the data path.
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

    std::vector<std::byte> handle_open(const std::byte *data,
                                       std::size_t size,
                                       std::uint64_t correlation_id);
    std::vector<std::byte> handle_close(const std::byte *data,
                                        std::size_t size,
                                        std::uint64_t correlation_id);

    PublisherConfig config;
    UcxContext context;
    UcxWorker worker;
    RegisteredRing ring;

    BulkSource source;
    BoundedQueue<PublishItem> publish_queue;
    CreditWindow window;                 ///< engine thread only
    std::vector<InFlight> inflight;      ///< engine thread only

    /// Sends handed to UCX whose completion callback has not run yet.  Engine
    /// thread only: raised in submit(), lowered in on_send_complete, both of
    /// which run there.  Teardown drains it so no request is left unfreed.
    std::size_t sends_inflight{0};
    AtomicPublisherCounters counters;

    // Admission control, read by publish() on application threads.
    //
    // `admitted` counts frames publish() accepted; `credited` tracks the
    // engine's window base.  Their difference is exactly "accepted but not yet
    // released by the consumer", which is what the credit window bounds -- and
    // computing it on the application thread is what lets publish() answer
    // CreditStalled itself instead of discovering it later on the engine.
    std::atomic<std::uint64_t> admitted{0};
    std::atomic<std::uint64_t> credited{0};
    std::atomic<std::uint64_t> dropped_before{0};

    std::atomic<bool> running{true};
    std::atomic<bool> session_armed{false};
    std::thread engine;
    std::thread::id engine_thread_id{};

    std::mutex connect_mutex;
    std::condition_variable connect_done;
    std::vector<ConnectTask *> connect_queue;

    // M2 carries one session.  `max_sessions` is validated and reported but not
    // yet honoured beyond one; fan-out is M5 and needs the capacity argument
    // from DESIGN, not just a bigger array.
    std::mutex session_mutex;
    ucp_ep_h endpoint{nullptr};
    Protocol::SessionId session_id{};
    Protocol::StreamId stream_id{0};
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

    if(!impl_->session_armed.load(std::memory_order_acquire))
    {
        // 4.2: arm-before-send.  A frame submitted to a session that is not
        // Armed could land in a receive ring that is not registered yet.
        impl_->counters.dropped_no_session.fetch_add(1, std::memory_order_relaxed);
        impl_->dropped_before.fetch_add(1, std::memory_order_relaxed);
        lease.reset();
        return PublishResult::NoSession;
    }

    const std::uint64_t admitted = impl_->admitted.load(std::memory_order_acquire);
    const std::uint64_t credited = impl_->credited.load(std::memory_order_acquire);
    if(admitted - credited >= impl_->config.credit_window)
    {
        impl_->counters.dropped_credit_stalled.fetch_add(1, std::memory_order_relaxed);
        impl_->dropped_before.fetch_add(1, std::memory_order_relaxed);
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
    // 9.3 puts geometry changes out of scope for M2, and 4.3's interlock is not
    // something to approximate: it has to hold both rings registered, route by
    // generation, and free the old ring only when both the ack has arrived and
    // the old epoch has drained.  Returning a status beats a partial version.
    return Status::Internal;
}

std::uint32_t BulkPublisher::generation() const noexcept
{
    return impl_->generation;
}

std::size_t BulkPublisher::session_count() const noexcept
{
    return impl_->session_armed.load(std::memory_order_acquire) ? 1u : 0u;
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
    // the M2 tests drive the whole session lifecycle through it with no Tango
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
        case Protocol::CoordType::Close:
            return impl_->handle_close(data, size, envelope.correlation_id);
        default:
            // Renew, Query and the rest arrive with M3.  Saying so beats a stub
            // that answers Ok and quietly does nothing.
            return Protocol::encode(
                Protocol::ErrorMessage{Status::Internal,
                                       std::string(Protocol::to_string(envelope.msg_type)) +
                                           " is not implemented before M3"},
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

    std::lock_guard<std::mutex> lock(session_mutex);

    // 3.5 step 4.  M2 has room for one; 4.4's rule that a duplicate Open makes a
    // *new* session rather than reusing one still holds -- there is simply
    // nowhere to put the second until M3.
    if(endpoint != nullptr)
    {
        counters.sessions_rejected.fetch_add(1, std::memory_order_relaxed);
        return Protocol::encode(
            Protocol::ErrorMessage{Status::TooManySessions, "M2 supports a single session"},
            correlation_id);
    }

    // 3.5 step 5: clamp downward.  A grant is never larger than requested and
    // never larger than configured; clamping is normal and is reported in the
    // reply rather than raised as an error.
    Protocol::GeometryBlock geometry;
    geometry.generation = generation;
    geometry.element_type = ElementType::Byte;
    geometry.element_size = 1;
    geometry.rank = 0;
    geometry.max_frame_bytes =
        std::min(request.requested_max_frame_bytes, config.max_frame_bytes);
    geometry.ring_depth = std::min(request.requested_ring_depth, config.ring_depth);
    geometry.credit_window =
        std::min(request.requested_credit_window, config.credit_window);

    if(geometry.max_frame_bytes == 0 || geometry.ring_depth == 0 || geometry.credit_window == 0)
    {
        return Protocol::encode(
            Protocol::ErrorMessage{Status::MalformedMessage, "Open requested a zero geometry"},
            correlation_id);
    }

    // 3.5 step 7.  The endpoint has to exist before the session is armed, and
    // creating it is a ucp_* call, so it goes to the engine and this thread
    // waits.
    endpoint = connect(request.client_ucx_address.data(), request.client_ucx_address.size());
    session_id = Protocol::generate_session_id();
    stream_id = Protocol::generate_stream_id();

    // M2 arms on Open rather than on ProbeAck.
    //
    // 4.2 makes arm-before-send mandatory to stop a frame landing in a receive
    // ring that is not registered yet.  The subscriber registers and arms its
    // ring before it sends Open (4.1, `Opening` entry action), so by the time
    // this runs the ring exists; what the probe adds on top is proof of
    // *reachability*, which is what M3 restores.  Recorded here rather than in a
    // commit message because it is a deliberate weakening of a MUST.
    session_armed.store(true, std::memory_order_release);
    counters.sessions_opened.fetch_add(1, std::memory_order_relaxed);

    Protocol::OpenReply reply;
    reply.version_selected = Protocol::k_version_major;
    reply.status = Status::Ok;
    reply.granted_caps = request.requested_caps & Protocol::k_caps_credit_coalescing;
    reply.session_id = session_id;
    reply.stream_id = stream_id;
    reply.lease_ttl_ms = config.lease_ttl_ms;
    reply.renew_interval_ms = config.renew_interval_ms;
    reply.transport_selected = Protocol::Transport::ActiveMessage;
    reply.server_epoch_id = server_epoch_id;
    reply.geometry = geometry;
    reply.server_ucx_address = worker.address();

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

    std::lock_guard<std::mutex> lock(session_mutex);

    Protocol::CloseReply reply;
    reply.session_id = request.session_id;

    // 4.4: a duplicate Close is UnknownSession, no work performed, and not an
    // error -- a client retrying a close it already made is behaving correctly.
    if(endpoint == nullptr || request.session_id != session_id)
    {
        reply.status = Status::UnknownSession;
        return Protocol::encode(reply, correlation_id);
    }

    // Teardown belongs to the engine thread, because steps 2 and 3 are ucp_*
    // calls on the worker.  Stopping the engine runs it in the right order on
    // the way out; M3 replaces this with a per-session Expiring transition so a
    // close stops being a whole-publisher event.
    running.store(false, std::memory_order_release);
    if(engine.joinable())
    {
        engine.join();
    }

    reply.status = Status::Ok;
    reply.frames_credited_final = static_cast<std::uint32_t>(
        counters.frames_credited.load(std::memory_order_relaxed));

    return Protocol::encode(reply, correlation_id);
}

} // namespace TangoBulk
