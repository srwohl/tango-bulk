// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_UCX_SUBSCRIBER_ENGINE_H
#define TANGO_BULK_SRC_UCX_SUBSCRIBER_ENGINE_H

#include <ucx/locality.h>
#include <ucx/registered_ring.h>
#include <ucx/ucx_context.h>

#include <core/bounded_queue.h>
#include <core/copy_pool.h>
#include <core/credit_window.h>
#include <core/delivery_queue.h>
#include <core/lease_pool.h>
#include <core/receive_slot.h>
#include <core/subscriber_transport.h>

#include <core/protocol.h>
#include <tango-bulk/subscriber.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

/// The UCX receive engine behind the internal SubscriberTransport seam.
/// Coordination remains outside this layer, so the engine can be tested with
/// protocol bytes without a Tango device proxy.
namespace TangoBulk::detail
{

/// Everything a delivered `FrameView` must keep alive.
///
/// The registered ring, the slot descriptions, the control-block pool, and the
/// credit-return queue, in one object owned by `shared_ptr`.  Every lease holds a
/// reference, so a view that outlives its subscriber keeps the memory it points
/// at mapped and has somewhere safe to put its credit -- and the `ucp_mem_unmap`
/// happens when the last of them goes, not when the engine stops.
///
/// It holds the `UcxContext` too, because unmapping needs the context to still
/// exist.  Getting that wrong is not a leak; it is an unmap against a freed
/// context during teardown.
class ReceiveArena : public CreditSink
{
  public:
    /// An empty `region.owner` allocates a host ring; otherwise the caller's
    /// region is registered and its ownership shared for as long as any view
    /// or in-flight receive refers to it. `copy_buffers` above zero builds the
    /// copied-delivery pool, that many buffers of `slot_bytes`.
    ReceiveArena(std::shared_ptr<UcxContext> context,
                 std::uint64_t slot_bytes,
                 std::uint32_t depth,
                 std::uint64_t pinned_limit,
                 ReceiveRegion region,
                 std::size_t copy_buffers);

    /// 5.5: called from the lease destructor, on any thread.  Pushes and
    /// nothing else -- no blocking, no allocation, no UCX.
    void return_credit(std::uint64_t sequence) noexcept override;

    /// Engine thread only.
    bool try_pop_credit(std::uint64_t &sequence) noexcept
    {
        return credit_returns_.try_pop(sequence);
    }

    ReceiveSlot &slot(std::size_t index) noexcept
    {
        return slots_[index];
    }

    RegisteredRing &ring() noexcept
    {
        return ring_;
    }

    /// By `shared_ptr` because a lease's control block is allocated *from* this
    /// pool while the lease keeps this arena alive; see PoolAllocator for the
    /// use-after-free that ownership prevents.
    const std::shared_ptr<LeasePool> &lease_pool() noexcept
    {
        return lease_pool_;
    }

    /// Null unless the arena was built for copied delivery.
    const std::shared_ptr<CopyPool> &copy_pool() const noexcept
    {
        return copy_pool_;
    }

    std::uint64_t credits_returned() const noexcept
    {
        return credits_returned_.load(std::memory_order_relaxed);
    }

    /// A credit that returned on the engine thread at delivery, without a
    /// lease: copied delivery.
    void note_credit_returned() noexcept
    {
        credits_returned_.fetch_add(1, std::memory_order_relaxed);
    }

    std::uint64_t views_outstanding() const noexcept
    {
        return views_outstanding_.load(std::memory_order_relaxed);
    }

    void note_view_issued() noexcept
    {
        views_outstanding_.fetch_add(1, std::memory_order_relaxed);
    }

  private:
    std::shared_ptr<UcxContext> context_;
    RegisteredRing ring_;
    std::vector<ReceiveSlot> slots_;

    // 5.3: capacity 2 x ring_depth makes overflow unreachable, because at most
    // ring_depth credits can be outstanding.  The spec requires an assert rather
    // than handling on overflow, since a full credit queue would deadlock the
    // window and a silently dropped credit is unrecoverable.
    BoundedQueue<std::uint64_t> credit_returns_;

    std::shared_ptr<LeasePool> lease_pool_;
    std::shared_ptr<CopyPool> copy_pool_;
    std::atomic<std::uint64_t> credits_returned_{0};
    std::atomic<std::uint64_t> views_outstanding_{0};
};

class SubscriberEngine final : public SubscriberTransport
{
  public:
    SubscriberEngine(ReceivePlan plan,
                     DeliveryOwnership ownership,
                     FlowPolicy flow,
                     ReceiveRegion region,
                     std::uint64_t pinned_limit,
                     TransportOptions options,
                     std::shared_ptr<DeliveryIngress> delivery);
    ~SubscriberEngine() override;

    const std::vector<std::byte> &local_address() const noexcept override
    {
        return worker_->address();
    }

    Status activate(Protocol::StreamId stream_id,
                    const Geometry &granted,
                    const std::vector<std::byte> &server_address) override;

    BulkError last_error() const noexcept override;

    SubscriberState state() const noexcept override
    {
        return state_.load(std::memory_order_acquire);
    }

    SubscriberCounters counters() const noexcept override;

    /// Where the registered receive ring actually is.
    ///
    /// The one instrumentation seam this class offers, for RFC 9.3's zero-copy
    /// criterion: the delivered payload must live inside the registered ring,
    /// "verified by pointer/registration instrumentation, not by inspection".
    /// This is the pointer half; `counters().bytes_copied` is the registration
    /// half.  It reports the ring's own geometry rather than answering
    /// questions about it, so the assertions -- is this pointer in the ring,
    /// does slot `sequence % depth` hold this frame -- are written where they
    /// are made instead of as library members with one caller each.
    struct RingPlacement
    {
        const std::byte *base{nullptr};
        std::size_t slot_bytes{0};
        std::uint32_t depth{0};
    };

    RingPlacement ring_placement() const noexcept;

    /// Observed placement, valid once the state reaches `Active`.
    const Locality &locality() const noexcept
    {
        return locality_;
    }

  private:
    struct Pending;

    void engine_loop();
    void register_am_handlers();
    bool drain_credit_returns();
    bool send_pending_credit();

    /// 4.1's `Probing` exit: answer the publisher's `Probe` and become `Active`.
    ///
    /// Sent from the loop rather than from the AM callback that received the
    /// probe, for the same reason credit is: a callback runs inside
    /// `ucp_worker_progress`, and this engine does not re-enter UCX from there.
    bool send_pending_probe_ack();

    /// Bring the worker to a state where destroying it is safe, whatever the
    /// peer did.  Engine thread, after the loop has stopped.  Returns false if
    /// it could not prove that -- the caller must then not destroy the worker.
    bool quiesce() noexcept;

    /// Progress until no rendezvous receive is outstanding, or the budget runs
    /// out.  Returns whether it reached zero.
    bool drain_inflight_receives() noexcept;

    static ucs_status_t on_frame_am(void *arg,
                                    const void *header,
                                    std::size_t header_length,
                                    void *data,
                                    std::size_t length,
                                    const ucp_am_recv_param_t *param) noexcept;

    static void on_rndv_complete(void *request,
                                 ucs_status_t status,
                                 std::size_t length,
                                 void *user_data) noexcept;

    static void on_credit_sent(void *request, ucs_status_t status, void *user_data) noexcept;

    static ucs_status_t on_probe_am(void *arg,
                                    const void *header,
                                    std::size_t header_length,
                                    void *data,
                                    std::size_t length,
                                    const ucp_am_recv_param_t *param) noexcept;

    ucs_status_t handle_frame(const std::byte *header,
                              std::size_t header_length,
                              void *data,
                              std::size_t length,
                              const ucp_am_recv_param_t *param) noexcept;

    void commit(std::size_t slot_index) noexcept;

    /// Copied delivery: copy the slot out, free it, queue the copy. When the
    /// credit goes back with it depends on the flow; see the definition.
    void commit_copied(ReceiveSlot &slot) noexcept;

    /// Whether a copied frame's credit waits for the application to take it.
    bool defers_credit_to_handoff() const noexcept
    {
        return ownership_ == DeliveryOwnership::Copy && flow_ == FlowPolicy::Lossless;
    }

    void fail(Status status, const char *reason) noexcept;

    MemoryKind memory_kind_;
    DeliveryOwnership ownership_;
    FlowPolicy flow_;
    int engine_cpu_affinity_;
    std::shared_ptr<UcxContext> context_;
    std::unique_ptr<UcxWorker> worker_;
    std::shared_ptr<ReceiveArena> arena_;
    std::shared_ptr<CreditSink> sink_; ///< arena_, as the leases see it

    ReleaseTracker tracker_;

    std::shared_ptr<DeliveryIngress> delivery_;

    std::vector<Pending> pending_; ///< engine thread only; indexed by slot

    /// Rendezvous receives currently in flight.  Engine thread only -- raised in
    /// the AM handler, lowered in the completion callback, both of which run
    /// inside `ucp_worker_progress`.
    std::size_t rndv_inflight_{0};

    /// Set once `quiesce()` starts, to stop the AM handler taking on new work.
    ///
    /// Not an atomic and not part of `SubscriberState`: both the write and the
    /// read happen on the engine thread, because `quiesce()` reaches the AM
    /// handler through its own `ucp_worker_progress` calls.  That is exactly the
    /// problem it solves -- without it, draining outstanding receives starts
    /// fresh ones and the drain cannot finish while a publisher is still
    /// sending.
    bool closing_{false};

    /// A `Probe` received and not yet answered.  Written by the probe AM
    /// handler, cleared by the loop that sends the ack; both are the engine.
    std::uint64_t pending_probe_token_{0};
    bool probe_ack_pending_{false};

    std::thread engine_;
    std::atomic<bool> running_{true};

    /// Whether `quiesce()` proved the worker safe to destroy.  Written by the
    /// engine thread as its last act, read by the destructor after the join.
    std::atomic<bool> quiesced_{false};
    std::atomic<SubscriberState> state_{SubscriberState::Closed};

    std::atomic<const char *> failure_reason_{nullptr};
    std::atomic<Status> failure_status_{Status::Ok};

    ucp_ep_h endpoint_{nullptr};

    Protocol::StreamId stream_id_{0};
    Geometry granted_{};

    /// Where this subscriber's ring, NIC and engine landed.  Engine thread
    /// writes it when the probe is answered; read for diagnostics only.
    Locality locality_;


    std::atomic<std::uint64_t> frames_received_{0};
    std::atomic<std::uint64_t> dropped_bad_header_{0};
    std::atomic<std::uint64_t> dropped_oversize_{0};
    std::atomic<std::uint64_t> dropped_stale_epoch_{0};
    std::atomic<std::uint64_t> dropped_duplicate_seq_{0};
    std::atomic<std::uint64_t> dropped_geometry_mismatch_{0};
    std::atomic<std::uint64_t> credit_messages_sent_{0};
    std::atomic<std::uint64_t> transport_errors_{0};
    std::atomic<std::uint64_t> frames_copied_{0};
    std::atomic<std::uint64_t> bytes_copied_{0};
};

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_UCX_SUBSCRIBER_ENGINE_H
