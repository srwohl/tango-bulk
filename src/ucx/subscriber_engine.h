// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_UCX_SUBSCRIBER_ENGINE_H
#define TANGO_BULK_SRC_UCX_SUBSCRIBER_ENGINE_H

#include <ucx/locality.h>
#include <ucx/registered_ring.h>
#include <ucx/ucx_context.h>

#include <core/bounded_queue.h>
#include <core/credit_window.h>
#include <core/lease_pool.h>
#include <core/receive_slot.h>
#include <core/subscriber_transport.h>

#include <tango-bulk/protocol.h>
#include <tango-bulk/subscriber.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

/// The receiving half of the transport, below `BulkSubscriber`.
///
/// This is deliberately **not** `BulkSubscriber`.  2.4 gives that class a
/// `Tango::DeviceProxy &` constructor parameter, and 1.1 forbids `src/ucx/` from
/// seeing `tango/*`; docs/EXTRACTION.md deviation 2 resolves the pair by putting
/// the transport engine in this layer and `BulkSubscriber` in the Tango layer,
/// meeting at an interface.  This is that engine, and the interface it meets is
/// `detail::SubscriberTransport` in `core/subscriber_transport.h`.
///
/// M4 built the other half: `BulkSubscriber` in `src/tango/proxy_client.cpp` is
/// a shell over this class, reached through that interface and constructed
/// through `make_subscriber_transport()`.  The UCX tests keep driving the
/// coordination bytes by hand -- which is what 9.3 asks for, and what keeps this
/// layer testable on a machine with no Tango database.
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
    ReceiveArena(std::shared_ptr<UcxContext> context,
                 std::uint64_t slot_bytes,
                 std::uint32_t depth,
                 std::uint64_t pinned_limit,
                 std::shared_ptr<void> receive_buffer,
                 std::uint64_t receive_buffer_bytes,
                 MemoryKind memory_kind);

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

    std::uint64_t credits_returned() const noexcept
    {
        return credits_returned_.load(std::memory_order_relaxed);
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
    std::atomic<std::uint64_t> credits_returned_{0};
    std::atomic<std::uint64_t> views_outstanding_{0};
};

class SubscriberEngine final : public SubscriberTransport
{
  public:
    explicit SubscriberEngine(SubscriberConfig config);
    ~SubscriberEngine() override;

    /// Encoded `Open`, to be carried to the publisher by whatever the caller
    /// has -- a Tango command from `BulkSubscriber`, a direct
    /// `handle_coordination` call in the tests here.
    ///
    /// By the time this returns, the receive ring is allocated, registered and
    /// armed: 4.1's `Opening` entry action, in that order, because the publisher
    /// may send the first frame the instant it replies.
    std::vector<std::byte> make_open_request(std::uint64_t correlation_id) const override;

    /// Adopt an `OpenReply`: stream id, granted geometry, endpoint to the
    /// server.  Starts the engine thread and leaves the state in `Probing`; the
    /// publisher's `Probe` moves it to `Active` (4.1).
    Status adopt_open_reply(const std::byte *data, std::size_t size) override;

    /// 3.7's lease renewal, as a pair of byte-level steps.
    ///
    /// There is no renew *timer* here.  5.1 puts one on the subscriber's control
    /// thread, and that thread's other job is calling `DeviceProxy` -- so the
    /// timer lives with the proxy, in `BulkSubscriber`, and this layer exposes
    /// the two ends exactly as it does for `Open` and `Close`.
    std::vector<std::byte> make_renew_request(std::uint64_t correlation_id) override;

    /// Adopt a `RenewReply`.  `SessionExpired` and `UnknownSession` are terminal
    /// for this session (3.7: "There is no resurrection"); the state goes to
    /// `Failed` and the caller must open a new one.
    Status adopt_renew_reply(const std::byte *data, std::size_t size) override;

    std::vector<std::byte> make_close_request(std::uint64_t correlation_id) const override;

    /// The lease terms the server granted, as they stand after the last reply.
    /// M4's renew timer reads these; 3.7 lets the server change them at any
    /// renewal and requires the client to adopt the new values.
    std::uint32_t lease_ttl_ms() const noexcept override
    {
        return lease_ttl_ms_;
    }

    std::uint32_t renew_interval_ms() const noexcept override
    {
        return renew_interval_ms_;
    }

    /// Invokes `cb` on the CALLING thread and returns how many frames it
    /// dispatched.  Which thread that is belongs to the layer above: a test
    /// calls this directly, and `BulkSubscriber` calls it from its dispatch
    /// thread.  Either way it is never the engine thread (5.2).
    std::size_t poll(std::chrono::milliseconds timeout, const FrameCallback &cb) override;

    SubscriberState state() const noexcept override
    {
        return state_.load(std::memory_order_acquire);
    }

    std::uint32_t generation() const noexcept override
    {
        return generation_;
    }

    SubscriberCounters counters() const noexcept override;

    /// Test instrumentation for 9.3's zero-copy criterion.
    ///
    /// The criterion asks for the delivered payload to live inside the
    /// registered receive ring "verified by pointer/registration
    /// instrumentation, not by inspection".  `ring_contains()` is the pointer
    /// half; `bytes_copied()` is the registration half -- it counts bytes that
    /// went through a staging copy, and for a rendezvous-sized frame it must
    /// stay at zero.
    bool ring_contains(const void *p) const noexcept;

    /// Address of receive slot `index`, so a test can assert 3.11's mapping --
    /// "the slot index is sequence % ring_depth" -- against where the payload
    /// actually landed rather than against a number the library reports.
    const std::byte *slot_address(std::size_t index) const noexcept;

    std::uint32_t granted_ring_depth() const noexcept
    {
        return granted_depth_;
    }

    std::uint64_t bytes_copied() const noexcept
    {
        return bytes_copied_.load(std::memory_order_relaxed);
    }

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

    SubscriberConfig config_;
    std::shared_ptr<UcxContext> context_;
    std::unique_ptr<UcxWorker> worker_;
    std::shared_ptr<ReceiveArena> arena_;
    std::shared_ptr<CreditSink> sink_; ///< arena_, as the leases see it

    ReleaseTracker tracker_;
    BoundedQueue<FrameView> delivery_;

    std::vector<Pending> pending_; ///< engine thread only; indexed by slot

    /// Rendezvous receives currently in flight.  Engine thread only -- raised in
    /// the AM handler, lowered in the completion callback, both of which run
    /// inside `ucp_worker_progress`.
    std::size_t rndv_inflight_{0};

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

    ucp_ep_h endpoint_{nullptr};
    Protocol::StreamId stream_id_{0};
    Protocol::SessionId session_id_{};
    std::uint32_t generation_{0};
    /// Where this subscriber's ring, NIC and engine landed.  Engine thread
    /// writes it when the probe is answered; read for diagnostics only.
    Locality locality_;

    std::uint32_t granted_depth_{0};
    std::uint64_t granted_frame_bytes_{0};
    std::uint32_t lease_ttl_ms_{0};
    std::uint32_t renew_interval_ms_{0};

    std::atomic<std::uint64_t> renewals_sent_{0};
    std::atomic<std::uint64_t> renewals_failed_{0};

    std::atomic<std::uint64_t> frames_received_{0};
    std::atomic<std::uint64_t> frames_delivered_{0};
    std::atomic<std::uint64_t> dropped_queue_full_{0};
    std::atomic<std::uint64_t> dropped_bad_header_{0};
    std::atomic<std::uint64_t> dropped_oversize_{0};
    std::atomic<std::uint64_t> dropped_stale_epoch_{0};
    std::atomic<std::uint64_t> dropped_duplicate_seq_{0};
    std::atomic<std::uint64_t> credit_messages_sent_{0};
    std::atomic<std::uint64_t> delivery_high_water_{0};
    std::atomic<std::uint64_t> transport_errors_{0};
    std::atomic<std::uint64_t> bytes_copied_{0};
};

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_UCX_SUBSCRIBER_ENGINE_H
