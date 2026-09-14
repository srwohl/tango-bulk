// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_CORE_DELIVERY_QUEUE_H
#define TANGO_BULK_SRC_CORE_DELIVERY_QUEUE_H

#include <core/bounded_queue.h>

#include <tango-bulk/frame.h>
#include <tango-bulk/subscriber.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>

namespace TangoBulk::detail
{

class DeliveryIngress;

struct DeliveryRead
{
    enum class Kind : std::uint32_t
    {
        Frame,
        Empty,
        Timeout,
        Closed,
        Interrupted,
        SessionFailed,
        CallbackFailed,
    };

    Kind kind{Kind::Empty};
    FrameView frame;
    BulkError error{};
};

class DeliveryQueue
{
  public:
    struct Stats
    {
        std::size_t depth{0};
        std::uint64_t taken{0};      ///< handed to a consumer
        std::uint64_t dropped{0};    ///< refused or evicted by the queue policy
        std::uint64_t discarded{0};  ///< let go because a session or delivery ended
        std::uint64_t high_water{0};
    };

    /// Told the sequence of each frame as it is handed to the application.
    ///
    /// This is the one place the delivery queue and the credit path meet. A
    /// lossless copied frame has left its receive slot but has not reached
    /// anybody while it sits here, so its credit stays withheld until this
    /// fires -- otherwise the publisher would run ahead of a queue that then
    /// has to drop, which is the loss the session was promised would not
    /// happen. Every other combination returns credit elsewhere and installs
    /// no handler.
    ///
    /// Runs on the claiming thread, inside a noexcept path: it must not throw,
    /// block or allocate.
    using ClaimHandler = std::function<void(std::uint64_t sequence)>;

    DeliveryQueue(std::size_t capacity, QueuePolicy policy);
    ~DeliveryQueue();

    DeliveryQueue(const DeliveryQueue &) = delete;
    DeliveryQueue &operator=(const DeliveryQueue &) = delete;

    bool push(FrameView frame) noexcept;

    DeliveryRead try_read_result();

    DeliveryRead read_result(std::chrono::steady_clock::time_point deadline);

    /// Accepted frames remain claimable before a final session failure is
    /// reported. Close and interrupt instead discard queued frames first.
    void fail(BulkError error) noexcept;

    void callback_failed(BulkError error) noexcept;

    void close() noexcept;

    void interrupt() noexcept;

    std::size_t discard() noexcept;

    /// Each transport receives a capability sharing this queue's storage.
    /// Retiring it makes late transport progress harmless.
    std::shared_ptr<DeliveryIngress> make_ingress();

    void retire_ingress() noexcept;

    int fd() const noexcept;

    Stats stats() const noexcept;

    // Definitions stay private to delivery_queue.cpp; the forward declarations
    // are public only so its small non-member synchronization helpers can use
    // the opaque shared state without exposing queue storage.
    struct State;
    struct Ingress;

  private:
    friend class DeliveryIngress;

    bool push_from(const std::shared_ptr<Ingress> &ingress, FrameView frame) noexcept;

    std::shared_ptr<State> state_;
};

/// The only capability a transport needs: submit an accepted frame. Queue
/// storage, wakeups, and terminal policy remain owned by DeliveryQueue.
class DeliveryIngress
{
  public:
    bool push(FrameView frame) noexcept;

    /// Install the queue's claim handler. Called once by the transport that
    /// owns this ingress, before any frame is pushed through it.
    void set_claim_handler(DeliveryQueue::ClaimHandler handler);

  private:
    friend class DeliveryQueue;

    explicit DeliveryIngress(std::shared_ptr<DeliveryQueue::Ingress> ingress) noexcept;

    std::shared_ptr<DeliveryQueue::Ingress> ingress_;
};

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_CORE_DELIVERY_QUEUE_H
