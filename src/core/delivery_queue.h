// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_CORE_DELIVERY_QUEUE_H
#define TANGO_BULK_SRC_CORE_DELIVERY_QUEUE_H

#include <core/bounded_queue.h>

#include <tango-bulk/frame.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace TangoBulk::detail
{

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
        std::size_t capacity{0};
        std::uint64_t taken{0};      ///< handed to a consumer
        std::uint64_t dropped{0};    ///< refused or evicted by the queue policy
        std::uint64_t discarded{0};  ///< let go because a session or delivery ended
        std::uint64_t high_water{0};
    };

    DeliveryQueue(std::size_t capacity, DropPolicy policy);
    ~DeliveryQueue();

    DeliveryQueue(const DeliveryQueue &) = delete;
    DeliveryQueue &operator=(const DeliveryQueue &) = delete;

    bool push(FrameView frame) noexcept;

    DeliveryRead try_read_result();

    DeliveryRead read_result(std::chrono::steady_clock::time_point deadline);

    bool try_take(FrameView &out) noexcept;

    bool take(FrameView &out, std::chrono::steady_clock::time_point deadline) noexcept;

    /// Accepted frames remain claimable before a final session failure is
    /// reported. Close and interrupt instead discard queued frames first.
    void fail(BulkError error) noexcept;

    void callback_failed(BulkError error) noexcept;

    void close() noexcept;

    void interrupt() noexcept;

    std::size_t discard() noexcept;

    /// Each transport receives a capability sharing this queue's storage and
    /// descriptor. Retiring it makes late transport progress harmless.
    std::shared_ptr<DeliveryQueue> make_ingress();

    void retire_ingress() noexcept;

    int fd() const noexcept;

    Stats stats() const noexcept;

    // Definitions stay private to delivery_queue.cpp; the forward declarations
    // are public only so its small non-member synchronization helpers can use
    // the opaque shared state without exposing queue storage.
    struct State;
    struct Ingress;

  private:
    DeliveryQueue(std::shared_ptr<State> state, std::shared_ptr<Ingress> ingress) noexcept;

    std::shared_ptr<State> state_;
    std::shared_ptr<Ingress> ingress_;
};

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_CORE_DELIVERY_QUEUE_H
