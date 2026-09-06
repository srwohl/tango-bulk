// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_CORE_DELIVERY_QUEUE_H
#define TANGO_BULK_SRC_CORE_DELIVERY_QUEUE_H

#include <core/bounded_queue.h>

#include <tango-bulk/frame.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>

namespace TangoBulk::detail
{

class DeliveryQueue
{
  public:
    struct Stats
    {
        std::size_t depth{0};
        std::size_t capacity{0};
        std::uint64_t taken{0};      ///< handed to a consumer
        std::uint64_t dropped{0};    ///< refused or evicted by the queue policy
        std::uint64_t discarded{0};  ///< let go because a session retired
        std::uint64_t high_water{0};
    };

    DeliveryQueue(std::size_t capacity, DropPolicy policy);
    ~DeliveryQueue();

    DeliveryQueue(const DeliveryQueue &) = delete;
    DeliveryQueue &operator=(const DeliveryQueue &) = delete;

    bool push(FrameView frame) noexcept;

    bool try_take(FrameView &out) noexcept;

    bool take(FrameView &out, std::chrono::steady_clock::time_point deadline) noexcept;

    void stop() noexcept;

    std::size_t discard() noexcept;

    int fd() const noexcept
    {
        return wakeup_fd_;
    }

    Stats stats() const noexcept;

  private:
    void arm() noexcept
    {
        consumer_waiting_.store(true, std::memory_order_release);
    }

    void drain() noexcept;

    bool await(std::chrono::steady_clock::time_point deadline) noexcept;

    BoundedQueue<FrameView> queue_;
    DropPolicy policy_;

    int wakeup_fd_{-1};

    std::atomic<bool> consumer_waiting_{false};

    std::atomic<bool> stopped_{false};
    std::atomic<std::uint64_t> taken_{0};
    std::atomic<std::uint64_t> dropped_{0};
    std::atomic<std::uint64_t> discarded_{0};
    std::atomic<std::uint64_t> high_water_{0};
};

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_CORE_DELIVERY_QUEUE_H
