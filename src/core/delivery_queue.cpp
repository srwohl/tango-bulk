// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <core/delivery_queue.h>

#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <thread>

namespace TangoBulk::detail
{

DeliveryQueue::DeliveryQueue(std::size_t capacity, DropPolicy policy) :
    queue_(capacity),
    policy_(policy)
{
    wakeup_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
}

DeliveryQueue::~DeliveryQueue()
{
    if(wakeup_fd_ >= 0)
    {
        ::close(wakeup_fd_);
    }

    // Frames still queued die here, each returning its credit. The interlock is
    // the FrameView, not this queue.
}

bool DeliveryQueue::push(FrameView frame) noexcept
{
    bool queued = queue_.try_push(std::move(frame));

    if(queued)
    {
        const std::uint64_t depth = queue_.size();
        if(depth > high_water_.load(std::memory_order_relaxed))
        {
            high_water_.store(depth, std::memory_order_relaxed);
        }
    }
    else
    {
        if(policy_ == DropPolicy::DropOldest)
        {
            FrameView oldest;
            if(queue_.try_pop(oldest))
            {
                // Destroying the head returns its credit, which is the room.
                oldest.reset();
                queued = queue_.try_push(std::move(frame));
            }
        }

        // Whatever was not queued goes out of scope here and returns its credit.
        dropped_.fetch_add(1, std::memory_order_relaxed);
    }

    // Only on the armed-to-disarmed transition, so a consumer that is keeping up
    // costs no syscall.
    if(wakeup_fd_ >= 0 && !queue_.empty() &&
       consumer_waiting_.exchange(false, std::memory_order_acq_rel))
    {
        const std::uint64_t one = 1;
        const ssize_t written = ::write(wakeup_fd_, &one, sizeof(one));
        static_cast<void>(written); // EAGAIN: already signalled
    }

    return queued;
}

std::size_t DeliveryQueue::discard() noexcept
{
    std::size_t dropped = 0;

    FrameView frame;
    while(queue_.try_pop(frame))
    {
        frame.reset();
        ++dropped;
    }

    discarded_.fetch_add(dropped, std::memory_order_relaxed);
    return dropped;
}

void DeliveryQueue::stop() noexcept
{
    // The flag before the descriptor: a consumer woken by the write must be able
    // to see why.
    stopped_.store(true, std::memory_order_release);

    if(wakeup_fd_ < 0)
    {
        return;
    }

    // Unconditional, unlike the wakeup in push(): every waiter has to come back,
    // armed or not.
    consumer_waiting_.store(false, std::memory_order_release);

    const std::uint64_t one = 1;
    const ssize_t written = ::write(wakeup_fd_, &one, sizeof(one));
    static_cast<void>(written);
}

void DeliveryQueue::drain() noexcept
{
    if(wakeup_fd_ < 0)
    {
        return;
    }

    // A notification, not a count: the queue is the truth about how many frames
    // there are, so one read clears however many writes.
    std::uint64_t drained = 0;
    const ssize_t got = ::read(wakeup_fd_, &drained, sizeof(drained));
    static_cast<void>(got); // EAGAIN: nothing pending
}

bool DeliveryQueue::await(std::chrono::steady_clock::time_point deadline) noexcept
{
    const auto now = std::chrono::steady_clock::now();
    if(now >= deadline)
    {
        return false;
    }

    const auto remaining =
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count();

    if(wakeup_fd_ < 0)
    {
        std::this_thread::sleep_for(std::chrono::microseconds(50));
        return true;
    }

    // Arm before re-checking. The other order loses wakeups: the producer can
    // push and find the flag clear between the check and the block.
    arm();

    if(!queue_.empty())
    {
        consumer_waiting_.store(false, std::memory_order_release);
        return true;
    }

    pollfd descriptor{};
    descriptor.fd = wakeup_fd_;
    descriptor.events = POLLIN;

    const int ready = ::poll(&descriptor, 1, remaining > 0 ? static_cast<int>(remaining) : 1);

    consumer_waiting_.store(false, std::memory_order_release);

    if(ready > 0)
    {
        drain();
    }

    // ready == 0 is the timeout, ready < 0 is EINTR. Both mean look again; the
    // caller's deadline decides whether to keep waiting.
    return true;
}

bool DeliveryQueue::try_take(FrameView &out) noexcept
{
    if(queue_.try_pop(out))
    {
        taken_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    // Arm and re-check before reporting empty, so a caller watching fd() cannot
    // wait for a descriptor nobody is going to write.
    arm();

    if(queue_.try_pop(out))
    {
        consumer_waiting_.store(false, std::memory_order_release);
        taken_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    return false;
}

bool DeliveryQueue::take(FrameView &out, std::chrono::steady_clock::time_point deadline) noexcept
{
    for(;;)
    {
        // Through try_take() so that every empty call leaves the consumer armed,
        // a zero-timeout take() included.
        if(try_take(out))
        {
            return true;
        }

        // After the take, so a stopped queue still hands over what it holds.
        if(stopped_.load(std::memory_order_acquire) || !await(deadline))
        {
            return false;
        }
    }
}

DeliveryQueue::Stats DeliveryQueue::stats() const noexcept
{
    Stats out;
    out.depth = queue_.size();
    out.capacity = queue_.capacity();
    out.taken = taken_.load(std::memory_order_relaxed);
    out.dropped = dropped_.load(std::memory_order_relaxed);
    out.discarded = discarded_.load(std::memory_order_relaxed);
    out.high_water = high_water_.load(std::memory_order_relaxed);
    return out;
}

} // namespace TangoBulk::detail
