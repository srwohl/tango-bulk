// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <core/delivery_queue.h>

#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
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

    // Whatever is still queued dies here, and each frame's destructor returns
    // its credit to whichever session delivered it. Nothing else is needed:
    // the credit interlock is the FrameView, not this queue.
}

bool DeliveryQueue::push(FrameView frame) noexcept
{
    if(queue_.try_push(std::move(frame)))
    {
        const std::uint64_t depth = queue_.size();
        if(depth > high_water_.load(std::memory_order_relaxed))
        {
            high_water_.store(depth, std::memory_order_relaxed);
        }
        return true;
    }

    // Full. Apply the queue policy, and never stall the producer to do it.
    if(policy_ == DropPolicy::DropOldest)
    {
        FrameView oldest;
        if(queue_.try_pop(oldest))
        {
            // Destroying the oldest view releases its slot and returns its
            // credit, which is what makes the room.
            oldest.reset();
            if(queue_.try_push(std::move(frame)))
            {
                dropped_.fetch_add(1, std::memory_order_relaxed);
                return true;
            }
        }
    }

    // DropNewest, or DropOldest that lost a race for the space it just made:
    // let `frame` go out of scope. Its destructor releases the slot and returns
    // the credit immediately, which is why there is nothing else to do here.
    dropped_.fetch_add(1, std::memory_order_relaxed);
    return false;
}

std::size_t DeliveryQueue::discard() noexcept
{
    std::size_t dropped = 0;

    FrameView frame;
    while(queue_.try_pop(frame))
    {
        // Destroying the view returns its credit to whichever session delivered
        // it, which is the right destination even when that session is retiring:
        // the arena outlives the transport precisely so this stays valid.
        frame.reset();
        ++dropped;
    }

    discarded_.fetch_add(dropped, std::memory_order_relaxed);
    return dropped;
}

void DeliveryQueue::notify() noexcept
{
    if(wakeup_fd_ < 0 || queue_.empty())
    {
        return;
    }

    // Only on the true-to-false transition. A consumer that is keeping up never
    // sets the flag, so the case that matters -- consumer behind, queue never
    // empty -- makes no syscall at all.
    if(!consumer_waiting_.exchange(false, std::memory_order_acq_rel))
    {
        return;
    }

    const std::uint64_t one = 1;
    const ssize_t written = ::write(wakeup_fd_, &one, sizeof(one));
    static_cast<void>(written); // EAGAIN means it is already signalled
}

void DeliveryQueue::stop() noexcept
{
    // The flag first, the descriptor second. A consumer woken by the write must
    // be able to see why it was woken; the other order lets it wake, re-check,
    // find the flag still clear and block again -- which is the failure this
    // exists to prevent.
    stopped_.store(true, std::memory_order_release);

    if(wakeup_fd_ < 0)
    {
        return;
    }

    // Unconditional, unlike notify(): every waiter has to come back, whether or
    // not one of them happened to be armed. The descriptor stays readable until
    // somebody drains it, which is what makes this safe against a consumer that
    // has not reached ::poll() yet.
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

    // The descriptor is a notification, not a count -- the queue is the truth
    // about how many frames there are, so one read clears however many writes.
    std::uint64_t drained = 0;
    const ssize_t got = ::read(wakeup_fd_, &drained, sizeof(drained));
    static_cast<void>(got); // EAGAIN: nothing pending, which is fine
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
        // No descriptor: the short sleep this replaced, and the same behaviour.
        std::this_thread::sleep_for(std::chrono::microseconds(50));
        return true;
    }

    // Declare the intent to block BEFORE looking at the queue again. The other
    // order loses wakeups: the producer could push and find the flag still
    // clear between the check and the block, and nothing would write the
    // descriptor.
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

    // Whoever gets here first clears it; the producer may already have.
    consumer_waiting_.store(false, std::memory_order_release);

    if(ready > 0)
    {
        drain();
    }

    // ready == 0 is the timeout and ready < 0 is EINTR -- a signal arriving,
    // which is the whole reason a blocking read is interruptible instead of
    // swallowing Ctrl-C for a full poll timeout. Both mean "look again", and
    // the caller's deadline decides whether to keep waiting.
    return true;
}

bool DeliveryQueue::try_take(FrameView &out) noexcept
{
    if(queue_.try_pop(out))
    {
        taken_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    // Arm and re-check before reporting empty. A caller watching fd() would
    // otherwise miss a frame pushed between the pop above and its next wait,
    // and would then wait for a descriptor nobody is going to write.
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
        // Through try_take() rather than straight to the queue, so that every
        // consumer call that comes up empty leaves the consumer armed -- this
        // one included, and a zero-timeout take() especially. A caller driving
        // its own event loop off fd() then needs no protocol of its own, which
        // is the rule ADR 0005 states and the reason arm/drain are private.
        if(try_take(out))
        {
            return true;
        }

        // Checked after the take, so a stopped queue still hands over what it
        // already holds, and before the wait, so nothing blocks after a stop.
        if(stopped() || !await(deadline))
        {
            return false;
        }
    }
}

} // namespace TangoBulk::detail
