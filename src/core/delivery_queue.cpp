// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <core/delivery_queue.h>

#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <limits>
#include <mutex>
#include <thread>

namespace TangoBulk::detail
{

namespace
{

enum class Terminal : std::uint32_t
{
    None,
    Closed,
    Interrupted,
    SessionFailed,
    CallbackFailed,
};

DeliveryRead::Kind read_kind(Terminal terminal) noexcept
{
    switch(terminal)
    {
    case Terminal::Closed:
        return DeliveryRead::Kind::Closed;
    case Terminal::Interrupted:
        return DeliveryRead::Kind::Interrupted;
    case Terminal::SessionFailed:
        return DeliveryRead::Kind::SessionFailed;
    case Terminal::CallbackFailed:
        return DeliveryRead::Kind::CallbackFailed;
    case Terminal::None:
        break;
    }
    return DeliveryRead::Kind::Empty;
}

} // namespace

struct DeliveryQueue::Ingress
{
    std::weak_ptr<State> state;
    std::atomic<bool> retired{false};
    std::atomic<std::uint64_t> active_pushes{0};
};

struct DeliveryQueue::State
{
    State(std::size_t capacity, DropPolicy drop_policy) : queue(capacity), policy(drop_policy)
    {
        wakeup_fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC | EFD_SEMAPHORE);
    }

    ~State()
    {
        if(wakeup_fd >= 0)
        {
            ::close(wakeup_fd);
        }
    }

    BoundedQueue<FrameView> queue;
    DropPolicy policy;
    int wakeup_fd{-1};

    // A producer enters this barrier before observing accepting. Terminal
    // publication waits for the barrier, so every accepted frame is visible
    // before SessionFailed becomes observable.
    std::atomic<std::uint64_t> active_pushes{0};
    std::atomic<bool> accepting{true};
    std::atomic<Terminal> terminal{Terminal::None};

    std::atomic<std::uint64_t> waiting_readers{0};
    std::atomic<bool> armed_reader{false};
    std::mutex terminal_mutex;
    std::mutex transition_mutex;
    BulkError terminal_error;

    std::mutex ingress_mutex;
    std::shared_ptr<DeliveryQueue::Ingress> current_ingress;

    std::atomic<std::uint64_t> taken{0};
    std::atomic<std::uint64_t> dropped{0};
    std::atomic<std::uint64_t> discarded{0};
    std::atomic<std::uint64_t> high_water{0};
};

DeliveryQueue::DeliveryQueue(std::size_t capacity, DropPolicy policy) :
    state_(std::make_shared<State>(capacity, policy))
{
}

DeliveryQueue::~DeliveryQueue() = default;

void signal_one(std::shared_ptr<DeliveryQueue::State> const &state) noexcept
{
    std::uint64_t waiting = state->waiting_readers.load(std::memory_order_acquire);
    while(waiting != 0 &&
          !state->waiting_readers.compare_exchange_weak(
              waiting, waiting - 1, std::memory_order_acq_rel, std::memory_order_acquire))
    {
    }

    if(waiting == 0 &&
       !state->armed_reader.exchange(false, std::memory_order_acq_rel))
    {
        return;
    }

    if(state->wakeup_fd < 0)
    {
        return;
    }

    const std::uint64_t one = 1;
    const ssize_t written = ::write(state->wakeup_fd, &one, sizeof(one));
    static_cast<void>(written); // EAGAIN means another wake token is pending.
}

void signal_all(std::shared_ptr<DeliveryQueue::State> const &state) noexcept
{
    const std::uint64_t waiting =
        state->waiting_readers.exchange(0, std::memory_order_acq_rel);
    if(state->wakeup_fd < 0)
    {
        return;
    }

    const std::uint64_t tokens = std::max<std::uint64_t>(waiting, 1);
    const ssize_t written = ::write(state->wakeup_fd, &tokens, sizeof(tokens));
    static_cast<void>(written);
}

void remove_waiter(std::shared_ptr<DeliveryQueue::State> const &state) noexcept
{
    std::uint64_t waiting = state->waiting_readers.load(std::memory_order_acquire);
    while(waiting != 0 &&
          !state->waiting_readers.compare_exchange_weak(
              waiting, waiting - 1, std::memory_order_acq_rel, std::memory_order_acquire))
    {
    }
}

void consume_one(std::shared_ptr<DeliveryQueue::State> const &state) noexcept
{
    if(state->wakeup_fd < 0)
    {
        return;
    }

    std::uint64_t token = 0;
    const ssize_t read = ::read(state->wakeup_fd, &token, sizeof(token));
    static_cast<void>(read); // EAGAIN means this frame had no waiter token.
}

void drain_wakeups(std::shared_ptr<DeliveryQueue::State> const &state) noexcept
{
    if(state->wakeup_fd < 0)
    {
        return;
    }

    std::uint64_t token = 0;
    while(::read(state->wakeup_fd, &token, sizeof(token)) > 0)
    {
    }
}

void update_high_water(std::shared_ptr<DeliveryQueue::State> const &state,
                       std::uint64_t depth) noexcept
{
    std::uint64_t high_water = state->high_water.load(std::memory_order_relaxed);
    while(depth > high_water &&
          !state->high_water.compare_exchange_weak(
              high_water, depth, std::memory_order_relaxed, std::memory_order_relaxed))
    {
    }
}

bool push_frame(std::shared_ptr<DeliveryQueue::State> const &state,
                std::shared_ptr<DeliveryQueue::Ingress> const &ingress,
                FrameView frame) noexcept
{
    if(ingress != nullptr)
    {
        ingress->active_pushes.fetch_add(1, std::memory_order_acq_rel);
    }
    state->active_pushes.fetch_add(1, std::memory_order_acq_rel);

    const bool current = ingress == nullptr || !ingress->retired.load(std::memory_order_acquire);
    const bool accepting = state->accepting.load(std::memory_order_acquire);
    bool queued = false;
    bool dropped = false;

    if(current && accepting)
    {
        queued = state->queue.try_push(std::move(frame));

        if(!queued && state->policy == DropPolicy::DropOldest)
        {
            FrameView oldest;
            if(state->queue.try_pop(oldest))
            {
                oldest.reset();
                consume_one(state);
                dropped = true;
                queued = state->queue.try_push(std::move(frame));
            }
        }

        if(!queued)
        {
            dropped = true;
        }
    }

    if(!current || !accepting)
    {
        frame.reset();
        state->discarded.fetch_add(1, std::memory_order_relaxed);
        if(ingress != nullptr)
        {
            ingress->active_pushes.fetch_sub(1, std::memory_order_acq_rel);
        }
        state->active_pushes.fetch_sub(1, std::memory_order_acq_rel);
        return false;
    }

    if(dropped)
    {
        state->dropped.fetch_add(1, std::memory_order_relaxed);
    }

    if(queued)
    {
        update_high_water(state, state->queue.size());
        signal_one(state);
    }

    if(ingress != nullptr)
    {
        ingress->active_pushes.fetch_sub(1, std::memory_order_acq_rel);
    }
    state->active_pushes.fetch_sub(1, std::memory_order_acq_rel);

    return queued;
}

bool DeliveryQueue::push_from(const std::shared_ptr<Ingress> &ingress, FrameView frame) noexcept
{
    return push_frame(state_, ingress, std::move(frame));
}

bool DeliveryQueue::push(FrameView frame) noexcept
{
    return push_from(nullptr, std::move(frame));
}

DeliveryIngress::DeliveryIngress(std::shared_ptr<DeliveryQueue::Ingress> ingress) noexcept :
    ingress_(std::move(ingress))
{
}

bool DeliveryIngress::push(FrameView frame) noexcept
{
    if(!ingress_)
    {
        frame.reset();
        return false;
    }

    const std::shared_ptr<DeliveryQueue::State> state = ingress_->state.lock();
    if(!state)
    {
        frame.reset();
        return false;
    }
    return push_frame(state, ingress_, std::move(frame));
}

std::shared_ptr<DeliveryIngress> DeliveryQueue::make_ingress()
{
    auto ingress = std::make_shared<Ingress>();
    ingress->state = state_;
    std::shared_ptr<Ingress> previous;
    {
        std::lock_guard<std::mutex> lock(state_->ingress_mutex);
        previous = std::move(state_->current_ingress);
        if(previous)
        {
            previous->retired.store(true, std::memory_order_release);
        }
        state_->current_ingress = ingress;
    }

    if(previous)
    {
        while(previous->active_pushes.load(std::memory_order_acquire) != 0)
        {
            std::this_thread::yield();
        }
    }

    return std::shared_ptr<DeliveryIngress>(new DeliveryIngress(std::move(ingress)));
}

void DeliveryQueue::retire_ingress() noexcept
{
    std::shared_ptr<Ingress> current;
    {
        std::lock_guard<std::mutex> lock(state_->ingress_mutex);
        current = std::move(state_->current_ingress);
        if(current)
        {
            current->retired.store(true, std::memory_order_release);
        }
    }

    if(current)
    {
        while(current->active_pushes.load(std::memory_order_acquire) != 0)
        {
            std::this_thread::yield();
        }
    }
}

std::size_t DeliveryQueue::discard() noexcept
{
    std::size_t discarded = 0;

    FrameView frame;
    while(state_->queue.try_pop(frame))
    {
        frame.reset();
        consume_one(state_);
        ++discarded;
    }

    state_->discarded.fetch_add(discarded, std::memory_order_relaxed);
    drain_wakeups(state_);
    return discarded;
}

void set_terminal(std::shared_ptr<DeliveryQueue::State> const &state,
                  Terminal terminal,
                  BulkError error,
                  bool wait_for_pushes,
                  bool discard) noexcept
{
    std::lock_guard<std::mutex> transition_lock(state->transition_mutex);
    if(state->terminal.load(std::memory_order_acquire) != Terminal::None)
    {
        return;
    }

    state->accepting.store(false, std::memory_order_release);
    while(wait_for_pushes && state->active_pushes.load(std::memory_order_acquire) != 0)
    {
        std::this_thread::yield();
    }

    if(discard)
    {
        FrameView frame;
        while(state->queue.try_pop(frame))
        {
            frame.reset();
            state->discarded.fetch_add(1, std::memory_order_relaxed);
        }
        drain_wakeups(state);
    }
    else if(terminal == Terminal::Interrupted)
    {
        // Interrupt is deliberately constant-time. The terminal boundary
        // makes queued frames unclaimable; the queue storage releases them
        // when the subscription is destroyed instead of walking it here.
        state->discarded.fetch_add(state->queue.size(), std::memory_order_relaxed);
    }

    {
        std::lock_guard<std::mutex> error_lock(state->terminal_mutex);
        state->terminal_error = std::move(error);
    }
    state->terminal.store(terminal, std::memory_order_release);
    signal_all(state);
}

void DeliveryQueue::fail(BulkError error) noexcept
{
    set_terminal(state_, Terminal::SessionFailed, std::move(error), true, false);
}

void DeliveryQueue::callback_failed(BulkError error) noexcept
{
    set_terminal(state_, Terminal::CallbackFailed, std::move(error), true, true);
}

void DeliveryQueue::close() noexcept
{
    set_terminal(state_, Terminal::Closed, BulkError{}, true, true);
}

void DeliveryQueue::interrupt() noexcept
{
    set_terminal(state_, Terminal::Interrupted, BulkError{}, false, false);
}

DeliveryQueue::Stats DeliveryQueue::stats() const noexcept
{
    Stats out;
    out.depth = state_->queue.size();
    out.capacity = state_->queue.capacity();
    out.taken = state_->taken.load(std::memory_order_relaxed);
    out.dropped = state_->dropped.load(std::memory_order_relaxed);
    out.discarded = state_->discarded.load(std::memory_order_relaxed);
    out.high_water = state_->high_water.load(std::memory_order_relaxed);
    if(state_->terminal.load(std::memory_order_acquire) == Terminal::Interrupted)
    {
        out.depth = 0;
    }
    return out;
}

int DeliveryQueue::fd() const noexcept
{
    return state_->wakeup_fd;
}

bool DeliveryQueue::try_take(FrameView &out) noexcept
{
    if(state_->queue.try_pop(out))
    {
        state_->armed_reader.store(false, std::memory_order_release);
        consume_one(state_);
        state_->taken.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    state_->armed_reader.store(true, std::memory_order_release);
    if(state_->queue.try_pop(out))
    {
        state_->armed_reader.store(false, std::memory_order_release);
        consume_one(state_);
        state_->taken.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
    return false;
}

bool take_one(std::shared_ptr<DeliveryQueue::State> const &state, FrameView &out) noexcept
{
    if(!state->queue.try_pop(out))
    {
        return false;
    }

    state->armed_reader.store(false, std::memory_order_release);
    consume_one(state);
    state->taken.fetch_add(1, std::memory_order_relaxed);
    return true;
}

bool await(std::shared_ptr<DeliveryQueue::State> const &state,
           std::chrono::steady_clock::time_point deadline) noexcept
{
    const auto now = std::chrono::steady_clock::now();
    if(now >= deadline)
    {
        return false;
    }

    state->waiting_readers.fetch_add(1, std::memory_order_acq_rel);
    state->armed_reader.store(false, std::memory_order_release);
    if(!state->queue.empty() || state->terminal.load(std::memory_order_acquire) != Terminal::None)
    {
        remove_waiter(state);
        return true;
    }

    if(state->wakeup_fd < 0)
    {
        while(std::chrono::steady_clock::now() < deadline &&
              state->queue.empty() &&
              state->terminal.load(std::memory_order_acquire) == Terminal::None)
        {
            std::this_thread::yield();
        }
        remove_waiter(state);
        return true;
    }

    const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(deadline - now);
    const auto bounded_remaining = std::min<std::int64_t>(
        remaining.count(), std::numeric_limits<int>::max());
    const int timeout_ms = static_cast<int>(std::max<std::int64_t>(1, bounded_remaining));

    pollfd descriptor{};
    descriptor.fd = state->wakeup_fd;
    descriptor.events = POLLIN;
    const int ready = ::poll(&descriptor, 1, timeout_ms);
    if(ready > 0)
    {
        consume_one(state);
    }
    remove_waiter(state);

    return ready > 0;
}

DeliveryRead terminal_result(std::shared_ptr<DeliveryQueue::State> const &state,
                              Terminal terminal)
{
    DeliveryRead result;
    result.kind = read_kind(terminal);
    if(terminal == Terminal::SessionFailed || terminal == Terminal::CallbackFailed)
    {
        std::lock_guard<std::mutex> lock(state->terminal_mutex);
        result.error = state->terminal_error;
    }
    return result;
}

DeliveryRead DeliveryQueue::try_read_result()
{
    DeliveryRead result;
    Terminal terminal = state_->terminal.load(std::memory_order_acquire);
    if(terminal != Terminal::None && terminal != Terminal::SessionFailed)
    {
        return terminal_result(state_, terminal);
    }

    if(take_one(state_, result.frame))
    {
        result.kind = DeliveryRead::Kind::Frame;
        return result;
    }

    terminal = state_->terminal.load(std::memory_order_acquire);
    if(terminal != Terminal::None)
    {
        return terminal_result(state_, terminal);
    }
    state_->armed_reader.store(true, std::memory_order_release);
    result.kind = DeliveryRead::Kind::Empty;
    return result;
}

DeliveryRead DeliveryQueue::read_result(std::chrono::steady_clock::time_point deadline)
{
    for(;;)
    {
        DeliveryRead result;
        Terminal terminal = state_->terminal.load(std::memory_order_acquire);
        if(terminal != Terminal::None && terminal != Terminal::SessionFailed)
        {
            return terminal_result(state_, terminal);
        }

        if(take_one(state_, result.frame))
        {
            result.kind = DeliveryRead::Kind::Frame;
            return result;
        }

        terminal = state_->terminal.load(std::memory_order_acquire);
        if(terminal != Terminal::None)
        {
            return terminal_result(state_, terminal);
        }

        if(!await(state_, deadline))
        {
            state_->armed_reader.store(true, std::memory_order_release);
            result.kind = DeliveryRead::Kind::Timeout;
            return result;
        }
    }
}

bool DeliveryQueue::take(FrameView &out,
                         std::chrono::steady_clock::time_point deadline) noexcept
{
    for(;;)
    {
        if(take_one(state_, out))
        {
            return true;
        }

        if(state_->terminal.load(std::memory_order_acquire) != Terminal::None ||
           !await(state_, deadline))
        {
            return false;
        }
    }
}

} // namespace TangoBulk::detail
