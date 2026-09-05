// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_TESTS_UNIT_FAKE_TRANSPORT_H
#define TANGO_BULK_TESTS_UNIT_FAKE_TRANSPORT_H

#include <tango-bulk/unstable/session_supervisor.h>

#include <atomic>
#include <chrono>
#include <array>
#include <deque>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

/// A transport and a coordination channel that can be told to misbehave.
///
/// This exists for one reason: a real publisher cannot be made to refuse a
/// renewal, grant a session and then never probe, or fail an open four times in
/// a row -- and those are the paths the control loop exists to handle. The
/// integration suite in tests/tango covers the happy paths against a real
/// device server and keeps covering them; nothing here replaces it.
///
/// Everything scripted lives in `Script`, not in the transport, because the
/// supervisor builds a *new* transport per session and a script that died with
/// the transport could not describe a reconnect.
namespace TangoBulkTests
{

using namespace TangoBulk;
using namespace std::chrono_literals;

struct Script
{
    std::mutex mutex;

    /// What each successive `adopt_open_reply` returns. Exhausted entries mean
    /// `Ok` -- a script says what is unusual and stays quiet otherwise.
    std::deque<Status> open_results;

    /// What each successive `adopt_renew_reply` returns.
    std::deque<Status> renew_results;

    /// Whether a granted session ever reaches `Active`. False models a
    /// publisher that answered `Open` but cannot reach this client's endpoint.
    bool probe_arrives{true};

    /// Set to make every grant after the first describe a different array, as a
    /// detector reconfigured between sessions would.
    bool reshape_after_first{false};

    /// Set to make the coordination channel throw, as an unreachable device
    /// does. Counted down; zero means "answer normally".
    int channel_throws{0};

    std::atomic<int> transports_built{0};
    std::atomic<int> opens{0};
    std::atomic<int> renews{0};
    std::atomic<int> closes{0};

    /// Which threads have called the channel, and whether two were ever inside
    /// it at once. The supervisor promises the calls never overlap -- which is
    /// what the Python binding's GIL story rests on -- so it is asserted rather
    /// than assumed. It does NOT promise a single thread: the first open runs
    /// on the caller's.
    std::set<std::thread::id> channel_threads;
    std::atomic<int> channel_inside{0};
    std::atomic<int> channel_max_concurrent{0};

    Status next_open()
    {
        std::lock_guard<std::mutex> lock(mutex);
        if(open_results.empty())
        {
            return Status::Ok;
        }
        const Status status = open_results.front();
        open_results.pop_front();
        return status;
    }

    Status next_renew()
    {
        std::lock_guard<std::mutex> lock(mutex);
        if(renew_results.empty())
        {
            return Status::Ok;
        }
        const Status status = renew_results.front();
        renew_results.pop_front();
        return status;
    }

    bool probes()
    {
        std::lock_guard<std::mutex> lock(mutex);
        return probe_arrives;
    }

    /// True from the second grant onward, once reshape_after_first is set.
    bool reshaped()
    {
        std::lock_guard<std::mutex> lock(mutex);
        const bool answer = reshape_after_first && grants_made > 0;
        ++grants_made;
        return answer;
    }

    int grants_made{0};
};

class FakeTransport final : public detail::SubscriberTransport
{
  public:
    explicit FakeTransport(Script &script) : script_(script)
    {
        script_.transports_built.fetch_add(1, std::memory_order_relaxed);
    }

    std::vector<std::byte> make_open_request(std::uint64_t) const override
    {
        return {std::byte{1}};
    }

    Status adopt_open_reply(const std::byte *, std::size_t) override
    {
        const Status status = script_.next_open();
        if(status != Status::Ok)
        {
            return status;
        }

        // A real engine reaches Active when the publisher's Probe is answered.
        // This is the knob a real one does not have.
        state_.store(script_.probes() ? SubscriberState::Active : SubscriberState::Probing,
                     std::memory_order_release);

        const bool reshaped = script_.reshaped();

        granted_ = Protocol::GeometryBlock{};
        granted_.generation = 1;
        granted_.element_type = ElementType::UInt16;
        granted_.element_size = 2;
        granted_.rank = 2;
        granted_.max_frame_bytes = 64u << 10;
        granted_.ring_depth = 4;
        granted_.credit_window = 2;
        granted_.shape = reshaped ? std::array<std::uint64_t, k_max_rank>{4, 16, 0, 0}
                                  : std::array<std::uint64_t, k_max_rank>{8, 8, 0, 0};
        granted_.strides = reshaped ? std::array<std::uint64_t, k_max_rank>{32, 2, 0, 0}
                                    : std::array<std::uint64_t, k_max_rank>{16, 2, 0, 0};

        generation_ = 1;
        return Status::Ok;
    }

    std::vector<std::byte> make_renew_request(std::uint64_t) override
    {
        return {std::byte{3}};
    }

    Status adopt_renew_reply(const std::byte *, std::size_t) override
    {
        return script_.next_renew();
    }

    std::vector<std::byte> make_close_request(std::uint64_t) const override
    {
        return {std::byte{5}};
    }

    std::uint32_t lease_ttl_ms() const noexcept override
    {
        return 200;
    }

    std::uint32_t renew_interval_ms() const noexcept override
    {
        return 20;
    }

    int fd() const noexcept override
    {
        return -1; // no data path, so nothing to wait on
    }

    void arm_wakeup() noexcept override {}
    void drain_wakeup() noexcept override {}

    std::size_t poll(std::chrono::milliseconds timeout,
                     const FrameCallback &,
                     std::size_t = 0) override
    {
        std::this_thread::sleep_for(std::min(timeout, std::chrono::milliseconds{2}));
        return 0;
    }

    BulkError last_error() const noexcept override
    {
        return failed_ ? BulkError{Status::TransportFailure, "the fake was told to fail", "transport"}
                       : BulkError{};
    }

    Protocol::GeometryBlock granted_geometry() const noexcept override
    {
        return granted_;
    }

    SubscriberState state() const noexcept override
    {
        return state_.load(std::memory_order_acquire);
    }

    std::uint32_t generation() const noexcept override
    {
        return generation_;
    }

    SubscriberCounters counters() const noexcept override
    {
        SubscriberCounters out;
        out.sessions_opened = 1;
        return out;
    }

    void fail() noexcept
    {
        failed_ = true;
        state_.store(SubscriberState::Failed, std::memory_order_release);
    }

  private:
    Script &script_;
    std::atomic<SubscriberState> state_{SubscriberState::Opening};
    std::uint32_t generation_{0};
    Protocol::GeometryBlock granted_{};
    std::atomic<bool> failed_{false};
};

/// A factory over a script, and the last transport it built -- so a test can
/// reach in and fail the live one.
inline detail::TransportFactory fake_factory(Script &script,
                                             std::shared_ptr<FakeTransport *> latest = nullptr)
{
    return [&script, latest](const SubscriberConfig &) -> std::unique_ptr<detail::SubscriberTransport>
    {
        auto transport = std::make_unique<FakeTransport>(script);
        if(latest)
        {
            *latest = transport.get();
        }
        return transport;
    };
}

inline detail::CoordinationChannel fake_channel(Script &script)
{
    return [&script](Protocol::CoordType kind,
                     const std::vector<std::byte> &) -> std::vector<std::byte>
    {
        const int inside = script.channel_inside.fetch_add(1, std::memory_order_acq_rel) + 1;
        int observed = script.channel_max_concurrent.load(std::memory_order_relaxed);
        while(inside > observed &&
              !script.channel_max_concurrent.compare_exchange_weak(observed, inside))
        {
        }
        // Held for the whole body, so an overlap anywhere in it is recorded.
        struct Leave
        {
            Script &script;
            ~Leave()
            {
                script.channel_inside.fetch_sub(1, std::memory_order_acq_rel);
            }
        } leave{script};

        {
            std::lock_guard<std::mutex> lock(script.mutex);
            script.channel_threads.insert(std::this_thread::get_id());
            if(script.channel_throws > 0)
            {
                --script.channel_throws;
                throw BulkException(
                    BulkError{Status::TransportFailure, "the device is unreachable", "tango"});
            }
        }

        // Wide enough that an overlapping caller would be caught.
        std::this_thread::sleep_for(std::chrono::microseconds{200});

        switch(kind)
        {
        case Protocol::CoordType::Open:
            script.opens.fetch_add(1, std::memory_order_relaxed);
            break;
        case Protocol::CoordType::Renew:
            script.renews.fetch_add(1, std::memory_order_relaxed);
            break;
        case Protocol::CoordType::Close:
            script.closes.fetch_add(1, std::memory_order_relaxed);
            break;
        default:
            break;
        }

        return {std::byte{0}};
    };
}

inline SubscriberConfig supervisor_config()
{
    SubscriberConfig config;
    config.stream_name = "bulk.unit";
    config.max_frame_bytes = 64u << 10;
    config.ring_depth = 4;
    config.credit_window = 2;
    config.delivery_queue_depth = 8;
    config.delivery_mode = DeliveryMode::Manual;
    config.reconnect_backoff_ms = 5;
    config.reconnect_max_attempts = 3;
    config.command_timeout_ms = 500;
    config.probe_timeout_ms = 60;
    return config;
}

inline detail::SessionCallbacks noop_callbacks()
{
    detail::SessionCallbacks callbacks;
    callbacks.on_frame = [](FrameView) {};
    callbacks.on_state = [](SubscriberState, const BulkError &) {};
    return callbacks;
}

template <typename Predicate>
bool eventually(Predicate predicate, std::chrono::milliseconds budget = 4s)
{
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while(std::chrono::steady_clock::now() < deadline)
    {
        if(predicate())
        {
            return true;
        }
        std::this_thread::sleep_for(1ms);
    }
    return predicate();
}

} // namespace TangoBulkTests

#endif // TANGO_BULK_TESTS_UNIT_FAKE_TRANSPORT_H
