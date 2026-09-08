// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/tango.h>

#include <tango/tango.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>

/// Subscribe to a device's bulk stream over a stock `Tango::DeviceProxy`.
///
///     tango-bulk-example-client bulk/example/1 [stream]
///     tango-bulk-example-client "localhost:10000/bulk/example/1#dbase=no"
///
/// The client-facing lifecycle is one Subscription: construct it, consume
/// frames through its callback, and let close/destruction release the session.
/// Everything else here is printing.
namespace
{

std::atomic<bool> running{true};

void on_signal(int)
{
    running.store(false);
}

} // namespace

int main(int argc, char *argv[])
{
    if(argc < 2)
    {
        std::cerr << "usage: " << argv[0] << " <device> [stream]\n";
        return 2;
    }

    const std::string device = argv[1];
    const std::string stream = argc > 2 ? argv[2] : "image";

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    try
    {
        // BORROWED by the subscriber, which is why it is declared here and not
        // inside a scope that ends first: 7.4 makes the caller responsible for
        // keeping it alive, and the subscriber never copies it.
        Tango::DeviceProxy proxy(device);

        // BulkOpen can clamp a request downward, but it cannot make an
        // undersized receive slot larger. Until StreamOffer discovery is
        // available, use an explicit complete upper plan rather than a query
        // shaped preflight. This is conservative but safe for any valid peer.
        TangoBulk::SubscriberConfig config;
        config.stream_name = stream;
        config.receive_plan = TangoBulk::ReceivePlan::from_limits(
            TangoBulk::k_max_frame_bytes_hard_cap, TangoBulk::k_min_ring_depth, 2);

        // The default Push mode. A library-owned dispatch thread invokes the callback, so
        // it never runs on the UCX engine thread (5.2) -- which is what lets a
        // slow consumer be slow without stalling the transport.  Swap in
        // Pull mode and call read_for() to own frame reads yourself.
        config.delivery_mode = TangoBulk::DeliveryMode::DispatchThread;

        // 4.1: keep trying when the link drops, up to ten times with an
        // exponential backoff capped at the lease TTL.
        config.reconnect_policy = TangoBulk::ReconnectPolicy::BoundedRetry;

        std::atomic<std::uint64_t> frames{0};
        std::atomic<std::uint64_t> bytes{0};

        TangoBulk::SubscriptionCallbacks callbacks;

        callbacks.on_frame =
            [&frames, &bytes](TangoBulk::FrameView view)
            {
                // `view.data()` points into the registered receive ring.  It is
                // valid for as long as any copy of this view is alive, and the
                // credit for its slot is withheld until the last one dies (5.5).
                // So: consume it here, or copy it -- but do not stash the view
                // and expect the stream to keep flowing.
                frames.fetch_add(1, std::memory_order_relaxed);
                bytes.fetch_add(view.size(), std::memory_order_relaxed);
            };

        auto subscription = TangoBulk::subscribe(proxy, config, std::move(callbacks));
        const TangoBulk::ReceivePlan actual = subscription->plan();
        std::cout << "granted receive plan: max_frame_bytes=" << actual.max_frame_bytes
                  << " ring_depth=" << actual.ring_depth
                  << " credit_window=" << actual.credit_window << std::endl;

        auto last = std::chrono::steady_clock::now();
        std::uint64_t last_frames = 0;
        std::uint64_t last_bytes = 0;

        while(running.load())
        {
            std::this_thread::sleep_for(std::chrono::seconds(1));

            const auto now = std::chrono::steady_clock::now();
            const double seconds = std::chrono::duration<double>(now - last).count();
            const std::uint64_t total_frames = frames.load();
            const std::uint64_t total_bytes = bytes.load();

            const double rate = static_cast<double>(total_frames - last_frames) / seconds;
            const double gigabytes =
                static_cast<double>(total_bytes - last_bytes) / seconds / (1u << 30);

            std::cout << rate << " frame/s, " << gigabytes << " GiB/s, "
                      << total_frames << " total" << std::endl;

            last = now;
            last_frames = total_frames;
            last_bytes = total_bytes;
        }

        const TangoBulk::SubscriberCounters counters = subscription->counters();

        subscription.reset();
        std::cout << "received=" << counters.frames_received
                  << " delivered=" << counters.frames_delivered
                  << " dropped_queue_full=" << counters.frames_dropped_queue_full
                  << " credits_returned=" << counters.credits_returned
                  << " credit_messages=" << counters.credit_messages_sent
                  << " reconnects=" << counters.reconnects << std::endl;
    }
    catch(const TangoBulk::BulkException &e)
    {
        std::cerr << "bulk error (" << TangoBulk::to_string(e.error().status)
                  << ", from " << e.error().origin << "): " << e.error().message << std::endl;
        return 1;
    }
    catch(const Tango::DevFailed &failure)
    {
        Tango::Except::print_exception(failure);
        return 1;
    }

    return 0;
}
