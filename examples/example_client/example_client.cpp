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
/// The whole client API is six calls: construct, two callbacks, start, stop.
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

        TangoBulk::SubscriberConfig config;
        config.stream_name = stream;
        config.max_frame_bytes = 8ull << 20;
        config.ring_depth = 32;
        config.credit_window = 16;

        // The default.  A library-owned dispatch thread invokes the callback, so
        // it never runs on the UCX engine thread (5.2) -- which is what lets a
        // slow consumer be slow without stalling the transport.  Swap in
        // DeliveryMode::Manual and call poll() to own the thread yourself.
        config.delivery_mode = TangoBulk::DeliveryMode::DispatchThread;

        // 4.1: keep trying when the link drops, up to ten times with an
        // exponential backoff capped at the lease TTL.
        config.reconnect_policy = TangoBulk::ReconnectPolicy::BoundedRetry;

        TangoBulk::BulkSubscriber subscriber(proxy, config);

        // If the device's commands carry a prefix, say so before start():
        //
        //     set_command_names(subscriber, TangoBulk::CommandNames::with_prefix("Xyz"));

        std::atomic<std::uint64_t> frames{0};
        std::atomic<std::uint64_t> bytes{0};

        subscriber.set_frame_callback(
            [&frames, &bytes](TangoBulk::FrameView view)
            {
                // `view.data()` points into the registered receive ring.  It is
                // valid for as long as any copy of this view is alive, and the
                // credit for its slot is withheld until the last one dies (5.5).
                // So: consume it here, or copy it -- but do not stash the view
                // and expect the stream to keep flowing.
                frames.fetch_add(1, std::memory_order_relaxed);
                bytes.fetch_add(view.size(), std::memory_order_relaxed);
            });

        subscriber.set_state_callback(
            [](TangoBulk::SubscriberState state, const TangoBulk::BulkError &error)
            {
                std::cout << "state: " << TangoBulk::to_string(state);
                if(error.status != TangoBulk::Status::Ok)
                {
                    std::cout << " (" << TangoBulk::to_string(error.status) << ": "
                              << error.message << ")";
                }
                std::cout << std::endl;
            });

        subscriber.start();

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

        // Sends BulkClose, joins the threads, and releases the ring.  Idempotent,
        // and the destructor would do it anyway -- but a client that closes
        // explicitly gives the publisher its slots back now rather than one
        // lease TTL from now.
        subscriber.stop();

        const TangoBulk::SubscriberCounters counters = subscriber.counters();
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
