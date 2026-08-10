// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/tango.h>

#include <tango/tango.h>

#include <cuda_runtime_api.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

/// Receive frames directly into a CUDA allocation through UCX/GPUDirect RDMA.
///
///     tango-bulk-example-cuda-client <device> [stream] [gpu]
///
/// UCX must have CUDA support and the CUDA device, RNIC and kernel driver must
/// support GPUDirect RDMA. Use UCX_LOG_LEVEL=info to inspect transport choice.
namespace
{

std::atomic<bool> running{true};

void on_signal(int)
{
    running.store(false);
}

void check_cuda(cudaError_t status, const char *operation)
{
    if(status != cudaSuccess)
    {
        throw std::runtime_error(std::string(operation) + ": " + cudaGetErrorString(status));
    }
}

} // namespace

int main(int argc, char *argv[])
{
    if(argc < 2)
    {
        std::cerr << "usage: " << argv[0] << " <device> [stream] [gpu]\n";
        return 2;
    }

    const std::string device = argv[1];
    const std::string stream = argc > 2 ? argv[2] : "image";
    const int gpu = argc > 3 ? std::stoi(argv[3]) : 0;

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    try
    {
        Tango::DeviceProxy proxy(device);

        // Discover the stream before allocating GPU memory. BulkOpen can clamp
        // an oversized request, but cannot enlarge an undersized CUDA slot.
        const TangoBulk::BulkQueryResult publisher = TangoBulk::bulk_query(proxy);
        if(publisher.status != TangoBulk::Status::Ok)
        {
            throw TangoBulk::BulkException(
                {publisher.status, "publisher is not ready", "BulkQuery"});
        }

        // Preserve the example's original 256 MiB receive-ring target. The
        // protocol requires at least two slots, so an exceptionally large frame
        // can raise the actual allocation above that target (up to 512 MiB).
        constexpr std::uint64_t ring_memory_target = 256ull << 20;
        const std::uint64_t slots_in_target =
            ring_memory_target / publisher.max_frame_bytes;
        const std::uint32_t ring_depth = std::min<std::uint32_t>(
            publisher.ring_depth,
            static_cast<std::uint32_t>(std::max<std::uint64_t>(2, slots_in_target)));
        const std::uint32_t credit_window =
            std::min(publisher.credit_window, ring_depth);
        const std::uint64_t ring_bytes = publisher.max_frame_bytes * ring_depth;

        std::cout << "publisher geometry: max_frame_bytes=" << publisher.max_frame_bytes
                  << " ring_depth=" << ring_depth
                  << " credit_window=" << credit_window
                  << " cuda_ring_bytes=" << ring_bytes << std::endl;

        check_cuda(cudaSetDevice(gpu), "cudaSetDevice");

        void *device_memory = nullptr;
        check_cuda(cudaMalloc(&device_memory, static_cast<std::size_t>(ring_bytes)), "cudaMalloc");

        // Shared ownership is intentional: a FrameView may outlive the
        // subscriber, so the CUDA allocation must follow the receive arena's
        // lifetime rather than main()'s lexical scope.
        std::shared_ptr<void> receive_buffer(device_memory,
                                             [](void *pointer)
                                             {
                                                 if(pointer != nullptr)
                                                 {
                                                     (void) cudaFree(pointer);
                                                 }
                                             });

        TangoBulk::SubscriberConfig config;
        config.stream_name = stream;
        config.max_frame_bytes = publisher.max_frame_bytes;
        config.ring_depth = ring_depth;
        config.credit_window = credit_window;
        config.delivery_mode = TangoBulk::DeliveryMode::DispatchThread;
        config.reconnect_policy = TangoBulk::ReconnectPolicy::BoundedRetry;
        config.receive_buffer = receive_buffer;
        config.receive_buffer_bytes = ring_bytes;
        config.receive_memory_kind = TangoBulk::MemoryKind::Cuda;

        TangoBulk::BulkSubscriber subscriber(proxy, config);

        std::atomic<std::uint64_t> frames{0};
        std::atomic<std::uint64_t> bytes{0};

        const auto buffer_begin = reinterpret_cast<std::uintptr_t>(device_memory);
        const auto buffer_end = buffer_begin + ring_bytes;

        subscriber.set_frame_callback(
            [&frames, &bytes, buffer_begin, buffer_end](TangoBulk::FrameView view)
            {
                // This is a CUDA device address: never dereference it on the CPU.
                const auto address = reinterpret_cast<std::uintptr_t>(view.data());
                if(address < buffer_begin || address + view.size() > buffer_end)
                {
                    std::cerr << "received a frame outside the CUDA ring\n";
                    running.store(false);
                    return;
                }

                // A real GPU consumer can launch a kernel on view.data() here.
                // It must retain the FrameView until that kernel/stream has
                // finished, because releasing the view returns the slot credit.
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

            std::cout << static_cast<double>(total_frames - last_frames) / seconds << " frame/s, "
                      << static_cast<double>(total_bytes - last_bytes) / seconds / (1u << 30)
                      << " GiB/s into GPU, " << total_frames << " total" << std::endl;

            last = now;
            last_frames = total_frames;
            last_bytes = total_bytes;
        }

        subscriber.stop();
        const TangoBulk::SubscriberCounters counters = subscriber.counters();
        std::cout << "received=" << counters.frames_received
                  << " delivered=" << counters.frames_delivered
                  << " credits_returned=" << counters.credits_returned << std::endl;
    }
    catch(const Tango::DevFailed &failure)
    {
        Tango::Except::print_exception(failure);
        return 1;
    }
    catch(const std::exception &error)
    {
        std::cerr << "cuda client: " << error.what() << std::endl;
        return 1;
    }

    return 0;
}
