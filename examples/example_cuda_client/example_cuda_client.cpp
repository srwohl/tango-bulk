// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <tango-bulk/tango.h>

#include <tango/tango.h>

#include <cuda_runtime_api.h>

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

        // BulkOpen can clamp a request downward, but it cannot enlarge an
        // undersized CUDA slot. Until StreamOffer discovery is available, use
        // one complete conservative upper plan before allocating the device
        // ring. It reserves 512 MiB at most and works for every valid peer.
        constexpr std::uint32_t ring_depth = TangoBulk::k_min_ring_depth;
        constexpr std::uint32_t credit_window = 2;
        constexpr std::uint64_t slot_bytes = TangoBulk::k_max_frame_bytes_hard_cap;
        constexpr std::uint64_t ring_bytes = slot_bytes * ring_depth;

        std::cout << "requested receive plan: max_frame_bytes=" << slot_bytes
                  << " ring_depth=" << ring_depth << " credit_window=" << credit_window
                  << " cuda_ring_bytes=" << ring_bytes << std::endl;

        check_cuda(cudaSetDevice(gpu), "cudaSetDevice");

        void *device_memory = nullptr;
        check_cuda(cudaMalloc(&device_memory, static_cast<std::size_t>(ring_bytes)), "cudaMalloc");

        // Shared ownership is intentional: a FrameView may outlive the
        // subscriber, so the CUDA allocation must follow the receive arena's
        // lifetime rather than main()'s lexical scope.
        std::shared_ptr<void> receive_buffer(device_memory,
                                             [gpu](void *pointer)
                                             {
                                                 if(pointer == nullptr)
                                                 {
                                                     return;
                                                 }
                                                 // Which thread drops the last
                                                 // reference is not knowable
                                                 // here -- it may be the engine,
                                                 // or a FrameView released after
                                                 // this scope ended -- and the
                                                 // CUDA runtime's current device
                                                 // is per thread.  Freeing a
                                                 // device-`gpu` pointer from a
                                                 // thread still on device 0
                                                 // fails, and the failure is
                                                 // this allocation leaking.
                                                 (void) cudaSetDevice(gpu);
                                                 (void) cudaFree(pointer);
                                             });

        TangoBulk::SubscriberConfig config;
        config.stream_name = stream;
        config.receive_plan = TangoBulk::ReceivePlan::from_limits(
            slot_bytes, ring_depth, credit_window);
        config.delivery_mode = TangoBulk::DeliveryMode::DispatchThread;
        config.reconnect_policy = TangoBulk::ReconnectPolicy::BoundedRetry;
        config.receive_buffer = receive_buffer;
        config.receive_buffer_bytes = ring_bytes;
        config.receive_memory_kind = TangoBulk::MemoryKind::Cuda;

        TangoBulk::SubscriptionCallbacks callbacks;

        std::atomic<std::uint64_t> frames{0};
        std::atomic<std::uint64_t> bytes{0};

        const auto buffer_begin = reinterpret_cast<std::uintptr_t>(device_memory);
        const auto buffer_end = buffer_begin + ring_bytes;

        callbacks.on_frame =
            [&frames, &bytes, gpu, buffer_begin, buffer_end](TangoBulk::FrameView view)
            {
                // DeliveryMode::DispatchThread runs this on a library thread,
                // and the CUDA runtime's current device is per thread: the
                // cudaSetDevice above bound main(), not this one, so a kernel
                // launched below would go to device 0 whatever `gpu` says.
                // This is the first code of ours the dispatch thread runs, so it
                // is the earliest place to bind it, and once is enough.
                thread_local const cudaError_t bound = cudaSetDevice(gpu);
                if(bound != cudaSuccess)
                {
                    std::cerr << "dispatch thread could not select GPU " << gpu << ": "
                              << cudaGetErrorString(bound) << "\n";
                    running.store(false);
                    return;
                }

                // This is a CUDA device address: never dereference it on the CPU.
                //
                // Written as a subtraction because `address + view.size()` can
                // wrap, and a wrapped sum compares as in-range.
                const auto address = reinterpret_cast<std::uintptr_t>(view.data());
                if(address < buffer_begin || address > buffer_end ||
                   view.size() > buffer_end - address)
                {
                    std::cerr << "received a frame outside the CUDA ring\n";
                    running.store(false);
                    return;
                }

                // A real GPU consumer launches a kernel on view.data() here and
                // must hold the FrameView until that kernel has finished, not
                // until this callback returns: releasing the view returns the
                // slot credit, and the publisher may then overwrite the slot
                // while the kernel is still reading it.  A launch is
                // asynchronous, so the shape that works is to record a CUDA
                // event after the launch and move the view onto a pending queue,
                // destroying it only once that event is complete.
                //
                // The ordering is also why the kernel is launched from here
                // rather than by a kernel already resident on the GPU: NVIDIA's
                // GPUDirect RDMA documentation is explicit that a running kernel
                // cannot safely observe an incoming RDMA write, and that the CPU
                // must see the network completion before it submits work that
                // depends on it.  This callback runs after exactly that
                // completion.
                frames.fetch_add(1, std::memory_order_relaxed);
                bytes.fetch_add(view.size(), std::memory_order_relaxed);
            });

        auto subscription =
            TangoBulk::subscribe(proxy, config, std::move(callbacks));
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

            std::cout << static_cast<double>(total_frames - last_frames) / seconds << " frame/s, "
                      << static_cast<double>(total_bytes - last_bytes) / seconds / (1u << 30)
                      << " GiB/s into GPU, " << total_frames << " total" << std::endl;

            last = now;
            last_frames = total_frames;
            last_bytes = total_bytes;
        }

        const TangoBulk::SubscriberCounters counters = subscription->counters();
        subscription.reset();
        std::cout << "received=" << counters.frames_received
                  << " delivered=" << counters.frames_delivered
                  << " credits_returned=" << counters.credits_returned
                  << " transport_errors=" << counters.transport_errors
                  << " reconnects=" << counters.reconnects << std::endl;
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
