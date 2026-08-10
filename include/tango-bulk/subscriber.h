// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SUBSCRIBER_H
#define TANGO_BULK_SUBSCRIBER_H

#include <tango-bulk/counters.h>
#include <tango-bulk/errors.h>
#include <tango-bulk/frame.h>
#include <tango-bulk/limits.h>
#include <tango-bulk/publisher.h> // DropPolicy

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

// Forward declaration, deliberately not an include.
//
// BulkSubscriber names a Tango type in its constructor, but this header is
// consumed by translation units in the UCX layer, which MUST NOT see tango/*
// (IMPLEMENTATION_SPEC.md 1.1).  A forward declaration is enough for a
// reference parameter and keeps the header on the clean side of the boundary.
namespace Tango
{
class DeviceProxy;
} // namespace Tango

namespace TangoBulk
{

/// Declared in <tango-bulk/tango.h>, which this header deliberately does not
/// include.  A forward declaration is all `set_command_names()` needs, and it
/// keeps the Tango-only knob out of the header the UCX layer compiles.
struct CommandNames;

enum class SubscriberState : std::uint32_t
{
    Closed = 0,
    Opening = 1,
    Probing = 2,
    Active = 3,
    Reconnecting = 4,
    Failed = 5,
};

const char *to_string(SubscriberState state) noexcept;

enum class DeliveryMode : std::uint32_t
{
    DispatchThread = 0, ///< library-owned thread invokes the callback (default)
    Manual = 1,         ///< application calls poll(); no dispatch thread created
};

enum class ReconnectPolicy : std::uint32_t
{
    FailFast = 0,
    BoundedRetry = 1,
    Manual = 2,
};

struct SubscriberConfig
{
    std::string stream_name;
    std::uint64_t max_frame_bytes{8ull << 20};
    std::uint32_t ring_depth{32};
    std::uint32_t credit_window{16};
    std::uint32_t delivery_queue_depth{64};
    DeliveryMode delivery_mode{DeliveryMode::DispatchThread};
    DropPolicy drop_policy{DropPolicy::DropNewest};
    ReconnectPolicy reconnect_policy{ReconnectPolicy::BoundedRetry};
    std::uint32_t reconnect_max_attempts{10};
    std::uint32_t reconnect_backoff_ms{500}; ///< exponential, capped at lease TTL
    std::uint32_t command_timeout_ms{5'000};
    std::uint64_t pinned_memory_limit_bytes{1ull << 30};
    std::string ucx_tls;
    int engine_cpu_affinity{-1};

    Status validate() const noexcept;
};

using FrameCallback = std::function<void(FrameView)>;
using StateCallback = std::function<void(SubscriberState, const BulkError &)>;

class BulkSubscriber
{
  public:
    /// `proxy` is BORROWED.  The caller MUST keep it alive until the subscriber
    /// is destroyed.  The subscriber never takes a Tango lock and never calls
    /// the proxy from the engine or dispatch thread.
    BulkSubscriber(Tango::DeviceProxy &proxy, SubscriberConfig config);
    ~BulkSubscriber();
    BulkSubscriber(const BulkSubscriber &) = delete;
    BulkSubscriber &operator=(const BulkSubscriber &) = delete;

    /// Both MUST be called before start().
    void set_frame_callback(FrameCallback cb);
    void set_state_callback(StateCallback cb);

    void start();         ///< throws BulkException on open failure under FailFast
    void stop() noexcept; ///< idempotent; sends BulkClose best-effort, joins threads

    /// Manual mode only.  Returns the number of frames dispatched, invoking the
    /// frame callback on the CALLING thread.  Throws if delivery_mode != Manual.
    std::size_t poll(std::chrono::milliseconds timeout = std::chrono::milliseconds{0});

    SubscriberState state() const noexcept;
    std::uint32_t generation() const noexcept;
    SubscriberCounters counters() const noexcept;

  private:
    /// The one thing outside this class that reaches into it: the command-name
    /// override, which is a Tango concept and so is declared in
    /// <tango-bulk/tango.h> and defined next to this class's implementation.
    friend void set_command_names(BulkSubscriber &subscriber, const CommandNames &names);

    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace TangoBulk

#endif // TANGO_BULK_SUBSCRIBER_H
