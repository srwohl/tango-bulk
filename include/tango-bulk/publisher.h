// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_PUBLISHER_H
#define TANGO_BULK_PUBLISHER_H

#include <tango-bulk/counters.h>
#include <tango-bulk/errors.h>
#include <tango-bulk/frame.h>
#include <tango-bulk/limits.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace TangoBulk
{

// DropPolicy is declared in <tango-bulk/frame.h>, which this header includes.
// See the note there for why it does not live here.

/// How a publisher treats an armed subscriber that has exhausted its credit.
enum class FanoutMode : std::uint32_t
{
    BestEffort = 0, ///< skip only the lagging subscriber; other clients continue
    AllActive = 1,  ///< accept a frame only when every armed subscriber can receive it
};

const char *to_string(FanoutMode mode) noexcept;

struct PublisherConfig
{
    std::string stream_name; ///< 1..64 bytes, [A-Za-z0-9_.-]
    /// Initial stream layout advertised by Open and Query.  Leave Unknown/rank
    /// zero for an opaque byte stream; array publishers should declare their
    /// real type and shape before accepting clients.
    FrameMetadata frame_metadata{};
    std::uint64_t max_frame_bytes{8ull << 20};
    std::uint32_t ring_depth{32};
    std::uint32_t credit_window{16}; ///< MUST be <= ring_depth
    std::uint32_t max_sessions{4};
    FanoutMode fanout_mode{FanoutMode::BestEffort};
    std::uint32_t publish_queue_depth{256};
    std::uint32_t lease_ttl_ms{10'000};
    std::uint32_t renew_interval_ms{3'333};

    /// Accepted renewals per TTL window before `RenewTooFrequent` (3.7).
    ///
    /// 6.1 lists this among the configurable quantities; 2.3's struct does not
    /// name a field for it, so this one is an addition rather than a rename.
    /// See docs/EXTRACTION.md.
    std::uint32_t max_renewals_per_ttl{10};
    std::uint64_t pinned_memory_limit_bytes{1ull << 30};
    DropPolicy drop_policy{DropPolicy::DropNewest};
    std::string ucx_tls;           ///< empty => let UCX choose; tests pin
    int engine_cpu_affinity{-1};   ///< -1 => unpinned
    bool pad_slot_stride{true};    ///< see IMPLEMENTATION_SPEC.md 6.2

    Status validate() const noexcept;
};

namespace detail
{
/// The producer ring, its registration and its free list, defined in
/// `src/ucx/producer_slots.h`. Forward-declared because that header is internal
/// and this one is installed: a `shared_ptr` member needs no complete type here.
class ProducerSlots;
} // namespace detail

enum class PublishResult : std::uint32_t
{
    Accepted = 0,
    NoSession = 1,     ///< no armed session; frame dropped, counted
    QueueFull = 2,     ///< publish queue full; lease returned to caller
    CreditStalled = 3, ///< credit window closed; frame dropped, counted
    BadMetadata = 4,
    Shutdown = 5,
    WouldBlock = 6, ///< AllActive subscriber lacks credit; lease returned to caller
};

const char *to_string(PublishResult result) noexcept;

class BulkPublisher
{
  public:
    explicit BulkPublisher(PublisherConfig config); ///< throws BulkException
    ~BulkPublisher();
    BulkPublisher(BulkPublisher &&) noexcept;
    BulkPublisher &operator=(BulkPublisher &&) noexcept;
    BulkPublisher(const BulkPublisher &) = delete;
    BulkPublisher &operator=(const BulkPublisher &) = delete;

    /// An application's exclusive hold on one publisher slot before publication.
    ///
    /// It shares ownership of the ring rather than pointing at its publisher, so
    /// a handle outliving the publisher keeps its storage valid instead of
    /// dangling (ADR 0003). The share is taken by copying a refcount during
    /// acquisition, which allocates nothing.
    class SlotHandle
    {
      public:
        SlotHandle() noexcept;
        SlotHandle(SlotHandle &&) noexcept;
        SlotHandle &operator=(SlotHandle &&) noexcept;
        SlotHandle(const SlotHandle &) = delete;
        SlotHandle &operator=(const SlotHandle &) = delete;
        ~SlotHandle(); ///< an unpublished handle returns the slot

        explicit operator bool() const noexcept;
        void *data() const noexcept;
        std::size_t capacity() const noexcept;
        std::size_t index() const noexcept;
        MemoryKind memory_kind() const noexcept;
        void reset() noexcept;

      private:
        friend class BulkPublisher;

        std::shared_ptr<detail::ProducerSlots> slots_;
        std::byte *data_{nullptr};
        std::size_t capacity_{0};
        std::uint32_t index_{0};
    };

    /// Non-blocking by design: an acquisition thread MUST be able to drop rather
    /// than wait while slots are retained by a slow or dead consumer.
    SlotHandle try_acquire() noexcept;

    std::size_t slot_bytes() const noexcept; ///< usable payload capacity per slot
    std::size_t retained() const noexcept;   ///< slots currently held or in flight

    /// Consumes the handle on Accepted; leaves it engaged in the caller's hands
    /// on QueueFull and WouldBlock.  Never blocks, never throws, never allocates.
    PublishResult publish(SlotHandle &&handle, const FrameMetadata &meta) noexcept;

    std::uint32_t generation() const noexcept;
    std::size_t session_count() const noexcept;
    PublisherCounters counters() const noexcept;

    /// Coordination entry point: encoded bytes in, encoded bytes out.
    ///
    /// This is the seam that keeps Tango out of the core.  The Tango adapter is
    /// a thin DevVarCharArray wrapper over it, and the unit tests drive the
    /// entire session lifecycle through it with no Tango process at all.
    std::vector<std::byte> handle_coordination(const std::byte *data,
                                               std::size_t size) noexcept;

  private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace TangoBulk

#endif // TANGO_BULK_PUBLISHER_H
