// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_PUBLISHER_H
#define TANGO_BULK_PUBLISHER_H

#include <tango-bulk/counters.h>
#include <tango-bulk/errors.h>
#include <tango-bulk/frame.h>
#include <tango-bulk/geometry.h>
#include <tango-bulk/limits.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace TangoBulk
{

struct PublisherConfig
{
    std::string stream_name; ///< 1..64 bytes, [A-Za-z0-9_.-]
    /// Initial stream layout advertised by Open and StreamOffer discovery. Leave Unknown/rank
    /// zero for an opaque byte stream; array publishers should declare their
    /// real type and shape before accepting clients.
    FrameMetadata frame_metadata{};
    std::uint64_t max_frame_bytes{8ull << 20};
    std::uint32_t ring_depth{32};
    std::uint32_t credit_window{16}; ///< MUST be <= ring_depth
    std::uint32_t max_sessions{4};

    /// Whether this publisher will grant a lossless session at all.
    ///
    /// How a subscriber is treated when it runs out of credit is that
    /// subscriber's declaration at Open, not a publisher-wide mode: a live
    /// viewer and a file writer routinely attach to the same stream and need
    /// opposite answers. What stays with the publisher is the right to refuse,
    /// because a lossless session can hold up acquisition for everyone. Off by
    /// default, so no deployment grants that power by accident.
    bool allow_lossless{false};
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
    std::string ucx_tls;           ///< empty => let UCX choose; tests pin
    int engine_cpu_affinity{-1};   ///< -1 => unpinned
    bool pad_slot_stride{true};    ///< see IMPLEMENTATION_SPEC.md 6.2

    Status validate() const noexcept;
};

namespace detail
{
class ProducerSlots;
class PublisherAccess;
} // namespace detail

enum class PublishResult : std::uint32_t
{
    Accepted = 0,
    NoSession = 1,     ///< no armed session; frame dropped, counted
    QueueFull = 2,     ///< publish queue full; lease returned to caller
    CreditStalled = 3, ///< credit window closed; frame dropped, counted
    BadMetadata = 4,
    Shutdown = 5,
    WouldBlock = 6, ///< a lossless subscriber lacks credit; lease returned to caller
};

const char *to_string(PublishResult result) noexcept;

/// A copied owner observation of one publisher.
///
/// The geometry and counters are sampled together as one diagnostic value;
/// `sampled_at_steady_ns` makes its age observable.  It is never authority for
/// admission or session reclamation, and a StreamOffer made from it remains an
/// upper bound until OpenReply supplies the actual grant.
struct PublisherSnapshot
{
    struct SessionObservation
    {
        /// Assigned at Open and never reused within a publisher, so it is a
        /// stable handle for the session's whole life -- which a position in
        /// this vector is not, since a session closing renumbers its successors.
        /// It is what disambiguates two clients that chose the same label.
        std::uint64_t ordinal{0};

        std::string session_id;   ///< truncated for logs; never the full credential
        std::string client_label; ///< operator-facing; empty when none was given
        std::string state;
        std::string flow; ///< Lossy or Lossless, as declared at Open
        std::uint64_t lag_frames{0};
    };

    std::string stream_name;
    Geometry geometry{};
    PublisherCounters counters{};
    /// Live sessions; Tango renders each as ordinal|label|state|flow|lag_frames.
    std::vector<SessionObservation> sessions;
    std::size_t active_sessions{0};
    std::string transport; ///< selected data transport, not a discovery result
    std::uint64_t worst_lag_frames{0}; ///< maximum live-session lag in frames
    std::uint64_t sampled_at_steady_ns{0};
    bool accepting{false};

    /// Sum the publisher-side drop reasons from this one sampled counter set.
    std::uint64_t frames_dropped() const noexcept
    {
        return counters.dropped_no_session + counters.dropped_queue_full +
               counters.dropped_credit_stalled + counters.dropped_bad_metadata;
    }

    StreamOffer stream_offer() const;
};

class BulkPublisher
{
  public:
    explicit BulkPublisher(PublisherConfig config); ///< throws BulkException
    ~BulkPublisher();
    BulkPublisher(BulkPublisher &&) noexcept;
    BulkPublisher &operator=(BulkPublisher &&) noexcept;
    BulkPublisher(const BulkPublisher &) = delete;
    BulkPublisher &operator=(const BulkPublisher &) = delete;

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
        void reset() noexcept;

      private:
        friend class BulkPublisher;

        std::shared_ptr<detail::ProducerSlots> slots_;
        std::byte *data_{nullptr};
        std::size_t capacity_{0};
        std::uint32_t index_{0};
    };

    SlotHandle try_acquire() noexcept;

    /// on QueueFull and WouldBlock.  Never blocks, never throws, never allocates.
    PublishResult publish(SlotHandle &&handle, const FrameMetadata &meta) noexcept;

    std::uint32_t generation() const noexcept;
    std::size_t session_count() const noexcept;
    PublisherCounters counters() const noexcept;
    PublisherSnapshot snapshot() const;

  private:
    friend class detail::PublisherAccess;

    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace TangoBulk

#endif // TANGO_BULK_PUBLISHER_H
