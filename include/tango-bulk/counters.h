// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_COUNTERS_H
#define TANGO_BULK_COUNTERS_H

#include <cstdint>

namespace TangoBulk
{

/// Publisher-side observability.
///
/// Every field is monotonic within a process except those marked as a gauge.
/// The library stores them as relaxed atomics and hands out a snapshot copy, so
/// reading from any thread is safe and costs nothing on the data path.
struct PublisherCounters
{
    std::uint64_t frames_published{0};  ///< publish() returned Accepted
    std::uint64_t frames_submitted{0};  ///< handed to UCX
    std::uint64_t frames_completed{0};  ///< UCX send completion observed
    std::uint64_t frames_credited{0};   ///< credit received; source lease released
    std::uint64_t dropped_no_session{0};
    std::uint64_t dropped_queue_full{0};
    std::uint64_t dropped_credit_stalled{0};
    std::uint64_t dropped_bad_metadata{0};
    std::uint64_t acquire_failed{0};    ///< try_acquire() returned empty
    std::uint64_t leases_retained{0};   ///< gauge
    std::uint64_t credits_outstanding{0}; ///< gauge
    std::uint64_t publish_queue_depth{0}; ///< gauge
    std::uint64_t publish_queue_high_water{0};
    std::uint64_t sessions_opened{0};
    std::uint64_t sessions_closed{0};
    std::uint64_t sessions_expired{0};
    std::uint64_t sessions_rejected{0};
    std::uint64_t renewals_accepted{0};
    std::uint64_t renewals_late{0};
    std::uint64_t renewals_rejected{0};
    std::uint64_t geometry_changes{0};
    std::uint64_t malformed_messages{0};
    std::uint64_t transport_errors{0};
    std::uint64_t pinned_bytes{0};      ///< gauge
};

/// Subscriber-side observability.
///
/// `credit_messages_sent` against `credits_returned` is the direct measurement
/// of credit coalescing: the interesting quantity is the ratio under load,
/// which is why it is a counter and not a log line.
struct SubscriberCounters
{
    std::uint64_t frames_received{0};
    std::uint64_t frames_delivered{0}; ///< callback invoked
    std::uint64_t frames_dropped_queue_full{0};
    std::uint64_t frames_dropped_stale_epoch{0};
    std::uint64_t frames_dropped_bad_header{0};
    std::uint64_t frames_dropped_oversize{0};
    std::uint64_t frames_dropped_duplicate_seq{0};

    std::uint64_t frames_dropped_geometry_mismatch{0};
    std::uint64_t credits_returned{0};
    std::uint64_t credit_messages_sent{0};
    std::uint64_t views_outstanding{0};    ///< gauge
    std::uint64_t delivery_queue_depth{0}; ///< gauge
    std::uint64_t delivery_queue_high_water{0};
    std::uint64_t sessions_opened{0};
    std::uint64_t reconnects{0};
    std::uint64_t renewals_sent{0};
    std::uint64_t renewals_failed{0};
    std::uint64_t geometry_changes{0};
    std::uint64_t transport_errors{0};
    std::uint64_t pinned_bytes{0}; ///< gauge
};

} // namespace TangoBulk

#endif // TANGO_BULK_COUNTERS_H
