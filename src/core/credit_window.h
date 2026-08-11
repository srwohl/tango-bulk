// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_CORE_CREDIT_WINDOW_H
#define TANGO_BULK_SRC_CORE_CREDIT_WINDOW_H

#include <tango-bulk/errors.h>

#include <cstdint>
#include <vector>

/// The two halves of the credit protocol, one per side of the wire.
///
/// Credit is cumulative: a `Credit` message says "every sequence at or below
/// ack_sequence is released".  That single choice is what makes credit
/// idempotent, loss-tolerant without any retransmit logic, and free to coalesce.
/// The cost is that out-of-order release has to be reassembled into a contiguous
/// prefix somewhere, and the right somewhere is the subscriber -- the side that
/// actually knows which views the application let go of.
///
/// The prototype kept a `std::set<std::uint64_t> credited_ahead` on the publisher
/// for this.  Here it is a fixed-size bitmap on the subscriber: same invariant,
/// no allocation on the data path, and on the side that has the information.
namespace TangoBulk::detail
{

/// Publisher side: how far the window has advanced and whether it is open.
class CreditWindow
{
  public:
    CreditWindow(std::uint32_t ring_depth, std::uint32_t width) noexcept;

    /// True while fewer than `width` sequences are outstanding.
    bool can_submit() const noexcept;

    /// The sequence the next submission will carry.
    std::uint64_t next_sequence() const noexcept;

    /// Assign `next_sequence()` to a submission and advance.  The caller must
    /// have checked can_submit().
    ///
    /// Sequence assignment happens here, at the point the engine accepts the
    /// frame, and nowhere else.  A frame that fails to submit is never assigned a
    /// sequence, which is what makes an uncreditable hole in the window
    /// impossible rather than merely unlikely.
    std::uint64_t note_submitted() noexcept;

    struct CreditOutcome
    {
        Status status{Status::Ok};
        std::uint64_t released{0}; ///< sequences whose leases may now be freed
    };

    /// Apply a cumulative credit.
    ///
    /// An `ack_sequence` above the highest submitted sequence is a protocol
    /// violation: it is dropped, reported as MalformedMessage, and the window is
    /// **not** advanced.  That check is what stops a buggy or hostile subscriber
    /// from talking the publisher into recycling a slot the NIC is still reading.
    ///
    /// A duplicate or reordered credit with a lower ack is accepted and ignored;
    /// the window base only ever moves forward.
    CreditOutcome apply_credit(std::uint64_t ack_sequence) noexcept;

    /// First sequence not yet credited.  Every sequence below it is released.
    std::uint64_t credited_end() const noexcept;

    std::uint64_t outstanding() const noexcept;

    std::uint32_t ring_depth() const noexcept;
    std::uint32_t width() const noexcept;

    /// Restart at `first_sequence`, discarding all window state.  Used on
    /// reconnect, where sequences restart rather than continue.
    void reset(std::uint64_t first_sequence) noexcept;

  private:
    std::uint32_t ring_depth_;
    std::uint32_t width_;
    std::uint64_t next_sequence_{0};
    std::uint64_t credited_end_{0};
};

/// Subscriber side: turns out-of-order slot releases into one cumulative ack.
class ReleaseTracker
{
  public:
    explicit ReleaseTracker(std::uint32_t ring_depth);

    enum class Release
    {
        Advanced,   ///< recorded, and the contiguous prefix grew
        Recorded,   ///< recorded, but an earlier sequence is still outstanding
        Duplicate,  ///< already released; ignored
        OutOfWindow ///< outside [released_end, released_end + ring_depth)
    };

    Release release(std::uint64_t sequence) noexcept;

    /// Fetch the ack to send, if it differs from the last one fetched.
    ///
    /// The engine calls this once per progress iteration, which is why
    /// `credit_messages_sent` can be far below `credits_returned` under load --
    /// and why the ratio between them is the direct measurement of coalescing.
    bool take_pending_ack(std::uint64_t &ack_sequence) noexcept;

    /// Offer the last taken ack again, because sending it failed.
    ///
    /// A lost `Credit` message is normally self-repairing, and cumulative credit
    /// is why: the next one carries everything the lost one would have.  That
    /// argument holds for every ack except the last.  If nothing else is
    /// released there is no next message, and the publisher waits out its lease
    /// holding a slot the application gave back long ago -- the one credit whose
    /// loss is not recoverable is the one at the end of a stream.
    ///
    /// Rolling back rather than tracking the send is deliberate.  Re-offering an
    /// ack that in fact arrived costs one duplicate message, which a cumulative
    /// protocol is required to ignore anyway.
    void mark_ack_failed() noexcept;

    /// First sequence not yet released contiguously.
    std::uint64_t released_end() const noexcept;

    /// Sequences recorded but not yet contiguous, i.e. holes below them.
    std::uint32_t held_ahead() const noexcept;

    std::uint32_t ring_depth() const noexcept;

    void reset(std::uint64_t first_sequence) noexcept;

  private:
    bool test(std::uint64_t sequence) const noexcept;
    void set(std::uint64_t sequence) noexcept;
    void clear(std::uint64_t sequence) noexcept;

    std::uint32_t ring_depth_;
    std::vector<std::uint64_t> words_; ///< sized once, in the constructor
    std::uint64_t start_{0};           ///< first sequence of the current epoch
    std::uint64_t released_end_{0};
    std::uint32_t held_ahead_{0};
    std::uint64_t last_ack_sent_{0};
    bool ack_sent_{false};
};

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_CORE_CREDIT_WINDOW_H
