// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include "core/credit_window.h"

#include "core/byte_order.h"

#include <cassert>

namespace TangoBulk::detail
{

// ---------------------------------------------------------------------------
// CreditWindow -- publisher side
// ---------------------------------------------------------------------------

CreditWindow::CreditWindow(std::uint32_t ring_depth, std::uint32_t width) noexcept :
    ring_depth_(ring_depth),
    width_(width)
{
}

bool CreditWindow::can_submit() const noexcept
{
    return outstanding() < width_;
}

std::uint64_t CreditWindow::next_sequence() const noexcept
{
    return next_sequence_;
}

std::uint64_t CreditWindow::note_submitted() noexcept
{
    assert(can_submit() && "caller must check can_submit() first");

    return next_sequence_++;
}

CreditWindow::CreditOutcome CreditWindow::apply_credit(std::uint64_t ack_sequence) noexcept
{
    // Highest submitted sequence is next_sequence_ - 1, so a valid ack is
    // strictly below next_sequence_.  When nothing has been submitted this
    // rejects every ack, which is correct: there is nothing to credit.
    if(ack_sequence >= next_sequence_)
    {
        return {Status::MalformedMessage, 0};
    }

    const std::uint64_t new_end = ack_sequence + 1;

    if(new_end <= credited_end_)
    {
        return {Status::Ok, 0};
    }

    const std::uint64_t released = new_end - credited_end_;
    credited_end_ = new_end;

    return {Status::Ok, released};
}

std::uint64_t CreditWindow::credited_end() const noexcept
{
    return credited_end_;
}

std::uint64_t CreditWindow::outstanding() const noexcept
{
    return next_sequence_ - credited_end_;
}

std::uint32_t CreditWindow::ring_depth() const noexcept
{
    return ring_depth_;
}

std::uint32_t CreditWindow::width() const noexcept
{
    return width_;
}

void CreditWindow::reset(std::uint64_t first_sequence) noexcept
{
    next_sequence_ = first_sequence;
    credited_end_ = first_sequence;
}

// ---------------------------------------------------------------------------
// ReleaseTracker -- subscriber side
// ---------------------------------------------------------------------------

namespace
{

constexpr std::uint32_t k_bits_per_word = 64;

std::size_t word_count(std::uint32_t ring_depth) noexcept
{
    return (static_cast<std::size_t>(ring_depth) + k_bits_per_word - 1) /
           k_bits_per_word;
}

} // namespace

ReleaseTracker::ReleaseTracker(std::uint32_t ring_depth) :
    ring_depth_(ring_depth),
    words_(word_count(ring_depth), 0)
{
}

bool ReleaseTracker::test(std::uint64_t sequence) const noexcept
{
    const std::uint32_t bit = static_cast<std::uint32_t>(sequence % ring_depth_);
    return (words_[bit / k_bits_per_word] & (std::uint64_t{1} << (bit % k_bits_per_word))) != 0;
}

void ReleaseTracker::set(std::uint64_t sequence) noexcept
{
    const std::uint32_t bit = static_cast<std::uint32_t>(sequence % ring_depth_);
    words_[bit / k_bits_per_word] |= std::uint64_t{1} << (bit % k_bits_per_word);
}

void ReleaseTracker::clear(std::uint64_t sequence) noexcept
{
    const std::uint32_t bit = static_cast<std::uint32_t>(sequence % ring_depth_);
    words_[bit / k_bits_per_word] &= ~(std::uint64_t{1} << (bit % k_bits_per_word));
}

ReleaseTracker::Release ReleaseTracker::release(std::uint64_t sequence) noexcept
{
    if(sequence < released_end_)
    {
        return Release::Duplicate;
    }

    // Only ring_depth sequences can be in flight at once, so anything beyond the
    // window is a peer that violated its own credit window.  Bit index is
    // `sequence % ring_depth`, which is unique across exactly that range -- the
    // same mapping the receive ring uses for its slots.
    std::uint64_t window_end = 0;
    if(wire::add_overflow(released_end_, ring_depth_, window_end) ||
       sequence >= window_end)
    {
        return Release::OutOfWindow;
    }

    if(test(sequence))
    {
        return Release::Duplicate;
    }

    set(sequence);
    ++held_ahead_;

    const std::uint64_t before = released_end_;
    while(held_ahead_ > 0 && test(released_end_))
    {
        clear(released_end_);
        --held_ahead_;
        ++released_end_;
    }

    return released_end_ > before ? Release::Advanced : Release::Recorded;
}

bool ReleaseTracker::take_pending_ack(std::uint64_t &ack_sequence) noexcept
{
    // Nothing contiguous released yet.  Compared against the epoch's first
    // sequence, not against zero: after a reconnect the window can start
    // anywhere, and `released_end_ - 1` would otherwise ack a sequence that was
    // never delivered.
    if(released_end_ == start_)
    {
        return false;
    }

    const std::uint64_t candidate = released_end_ - 1;

    if(ack_sent_ && candidate == last_ack_sent_)
    {
        return false;
    }

    last_ack_sent_ = candidate;
    ack_sent_ = true;
    ack_sequence = candidate;

    return true;
}

void ReleaseTracker::mark_ack_failed() noexcept
{
    // Clearing the flag rather than restoring `last_ack_sent_`: the next take
    // offers `released_end_ - 1`, which is the failed ack or something newer,
    // and either way it is the value the publisher needs.
    ack_sent_ = false;
}

std::uint64_t ReleaseTracker::released_end() const noexcept
{
    return released_end_;
}

std::uint32_t ReleaseTracker::held_ahead() const noexcept
{
    return held_ahead_;
}

std::uint32_t ReleaseTracker::ring_depth() const noexcept
{
    return ring_depth_;
}

void ReleaseTracker::reset(std::uint64_t first_sequence) noexcept
{
    for(auto &word : words_)
    {
        word = 0;
    }

    start_ = first_sequence;
    released_end_ = first_sequence;
    held_ahead_ = 0;
    last_ack_sent_ = 0;
    ack_sent_ = false;
}

} // namespace TangoBulk::detail
