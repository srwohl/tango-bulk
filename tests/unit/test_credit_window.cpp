// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

// The two halves of the credit protocol.
//
// Getting this wrong is the one failure mode in the design whose symptom is
// silent payload corruption rather than an error: a window that advances past a
// sequence the peer never released hands a slot back to the producer while the
// NIC may still be reading it.

#include "core/credit_window.h"

#include <catch2/catch_test_macros.hpp>

#include <vector>

using namespace TangoBulk;
using namespace TangoBulk::detail;

// ---------------------------------------------------------------------------
// Publisher side
// ---------------------------------------------------------------------------

TEST_CASE("the window opens and closes at its width", "[core][credit]")
{
    CreditWindow window{32, 4};

    CHECK(window.outstanding() == 0);
    CHECK(window.next_sequence() == 0);

    for(std::uint64_t i = 0; i < 4; ++i)
    {
        REQUIRE(window.can_submit());
        CHECK(window.note_submitted() == i);
    }

    CHECK(window.outstanding() == 4);
    CHECK_FALSE(window.can_submit());

    // One cumulative credit for sequence 1 releases 0 and 1 together.
    const auto outcome = window.apply_credit(1);
    CHECK(outcome.status == Status::Ok);
    CHECK(outcome.released == 2);

    CHECK(window.credited_end() == 2);
    CHECK(window.outstanding() == 2);
    CHECK(window.can_submit());
}

TEST_CASE("credit is cumulative and idempotent", "[core][credit]")
{
    CreditWindow window{32, 16};

    for(int i = 0; i < 10; ++i)
    {
        window.note_submitted();
    }

    CHECK(window.apply_credit(5).released == 6);
    CHECK(window.credited_end() == 6);

    // A duplicate is accepted and does nothing.  A dropped credit message is
    // repaired by the next one, which is why there is no retransmit logic.
    const auto duplicate = window.apply_credit(5);
    CHECK(duplicate.status == Status::Ok);
    CHECK(duplicate.released == 0);
    CHECK(window.credited_end() == 6);

    // A reordered, lower ack must not move the base backwards.
    const auto reordered = window.apply_credit(2);
    CHECK(reordered.status == Status::Ok);
    CHECK(reordered.released == 0);
    CHECK(window.credited_end() == 6);

    // And a later one repairs everything in between at once.
    CHECK(window.apply_credit(9).released == 4);
    CHECK(window.credited_end() == 10);
    CHECK(window.outstanding() == 0);
}

TEST_CASE("an ack above the highest submitted sequence is refused", "[core][credit]")
{
    CreditWindow window{32, 16};

    // Nothing submitted: every ack is a protocol violation.
    auto outcome = window.apply_credit(0);
    CHECK(outcome.status == Status::MalformedMessage);
    CHECK(outcome.released == 0);
    CHECK(window.credited_end() == 0);

    window.note_submitted(); // sequence 0
    window.note_submitted(); // sequence 1

    // Highest submitted is 1, so 2 is out of range.
    outcome = window.apply_credit(2);
    CHECK(outcome.status == Status::MalformedMessage);
    CHECK(window.credited_end() == 0);

    // The window must not have moved, even a little.  This is the check that
    // stops a buggy or hostile subscriber from talking the publisher into
    // recycling a slot the NIC is still reading.
    CHECK(window.outstanding() == 2);

    outcome = window.apply_credit(~std::uint64_t{0});
    CHECK(outcome.status == Status::MalformedMessage);
    CHECK(window.credited_end() == 0);

    // The legal edge still works.
    CHECK(window.apply_credit(1).released == 2);
}

TEST_CASE("the window survives many wraps of the ring", "[core][credit]")
{
    constexpr std::uint32_t depth = 8;
    CreditWindow window{depth, depth};

    std::uint64_t credited = 0;

    for(std::uint64_t round = 0; round < 4 * depth; ++round)
    {
        REQUIRE(window.can_submit());
        const std::uint64_t seq = window.note_submitted();

        CHECK(seq == round);

        // Slot mapping is sequence % ring_depth, so slot i holds sequence s iff
        // s % depth == i.
        CHECK(seq % depth == round % depth);

        const auto outcome = window.apply_credit(seq);
        CHECK(outcome.status == Status::Ok);
        CHECK(outcome.released == 1);

        credited += outcome.released;
    }

    CHECK(credited == 4 * depth);
    CHECK(window.outstanding() == 0);
}

TEST_CASE("reset restarts the window at a given sequence", "[core][credit]")
{
    CreditWindow window{16, 8};

    window.note_submitted();
    window.note_submitted();

    window.reset(1000);

    CHECK(window.next_sequence() == 1000);
    CHECK(window.credited_end() == 1000);
    CHECK(window.outstanding() == 0);

    // A straggler credit from before the reset is stale, not hostile: a
    // reconnect can easily be overtaken by a credit the old session already
    // sent.  It is ignored, which leaves the window where it is, and it is not
    // counted as malformed -- doing so would report a normal race as an attack.
    const auto stale = window.apply_credit(999);
    CHECK(stale.status == Status::Ok);
    CHECK(stale.released == 0);
    CHECK(window.credited_end() == 1000);

    // An ack at or above the next sequence is still a violation, because nothing
    // has been submitted in this epoch for it to be acknowledging.
    CHECK(window.apply_credit(1000).status == Status::MalformedMessage);
    CHECK(window.credited_end() == 1000);
}

// ---------------------------------------------------------------------------
// Subscriber side
// ---------------------------------------------------------------------------

TEST_CASE("in-order release advances the ack every time", "[core][credit]")
{
    ReleaseTracker tracker{8};

    std::uint64_t ack = 0;
    CHECK_FALSE(tracker.take_pending_ack(ack));

    for(std::uint64_t seq = 0; seq < 8; ++seq)
    {
        CHECK(tracker.release(seq) == ReleaseTracker::Release::Advanced);
        REQUIRE(tracker.take_pending_ack(ack));
        CHECK(ack == seq);
    }

    CHECK(tracker.released_end() == 8);
    CHECK(tracker.held_ahead() == 0);
}

TEST_CASE("out-of-order release advances only across the contiguous prefix",
          "[core][credit]")
{
    ReleaseTracker tracker{8};

    // Release 3, 2, 1 -- none of them contiguous with the base at 0.
    CHECK(tracker.release(3) == ReleaseTracker::Release::Recorded);
    CHECK(tracker.release(2) == ReleaseTracker::Release::Recorded);
    CHECK(tracker.release(1) == ReleaseTracker::Release::Recorded);

    CHECK(tracker.released_end() == 0);
    CHECK(tracker.held_ahead() == 3);

    std::uint64_t ack = 0;
    CHECK_FALSE(tracker.take_pending_ack(ack));

    // Releasing 0 closes the hole and the ack jumps all the way to 3.
    CHECK(tracker.release(0) == ReleaseTracker::Release::Advanced);
    CHECK(tracker.released_end() == 4);
    CHECK(tracker.held_ahead() == 0);

    REQUIRE(tracker.take_pending_ack(ack));
    CHECK(ack == 3);
}

TEST_CASE("the ack is fetched once per change", "[core][credit]")
{
    // This is what makes credit_messages_sent fall below credits_returned: the
    // engine asks once per progress iteration and gets nothing when nothing
    // moved.
    ReleaseTracker tracker{8};

    tracker.release(0);
    tracker.release(1);
    tracker.release(2);

    std::uint64_t ack = 0;
    REQUIRE(tracker.take_pending_ack(ack));
    CHECK(ack == 2);

    // Three releases, one message.
    CHECK_FALSE(tracker.take_pending_ack(ack));

    tracker.release(3);
    REQUIRE(tracker.take_pending_ack(ack));
    CHECK(ack == 3);
    CHECK_FALSE(tracker.take_pending_ack(ack));
}

TEST_CASE("a duplicate release is ignored", "[core][credit]")
{
    ReleaseTracker tracker{8};

    CHECK(tracker.release(2) == ReleaseTracker::Release::Recorded);
    CHECK(tracker.release(2) == ReleaseTracker::Release::Duplicate);
    CHECK(tracker.held_ahead() == 1);

    CHECK(tracker.release(0) == ReleaseTracker::Release::Advanced);
    CHECK(tracker.release(1) == ReleaseTracker::Release::Advanced);
    CHECK(tracker.released_end() == 3);

    // Already behind the base.
    CHECK(tracker.release(0) == ReleaseTracker::Release::Duplicate);
    CHECK(tracker.released_end() == 3);
}

TEST_CASE("a release beyond the window is refused", "[core][credit]")
{
    ReleaseTracker tracker{8};

    // Only ring_depth sequences can be in flight, so 8 is out of the window
    // while the base is 0.  Accepting it would alias onto slot 0.
    CHECK(tracker.release(8) == ReleaseTracker::Release::OutOfWindow);
    CHECK(tracker.release(7) == ReleaseTracker::Release::Recorded);
    CHECK(tracker.released_end() == 0);

    for(std::uint64_t seq = 0; seq < 7; ++seq)
    {
        tracker.release(seq);
    }

    CHECK(tracker.released_end() == 8);

    // With the base at 8 the window is [8, 16).
    CHECK(tracker.release(15) == ReleaseTracker::Release::Recorded);
    CHECK(tracker.release(16) == ReleaseTracker::Release::OutOfWindow);
}

TEST_CASE("the tracker survives many wraps", "[core][credit]")
{
    constexpr std::uint32_t depth = 8;
    ReleaseTracker tracker{depth};

    for(std::uint64_t seq = 0; seq < 100; ++seq)
    {
        REQUIRE(tracker.release(seq) == ReleaseTracker::Release::Advanced);
    }

    CHECK(tracker.released_end() == 100);

    std::uint64_t ack = 0;
    REQUIRE(tracker.take_pending_ack(ack));
    CHECK(ack == 99);
}

TEST_CASE("reset clears the bitmap and the ack baseline", "[core][credit]")
{
    ReleaseTracker tracker{8};

    tracker.release(0);
    tracker.release(1);
    tracker.release(3);

    tracker.reset(1000);

    CHECK(tracker.released_end() == 1000);
    CHECK(tracker.held_ahead() == 0);

    // Nothing has been released in the new epoch, so there is nothing to ack --
    // in particular not sequence 999, which was never delivered.
    std::uint64_t ack = 0;
    CHECK_FALSE(tracker.take_pending_ack(ack));

    // And a stale bit from before the reset must not make 1001 look contiguous.
    CHECK(tracker.release(1001) == ReleaseTracker::Release::Recorded);
    CHECK(tracker.released_end() == 1000);
    CHECK_FALSE(tracker.take_pending_ack(ack));

    CHECK(tracker.release(1000) == ReleaseTracker::Release::Advanced);
    REQUIRE(tracker.take_pending_ack(ack));
    CHECK(ack == 1001);
}

// ---------------------------------------------------------------------------
// The two halves against each other
// ---------------------------------------------------------------------------

TEST_CASE("publisher and subscriber agree under out-of-order release",
          "[core][credit]")
{
    constexpr std::uint32_t depth = 8;

    CreditWindow publisher{depth, depth};
    ReleaseTracker subscriber{depth};

    for(std::uint64_t i = 0; i < depth; ++i)
    {
        REQUIRE(publisher.can_submit());
        publisher.note_submitted();
    }

    CHECK_FALSE(publisher.can_submit());

    // The application lets views go in a scrambled order.
    const std::vector<std::uint64_t> order{5, 2, 7, 0, 1, 6, 3, 4};

    std::uint64_t messages_sent = 0;

    for(const std::uint64_t seq : order)
    {
        subscriber.release(seq);

        std::uint64_t ack = 0;
        if(subscriber.take_pending_ack(ack))
        {
            ++messages_sent;
            const auto outcome = publisher.apply_credit(ack);
            CHECK(outcome.status == Status::Ok);
        }
    }

    CHECK(publisher.credited_end() == depth);
    CHECK(publisher.outstanding() == 0);

    // Eight releases, strictly fewer than eight credit messages: the coalescing
    // that `credit_messages_sent` against `credits_returned` measures.
    CHECK(messages_sent < depth);
}

TEST_CASE("a retained view withholds exactly one credit", "[core][credit]")
{
    constexpr std::uint32_t depth = 4;

    CreditWindow publisher{depth, depth};
    ReleaseTracker subscriber{depth};

    for(std::uint64_t i = 0; i < depth; ++i)
    {
        publisher.note_submitted();
    }

    // The application holds sequence 1 and releases everything else.
    for(std::uint64_t seq : {0u, 2u, 3u})
    {
        subscriber.release(seq);
    }

    std::uint64_t ack = 0;
    REQUIRE(subscriber.take_pending_ack(ack));
    CHECK(ack == 0);

    publisher.apply_credit(ack);

    // Exactly one slot beyond the retained one is free again; the two above it
    // stay withheld, because credit is cumulative and cannot skip a hole.
    CHECK(publisher.outstanding() == depth - 1);
    CHECK(publisher.can_submit());

    // Releasing the held view frees everything at once.
    subscriber.release(1);
    REQUIRE(subscriber.take_pending_ack(ack));
    CHECK(ack == 3);

    publisher.apply_credit(ack);
    CHECK(publisher.outstanding() == 0);
}
