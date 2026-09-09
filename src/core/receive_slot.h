// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_CORE_RECEIVE_SLOT_H
#define TANGO_BULK_SRC_CORE_RECEIVE_SLOT_H

#include <tango-bulk/frame.h>

#include <core/frame_fields.h>

#include <cstdint>
#include <memory>

namespace TangoBulk::detail
{

/// Where a released receive slot's credit goes.
///
/// An interface rather than a direct call for two reasons, one structural and
/// one about layering.  Structurally, IMPLEMENTATION_SPEC.md 5.5 says the
/// release may happen on *any* thread and must never call UCX directly; an
/// interface whose only implementation pushes onto a queue makes that difficult
/// to violate by accident.  As for layering, `FrameView` and its lease are
/// public API and therefore live in `tango-bulk-core`, which 1.1 forbids from
/// seeing `ucp/*` -- so the engine has to be reached through an abstraction, and
/// this is it.
class CreditSink
{
  public:
    virtual ~CreditSink();

    /// Called from the ReceiveSlotLease destructor.  MUST NOT block, allocate,
    /// throw, or touch UCX.
    virtual void return_credit(std::uint64_t sequence) noexcept = 0;
};

/// One delivered frame's hold on its receive slot.
///
/// Held through a `shared_ptr` by every `FrameView` copy.  When the last copy
/// goes, this is destroyed and the credit returns -- 5.5's rule, expressed as a
/// destructor so that an application cannot forget to call anything.
class ReceiveSlotLease
{
  public:
    /// The sink is held by `shared_ptr`, not by reference, and that is
    /// load-bearing rather than stylistic.
    ///
    /// A `FrameView` may outlive the subscriber that produced it -- 9.3's exit
    /// criteria require teardown with views still outstanding to be clean, and
    /// 4.3 step 5 frees a retired ring only "when the last FrameView from the old
    /// epoch is released".  Both mean the receive ring, its registration, and the
    /// place credits go must all stay alive as long as any view does.  Sharing
    /// ownership of the sink is what arranges that: the implementation of
    /// `CreditSink` owns the arena, so holding the sink holds the memory the view
    /// points at.
    ReceiveSlotLease(std::shared_ptr<CreditSink> sink, std::uint64_t sequence) noexcept;
    ~ReceiveSlotLease();

    ReceiveSlotLease(const ReceiveSlotLease &) = delete;
    ReceiveSlotLease &operator=(const ReceiveSlotLease &) = delete;

    std::uint64_t sequence() const noexcept
    {
        return sequence_;
    }

    /// The only way a FrameView referencing a slot is constructed.
    ///
    /// FrameView's constructor is private and this class is its only friend, so
    /// every view in existence came through here and therefore holds a lease.
    /// There is no path that produces a view without one.
    static FrameView make_view(std::shared_ptr<ReceiveSlotLease> lease,
                               const std::byte *data,
                               const FrameFields *fields) noexcept;

  private:
    std::shared_ptr<CreditSink> sink_;
    std::uint64_t sequence_;
};

/// One entry of the receive ring: where the payload lands and what describes it.
///
/// `fields` is populated by the AM callback before the view is handed out and is
/// read-only for as long as any view references the slot.  It is a member rather
/// than something carried alongside the view because a `FrameView` copy must
/// stay valid without copying 96 bytes of description with it.
struct ReceiveSlot
{
    FrameFields fields{};
    std::byte *data{nullptr};       ///< into the registered receive ring
    std::size_t capacity{0};        ///< granted max_frame_bytes
    bool occupied{false};           ///< engine thread only
};

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_CORE_RECEIVE_SLOT_H
