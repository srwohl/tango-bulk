// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#include <core/receive_slot.h>

#include <tango-bulk/frame.h>

namespace TangoBulk
{
namespace
{

/// What the accessors report for a disengaged view.
///
/// A default-constructed or reset() FrameView is a legal object -- `poll()`
/// returns one when there is nothing to deliver -- so every accessor has to
/// answer something.  Zeroes throughout, and `operator bool` is the way to tell
/// the two cases apart.
const FrameView::Fields &empty_fields() noexcept
{
    static const FrameView::Fields k_empty{};
    return k_empty;
}

} // namespace

FrameView::FrameView(std::shared_ptr<detail::ReceiveSlotLease> lease,
                     const std::byte *data,
                     const Fields *fields) noexcept :
    lease_(std::move(lease)),
    data_(data),
    fields_(fields)
{
}

FrameView::operator bool() const noexcept
{
    return lease_ != nullptr && fields_ != nullptr;
}

const std::byte *FrameView::data() const noexcept
{
    return data_;
}

std::size_t FrameView::size() const noexcept
{
    return fields_ != nullptr ? static_cast<std::size_t>(fields_->payload_bytes) : 0;
}

const std::byte *FrameView::begin() const noexcept
{
    return data_;
}

const std::byte *FrameView::end() const noexcept
{
    return data_ != nullptr ? data_ + size() : nullptr;
}

ElementType FrameView::element_type() const noexcept
{
    return fields_ != nullptr ? fields_->element_type : ElementType::Unknown;
}

std::uint32_t FrameView::element_size() const noexcept
{
    return fields_ != nullptr ? fields_->element_size : 0;
}

std::uint32_t FrameView::rank() const noexcept
{
    return fields_ != nullptr ? fields_->rank : 0;
}

const std::array<std::uint64_t, k_max_rank> &FrameView::shape() const noexcept
{
    return fields_ != nullptr ? fields_->shape : empty_fields().shape;
}

const std::array<std::uint64_t, k_max_rank> &FrameView::strides() const noexcept
{
    return fields_ != nullptr ? fields_->strides : empty_fields().strides;
}

std::uint64_t FrameView::sequence() const noexcept
{
    return fields_ != nullptr ? fields_->sequence : 0;
}

std::uint64_t FrameView::event_counter() const noexcept
{
    return fields_ != nullptr ? fields_->event_counter : 0;
}

std::uint64_t FrameView::timestamp_ns() const noexcept
{
    return fields_ != nullptr ? fields_->timestamp_ns : 0;
}

std::uint64_t FrameView::dropped_before() const noexcept
{
    return fields_ != nullptr ? fields_->dropped_before : 0;
}

std::uint32_t FrameView::quality() const noexcept
{
    return fields_ != nullptr ? fields_->quality : 0;
}

std::uint32_t FrameView::generation() const noexcept
{
    return fields_ != nullptr ? fields_->generation : 0;
}

MemoryKind FrameView::memory_kind() const noexcept
{
    return fields_ != nullptr ? fields_->memory_kind : MemoryKind::Host;
}

Endian FrameView::endian() const noexcept
{
    return fields_ != nullptr ? fields_->endian : Endian::Little;
}

long FrameView::use_count() const noexcept
{
    return lease_.use_count();
}

void FrameView::reset() noexcept
{
    // Order matters only for readability here -- the credit goes back when the
    // shared_ptr assignment drops the last reference -- but clearing the
    // description first means there is no window in which the view looks
    // engaged while its slot is already recyclable.
    data_ = nullptr;
    fields_ = nullptr;
    lease_.reset();
}

namespace detail
{

CreditSink::~CreditSink() = default;

ReceiveSlotLease::ReceiveSlotLease(std::shared_ptr<CreditSink> sink,
                                   std::uint64_t sequence) noexcept :
    sink_(std::move(sink)),
    sequence_(sequence)
{
}

ReceiveSlotLease::~ReceiveSlotLease()
{
    // 5.5: the release may happen on any thread, and it never calls UCX.  All it
    // does is hand the sequence to the engine, which coalesces releases into one
    // cumulative Credit per progress iteration.
    sink_->return_credit(sequence_);
}

FrameView ReceiveSlotLease::make_view(std::shared_ptr<ReceiveSlotLease> lease,
                                      const std::byte *data,
                                      const FrameView::Fields *fields) noexcept
{
    return FrameView(std::move(lease), data, fields);
}

} // namespace detail

} // namespace TangoBulk
