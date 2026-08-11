// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_FRAME_H
#define TANGO_BULK_FRAME_H

#include <tango-bulk/errors.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace TangoBulk
{

inline constexpr std::size_t k_max_rank = 4;

/// Numeric values are on the wire; fixed for protocol major 1.
enum class ElementType : std::uint32_t
{
    Unknown = 0,
    UInt8 = 1,
    Int8 = 2,
    UInt16 = 3,
    Int16 = 4,
    UInt32 = 5,
    Int32 = 6,
    UInt64 = 7,
    Int64 = 8,
    Float32 = 9,
    Float64 = 10,
    Byte = 11, ///< opaque, element_size 1
};

enum class MemoryKind : std::uint32_t
{
    Host = 0,
    Cuda = 1,
    Rocm = 2,
};

enum class Endian : std::uint32_t
{
    Little = 0,
    Big = 1,
};

/// What to discard when a bounded queue or ring is full.
///
/// IMPLEMENTATION_SPEC.md declares this in publisher.h.  It lives here with the
/// other vocabulary types because the coordination plane carries it in `Open`,
/// so protocol.h needs it too, and protocol.h has no business including the
/// publisher API.  The declaration a consumer sees is unchanged: publisher.h and
/// subscriber.h both include this header.
enum class DropPolicy : std::uint32_t
{
    DropNewest = 0, ///< reject the frame being published (producer default)
    DropOldest = 1, ///< delivery queue only; illegal on the producer ring
};

const char *to_string(DropPolicy policy) noexcept;

const char *to_string(ElementType type) noexcept;
const char *to_string(MemoryKind kind) noexcept;

/// Bytes per element, or 0 for ElementType::Unknown.
std::uint32_t element_size_of(ElementType type) noexcept;

/// Producer-supplied description of one frame.
///
/// Populated from device state, never derived from an AttributeValue_5 or any
/// other CDR object.  That is the point: a bulk frame is self-contained, and
/// nothing about interpreting it requires a Tango round trip.
struct FrameMetadata
{
    ElementType element_type{ElementType::Unknown};
    std::uint32_t element_size{0}; ///< 0 => infer via element_size_of()
    std::uint32_t rank{0};         ///< 0..k_max_rank
    std::array<std::uint64_t, k_max_rank> shape{};
    std::array<std::uint64_t, k_max_rank> strides{}; ///< bytes; 0 => infer C-contiguous
    std::uint64_t payload_bytes{0};  ///< 0 => infer from shape x element_size
    std::uint64_t timestamp_ns{0};   ///< CLOCK_REALTIME; 0 => stamp at publish
    std::uint64_t event_counter{0};  ///< producer-defined; opaque to the protocol
    std::uint32_t quality{0};        ///< producer-defined
    MemoryKind memory_kind{MemoryKind::Host};
    Endian endian{Endian::Little};   ///< byte order of payload *elements*

    /// Fill in the fields documented above as inferrable, in place.
    ///
    /// Returns Status::Ok and leaves `*this` fully populated, or returns the
    /// reason it could not and leaves `*this` unchanged.  Callers that want to
    /// check without mutating should copy first.
    Status resolve(std::uint64_t max_frame_bytes) noexcept;

    /// Rejects rank > k_max_rank, zero/oversize element_size, stride/shape
    /// products that overflow or exceed payload_bytes, and Unknown with
    /// rank > 0.  Does not infer: a metadata that still has an inferrable zero
    /// in it is rejected, so validate() never silently accepts an incomplete
    /// description.
    Status validate(std::uint64_t max_frame_bytes) const noexcept;
};

namespace detail
{
class ReceiveSlotLease;
} // namespace detail

/// Read-only, reference-counted view of one delivered frame.
///
/// Every copy keeps the receive slot alive, and the credit for that slot is
/// withheld until the last copy is destroyed.  Applications needing unbounded
/// ownership MUST copy the bytes out and release the view.
///
/// The shared_ptr control block *is* the credit interlock.  Destruction and
/// reset() are the only ways a credit returns; there is deliberately no
/// release_credit() call for an application to forget.
class FrameView
{
  public:
    FrameView() noexcept = default;
    FrameView(const FrameView &) noexcept = default;
    FrameView(FrameView &&) noexcept = default;
    FrameView &operator=(const FrameView &) noexcept = default;
    FrameView &operator=(FrameView &&) noexcept = default;
    ~FrameView() = default;

    explicit operator bool() const noexcept;

    const std::byte *data() const noexcept;
    std::size_t size() const noexcept;
    const std::byte *begin() const noexcept;
    const std::byte *end() const noexcept;

    ElementType element_type() const noexcept;
    std::uint32_t element_size() const noexcept;
    std::uint32_t rank() const noexcept;
    const std::array<std::uint64_t, k_max_rank> &shape() const noexcept;
    const std::array<std::uint64_t, k_max_rank> &strides() const noexcept;

    std::uint64_t sequence() const noexcept;
    std::uint64_t event_counter() const noexcept;
    std::uint64_t timestamp_ns() const noexcept;
    std::uint64_t dropped_before() const noexcept; ///< cumulative, publisher-side
    std::uint32_t quality() const noexcept;
    std::uint32_t generation() const noexcept; ///< geometry epoch
    MemoryKind memory_kind() const noexcept;
    Endian endian() const noexcept;

    long use_count() const noexcept; ///< diagnostics/tests only
    void reset() noexcept;           ///< release early; returns the credit

    /// POD carried by value inside the receive slot.  Public so the receive
    /// path can populate one; there is no reason for it to be otherwise, and
    /// hiding it would only force a friend declaration per implementation file.
    struct Fields
    {
        std::array<std::uint64_t, k_max_rank> shape{};
        std::array<std::uint64_t, k_max_rank> strides{};
        std::uint64_t sequence{0};
        std::uint64_t event_counter{0};
        std::uint64_t timestamp_ns{0};
        std::uint64_t dropped_before{0};
        std::uint64_t payload_bytes{0};
        ElementType element_type{ElementType::Unknown};
        std::uint32_t element_size{0};
        std::uint32_t rank{0};
        std::uint32_t quality{0};
        std::uint32_t generation{0};
        MemoryKind memory_kind{MemoryKind::Host};
        Endian endian{Endian::Little};
    };

    /// A view over caller-owned memory that references no receive slot.
    ///
    /// Every other FrameView comes out of the receive path, whose constructor is
    /// private -- which left an application unable to build one at all, and so
    /// unable to unit-test any function that takes a FrameView.  Since the
    /// callback signature is `void(FrameView)`, that is most of what an
    /// application writes against this library: the geometry checks, the
    /// retention policy, the fan-out to a consumer.  Those were testable only
    /// against a live publisher, which is not a unit test and does not run in
    /// CI.  This factory is the fix.
    ///
    /// The view it returns is observationally identical to a delivered one.  It
    /// holds a real lease, so `operator bool` is true, `use_count()` counts
    /// copies, and `reset()` drops the reference -- the credit simply goes to a
    /// sink that discards it.  That equivalence is the point: it is what makes
    /// "does my code hold the view longer than it should?" a question a test can
    /// ask, and holding a view too long is the mistake this API is easiest to
    /// make.
    ///
    /// `owner` shares ownership of whatever keeps `data` alive and may be null
    /// when the caller guarantees that lifetime by other means.  `data` is not
    /// dereferenced here and may point at device memory.  `fields` is copied,
    /// so the caller need not keep it alive; note that `size()` reports
    /// `fields.payload_bytes` and nothing cross-checks it against the
    /// allocation behind `data`.
    ///
    ///     const auto pixels = std::make_shared<std::vector<std::uint16_t>>(w * h);
    ///     FrameView::Fields fields;
    ///     fields.rank = 2;
    ///     fields.shape = {h, w, 0, 0};
    ///     fields.strides = {w * 2, 2, 0, 0};
    ///     fields.element_type = ElementType::UInt16;
    ///     fields.element_size = 2;
    ///     fields.payload_bytes = w * h * 2;
    ///
    ///     const FrameView frame = FrameView::detached(
    ///         pixels, reinterpret_cast<const std::byte *>(pixels->data()), fields);
    static FrameView detached(std::shared_ptr<const void> owner,
                              const std::byte *data,
                              const Fields &fields);

  private:
    friend class detail::ReceiveSlotLease;

    FrameView(std::shared_ptr<detail::ReceiveSlotLease> lease,
              const std::byte *data,
              const Fields *fields) noexcept;

    std::shared_ptr<detail::ReceiveSlotLease> lease_;
    const std::byte *data_{nullptr};
    const Fields *fields_{nullptr};
};

} // namespace TangoBulk

#endif // TANGO_BULK_FRAME_H
