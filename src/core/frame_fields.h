// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_CORE_FRAME_FIELDS_H
#define TANGO_BULK_SRC_CORE_FRAME_FIELDS_H

#include <tango-bulk/frame.h>

namespace TangoBulk::detail
{

struct FrameFields
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

class DetachedFrameFactory
{
  public:
    static FrameView make(std::shared_ptr<const void> owner,
                          const std::byte *data,
                          const FrameFields &fields);
};

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_CORE_FRAME_FIELDS_H
