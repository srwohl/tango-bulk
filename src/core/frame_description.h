// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_CORE_FRAME_DESCRIPTION_H
#define TANGO_BULK_SRC_CORE_FRAME_DESCRIPTION_H

#include <tango-bulk/frame.h>

namespace TangoBulk::detail
{

/// One frame as the data plane describes it: what the producer supplied plus
/// what the publisher stamped. A receive slot owns one for the frame it holds;
/// a copied frame's sink owns its own copy. Every FrameView accessor reads it.
struct FrameDescription : FrameMetadata
{
    std::uint64_t sequence{0}; ///< monotonic within a session, from 0, never wraps
    std::uint64_t dropped_before{0}; ///< cumulative, publisher-side
    std::uint32_t generation{0};     ///< the geometry epoch the frame belongs to
};

/// Builds the view of a copied frame: `owner` keeps `data` alive for as long
/// as any copy of the view exists, and releasing the view withholds no credit.
/// The production copied-delivery path and every synthetic test frame come
/// through here.
class CopiedFrameFactory
{
  public:
    static FrameView make(std::shared_ptr<const void> owner,
                          const std::byte *data,
                          const FrameDescription &fields);
};

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_CORE_FRAME_DESCRIPTION_H
