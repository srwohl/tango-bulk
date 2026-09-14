// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_TANGO_BULK_STREAM_ROW_H
#define TANGO_BULK_SRC_TANGO_BULK_STREAM_ROW_H

#include <tango-bulk/geometry.h>

#include <string>

// Internal header of the Tango layer.  The row format exists only because
// BulkStreams is a Tango string spectrum, so it lives with the adapter that
// reads and writes that attribute rather than in the core type it happens to
// carry.  Nothing outside src/tango/ has ever needed it.

namespace TangoBulk::detail
{

/// The fixed BulkStreams row, in the one field order both ends agree on:
/// version|name|generation|element_type|element_size|rank|max_frame_bytes|
/// ring_depth|credit_window|shape[0..3]|strides[0..3]|age_ms.
///
/// A row that does not decode comes back as a malformed offer rather than an
/// exception: discovery reads whatever a peer published, and an unparseable row
/// is an answer about that stream, not a failure of the reader.
StreamOffer stream_offer_from_row(const std::string &row);

/// One available offer, in the same field order.  An unavailable offer has no
/// row and therefore encodes as an empty string.
///
/// Byte order is deliberately absent: the row is a pre-Open upper bound on
/// sizing, and every array term a client acts on comes from OpenReply, which
/// does carry it.
std::string stream_offer_to_row(const StreamOffer &offer);

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_TANGO_BULK_STREAM_ROW_H
