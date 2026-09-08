// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_CORE_PUBLISHER_INTERNAL_H
#define TANGO_BULK_SRC_CORE_PUBLISHER_INTERNAL_H

#include <tango-bulk/protocol.h>
#include <tango-bulk/publisher.h>

#include <cstddef>
#include <optional>
#include <vector>

namespace TangoBulk::detail
{

/// Internal access for byte-bearing coordination adapters.
///
/// The public publisher owns publication and session state; this access shim
/// keeps the encoded ingress out of the public class. Tango and in-process
/// adapters carry bytes through the same boundary, while typed request
/// handling remains private to `BulkPublisher::Impl`.
class PublisherAccess
{
  public:
    static std::vector<std::byte> coordination(
        BulkPublisher &publisher,
        const std::byte *data,
        std::size_t size,
        std::optional<Protocol::CoordType> expected = std::nullopt) noexcept;
};

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_CORE_PUBLISHER_INTERNAL_H
