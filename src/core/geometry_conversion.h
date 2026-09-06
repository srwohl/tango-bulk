// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later

#ifndef TANGO_BULK_SRC_CORE_GEOMETRY_CONVERSION_H
#define TANGO_BULK_SRC_CORE_GEOMETRY_CONVERSION_H

#include <tango-bulk/geometry.h>
#include <tango-bulk/protocol.h>

namespace TangoBulk::detail
{

Geometry to_geometry(const Protocol::GeometryBlock &wire) noexcept;

} // namespace TangoBulk::detail

#endif // TANGO_BULK_SRC_CORE_GEOMETRY_CONVERSION_H
