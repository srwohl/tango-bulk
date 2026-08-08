// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later
//
// Fixture: the Tango adapter may include tango/* but never ucp/*.  This is the
// load-bearing rule -- a device server that links the adapter must not inherit
// UCX headers.  Not compiled.

#include <tango/tango.h>

#include <ucp/api/ucp.h>
