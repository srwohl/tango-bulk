// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later
//
// Fixture: the Python adapter reaches Subscription through the private
// construction seam and must not compile against Tango or UCX directly.

#include <tango/tango.h>
#include <ucp/api/ucp.h>
