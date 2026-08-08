// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
//
// SPDX-License-Identifier: LGPL-3.0-or-later
//
// Fixture: the UCX layer may include ucp/* but never tango/*.  Not compiled.

#include <ucp/api/ucp.h>

#include <tango/client/DeviceProxy.h>
