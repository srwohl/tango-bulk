#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
#
# SPDX-License-Identifier: LGPL-3.0-or-later
#
# Run the sanitizer-relevant suites under ThreadSanitizer.
#
# Directly rather than through ctest: TSan needs ASLR disabled to map its shadow
# memory, and without that even --list-tests aborts at startup, so
# catch_discover_tests cannot enumerate anything at build time.
set -euo pipefail

export TSAN_OPTIONS="${TSAN_OPTIONS:-halt_on_error=0 history_size=4}"

if ! setarch -R true 2>/dev/null; then
    echo "run_tsan_tests: setarch -R is unavailable; skipping." >&2
    exit 0
fi

status=0
for binary in build-tsan/tests/tango-bulk-ucx-tests build-tsan/tests/tango-bulk-unit-tests; do
    if [[ ! -x "$binary" ]]; then
        echo "run_tsan_tests: $binary not built; skipping." >&2
        continue
    fi
    echo "== $binary =="
    setarch -R "$binary" || status=$?
done

exit "$status"
