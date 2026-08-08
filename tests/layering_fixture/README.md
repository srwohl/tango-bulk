<!--
SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project

SPDX-License-Identifier: LGPL-3.0-or-later
-->

# Layering-check fixture

Deliberately broken sources, used as the negative control for
`scripts/check_layering.py`. Every file here violates exactly one rule from
IMPLEMENTATION_SPEC.md 1.1.

Nothing here is compiled, and the real layering run does not scan this
directory: `check_layering.py`'s rules name `src/` and `tests/unit`,
`tests/ucx` explicitly, so the fixture is reachable only by pointing `--root`
at it.
