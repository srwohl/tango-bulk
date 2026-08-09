<!--
SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project

SPDX-License-Identifier: LGPL-3.0-or-later
-->

# Committed sweeps

Raw benchmark output, unedited, one file per run. §9.4 asks for results as
"reproducible command lines, machine/NIC/UCX configuration, raw output, and a
conclusion": the first three live here, the conclusions in
[../../docs/THROUGHPUT.md](../../docs/THROUGHPUT.md).

Raw rather than summarised on purpose. A table in a document is someone's reading
of a run; these are the runs. Every file names the command that produced it, so a
number can be re-checked rather than trusted.

Files are named `<date>-<transport>[-<note>].txt` and are append-only history —
a later run is a new file, never an edit to an old one.
