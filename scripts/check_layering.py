#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
#
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Enforce the target layering from IMPLEMENTATION_SPEC.md 1.1.

    tango-bulk-core     libstdc++ only          no ucp/*, no tango/*
    tango-bulk-ucx      core + ucp/*            no tango/*
    tango-bulk-tango    core + tango/*          no ucp/*
    tango-bulk          all three

The spec calls this out as "cheap, and the only thing that keeps the layering
real".  It is deliberately a standalone script rather than a build step so CI
can run it on a checkout without a configured tree, and so a developer can run
it in under a second.

The load-bearing rule is the third one.  A device server that links the Tango
adapter must not inherit UCX headers; the adapter deals in encoded byte vectors
and opaque handles precisely so that it cannot.

Usage:
    scripts/check_layering.py [--root DIR] [--verbose]
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

SOURCE_SUFFIXES = {".h", ".hpp", ".hh", ".c", ".cc", ".cpp", ".cxx"}

# Matches a real preprocessor include only: the '#' must be the first
# non-whitespace character, so a commented-out include is not a violation.
INCLUDE_RE = re.compile(r"^\s*#\s*include\s*[<\"]([^>\"]+)[>\"]")

UCX_PREFIXES = ("ucp/", "uct/", "ucs/", "ucm/")
TANGO_PREFIXES = ("tango/",)


@dataclass(frozen=True)
class Rule:
    """Forbidden includes for one region of the tree."""

    name: str
    # Paths are repository-relative, matched as prefixes.
    paths: tuple[str, ...]
    forbidden: tuple[tuple[str, str], ...]  # (include prefix, why)
    exclude: tuple[str, ...] = ()


NO_UCX = tuple(
    (prefix, "UCX headers are confined to tango-bulk-ucx") for prefix in UCX_PREFIXES
)
NO_TANGO = tuple(
    (prefix, "Tango headers are confined to tango-bulk-tango") for prefix in TANGO_PREFIXES
)

RULES: tuple[Rule, ...] = (
    Rule(
        name="tango-bulk-core",
        paths=("src/core",),
        forbidden=NO_UCX + NO_TANGO,
    ),
    Rule(
        name="tango-bulk-ucx",
        paths=("src/ucx",),
        forbidden=NO_TANGO,
    ),
    Rule(
        name="tango-bulk-tango",
        paths=("src/tango",),
        forbidden=NO_UCX,
    ),
    # Public headers are compiled into every consumer, so they carry the
    # strictest rule.  tango.h is the declared exception: it is the Tango
    # adapter's own header and may name Tango types.
    Rule(
        name="public headers",
        paths=("include/tango-bulk",),
        forbidden=NO_UCX + NO_TANGO,
        exclude=("include/tango-bulk/tango.h",),
    ),
    Rule(
        name="public header include/tango-bulk/tango.h",
        paths=("include/tango-bulk/tango.h",),
        forbidden=NO_UCX,
    ),
    # Tests inherit the layering of the thing they test, so that a test cannot
    # quietly prove a property the library does not have.
    Rule(
        name="tests/unit",
        paths=("tests/unit",),
        forbidden=NO_UCX + NO_TANGO,
    ),
    Rule(
        name="tests/ucx",
        paths=("tests/ucx",),
        forbidden=NO_TANGO,
    ),
    # The benchmark links tango-bulk-ucx and inherits its rule.  This is not
    # bookkeeping: the whole point of the benchmark is that it runs on a machine
    # with a NIC and no Tango database, and a stray tango/* include would take
    # that away without breaking any test.
    Rule(
        name="benchmarks",
        paths=("benchmarks",),
        forbidden=NO_TANGO,
    ),
)


@dataclass
class Violation:
    path: Path
    line_number: int
    include: str
    rule: Rule
    why: str


@dataclass
class Report:
    checked: int = 0
    violations: list[Violation] = field(default_factory=list)


def iter_sources(root: Path, rule: Rule):
    excluded = {(root / e).resolve() for e in rule.exclude}

    for entry in rule.paths:
        base = root / entry
        if base.is_file():
            candidates = [base]
        elif base.is_dir():
            candidates = sorted(base.rglob("*"))
        else:
            continue

        for path in candidates:
            if not path.is_file() or path.suffix not in SOURCE_SUFFIXES:
                continue
            if path.resolve() in excluded:
                continue
            yield path


def check_rule(root: Path, rule: Rule, report: Report) -> None:
    for path in iter_sources(root, rule):
        report.checked += 1
        text = path.read_text(encoding="utf-8", errors="replace")

        for line_number, line in enumerate(text.splitlines(), start=1):
            match = INCLUDE_RE.match(line)
            if not match:
                continue

            included = match.group(1)
            for prefix, why in rule.forbidden:
                if included.startswith(prefix):
                    report.violations.append(
                        Violation(
                            path=path.relative_to(root),
                            line_number=line_number,
                            include=included,
                            rule=rule,
                            why=why,
                        )
                    )
                    break


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parent.parent,
        help="repository root (default: the directory above this script)",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    root = args.root.resolve()
    report = Report()

    for rule in RULES:
        before = len(report.violations)
        check_rule(root, rule, report)
        if args.verbose:
            found = len(report.violations) - before
            print(f"{rule.name}: {found} violation(s)")

    if report.violations:
        print(
            f"Layering violations ({len(report.violations)}) "
            "-- see IMPLEMENTATION_SPEC.md 1.1:",
            file=sys.stderr,
        )
        for v in report.violations:
            print(
                f"  {v.path}:{v.line_number}: #include <{v.include}> "
                f"in {v.rule.name}\n      {v.why}",
                file=sys.stderr,
            )
        return 1

    print(f"Layering OK ({report.checked} files checked)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
