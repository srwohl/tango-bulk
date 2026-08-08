# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
#
# SPDX-License-Identifier: LGPL-3.0-or-later
#
# Exercises tango_bulk_require_installed_tango() in isolation, via `cmake -P`.
#
# The guard is the mechanical form of the M0 exit criterion, so it needs a
# negative control: a check that has never been seen to fail is not a check.
# Run as:
#
#   cmake -DCASE=<clean|build_tree|source_tree> -DSCRATCH=<dir> \
#         -DMODULE_DIR=<dir> -P installed_tango_guard.cmake

cmake_minimum_required(VERSION 3.18)

if(NOT DEFINED CASE OR NOT DEFINED SCRATCH OR NOT DEFINED MODULE_DIR)
    message(FATAL_ERROR "CASE, SCRATCH and MODULE_DIR are all required")
endif()

list(APPEND CMAKE_MODULE_PATH "${MODULE_DIR}")
include(TangoBulkLayering)

# The fixtures live under SCRATCH, which in practice is inside the project's own
# build tree.  That is safe because _tango_bulk_classify_tree() stops at the
# *deepest* match walking upward, so a fixture marker is always found before the
# enclosing build tree -- and the assertions below check which directory the
# guard named, not merely that it complained.
set(fixture "${SCRATCH}/${CASE}")
file(REMOVE_RECURSE "${fixture}")

if(CASE STREQUAL "clean")
    # Deliberately NOT under SCRATCH.  The clean case is the one where nothing
    # may be found all the way up to the filesystem root, and SCRATCH is inside
    # this project's own build tree -- which the guard is correct to reject.
    # A synthetic path that does not exist exercises exactly the walk a real
    # installed prefix takes, without needing one.
    set(Tango_DIR "/nonexistent-tango-bulk-guard/prefix/lib/cmake/tango")
    set(Tango_INCLUDE_DIRS "/nonexistent-tango-bulk-guard/prefix/include")

    tango_bulk_require_installed_tango()

    message(STATUS "GUARD-TEST: clean prefix accepted")

elseif(CASE STREQUAL "build_tree")
    # A CMakeCache.txt above the package directory: this is what
    # -DTango_DIR=/path/to/cppTango/build/tango looks like.
    file(MAKE_DIRECTORY "${fixture}/bt/tango")
    file(WRITE "${fixture}/bt/CMakeCache.txt" "# fixture\n")

    set(Tango_DIR "${fixture}/bt/tango")

    tango_bulk_require_installed_tango()

    message(FATAL_ERROR "GUARD-TEST: build tree was NOT rejected")

elseif(CASE STREQUAL "source_tree")
    # Both cppTango source-tree markers, with the package resolving to a prefix
    # nested inside the checkout -- the shape a `pixi install` into the source
    # tree's own environment produces.
    file(MAKE_DIRECTORY "${fixture}/st/src/include/tango")
    file(MAKE_DIRECTORY "${fixture}/st/configure")
    file(MAKE_DIRECTORY "${fixture}/st/prefix/lib/cmake/tango")
    file(WRITE "${fixture}/st/src/include/tango/tango.h" "// fixture\n")
    file(WRITE "${fixture}/st/configure/functions.cmake" "# fixture\n")

    set(Tango_DIR "${fixture}/st/prefix/lib/cmake/tango")

    tango_bulk_require_installed_tango()

    message(FATAL_ERROR "GUARD-TEST: source tree was NOT rejected")

else()
    message(FATAL_ERROR "unknown CASE '${CASE}'")
endif()
