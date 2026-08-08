# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
#
# SPDX-License-Identifier: LGPL-3.0-or-later
#
# Mechanical enforcement of the two boundaries the project is built around.
#
#   1. tango-bulk depends on an INSTALLED cppTango, never on a source or build
#      tree.  (IMPLEMENTATION_SPEC.md 1.2)
#   2. The four library targets obey the include rules in 1.1.  That check lives
#      in scripts/check_layering.py so CI can run it without configuring.
#
# Both exist because "out of tree" and "layered" are otherwise properties that
# survive only as long as everyone remembers them in review.

include_guard(GLOBAL)

# Walk from `path` up to the filesystem root looking for evidence that it sits
# inside a CMake build tree or a cppTango source checkout.  Sets `out_reason` to
# a human-readable explanation, or to the empty string if the path is clean.
function(_tango_bulk_classify_tree path out_reason)
    set(reason "")

    get_filename_component(dir "${path}" ABSOLUTE)
    if(NOT IS_DIRECTORY "${dir}")
        get_filename_component(dir "${dir}" DIRECTORY)
    endif()

    set(previous "")
    while(NOT dir STREQUAL previous)
        if(EXISTS "${dir}/CMakeCache.txt")
            set(reason "it is inside the CMake build tree '${dir}'")
            break()
        endif()

        # cppTango source-tree fingerprint: both markers, so an unrelated
        # project that merely has a `configure/` directory is not flagged.
        if(EXISTS "${dir}/src/include/tango/tango.h"
           AND EXISTS "${dir}/configure/functions.cmake")
            set(reason "it is inside the cppTango source tree '${dir}'")
            break()
        endif()

        set(previous "${dir}")
        get_filename_component(dir "${dir}" DIRECTORY)
    endwhile()

    set(${out_reason}
        "${reason}"
        PARENT_SCOPE)
endfunction()

# Fail configuration unless the Tango package that find_package() resolved is a
# real installation.
#
# This is the mechanical form of the M0 exit criterion.  Building against a
# cppTango build tree is how an extension acquires an accidental dependency on
# an unreleased header or a locally patched ABI, and it is exactly the failure
# this project was restructured to make impossible.
function(tango_bulk_require_installed_tango)
    set(candidates "")

    foreach(var Tango_DIR Tango_CONFIG Tango_INCLUDE_DIRS)
        if(DEFINED ${var})
            list(APPEND candidates ${${var}})
        endif()
    endforeach()

    if(TARGET Tango::Tango)
        get_target_property(includes Tango::Tango
                            INTERFACE_INCLUDE_DIRECTORIES)
        if(includes)
            foreach(inc IN LISTS includes)
                # Generator expressions cannot be evaluated at configure time.
                # An installed package does not use them; a build-tree export
                # does, and is caught by Tango_DIR anyway.
                if(NOT inc MATCHES "\\$<")
                    list(APPEND candidates "${inc}")
                endif()
            endforeach()
        endif()
    endif()

    list(REMOVE_DUPLICATES candidates)

    set(problems "")
    foreach(candidate IN LISTS candidates)
        _tango_bulk_classify_tree("${candidate}" reason)
        if(reason)
            list(APPEND problems "  ${candidate}\n      -> ${reason}")
        endif()
    endforeach()

    if(problems)
        string(REPLACE ";" "\n" problems "${problems}")
        message(
            FATAL_ERROR
                "find_package(Tango) resolved into a cppTango source or build tree.\n"
                "tango-bulk must build against an INSTALLED cppTango package "
                "(IMPLEMENTATION_SPEC.md 1.2).\n\n"
                "${problems}\n\n"
                "Install cppTango to a prefix and point CMAKE_PREFIX_PATH at "
                "that prefix, or use the conda-forge 'cpptango' package "
                "(`pixi install` in this repository does exactly that).")
    endif()

    message(STATUS "Tango resolved to an installed package: ${Tango_DIR}")
endfunction()
