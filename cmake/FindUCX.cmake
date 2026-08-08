# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
#
# SPDX-License-Identifier: LGPL-3.0-or-later
#
# FindUCX
# -------
#
# Locates UCX (https://openucx.org) and defines:
#
#   UCX::ucp        imported target for libucp, libuct, libucs
#   UCX_FOUND
#   UCX_VERSION
#   UCX_INCLUDE_DIR
#
# UCX ships a pkg-config file in most packagings but not all, and its own CMake
# config package is not universally installed, so this module tries pkg-config
# first and then falls back to a manual search.  Version comes from
# ucp/api/ucp_version.h, which is present in every packaging.

include(FindPackageHandleStandardArgs)

find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(PC_UCX QUIET ucx)
endif()

find_path(
    UCX_INCLUDE_DIR
    NAMES ucp/api/ucp.h
    HINTS ${PC_UCX_INCLUDE_DIRS} ${UCX_ROOT} ENV UCX_ROOT
    PATH_SUFFIXES include)

find_library(
    UCX_UCP_LIBRARY
    NAMES ucp
    HINTS ${PC_UCX_LIBRARY_DIRS} ${UCX_ROOT} ENV UCX_ROOT
    PATH_SUFFIXES lib lib64)

find_library(
    UCX_UCT_LIBRARY
    NAMES uct
    HINTS ${PC_UCX_LIBRARY_DIRS} ${UCX_ROOT} ENV UCX_ROOT
    PATH_SUFFIXES lib lib64)

find_library(
    UCX_UCS_LIBRARY
    NAMES ucs
    HINTS ${PC_UCX_LIBRARY_DIRS} ${UCX_ROOT} ENV UCX_ROOT
    PATH_SUFFIXES lib lib64)

if(UCX_INCLUDE_DIR AND EXISTS "${UCX_INCLUDE_DIR}/ucp/api/ucp_version.h")
    file(STRINGS "${UCX_INCLUDE_DIR}/ucp/api/ucp_version.h" _ucx_version_lines
         REGEX "^#define[ \t]+UCP_API_(MAJOR|MINOR)[ \t]+[0-9]+")

    string(REGEX MATCH "UCP_API_MAJOR[ \t]+([0-9]+)" _ "${_ucx_version_lines}")
    set(_ucx_major "${CMAKE_MATCH_1}")
    string(REGEX MATCH "UCP_API_MINOR[ \t]+([0-9]+)" _ "${_ucx_version_lines}")
    set(_ucx_minor "${CMAKE_MATCH_1}")

    if(_ucx_major AND NOT _ucx_minor STREQUAL "")
        # ucp_version.h carries the API version, which tracks the release
        # major.minor.  The patch level is not exposed there; report 0 rather
        # than guess, and let pkg-config override when it is available.
        set(UCX_VERSION "${_ucx_major}.${_ucx_minor}.0")
    endif()
endif()

if(PC_UCX_VERSION)
    set(UCX_VERSION "${PC_UCX_VERSION}")
endif()

find_package_handle_standard_args(
    UCX
    REQUIRED_VARS UCX_INCLUDE_DIR UCX_UCP_LIBRARY UCX_UCT_LIBRARY
                  UCX_UCS_LIBRARY
    VERSION_VAR UCX_VERSION)

if(UCX_FOUND AND NOT TARGET UCX::ucp)
    add_library(UCX::ucs UNKNOWN IMPORTED)
    set_target_properties(UCX::ucs PROPERTIES IMPORTED_LOCATION
                                              "${UCX_UCS_LIBRARY}")

    add_library(UCX::uct UNKNOWN IMPORTED)
    set_target_properties(UCX::uct PROPERTIES IMPORTED_LOCATION
                                              "${UCX_UCT_LIBRARY}")

    add_library(UCX::ucp UNKNOWN IMPORTED)
    set_target_properties(
        UCX::ucp
        PROPERTIES IMPORTED_LOCATION "${UCX_UCP_LIBRARY}"
                   INTERFACE_INCLUDE_DIRECTORIES "${UCX_INCLUDE_DIR}"
                   INTERFACE_LINK_LIBRARIES "UCX::uct;UCX::ucs")
endif()

mark_as_advanced(UCX_INCLUDE_DIR UCX_UCP_LIBRARY UCX_UCT_LIBRARY
                 UCX_UCS_LIBRARY)
