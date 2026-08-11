# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
#
# SPDX-License-Identifier: LGPL-3.0-or-later

cmake_minimum_required(VERSION 3.18)

project(
    tango-bulk
    VERSION 0.1.0
    DESCRIPTION "UCX bulk-data plane for Tango device servers"
    LANGUAGES CXX)

# Linux only, and it must fail at configure time rather than emit stub APIs.
# IMPLEMENTATION_SPEC.md section 0.
if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(
        FATAL_ERROR
            "tango-bulk supports Linux only (found '${CMAKE_SYSTEM_NAME}'). "
            "UCX does not support this platform, and the project deliberately "
            "does not ship stub APIs that would compile but never carry data.")
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(TangoBulkLayering)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE
        RelWithDebInfo
        CACHE STRING "" FORCE)
endif()

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(TANGO_BULK_BUILD_TANGO_ADAPTER "Build the Tango adapter library" ON)
option(TANGO_BULK_BUILD_EXAMPLES "Build the example device and client" OFF)
option(TANGO_BULK_BUILD_BENCHMARKS "Build the throughput benchmarks" OFF)
option(TANGO_BULK_WARNINGS_AS_ERRORS "Treat compiler warnings as errors" OFF)

# The truncation and corruption sweeps assert that no input walks a decoder off
# the end of a buffer.  No return value can express that, so the sweeps are only
# worth their runtime when something is watching the memory -- which makes this
# option part of the test strategy rather than a debugging convenience.
set(TANGO_BULK_SANITIZERS
    ""
    CACHE STRING "Comma-separated -fsanitize list, e.g. address,undefined")

include(CTest)

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

find_package(Threads REQUIRED)

# One variable, used here and substituted into tango-bulk-config.cmake.in, so a
# consumer is asked for the version this project actually requires.  Writing the
# found version into the package config instead -- which is what
# find_dependency(UCX @UCX_VERSION@) did -- pins consumers to whatever UCX
# happened to be on the machine that built the package, and refuses a host with
# a UCX this library supports.
set(TANGO_BULK_UCX_MIN_VERSION 1.21.0)
find_package(UCX ${TANGO_BULK_UCX_MIN_VERSION} REQUIRED)

if(TANGO_BULK_BUILD_TANGO_ADAPTER)
    # Config mode only. The Tango package must be an INSTALLED package -- see
    # tango_bulk_require_installed_tango() below for why that is checked rather
    # than assumed.
    find_package(Tango REQUIRED)
    tango_bulk_require_installed_tango()
endif()

# ---------------------------------------------------------------------------
# Common compile settings
#
# An INTERFACE target rather than directory-scope flags, so the settings travel
# with the targets and nothing leaks into a consumer's build.
# ---------------------------------------------------------------------------

# tango-bulk-settings carries what genuinely IS part of this library's interface
# contract: the language standard its headers require, and -- when built with
# them -- the sanitizer flags, which change the ABI of the archives being
# installed and so must reach anyone linking them.
#
# The warning set is not on it, and that separation is the whole design.  See
# below.
add_library(tango-bulk-settings INTERFACE)
add_library(tango-bulk::settings ALIAS tango-bulk-settings)

# ---------------------------------------------------------------------------
# The warning policy
#
# A plain list applied PRIVATE, deliberately not an INTERFACE target.
#
# These flags are how this project chooses to compile its own source.  They are
# editorial policy, not an interface contract, and a consumer must never inherit
# them: a translation unit that includes a C library's headers then fails to
# compile on our house style.  GStreamer's GST_BUFFER_PTS and glib's
# G_TYPE_CHECK_INSTANCE_CAST expand to C casts at the CONSUMER's line numbers,
# where -Wold-style-cast has nothing useful to say and -Werror is fatal.
#
# This began life as an INTERFACE target wrapped in $<BUILD_INTERFACE:>, which
# stopped the flags at the installed package and nowhere else.  A consumer using
# FetchContent or add_subdirectory *is* the build tree, so BUILD_INTERFACE
# evaluates true and every flag lands on their sources -- the exact failure the
# guard was written to prevent, reached by the other road.
#
# PRIVATE on an INTERFACE target would not have fixed it either: a PRIVATE
# dependency of a static library is still recorded as $<LINK_ONLY:...> in its
# INTERFACE_LINK_LIBRARIES, so install(EXPORT) would refuse to export
# tango-bulk-core until tango-bulk-warnings joined the export set -- putting a
# target whose entire purpose is to not be installed into the installed package.
#
# A list and a function have neither problem.  PRIVATE compile options never
# appear in INTERFACE_COMPILE_OPTIONS, so there is no route by which these can
# reach a consumer, by any consumption mechanism, whether or not anyone
# remembers this comment.
# ---------------------------------------------------------------------------

set(TANGO_BULK_WARNING_FLAGS
    -Wall
    -Wextra
    -Wpedantic
    -Wconversion
    -Wsign-conversion
    -Wshadow
    -Wold-style-cast
    -Wnon-virtual-dtor)

if(TANGO_BULK_WARNINGS_AS_ERRORS)
    list(APPEND TANGO_BULK_WARNING_FLAGS -Werror)
endif()

# Applies the project warning policy to targets built from this project's own
# source.  Every such target calls it; nothing else does.
function(tango_bulk_apply_warnings)
    foreach(target IN LISTS ARGN)
        target_compile_options(${target} PRIVATE ${TANGO_BULK_WARNING_FLAGS})
    endforeach()
endfunction()

if(TANGO_BULK_SANITIZERS)
    target_compile_options(
        tango-bulk-settings INTERFACE -fsanitize=${TANGO_BULK_SANITIZERS}
                                      -fno-omit-frame-pointer)
    target_link_options(tango-bulk-settings INTERFACE
                        -fsanitize=${TANGO_BULK_SANITIZERS})
    message(STATUS "  Sanitizers      ${TANGO_BULK_SANITIZERS}")
endif()

target_compile_features(tango-bulk-settings INTERFACE cxx_std_17)

# ---------------------------------------------------------------------------
# Targets
#
# The layering in section 1.1 of the spec is the reason there are four targets
# rather than one library. It is enforced mechanically by
# scripts/check_layering.py, wired below as a test.
#
#   tango-bulk-core   libstdc++ only      no ucp/*, no tango/*
#   tango-bulk-ucx    core + ucp/*        no tango/*
#   tango-bulk-tango  core + tango/*      no ucp/*
#   tango-bulk        all three
# ---------------------------------------------------------------------------

add_subdirectory(src/core)
add_subdirectory(src/ucx)

if(TANGO_BULK_BUILD_TANGO_ADAPTER)
    add_subdirectory(src/tango)
endif()

# The umbrella. Header-only by construction: it exists so an application can
# link one name and get the whole extension, not to hold code of its own.
add_library(tango-bulk INTERFACE)
add_library(tango-bulk::tango-bulk ALIAS tango-bulk)

target_link_libraries(tango-bulk INTERFACE tango-bulk::core tango-bulk::ucx)

if(TANGO_BULK_BUILD_TANGO_ADAPTER)
    target_link_libraries(tango-bulk INTERFACE tango-bulk::tango)
endif()

# ---------------------------------------------------------------------------
# Tests, examples, benchmarks
# ---------------------------------------------------------------------------

if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

if(TANGO_BULK_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if(TANGO_BULK_BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()

# ---------------------------------------------------------------------------
# Install and export
# ---------------------------------------------------------------------------

set(TANGO_BULK_INSTALL_TARGETS tango-bulk tango-bulk-settings tango-bulk-core
                               tango-bulk-ucx)

if(TANGO_BULK_BUILD_TANGO_ADAPTER)
    list(APPEND TANGO_BULK_INSTALL_TARGETS tango-bulk-tango)
endif()

# install(EXPORT ... NAMESPACE tango-bulk::) prefixes the TARGET name, and the
# in-tree ALIASes do not survive export.  Without EXPORT_NAME the installed
# package therefore offers tango-bulk::tango-bulk-tango while this repository's
# own examples, its README and its documentation all say tango-bulk::tango --
# so every code sample here is wrong for the installed package, and a consumer
# has to read the generated targets file to find out what to link.
#
# EXPORT_NAME makes the two spellings the same one.  The umbrella needs no entry:
# tango-bulk exports as tango-bulk::tango-bulk, which is already its alias.
set_target_properties(tango-bulk-settings PROPERTIES EXPORT_NAME settings)
set_target_properties(tango-bulk-core PROPERTIES EXPORT_NAME core)
set_target_properties(tango-bulk-ucx PROPERTIES EXPORT_NAME ucx)

if(TANGO_BULK_BUILD_TANGO_ADAPTER)
    set_target_properties(tango-bulk-tango PROPERTIES EXPORT_NAME tango)
endif()

install(
    TARGETS ${TANGO_BULK_INSTALL_TARGETS}
    EXPORT tango-bulk-targets
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})

install(DIRECTORY include/tango-bulk
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(
    EXPORT tango-bulk-targets
    FILE tango-bulk-targets.cmake
    NAMESPACE tango-bulk::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/tango-bulk)

configure_package_config_file(
    cmake/tango-bulk-config.cmake.in
    "${CMAKE_CURRENT_BINARY_DIR}/tango-bulk-config.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/tango-bulk)

write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/tango-bulk-config-version.cmake"
    COMPATIBILITY SameMinorVersion)

install(
    FILES "${CMAKE_CURRENT_BINARY_DIR}/tango-bulk-config.cmake"
          "${CMAKE_CURRENT_BINARY_DIR}/tango-bulk-config-version.cmake"
          "${CMAKE_CURRENT_SOURCE_DIR}/cmake/FindUCX.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/tango-bulk)

# ---------------------------------------------------------------------------

message(STATUS "tango-bulk ${PROJECT_VERSION}")
message(STATUS "  UCX             ${UCX_VERSION} (${UCX_INCLUDE_DIR})")
if(TANGO_BULK_BUILD_TANGO_ADAPTER)
    message(STATUS "  Tango           ${Tango_VERSION} (${Tango_DIR})")
else()
    message(STATUS "  Tango           adapter disabled")
endif()
message(STATUS "  Tests           ${BUILD_TESTING}")
