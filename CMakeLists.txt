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

include(CTest)

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

find_package(Threads REQUIRED)
find_package(UCX 1.21.0 REQUIRED)

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

add_library(tango-bulk-settings INTERFACE)
add_library(tango-bulk::settings ALIAS tango-bulk-settings)

target_compile_options(
    tango-bulk-settings
    INTERFACE -Wall
              -Wextra
              -Wpedantic
              -Wconversion
              -Wsign-conversion
              -Wshadow
              -Wold-style-cast
              -Wnon-virtual-dtor)

if(TANGO_BULK_WARNINGS_AS_ERRORS)
    target_compile_options(tango-bulk-settings INTERFACE -Werror)
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
