# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
#
# SPDX-License-Identifier: LGPL-3.0-or-later

# tango-bulk-ucx: core + ucp/*.  No tango/*.
#
# This is the only target that may name a UCX type.  The ucp_worker lives here,
# is private to the engine, and is never returned by any accessor -- see
# IMPLEMENTATION_SPEC.md 5.1.

add_library(
    tango-bulk-ucx
    locality.cpp
    publisher_impl.cpp
    registered_memory.cpp
    registered_ring.cpp
    subscriber_engine.cpp
    ucx_context.cpp
    ucx_support.cpp)
add_library(tango-bulk::ucx ALIAS tango-bulk-ucx)

target_include_directories(
    tango-bulk-ucx
    PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
           $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE ${PROJECT_SOURCE_DIR}/src)

# UCX is PRIVATE: a consumer of tango-bulk links the transport but never
# compiles against its headers.
target_link_libraries(
    tango-bulk-ucx
    PUBLIC tango-bulk::core tango-bulk::settings
    PRIVATE UCX::ucp Threads::Threads)
