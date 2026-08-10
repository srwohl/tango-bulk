# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
#
# SPDX-License-Identifier: LGPL-3.0-or-later

# tango-bulk-tango: core + tango/*.  No ucp/*.
#
# The rule that matters is the negative one.  This target is what a device
# server links, and it deals only in encoded byte vectors and opaque handles, so
# linking it never drags UCX headers into the device server's build.

add_library(tango-bulk-tango commands.cpp proxy_client.cpp registration.cpp
                             tango_support.cpp)
add_library(tango-bulk::tango ALIAS tango-bulk-tango)

target_include_directories(
    tango-bulk-tango
    PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
           $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE ${PROJECT_SOURCE_DIR}/src)

# Tango is PUBLIC: tango-bulk/tango.h names Tango types, so anything compiling
# against this target needs the Tango headers anyway.
#
# tango-bulk::ucx is PRIVATE, and the distinction is the whole point of the
# layering rule.  BulkSubscriber has to *construct* a transport, which happens
# through the factory in core/subscriber_transport.h whose definition lives in
# the UCX library -- so this target links it.  PRIVATE means CMake records the
# dependency as $<LINK_ONLY:>, so a device server linking tango-bulk::tango gets
# the symbols and not one UCX include directory.  UCX::ucp is itself PRIVATE to
# tango-bulk-ucx, so there is nothing to inherit even transitively.
target_link_libraries(
    tango-bulk-tango
    PUBLIC tango-bulk::core tango-bulk::settings Tango::Tango
    PRIVATE tango-bulk::ucx Threads::Threads)

# cppTango's public headers do not survive this project's warning set, and it is
# not this project's business to demand that they should.  No action is needed:
# CMake already treats an IMPORTED target's interface includes as SYSTEM, so
# Tango::Tango suppresses its own warnings.
#
# Re-adding them here with SYSTEM PUBLIC, as an earlier revision did, breaks
# install(EXPORT): the conda prefix lives under .pixi/ inside this source tree,
# and an exported target may not carry an include directory inside the source
# directory.  The symptom is a generate-step failure that names cppTango and has
# nothing to do with it.
