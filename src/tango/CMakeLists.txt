# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
#
# SPDX-License-Identifier: LGPL-3.0-or-later

# tango-bulk-tango: core + tango/*.  No ucp/*.
#
# The rule that matters is the negative one.  This target is what a device
# server links, and it deals only in encoded byte vectors and opaque handles, so
# linking it never drags UCX headers into the device server's build.

add_library(tango-bulk-tango registration.cpp tango_support.cpp)
add_library(tango-bulk::tango ALIAS tango-bulk-tango)

target_include_directories(
    tango-bulk-tango
    PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
           $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE ${PROJECT_SOURCE_DIR}/src)

# Tango is PUBLIC: tango-bulk/tango.h names Tango types, so anything compiling
# against this target needs the Tango headers anyway.
target_link_libraries(
    tango-bulk-tango
    PUBLIC tango-bulk::core tango-bulk::settings Tango::Tango
    PRIVATE Threads::Threads)

# cppTango's public headers do not survive this project's warning set -- they
# are not compiled with it upstream and it is not this project's business to
# demand that they should.  Marking them SYSTEM suppresses warnings from them
# without weakening anything applied to our own code.
get_target_property(_tango_includes Tango::Tango INTERFACE_INCLUDE_DIRECTORIES)
if(_tango_includes)
    target_include_directories(tango-bulk-tango SYSTEM PUBLIC ${_tango_includes})
endif()
