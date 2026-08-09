# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
#
# SPDX-License-Identifier: LGPL-3.0-or-later

# tango-bulk-core: libstdc++ only.  No ucp/*, no tango/*.
#
# Everything here is testable in milliseconds with no external resource, which
# is the point of drawing the line where it is drawn: the protocol, the credit
# arithmetic, and the geometry rules are the parts most worth testing
# exhaustively and the parts least able to afford a transport to test against.

add_library(
    tango-bulk-core
    config.cpp
    credit_window.cpp
    enums.cpp
    errors.cpp
    frame_view.cpp
    geometry.cpp
    lease_pool.cpp
    pinned_budget.cpp
    protocol.cpp
    session_id.cpp)
add_library(tango-bulk::core ALIAS tango-bulk-core)

target_include_directories(
    tango-bulk-core
    PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
           $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE ${PROJECT_SOURCE_DIR}/src)

target_link_libraries(tango-bulk-core PUBLIC tango-bulk::settings)

set_target_properties(tango-bulk-core PROPERTIES OUTPUT_NAME tango-bulk-core)
