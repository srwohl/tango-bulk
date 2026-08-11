# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
#
# SPDX-License-Identifier: LGPL-3.0-or-later

# The benchmark links tango-bulk::ucx rather than the umbrella target: it needs
# no Tango, and keeping it off that dependency is what lets it run on a machine
# with a NIC and no Tango database.
#
# It reaches into ${PROJECT_SOURCE_DIR}/src for detail::SubscriberEngine, as the
# UCX tests do, because BulkSubscriber needs a Tango::DeviceProxy and arrives
# with M4. That include path goes away when it does.

add_executable(tango-bulk-bench bulk_bench.cpp)

target_link_libraries(tango-bulk-bench PRIVATE tango-bulk::ucx)

target_include_directories(tango-bulk-bench PRIVATE ${PROJECT_SOURCE_DIR}/src)

tango_bulk_apply_warnings(tango-bulk-bench)

set_target_properties(tango-bulk-bench PROPERTIES RUNTIME_OUTPUT_NAME tango-bulk-bench)

# A loopback smoke run, so "the benchmark still builds and moves bytes" is a
# thing CI observes rather than a thing someone remembers to check. Small and
# quick on purpose: throughput numbers are not a test result, but a benchmark
# that has silently stopped working is a regression like any other.
if(BUILD_TESTING)
    add_test(NAME benchmark-smoke
             COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/smoke.sh
                     $<TARGET_FILE:tango-bulk-bench>)
    set_tests_properties(benchmark-smoke PROPERTIES TIMEOUT 120)
endif()
