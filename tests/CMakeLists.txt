# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
#
# SPDX-License-Identifier: LGPL-3.0-or-later

find_package(Catch2 3.1.1 REQUIRED)

include(Catch)

# One executable per layer, matching the target layering.  This is not tidiness:
# tests/unit must be runnable on a machine with no UCX device and no Tango
# database, and keeping them in separate binaries is what makes that true rather
# than merely intended.

# --- tests/unit: core only, no UCX, no Tango -------------------------------

add_executable(
    tango-bulk-unit-tests
    unit/test_cpu_topology.cpp
    unit/test_credit_window.cpp
    unit/test_enums.cpp
    unit/test_errors.cpp
    unit/test_frame_view.cpp
    unit/test_geometry.cpp
    unit/test_session_supervisor.cpp
    unit/test_identifiers.cpp
    unit/test_protocol_golden.cpp
    unit/test_protocol_layout.cpp
    unit/test_protocol_malformed.cpp
    unit/test_protocol_roundtrip.cpp)

target_link_libraries(tango-bulk-unit-tests PRIVATE tango-bulk::core
                                                    Catch2::Catch2WithMain)

target_include_directories(tango-bulk-unit-tests PRIVATE ${PROJECT_SOURCE_DIR}/src)

tango_bulk_apply_warnings(tango-bulk-unit-tests)

# Catch2 enumerates test cases by RUNNING the binary at build time. Under
# ThreadSanitizer that aborts before main -- TSan's shadow mapping collides with
# a real one unless ASLR is off -- so discovery fails and takes the build with
# it, for binaries that are otherwise perfectly good. `pixi run test-tsan` runs
# them directly under setarch -R instead, so nothing is lost by skipping it.
if(TANGO_BULK_SANITIZERS MATCHES "thread")
    set(TANGO_BULK_SKIP_TEST_DISCOVERY ON)
    message(STATUS "  Test discovery  skipped (thread sanitizer; use pixi run test-tsan)")
endif()

if(NOT TANGO_BULK_SKIP_TEST_DISCOVERY)
    catch_discover_tests(tango-bulk-unit-tests)
endif()

# --- tests/ucx: loopback UCX, no Tango process -----------------------------

add_executable(tango-bulk-ucx-tests ucx/test_registered_ring.cpp
                                    ucx/test_session_lifecycle.cpp
                                    ucx/test_ucx_support.cpp
                                    ucx/test_vertical_slice.cpp)

target_link_libraries(tango-bulk-ucx-tests PRIVATE tango-bulk::ucx
                                                   Catch2::Catch2WithMain)

target_include_directories(tango-bulk-ucx-tests PRIVATE ${PROJECT_SOURCE_DIR}/src)

tango_bulk_apply_warnings(tango-bulk-ucx-tests)

if(NOT TANGO_BULK_SKIP_TEST_DISCOVERY)
    catch_discover_tests(tango-bulk-ucx-tests)
endif()

# --- tests/tango: stock-cppTango device fixture ----------------------------

if(TANGO_BULK_BUILD_TANGO_ADAPTER)
    # A real device server, in its own process, running -nodb.  The M4 exit
    # criterion is that an example device and client use only installed cppTango
    # headers plus extension headers -- which is a property of a process, not of
    # a translation unit, so the fixture is a process.
    add_executable(tango-bulk-fixture-device tango/fixture_device.cpp)

    # tango-bulk::tango only, exactly as a real device server would link it: the
    # transport comes along as a LINK_ONLY dependency and brings no UCX include
    # directory with it.  Naming tango-bulk::ucx here as well would work and
    # would quietly stop this fixture from testing that.
    target_link_libraries(tango-bulk-fixture-device PRIVATE tango-bulk::tango)

    add_executable(tango-bulk-tango-tests tango/test_adapter.cpp
                                          tango/test_command_names.cpp)

    target_link_libraries(tango-bulk-tango-tests PRIVATE tango-bulk::tango
                                                         Catch2::Catch2WithMain)

    target_include_directories(tango-bulk-tango-tests
                               PRIVATE ${PROJECT_SOURCE_DIR}/src/tango)

    # The test spawns the fixture, so it needs to know where it landed.  A
    # generator expression rather than a guessed path: multi-config generators
    # put it somewhere this file cannot predict.
    target_compile_definitions(
        tango-bulk-tango-tests
        PRIVATE
            TANGO_BULK_FIXTURE_DEVICE="$<TARGET_FILE:tango-bulk-fixture-device>")

    add_dependencies(tango-bulk-tango-tests tango-bulk-fixture-device)

    tango_bulk_apply_warnings(tango-bulk-fixture-device tango-bulk-tango-tests)

    # Every case in test_adapter.cpp starts a CORBA server and waits on real
    # leases, so the default 1 500 s ctest timeout is generous and the serial
    # property is what keeps two cases from fighting over the same fixture.
    catch_discover_tests(tango-bulk-tango-tests PROPERTIES RUN_SERIAL TRUE)
endif()

# --- Layering ---------------------------------------------------------------
#
# The spec requires a CI step that greps each target's translation units for
# forbidden includes and fails the build on violation.  Running it as a test as
# well means a developer trips it locally, before CI does.

find_package(Python3 COMPONENTS Interpreter)

if(Python3_Interpreter_FOUND)
    add_test(NAME layering
             COMMAND ${Python3_EXECUTABLE}
                     ${PROJECT_SOURCE_DIR}/scripts/check_layering.py --verbose
                     --root ${PROJECT_SOURCE_DIR})

    # Negative control.  Without it, "layering" passing tells us only that the
    # script ran -- not that it can still detect anything.
    add_test(NAME layering-detects-violations
             COMMAND ${Python3_EXECUTABLE}
                     ${PROJECT_SOURCE_DIR}/scripts/check_layering.py
                     --root ${PROJECT_SOURCE_DIR}/tests/layering_fixture)

    set_tests_properties(
        layering-detects-violations
        PROPERTIES
            WILL_FAIL TRUE
            # One per fixture file, and no more: a commented-out include in the
            # core fixture must not be counted.
            FAIL_REGULAR_EXPRESSION "Layering OK")
else()
    message(WARNING "Python 3 not found: the layering test will not run locally. "
                    "CI still enforces it via scripts/check_layering.py.")
endif()

# --- The installed-Tango guard ----------------------------------------------
#
# Three cases through `cmake -P`, checked on the message the guard emits rather
# than on the exit code, so a case cannot pass by failing for another reason.

set(_guard_script ${CMAKE_CURRENT_SOURCE_DIR}/cmake/installed_tango_guard.cmake)
set(_guard_scratch ${CMAKE_CURRENT_BINARY_DIR}/guard-scratch)

foreach(case_name clean build_tree source_tree)
    add_test(
        NAME installed-tango-guard-${case_name}
        COMMAND
            ${CMAKE_COMMAND} -DCASE=${case_name} -DSCRATCH=${_guard_scratch}
            -DMODULE_DIR=${PROJECT_SOURCE_DIR}/cmake -P ${_guard_script})
endforeach()

set_tests_properties(
    installed-tango-guard-clean
    PROPERTIES PASS_REGULAR_EXPRESSION "GUARD-TEST: clean prefix accepted")

# Each rejection must name the fixture directory it rejected, which is what
# distinguishes "the guard fired" from "cmake failed".
set_tests_properties(
    installed-tango-guard-build_tree
    PROPERTIES PASS_REGULAR_EXPRESSION
               "CMake build tree '${_guard_scratch}/build_tree/bt'")

set_tests_properties(
    installed-tango-guard-source_tree
    PROPERTIES PASS_REGULAR_EXPRESSION
               "cppTango source tree '${_guard_scratch}/source_tree/st'")
