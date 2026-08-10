# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
#
# SPDX-License-Identifier: LGPL-3.0-or-later

# The M4 exit criterion these targets exist to satisfy: "an example device and
# client use only installed cppTango public headers plus extension headers; the
# cppTango source tree is absent from their build environment."
#
# They therefore link the public targets only -- never a src/ include path -- so
# an example that reached inside the library would fail to compile rather than
# quietly work in this tree and nowhere else.
#
# tango-bulk::tango carries the adapter and, as a LINK_ONLY dependency, the
# transport behind it.  Neither brings a ucp/* header with it, which is the
# property scripts/check_layering.py exists to keep true.

if(NOT TANGO_BULK_BUILD_TANGO_ADAPTER)
    message(
        FATAL_ERROR
            "TANGO_BULK_BUILD_EXAMPLES requires TANGO_BULK_BUILD_TANGO_ADAPTER: "
            "every example is either a device server or a DeviceProxy client.")
endif()

add_executable(tango-bulk-example-device example_device/example_device.cpp)
target_link_libraries(tango-bulk-example-device PRIVATE tango-bulk::tango)

# The replay server is deliberately an example-only HDF5 dependency.  The
# tango-bulk libraries do not know about file formats.
find_package(HDF5 REQUIRED COMPONENTS CXX)
add_executable(tango-bulk-example-hdf5-device
               example_hdf5_device/example_hdf5_device.cpp)
target_link_libraries(tango-bulk-example-hdf5-device
                      PRIVATE tango-bulk::tango HDF5::HDF5)

add_executable(tango-bulk-example-preview example_preview/example_preview.cpp)
target_link_libraries(tango-bulk-example-preview PRIVATE tango-bulk::tango)

add_executable(tango-bulk-example-client example_client/example_client.cpp)
target_link_libraries(tango-bulk-example-client PRIVATE tango-bulk::tango)

# GPUDirect is an optional example dependency, not a library dependency. The
# subscriber accepts caller-owned accelerator memory without including CUDA.
find_package(CUDAToolkit QUIET)
if(CUDAToolkit_FOUND)
    add_executable(tango-bulk-example-cuda-client
                   example_cuda_client/example_cuda_client.cpp)
    target_include_directories(tango-bulk-example-cuda-client
                               SYSTEM PRIVATE ${CUDAToolkit_INCLUDE_DIRS})
    if(CUDAToolkit_BIN_DIR STREQUAL "/usr/bin")
        # A Conda compiler uses a sysroot and CMake otherwise drops /usr/include
        # as an "implicit" host path, even though this system CUDA installation
        # lives there.
        target_compile_options(tango-bulk-example-cuda-client
                               PRIVATE -idirafter/usr/include)
    endif()
    target_link_libraries(tango-bulk-example-cuda-client
                          PRIVATE tango-bulk::tango CUDA::cudart)
endif()
