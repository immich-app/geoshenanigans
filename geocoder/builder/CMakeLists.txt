cmake_minimum_required(VERSION 3.16)
project(traccar-geocoder-build LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)


find_package(Threads REQUIRED)
find_package(ZLIB REQUIRED)
find_package(BZip2 REQUIRED)
find_package(EXPAT REQUIRED)
find_package(s2 QUIET)

# --- Generate embedded_polys.h from data/*.poly ---
#
# Continent boundary .poly files are embedded directly into the build-index
# binary so they can't be misplaced at runtime (see continent_boundaries.cpp).
# Each file becomes a byte array in the generated header, exposed via an
# array of (name, data_ptr, size) entries.
set(POLY_NAMES africa antarctica asia australia-oceania central-america europe north-america south-america)
set(EMBEDDED_POLY_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
set(EMBEDDED_POLY_HEADER "${EMBEDDED_POLY_DIR}/embedded_polys.h")
file(MAKE_DIRECTORY "${EMBEDDED_POLY_DIR}")

# Re-run configure if any .poly changes
foreach(_pname ${POLY_NAMES})
    set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/data/${_pname}.poly")
endforeach()

set(_hdr "// Auto-generated from data/*.poly — do not edit\n#pragma once\n\n#include <cstddef>\n\nnamespace embedded_polys {\n\n")
foreach(_pname ${POLY_NAMES})
    set(_fp "${CMAKE_CURRENT_SOURCE_DIR}/data/${_pname}.poly")
    file(READ "${_fp}" _hex HEX)
    string(REGEX REPLACE "(..)" "0x\\1," _bytes "${_hex}")
    string(REPLACE "-" "_" _var "${_pname}")
    string(APPEND _hdr "static constexpr unsigned char k_${_var}[] = { ${_bytes} 0 };\n")
endforeach()

string(APPEND _hdr "\nstruct Entry { const char* name; const unsigned char* data; size_t size; };\n\n")
string(APPEND _hdr "inline const Entry* entries() {\n    static const Entry arr[] = {\n")
foreach(_pname ${POLY_NAMES})
    string(REPLACE "-" "_" _var "${_pname}")
    string(APPEND _hdr "        {\"${_pname}\", k_${_var}, sizeof(k_${_var}) - 1},\n")
endforeach()
string(APPEND _hdr "    };\n    return arr;\n}\n\n")
list(LENGTH POLY_NAMES _count)
string(APPEND _hdr "inline constexpr size_t kCount = ${_count};\n\n}  // namespace embedded_polys\n")

file(WRITE "${EMBEDDED_POLY_HEADER}" "${_hdr}")

add_executable(build-index
    src/build_index.cpp
    src/pbf_reader.cpp
    src/s2_helpers.cpp
    src/ring_assembly.cpp
    src/cache.cpp
    src/continent_filter.cpp
    src/continent_boundaries.cpp
    src/cell_index.cpp
    src/interpolation.cpp
)

# Patch tools (no S2 dependency)

add_executable(geocoder-diff tools/geocoder_diff.cpp)
target_link_libraries(geocoder-diff PRIVATE Threads::Threads)
target_include_directories(geocoder-diff PRIVATE tools src)

add_executable(geocoder-patch tools/geocoder_patch.cpp)
target_link_libraries(geocoder-patch PRIVATE Threads::Threads)
target_include_directories(geocoder-patch PRIVATE tools src)
# pread offsets past 4 GiB on 32-bit glibc too (sequential_file_reader.h).
target_compile_definitions(geocoder-patch PRIVATE _FILE_OFFSET_BITS=64)

# osmium apply-changes for the nightly planet update, blob by blob.
add_executable(pbf-apply tools/pbf_apply.cpp src/pbf_reader.cpp)
target_link_libraries(pbf-apply PRIVATE ZLIB::ZLIB EXPAT::EXPAT Threads::Threads)
target_include_directories(pbf-apply PRIVATE tools src)
target_compile_definitions(pbf-apply PRIVATE _FILE_OFFSET_BITS=64)

target_include_directories(build-index PRIVATE "${EMBEDDED_POLY_DIR}")

if(EXISTS /opt/homebrew/include)
    target_include_directories(build-index PRIVATE /opt/homebrew/include)
endif()

if(s2_FOUND)
    target_link_libraries(build-index PRIVATE
        s2::s2
        ZLIB::ZLIB
        BZip2::BZip2
        EXPAT::EXPAT
        Threads::Threads
    )
else()
    target_include_directories(build-index PRIVATE /usr/include/s2)
    target_link_libraries(build-index PRIVATE
        s2
        ZLIB::ZLIB
        BZip2::BZip2
        EXPAT::EXPAT
        Threads::Threads
    )
endif()

# --- Unit tests (geocoder-tests) ---
#
# Unit tests for the builder's pure logic and the tools (no S2 needed — the
# helpers under test need at most zlib, expat and protozero).
# Every tests/*.cpp is auto-discovered; run with `ctest` or by executing the
# geocoder-tests binary directly. Not in the default critical build path: build
# explicitly with `make geocoder-tests`.
enable_testing()
file(GLOB GEOCODER_TEST_SRCS CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp")
add_executable(geocoder-tests
    ${GEOCODER_TEST_SRCS}
    src/ring_assembly.cpp
    src/interpolation.cpp
    src/pbf_reader.cpp
)
target_include_directories(geocoder-tests PRIVATE src tools tests)
target_compile_definitions(geocoder-tests PRIVATE
    GEOCODER_TEMPLATE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/configurations.template.json"
    _FILE_OFFSET_BITS=64)
target_link_libraries(geocoder-tests PRIVATE ZLIB::ZLIB EXPAT::EXPAT Threads::Threads)
add_test(NAME geocoder-tests COMMAND geocoder-tests)

# GEOCODER_SANITIZE=ON builds the unit tests with ASan + UBSan, undefined
# behaviour failing the run (CI's sanitizer job). Tests only: build-index
# links the prebuilt S2, whose absl containers change layout under ASan.
option(GEOCODER_SANITIZE "Build geocoder-tests with ASan and UBSan" OFF)
if(GEOCODER_SANITIZE)
    set(GEOCODER_SANITIZE_FLAGS -fsanitize=address,undefined -fno-sanitize-recover=undefined -fno-omit-frame-pointer)
    target_compile_options(geocoder-tests PRIVATE ${GEOCODER_SANITIZE_FLAGS})
    target_link_options(geocoder-tests PRIVATE ${GEOCODER_SANITIZE_FLAGS})
endif()
