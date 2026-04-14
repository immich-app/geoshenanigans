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
add_executable(geocoder-canonicalize tools/geocoder_canonicalize.cpp)
target_link_libraries(geocoder-canonicalize PRIVATE Threads::Threads)
target_include_directories(geocoder-canonicalize PRIVATE tools)

add_executable(geocoder-diff tools/geocoder_diff.cpp)
target_link_libraries(geocoder-diff PRIVATE Threads::Threads)
target_include_directories(geocoder-diff PRIVATE tools)

add_executable(geocoder-patch tools/geocoder_patch.cpp)
target_link_libraries(geocoder-patch PRIVATE Threads::Threads)
target_include_directories(geocoder-patch PRIVATE tools)

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
