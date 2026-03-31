cmake_minimum_required(VERSION 3.16)
project(traccar-geocoder-build LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)


find_package(Threads REQUIRED)
find_package(ZLIB REQUIRED)
find_package(BZip2 REQUIRED)
find_package(EXPAT REQUIRED)
find_package(s2 QUIET)

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
