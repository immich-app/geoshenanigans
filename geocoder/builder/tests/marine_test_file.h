// A marine areas file as tools/marine_import.py writes it, for the tests of
// its reader (tests/test_marine.cpp) and of build-index's --marine
// (tests/s2/test_water_dirs_build.cpp).
#pragma once

#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>

namespace gctest {

// The file marine_import.py's write_marine gives two areas: the ocean 1902
// of two polygons (a triangle; a square with a triangular hole), and the sea
// 2350 of one triangle. test_marine_import.py writes them and checks it gets
// these bytes.
inline std::string two_areas_path() { return std::string(GEOCODER_TESTDATA_DIR) + "/marine_two_areas.bin"; }

inline std::string two_areas_bytes() {
    std::ifstream f(two_areas_path(), std::ios::binary);
    if (!f) throw std::runtime_error("can't open " + two_areas_path());
    return std::string(std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>());
}

}  // namespace gctest
