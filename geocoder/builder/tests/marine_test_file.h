// A marine areas file as tools/marine_import.py writes it, for the tests of
// its reader (tests/test_marine.cpp) and of build-index's --marine
// (tests/s2/test_water_dirs_build.cpp).
#pragma once

#include <string>

namespace gctest {

// The bytes marine_import.py's write_marine gives two areas: the ocean 1902
// of two polygons (a triangle; a square with a triangular hole), and the sea
// 2350 of one triangle (test_marine_import.py checks them on its side).
inline constexpr const char* kTwoAreasHex =
    "47434d4152494e4501000000020000006e070000000000000214004e6f7274682041746c616e746963204f6365616e020000000100000003"
    "00000000000000005d1eee0000000000e1f50500e1f505800f05fd0200000004000000806967ff806967ff00000000002d3101002d310100"
    "0000000000000000d3cefe03000000c0b4b3ffc0b4b3ff8096980000000000c0b4b3ff809698002e090000000000000109004e6f72746820"
    "5365610100000001000000030000008028971f80c3c901000000008096980080969800c0b4b3ff";

inline std::string bytes_of_hex(const char* hex) {
    std::string out;
    for (const char* p = hex; p[0] && p[1]; p += 2) out.push_back(static_cast<char>(std::stoi(std::string(p, 2), nullptr, 16)));
    return out;
}

}  // namespace gctest
