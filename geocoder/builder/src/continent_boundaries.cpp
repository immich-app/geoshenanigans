#include "continent_boundaries.h"
#include "continent_filter.h"
#include "embedded_polys.h"

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string_view>

// Parse a Geofabrik .poly file from an in-memory buffer.
// Format: name\nring_name\n  lng lat\n  ...\nEND\nEND\n
// Note: longitude comes first in .poly files.
static ContinentPolygon parse_poly_data(std::string_view data, const std::string& name) {
    ContinentPolygon poly;
    poly.name = name;

    std::istringstream f{std::string(data)};
    std::string line;
    while (std::getline(f, line)) {
        size_t start = line.find_first_not_of(" \t\r\n");
        if (start == std::string::npos) continue;
        std::string trimmed = line.substr(start);

        if (trimmed == "END") continue;

        double lng, lat;
        std::istringstream iss(trimmed);
        if (iss >> lng >> lat) {
            poly.vertices.push_back({lat, lng});
        }
    }

    if (!poly.vertices.empty() && poly.vertices.front() != poly.vertices.back()) {
        poly.vertices.push_back(poly.vertices.front());
    }

    return poly;
}

std::vector<ContinentPolygon> get_continent_polygons() {
    // Our internal continent name → filename stem embedded by CMake.
    // Order must match kContinents[] in continent_filter.cpp.
    struct Def { const char* our_name; const char* embedded_name; };
    static const Def defs[] = {
        {"africa",          "africa"},
        {"asia",            "asia"},
        {"europe",          "europe"},
        {"north-america",   "north-america"},
        {"south-america",   "south-america"},
        {"oceania",         "australia-oceania"},
        {"central-america", "central-america"},
        {"antarctica",      "antarctica"},
    };

    // defs[] and kContinents[] (continent_filter.cpp) are two parallel
    // hand-ordered name tables that MUST stay aligned element-for-element —
    // downstream code pairs a continent's bbox/bitmask (from kContinents[])
    // with its polygon (from defs[]) by index. This guard fires only if the
    // tables have drifted (a programming error); on the success path it is a
    // no-op with no effect on emitted output.
    constexpr size_t kDefsCount = sizeof(defs) / sizeof(defs[0]);
    if (kDefsCount != kContinentCount) {
        std::cerr << "FATAL: continent table size mismatch: defs[]=" << kDefsCount
                  << " kContinents[]=" << kContinentCount
                  << " (continent_boundaries.cpp vs continent_filter.cpp)" << std::endl;
        std::exit(1);
    }
    for (size_t i = 0; i < kDefsCount; i++) {
        if (std::strcmp(defs[i].our_name, kContinents[i].name) != 0) {
            std::cerr << "FATAL: continent table order mismatch at index " << i
                      << ": defs[]=\"" << defs[i].our_name
                      << "\" kContinents[]=\"" << kContinents[i].name
                      << "\" (continent_boundaries.cpp vs continent_filter.cpp)" << std::endl;
            std::exit(1);
        }
    }

    const embedded_polys::Entry* entries = embedded_polys::entries();
    std::vector<ContinentPolygon> polys;

    for (const auto& def : defs) {
        const embedded_polys::Entry* hit = nullptr;
        for (size_t i = 0; i < embedded_polys::kCount; i++) {
            if (std::strcmp(entries[i].name, def.embedded_name) == 0) {
                hit = &entries[i];
                break;
            }
        }
        if (!hit) {
            std::cerr << "Warning: embedded poly missing for " << def.our_name
                      << " (" << def.embedded_name << ")" << std::endl;
            polys.push_back({def.our_name, {}});
            continue;
        }
        auto poly = parse_poly_data(
            std::string_view(reinterpret_cast<const char*>(hit->data), hit->size),
            def.our_name);
        std::cerr << "  Loaded " << def.our_name << " boundary: "
                  << poly.vertices.size() << " vertices" << std::endl;
        polys.push_back(std::move(poly));
    }

    return polys;
}
