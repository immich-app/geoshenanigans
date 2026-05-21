#include "continent_boundaries.h"
#include "embedded_polys.h"

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
