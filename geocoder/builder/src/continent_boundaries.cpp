#include "continent_boundaries.h"

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>

// Parse a Geofabrik .poly file into a ContinentPolygon.
// Format: name\nring_name\n  lng lat\n  ...\nEND\nEND\n
// Note: longitude comes first in .poly files.
static ContinentPolygon parse_poly_file(const std::string& path, const std::string& name) {
    ContinentPolygon poly;
    poly.name = name;

    std::ifstream f(path);
    if (!f) {
        std::cerr << "Warning: cannot open poly file: " << path << std::endl;
        return poly;
    }

    std::string line;
    // Skip header lines until we get to coordinates
    bool in_ring = false;
    while (std::getline(f, line)) {
        // Trim whitespace
        size_t start = line.find_first_not_of(" \t\r\n");
        if (start == std::string::npos) continue;
        std::string trimmed = line.substr(start);

        if (trimmed == "END") {
            in_ring = false;
            continue;
        }

        // Try to parse as "lng lat"
        double lng, lat;
        std::istringstream iss(trimmed);
        if (iss >> lng >> lat) {
            poly.vertices.push_back({lat, lng}); // store as (lat, lng)
            in_ring = true;
        }
        // Otherwise it's a name/ring_name line — skip
    }

    // Close polygon if not already closed
    if (!poly.vertices.empty() && poly.vertices.front() != poly.vertices.back()) {
        poly.vertices.push_back(poly.vertices.front());
    }

    return poly;
}

std::vector<ContinentPolygon> get_continent_polygons() {
    // Try multiple search paths for poly files
    std::vector<std::string> search_paths = {
        "data/",                    // relative to working dir
        "../data/",                 // from build dir
        "/root/traccar-geocoder/builder/data/",  // absolute
    };

    // Continent names matching kContinents[] order in continent_filter.cpp
    struct PolyDef {
        const char* name;       // our continent name
        const char* filename;   // geofabrik filename
    };
    PolyDef defs[] = {
        {"africa", "africa.poly"},
        {"asia", "asia.poly"},
        {"europe", "europe.poly"},
        {"north-america", "north-america.poly"},
        {"south-america", "south-america.poly"},
        {"oceania", "australia-oceania.poly"},
        {"central-america", "central-america.poly"},
        {"antarctica", "antarctica.poly"},
    };

    std::vector<ContinentPolygon> polys;

    for (const auto& def : defs) {
        ContinentPolygon poly;
        bool found = false;
        for (const auto& prefix : search_paths) {
            poly = parse_poly_file(prefix + def.filename, def.name);
            if (!poly.vertices.empty()) {
                found = true;
                break;
            }
        }
        if (!found) {
            std::cerr << "Warning: no poly file found for " << def.name
                      << " — continent will have no data" << std::endl;
        } else {
            std::cerr << "  Loaded " << def.name << " boundary: "
                      << poly.vertices.size() << " vertices" << std::endl;
        }
        polys.push_back(std::move(poly));
    }

    return polys;
}
