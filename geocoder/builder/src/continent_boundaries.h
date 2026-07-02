#pragma once

// Simplified continent boundary polygons for filtering.
// Each continent is defined as a set of (lat, lng) vertices forming a
// closed polygon that approximates its geographic boundary.
// These are rough boundaries — accurate enough for geocoding data
// separation but not for cartographic purposes.

#include <string>
#include <vector>
#include <utility>

#include "types.h"

struct ContinentPolygon {
    std::string name;
    std::vector<std::pair<double,double>> vertices; // (lat, lng) pairs
};

// Get the simplified boundary polygons for all continents.
std::vector<ContinentPolygon> get_continent_polygons();

// Point-in-polygon test (ray casting algorithm)
inline bool point_in_polygon(double lat, double lng,
                              const std::vector<std::pair<double,double>>& poly) {
    bool inside = false;
    size_t n = poly.size();
    for (size_t i = 0, j = n - 1; i < n; j = i++) {
        double yi = poly[i].first, xi = poly[i].second;
        double yj = poly[j].first, xj = poly[j].second;
        if (((yi > lat) != (yj > lat)) &&
            (lng < (xj - xi) * (lat - yi) / (yj - yi) + xi))
            inside = !inside;
    }
    return inside;
}

// NodeCoord-based overload — used by the build_index place-node
// containment pass against packed admin polygon vertices.
inline bool point_in_polygon_nc(float lat, float lng,
                                const NodeCoord* verts, size_t n) {
    bool inside = false;
    for (size_t i = 0, j = n - 1; i < n; j = i++) {
        float yi = verts[i].lat, xi = verts[i].lng;
        float yj = verts[j].lat, xj = verts[j].lng;
        if (((yi > lat) != (yj > lat)) &&
            (lng < (xj - xi) * (lat - yi) / (yj - yi) + xi))
            inside = !inside;
    }
    return inside;
}
