#pragma once

// Simplified continent boundary polygons for filtering.
// Each continent is defined as a set of (lat, lng) vertices forming a
// closed polygon that approximates its geographic boundary.
// These are rough boundaries — accurate enough for geocoding data
// separation but not for cartographic purposes.

#include <cstdint>
#include <string>
#include <vector>
#include <utility>

#include "ring_contains.h"
#include "types.h"

struct ContinentPolygon {
    std::string name;
    std::vector<std::pair<double,double>> vertices; // (lat, lng) pairs
};

// Get the simplified boundary polygons for all continents.
std::vector<ContinentPolygon> get_continent_polygons();

// Whether the edge between (lat, lng) vertices vi and vj crosses the ray from
// (lat, lng) towards +lng.
inline bool polygon_edge_crosses(const std::pair<double,double>& vi, const std::pair<double,double>& vj,
                                 double lat, double lng) {
    double yi = vi.first, xi = vi.second;
    double yj = vj.first, xj = vj.second;
    return ((yi > lat) != (yj > lat)) && (lng < (xj - xi) * (lat - yi) / (yj - yi) + xi);
}

// Ray-casting point-in-polygon for many points against one polygon (its ray
// runs along longitude, so the edges are bucketed by latitude): the continent
// split tests every record and cell against its polygon.
class PolygonIndex {
public:
    // A bucket per vertex: continent polygons are a hundred-odd long edges.
    // poly must outlive the index.
    explicit PolygonIndex(const std::vector<std::pair<double,double>>& poly)
        : poly_(poly),
          buckets_(static_cast<uint32_t>(poly.size()), static_cast<uint32_t>(poly.size()),
                   [&poly](uint32_t k) { return poly[k].first; }) {}

    bool contains(double lat, double lng) const {
        return buckets_.odd_crossings(lat, [&](uint32_t i, uint32_t j) {
            return polygon_edge_crosses(poly_[i], poly_[j], lat, lng);
        });
    }

private:
    const std::vector<std::pair<double,double>>& poly_;
    RingEdgeBuckets buckets_;
};

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
