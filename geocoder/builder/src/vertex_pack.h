// Polygon vertex packing shared by the admin, POI and addr vertex files: a
// POLY_HEADER_BYTES header (encoding tag, pad, bbox_min lat/lng as f32), then
// every vertex as unsigned deltas from bbox_min on the finest grid the
// polygon's span fits. Layout in types.h (VertexEncoding).
#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <utility>
#include <vector>

#include "types.h"

inline double vertex_lat(const NodeCoord& v) { return v.lat; }
inline double vertex_lng(const NodeCoord& v) { return v.lng; }
inline double vertex_lat(const std::pair<double, double>& v) { return v.first; }
inline double vertex_lng(const std::pair<double, double>& v) { return v.second; }

// How one polygon packs: its encoding, grid, bbox minimum and byte size.
struct PolygonPacking {
    VertexEncoding enc;
    double scale;
    double min_lat, min_lng;
    size_t bytes;
};

// Picks the smallest encoding that fits n >= 1 vertices. u16 max = 65535;
// span thresholds use 65535 * scale to leave no headroom.
template <class Vertex>
PolygonPacking plan_polygon(const Vertex* verts, size_t n) {
    double min_lat = vertex_lat(verts[0]), max_lat = min_lat;
    double min_lng = vertex_lng(verts[0]), max_lng = min_lng;
    for (size_t k = 1; k < n; k++) {
        double lat = vertex_lat(verts[k]), lng = vertex_lng(verts[k]);
        if (lat < min_lat) min_lat = lat;
        if (lat > max_lat) max_lat = lat;
        if (lng < min_lng) min_lng = lng;
        if (lng > max_lng) max_lng = lng;
    }
    double max_span = std::max(max_lat - min_lat, max_lng - min_lng);

    PolygonPacking p{VertexEncoding::U32_1CM, 1e-7, min_lat, min_lng, 0};
    if (max_span < 65535.0 * 1e-6) {
        // 11 cm grid, 7.3 km bbox — POI building footprints
        p.enc = VertexEncoding::U16_011M; p.scale = 1e-6;
    } else if (max_span < 65535.0 * 1e-5) {
        // 1.1 m grid, 73 km bbox — most cities, suburbs, neighbourhoods
        p.enc = VertexEncoding::U16_1M; p.scale = 1e-5;
    } else if (max_span < 65535.0 * 1e-4) {
        // 11 m grid, 730 km bbox — counties, regions, small countries
        p.enc = VertexEncoding::U16_11M; p.scale = 1e-4;
    }
    // else u32 @ 1 cm — fits any polygon (max span ~214 deg)
    p.bytes = POLY_HEADER_BYTES + n * VERTEX_STRIDE[static_cast<uint8_t>(p.enc)];
    return p;
}

// Writes the p.bytes of one polygon at dst.
template <class Vertex>
void pack_polygon(uint8_t* dst, const PolygonPacking& p, const Vertex* verts, size_t n) {
    dst[0] = static_cast<uint8_t>(p.enc);
    dst[1] = 0;  // padding
    float bml = static_cast<float>(p.min_lat);
    float bmg = static_cast<float>(p.min_lng);
    std::memcpy(dst + 2, &bml, 4);
    std::memcpy(dst + 6, &bmg, 4);
    dst += POLY_HEADER_BYTES;
    for (size_t k = 0; k < n; k++) {
        double dlat = (vertex_lat(verts[k]) - p.min_lat) / p.scale;
        double dlng = (vertex_lng(verts[k]) - p.min_lng) / p.scale;
        if (p.enc == VertexEncoding::U32_1CM) {
            uint32_t qlat = static_cast<uint32_t>(std::lround(dlat));
            uint32_t qlng = static_cast<uint32_t>(std::lround(dlng));
            std::memcpy(dst, &qlat, 4);
            std::memcpy(dst + 4, &qlng, 4);
            dst += 8;
        } else {
            uint16_t qlat = static_cast<uint16_t>(std::lround(dlat));
            uint16_t qlng = static_cast<uint16_t>(std::lround(dlng));
            std::memcpy(dst, &qlat, 2);
            std::memcpy(dst + 2, &qlng, 2);
            dst += 4;
        }
    }
}

// Appends one polygon to `out` and returns the byte offset of its header.
template <class Vertex>
uint32_t append_polygon(std::vector<uint8_t>& out, const Vertex* verts, size_t n) {
    PolygonPacking p = plan_polygon(verts, n);
    size_t offset = out.size();
    out.resize(offset + p.bytes);
    pack_polygon(out.data() + offset, p, verts, n);
    return static_cast<uint32_t>(offset);
}
