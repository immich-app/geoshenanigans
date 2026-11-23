// Point-in-polygon on OSM's own e7 grid, in integers: the one rounding of a
// point to that grid (to_e7) and the one ray cast (exact_edge_crosses) that
// every builder link and the builder's admin cell map use, with the box and
// the edge index in front of it. Neighbouring polygons share their edges
// vertex for vertex, and a point on a shared edge crosses nothing there, so
// it is inside exactly one of them. testdata/pip_edges.tsv and
// testdata/point_e7.tsv hold the cases a reader of the same rings must agree
// on.
#pragma once

#include <algorithm>
#include <climits>
#include <cmath>
#include <cstdint>
#include <vector>

#include "packed_location.h"
#include "polygon_rings.h"
#include "ring_edge_buckets.h"

// A coordinate in degrees on the e7 grid, half away from zero: OSM's own e7
// value for a coordinate read from it (lat_e7 / 1e7). A record's f32
// coordinate is widened to double first. |deg| <= 180.
inline int32_t to_e7(double deg) { return static_cast<int32_t>(std::llround(deg * 1e7)); }

inline PackedLocation point_e7(double lat, double lng) { return {to_e7(lat), to_e7(lng)}; }

// Whether the edge a-b, a straight line in (lat, lng), crosses the ray from p
// towards +lat: it straddles p's longitude (one end east of it, the other at
// or west of it) and passes strictly north of p there. On the edge (the
// cross product lhs - rhs is 0) it crosses nothing. Each product is at most
// 1.8e9 * 3.6e9 = 6.5e18 < 2^63, so both are exact; their difference
// needn't fit, so they are only compared. Either direction of the edge gives
// the same answer.
inline bool exact_edge_crosses(PackedLocation a, PackedLocation b, PackedLocation p) {
    if ((a.lon_e7 > p.lon_e7) == (b.lon_e7 > p.lon_e7)) return false;
    const int64_t lhs = (int64_t(p.lat_e7) - a.lat_e7) * (int64_t(b.lon_e7) - a.lon_e7);
    const int64_t rhs = (int64_t(b.lat_e7) - a.lat_e7) * (int64_t(p.lon_e7) - a.lon_e7);
    return lhs != rhs && ((lhs < rhs) != (b.lon_e7 < a.lon_e7));
}

// Even-odd containment of p in the ring v[0, n), its last vertex joined back
// to its first (a closed ring's closing edge has no length and crosses
// nothing).
inline bool exact_ring_contains(const PackedLocation* v, uint32_t n, PackedLocation p) {
    bool inside = false;
    for (uint32_t a = 0, b = n - 1; a < n; b = a++)
        if (exact_edge_crosses(v[b], v[a], p)) inside = !inside;
    return inside;
}

// Whether p lies on the segment a-b: on its line (exact_edge_crosses' cross
// product is 0, as exact) and within its box.
inline bool exact_on_segment(PackedLocation a, PackedLocation b, PackedLocation p) {
    const int64_t lhs = (int64_t(p.lat_e7) - a.lat_e7) * (int64_t(b.lon_e7) - a.lon_e7);
    const int64_t rhs = (int64_t(b.lat_e7) - a.lat_e7) * (int64_t(p.lon_e7) - a.lon_e7);
    return lhs == rhs && std::min(a.lat_e7, b.lat_e7) <= p.lat_e7 && p.lat_e7 <= std::max(a.lat_e7, b.lat_e7) &&
           std::min(a.lon_e7, b.lon_e7) <= p.lon_e7 && p.lon_e7 <= std::max(a.lon_e7, b.lon_e7);
}

// Whether p lies on an edge of the ring v[0, n), its last vertex joined back
// to its first.
inline bool exact_ring_touches(const PackedLocation* v, uint32_t n, PackedLocation p) {
    for (uint32_t a = 0, b = n - 1; a < n; b = a++)
        if (exact_on_segment(v[b], v[a], p)) return true;
    return false;
}

// Whether the ring v[0, n) covers p, as PostGIS's ST_Covers: p is inside it
// or on its edges, which exact_ring_contains leaves to one side or the other.
inline bool exact_ring_covers(const PackedLocation* v, uint32_t n, PackedLocation p) {
    return exact_ring_touches(v, n, p) || exact_ring_contains(v, n, p);
}

// Whether the polygons (each an outer ring and its holes; a ring of fewer
// than 3 vertices is none) cover p, as ST_Covers of their multipolygon: p is
// on an edge of one of their rings, a hole's too, or inside an outer ring and
// none of its holes.
inline bool exact_polygons_cover(const std::vector<NestedPolygon<PackedLocation>>& polygons, PackedLocation p) {
    auto touches = [&](const std::vector<PackedLocation>& ring) {
        return ring.size() >= 3 && exact_ring_touches(ring.data(), static_cast<uint32_t>(ring.size()), p);
    };
    auto contains = [&](const std::vector<PackedLocation>& ring) {
        return ring.size() >= 3 && exact_ring_contains(ring.data(), static_cast<uint32_t>(ring.size()), p);
    };
    for (const auto& polygon : polygons) {
        if (touches(polygon.outer) || std::any_of(polygon.holes.begin(), polygon.holes.end(), touches)) return true;
        if (contains(polygon.outer) && std::none_of(polygon.holes.begin(), polygon.holes.end(), contains)) return true;
    }
    return false;
}

// A ring's bounds, for a reject in front of exact_ring_contains.
struct ExactRingBox {
    int32_t lat_lo, lat_hi, lng_lo, lng_hi;
};

inline ExactRingBox exact_ring_box(const PackedLocation* v, uint32_t n) {
    ExactRingBox box{INT32_MAX, INT32_MIN, INT32_MAX, INT32_MIN};
    for (uint32_t k = 0; k < n; k++) {
        box.lat_lo = std::min(box.lat_lo, v[k].lat_e7);
        box.lat_hi = std::max(box.lat_hi, v[k].lat_e7);
        box.lng_lo = std::min(box.lng_lo, v[k].lon_e7);
        box.lng_hi = std::max(box.lng_hi, v[k].lon_e7);
    }
    return box;
}

// True only where exact_ring_contains is false, with no slack: no edge
// straddles a longitude below every vertex's or at or above every vertex's;
// at or above the top no edge passes north of p; below the bottom every
// straddling edge does, and they come in pairs going round the ring.
inline bool exact_ring_box_excludes(const ExactRingBox& box, PackedLocation p) {
    return p.lon_e7 < box.lng_lo || p.lon_e7 >= box.lng_hi || p.lat_e7 < box.lat_lo || p.lat_e7 >= box.lat_hi;
}

// exact_ring_contains for many points against one ring, its edges bucketed
// by longitude (RingEdgeBuckets): the point's bucket holds every edge that
// can straddle it.
class ExactRingEdgeIndex {
public:
    // verts must outlive the index.
    ExactRingEdgeIndex(const PackedLocation* verts, uint32_t cnt)
        : verts_(verts),
          buckets_(cnt, std::min<uint32_t>(cnt / 16, 1u << 20), [verts](uint32_t k) { return verts[k].lon_e7; }) {}

    bool contains(PackedLocation p) const {
        return buckets_.odd_crossings(p.lon_e7, [&](uint32_t a, uint32_t b) {
            return exact_edge_crosses(verts_[b], verts_[a], p);
        });
    }

private:
    const PackedLocation* verts_;
    RingEdgeBuckets buckets_;
};

// Twice the signed shoelace area of the ring v[0, n), in e7 units squared:
// up to 2 * 3.6e9 * 1.8e9 for a ring round the globe, past 64 bits once
// summed, so in 128.
inline __int128 exact_ring_area2(const PackedLocation* v, uint32_t n) {
    __int128 sum = 0;
    for (uint32_t a = 0, b = n - 1; a < n; b = a++)
        sum += static_cast<__int128>(v[b].lat_e7) * v[a].lon_e7 - static_cast<__int128>(v[a].lat_e7) * v[b].lon_e7;
    return sum;
}

// The area of a polygon's rings (the outer, then its holes) in square
// degrees, the holes taken off (none below 0), summed exactly and rounded
// once: whatever the rings' rotation or direction, the same float.
inline float exact_polygon_area(const PackedLocation* v, const RingSpan* rings, uint32_t ring_count) {
    auto magnitude = [&](const RingSpan& r) {
        const __int128 a2 = exact_ring_area2(v + r.begin, r.count);
        return a2 < 0 ? -a2 : a2;
    };
    __int128 area2 = ring_count ? magnitude(rings[0]) : 0;
    for (uint32_t r = 1; r < ring_count; r++) area2 -= magnitude(rings[r]);
    if (area2 < 0) area2 = 0;
    return static_cast<float>(static_cast<double>(area2) * 0.5e-14);
}

// The centroid (lat, lng) of a polygon's rings (the outer, then its holes) in
// degrees: the outer ring's area-weighted centre less its holes', as PostGIS's
// ST_Centroid of a lon/lat polygon. A polygon of no area gives its outer
// ring's first vertex.
inline LatLng exact_polygon_centroid(const PackedLocation* v, const RingSpan* rings, uint32_t ring_count) {
    if (ring_count == 0 || rings[0].count == 0) return {0, 0};
    const PackedLocation origin = v[rings[0].begin];
    double area = 0, lat = 0, lng = 0;
    for (uint32_t r = 0; r < ring_count; r++) {
        const PackedLocation* ring = v + rings[r].begin;
        const uint32_t n = rings[r].count;
        double a2 = 0, sum_lat = 0, sum_lng = 0;
        for (uint32_t a = 0, b = n - 1; a < n; b = a++) {
            const double y0 = (ring[b].lat_e7 - origin.lat_e7) * 1e-7, x0 = (ring[b].lon_e7 - origin.lon_e7) * 1e-7;
            const double y1 = (ring[a].lat_e7 - origin.lat_e7) * 1e-7, x1 = (ring[a].lon_e7 - origin.lon_e7) * 1e-7;
            const double cross = x0 * y1 - x1 * y0;
            a2 += cross;
            sum_lat += (y0 + y1) * cross;
            sum_lng += (x0 + x1) * cross;
        }
        if (a2 == 0) continue;
        // The ring's centre is its sums over 3 times a2; it weighs |a2|, a hole against.
        const double weight = (r == 0 ? 1.0 : -1.0) * std::fabs(a2);
        area += weight;
        lat += weight * sum_lat / (3 * a2);
        lng += weight * sum_lng / (3 * a2);
    }
    if (!(area > 0)) return {origin.lat_e7 * 1e-7, origin.lon_e7 * 1e-7};
    return {origin.lat_e7 * 1e-7 + lat / area, origin.lon_e7 * 1e-7 + lng / area};
}
