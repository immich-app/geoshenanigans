#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <functional>
#include <unordered_set>
#include <vector>

// Coordinate key matching osmium's Location (int32_t nanodegrees).
// Two points at the same geographic location always produce the same key,
// regardless of their OSM node IDs (handles split/replaced nodes).
inline int64_t coord_key(double lat, double lng) {
    int32_t ilat = static_cast<int32_t>(lat * 1e7 + (lat >= 0 ? 0.5 : -0.5));
    int32_t ilng = static_cast<int32_t>(lng * 1e7 + (lng >= 0 ? 0.5 : -0.5));
    return (static_cast<int64_t>(ilat) << 32) | static_cast<uint32_t>(ilng);
}

// Approximate polygon area in square degrees (shoelace formula)
inline float polygon_area(const std::vector<std::pair<double,double>>& vertices) {
    double area = 0;
    size_t n = vertices.size();
    for (size_t i = 0; i < n; i++) {
        size_t j = (i + 1) % n;
        area += vertices[i].first * vertices[j].second;
        area -= vertices[j].first * vertices[i].second;
    }
    return static_cast<float>(std::fabs(area) / 2.0);
}

// Check if segment (a1,a2) crosses segment (b1,b2) using cross product orientation
inline bool segments_intersect(double ax1, double ay1, double ax2, double ay2,
                               double bx1, double by1, double bx2, double by2) {
    auto cross = [](double ox, double oy, double ax, double ay, double bx, double by) -> double {
        return (ax - ox) * (by - oy) - (ay - oy) * (bx - ox);
    };
    double d1 = cross(bx1, by1, bx2, by2, ax1, ay1);
    double d2 = cross(bx1, by1, bx2, by2, ax2, ay2);
    double d3 = cross(ax1, ay1, ax2, ay2, bx1, by1);
    double d4 = cross(ax1, ay1, ax2, ay2, bx2, by2);
    if (((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) &&
        ((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0))) {
        return true;
    }
    return false;
}

// Sweep-line inspired self-intersection check for rings.
// Brute force for small rings (<=32 segments), sweep-line pruning for large ones.
inline bool ring_has_self_intersection(const std::vector<std::pair<double,double>>& ring) {
    static constexpr size_t BRUTE_FORCE_THRESHOLD = 32;
    size_t n = ring.size();
    if (n < 4) return false;

    if (n <= BRUTE_FORCE_THRESHOLD) {
        for (size_t i = 0; i + 1 < n; i++) {
            for (size_t j = i + 2; j + 1 < n; j++) {
                if (j == i + 1 || (i == 0 && j == n - 2)) continue;
                if (segments_intersect(ring[i].first, ring[i].second,
                                       ring[i+1].first, ring[i+1].second,
                                       ring[j].first, ring[j].second,
                                       ring[j+1].first, ring[j+1].second))
                    return true;
            }
        }
        return false;
    }

    // For larger rings, sort segments by min-x and prune non-overlapping
    struct Seg { double min_x, max_x; size_t idx; };
    std::vector<Seg> segs(n - 1);
    for (size_t i = 0; i + 1 < n; i++) {
        double x1 = ring[i].second, x2 = ring[i+1].second; // use lng as x
        segs[i] = {std::min(x1,x2), std::max(x1,x2), i};
    }
    std::sort(segs.begin(), segs.end(), [](const Seg& a, const Seg& b) {
        return a.min_x < b.min_x;
    });

    for (size_t a = 0; a + 1 < segs.size(); a++) {
        for (size_t b = a + 1; b < segs.size(); b++) {
            if (segs[b].min_x > segs[a].max_x) break;
            size_t i = segs[a].idx, j = segs[b].idx;
            if (j == i + 1 || i == j + 1) continue;
            if ((i == 0 && j == n - 2) || (j == 0 && i == n - 2)) continue;
            if (segments_intersect(ring[i].first, ring[i].second,
                                   ring[i+1].first, ring[i+1].second,
                                   ring[j].first, ring[j].second,
                                   ring[j+1].first, ring[j+1].second))
                return true;
        }
    }
    return false;
}

// Check if a ring has duplicate coordinates (spikes/figure-8 from merged holes).
// A valid simple polygon visits each coordinate exactly once (except closing point).
inline bool ring_has_duplicate_coords(const std::vector<std::pair<double,double>>& ring) {
    std::unordered_set<int64_t> seen;
    for (size_t i = 0; i + 1 < ring.size(); i++) {
        int64_t k = coord_key(ring[i].first, ring[i].second);
        if (!seen.insert(k).second) return true;
    }
    return false;
}

// Douglas-Peucker line simplification
inline void dp_simplify(const std::vector<std::pair<double,double>>& pts,
                        size_t start, size_t end, double epsilon,
                        std::vector<bool>& keep) {
    if (end <= start + 1) return;

    double max_dist = 0;
    size_t max_idx = start;

    double ax = pts[start].first, ay = pts[start].second;
    double bx = pts[end].first, by = pts[end].second;
    double dx = bx - ax, dy = by - ay;
    double len_sq = dx * dx + dy * dy;

    for (size_t i = start + 1; i < end; i++) {
        double px = pts[i].first - ax, py = pts[i].second - ay;
        double dist;
        if (len_sq == 0) {
            dist = std::sqrt(px * px + py * py);
        } else {
            double t = std::max(0.0, std::min(1.0, (px * dx + py * dy) / len_sq));
            double proj_x = t * dx - px, proj_y = t * dy - py;
            dist = std::sqrt(proj_x * proj_x + proj_y * proj_y);
        }
        if (dist > max_dist) {
            max_dist = dist;
            max_idx = i;
        }
    }

    if (max_dist > epsilon) {
        keep[max_idx] = true;
        dp_simplify(pts, start, max_idx, epsilon, keep);
        dp_simplify(pts, max_idx, end, epsilon, keep);
    }
}

static constexpr size_t MAX_POLYGON_VERTICES = 500;

// Simplify to at most max_vertices (binary search for epsilon). Original behavior.
inline std::vector<std::pair<double,double>> simplify_polygon(
    const std::vector<std::pair<double,double>>& pts, size_t max_vertices = MAX_POLYGON_VERTICES) {
    if (pts.size() <= max_vertices) return pts;

    double lo = 0, hi = 1.0;
    std::vector<std::pair<double,double>> result;

    for (int iter = 0; iter < 20; iter++) {
        double epsilon = (lo + hi) / 2;
        std::vector<bool> keep(pts.size(), false);
        keep[0] = true;
        keep[pts.size() - 1] = true;
        dp_simplify(pts, 0, pts.size() - 1, epsilon, keep);

        size_t count = 0;
        for (bool k : keep) if (k) count++;

        if (count > max_vertices) {
            lo = epsilon;
        } else {
            hi = epsilon;
        }
    }

    std::vector<bool> keep(pts.size(), false);
    keep[0] = true;
    keep[pts.size() - 1] = true;
    dp_simplify(pts, 0, pts.size() - 1, hi, keep);

    result.clear();
    for (size_t i = 0; i < pts.size(); i++) {
        if (keep[i]) result.push_back(pts[i]);
    }
    return result;
}

// Error-bounded simplification: maximum displacement of epsilon_deg (in degrees).
// Epsilon is in the same coordinate units as the vertices (degrees).
// Converts meters to degrees using latitude: epsilon_deg = meters / (111320 * cos(lat))
// Always keeps at least min_vertices (default 4) to preserve basic shape.
inline std::vector<std::pair<double,double>> simplify_polygon_epsilon(
    const std::vector<std::pair<double,double>>& pts, double epsilon_deg,
    size_t min_vertices = 4) {
    if (pts.size() <= min_vertices) return pts;

    std::vector<bool> keep(pts.size(), false);
    keep[0] = true;
    keep[pts.size() - 1] = true;
    dp_simplify(pts, 0, pts.size() - 1, epsilon_deg, keep);

    size_t count = 0;
    for (bool k : keep) if (k) count++;

    // If simplification went below min_vertices, reduce epsilon until we have enough
    if (count < min_vertices) {
        double lo = 0, hi = epsilon_deg;
        for (int iter = 0; iter < 20; iter++) {
            double mid = (lo + hi) / 2;
            std::vector<bool> try_keep(pts.size(), false);
            try_keep[0] = true;
            try_keep[pts.size() - 1] = true;
            dp_simplify(pts, 0, pts.size() - 1, mid, try_keep);
            size_t c = 0;
            for (bool k : try_keep) if (k) c++;
            if (c >= min_vertices) {
                lo = mid;
                keep = try_keep;
                count = c;
            } else {
                hi = mid;
            }
        }
    }

    std::vector<std::pair<double,double>> result;
    for (size_t i = 0; i < pts.size(); i++) {
        if (keep[i]) result.push_back(pts[i]);
    }
    return result;
}

// Convert meters to degrees at a given latitude
inline double meters_to_degrees(double meters, double latitude_deg) {
    double cos_lat = std::cos(latitude_deg * M_PI / 180.0);
    if (cos_lat < 0.01) cos_lat = 0.01; // clamp near poles
    return meters / (111320.0 * cos_lat);
}

// Per-level epsilon defaults in meters. Index 0-1 unused, 2=country ... 8=city.
// Can be overridden at runtime via kAdminEpsilonMeters and kEpsilonScale.
inline double kDefaultEpsilon[] = {
    15.0,   // 0 (unused)
    15.0,   // 1 (unused)
    500.0,  // 2 country
    200.0,  // 3 large region
    100.0,  // 4 state/province
    50.0,   // 5 district
    30.0,   // 6 county
    20.0,   // 7 municipality
    15.0,   // 8 city/town
    15.0,   // 9
    15.0,   // 10
    15.0,   // 11 postal
};

// Runtime-configurable epsilon table and scale factor.
// Set by main() from CLI args. Scale multiplies all values uniformly.
inline double kAdminEpsilonMeters[12] = {
    15, 15, 500, 200, 100, 50, 30, 20, 15, 15, 15, 15
};
inline double kEpsilonScale = 1.0;

inline double admin_epsilon_meters(uint8_t admin_level) {
    int idx = (admin_level <= 11) ? admin_level : 11;
    return kAdminEpsilonMeters[idx] * kEpsilonScale;
}

// Parse leading digits from a house number string
inline uint32_t parse_house_number(const char* s) {
    if (!s) return 0;
    uint32_t n = 0;
    while (*s >= '0' && *s <= '9') {
        n = n * 10 + (*s - '0');
        s++;
    }
    return n;
}

// Matches Nominatim's default MAIN_TAGS_STREETS preset
// (lib-lua/themes/nominatim/presets.lua). Always-include: motorway,
// trunk, primary, secondary, tertiary, unclassified, residential,
// road, living_street, pedestrian. "Named only" types (service,
// cycleway, path, footway, steps, track) flow through this filter
// too because the caller already gates on a non-empty name. Only
// construction and bridleway are excluded.
//
// footway subtype filter is handled by is_included_footway_subtype
// below — Nominatim's filter_footways excludes sidewalk / crossing /
// link / traffic_aisle even when the footway has a name, because
// those are structural subdivisions of a parent road rather than
// addressable features in their own right.
inline bool is_included_highway(const char* value) {
    switch (value[0]) {
        case 'c': return std::strcmp(value, "construction") != 0;
        case 'b': return std::strcmp(value, "bridleway") != 0;
        default: return true;
    }
}

// Mirrors lua/themes/nominatim/presets.lua filter_footways: excludes
// footway=sidewalk / crossing / link / traffic_aisle even on named
// ways. Called when highway==footway; other highway types ignore the
// footway tag.
inline bool is_included_footway_subtype(const char* footway) {
    if (footway == nullptr) return true; // named footway with no subtype
    switch (footway[0]) {
        case 's': return std::strcmp(footway, "sidewalk") != 0;
        case 'c': return std::strcmp(footway, "crossing") != 0;
        case 'l': return std::strcmp(footway, "link") != 0;
        case 't': return std::strcmp(footway, "traffic_aisle") != 0;
        default: return true;
    }
}

// Convenience: full highway acceptance check including footway
// subtype filter for highway=footway ways.
inline bool is_included_highway_full(const char* highway, const char* footway) {
    if (!is_included_highway(highway)) return false;
    if (std::strcmp(highway, "footway") == 0) {
        return is_included_footway_subtype(footway);
    }
    return true;
}
