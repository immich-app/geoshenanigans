#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
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

// Validate a postcode string for the centroid index. Mirrors
// Nominatim's clean_postcodes sanitizer — rejects values that
// don't conform to any known postcode pattern. The builder
// doesn't track per-feature country codes at addr extraction
// time, so this is a universal filter rather than per-country.
//
// Accepts: 3-7 digit postcodes (covers ~80% of countries),
// spaced digit patterns (SE/GR "ddd dd", IN "ddd ddd"),
// letter-digit patterns (UK "SW1A 1AA", CA "K1A 0B1",
// NL "1234 AB", AR "B1234ABC"), hyphenated (BR "12345-678",
// JP "123-4567").
//
// Rejects: PO boxes, semicolons, zip+4 ("12345-1234"),
// strings > 10 chars, all-zero placeholders, freeform text.
inline bool is_valid_postcode(const char* pc) {
    if (!pc || !pc[0]) return false;
    size_t len = std::strlen(pc);
    if (len > 10) return false;
    // Reject semicolons (multiple values)
    for (size_t i = 0; i < len; i++) {
        if (pc[i] == ';') return false;
    }
    // Reject "PO" prefix
    if (len >= 2 && pc[0] == 'P' && pc[1] == 'O') return false;
    // Reject US zip+4 (5 digits, dash, 4 digits)
    if (len == 10 && pc[5] == '-') {
        bool all_digits = true;
        for (size_t i = 0; i < 10; i++) {
            if (i == 5) continue;
            if (pc[i] < '0' || pc[i] > '9') { all_digits = false; break; }
        }
        if (all_digits) return false;
    }
    // Reject all-same-char (placeholders like "00000")
    bool all_same = true;
    for (size_t i = 1; i < len; i++) {
        if (pc[i] != pc[0]) { all_same = false; break; }
    }
    if (all_same && len >= 3) return false;
    // Must contain at least one digit (pure-letter strings aren't postcodes)
    bool has_digit = false;
    for (size_t i = 0; i < len; i++) {
        if (pc[i] >= '0' && pc[i] <= '9') { has_digit = true; break; }
    }
    if (!has_digit) return false;
    return true;
}

// Approximate ICU transliteration for street name matching.
// Lowercase, strip common Latin accents, collapse whitespace,
// remove apostrophes/hyphens. Good enough for matching
// "Place de l'Hôtel de Ville" against "Place de l'Hotel de Ville".
inline std::string normalise_for_matching(const char* s) {
    if (!s) return "";
    std::string out;
    out.reserve(std::strlen(s));
    bool last_space = true; // trim leading
    const unsigned char* p = reinterpret_cast<const unsigned char*>(s);
    while (*p) {
        unsigned char c = *p;
        // Handle 2-byte UTF-8 sequences for common accented chars
        if (c >= 0xC0 && c < 0xE0 && p[1] >= 0x80) {
            uint16_t cp = ((c & 0x1F) << 6) | (p[1] & 0x3F);
            char replacement = 0;
            // Latin accented → base letter
            if (cp >= 0xC0 && cp <= 0xC5) replacement = 'a';
            else if (cp == 0xC6) replacement = 'a'; // Æ→a
            else if (cp == 0xC7) replacement = 'c'; // Ç
            else if (cp >= 0xC8 && cp <= 0xCB) replacement = 'e';
            else if (cp >= 0xCC && cp <= 0xCF) replacement = 'i';
            else if (cp == 0xD1) replacement = 'n';
            else if (cp >= 0xD2 && cp <= 0xD6) replacement = 'o';
            else if (cp == 0xD8) replacement = 'o';
            else if (cp >= 0xD9 && cp <= 0xDC) replacement = 'u';
            else if (cp == 0xDD) replacement = 'y';
            else if (cp >= 0xE0 && cp <= 0xE5) replacement = 'a';
            else if (cp == 0xE6) replacement = 'a';
            else if (cp == 0xE7) replacement = 'c';
            else if (cp >= 0xE8 && cp <= 0xEB) replacement = 'e';
            else if (cp >= 0xEC && cp <= 0xEF) replacement = 'i';
            else if (cp == 0xF1) replacement = 'n';
            else if (cp >= 0xF2 && cp <= 0xF6) replacement = 'o';
            else if (cp == 0xF8) replacement = 'o';
            else if (cp >= 0xF9 && cp <= 0xFC) replacement = 'u';
            else if (cp == 0xFD || cp == 0xFF) replacement = 'y';
            if (replacement) {
                out += replacement;
                last_space = false;
            }
            p += 2;
            continue;
        }
        // Skip 3+ byte UTF-8 sequences
        if (c >= 0xE0) {
            int skip = (c < 0xF0) ? 3 : 4;
            p += skip;
            continue;
        }
        // ASCII range
        if (c == '\'' || c == '\x60' || c == '\xB4') { p++; continue; } // apostrophes
        if (c == '-') { p++; continue; } // hyphens
        if (c == ' ' || c == '\t' || c == '\n') {
            if (!last_space && out.size() > 0) out += ' ';
            last_space = true;
            p++;
            continue;
        }
        if (c >= 'A' && c <= 'Z') c = c + 32; // lowercase
        out += static_cast<char>(c);
        last_space = false;
        p++;
    }
    // Trim trailing space
    if (!out.empty() && out.back() == ' ') out.pop_back();
    return out;
}

// Check if any normalised word token from string A appears in string B.
// Matches Nominatim's token_matches_street which uses PostgreSQL's
// array overlap operator (&&) on tokenised name vectors.
inline bool tokens_overlap(const char* a, const char* b) {
    if (!a || !b || !a[0] || !b[0]) return false;
    std::string na = normalise_for_matching(a);
    std::string nb = normalise_for_matching(b);
    if (na.empty() || nb.empty()) return false;
    // Split A into tokens, check each against B's tokens
    // For efficiency with typical street names (2-5 words),
    // collect B's tokens into a set then check A's tokens.
    std::unordered_set<std::string> b_tokens;
    {
        size_t pos = 0;
        while (pos < nb.size()) {
            size_t next = nb.find(' ', pos);
            if (next == std::string::npos) next = nb.size();
            std::string tok = nb.substr(pos, next - pos);
            if (tok.size() >= 2) b_tokens.insert(std::move(tok)); // skip tiny words
            pos = next + 1;
        }
    }
    // Check A's tokens
    size_t pos = 0;
    while (pos < na.size()) {
        size_t next = na.find(' ', pos);
        if (next == std::string::npos) next = na.size();
        std::string tok = na.substr(pos, next - pos);
        if (tok.size() >= 2 && b_tokens.count(tok)) return true;
        pos = next + 1;
    }
    return false;
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

// Allow-list of highway=* values that count as "streets" for reverse
// geocoding. Mirrors Nominatim's `MAIN_TAGS_STREETS.default` from
// lib-lua/themes/nominatim/presets.lua, which is the definitive
// source for the planet-wide import style.
//
// Everything NOT in this list is rejected — critically this excludes
// highway=platform (tram/bus stops with their own name), bus_stop,
// raceway, proposed, razed, construction, services, rest_area, etc.
// A live bug in Dresden had our "highway=platform name=Postplatz"
// tram-stop ways beating the real "highway=service name=Wallstraße"
// road at every query that landed on the platform geometry, because
// the old deny-list let `platform` through.
//
// Named-vs-always distinction (Nominatim: service/cycleway/path/
// steps/bridleway/track/*_link are 'named' — indexed only when they
// carry a name tag) is enforced at the call site by the `&& t_name`
// guard before is_included_highway_full() is ever reached, so it's
// already implicit.
inline bool is_included_highway(const char* value) {
    if (!value) return false;
    // Dispatch on first char to cut strcmp cost. The lists below are
    // kept in sync with Nominatim's presets.lua MAIN_TAGS_STREETS.default.
    switch (value[0]) {
        case 'b':
            return std::strcmp(value, "bridleway") == 0;
        case 'c':
            return std::strcmp(value, "cycleway") == 0;
        case 'f':
            return std::strcmp(value, "footway") == 0;
        case 'l':
            return std::strcmp(value, "living_street") == 0;
        case 'm':
            return std::strcmp(value, "motorway") == 0 ||
                   std::strcmp(value, "motorway_link") == 0;
        case 'p':
            return std::strcmp(value, "primary") == 0 ||
                   std::strcmp(value, "primary_link") == 0 ||
                   std::strcmp(value, "pedestrian") == 0 ||
                   std::strcmp(value, "path") == 0;
        case 'r':
            return std::strcmp(value, "road") == 0 ||
                   std::strcmp(value, "residential") == 0;
        case 's':
            return std::strcmp(value, "secondary") == 0 ||
                   std::strcmp(value, "secondary_link") == 0 ||
                   std::strcmp(value, "service") == 0 ||
                   std::strcmp(value, "steps") == 0;
        case 't':
            return std::strcmp(value, "tertiary") == 0 ||
                   std::strcmp(value, "tertiary_link") == 0 ||
                   std::strcmp(value, "trunk") == 0 ||
                   std::strcmp(value, "trunk_link") == 0 ||
                   std::strcmp(value, "track") == 0;
        case 'u':
            return std::strcmp(value, "unclassified") == 0;
        default:
            return false;
    }
}

// Full filter including footway subtype and access checks.
// Excludes footway=sidewalk/crossing (unnamed adjuncts) and
// any highway with access=private + tunnel=yes (service tunnels
// like Treasury Annex Tunnel near the White House).
inline bool is_included_highway_full(const char* highway, const char* footway,
                                      const char* access = nullptr, const char* tunnel = nullptr) {
    if (!is_included_highway(highway)) return false;
    // Exclude footway subtypes that are road adjuncts
    if (highway[0] == 'f' && std::strcmp(highway, "footway") == 0 && footway) {
        if (std::strcmp(footway, "sidewalk") == 0) return false;
        if (std::strcmp(footway, "crossing") == 0) return false;
    }
    // No access/tunnel filtering — Nominatim indexes all highways
    // unconditionally. The Treasury Annex Tunnel case is handled by
    // the addr_point area→node preference rule (the 1600 Penn Ave
    // addr_point at ~25m beats the tunnel at ~21m because it's inside
    // the President's Park polygon and the preference rule demotes
    // the enclosing POI).
    return true;
}
