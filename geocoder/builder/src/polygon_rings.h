// Admin polygons with holes, stored so that a plain crossing-number test
// honours them ("keyhole" encoding).
//
// An admin polygon record covers one outer ring of its relation. The
// relation's inner rings inside that outer ring (enclaves: the Vatican in
// Italy and Rome, Büsingen in Switzerland, Baarle-Hertog's Belgian pieces in
// the Netherlands) follow the outer ring in the same vertex range, each
// reached by an excursion from the outer ring's first vertex v0:
//
//   v0 ... v0                      the outer ring, closed
//   v0, c, h0 ... h0, c            per hole, with connector c = (v0.lat, h0.lng)
//
// and the range's closing edge (last c back to v0) ends the last excursion.
// Every connector edge is horizontal (v0 - c) or vertical (c - h0) and is
// walked once each way. A vertical edge never straddles a point's longitude.
// A horizontal edge reaches exactly its own latitude at any longitude,
// whichever way it is walked (its slope term is an exact zero), so its two
// crossings cancel. A crossing-number test over the range (ring_contains,
// the reader's point_in_polygon, and their axis-swapped variants) therefore
// counts the rings' own edges only: a point is inside when it is inside the
// outer ring and outside every hole, as Nominatim's PostGIS geometry has it.
// Snapping the vertices to a grid keeps the connectors axis-aligned, as c
// shares v0's latitude and h0's longitude bit for bit.
//
// Holes run against the outer ring, so the range's shoelace area is the
// outer ring's minus the holes' (the connector terms cancel exactly).
// AdminPolygon::hole_count says how many excursions follow the outer ring;
// split_rings recovers the rings for the passes that need them one by one
// (S2 covering, simplification, interior points).
#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

#include "types.h"

using LatLng = std::pair<double, double>;

inline double ring_lat(const LatLng& p) { return p.first; }
inline double ring_lng(const LatLng& p) { return p.second; }
inline double ring_lat(const NodeCoord& p) { return p.lat; }
inline double ring_lng(const NodeCoord& p) { return p.lng; }

template <class P>
inline bool same_vertex(const P& a, const P& b) {
    return ring_lat(a) == ring_lat(b) && ring_lng(a) == ring_lng(b);
}

// A ring inside a polygon's vertex range: `count` vertices from `begin`,
// the last repeating the first.
struct RingSpan {
    uint32_t begin;
    uint32_t count;
};

// Vertices of one excursion beyond its hole ring: v0, c before it and c after.
static constexpr uint32_t kExcursionConnectors = 3;
// A closed ring's fewest vertices (a triangle and its closing vertex).
static constexpr uint32_t kMinClosedRing = 4;

// The keyhole vertex range of a closed outer ring and closed holes.
inline std::vector<LatLng> encode_rings(const std::vector<LatLng>& outer,
                                        const std::vector<std::vector<LatLng>>& holes) {
    size_t total = outer.size();
    for (const auto& h : holes) total += h.size() + kExcursionConnectors;
    std::vector<LatLng> out;
    out.reserve(total);
    out.insert(out.end(), outer.begin(), outer.end());
    const LatLng v0 = outer.front();
    for (const auto& h : holes) {
        const LatLng c{v0.first, h.front().second};
        out.push_back(v0);
        out.push_back(c);
        out.insert(out.end(), h.begin(), h.end());
        out.push_back(c);
    }
    return out;
}

// The outer ring (first) and the holes of a keyhole range of n vertices with
// `holes` excursions, or false when the range doesn't parse as that (spans
// then holds the whole range as one ring). Excursions are peeled off the end:
// each is v0, c, h0 ... h0, c, so the hole ring starts at the last h0 that
// follows v0 and c.
template <class P>
bool split_rings(const P* v, uint32_t n, uint16_t holes, std::vector<RingSpan>& spans) {
    spans.clear();
    uint32_t end = n;
    bool ok = true;
    for (uint16_t k = 0; k < holes && ok; k++) {
        ok = false;
        if (end < kMinClosedRing + kExcursionConnectors + kMinClosedRing) break;
        const P& c = v[end - 1];
        const P& h0 = v[end - 2];
        for (uint32_t j = end - 1 - kMinClosedRing; j >= kMinClosedRing + 2; j--) {
            if (same_vertex(v[j], h0) && same_vertex(v[j - 1], c) && same_vertex(v[j - 2], v[0])) {
                spans.push_back({j, end - 1 - j});
                end = j - 2;
                ok = true;
                break;
            }
        }
    }
    if (ok && (end < kMinClosedRing || !same_vertex(v[end - 1], v[0]))) ok = false;
    if (!ok) {
        spans.assign(1, RingSpan{0, n});
        return false;
    }
    spans.push_back({0, end});
    std::reverse(spans.begin(), spans.end());
    return true;
}

// Number of vertices of the outer ring of a keyhole range (all n when it has
// no holes or doesn't parse).
template <class P>
uint32_t outer_ring_count(const P* v, uint32_t n, uint16_t holes) {
    if (holes == 0) return n;
    std::vector<RingSpan> spans;
    split_rings(v, n, holes, spans);
    return spans.front().count;
}

// Twice the signed shoelace area of a closed ring, in degrees squared.
inline double signed_area2(const std::vector<LatLng>& ring) {
    double a = 0;
    for (size_t i = 0, j = ring.size() - 1; i < ring.size(); j = i++)
        a += ring[j].first * ring[i].second - ring[i].first * ring[j].second;
    return a;
}

// Crossing-number containment in double precision (the ring closed or not).
inline bool ring_contains_latlng(const std::vector<LatLng>& ring, double lat, double lng) {
    bool inside = false;
    for (size_t a = 0, b = ring.size() - 1; a < ring.size(); b = a++) {
        const LatLng& va = ring[a];
        const LatLng& vb = ring[b];
        if (((va.second > lng) != (vb.second > lng)) &&
            (lat < (vb.first - va.first) * (lng - va.second) / (vb.second - va.second) + va.first))
            inside = !inside;
    }
    return inside;
}

// A point inside the polygon made of `rings` (outer first, then holes), as
// GEOS's InteriorPointArea finds it (PostGIS ST_PointOnSurface, which
// Nominatim's get_center_point uses for areas): the midpoint of the widest
// inside stretch of the horizontal line half-way between the vertex
// latitudes nearest the middle of the latitude range. Falls back to the
// first vertex when no stretch has width (a degenerate ring). Returns
// (lat, lng).
template <class P>
LatLng interior_point(const P* v, const std::vector<RingSpan>& rings) {
    const RingSpan& outer = rings.front();
    double lo = std::numeric_limits<double>::infinity(), hi = -lo;
    for (uint32_t k = 0; k < outer.count; k++) {
        lo = std::min(lo, ring_lat(v[outer.begin + k]));
        hi = std::max(hi, ring_lat(v[outer.begin + k]));
    }
    const double centre = (lo + hi) / 2;
    for (const RingSpan& r : rings)
        for (uint32_t k = 0; k < r.count; k++) {
            const double y = ring_lat(v[r.begin + k]);
            if (y <= centre) {
                if (y > lo) lo = y;
            } else if (y < hi) {
                hi = y;
            }
        }
    const double scan = (lo + hi) / 2;

    std::vector<double> crossings;
    for (const RingSpan& r : rings)
        for (uint32_t k = 1; k < r.count; k++) {
            const double y0 = ring_lat(v[r.begin + k - 1]), y1 = ring_lat(v[r.begin + k]);
            const double x0 = ring_lng(v[r.begin + k - 1]), x1 = ring_lng(v[r.begin + k]);
            if ((y0 > scan && y1 > scan) || (y0 < scan && y1 < scan)) continue;
            if (y0 == y1) continue;
            if (y0 == scan && y1 < scan) continue;  // downward edge leaves out its start
            if (y1 == scan && y0 < scan) continue;  // upward edge leaves out its end
            crossings.push_back(x0 == x1 ? x0 : x0 + (scan - y0) / ((y1 - y0) / (x1 - x0)));
        }
    std::sort(crossings.begin(), crossings.end());
    LatLng best{ring_lat(v[outer.begin]), ring_lng(v[outer.begin])};
    double best_width = 0;
    for (size_t k = 0; k + 1 < crossings.size(); k += 2) {
        const double width = crossings[k + 1] - crossings[k];
        if (width > best_width) {
            best_width = width;
            best = {scan, (crossings[k] + crossings[k + 1]) / 2};
        }
    }
    return best;
}

// interior_point of a polygon's keyhole range.
template <class P>
LatLng polygon_interior_point(const P* v, uint32_t n, uint16_t holes) {
    std::vector<RingSpan> spans;
    split_rings(v, n, holes, spans);
    return interior_point(v, spans);
}

// For each closed ring of a relation, the index of the ring immediately
// around it, or -1: the smallest larger ring that holds its interior point.
// Rings that don't cross either nest or lie apart, so a ring holding another's
// interior point and larger than it is around it. This is the nesting osmium's
// assembler finds (its find_enclosing_ring counts the rings below a ring's
// lowest point) without looking at member roles.
inline std::vector<int> enclosing_rings(const std::vector<std::vector<LatLng>>& rings) {
    struct Box { double lat_lo, lat_hi, lng_lo, lng_hi; };
    const size_t n = rings.size();
    std::vector<Box> boxes(n);
    std::vector<double> areas(n);
    std::vector<LatLng> inner_point(n);
    std::vector<RingSpan> whole(1);
    for (size_t i = 0; i < n; i++) {
        Box b{INFINITY, -INFINITY, INFINITY, -INFINITY};
        for (const auto& [lat, lng] : rings[i])
            b = {std::min(b.lat_lo, lat), std::max(b.lat_hi, lat), std::min(b.lng_lo, lng), std::max(b.lng_hi, lng)};
        boxes[i] = b;
        areas[i] = std::fabs(signed_area2(rings[i]));
        whole[0] = {0, static_cast<uint32_t>(rings[i].size())};
        if (rings[i].size() >= kMinClosedRing) inner_point[i] = interior_point(rings[i].data(), whole);
    }
    std::vector<uint32_t> by_lat(n);
    for (uint32_t i = 0; i < n; i++) by_lat[i] = i;
    std::sort(by_lat.begin(), by_lat.end(), [&](uint32_t a, uint32_t b) { return inner_point[a].first < inner_point[b].first; });

    std::vector<int> around(n, -1);
    std::vector<uint32_t> candidates;
    std::vector<char> odd;
    for (size_t j = 0; j < n; j++) {
        const auto& ring = rings[j];
        if (ring.size() < kMinClosedRing) continue;
        const Box& b = boxes[j];
        // Smaller rings whose interior point is in ring j's box and that have
        // no smaller ring around them yet.
        candidates.clear();
        auto lo = std::lower_bound(by_lat.begin(), by_lat.end(), b.lat_lo,
                                   [&](uint32_t i, double lat) { return inner_point[i].first < lat; });
        for (auto it = lo; it != by_lat.end() && inner_point[*it].first <= b.lat_hi; ++it) {
            const uint32_t i = *it;
            if (i == j || rings[i].size() < kMinClosedRing || areas[i] >= areas[j]) continue;
            if (inner_point[i].second < b.lng_lo || inner_point[i].second > b.lng_hi) continue;
            if (around[i] >= 0 && areas[around[i]] <= areas[j]) continue;
            candidates.push_back(i);
        }
        if (candidates.empty()) continue;
        // One pass over ring j's edges counts the crossings for all of them
        // (ring_contains_latlng's rule), the points sorted by longitude so
        // each edge visits only those in its longitude span.
        std::sort(candidates.begin(), candidates.end(),
                  [&](uint32_t a, uint32_t c) { return inner_point[a].second < inner_point[c].second; });
        odd.assign(candidates.size(), 0);
        auto lng_lower = [&](double lng) {
            return std::lower_bound(candidates.begin(), candidates.end(), lng,
                                    [&](uint32_t i, double x) { return inner_point[i].second < x; }) - candidates.begin();
        };
        for (size_t k = 1; k < ring.size(); k++) {
            const LatLng& va = ring[k];
            const LatLng& vb = ring[k - 1];
            const auto [x0, x1] = std::minmax(va.second, vb.second);
            if (x0 == x1) continue;
            for (auto c = lng_lower(x0), end = lng_lower(x1); c < end; c++) {
                const auto [lat, lng] = inner_point[candidates[c]];
                if (lat < (vb.first - va.first) * (lng - va.second) / (vb.second - va.second) + va.first) odd[c] ^= 1;
            }
        }
        for (size_t c = 0; c < candidates.size(); c++)
            if (odd[c]) around[candidates[c]] = static_cast<int>(j);
    }
    return around;
}

// How many rings are around each ring, following enclosing_rings' chains.
inline std::vector<int> ring_depths(const std::vector<int>& around) {
    std::vector<int> depth(around.size(), 0);
    for (size_t i = 0; i < around.size(); i++)
        for (int r = around[i]; r >= 0; r = around[r]) depth[i]++;
    return depth;
}

// One outer ring of a relation with the holes in it.
struct NestedPolygon {
    std::vector<LatLng> outer;
    std::vector<std::vector<LatLng>> holes;
};

// The rings as polygons, nested as osmium nests them: each ring at even depth
// is an outer ring (an island in a lake in an island is one again), in ring
// order, and each ring at odd depth a hole in the ring around it.
inline std::vector<NestedPolygon> nest_rings(std::vector<std::vector<LatLng>> rings, const std::vector<int>& around,
                                             const std::vector<int>& depth) {
    std::vector<NestedPolygon> polygons;
    std::vector<int> polygon_of(rings.size(), -1);
    for (size_t i = 0; i < rings.size(); i++) {
        if (depth[i] % 2) continue;
        polygon_of[i] = static_cast<int>(polygons.size());
        polygons.push_back({std::move(rings[i]), {}});
    }
    for (size_t i = 0; i < rings.size(); i++)
        if (depth[i] % 2) polygons[polygon_of[around[i]]].holes.push_back(std::move(rings[i]));
    return polygons;
}

// The keyhole range of a closed outer ring and its closed holes once
// `simplify` (closed ring in, closed ring out) has run on each, and how many
// holes it holds: holes that collapse below a triangle are dropped, the rest
// are put in a canonical order (by first vertex) and turned against the
// outer ring. Without holes the range is just the simplified outer ring.
template <class Simplify>
std::pair<std::vector<LatLng>, uint16_t> simplify_rings(const std::vector<LatLng>& outer,
                                                        const std::vector<std::vector<LatLng>>& holes,
                                                        Simplify simplify) {
    std::vector<LatLng> out = simplify(outer);
    if (holes.empty() || out.size() < kMinClosedRing) return {std::move(out), 0};
    const bool outer_ccw = signed_area2(out) > 0;
    std::vector<std::vector<LatLng>> kept;
    for (const auto& h : holes) {
        std::vector<LatLng> s = simplify(h);
        if (s.size() < kMinClosedRing || !same_vertex(s.front(), s.back())) continue;
        if ((signed_area2(s) > 0) == outer_ccw) std::reverse(s.begin(), s.end());
        kept.push_back(std::move(s));
    }
    if (kept.size() > std::numeric_limits<uint16_t>::max()) {
        std::stable_sort(kept.begin(), kept.end(), [](const auto& a, const auto& b) {
            return std::fabs(signed_area2(a)) > std::fabs(signed_area2(b));
        });
        kept.resize(std::numeric_limits<uint16_t>::max());
    }
    if (kept.empty()) return {std::move(out), 0};
    std::stable_sort(kept.begin(), kept.end(), [](const auto& a, const auto& b) { return a.front() < b.front(); });
    std::vector<LatLng> range = encode_rings(out, kept);
    const auto count = static_cast<uint16_t>(kept.size());
    // The excursions must peel off again as the same rings.
    std::vector<RingSpan> spans;
    bool round_trip = split_rings(range.data(), static_cast<uint32_t>(range.size()), count, spans) &&
                      spans[0].count == out.size();
    for (size_t k = 0; round_trip && k < kept.size(); k++) round_trip = spans[k + 1].count == kept[k].size();
    if (!round_trip) return {std::move(out), 0};
    return {std::move(range), count};
}
