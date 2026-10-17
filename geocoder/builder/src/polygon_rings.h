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

// For each of a relation's inner rings (closed), the index of the outer ring
// it is a hole of, or -1: the smallest outer ring larger than it that holds
// its interior point. An enclave inside an enclave (Baarle's Dutch pieces in
// the Belgian ones) is an outer ring inside a hole; being smaller than the
// hole it can't claim it.
inline std::vector<int> assign_holes(const std::vector<std::vector<LatLng>>& outers,
                                     const std::vector<std::vector<LatLng>>& inners) {
    struct Box { double lat_lo, lat_hi, lng_lo, lng_hi; };
    auto box_of = [](const std::vector<LatLng>& r) {
        Box b{INFINITY, -INFINITY, INFINITY, -INFINITY};
        for (const auto& [lat, lng] : r)
            b = {std::min(b.lat_lo, lat), std::max(b.lat_hi, lat), std::min(b.lng_lo, lng), std::max(b.lng_hi, lng)};
        return b;
    };
    std::vector<Box> boxes;
    std::vector<double> areas;
    for (const auto& o : outers) {
        boxes.push_back(box_of(o));
        areas.push_back(std::fabs(signed_area2(o)));
    }
    std::vector<int> owner(inners.size(), -1);
    std::vector<RingSpan> whole(1);
    for (size_t i = 0; i < inners.size(); i++) {
        const auto& in = inners[i];
        if (in.size() < kMinClosedRing) continue;
        whole[0] = {0, static_cast<uint32_t>(in.size())};
        const auto [lat, lng] = interior_point(in.data(), whole);
        const double area = std::fabs(signed_area2(in));
        double best = INFINITY;
        for (size_t o = 0; o < outers.size(); o++) {
            const Box& b = boxes[o];
            if (areas[o] <= area || areas[o] >= best) continue;
            if (lat < b.lat_lo || lat > b.lat_hi || lng < b.lng_lo || lng > b.lng_hi) continue;
            if (!ring_contains_latlng(outers[o], lat, lng)) continue;
            best = areas[o];
            owner[i] = static_cast<int>(o);
        }
    }
    return owner;
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
