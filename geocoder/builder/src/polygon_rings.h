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
#include <memory>
#include <string>
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

// --- Ring topology ---
//
// The crossing-number test counts a point inside when an odd number of the
// range's rings hold it. That is the outer ring minus the holes only while
// every hole lies inside the outer ring and no two holes overlap: the part of
// a hole outside the outer ring counts as inside, and so does the overlap of
// two holes. Simplifying each ring on its own can break this near a boundary
// (the outer ring cut short across an enclave beside it, or two enclaves
// sharing a border each straightened their own way); rings_nest finds it.

// Twice the signed area of the triangle a, b, c with lng as x and lat as y:
// > 0 when c is left of a -> b, 0 when the three are collinear.
inline double orientation(const LatLng& a, const LatLng& b, const LatLng& c) {
    return (b.second - a.second) * (c.first - a.first) - (b.first - a.first) * (c.second - a.second);
}

// Whether c lies on the closed segment a-b.
inline bool on_segment(const LatLng& a, const LatLng& b, const LatLng& c) {
    return orientation(a, b, c) == 0 &&
           std::min(a.first, b.first) <= c.first && c.first <= std::max(a.first, b.first) &&
           std::min(a.second, b.second) <= c.second && c.second <= std::max(a.second, b.second);
}

// Where points lie against one closed ring: on one of its edges, else inside
// or outside by ring_contains_latlng's crossing rule. Edges are bucketed by
// longitude band, so a point reads only the edges at its own longitude, the
// only ones it can lie on or count a crossing with.
class RingLocator {
public:
    enum Side { kOutside, kOn, kInside };

    explicit RingLocator(const std::vector<LatLng>& ring) : ring_(ring) {
        for (const LatLng& p : ring) {
            lo_ = std::min(lo_, p.second);
            hi_ = std::max(hi_, p.second);
        }
        const size_t edges = ring.size() > 1 ? ring.size() - 1 : 0;
        bands_.resize(std::clamp<size_t>(edges / 4, 1, size_t{1} << 14));
        width_ = (hi_ - lo_) / static_cast<double>(bands_.size());
        for (uint32_t k = 0; k < edges; k++) {
            const size_t last = band(std::max(ring[k].second, ring[k + 1].second));
            for (size_t b = band(std::min(ring[k].second, ring[k + 1].second)); b <= last; b++)
                bands_[b].push_back(k);
        }
    }

    Side locate(const LatLng& p) const {
        if (!(p.second >= lo_ && p.second <= hi_)) return kOutside;
        bool inside = false;
        for (uint32_t k : bands_[band(p.second)]) {
            const LatLng& a = ring_[k];
            const LatLng& b = ring_[k + 1];
            if (on_segment(a, b, p)) return kOn;
            if (((a.second > p.second) != (b.second > p.second)) &&
                (p.first < (b.first - a.first) * (p.second - a.second) / (b.second - a.second) + a.first))
                inside = !inside;
        }
        return inside ? kInside : kOutside;
    }

private:
    // Monotonic in lng, so an edge's bands run from its low end's to its high end's.
    size_t band(double lng) const {
        if (!(width_ > 0)) return 0;
        const double x = std::floor((lng - lo_) / width_);
        return x <= 0 ? 0 : std::min(static_cast<size_t>(x), bands_.size() - 1);
    }

    const std::vector<LatLng>& ring_;
    double lo_ = std::numeric_limits<double>::infinity();
    double hi_ = -std::numeric_limits<double>::infinity();
    double width_ = 0;
    std::vector<std::vector<uint32_t>> bands_;
};

// Whether a crossing-number test over the keyhole range of these closed
// rings gives the outer ring minus the holes: no edge of one ring crosses an
// edge of another, every hole lies inside the outer ring, and no hole inside
// (or on top of) another. Rings may touch, at a vertex or along an edge they
// share, as OSM's enclaves do.
//
// A sweep by longitude meets every edge with the edges of other rings whose
// ranges overlap it, failing at a crossing. With none, each hole edge lies
// wholly inside, outside or along another ring between the points where
// that ring touches it, so the hole's vertices and the middle of each such
// stretch place the whole hole. The doubles are taken as exact; the one
// rounding that matters (a point collinear with a slanted edge it lies on,
// read as just off it) can only turn a yes into a no.
inline bool rings_nest(const std::vector<LatLng>& outer, const std::vector<std::vector<LatLng>>& holes) {
    if (holes.empty()) return true;
    std::vector<const std::vector<LatLng>*> rings{&outer};
    for (const auto& h : holes) rings.push_back(&h);

    struct Edge {
        double lng_lo, lng_hi, lat_lo, lat_hi;
        uint32_t ring, k;  // from vertex k to k + 1 of rings[ring]
    };
    std::vector<Edge> edges;
    for (uint32_t r = 0; r < rings.size(); r++) {
        const auto& ring = *rings[r];
        for (uint32_t k = 0; k + 1 < ring.size(); k++) {
            const LatLng& a = ring[k];
            const LatLng& b = ring[k + 1];
            if (same_vertex(a, b)) continue;
            edges.push_back({std::min(a.second, b.second), std::max(a.second, b.second),
                             std::min(a.first, b.first), std::max(a.first, b.first), r, k});
        }
    }
    std::sort(edges.begin(), edges.end(), [](const Edge& x, const Edge& y) {
        if (x.lng_lo != y.lng_lo) return x.lng_lo < y.lng_lo;
        return x.ring != y.ring ? x.ring < y.ring : x.k < y.k;
    });

    // Per hole edge: whether another ring has the same edge, and the vertices
    // of other rings inside it, where they touch it.
    std::vector<std::vector<char>> shared(rings.size());
    for (uint32_t r = 0; r < rings.size(); r++) shared[r].assign(rings[r]->size(), 0);
    struct Touch {
        uint32_t ring, k;
        LatLng p;
    };
    std::vector<Touch> touches;
    auto inside_edge = [](const LatLng& a, const LatLng& b, const LatLng& p) {
        return on_segment(a, b, p) && !same_vertex(p, a) && !same_vertex(p, b);
    };
    // Meets two edges of different rings; false when they cross.
    auto meet = [&](const Edge& e, const Edge& f) {
        if (e.lat_hi < f.lat_lo || f.lat_hi < e.lat_lo) return true;
        const LatLng& a = (*rings[e.ring])[e.k];
        const LatLng& b = (*rings[e.ring])[e.k + 1];
        const LatLng& c = (*rings[f.ring])[f.k];
        const LatLng& d = (*rings[f.ring])[f.k + 1];
        const double o1 = orientation(a, b, c), o2 = orientation(a, b, d);
        const double o3 = orientation(c, d, a), o4 = orientation(c, d, b);
        if (((o1 > 0 && o2 < 0) || (o1 < 0 && o2 > 0)) && ((o3 > 0 && o4 < 0) || (o3 < 0 && o4 > 0))) return false;
        if ((same_vertex(a, c) && same_vertex(b, d)) || (same_vertex(a, d) && same_vertex(b, c))) {
            shared[e.ring][e.k] = shared[f.ring][f.k] = 1;
            return true;
        }
        for (const LatLng* p : {&c, &d})
            if (e.ring > 0 && inside_edge(a, b, *p)) touches.push_back({e.ring, e.k, *p});
        for (const LatLng* p : {&a, &b})
            if (f.ring > 0 && inside_edge(c, d, *p)) touches.push_back({f.ring, f.k, *p});
        return true;
    };
    // Meets edge e with the edges in `active` still reaching its longitude,
    // dropping those that end before it.
    auto sweep = [&](const Edge& e, std::vector<uint32_t>& active) {
        for (size_t j = 0; j < active.size();) {
            const Edge& f = edges[active[j]];
            if (f.lng_hi < e.lng_lo) {
                active[j] = active.back();
                active.pop_back();
                continue;
            }
            if (f.ring != e.ring && !meet(e, f)) return false;
            j++;
        }
        return true;
    };
    // The outer ring's edges meet hole edges only: an outer ring of a
    // million vertices costs its sort and nothing per pair of its own edges.
    std::vector<uint32_t> outer_active, hole_active;
    for (uint32_t i = 0; i < edges.size(); i++) {
        const Edge& e = edges[i];
        if (!sweep(e, hole_active)) return false;
        if (e.ring == 0) {
            outer_active.push_back(i);
        } else {
            if (!sweep(e, outer_active)) return false;
            hole_active.push_back(i);
        }
    }

    // Each hole's sample points: its vertices and the middle of every
    // stretch of its edges between touching vertices, edges another ring
    // shares left out.
    std::sort(touches.begin(), touches.end(), [](const Touch& x, const Touch& y) {
        if (x.ring != y.ring) return x.ring < y.ring;
        return x.k != y.k ? x.k < y.k : x.p < y.p;
    });
    std::vector<std::vector<LatLng>> samples(rings.size());
    size_t t = 0;
    std::vector<LatLng> stops;
    for (uint32_t r = 1; r < rings.size(); r++) {
        const auto& ring = *rings[r];
        for (uint32_t k = 0; k + 1 < ring.size(); k++) {
            samples[r].push_back(ring[k]);
            stops.assign({ring[k], ring[k + 1]});
            for (; t < touches.size() && touches[t].ring == r && touches[t].k == k; t++) stops.push_back(touches[t].p);
            if (shared[r][k] || same_vertex(ring[k], ring[k + 1])) continue;
            // Points on a segment sort along it.
            std::sort(stops.begin(), stops.end());
            stops.erase(std::unique(stops.begin(), stops.end()), stops.end());
            for (size_t j = 0; j + 1 < stops.size(); j++)
                samples[r].push_back({(stops[j].first + stops[j + 1].first) / 2,
                                      (stops[j].second + stops[j + 1].second) / 2});
        }
    }

    const RingLocator outer_at(outer);
    for (uint32_t r = 1; r < rings.size(); r++)
        for (const LatLng& p : samples[r])
            if (outer_at.locate(p) == RingLocator::kOutside) return false;

    // Holes whose boxes overlap, found by a sweep by longitude: whether a
    // sample of hole r lies inside hole q, or every one on it (r is q again).
    std::vector<std::unique_ptr<RingLocator>> at(rings.size());
    auto overlaps = [&](uint32_t r, uint32_t q) {
        if (!at[q]) at[q] = std::make_unique<RingLocator>(*rings[q]);
        bool off = false;
        for (const LatLng& p : samples[r]) {
            const RingLocator::Side side = at[q]->locate(p);
            if (side == RingLocator::kInside) return true;
            off |= side == RingLocator::kOutside;
        }
        return !off;
    };
    struct Box {
        double lng_lo, lng_hi, lat_lo, lat_hi;
        uint32_t ring;
    };
    std::vector<Box> boxes;
    for (uint32_t r = 1; r < rings.size(); r++) {
        Box b{INFINITY, -INFINITY, INFINITY, -INFINITY, r};
        for (const LatLng& p : *rings[r]) {
            b.lng_lo = std::min(b.lng_lo, p.second);
            b.lng_hi = std::max(b.lng_hi, p.second);
            b.lat_lo = std::min(b.lat_lo, p.first);
            b.lat_hi = std::max(b.lat_hi, p.first);
        }
        boxes.push_back(b);
    }
    std::sort(boxes.begin(), boxes.end(), [](const Box& x, const Box& y) {
        return x.lng_lo != y.lng_lo ? x.lng_lo < y.lng_lo : x.ring < y.ring;
    });
    std::vector<uint32_t> open;
    for (uint32_t i = 0; i < boxes.size(); i++) {
        const Box& b = boxes[i];
        for (size_t j = 0; j < open.size();) {
            const Box& o = boxes[open[j]];
            if (o.lng_hi < b.lng_lo) {
                open[j] = open.back();
                open.pop_back();
                continue;
            }
            if (!(o.lat_hi < b.lat_lo || b.lat_hi < o.lat_lo) && (overlaps(b.ring, o.ring) || overlaps(o.ring, b.ring)))
                return false;
            j++;
        }
        open.push_back(i);
    }
    return true;
}

// simplify_rings simplifies rings that don't nest again with the tolerance
// halved, up to this many times.
static constexpr uint8_t kToleranceHalvings = 8;
// SimplifiedRings::halvings when no tolerance nested the rings and they were
// kept as given.
static constexpr uint8_t kKeptUnsimplified = kToleranceHalvings + 1;

// A polygon's keyhole range as simplify_rings made it.
struct SimplifiedRings {
    std::vector<LatLng> range;
    uint16_t holes = 0;  // excursions in the range
    // Halvings of the tolerance it took for the simplified rings to nest, or
    // kKeptUnsimplified.
    uint8_t halvings = 0;
    // False when the rings don't nest as given either, so no tolerance can
    // keep them nesting; the range then holds them at the smallest tolerance
    // tried (halvings is kToleranceHalvings).
    bool nests = true;
};

// The outer ring and the holes `simplify` (closed ring in, closed ring out)
// leaves: holes that collapse below a triangle are dropped, the rest turned
// against the outer ring, at most 65535 (the largest), put in a canonical
// order (by first vertex). No holes when the outer ring collapses.
template <class Simplify>
std::pair<std::vector<LatLng>, std::vector<std::vector<LatLng>>> simplified_rings(
    const std::vector<LatLng>& outer, const std::vector<std::vector<LatLng>>& holes, Simplify simplify) {
    std::pair<std::vector<LatLng>, std::vector<std::vector<LatLng>>> rings{simplify(outer), {}};
    const std::vector<LatLng>& out = rings.first;
    std::vector<std::vector<LatLng>>& kept = rings.second;
    if (holes.empty() || out.size() < kMinClosedRing) return rings;
    const bool outer_ccw = signed_area2(out) > 0;
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
    std::stable_sort(kept.begin(), kept.end(), [](const auto& a, const auto& b) { return a.front() < b.front(); });
    return rings;
}

// The keyhole range of a closed outer ring and its closed holes once each
// has been simplified on its own by `simplify` (ring, tolerance scale) ->
// ring, the scale a factor on the caller's tolerance. Rings simplified on
// their own can stop nesting (rings_nest); the polygon is then simplified
// again from its given rings at half the tolerance, up to kToleranceHalvings
// times, and failing that kept as given, so the range nests whenever the
// given rings do. Given rings that don't nest (a hole across its outer ring
// in the source, or touching rings rounded a little apart) can't be kept
// nesting by any tolerance: they're left at the smallest tolerance tried,
// each ring within tolerance / 2^kToleranceHalvings of its given self,
// rather than at full size. Each step depends on the rings alone, so the
// result is deterministic. Without holes the range is just the simplified
// outer ring.
template <class Simplify>
SimplifiedRings simplify_rings(const std::vector<LatLng>& outer, const std::vector<std::vector<LatLng>>& holes,
                               Simplify simplify) {
    SimplifiedRings result;
    double scale = 1;
    auto at_scale = [&](const std::vector<LatLng>& ring) { return simplify(ring, scale); };
    auto rings = simplified_rings(outer, holes, at_scale);
    while (!rings_nest(rings.first, rings.second)) {
        if (result.halvings == kToleranceHalvings) {
            auto given = simplified_rings(outer, holes, [](const std::vector<LatLng>& ring) { return ring; });
            result.nests = rings_nest(given.first, given.second);
            if (result.nests) {
                result.halvings = kKeptUnsimplified;
                rings = std::move(given);
            }
            break;
        }
        result.halvings++;
        scale /= 2;
        rings = simplified_rings(outer, holes, at_scale);
    }

    auto& [out, kept] = rings;
    result.range = std::move(out);
    if (kept.empty()) return result;
    std::vector<LatLng> range = encode_rings(result.range, kept);
    const auto count = static_cast<uint16_t>(kept.size());
    // The excursions must peel off again as the same rings.
    std::vector<RingSpan> spans;
    bool round_trip = split_rings(range.data(), static_cast<uint32_t>(range.size()), count, spans) &&
                      spans[0].count == result.range.size();
    for (size_t k = 0; round_trip && k < kept.size(); k++) round_trip = spans[k + 1].count == kept[k].size();
    if (!round_trip) return result;
    result.range = std::move(range);
    result.holes = count;
    return result;
}

// How simplify_rings fared over many polygons, for the build log.
struct HoleTopologyCounts {
    uint64_t halved[kToleranceHalvings + 1] = {};  // [k]: nested after k halvings, k > 0
    uint64_t unsimplified = 0;                      // kept as given
    uint64_t not_nesting = 0;                       // not nesting as given either

    void add(uint8_t halvings, bool nests) {
        if (!nests) {
            not_nesting++;
        } else if (halvings == kKeptUnsimplified) {
            unsimplified++;
        } else if (halvings > 0) {
            halved[halvings]++;
        }
    }
    // "<n> polygons simplified again (tolerance/2 a, /4 b, ...), <m> kept unsimplified, <k> not nesting as
    // given (at tolerance/<2^kToleranceHalvings>)"
    std::string summary() const {
        uint64_t again = 0;
        std::string by_halving;
        for (size_t k = 1; k <= kToleranceHalvings; k++) {
            again += halved[k];
            by_halving += (k == 1 ? "tolerance/" : ", /") + std::to_string(1u << k) + " " + std::to_string(halved[k]);
        }
        return std::to_string(again) + " polygons simplified again (" + by_halving + "), " +
               std::to_string(unsimplified) + " kept unsimplified, " + std::to_string(not_nesting) +
               " not nesting as given (at tolerance/" + std::to_string(1u << kToleranceHalvings) + ")";
    }
};
