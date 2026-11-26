// How two admin polygons lie, on their exact e7 rings, in integers: PostGIS's
// ST_Contains and ST_Equals as placex_update asks them of its boundaries and
// place areas (rank_address.h). A polygon is an outer ring and its holes, as
// AdminExactRings holds them; a multipolygon is a list of those.
//
// Containment of polygon b in polygon c is decided on their boundaries. Where
// no edge of b properly crosses an edge of c, every edge of either is cut, at
// the vertices of the other lying on it, into pieces each inside the other
// polygon, outside it, or on its border: b lies in c when every piece of b's
// edges is in c or on its border and no piece of c's edges is strictly inside
// b, and some piece of b's (else a point inside b) is strictly inside c. The
// middle of a piece lies on the half e7 grid, so the point tests take doubled
// coordinates, whose products need 128 bits.
#pragma once

#include <algorithm>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

#include "edge_grid.h"
#include "exact_pip.h"
#include "polygon_rings.h"
#include "ring_edge_buckets.h"

// A polygon of exact rings: the outer ring, then its holes, each closed (its
// last vertex its first).
struct RingPolygon {
    const PackedLocation* v = nullptr;
    const RingSpan* rings = nullptr;
    uint32_t ring_count = 0;
};

// A point on the half e7 grid, as twice its e7 coordinates.
struct HalfPoint {
    int64_t lat2, lng2;
};

inline HalfPoint half_point(PackedLocation p) { return {2 * int64_t(p.lat_e7), 2 * int64_t(p.lon_e7)}; }

// The middle of the segment a-b.
inline HalfPoint midpoint(PackedLocation a, PackedLocation b) {
    return {int64_t(a.lat_e7) + b.lat_e7, int64_t(a.lon_e7) + b.lon_e7};
}

// The cross product of (b - a) and (p - a), in doubled units: its sign tells
// which side of the line a-b p lies on, 0 on it.
inline __int128 half_cross(PackedLocation a, PackedLocation b, HalfPoint p) {
    return static_cast<__int128>(int64_t(b.lon_e7) - a.lon_e7) * (p.lat2 - 2 * int64_t(a.lat_e7)) -
           static_cast<__int128>(int64_t(b.lat_e7) - a.lat_e7) * (p.lng2 - 2 * int64_t(a.lon_e7));
}

inline int sign_of(__int128 v) { return (v > 0) - (v < 0); }

// Whether p lies on the segment a-b, its ends included.
inline bool on_edge_half(PackedLocation a, PackedLocation b, HalfPoint p) {
    return half_cross(a, b, p) == 0 && p.lat2 >= 2 * int64_t(std::min(a.lat_e7, b.lat_e7)) &&
           p.lat2 <= 2 * int64_t(std::max(a.lat_e7, b.lat_e7)) && p.lng2 >= 2 * int64_t(std::min(a.lon_e7, b.lon_e7)) &&
           p.lng2 <= 2 * int64_t(std::max(a.lon_e7, b.lon_e7));
}

// exact_edge_crosses for a point on the half grid: whether the edge a-b
// crosses the ray from p towards +lat, nothing where p is on it.
inline bool edge_crosses_half(PackedLocation a, PackedLocation b, HalfPoint p) {
    if ((2 * int64_t(a.lon_e7) > p.lng2) == (2 * int64_t(b.lon_e7) > p.lng2)) return false;
    const __int128 lhs = static_cast<__int128>(p.lat2 - 2 * int64_t(a.lat_e7)) * (int64_t(b.lon_e7) - a.lon_e7);
    const __int128 rhs = static_cast<__int128>(int64_t(b.lat_e7) - a.lat_e7) * (p.lng2 - 2 * int64_t(a.lon_e7));
    return lhs != rhs && ((lhs < rhs) != (b.lon_e7 < a.lon_e7));
}

// Whether the segments a-b and p-q cross at a point inside both: each one's
// ends strictly either side of the other's line.
inline bool edges_cross_properly(PackedLocation a, PackedLocation b, PackedLocation p, PackedLocation q) {
    return sign_of(half_cross(a, b, half_point(p))) * sign_of(half_cross(a, b, half_point(q))) < 0 &&
           sign_of(half_cross(p, q, half_point(a))) * sign_of(half_cross(p, q, half_point(b))) < 0;
}

// Whether x lies on the segment a-b strictly between its ends.
inline bool inside_edge(PackedLocation x, PackedLocation a, PackedLocation b) {
    return x != a && x != b && on_edge_half(a, b, half_point(x));
}

enum class Where : uint8_t { OUTSIDE, BOUNDARY, INSIDE };

// A polygon's rings indexed for locating points and, unless built for points
// alone, for finding its edges near a box. Holds pointers into the rings,
// which must outlive it.
class PolygonIndex {
public:
    struct Edge {
        PackedLocation a, b;
    };

    // `edges_near`: whether for_each_edge_near is asked of it, which the edge
    // grid is built for.
    explicit PolygonIndex(RingPolygon polygon, bool edges_near = true) : polygon_(polygon), edges_near_(edges_near) {
        for (uint32_t r = 0; r < polygon.ring_count; r++) {
            const PackedLocation* v = polygon.v + polygon.rings[r].begin;
            const uint32_t n = polygon.rings[r].count;
            ring_boxes_.push_back(exact_ring_box(v, n));
            buckets_.push_back(n >= kMinBucketedRing
                                   ? std::make_unique<RingEdgeBuckets>(n, std::min<uint32_t>(n / 16, 1u << 20),
                                                                       [v](uint32_t k) { return v[k].lon_e7; })
                                   : nullptr);
            for (uint32_t k = 1; k < n; k++)
                if (v[k - 1] != v[k]) edges_.push_back({v[k - 1], v[k]});
        }
        box_ = ring_boxes_.empty() ? ExactRingBox{1, 0, 1, 0} : ring_boxes_[0];
        if (edges_near)
            grid_ = EdgeGrid(box_, static_cast<uint32_t>(edges_.size()), [&](uint32_t k) { return edge_box(edges_[k]); });
    }

    RingPolygon polygon() const { return polygon_; }
    // The outer ring's box, which holds the polygon.
    const ExactRingBox& box() const { return box_; }
    const std::vector<Edge>& edges() const { return edges_; }

    static ExactRingBox edge_box(const Edge& e) {
        return {std::min(e.a.lat_e7, e.b.lat_e7), std::max(e.a.lat_e7, e.b.lat_e7), std::min(e.a.lon_e7, e.b.lon_e7),
                std::max(e.a.lon_e7, e.b.lon_e7)};
    }

    // Where p lies: on an edge, else inside an odd number of rings or not.
    Where locate(HalfPoint p) const {
        bool inside = false;
        for (uint32_t r = 0; r < polygon_.ring_count; r++) {
            const ExactRingBox& b = ring_boxes_[r];
            // Outside the ring's closed box the ring adds no crossing and has no edge at p.
            if (p.lat2 < 2 * int64_t(b.lat_lo) || p.lat2 > 2 * int64_t(b.lat_hi) || p.lng2 < 2 * int64_t(b.lng_lo) ||
                p.lng2 > 2 * int64_t(b.lng_hi))
                continue;
            const PackedLocation* v = polygon_.v + polygon_.rings[r].begin;
            const uint32_t n = polygon_.rings[r].count;
            bool on = false;
            auto crosses = [&](uint32_t a, uint32_t b) {
                on |= on_edge_half(v[b], v[a], p);
                return edge_crosses_half(v[b], v[a], p);
            };
            bool odd = false;
            if (buckets_[r]) {
                odd = buckets_[r]->odd_crossings(static_cast<double>(p.lng2) / 2, crosses);
            } else {
                for (uint32_t a = 0, b = n - 1; a < n; b = a++)
                    if (crosses(a, b)) odd = !odd;
            }
            if (on) return Where::BOUNDARY;
            inside ^= odd;
        }
        return inside ? Where::INSIDE : Where::OUTSIDE;
    }

    // Calls visit(k) once for each edge k whose box may meet `box`; an index
    // built for points alone refuses.
    template <class Visit>
    void for_each_edge_near(const ExactRingBox& box, std::vector<uint32_t>& scratch, Visit visit) const {
        if (!edges_near_) throw std::logic_error("edges near a box of a polygon indexed for points alone");
        grid_.for_each_meeting(box, scratch, visit);
    }

private:
    // Shorter rings are scanned whole.
    static constexpr uint32_t kMinBucketedRing = 64;

    RingPolygon polygon_;
    ExactRingBox box_;
    std::vector<ExactRingBox> ring_boxes_;
    std::vector<std::unique_ptr<RingEdgeBuckets>> buckets_;  // by ring, null for a short one
    std::vector<Edge> edges_;                                // every ring's, none of no length
    bool edges_near_;
    EdgeGrid grid_;
};

inline bool box_holds(const ExactRingBox& outer, const ExactRingBox& inner) {
    return outer.lat_lo <= inner.lat_lo && inner.lat_hi <= outer.lat_hi && outer.lng_lo <= inner.lng_lo &&
           inner.lng_hi <= outer.lng_hi;
}

namespace polygon_relations_detail {

// Calls piece(middle) for each piece of the edge a-b cut at `cuts`, points
// strictly inside it (any order, repeats allowed; sorted here); stops at the
// first piece for which it returns false, and returns whether none did.
template <class Piece>
bool for_each_piece(PackedLocation a, PackedLocation b, std::vector<PackedLocation>& cuts, Piece piece) {
    if (cuts.empty()) return piece(midpoint(a, b));
    // Along a segment both distances from a only grow.
    auto along = [&](PackedLocation p) {
        return std::abs(int64_t(p.lat_e7) - a.lat_e7) + std::abs(int64_t(p.lon_e7) - a.lon_e7);
    };
    std::sort(cuts.begin(), cuts.end(), [&](PackedLocation x, PackedLocation y) { return along(x) < along(y); });
    cuts.erase(std::unique(cuts.begin(), cuts.end()), cuts.end());
    PackedLocation from = a;
    for (const PackedLocation& to : cuts) {
        if (!piece(midpoint(from, to))) return false;
        from = to;
    }
    return piece(midpoint(from, b));
}

}  // namespace polygon_relations_detail

// ST_Contains(c, b) for two polygons: b lies in c, its border included, and
// their insides meet. True of a polygon and itself.
inline bool polygon_contains(const PolygonIndex& c, const PolygonIndex& b) {
    using polygon_relations_detail::for_each_piece;
    if (b.edges().empty() || !box_holds(c.box(), b.box())) return false;
    std::vector<uint32_t> scratch;
    std::vector<PackedLocation> cuts;
    // b's vertices inside c's edges, by c's edge.
    std::vector<std::pair<uint32_t, PackedLocation>> c_cuts;
    bool crossed = false, inside_seen = false;
    for (const PolygonIndex::Edge& e : b.edges()) {
        cuts.clear();
        c.for_each_edge_near(PolygonIndex::edge_box(e), scratch, [&](uint32_t k) {
            const PolygonIndex::Edge& f = c.edges()[k];
            if (edges_cross_properly(e.a, e.b, f.a, f.b)) crossed = true;
            if (inside_edge(f.a, e.a, e.b)) cuts.push_back(f.a);
            if (inside_edge(f.b, e.a, e.b)) cuts.push_back(f.b);
            if (inside_edge(e.a, f.a, f.b)) c_cuts.push_back({k, e.a});
            if (inside_edge(e.b, f.a, f.b)) c_cuts.push_back({k, e.b});
        });
        if (crossed) return false;
        const bool in = for_each_piece(e.a, e.b, cuts, [&](HalfPoint m) {
            const Where w = c.locate(m);
            inside_seen |= w == Where::INSIDE;
            return w != Where::OUTSIDE;
        });
        if (!in) return false;
    }
    // c's border may not run inside b: a hole of c in b, or c's edges cutting a corner of b.
    std::sort(c_cuts.begin(), c_cuts.end(), [](const auto& x, const auto& y) { return x.first < y.first; });
    bool clear = true;
    std::vector<uint32_t> near;
    c.for_each_edge_near(b.box(), scratch, [&](uint32_t k) { near.push_back(k); });
    for (uint32_t k : near) {
        const PolygonIndex::Edge& f = c.edges()[k];
        if (!EdgeGrid::meets(PolygonIndex::edge_box(f), b.box())) continue;
        cuts.clear();
        auto at = std::lower_bound(c_cuts.begin(), c_cuts.end(), k, [](const auto& x, uint32_t key) { return x.first < key; });
        for (; at != c_cuts.end() && at->first == k; ++at) cuts.push_back(at->second);
        clear = for_each_piece(f.a, f.b, cuts, [&](HalfPoint m) { return b.locate(m) != Where::INSIDE; });
        if (!clear) return false;
    }
    if (inside_seen) return true;
    // b's border lies on c's: a point inside b tells whether b fills c or one of its holes.
    const RingPolygon bp = b.polygon();
    const LatLng in_b = interior_point(bp.v, bp.rings, bp.ring_count);
    const HalfPoint p = half_point(point_e7(in_b.first, in_b.second));
    return b.locate(p) == Where::INSIDE && c.locate(p) == Where::INSIDE;
}

// ST_Contains(c, b) for multipolygons, each a list of its polygons: every
// polygon of b lies in one of c's.
inline bool multipolygon_contains(const std::vector<const PolygonIndex*>& c, const std::vector<const PolygonIndex*>& b) {
    if (b.empty()) return false;
    for (const PolygonIndex* part : b) {
        bool held = false;
        for (const PolygonIndex* whole : c) {
            if (box_holds(whole->box(), part->box()) && polygon_contains(*whole, *part)) {
                held = true;
                break;
            }
        }
        if (!held) return false;
    }
    return true;
}

// Whether two closed rings are the same, whatever vertex each starts at and
// whichever way each runs.
inline bool rings_equal(const PackedLocation* a, uint32_t na, const PackedLocation* b, uint32_t nb) {
    if (na != nb) return false;
    if (na < kMinClosedRing) return std::equal(a, a + na, b);
    const uint32_t m = na - 1;  // the distinct vertices, the closing one left out
    for (uint32_t j = 0; j < m; j++) {
        if (b[j] != a[0]) continue;
        bool forward = true, backward = true;
        for (uint32_t k = 1; k < m && (forward || backward); k++) {
            forward = forward && a[k] == b[(j + k) % m];
            backward = backward && a[k] == b[(j + m - k) % m];
        }
        if (forward || backward) return true;
    }
    return false;
}

// Whether b lies on the segment from a to c, past a and before c: a vertex
// there adds no point to an outline (exact, in e7 integers).
inline bool straight_between(const PackedLocation& a, const PackedLocation& b, const PackedLocation& c) {
    const __int128 ux = int64_t(b.lon_e7) - a.lon_e7, uy = int64_t(b.lat_e7) - a.lat_e7;
    const __int128 vx = int64_t(c.lon_e7) - b.lon_e7, vy = int64_t(c.lat_e7) - b.lat_e7;
    return ux * vy == uy * vx && ux * vx + uy * vy > 0;
}

// The outline of the closed ring v (n vertices) as a closed ring, into `out`:
// one of each run of equal vertices, none that lies straight between the
// vertices either side of it, and the first again last.
inline void ring_outline(const PackedLocation* v, uint32_t n, std::vector<PackedLocation>& out) {
    out.clear();
    const uint32_t m = n > 1 && v[0] == v[n - 1] ? n - 1 : n;
    for (uint32_t k = 0; k < m; k++) {
        if (!out.empty() && out.back() == v[k]) continue;
        while (out.size() >= 2 && straight_between(out[out.size() - 2], out.back(), v[k])) out.pop_back();
        out.push_back(v[k]);
    }
    // Round the end, where the cycle closes.
    for (bool changed = true; changed;) {
        changed = false;
        if (out.size() >= 2 && out.back() == out.front()) {
            out.pop_back();
            changed = true;
        } else if (out.size() >= 3 && straight_between(out[out.size() - 2], out.back(), out.front())) {
            out.pop_back();
            changed = true;
        } else if (out.size() >= 3 && straight_between(out.back(), out.front(), out[1])) {
            out.erase(out.begin());
            changed = true;
        }
    }
    if (!out.empty()) out.push_back(out.front());
}

// ST_Equals as far as the rings tell it: the two multipolygons have the same
// rings, each taken as its outline (ring_outline): a vertex along a straight
// side, or a run of equal ones, adds no point, as PostGIS finds the same
// point set either way.
inline bool polygons_equal(const std::vector<RingPolygon>& a, const std::vector<RingPolygon>& b) {
    struct Ring {
        std::vector<PackedLocation> v;
        PackedLocation least;
    };
    auto rings_of = [](const std::vector<RingPolygon>& polygons) {
        std::vector<Ring> out;
        for (const RingPolygon& p : polygons)
            for (uint32_t r = 0; r < p.ring_count; r++) {
                Ring ring;
                ring_outline(p.v + p.rings[r].begin, p.rings[r].count, ring.v);
                ring.least = ring.v.empty() ? PackedLocation{0, 0} : *std::min_element(ring.v.begin(), ring.v.end());
                out.push_back(std::move(ring));
            }
        auto key_less = [](const Ring& x, const Ring& y) {
            return x.v.size() != y.v.size() ? x.v.size() < y.v.size() : x.least < y.least;
        };
        std::sort(out.begin(), out.end(), key_less);
        return out;
    };
    auto key_equal = [](const Ring& x, const Ring& y) { return x.v.size() == y.v.size() && x.least == y.least; };
    const std::vector<Ring> ra = rings_of(a), rb = rings_of(b);
    if (ra.size() != rb.size()) return false;
    std::vector<bool> used(rb.size());
    for (size_t i = 0; i < ra.size(); i++) {
        bool matched = false;
        // The rings of b with ring i's key are a run round place i.
        size_t j = i;
        while (j > 0 && key_equal(rb[j - 1], ra[i])) j--;
        for (; j < rb.size() && key_equal(rb[j], ra[i]); j++) {
            if (used[j] || !rings_equal(ra[i].v.data(), static_cast<uint32_t>(ra[i].v.size()), rb[j].v.data(),
                                        static_cast<uint32_t>(rb[j].v.size())))
                continue;
            used[j] = matched = true;
            break;
        }
        if (!matched) return false;
    }
    return true;
}
