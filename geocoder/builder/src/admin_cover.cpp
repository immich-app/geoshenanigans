#include "admin_cover.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <functional>
#include <memory>
#include <stdexcept>

#include <s2/s1chord_angle.h>
#include <s2/s2cap.h>
#include <s2/s2cell.h>
#include <s2/s2cell_id.h>
#include <s2/s2latlng.h>
#include <s2/s2latlng_rect.h>

#include "exact_pip.h"
#include "geometry.h"

namespace {

constexpr double kRadPerDeg = M_PI / 180.0;

// The point-in-ring test of a cell's centre: the ring's edges indexed by
// longitude once a ring is long enough to be tested many times over.
constexpr uint32_t kMinIndexedRingVertices = 64;

S2Point point_of(double lat, double lng) { return S2LatLng::FromDegrees(lat, lng).ToPoint(); }

// Whether p lies more than the angle whose sine is sin_reach inside each of
// the cell's four edge great circles (GetEdgeRaw: inward, not unit length).
bool deep_inside(const S2Cell& cell, const S2Point& p, double sin_reach) {
    for (int k = 0; k < 4; k++) {
        const S2Point normal = cell.GetEdgeRaw(k);
        if (p.DotProd(normal) <= sin_reach * normal.Norm()) return false;
    }
    return true;
}

// Appends the level cells within `reach` of the great-circle piece ab.
void add_piece_cells(const S2Point& a, const S2Point& b, int level, S1ChordAngle reach, double sin_reach,
                     std::vector<uint64_t>& out) {
    const S2CellId own = S2CellId(a).parent(level);
    if (own == S2CellId(b).parent(level)) {
        // Along the arc, the distance to a great circle never falls below the
        // nearer end's, so deep ends keep the whole reach in this cell.
        const S2Cell cell(own);
        if (deep_inside(cell, a, sin_reach) && deep_inside(cell, b, sin_reach)) {
            out.push_back(own.id());
            return;
        }
    }
    const S2Point mid = (a + b).Normalize();
    const S1ChordAngle radius = std::max(S1ChordAngle(mid, a), S1ChordAngle(mid, b)) + reach;
    thread_local std::vector<S2CellId> pending;
    pending.clear();
    S2Cap(mid, radius).GetCellUnionBound(&pending);
    for (S2CellId& id : pending)
        if (id.level() > level) id = id.parent(level);
    while (!pending.empty()) {
        const S2CellId id = pending.back();
        pending.pop_back();
        const S2Cell cell(id);
        if ((a == b ? cell.GetDistance(a) : cell.GetDistance(a, b)) > reach) continue;
        if (id.level() == level) {
            out.push_back(id.id());
            continue;
        }
        for (S2CellId child = id.child_begin(); child != id.child_end(); child = child.next())
            pending.push_back(child);
    }
}

// Appends the cells within reach of the straight edge from (lat1, lng1) to
// (lat2, lng2), whose points are a and b, followed in straight_edge_pieces
// pieces.
void add_edge_cells(double lat1, double lng1, const S2Point& a, double lat2, double lng2, const S2Point& b,
                    int level, S1ChordAngle reach, double sin_reach, std::vector<uint64_t>& out) {
    const uint32_t pieces = straight_edge_pieces(lat1, lng1, lat2, lng2);
    S2Point from = a;
    for (uint32_t k = 1; k <= pieces; k++) {
        const double t = static_cast<double>(k) / pieces;
        const S2Point to = k == pieces ? b : point_of(lat1 + t * (lat2 - lat1), lng1 + t * (lng2 - lng1));
        add_piece_cells(from, to, level, reach, sin_reach, out);
        from = to;
    }
}

// The cells of a polygon from the cells within reach of its edges
// (`boundary`, ascending, each once): those, flagged false, and every other
// cell inside, flagged true, as contains(cell centre) says. A cell no edge
// comes near is wholly inside or wholly outside, and its centre far enough
// from every edge for the ray cast to tell which. The ray cast puts nothing
// outside the vertices' bounds inside, so the cells start from those.
template <class Contains>
AdminRingCells classify_cells(const std::vector<uint64_t>& boundary, const S2LatLngRect& bounds, int level,
                              Contains contains) {
    std::vector<uint64_t> interior;
    std::vector<S2CellId> pending;
    bounds.GetCellUnionBound(&pending);
    for (S2CellId& id : pending)
        if (id.level() > level) id = id.parent(level);
    std::sort(pending.begin(), pending.end());
    pending.erase(std::unique(pending.begin(), pending.end()), pending.end());
    while (!pending.empty()) {
        const S2CellId id = pending.back();
        pending.pop_back();
        const auto near = std::lower_bound(boundary.begin(), boundary.end(), id.range_min().id());
        if (near != boundary.end() && *near <= id.range_max().id()) {
            if (id.level() == level) continue;
            for (S2CellId child = id.child_begin(); child != id.child_end(); child = child.next())
                if (bounds.MayIntersect(S2Cell(child))) pending.push_back(child);
            continue;
        }
        if (!contains(S2Cell(id).GetCenter())) continue;
        for (S2CellId cell = id.child_begin(level); cell != id.child_end(level); cell = cell.next())
            interior.push_back(cell.id());
    }
    std::sort(interior.begin(), interior.end());

    // The two lists share no cell.
    AdminRingCells out;
    out.reserve(boundary.size() + interior.size());
    for (size_t x = 0, y = 0; x < boundary.size() || y < interior.size();) {
        if (y == interior.size() || (x < boundary.size() && boundary[x] < interior[y]))
            out.emplace_back(boundary[x++], false);
        else
            out.emplace_back(interior[y++], true);
    }
    return out;
}

}  // namespace

uint32_t straight_edge_pieces(double lat1, double lng1, double lat2, double lng2) {
    const double a = std::abs(lng2 - lng1) * kRadPerDeg;
    const double b = std::abs(lat2 - lat1) * kRadPerDeg;
    const double lo = std::min(lat1, lat2) * kRadPerDeg;
    const double hi = std::max(lat1, lat2) * kRadPerDeg;
    // The largest |sin| and |cos| of the latitude along the edge.
    const double s = std::max(std::abs(std::sin(lo)), std::abs(std::sin(hi)));
    const double c = lo <= 0 && hi >= 0 ? 1.0 : std::max(std::cos(lo), std::cos(hi));
    // On a piece t in [0, 1] -> (lat, lng) with steps B, A (radians), the unit
    // point g(t) has g'' = -(B^2 + A^2 cos^2 lat) g - 2 A B sin lat e_east
    // + A^2 sin lat cos lat e_north. Against the unit normal n of the chord's
    // great circle, f = g . n is 0 at both ends and f'' + k f = h with
    // 0 <= k <= A^2 + B^2 and |h| <= s A sqrt(4 B^2 + A^2 c^2), so
    // |f| <= max|h| / (8 - k), and max|h| falls with the square of the piece
    // count. Pieces of at most ADMIN_COVER_MAX_PIECE_RAD keep k far below 1,
    // so 7 bounds 8 - k.
    constexpr double kChordBoundDivisor = 7.0;
    const double bow = s * a * std::sqrt(4 * b * b + a * a * c * c);
    const double by_chord = std::ceil(std::sqrt(bow / (kChordBoundDivisor * ADMIN_COVER_CHORD_TOLERANCE_RAD)));
    const double by_length = std::ceil(std::max(a, b) / ADMIN_COVER_MAX_PIECE_RAD);
    return static_cast<uint32_t>(std::max({1.0, by_chord, by_length}));
}

void admin_arc_cells(const PackedLocation* v, uint32_t count, int level, double reach_rad,
                     std::vector<uint64_t>& out) {
    if (count < 2) return;
    const S1ChordAngle reach = S1ChordAngle::Radians(reach_rad);
    const double sin_reach = std::sin(reach_rad);

    // Every cell within reach of an edge: a point inside a polygon lies
    // inside its straight edges or on them, and they lie within the chord
    // tolerance of their pieces. Past that the reach covers the e7 rounding
    // of the cell centres classify_cells tests.
    const size_t first = out.size();
    S2Point prev = point_of(v[0].lat(), v[0].lon());
    for (uint32_t i = 1; i < count; i++) {
        if (v[i] == v[i - 1]) continue;
        const S2Point here = point_of(v[i].lat(), v[i].lon());
        if (static_cast<uint64_t>(coord_key(v[i - 1])) < static_cast<uint64_t>(coord_key(v[i])))
            add_edge_cells(v[i - 1].lat(), v[i - 1].lon(), prev, v[i].lat(), v[i].lon(), here, level, reach, sin_reach,
                           out);
        else
            add_edge_cells(v[i].lat(), v[i].lon(), here, v[i - 1].lat(), v[i - 1].lon(), prev, level, reach, sin_reach,
                           out);
        prev = here;
    }
    std::sort(out.begin() + static_cast<std::ptrdiff_t>(first), out.end());
    out.erase(std::unique(out.begin() + static_cast<std::ptrdiff_t>(first), out.end()), out.end());
}

AdminRingCells admin_polygon_cells(const std::vector<uint64_t>& boundary, const PackedLocation* verts,
                                   const RingSpan* rings, uint32_t ring_count, int level) {
    if (ring_count == 0 || rings[0].count == 0) return {};
    if (std::adjacent_find(boundary.begin(), boundary.end(), std::greater_equal<uint64_t>()) != boundary.end())
        throw std::logic_error("admin polygon cells: boundary cells not ascending, each once");

    int32_t lat_lo = INT32_MAX, lat_hi = INT32_MIN, lng_lo = INT32_MAX, lng_hi = INT32_MIN;
    for (uint32_t r = 0; r < ring_count; r++) {
        const PackedLocation* v = verts + rings[r].begin;
        for (uint32_t i = 0; i < rings[r].count; i++) {
            lat_lo = std::min(lat_lo, v[i].lat_e7), lat_hi = std::max(lat_hi, v[i].lat_e7);
            lng_lo = std::min(lng_lo, v[i].lon_e7), lng_hi = std::max(lng_hi, v[i].lon_e7);
        }
    }
    const S2LatLngRect bounds(
        S2LatLng::FromDegrees(std::max(lat_lo / 1e7, -90.0), std::max(lng_lo / 1e7, -180.0)),
        S2LatLng::FromDegrees(std::min(lat_hi / 1e7, 90.0), std::min(lng_hi / 1e7, 180.0)));

    std::vector<std::unique_ptr<ExactRingEdgeIndex>> edges(ring_count);
    return classify_cells(boundary, bounds, level, [&](const S2Point& p) {
        const S2LatLng ll(p);
        const PackedLocation q = point_e7(ll.lat().degrees(), ll.lng().degrees());
        bool inside = false;
        for (uint32_t r = 0; r < ring_count; r++) {
            const PackedLocation* v = verts + rings[r].begin;
            const uint32_t n = rings[r].count;
            if (n < kMinIndexedRingVertices) {
                inside ^= exact_ring_contains(v, n, q);
                continue;
            }
            if (!edges[r]) edges[r] = std::make_unique<ExactRingEdgeIndex>(v, n);
            inside ^= edges[r]->contains(q);
        }
        return inside;
    });
}

AdminRingCells admin_ring_cells(const PackedLocation* verts, const RingSpan* rings, uint32_t ring_count, int level,
                                double reach_rad) {
    if (ring_count == 0 || rings[0].count == 0) return {};
    std::vector<uint64_t> boundary;
    std::vector<PackedLocation> closed;
    for (uint32_t r = 0; r < ring_count; r++) {
        const PackedLocation* v = verts + rings[r].begin;
        if (rings[r].count == 0) continue;
        closed.assign(v, v + rings[r].count);
        closed.push_back(v[0]);
        admin_arc_cells(closed.data(), static_cast<uint32_t>(closed.size()), level, reach_rad, boundary);
    }
    std::sort(boundary.begin(), boundary.end());
    boundary.erase(std::unique(boundary.begin(), boundary.end()), boundary.end());
    return admin_polygon_cells(boundary, verts, rings, ring_count, level);
}

bool admin_cell_routed(uint64_t cell, const std::function<bool(uint64_t)>& holds) {
    if (holds(cell)) return true;
    const S2CellId id(cell);
    thread_local std::vector<S2CellId> neighbours;
    neighbours.clear();
    id.AppendAllNeighbors(id.level(), &neighbours);
    return std::any_of(neighbours.begin(), neighbours.end(), [&](S2CellId n) { return holds(n.id()); });
}

// The cells of either of two rings of one polygon (admin_ring_cells, both
// ascending), interior where interior for both.
AdminRingCells merge_admin_ring_cells(const AdminRingCells& x, const AdminRingCells& y) {
    AdminRingCells out;
    out.reserve(std::max(x.size(), y.size()));
    for (size_t i = 0, j = 0; i < x.size() || j < y.size();) {
        if (j == y.size() || (i < x.size() && x[i].first < y[j].first)) {
            out.emplace_back(x[i++].first, false);
        } else if (i == x.size() || y[j].first < x[i].first) {
            out.emplace_back(y[j++].first, false);
        } else {
            out.emplace_back(x[i].first, x[i].second && y[j].second);
            i++, j++;
        }
    }
    return out;
}
