#include "admin_cover.h"

#include <algorithm>
#include <cmath>
#include <memory>

#include <s2/s1chord_angle.h>
#include <s2/s2cap.h>
#include <s2/s2cell.h>
#include <s2/s2cell_id.h>
#include <s2/s2latlng.h>
#include <s2/s2latlng_rect.h>

#include "ring_contains.h"

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

S2LatLngRect admin_ring_bounds(const NodeCoord* verts, uint32_t n) {
    float lat_lo = verts[0].lat, lat_hi = lat_lo, lng_lo = verts[0].lng, lng_hi = lng_lo;
    for (uint32_t k = 1; k < n; k++) {
        lat_lo = std::min(lat_lo, verts[k].lat);
        lat_hi = std::max(lat_hi, verts[k].lat);
        lng_lo = std::min(lng_lo, verts[k].lng);
        lng_hi = std::max(lng_hi, verts[k].lng);
    }
    return S2LatLngRect(S2LatLng::FromDegrees(std::max(lat_lo, -90.0f), std::max(lng_lo, -180.0f)),
                        S2LatLng::FromDegrees(std::min(lat_hi, 90.0f), std::min(lng_hi, 180.0f)));
}

std::vector<std::pair<uint64_t, bool>> admin_ring_cells(const NodeCoord* verts, uint32_t n, int level) {
    std::vector<std::pair<uint64_t, bool>> out;
    if (n == 0) return out;
    const S1ChordAngle reach = S1ChordAngle::Radians(ADMIN_COVER_REACH_RAD);
    const double sin_reach = std::sin(ADMIN_COVER_REACH_RAD);

    // Every cell within reach of an edge. A point the ray cast puts inside is
    // within ADMIN_COVER_READER_SLACK_DEG of the ring's straight edges or
    // inside them, and they lie within the chord tolerance of their pieces.
    std::vector<uint64_t> boundary;
    S2Point to = point_of(verts[n - 1].lat, verts[n - 1].lng);
    for (uint32_t i = 0, j = n - 1; i < n; j = i++) {
        const double lat1 = verts[j].lat, lng1 = verts[j].lng, lat2 = verts[i].lat, lng2 = verts[i].lng;
        if (lat1 == lat2 && lng1 == lng2) continue;
        S2Point a = to;
        to = point_of(lat2, lng2);
        const uint32_t pieces = straight_edge_pieces(lat1, lng1, lat2, lng2);
        for (uint32_t k = 1; k <= pieces; k++) {
            const double t = static_cast<double>(k) / pieces;
            const S2Point b = k == pieces ? to : point_of(lat1 + t * (lat2 - lat1), lng1 + t * (lng2 - lng1));
            add_piece_cells(a, b, level, reach, sin_reach, boundary);
            a = b;
        }
    }
    std::sort(boundary.begin(), boundary.end());
    boundary.erase(std::unique(boundary.begin(), boundary.end()), boundary.end());

    // The cells no edge comes near are wholly inside the ring or wholly
    // outside, and their centres are far enough from every edge for the ray
    // cast to tell which. The ray cast puts nothing outside the vertices'
    // bounds inside, so the cells start from those.
    const S2LatLngRect bounds = admin_ring_bounds(verts, n);
    std::unique_ptr<RingEdgeIndex> edges;
    auto contains = [&](const S2Point& p) {
        const S2LatLng ll(p);
        const float lat = static_cast<float>(ll.lat().degrees()), lng = static_cast<float>(ll.lng().degrees());
        if (n < kMinIndexedRingVertices) return ring_contains(verts, n, lat, lng);
        if (!edges) edges = std::make_unique<RingEdgeIndex>(verts, n);
        return edges->contains(lat, lng);
    };
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
    out.reserve(boundary.size() + interior.size());
    for (size_t x = 0, y = 0; x < boundary.size() || y < interior.size();) {
        if (y == interior.size() || (x < boundary.size() && boundary[x] < interior[y]))
            out.emplace_back(boundary[x++], false);
        else
            out.emplace_back(interior[y++], true);
    }
    return out;
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
