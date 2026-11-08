// A polygon's admin cells (src/admin_cover.h: its edges' cells,
// admin_arc_cells, classified by admin_polygon_cells) and the admin cell
// index over the arc tiers (compute_admin_index_cells): a reader looks a
// point up in its own cell only, so every point the ray cast puts inside a
// ring must find the ring listed in that cell, and an interior flag must hold
// for every point of its cell.
#include "admin_cover.h"

#include <algorithm>
#include <cmath>
#include <functional>
#include <map>
#include <random>
#include <vector>

#include <s2/s1chord_angle.h>
#include <s2/s2cell.h>
#include <s2/s2cell_id.h>
#include <s2/s2latlng.h>
#include <s2/s2latlng_rect.h>
#include <s2/s2loop.h>

#include "admin_arcs.h"
#include "admin_rank_config.h"
#include "exact_pip.h"
#include "cell_index.h"
#include "geometry.h"
#include "s2_helpers.h"
#include "test_framework.h"

namespace {

constexpr int kLevel = 10;

// The entry of `cell` in `cells`, or null.
const std::pair<uint64_t, bool>* cell_entry(const AdminRingCells& cells, uint64_t cell) {
    auto it = std::lower_bound(cells.begin(), cells.end(), std::make_pair(cell, false));
    return it != cells.end() && it->first == cell ? &*it : nullptr;
}

// The entry of the cell the reader looks (lat, lng) up in, or null.
const std::pair<uint64_t, bool>* own_cell_entry(const AdminRingCells& cells, double lat, double lng, int level) {
    return cell_entry(cells, S2CellId(S2LatLng::FromDegrees(lat, lng)).parent(level).id());
}

using Ring = std::vector<PackedLocation>;

// The cells of a polygon's rings of verts, each joined back to its first
// vertex: the cells of each ring's edges (admin_arc_cells of the ring
// closed), classified.
AdminRingCells rings_cells(const PackedLocation* verts, const RingSpan* rings, uint32_t ring_count, int level,
                           double reach_rad) {
    std::vector<uint64_t> boundary;
    for (uint32_t r = 0; r < ring_count; r++) {
        Ring closed(verts + rings[r].begin, verts + rings[r].begin + rings[r].count);
        if (!closed.empty() && closed.back() != closed.front()) closed.push_back(closed.front());
        admin_arc_cells(closed.data(), static_cast<uint32_t>(closed.size()), level, reach_rad, boundary);
    }
    std::sort(boundary.begin(), boundary.end());
    boundary.erase(std::unique(boundary.begin(), boundary.end()), boundary.end());
    return admin_polygon_cells(boundary, verts, rings, ring_count, level);
}

// The cells of one ring at the admin cell index's reach.
AdminRingCells ring_cells(const Ring& ring, int level) {
    const RingSpan span{0, static_cast<uint32_t>(ring.size())};
    return rings_cells(ring.data(), &span, 1, level, ADMIN_COVER_REACH_RAD);
}

// Points in the ring's bounds and a margin past them, and points within
// about 100 m of its edges, on the e7 grid the reader rounds a point to.
std::vector<PackedLocation> probe_points(const Ring& ring, std::mt19937_64& rng, size_t n) {
    double lat_lo = 90, lat_hi = -90, lng_lo = 180, lng_hi = -180;
    for (const PackedLocation& v : ring) {
        lat_lo = std::min(lat_lo, v.lat()), lat_hi = std::max(lat_hi, v.lat());
        lng_lo = std::min(lng_lo, v.lon()), lng_hi = std::max(lng_hi, v.lon());
    }
    std::uniform_real_distribution<double> unit(0, 1), near(-1e-3, 1e-3);
    std::vector<PackedLocation> out;
    for (size_t k = 0; k < n; k++) {
        double lat, lng;
        if (k % 2 == 0) {
            lat = lat_lo - 0.05 + unit(rng) * (lat_hi - lat_lo + 0.1);
            lng = lng_lo - 0.05 + unit(rng) * (lng_hi - lng_lo + 0.1);
        } else {
            const size_t e = rng() % ring.size();
            const PackedLocation& a = ring[e];
            const PackedLocation& b = ring[(e + 1) % ring.size()];
            const double t = unit(rng);
            lat = a.lat() + t * (b.lat() - a.lat()) + near(rng);
            lng = a.lon() + t * (b.lon() - a.lon()) + near(rng);
        }
        out.push_back(point_e7(std::clamp(lat, -90.0, 90.0), std::clamp(lng, -180.0, 180.0)));
    }
    return out;
}

// Every probe the exact ray cast puts inside the ring is listed in its own
// cell, and none in an interior-flagged cell is outside it. Returns the
// inside probes.
size_t check_ring(const Ring& ring, std::mt19937_64& rng, size_t probes, int level) {
    const AdminRingCells cells = ring_cells(ring, level);
    CHECK(std::is_sorted(cells.begin(), cells.end()));
    CHECK(std::adjacent_find(cells.begin(), cells.end(),
                             [](const auto& x, const auto& y) { return x.first == y.first; }) == cells.end());
    size_t inside = 0;
    for (const PackedLocation& p : probe_points(ring, rng, probes)) {
        const auto* entry = own_cell_entry(cells, p.lat(), p.lon(), level);
        const bool in = exact_ring_contains(ring.data(), static_cast<uint32_t>(ring.size()), p);
        if (in) {
            inside++;
            CHECK(entry != nullptr);
        }
        if (entry && entry->second) CHECK(in);
    }
    return inside;
}

Ring ring_of(const std::vector<std::pair<double, double>>& lat_lng) {
    Ring ring;
    for (const auto& [lat, lng] : lat_lng) ring.push_back(point_e7(lat, lng));
    return ring;
}

}  // namespace

TEST(a_point_in_the_bow_of_a_parallel_edge_is_listed_in_its_own_cell) {
    // Egypt's southern border runs 660 km along 22°N; its great circle bows
    // 3.3 km north of the parallel, past this point the ray cast puts inside.
    const Ring ring = ring_of({{22.0, 25.0}, {22.0, 31.39888}, {24.0, 31.39888}, {24.0, 25.0}});
    const PackedLocation p = point_e7(22.0019170, 27.5734270);
    REQUIRE(exact_ring_contains(ring.data(), static_cast<uint32_t>(ring.size()), p));
    std::vector<S2Point> points;
    for (const PackedLocation& v : ring) points.push_back(S2LatLng::FromDegrees(v.lat(), v.lon()).ToPoint());
    S2Loop loop(points);
    loop.Normalize();
    CHECK(!loop.Contains(S2LatLng::FromDegrees(p.lat(), p.lon()).ToPoint()));  // outside the great-circle ring

    const AdminRingCells cells = ring_cells(ring, kLevel);
    const auto* entry = own_cell_entry(cells, p.lat(), p.lon(), kLevel);
    REQUIRE(entry != nullptr);
    CHECK(!entry->second);  // 210 m from the edge: not interior
    // 10 km north the cell lies wholly inside.
    const auto* deep = own_cell_entry(cells, 22.5, 27.5734270, kLevel);
    REQUIRE(deep != nullptr);
    CHECK(deep->second);
}

TEST(a_rings_cells_list_every_point_the_ray_cast_puts_inside) {
    std::mt19937_64 rng(20261020);
    std::uniform_real_distribution<double> unit(0, 1);
    size_t inside = 0;
    for (int round = 0; round < 120; round++) {
        // A star around a random centre: short and long edges, some crossing
        // themselves, at every latitude.
        const double clat = -80 + 160 * unit(rng), clng = -170 + 340 * unit(rng);
        const double radius = round % 3 == 0 ? 5 * unit(rng) : 0.3 * unit(rng);
        const int n = 3 + static_cast<int>(rng() % (round % 4 == 0 ? 400 : 12));
        std::vector<std::pair<double, double>> pts;
        for (int k = 0; k < n; k++) {
            const double a = 2 * M_PI * (k + (round % 5 == 0 ? unit(rng) * 3 : 0.5 * unit(rng))) / n;
            const double r = radius * (0.2 + unit(rng));
            pts.emplace_back(std::clamp(clat + r * std::sin(a), -89.9, 89.9),
                             std::clamp(clng + r * std::cos(a) / std::cos(clat * M_PI / 180), -180.0, 180.0));
        }
        inside += check_ring(ring_of(pts), rng, 3000, round % 2 == 0 ? kLevel : 7);
    }
    CHECK(inside > 50000);
}

TEST(a_rings_cells_follow_edges_near_the_poles_and_the_long_way_round) {
    std::mt19937_64 rng(7);
    // The cap north of 85°N: its edges run along 85°N and along 90°N, a
    // single point on the sphere the ray cast spreads over every longitude.
    CHECK(check_ring(ring_of({{85, -180}, {85, -90}, {85, 0}, {85, 90}, {85, 180}, {90, 180}, {90, -180}}), rng,
                     20000, kLevel) > 5000);
    // A thin band whose 358° edges the ray cast takes the long way round,
    // through Greenwich, where S2 would join 179°E to 179°W directly.
    CHECK(check_ring(ring_of({{60, 179}, {60, -179}, {60.2, -179}, {60.2, 179}}), rng, 20000, kLevel) > 5000);
    // Antarctica's edge along 90°S and the antimeridian.
    CHECK(check_ring(ring_of({{-70, -180}, {-66, -60}, {-71, 60}, {-69, 180}, {-90, 180}, {-90, -180}}), rng,
                     20000, kLevel) > 5000);
}

TEST(a_rings_cells_cover_it_along_the_antimeridian_and_up_to_the_pole) {
    std::mt19937_64 rng(5);
    for (const Ring& ring : {ring_of({{-18, 179.12345}, {-18, 180}, {-17, 180}, {-17, 179.12345}}),
                             ring_of({{89.00003, 10}, {89.00003, 11}, {90, 11}, {90, 10}})})
        CHECK(check_ring(ring, rng, 20000, kLevel) > 2000);
}

TEST(admin_cell_routed_keeps_the_ring_of_cells_around_a_region) {
    // A region of the cells whose centre lies north of 10°N, walked south
    // along 20°E: its last cell, the first cell past it (routed by its
    // neighbour) and one 0.5° further south (not routed).
    const std::function<bool(uint64_t)> north = [](uint64_t c) {
        return S2CellId(c).ToLatLng().lat().degrees() >= 10;
    };
    auto cell_at = [](double lat) { return S2CellId(S2LatLng::FromDegrees(lat, 20)).parent(kLevel).id(); };
    double lat = 10.3;
    uint64_t last_held = cell_at(lat);
    REQUIRE(north(last_held));
    while (north(cell_at(lat))) last_held = cell_at(lat), lat -= 1e-4;
    const uint64_t first_out = cell_at(lat);
    CHECK(admin_cell_routed(last_held, north));
    CHECK(!north(first_out));
    CHECK(admin_cell_routed(first_out, north));
    CHECK(!admin_cell_routed(cell_at(lat - 0.5), north));
}

TEST(straight_edge_pieces_keep_each_chord_within_the_tolerance) {
    std::mt19937_64 rng(3);
    std::uniform_real_distribution<double> unit(0, 1);
    auto point = [](double lat, double lng) { return S2LatLng::FromDegrees(lat, lng).ToPoint(); };
    double worst = 0;
    for (int k = 0; k < 3000; k++) {
        const double lat1 = -89 + 178 * unit(rng), lng1 = -180 + 360 * unit(rng);
        const double span = k % 3 == 0 ? 20 : 2;
        const double lat2 = std::clamp(lat1 + span * (unit(rng) - 0.5), -90.0, 90.0);
        const double lng2 = lng1 + span * (unit(rng) - 0.5);
        const uint32_t pieces = straight_edge_pieces(lat1, lng1, lat2, lng2);
        for (uint32_t p = 0; p < pieces; p++) {
            const double t0 = static_cast<double>(p) / pieces, t1 = static_cast<double>(p + 1) / pieces;
            const S2Point a = point(lat1 + t0 * (lat2 - lat1), lng1 + t0 * (lng2 - lng1));
            const S2Point b = point(lat1 + t1 * (lat2 - lat1), lng1 + t1 * (lng2 - lng1));
            const S2Point normal = a.CrossProd(b);
            if (normal.Norm() < 1e-12) continue;
            for (int s = 1; s < 16; s++) {
                const double t = t0 + (t1 - t0) * s / 16;
                const S2Point g = point(lat1 + t * (lat2 - lat1), lng1 + t * (lng2 - lng1));
                worst = std::max(worst, std::asin(std::abs(g.DotProd(normal.Normalize()))));
            }
        }
    }
    CHECK(worst <= ADMIN_COVER_CHORD_TOLERANCE_RAD);
    CHECK(worst > ADMIN_COVER_CHORD_TOLERANCE_RAD / 4);  // the bound is not wildly loose
    CHECK_EQ(straight_edge_pieces(10, 20, 10.5, 20), 1u);  // a meridian is a great circle
    CHECK_EQ(straight_edge_pieces(0, 20, 0, 20.5), 1u);    // so is the equator
    CHECK_EQ(straight_edge_pieces(10, 20, 30, 20), 35u);   // 20° in pieces of at most 0.01 rad
    // 358° the long way round, not the 2° across the antimeridian.
    CHECK(straight_edge_pieces(10, 179, 10, -179) >= 625u);
}

TEST(admin_cover_union_marks_only_common_interiors) {
    AdminRingCells a{{1, true}, {3, true}, {4, false}, {5, true}};
    AdminRingCells b{{2, true}, {3, false}, {4, true}, {5, true}};
    AdminRingCells expected{{1, false}, {2, false}, {3, false}, {4, false}, {5, true}};
    CHECK(merge_admin_ring_cells(a, b) == expected);
    CHECK(merge_admin_ring_cells(b, a) == expected);
}

namespace {

// Whether `a` lists a cell `b` does not.
bool lists_more(const AdminRingCells& a, const AdminRingCells& b) {
    return std::any_of(a.begin(), a.end(), [&](const auto& entry) { return !cell_entry(b, entry.first); });
}

// Whether a cell both list is interior in only one.
bool interior_differs(const AdminRingCells& a, const AdminRingCells& b) {
    return std::any_of(a.begin(), a.end(), [&](const auto& entry) {
        const auto* other = cell_entry(b, entry.first);
        return other && other->second != entry.second;
    });
}

// One polygon's entries of an admin cell index.
AdminRingCells indexed_cells(const std::vector<CellItemPair>& pairs) {
    AdminRingCells cells;
    for (const auto& pair : pairs) {
        CHECK_EQ(pair.item_id & ~INTERIOR_FLAG, 0u);
        cells.emplace_back(pair.cell_id, (pair.item_id & INTERIOR_FLAG) != 0);
    }
    return cells;
}

// One polygon of the given ring (in degrees, closed here) and its arc
// stage, with its admin cell indexes.
struct Indexed {
    ParsedData data;
    AdminArcs arcs;
    AdminRingCells exact, simplified, minimal;
};

Indexed indexed(const std::vector<std::pair<double, double>>& lat_lng, uint8_t level, uint8_t place_type = 0) {
    Indexed out;
    std::vector<PackedLocation> ring;
    for (const auto& [lat, lng] : lat_lng) ring.push_back(point_e7(lat, lng));
    ring.push_back(ring.front());
    out.data.admin_exact_rings.append(ring, {});
    AdminPolygon p{};
    p.admin_level = level;
    p.place_type_override = place_type;
    out.data.admin_polygons.push_back(p);
    const AdminExactRings& store = out.data.admin_exact_rings;
    out.arcs = build_admin_arcs(store, {admin_minimal_keeps(level, place_type)}, "", 1);
    // Each tier's cells of its ring.
    auto cells_of = [&](size_t t) {
        std::vector<PackedLocation> stored, raw;
        std::vector<StoredVertex> scratch;
        tier_ring(out.arcs, store, kArcTiers[t], out.arcs.tiers[t], 0, stored, raw, scratch);
        if (stored.empty()) return AdminRingCells();
        const RingSpan span{0, static_cast<uint32_t>(stored.size())};
        return rings_cells(stored.data(), &span, 1, kAdminCellLevel, ADMIN_COVER_REACH_RAD);
    };
    out.simplified = cells_of(0);
    out.exact = cells_of(1);
    out.minimal = cells_of(2);
    CHECK(out.exact ==
          rings_cells(store.vertices.data(), store.rings_of(0), 1, kAdminCellLevel, ADMIN_COVER_REACH_RAD));
    compute_admin_index_cells(out.data, out.arcs, 1);
    return out;
}

// A level-8 rectangle 0.4° tall whose southern edge at `edge` degrees has a
// spike `depth` degrees deep (negative: a notch) at 0.3°E, which the
// simplified tier's 37.5 m drops.
std::vector<std::pair<double, double>> spiked(double edge, double depth) {
    return {{edge, 0.1}, {edge, 0.3}, {edge - depth, 0.30001}, {edge, 0.30002}, {edge, 0.5}, {0.4, 0.5}, {0.4, 0.1}};
}

}  // namespace

TEST(the_admin_index_covers_the_exact_and_the_simplified_rings) {
    // The spike crosses the equator, an S2 cell edge at every level, 11 m
    // south of the rectangle: only the exact rings reach the cells past it.
    const Indexed polygon = indexed(spiked(1e-4, 3e-4), 8);
    REQUIRE(lists_more(polygon.exact, polygon.simplified));
    CHECK(indexed_cells(polygon.data.admin_index_cells) == merge_admin_ring_cells(polygon.exact, polygon.simplified));
    // Admin-minimal's over its own tier alone.
    CHECK(indexed_cells(polygon.data.admin_minimal_index_cells) == polygon.minimal);
}

TEST(the_admin_index_flags_interior_only_where_every_tier_does) {
    // A notch north across the equator from 11 m south of it: the simplified
    // rectangle holds the cells past the equator whole, the exact rings
    // don't.
    const Indexed polygon = indexed(spiked(-1e-4, -3e-4), 8);
    REQUIRE(interior_differs(polygon.exact, polygon.simplified));
    CHECK(indexed_cells(polygon.data.admin_index_cells) == merge_admin_ring_cells(polygon.exact, polygon.simplified));
}

TEST(the_admin_minimal_index_keeps_rank_sixteen_city_areas_on_the_minimal_tier) {
    const std::vector<std::pair<double, double>> square = {{1.0, 1.0}, {1.0, 1.2}, {1.2, 1.2}, {1.2, 1.0}};
    REQUIRE(admin_minimal_keeps(15, uint8_t(AdminPlaceType::CITY)));
    const Indexed city = indexed(square, 15, uint8_t(AdminPlaceType::CITY));
    REQUIRE(!city.minimal.empty());
    CHECK(indexed_cells(city.data.admin_minimal_index_cells) == city.minimal);
    CHECK(indexed_cells(city.data.admin_index_cells) == merge_admin_ring_cells(city.exact, city.simplified));
    const Indexed area = indexed(square, 15);
    CHECK(area.data.admin_minimal_index_cells.empty());
    CHECK(!area.data.admin_index_cells.empty());
}

namespace {

// A polygon's exact rings in one vertex array: the outer ring, then the holes.
struct ExactPolygon {
    std::vector<PackedLocation> vertices;
    std::vector<RingSpan> rings;

    void add(std::vector<PackedLocation> ring) {
        ring.push_back(ring.front());
        rings.push_back({static_cast<uint32_t>(vertices.size()), static_cast<uint32_t>(ring.size())});
        vertices.insert(vertices.end(), ring.begin(), ring.end());
    }
    AdminRingCells cells(int level, double reach_rad) const {
        return rings_cells(vertices.data(), rings.data(), static_cast<uint32_t>(rings.size()), level, reach_rad);
    }
    // Even-odd over every ring, as the builder's links test it.
    bool contains(PackedLocation p) const {
        bool inside = false;
        for (const RingSpan& r : rings) inside ^= exact_ring_contains(vertices.data() + r.begin, r.count, p);
        return inside;
    }
};

// A star of n vertices round (lat, lng), radii between 0.3 and 1 times r.
std::vector<PackedLocation> star(std::mt19937_64& rng, double lat, double lng, double r, int n) {
    std::uniform_real_distribution<double> unit(0, 1);
    std::vector<PackedLocation> out;
    for (int k = 0; k < n; k++) {
        const double a = 2 * M_PI * (k + 0.5 * unit(rng)) / n, d = r * (0.3 + 0.7 * unit(rng));
        out.push_back(point_e7(std::clamp(lat + d * std::sin(a), -89.9, 89.9),
                               std::clamp(lng + d * std::cos(a) / std::cos(lat * M_PI / 180), -180.0, 180.0)));
    }
    return out;
}

}  // namespace

TEST(the_cells_of_exact_rings_with_holes_list_every_point_inside) {
    std::mt19937_64 rng(1103);
    std::uniform_real_distribution<double> unit(0, 1), near(-1e-3, 1e-3);
    size_t inside = 0, in_holes = 0;
    for (int round = 0; round < 60; round++) {
        const double lat = -80 + 160 * unit(rng), lng = -170 + 340 * unit(rng), r = round % 3 ? 0.3 : 3;
        ExactPolygon polygon;
        polygon.add(star(rng, lat, lng, r, 8 + static_cast<int>(rng() % 300)));
        for (int h = 0; h < 1 + round % 3; h++)
            polygon.add(star(rng, lat + 0.15 * r * (h - 1), lng + 0.15 * r * (h - 1), 0.12 * r, 6 + h));
        const int level = round % 2 ? kLevel : 7;
        const AdminRingCells cells = polygon.cells(level, ADMIN_COVER_REACH_RAD);
        CHECK(std::is_sorted(cells.begin(), cells.end()));
        for (int k = 0; k < 4000; k++) {
            PackedLocation p;
            if (k % 2) {
                p = point_e7(lat + r * (2 * unit(rng) - 1), lng + r * (2 * unit(rng) - 1) / std::cos(lat * M_PI / 180));
            } else {
                const RingSpan& ring = polygon.rings[rng() % polygon.rings.size()];
                const PackedLocation a = polygon.vertices[ring.begin + rng() % (ring.count - 1)];
                p = point_e7(a.lat() + near(rng), a.lon() + near(rng));
            }
            p = {std::clamp(p.lat_e7, -900000000, 900000000), std::clamp(p.lon_e7, -1800000000, 1800000000)};
            const auto* entry = own_cell_entry(cells, p.lat(), p.lon(), level);
            const bool in = polygon.contains(p);
            if (in) {
                inside++;
                CHECK(entry != nullptr);
            } else if (exact_ring_contains(polygon.vertices.data(), polygon.rings[0].count, p)) {
                in_holes++;
            }
            if (entry && entry->second) CHECK(in);
        }
    }
    CHECK(inside > 20000);
    CHECK(in_holes > 1000);
}

TEST(a_cell_8_m_outside_an_exact_edge_is_not_listed) {
    // A ring whose east edge runs along a meridian `gap` west of a cell: the
    // reach (6.4 m chord + 1.1 cm) lists the cell only from under 6.4 m off,
    // in the exact rings' covering and in the admin cell index over every
    // tier (an 11 m slack for an f32 reader listed it from 17.5 m).
    const S2Cell cell(S2CellId(S2LatLng::FromDegrees(5.3, 20.7)).parent(kAdminCellLevel));
    const S2LatLngRect rect = cell.GetRectBound();
    const S2LatLng centre(cell.GetCenter());
    auto ring_at = [&](double gap_deg) {
        const double east = rect.lng_lo().degrees() - gap_deg;
        const double south = rect.lat_lo().degrees() - 0.01, north = rect.lat_hi().degrees() + 0.01;
        return std::vector<std::pair<double, double>>{{south, east - 0.05}, {south, east}, {north, east}, {north, east - 0.05}};
    };
    auto listed = [&](const AdminRingCells& cells) {
        const auto* entry = own_cell_entry(cells, centre.lat().degrees(), centre.lng().degrees(), kAdminCellLevel);
        return entry != nullptr && !entry->second;
    };
    auto exact_listed = [&](double gap_deg) {
        ExactPolygon polygon;
        std::vector<PackedLocation> ring;
        for (const auto& [lat, lng] : ring_at(gap_deg)) ring.push_back(point_e7(lat, lng));
        polygon.add(ring);
        return listed(polygon.cells(kAdminCellLevel, ADMIN_COVER_REACH_RAD));
    };
    CHECK(!exact_listed(8e-5));
    CHECK(exact_listed(5e-5));
    CHECK(!listed(indexed_cells(indexed(ring_at(8e-5), 8).data.admin_index_cells)));
    CHECK(listed(indexed_cells(indexed(ring_at(5e-5), 8).data.admin_index_cells)));
}
