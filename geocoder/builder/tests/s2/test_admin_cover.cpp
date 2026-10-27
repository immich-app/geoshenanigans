// admin_ring_cells (src/admin_cover.h): a reader looks a point up in its own
// cell only, so every point the f32 ray cast puts inside a ring must find the
// ring listed in that cell, and an interior flag must hold for every point of
// its cell.
#include "admin_cover.h"
#include "cell_index.h"
#include "geometry.h"
#include "admin_rank_config.h"
#include "s2_helpers.h"
#include <map>

#include <algorithm>
#include <cmath>
#include <functional>
#include <random>
#include <vector>

#include <s2/s1chord_angle.h>
#include <s2/s2cell.h>
#include <s2/s2cell_id.h>
#include <s2/s2latlng.h>
#include <s2/s2latlng_rect.h>
#include <s2/s2loop.h>

#include "ring_contains.h"
#include "test_framework.h"
#include "vertex_pack.h"

namespace {

constexpr int kLevel = 10;

using Cells = std::vector<std::pair<uint64_t, bool>>;

// The entry of `cell` in `cells`, or null.
const std::pair<uint64_t, bool>* cell_entry(const Cells& cells, uint64_t cell) {
    auto it = std::lower_bound(cells.begin(), cells.end(), std::make_pair(cell, false));
    return it != cells.end() && it->first == cell ? &*it : nullptr;
}

// The entry of the cell the reader looks (lat, lng) up in, or null.
const std::pair<uint64_t, bool>* own_cell_entry(const Cells& cells, double lat, double lng, int level) {
    return cell_entry(cells, S2CellId(S2LatLng::FromDegrees(lat, lng)).parent(level).id());
}

// The reader's answer: the f32 ray cast at the f32 point.
bool reader_inside(const std::vector<NodeCoord>& ring, double lat, double lng) {
    return ring_contains(ring.data(), static_cast<uint32_t>(ring.size()), static_cast<float>(lat),
                         static_cast<float>(lng));
}

// Points in the ring's bounds and a margin past them, and points within
// about 100 m of its edges.
std::vector<std::pair<double, double>> probe_points(const std::vector<NodeCoord>& ring, std::mt19937_64& rng,
                                                    size_t n) {
    double lat_lo = 90, lat_hi = -90, lng_lo = 180, lng_hi = -180;
    for (const NodeCoord& v : ring) {
        lat_lo = std::min<double>(lat_lo, v.lat), lat_hi = std::max<double>(lat_hi, v.lat);
        lng_lo = std::min<double>(lng_lo, v.lng), lng_hi = std::max<double>(lng_hi, v.lng);
    }
    std::uniform_real_distribution<double> unit(0, 1), near(-1e-3, 1e-3);
    std::vector<std::pair<double, double>> out;
    for (size_t k = 0; k < n; k++) {
        double lat, lng;
        if (k % 2 == 0) {
            lat = lat_lo - 0.05 + unit(rng) * (lat_hi - lat_lo + 0.1);
            lng = lng_lo - 0.05 + unit(rng) * (lng_hi - lng_lo + 0.1);
        } else {
            const size_t e = rng() % ring.size();
            const NodeCoord& a = ring[e];
            const NodeCoord& b = ring[(e + 1) % ring.size()];
            const double t = unit(rng);
            lat = a.lat + t * (b.lat - a.lat) + near(rng);
            lng = a.lng + t * (b.lng - a.lng) + near(rng);
        }
        out.emplace_back(std::clamp(lat, -90.0, 90.0), std::clamp(lng, -180.0, 180.0));
    }
    return out;
}

// Every probe the reader puts inside the ring is listed in its own cell, and
// none in an interior-flagged cell is outside it. Returns the inside probes.
size_t check_ring(const std::vector<NodeCoord>& ring, std::mt19937_64& rng, size_t probes, int level) {
    const Cells cells = admin_ring_cells(ring.data(), static_cast<uint32_t>(ring.size()), level);
    CHECK(std::is_sorted(cells.begin(), cells.end()));
    CHECK(std::adjacent_find(cells.begin(), cells.end(),
                             [](const auto& x, const auto& y) { return x.first == y.first; }) == cells.end());
    size_t inside = 0;
    for (const auto& [lat, lng] : probe_points(ring, rng, probes)) {
        const auto* entry = own_cell_entry(cells, lat, lng, level);
        const bool in = reader_inside(ring, lat, lng);
        if (in) {
            inside++;
            CHECK(entry != nullptr);
        }
        if (entry && entry->second) CHECK(in);
    }
    return inside;
}

std::vector<NodeCoord> ring_of(const std::vector<std::pair<double, double>>& lat_lng) {
    std::vector<NodeCoord> ring;
    for (const auto& [lat, lng] : lat_lng) ring.push_back({static_cast<float>(lat), static_cast<float>(lng)});
    return ring;
}

}  // namespace

TEST(a_point_in_the_bow_of_a_parallel_edge_is_listed_in_its_own_cell) {
    // Egypt's southern border runs 660 km along 22°N; its great circle bows
    // 3.3 km north of the parallel, past this point the ray cast puts inside.
    const std::vector<NodeCoord> ring = ring_of({{22.0, 25.0}, {22.0, 31.39888}, {24.0, 31.39888}, {24.0, 25.0}});
    const double lat = 22.0019170, lng = 27.5734270;
    REQUIRE(reader_inside(ring, lat, lng));
    std::vector<S2Point> points;
    for (const NodeCoord& v : ring) points.push_back(S2LatLng::FromDegrees(v.lat, v.lng).ToPoint());
    S2Loop loop(points);
    loop.Normalize();
    CHECK(!loop.Contains(S2LatLng::FromDegrees(lat, lng).ToPoint()));  // outside the great-circle ring

    const Cells cells = admin_ring_cells(ring.data(), static_cast<uint32_t>(ring.size()), kLevel);
    const auto* entry = own_cell_entry(cells, lat, lng, kLevel);
    REQUIRE(entry != nullptr);
    CHECK(!entry->second);  // 210 m from the edge: not interior
    // 10 km north the cell lies wholly inside.
    const auto* deep = own_cell_entry(cells, 22.5, 27.5734270, kLevel);
    REQUIRE(deep != nullptr);
    CHECK(deep->second);
}

TEST(a_cell_within_the_reader_slack_of_an_edge_is_listed) {
    // The f32 ray cast can put a point up to 6.6e-5° (types.h) outside an
    // edge inside the ring, so a cell that close is listed though no edge
    // enters it. The ring's east edge runs along a meridian, a great circle,
    // 8e-5° (8.9 m) west of the cell: past the chord tolerance (6.4 m) alone.
    const S2Cell cell(S2CellId(S2LatLng::FromDegrees(5.3, 20.7)).parent(kLevel));
    const S2LatLngRect rect = cell.GetRectBound();
    const double east = rect.lng_lo().degrees() - 8e-5;
    const double south = rect.lat_lo().degrees() - 0.01, north = rect.lat_hi().degrees() + 0.01;
    const std::vector<NodeCoord> ring = ring_of({{south, east - 0.05}, {south, east}, {north, east}, {north, east - 0.05}});
    const S1ChordAngle gap = cell.GetDistance(S2LatLng::FromDegrees(ring[1].lat, ring[1].lng).ToPoint(),
                                              S2LatLng::FromDegrees(ring[2].lat, ring[2].lng).ToPoint());
    REQUIRE(gap > S1ChordAngle::Radians(ADMIN_COVER_CHORD_TOLERANCE_RAD));

    const Cells cells = admin_ring_cells(ring.data(), static_cast<uint32_t>(ring.size()), kLevel);
    const S2LatLng centre(cell.GetCenter());
    const auto* entry = own_cell_entry(cells, centre.lat().degrees(), centre.lng().degrees(), kLevel);
    REQUIRE(entry != nullptr);
    CHECK(!entry->second);
}

TEST(admin_ring_cells_list_every_point_the_ray_cast_puts_inside) {
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

TEST(admin_ring_cells_follow_edges_near_the_poles_and_the_long_way_round) {
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

TEST(admin_ring_cells_cover_rings_decoded_past_the_antimeridian_and_the_pole) {
    // Packed on the 1e-4 grid from these minima, 180 decodes to 180.0000458
    // and 90 to 90.0000305.
    const std::vector<std::vector<std::pair<double, double>>> rings = {
        {{-18, 179.12345}, {-18, 180}, {-17, 180}, {-17, 179.12345}},
        {{89.00003, 10}, {89.00003, 11}, {90, 11}, {90, 10}},
    };
    std::mt19937_64 rng(5);
    for (const auto& lat_lng : rings) {
        const std::vector<NodeCoord> ring = ring_of(lat_lng);
        const PolygonPacking p = plan_polygon(ring.data(), ring.size());
        std::vector<uint8_t> bytes(p.bytes);
        pack_polygon(bytes.data(), p, ring.data(), ring.size());
        const std::vector<NodeCoord> decoded = unpack_polygon(bytes.data(), ring.size());
        REQUIRE(std::any_of(decoded.begin(), decoded.end(), [](const NodeCoord& v) { return v.lng > 180 || v.lat > 90; }));
        CHECK(admin_ring_bounds(decoded.data(), static_cast<uint32_t>(decoded.size())).is_valid());
        CHECK(check_ring(decoded, rng, 20000, kLevel) > 2000);
    }
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
Cells variant_cells(const std::vector<NodeCoord>& ring, uint8_t level, double scale) {
    std::vector<std::pair<double, double>> pts;
    for (auto v : ring) pts.emplace_back(v.lat, v.lng);
    if (scale > 0) pts = simplify_polygon_epsilon(pts, meters_to_degrees(admin_epsilon_meters(level) * scale, pts[0].first));
    std::vector<uint8_t> bytes(plan_polygon(pts.data(), pts.size()).bytes);
    pack_polygon_at(bytes.data(), pts.data(), pts.size());
    auto decoded = unpack_polygon(bytes.data(), pts.size());
    return admin_ring_cells(decoded.data(), decoded.size(), kAdminCellLevel);
}

Cells indexed_cells(const std::vector<CellItemPair>& pairs) {
    Cells cells;
    for (const auto& pair : pairs) {
        CHECK_EQ(pair.item_id & ~INTERIOR_FLAG, 0u);
        cells.emplace_back(pair.cell_id, (pair.item_id & INTERIOR_FLAG) != 0);
    }
    return cells;
}

// Whether `a` lists a cell `b` does not.
bool lists_more(const Cells& a, const Cells& b) {
    return std::any_of(a.begin(), a.end(), [&](const auto& entry) { return !cell_entry(b, entry.first); });
}

// Whether a cell both list is interior in only one.
bool interior_differs(const Cells& a, const Cells& b) {
    return std::any_of(a.begin(), a.end(), [&](const auto& entry) {
        const auto* other = cell_entry(b, entry.first);
        return other && other->second != entry.second;
    });
}

// Every cell either variant lists, interior where both are.
Cells shared_covering(const Cells& a, const Cells& b) {
    std::map<uint64_t, int> interior_in;
    for (const Cells* variant : {&a, &b})
        for (const auto& [cell, interior] : *variant) interior_in[cell] += interior;
    Cells out;
    for (const auto& [cell, n] : interior_in) out.emplace_back(cell, n == 2);
    return out;
}

// The shared index of the level-8 polygon `ring` over q0 and q2.5, in either
// order, is the shared covering of its two variants.
void check_shared_index(const std::vector<NodeCoord>& ring, const Cells& original, const Cells& simplified) {
    const Cells want = shared_covering(original, simplified);
    ParsedData data;
    data.admin_vertices = ring;
    AdminPolygon ap{};
    ap.vertex_count = ring.size(); ap.vertex_offset = 0; ap.admin_level = 8;
    data.admin_polygons.push_back(ap);
    compute_admin_index_cells(data, {0, 2.5}, std::nullopt);
    CHECK(indexed_cells(data.admin_index_cells) == want);
    compute_admin_index_cells(data, {2.5, 0}, std::nullopt);
    CHECK(indexed_cells(data.admin_index_cells) == want);
}
}

TEST(admin_cover_shared_index_covers_both_quality_geometries) {
    // A shallow spike near an S2 edge is dropped by q2.5; uncapped must still
    // contribute the cells it reaches. Try adjacent offsets to avoid relying
    // on the particular rounding of an S2 edge's floating point coordinate.
    std::vector<NodeCoord> ring;
    Cells original, simplified;
    for (int shift = 1; shift <= 50 && !lists_more(original, simplified); shift++) {
        float edge = shift * 0.00001f;
        ring = {{edge, 0.1f}, {edge, 0.3f}, {edge - 0.0003f, 0.30001f},
                {edge, 0.30002f}, {edge, 0.5f}, {0.4f, 0.5f}, {0.4f, 0.1f}, {edge, 0.1f}};
        original = variant_cells(ring, 8, 0);
        simplified = variant_cells(ring, 8, 2.5);
    }
    REQUIRE(lists_more(original, simplified));
    check_shared_index(ring, original, simplified);
}

TEST(admin_cover_shared_index_flags_interior_only_where_both_geometries_do) {
    // A shallow notch across an S2 edge is dropped by q2.5, which leaves the
    // cell past the edge interior there; uncapped still reaches into that
    // cell, so the shared index must not flag it.
    std::vector<NodeCoord> ring;
    Cells original, simplified;
    for (int shift = 1; shift <= 50 && !interior_differs(original, simplified); shift++) {
        float edge = -shift * 0.00001f;
        ring = {{edge, 0.1f}, {edge, 0.3f}, {edge + 0.0003f, 0.30001f},
                {edge, 0.30002f}, {edge, 0.5f}, {0.4f, 0.5f}, {0.4f, 0.1f}, {edge, 0.1f}};
        original = variant_cells(ring, 8, 0);
        simplified = variant_cells(ring, 8, 2.5);
    }
    REQUIRE(interior_differs(original, simplified));
    check_shared_index(ring, original, simplified);
}

TEST(admin_cover_minimal_keeps_rank_sixteen_city_areas) {
    ParsedData data;
    data.admin_vertices = {{1.0f, 1.0f}, {1.0f, 1.2f}, {1.2f, 1.2f}, {1.2f, 1.0f}, {1.0f, 1.0f}};
    AdminPolygon ap{};
    ap.vertex_count = data.admin_vertices.size(); ap.vertex_offset = 0;
    ap.admin_level = 15; ap.place_type_override = uint8_t(AdminPlaceType::CITY);
    REQUIRE(admin_minimal_keeps(ap.admin_level, ap.place_type_override));
    data.admin_polygons.push_back(ap);
    const Cells want = variant_cells(data.admin_vertices, ap.admin_level, 2.5);
    REQUIRE(!want.empty());
    compute_admin_index_cells(data, {0}, 2.5);
    CHECK(indexed_cells(data.admin_minimal_index_cells) == want);
    data.admin_polygons[0].place_type_override = 0;
    compute_admin_index_cells(data, {0}, 2.5);
    CHECK(data.admin_minimal_index_cells.empty());
}
