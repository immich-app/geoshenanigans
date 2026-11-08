// The admin covering per arc (admin_tier_cells, compute_admin_index_cells):
// each arc's boundary cells found once per tier, and each polygon's merged
// from its arcs', give the cells covering every ring edge by edge gave, the
// builder's covering before the arcs' (a copy of its per-ring loop is kept
// here), on random maps of polygons that share their borders.
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <map>
#include <random>
#include <stdexcept>
#include <utility>
#include <vector>

#include "admin_arcs.h"
#include "admin_cover.h"
#include "admin_rank_config.h"
#include "cell_index.h"
#include "country_code.h"
#include "exact_pip.h"
#include "parsed_data.h"
#include "s2_helpers.h"
#include "test_framework.h"
#include "types.h"

namespace {

using Ring = std::vector<PackedLocation>;
using Entries = std::vector<std::pair<uint64_t, uint32_t>>;  // (cell, polygon | INTERIOR_FLAG)

// kAdminCellLevel for the length of one test.
struct AdminCellLevel {
    int saved = kAdminCellLevel;
    explicit AdminCellLevel(int level) { kAdminCellLevel = level; }
    ~AdminCellLevel() { kAdminCellLevel = saved; }
};

// The cells of a polygon's rings as the builder covered them before its
// arcs: every edge of every ring in turn, followed from its end of smaller
// coord_key, its first vertex joined back to its last: admin_ring_cells'
// per-ring loop kept as it was, each edge's cells admin_arc_cells' of the
// edge alone. Classified as admin_polygon_cells does.
AdminRingCells per_ring_cells(const PackedLocation* verts, const RingSpan* rings, uint32_t ring_count, int level,
                              double reach_rad) {
    if (ring_count == 0 || rings[0].count == 0) return {};
    std::vector<uint64_t> boundary;
    for (uint32_t r = 0; r < ring_count; r++) {
        const PackedLocation* v = verts + rings[r].begin;
        const uint32_t n = rings[r].count;
        if (n == 0) continue;
        for (uint32_t i = 0, j = n - 1; i < n; j = i++) {
            if (v[i] == v[j]) continue;
            if (static_cast<uint64_t>(coord_key(v[j])) < static_cast<uint64_t>(coord_key(v[i]))) {
                const PackedLocation edge[2] = {v[j], v[i]};
                admin_arc_cells(edge, 2, level, reach_rad, boundary);
            } else {
                const PackedLocation edge[2] = {v[i], v[j]};
                admin_arc_cells(edge, 2, level, reach_rad, boundary);
            }
        }
    }
    std::sort(boundary.begin(), boundary.end());
    boundary.erase(std::unique(boundary.begin(), boundary.end()), boundary.end());
    return admin_polygon_cells(boundary, verts, rings, ring_count, level);
}

// Tier t's covering of every polygon of its set ring by ring: the exact
// tier's over the store's rings, as the exact cells were found before the
// arcs, a simplified tier's over the rings tier_ring gives. Sorted.
Entries per_ring_tier(const AdminArcs& arcs, const AdminExactRings& store, size_t t) {
    const ArcTier& tier = kArcTiers[t];
    const std::vector<uint32_t>& ring_refs = tier.set == ArcSet::Full ? arcs.ring_refs : arcs.minimal.ring_refs;
    Entries out;
    for (uint32_t p = 0; p < store.polygons.size(); p++) {
        const auto& poly = store.polygons[p];
        AdminRingCells cells;
        if (tier.set == ArcSet::Full && !tier.simplifies()) {
            cells = per_ring_cells(store.vertices.data(), store.rings_of(p), poly.ring_count, kAdminCellLevel,
                                   ADMIN_COVER_REACH_RAD);
        } else {
            if (poly.ring_count == 0 || ring_refs[poly.first_ring + 1] == ring_refs[poly.first_ring]) continue;
            std::vector<PackedLocation> vertices, ring, raw;
            std::vector<StoredVertex> stored;
            std::vector<RingSpan> spans;
            for (uint32_t r = poly.first_ring; r < poly.first_ring + poly.ring_count; r++) {
                tier_ring(arcs, store, tier, arcs.tiers[t], r, ring, raw, stored);
                spans.push_back({static_cast<uint32_t>(vertices.size()), static_cast<uint32_t>(ring.size())});
                vertices.insert(vertices.end(), ring.begin(), ring.end());
            }
            cells = per_ring_cells(vertices.data(), spans.data(), poly.ring_count, kAdminCellLevel,
                                   ADMIN_COVER_REACH_RAD);
        }
        for (const auto& [cell, interior] : cells) out.push_back({cell, interior ? p | INTERIOR_FLAG : p});
    }
    std::sort(out.begin(), out.end());
    return out;
}

Entries sorted(const std::vector<CellItemPair>& pairs) {
    Entries out;
    for (const CellItemPair& e : pairs) out.push_back({e.cell_id, e.item_id});
    std::sort(out.begin(), out.end());
    return out;
}

// The admin cell index over these tiers' coverings: every (cell, polygon)
// any lists, flagged interior where each one flags it, in canonical order
// (cell_item_less: the flag is in the item).
Entries union_of(const std::vector<Entries>& tiers) {
    std::map<std::pair<uint64_t, uint32_t>, std::pair<size_t, bool>> seen;  // (listed, interior in all)
    for (const Entries& tier : tiers)
        for (const auto& [cell, item] : tier) {
            auto [it, fresh] = seen.try_emplace({cell, item & ID_MASK}, 0, true);
            it->second.first++;
            it->second.second = it->second.second && (item & INTERIOR_FLAG);
        }
    Entries out;
    for (const auto& [key, state] : seen)
        out.push_back({key.first, key.second | (state.second && state.first == tiers.size() ? INTERIOR_FLAG : 0)});
    std::sort(out.begin(), out.end());
    return out;
}

Entries table(const std::vector<CellItemPair>& pairs) {
    Entries out;
    for (const CellItemPair& e : pairs) out.push_back({e.cell_id, e.item_id});
    return out;
}

// A map's grid: rows x cols cells of `size` degrees from (lat0, lng0).
struct MapShape {
    int rows, cols;
    double lat0, lng0, size;
};

// Admin polygons on a random map over a grid (MapShape): each grid edge a
// polyline of its own between two grid corners, shared by the cells on its
// two sides, bowing off the straight line by up to 15 % of a cell (never
// past the corners' cross, so no two edges cross); polygons over blocks of
// cells at levels 2-10: every cell, 2 x 2 blocks, the west half, the whole
// grid as a country with an enclave cut out as a hole (another country), and
// an island ring of no junction.
struct Map {
    ParsedData data;
    std::vector<bool> in_minimal;  // admin_minimal_keeps
};

// The ring round the block of cells [i0, i1) x [j0, j1) of the grid edges h
// (h[i][j] from corner (i, j) to (i, j + 1)) and v (v[i][j] from corner
// (i, j) to (i + 1, j)), from a random place, either way round, closed.
Ring block_ring(std::mt19937_64& rng, int i0, int j0, int i1, int j1, const std::vector<std::vector<Ring>>& h,
                const std::vector<std::vector<Ring>>& v) {
    // Its south side east, east side north, north side west, west side south.
    Ring ring;
    auto walk = [&](const Ring& edge, bool backwards) {
        for (size_t k = 0; k < edge.size(); k++) {
            const PackedLocation p = backwards ? edge[edge.size() - 1 - k] : edge[k];
            if (ring.empty() || ring.back() != p) ring.push_back(p);
        }
    };
    for (int j = j0; j < j1; j++) walk(h[i0][j], false);
    for (int i = i0; i < i1; i++) walk(v[i][j1], false);
    for (int j = j1 - 1; j >= j0; j--) walk(h[i1][j], true);
    for (int i = i1 - 1; i >= i0; i--) walk(v[i][j0], true);
    ring.pop_back();
    std::rotate(ring.begin(), ring.begin() + static_cast<long>(rng() % ring.size()), ring.end());
    if (rng() % 2) std::reverse(ring.begin(), ring.end());
    ring.push_back(ring.front());
    return ring;
}

Map random_map(std::mt19937_64& rng, const MapShape& m) {
    std::uniform_real_distribution<double> unit(0, 1);
    auto corner = [&](int i, int j) {
        return point_e7(std::min(90.0, m.lat0 + i * m.size), std::min(180.0, m.lng0 + j * m.size));
    };
    // A grid edge from corner a to corner b, `north_south` along a meridian.
    auto edge = [&](PackedLocation a, PackedLocation b, bool north_south) {
        Ring line{a};
        const int inner = rng() % 4 == 0 ? 0 : 1 + static_cast<int>(rng() % 40);
        std::vector<double> at;
        for (int k = 0; k < inner; k++) at.push_back(0.02 + 0.96 * unit(rng));
        std::sort(at.begin(), at.end());
        const double bow = 0.15 * m.size * (2 * unit(rng) - 1), noise = 0.004 * m.size;
        for (double t : at) {
            double lat = a.lat() + t * (b.lat() - a.lat()), lng = a.lon() + t * (b.lon() - a.lon());
            const double jitter = unit(rng), grain = unit(rng);  // drawn in this order on every compiler
            const double off = std::sin(M_PI * t) * (bow * (0.7 + 0.3 * jitter) + noise * (2 * grain - 1));
            if (north_south) lng += off;
            else lat += off;
            const PackedLocation p = point_e7(std::clamp(lat, -90.0, 90.0), std::clamp(lng, -180.0, 180.0));
            if (p != line.back()) line.push_back(p);
        }
        if (b != line.back()) line.push_back(b);
        return line;
    };
    std::vector<std::vector<Ring>> h(m.rows + 1, std::vector<Ring>(m.cols)), v(m.rows, std::vector<Ring>(m.cols + 1));
    for (int i = 0; i <= m.rows; i++)
        for (int j = 0; j < m.cols; j++) h[i][j] = edge(corner(i, j), corner(i, j + 1), false);
    for (int i = 0; i < m.rows; i++)
        for (int j = 0; j <= m.cols; j++) v[i][j] = edge(corner(i, j), corner(i + 1, j), true);

    Map out;
    auto add = [&](uint8_t level, const Ring& outer, const std::vector<Ring>& holes, uint16_t country) {
        out.data.admin_exact_rings.append(outer, holes);
        AdminPolygon p{};
        p.admin_level = level;
        p.country_code = country;
        out.data.admin_polygons.push_back(p);
        out.in_minimal.push_back(admin_minimal_keeps(level, 0));
    };
    // Every cell, half of them at level 10, which admin-minimal leaves out:
    // where only those use a grid edge, the minimal set joins the arcs at
    // its ends.
    for (int i = 0; i < m.rows; i++)
        for (int j = 0; j < m.cols; j++) {
            const uint8_t level = rng() % 2 ? 10 : 8;
            add(level, block_ring(rng, i, j, i + 1, j + 1, h, v), {}, 0);
        }
    for (int i = 0; i + 2 <= m.rows; i += 2)
        for (int j = 0; j + 2 <= m.cols; j += 2) add(6, block_ring(rng, i, j, i + 2, j + 2, h, v), {}, 0);
    if (m.cols >= 2) add(4, block_ring(rng, 0, 0, m.rows, m.cols / 2, h, v), {}, 0);
    std::vector<Ring> holes;
    if (m.rows >= 3 && m.cols >= 3) {
        const int ei = 1 + static_cast<int>(rng() % (m.rows - 2)), ej = 1 + static_cast<int>(rng() % (m.cols - 2));
        const Ring enclave = block_ring(rng, ei, ej, ei + 1, ej + 1, h, v);
        holes.push_back(enclave);
        add(2, enclave, {}, pack_country_code('E', 'N'));
    }
    add(2, block_ring(rng, 0, 0, m.rows, m.cols, h, v), holes, pack_country_code('C', 'O'));
    // An island in the first cell's middle, on no grid edge.
    Ring island;
    const double clat = m.lat0 + 0.5 * m.size, clng = m.lng0 + 0.5 * m.size;
    const int n = 5 + static_cast<int>(rng() % 60);
    for (int k = 0; k < n; k++) {
        const double a = 2 * M_PI * (k + 0.5 * unit(rng)) / n, r = 0.2 * m.size * (0.4 + 0.6 * unit(rng));
        const PackedLocation p = point_e7(clat + r * std::sin(a), clng + r * std::cos(a));
        if (island.empty() || p != island.back()) island.push_back(p);
    }
    island.push_back(island.front());
    add(9, island, {}, 0);
    return out;
}

// The round's shape: grid cells from 330 m to 44 km, each a few index cells
// across, near the poles and the antimeridian too.
MapShape random_shape(std::mt19937_64& rng, int round, int& level) {
    std::uniform_real_distribution<double> unit(0, 1);
    MapShape m{};
    m.rows = 2 + static_cast<int>(rng() % 4);
    m.cols = 2 + static_cast<int>(rng() % 4);
    const int kind = round % 3;
    m.size = kind == 0 ? 0.003 : kind == 1 ? 0.03 : 0.4;
    level = (kind == 0 ? 17 : kind == 1 ? 14 : 11) + static_cast<int>(rng() % 2);
    m.lat0 = -80 + 160 * unit(rng);
    m.lng0 = -175 + 340 * unit(rng);
    if (round % 7 == 3) m.lat0 = 90 - m.rows * m.size;  // up to the pole
    if (round % 7 == 5) m.lng0 = 180 - m.cols * m.size;  // up to the antimeridian
    if (round % 7 == 6) m.lat0 = -90;                    // from the south pole
    return m;
}

}  // namespace

TEST(an_arcs_cells_are_its_edges_cells_whichever_way_it_runs) {
    std::mt19937_64 rng(1108);
    std::uniform_real_distribution<double> unit(0, 1);
    size_t cells = 0;
    for (int round = 0; round < 200; round++) {
        // A random walk: short and long steps, repeated vertices, the
        // antimeridian taken the long way round, up to the poles.
        Ring line;
        double lat = -89 + 178 * unit(rng), lng = -179 + 358 * unit(rng);
        const double step = round % 4 == 0 ? 2.0 : round % 4 == 1 ? 0.05 : 0.002;
        const int n = 1 + static_cast<int>(rng() % 30);
        for (int k = 0; k < n; k++) {
            line.push_back(point_e7(std::clamp(lat, -90.0, 90.0), std::clamp(lng, -180.0, 180.0)));
            if (rng() % 8 == 0) line.push_back(line.back());
            lat += step * (2 * unit(rng) - 1);
            lng += step * (2 * unit(rng) - 1);
            if (round % 10 == 0 && k == n / 2) lng = lng > 0 ? -179.5 : 179.5;
        }
        // The long way round at coarse levels only: 358 degrees of 600 m cells is slow.
        const int level = 6 + static_cast<int>(rng() % (round % 10 == 0 ? 5 : 9));
        std::vector<uint64_t> whole, backwards, edges;
        admin_arc_cells(line.data(), static_cast<uint32_t>(line.size()), level, ADMIN_COVER_REACH_RAD, whole);
        Ring reversed(line.rbegin(), line.rend());
        admin_arc_cells(reversed.data(), static_cast<uint32_t>(reversed.size()), level, ADMIN_COVER_REACH_RAD,
                        backwards);
        for (size_t k = 1; k < line.size(); k++)
            admin_arc_cells(line.data() + k - 1, 2, level, ADMIN_COVER_REACH_RAD, edges);
        std::sort(edges.begin(), edges.end());
        edges.erase(std::unique(edges.begin(), edges.end()), edges.end());
        CHECK(std::is_sorted(whole.begin(), whole.end()));
        CHECK(std::adjacent_find(whole.begin(), whole.end()) == whole.end());
        CHECK(whole == edges);
        CHECK(whole == backwards);
        // Appended after what `out` holds already, which stays.
        std::vector<uint64_t> after{7, 3};
        admin_arc_cells(line.data(), static_cast<uint32_t>(line.size()), level, ADMIN_COVER_REACH_RAD, after);
        CHECK(std::vector<uint64_t>(after.begin(), after.begin() + 2) == std::vector<uint64_t>({7, 3}));
        CHECK(std::vector<uint64_t>(after.begin() + 2, after.end()) == whole);
        cells += whole.size();
    }
    CHECK(cells > 2000);
    // One vertex, or one point twice, has no edge and no cells.
    std::vector<uint64_t> none;
    const Ring point{point_e7(10, 20), point_e7(10, 20)};
    admin_arc_cells(point.data(), 1, 10, ADMIN_COVER_REACH_RAD, none);
    admin_arc_cells(point.data(), 2, 10, ADMIN_COVER_REACH_RAD, none);
    CHECK(none.empty());
}

TEST(a_polygons_cells_are_refused_from_a_boundary_out_of_order) {
    const Ring ring{point_e7(10, 20), point_e7(10, 20.1), point_e7(10.1, 20.1), point_e7(10, 20)};
    const RingSpan span{0, static_cast<uint32_t>(ring.size())};
    std::vector<uint64_t> boundary;
    admin_arc_cells(ring.data(), static_cast<uint32_t>(ring.size()), 10, ADMIN_COVER_REACH_RAD, boundary);
    REQUIRE(boundary.size() >= 2);
    CHECK(!admin_polygon_cells(boundary, ring.data(), &span, 1, 10).empty());
    for (const std::vector<uint64_t>& bad : {std::vector<uint64_t>{boundary[1], boundary[0]},
                                             std::vector<uint64_t>{boundary[0], boundary[0]}}) {
        bool refused = false;
        try {
            admin_polygon_cells(bad, ring.data(), &span, 1, 10);
        } catch (const std::logic_error&) {
            refused = true;
        }
        CHECK(refused);
    }
    CHECK(admin_polygon_cells(boundary, ring.data(), &span, 0, 10).empty());
}

TEST(each_tiers_covering_per_arc_is_its_covering_per_ring) {
    std::mt19937_64 rng(20261108);
    size_t entries = 0, interior = 0, shared_refs = 0, tier_differs = 0, minimal_joins = 0;
    for (int round = 0; round < 21; round++) {
        int level = 0;
        const MapShape shape = random_shape(rng, round, level);
        const AdminCellLevel cell_level(level);
        const Map map = random_map(rng, shape);
        const AdminExactRings& store = map.data.admin_exact_rings;
        const AdminArcs arcs = build_admin_arcs(store, map.in_minimal, "", 1 + round % 3);
        shared_refs += arcs.refs.size() - arcs.arcs.size();
        minimal_joins += arcs.minimal.counts.joined_junctions;
        std::vector<Entries> per_ring(kArcTierCount);
        for (size_t t = 0; t < kArcTierCount; t++) {
            per_ring[t] = per_ring_tier(arcs, store, t);
            for (unsigned threads : {1u, 3u})
                CHECK(sorted(admin_tier_cells(arcs, store, t, threads).cells) == per_ring[t]);
            entries += per_ring[t].size();
            for (const auto& e : per_ring[t]) interior += (e.second & INTERIOR_FLAG) != 0;
        }
        tier_differs += per_ring[0] != per_ring[1];
    }
    // The maps share arcs, simplify them, join minimal arcs and hold
    // interior cells.
    CHECK(shared_refs > 600);
    CHECK(minimal_joins > 10);
    CHECK(tier_differs > 8);
    CHECK(entries > 60000);
    CHECK(interior > 30000);
}

TEST(the_admin_indexes_over_the_arcs_coverings_are_those_over_the_rings) {
    std::mt19937_64 rng(1109);
    size_t countries = 0;
    for (int round = 0; round < 14; round++) {
        int level = 0;
        const MapShape shape = random_shape(rng, round, level);
        const AdminCellLevel cell_level(level);
        Map map = random_map(rng, shape);
        const AdminExactRings& store = map.data.admin_exact_rings;
        const AdminArcs arcs = build_admin_arcs(store, map.in_minimal, "", 2);
        compute_admin_index_cells(map.data, arcs, 1 + round % 4);

        std::vector<Entries> full, minimal;
        for (size_t t = 0; t < kArcTierCount; t++)
            (kArcTiers[t].set == ArcSet::Full ? full : minimal).push_back(per_ring_tier(arcs, store, t));
        CHECK(table(map.data.admin_index_cells) == union_of(full));
        CHECK(table(map.data.admin_minimal_index_cells) == union_of(minimal));
        // The countries' share of the exact tier's, flags kept.
        Entries country_cells;
        for (const auto& e : per_ring_tier(arcs, store, 1))
            if (map.data.admin_polygons[e.second & ID_MASK].admin_level == 2) country_cells.push_back(e);
        REQUIRE(!kArcTiers[1].simplifies());
        CHECK(table(map.data.admin_country_cells) == country_cells);
        countries += country_cells.size();
    }
    CHECK(countries > 6000);
}
