// The admin cell index (compute_admin_index_cells) is one index over every
// tier of the full set, while each tier simplifies a polygon's holes its own
// way: the index must list the polygon in the cell of every point one of
// them puts inside, and flag a cell interior only where all of them hold it
// whole.
#include <s2/s2cell_id.h>
#include <s2/s2latlng.h>

#include <algorithm>
#include <cstdint>
#include <vector>

#include "admin_arcs.h"
#include "cell_index.h"
#include "exact_pip.h"
#include "parsed_data.h"
#include "s2_helpers.h"
#include "test_framework.h"
#include "types.h"

namespace {

using Ring = std::vector<PackedLocation>;

// kAdminCellLevel for the length of one test.
struct AdminCellLevel {
    int saved = kAdminCellLevel;
    explicit AdminCellLevel(int level) { kAdminCellLevel = level; }
    ~AdminCellLevel() { kAdminCellLevel = saved; }
};

Ring ring_of(const std::vector<std::pair<double, double>>& lat_lng) {
    Ring ring;
    for (const auto& [lat, lng] : lat_lng) ring.push_back(point_e7(lat, lng));
    ring.push_back(ring.front());
    return ring;
}

// Polygon p's rings as tier t stores them.
std::vector<Ring> stored_rings(const AdminArcs& arcs, const AdminExactRings& store, size_t t, uint32_t p) {
    std::vector<Ring> rings;
    std::vector<PackedLocation> raw;
    std::vector<StoredVertex> scratch;
    for (uint32_t r = 0; r < store.polygons[p].ring_count; r++) {
        rings.emplace_back();
        tier_ring(arcs, store, kArcTiers[t], arcs.tiers[t], store.polygons[p].first_ring + r, rings.back(), raw, scratch);
    }
    return rings;
}

bool contains(const std::vector<Ring>& rings, PackedLocation p) {
    bool inside = false;
    for (const Ring& ring : rings) inside ^= exact_ring_contains(ring.data(), static_cast<uint32_t>(ring.size()), p);
    return inside;
}

// Polygon p's entry in the index's cell of p, or null.
const CellItemPair* entry_of(const std::vector<CellItemPair>& index, PackedLocation at, uint32_t p) {
    const uint64_t cell = S2CellId(S2LatLng::FromDegrees(at.lat(), at.lon())).parent(kAdminCellLevel).id();
    const auto [first, last] = admin_cell_entries(index, cell);
    for (const CellItemPair* e = first; e != last; ++e)
        if ((e->item_id & ID_MASK) == p) return e;
    return nullptr;
}

}  // namespace

TEST(the_admin_index_lists_a_country_where_its_simplified_hole_gave_way) {
    // A country with an enclave whose north edge has a 30 m deep bulge,
    // which the simplified tier's 37.5 m straightens: there the bulge is the
    // country's. With 150 m cells, the cells along the bulge list the
    // country, not flagged interior, though the exact rings leave the
    // bulge's points to the enclave.
    const AdminCellLevel level(16);
    const Ring outer = ring_of({{10.0, 20.0}, {10.0, 20.05}, {10.075, 20.05}, {10.075, 20.0}});
    const Ring hole = ring_of({{10.05, 20.04}, {10.02, 20.04}, {10.02, 20.01}, {10.05, 20.01}, {10.05, 20.019},
                               {10.05027, 20.022}, {10.05027, 20.028}, {10.05, 20.031}});
    ParsedData data;
    data.admin_exact_rings.append(outer, {hole});
    data.admin_exact_rings.append(hole, {});
    for (uint8_t level_of : {uint8_t(2), uint8_t(8)}) {
        AdminPolygon p{};
        p.admin_level = level_of;
        data.admin_polygons.push_back(p);
    }
    const AdminExactRings& store = data.admin_exact_rings;
    const AdminArcs arcs = build_admin_arcs(store, {true, true}, "", 1);
    compute_admin_index_cells(data, arcs, admin_candidate_cells(store, 0, 2, 1), 1);

    const std::vector<Ring> simplified = stored_rings(arcs, store, 0, 0), exact = stored_rings(arcs, store, 1, 0);
    size_t bulge = 0;
    for (int i = 0; i <= 4; i++)
        for (int j = 0; j <= 10; j++) {
            const PackedLocation p = point_e7(10.05004 + 0.00004 * i, 20.0225 + 0.0005 * j);
            REQUIRE(!contains(exact, p));     // in the enclave as given
            REQUIRE(contains(simplified, p));  // the country's once simplified
            bulge++;
            const CellItemPair* entry = entry_of(data.admin_index_cells, p, 0);
            REQUIRE(entry != nullptr);
            CHECK(!(entry->item_id & INTERIOR_FLAG));
        }
    CHECK(bulge > 0);
}

TEST(the_admin_index_lists_each_polygon_wherever_a_tier_puts_a_point_in_it) {
    // A lattice of the country, its enclave and a neighbour sharing its east
    // edge, probed every 0.0007 degrees: a point a tier's rings put inside a
    // polygon finds it in its own cell, and a cell flagged interior holds
    // only points every tier puts inside.
    const AdminCellLevel level(15);
    const Ring outer = ring_of({{10.0, 20.0}, {10.0, 20.05}, {10.00013, 20.0501}, {10.075, 20.05}, {10.075, 20.0}});
    const Ring hole = ring_of({{10.05, 20.04}, {10.02, 20.04}, {10.02, 20.01}, {10.05, 20.01}, {10.05, 20.019},
                               {10.05027, 20.022}, {10.05027, 20.028}, {10.05, 20.031}});
    const Ring neighbour = ring_of({{10.0, 20.05}, {10.0, 20.09}, {10.075, 20.09}, {10.075, 20.05}, {10.00013, 20.0501}});
    ParsedData data;
    data.admin_exact_rings.append(outer, {hole});
    data.admin_exact_rings.append(hole, {});
    data.admin_exact_rings.append(neighbour, {});
    for (uint8_t level_of : {uint8_t(2), uint8_t(8), uint8_t(2)}) {
        AdminPolygon p{};
        p.admin_level = level_of;
        data.admin_polygons.push_back(p);
    }
    const AdminExactRings& store = data.admin_exact_rings;
    // The enclave is no polygon of the minimal set.
    const std::vector<bool> in_minimal = {true, false, true};
    const AdminArcs arcs = build_admin_arcs(store, in_minimal, "", 1);
    compute_admin_index_cells(data, arcs, admin_candidate_cells(store, 0, 3, 1), 1);

    size_t inside = 0, interior = 0, inside_minimal = 0;
    for (uint32_t p = 0; p < 3; p++) {
        const std::vector<Ring> tiers[] = {stored_rings(arcs, store, 0, p), stored_rings(arcs, store, 1, p)};
        // Its rings in the minimal tier: each empty for a polygon the set
        // leaves out.
        const std::vector<Ring> minimal = stored_rings(arcs, store, 2, p);
        REQUIRE(minimal.size() == store.polygons[p].ring_count);
        for (const Ring& ring : minimal) REQUIRE(ring.empty() != in_minimal[p]);
        for (double lat = 9.995; lat < 10.08; lat += 0.0007)
            for (double lng = 19.995; lng < 20.095; lng += 0.0007) {
                const PackedLocation at = point_e7(lat, lng);
                const CellItemPair* entry = entry_of(data.admin_index_cells, at, p);
                const bool in_any = contains(tiers[0], at) || contains(tiers[1], at);
                const bool in_all = contains(tiers[0], at) && contains(tiers[1], at);
                inside += in_any;
                if (in_any) CHECK(entry != nullptr);
                if (entry && (entry->item_id & INTERIOR_FLAG)) {
                    interior++;
                    CHECK(in_all);
                }
                // The minimal set's index alike over its own tier.
                const CellItemPair* small = entry_of(data.admin_minimal_index_cells, at, p);
                if (!in_minimal[p]) {
                    CHECK(small == nullptr);
                } else {
                    inside_minimal += contains(minimal, at);
                    if (contains(minimal, at)) CHECK(small != nullptr);
                    if (small && (small->item_id & INTERIOR_FLAG)) CHECK(contains(minimal, at));
                }
            }
    }
    CHECK(inside > 10000);
    CHECK(interior > 1000);
    CHECK(inside_minimal > 5000);
}
