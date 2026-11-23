// The arc stage as the water layer runs it (kWaterArcStage): rings cut at
// their member ways' ends as well as at their junctions, the crossing repair
// counting only the crossings of one polygon's own rings, no minimal set,
// and the encoding, its subsets and slots over the stage's two tiers.
#include "admin_arcs.h"

#include <algorithm>
#include <string>
#include <vector>

#include "captured_stderr.h"
#include "exact_pip.h"
#include "parsed_data.h"
#include "scratch_dir.h"
#include "test_framework.h"

namespace {

using Ring = std::vector<PackedLocation>;

PackedLocation pt(int32_t lat, int32_t lng) { return {lat, lng}; }

Ring closed(Ring ring) {
    ring.push_back(ring.front());
    return ring;
}

// A square of side 100 from (0, 0), its first two edges one way, its last
// two another.
struct TwoWays {
    Ring ring = closed({pt(0, 0), pt(0, 100), pt(100, 100), pt(100, 0)});
    std::vector<std::vector<WayRun>> runs = {{{11, 2}, {12, 2}}};
};

std::vector<Ring> arc_vertex_lists(const AdminArcs& arcs, const AdminExactRings& store) {
    std::vector<Ring> out;
    for (const Arc& a : arcs.arcs) {
        const ArcVertices av = arc_vertices(store, a.view);
        Ring v;
        for (uint32_t i = 0; i < av.size(); i++) v.push_back(av[i]);
        out.push_back(v);
    }
    return out;
}

const ArcTier& water_simplified() { return kWaterArcTiers[0]; }

}  // namespace

TEST(water_rings_are_cut_where_their_member_ways_meet) {
    const TwoWays square;
    AdminExactRings store;
    store.append(square.ring, {}, square.runs);
    const AdminArcs junctions = build_arc_topology(store, 1);
    REQUIRE(junctions.arcs.size() == 1u);
    CHECK(junctions.arcs[0].view.closed);
    const AdminArcs ends = build_arc_topology(store, 1, {}, ArcCuts::JunctionsAndWayEnds);
    CHECK_EQ(ends.counts.way_ends, size_t(2));
    CHECK_EQ(ends.counts.junctions, size_t(2));
    REQUIRE(ends.arcs.size() == 2u);
    for (const Ring& v : arc_vertex_lists(ends, store)) {
        CHECK_EQ(v.size(), size_t(3));
        CHECK(v.front() == pt(0, 0) || v.front() == pt(100, 100));
        CHECK(v.back() == pt(0, 0) || v.back() == pt(100, 100));
    }
    CHECK_EQ(ends.refs.size(), size_t(2));
}

TEST(a_ring_of_one_way_and_a_way_that_wraps_its_start_are_not_cut_there) {
    AdminExactRings store;
    // A closed way: one run.
    store.append(closed({pt(0, 0), pt(0, 100), pt(100, 100), pt(100, 0)}), {}, {{{7, 4}}});
    // A ring whose first and last runs are one way round its first vertex:
    // way 9 runs through (0, 1000), so only (1000, 1100) and (0, 1100) are
    // its ends.
    store.append(closed({pt(0, 1000), pt(0, 1100), pt(100, 1100), pt(100, 1000)}), {}, {{{9, 1}, {8, 2}, {9, 1}}});
    const AdminArcs arcs = build_arc_topology(store, 1, {}, ArcCuts::JunctionsAndWayEnds);
    CHECK_EQ(arcs.counts.way_ends, size_t(2));
    CHECK_EQ(arcs.arcs.size(), size_t(3));
    size_t closed_arcs = 0;
    for (const Arc& a : arcs.arcs) closed_arcs += a.view.closed;
    CHECK_EQ(closed_arcs, size_t(1));
}

TEST(a_way_end_cuts_every_ring_through_it_so_shared_shores_stay_one_arc) {
    // A lake's shore of two ways that meet at (100, 50), and a river area of
    // one closed way along its north side through that vertex: the river's
    // ring is cut there too, so each half of the stretch both run along is
    // one arc of both rings.
    AdminExactRings store;
    store.append(closed({pt(0, 0), pt(0, 100), pt(100, 100), pt(100, 50), pt(100, 0)}), {}, {{{21, 3}, {22, 2}}});
    store.append(closed({pt(100, 0), pt(100, 50), pt(100, 100), pt(200, 100), pt(200, 0)}), {}, {{{23, 5}}});
    auto shared_arcs = [&](ArcCuts cuts) {
        const AdminArcs arcs = build_arc_topology(store, 1, {}, cuts);
        std::vector<std::vector<uint32_t>> users(arcs.arcs.size());
        for (uint32_t r = 0; r < store.rings.size(); r++)
            for (uint32_t k = arcs.ring_refs[r]; k < arcs.ring_refs[r + 1]; k++) users[arcs.refs[k].arc].push_back(r);
        size_t shared = 0;
        for (const auto& u : users) shared += u.size() == 2;
        return shared;
    };
    CHECK_EQ(shared_arcs(ArcCuts::Junctions), size_t(1));
    CHECK_EQ(shared_arcs(ArcCuts::JunctionsAndWayEnds), size_t(2));
}

TEST(the_water_tier_repairs_crossings_of_a_polygons_own_rings_and_none_between_polygons) {
    // A shore bowing 3000 (under the 37.5 m tolerance) round an island
    // reaching 1000 past the straight line: simplified, the shore crosses it.
    const int32_t s = 100000;
    const Ring border = {pt(0, 0), pt(0, 40000), pt(3000, 45000), pt(3000, 55000), pt(0, 60000), pt(0, s)};
    Ring lake = border;
    lake.insert(lake.end(), {pt(s, s), pt(s, 0)});
    const Ring isle = closed({pt(-1000, 48000), pt(-1000, 52000), pt(2000, 52000), pt(2000, 48000)});
    // The island is a polygon of its own beside the lake (a wetland over the
    // shore). A second lake, `at` east, bows its shore out (south) round a
    // hole of its own instead: straightened, the shore leaves its hole
    // outside.
    const int32_t at = 1000000;
    const Ring lake2 = {pt(0, at), pt(0, 40000 + at), pt(-3000, 45000 + at), pt(-3000, 55000 + at), pt(0, 60000 + at),
                        pt(0, s + at), pt(s, s + at), pt(s, at)};
    const Ring hole2 = {pt(-2000, 48000 + at), pt(-2000, 52000 + at), pt(-1000, 52000 + at), pt(-1000, 48000 + at)};
    // A third lake, `2 * at` east, bows its shore out the same way round a
    // hole of its own that reaches past the straight line: straightened,
    // the shore crosses its hole.
    const int32_t at3 = 2 * at;
    const Ring lake3 = {pt(0, at3), pt(0, 40000 + at3), pt(-3000, 45000 + at3), pt(-3000, 55000 + at3), pt(0, 60000 + at3),
                        pt(0, s + at3), pt(s, s + at3), pt(s, at3)};
    const Ring hole3 = {pt(-2000, 48000 + at3), pt(-2000, 52000 + at3), pt(1000, 52000 + at3), pt(1000, 48000 + at3)};
    AdminExactRings store, own;
    store.append(closed(lake), {});
    store.append(isle, {});
    store.append(closed(lake2), {closed(hole2)});
    store.append(closed(lake3), {closed(hole3)});
    own.append(closed(lake3), {closed(hole3)});
    const AdminArcs arcs = build_arc_topology(store, 1, {}, ArcCuts::JunctionsAndWayEnds);
    const std::vector<bool> nests = exact_rings_nest(store, 1);
    const ArcTierState admin = build_arc_tier(arcs, store, kArcTiers[0], nests, 1);
    const ArcTierState water = build_arc_tier(arcs, store, water_simplified(), nests, 1);
    // The admin tier repairs the shore against the island; water leaves it,
    // and counts the crossings of the third lake's rings alone.
    CHECK(admin.counts.crossings_simplified >= 1);
    const AdminArcs own_arcs = build_arc_topology(own, 1, {}, ArcCuts::JunctionsAndWayEnds);
    const ArcTierState own_water = build_arc_tier(own_arcs, own, water_simplified(), exact_rings_nest(own, 1), 1);
    CHECK(own_water.counts.crossings_simplified >= 1);
    CHECK_EQ(water.counts.crossings_simplified, own_water.counts.crossings_simplified);
    CHECK_EQ(water.counts.crossings_left, size_t(0));
    // The second lake's own hole: its shore gets the bow back.
    CHECK(water.counts.nesting_restored >= 1);
    CHECK_EQ(water.counts.rings_nest_introduced, size_t(0));
    auto keeps = [&](const ArcTierState& state, PackedLocation p) {
        for (uint32_t a = 0; a < arcs.arcs.size(); a++) {
            std::vector<StoredVertex> stored;
            tier_vertices(arcs, store, water_simplified(), state, a, stored);
            for (const StoredVertex& v : stored)
                if (v.at == p) return true;
        }
        return false;
    };
    // The vertices of the bow: one comes back where the repair counts it.
    auto keeps_bow = [&](const ArcTierState& state, int32_t lat, int32_t shift) {
        return keeps(state, pt(lat, 45000 + shift)) || keeps(state, pt(lat, 55000 + shift));
    };
    CHECK(keeps_bow(admin, 3000, 0));
    CHECK(!keeps_bow(water, 3000, 0));
    CHECK(keeps_bow(water, -3000, at));
    CHECK(keeps_bow(water, -3000, at3));
}

TEST(the_water_stage_builds_and_encodes_its_two_tiers_with_no_minimal_set) {
    AdminExactRings store;
    const TwoWays square;
    store.append(square.ring, {}, square.runs);
    store.append(closed({pt(0, 200), pt(0, 300), pt(100, 300), pt(100, 200)}),
                 {closed({pt(20, 220), pt(20, 280), pt(80, 280), pt(80, 220)})}, {{{31, 4}}, {{32, 4}}});
    AdminArcs arcs;
    AdminArcBlocks blocks;
    std::string log;
    {
        const gctest::CapturedStderr captured;
        arcs = build_arc_stage(kWaterArcStage, store, {}, "", 2);
        blocks = encode_admin_arcs(arcs, store, 2);
        log = captured.str();
    }
    CHECK(arcs.stage == &kWaterArcStage);
    CHECK(blocks.stage == &kWaterArcStage);
    REQUIRE(arcs.tiers.size() == kWaterArcTierCount);
    CHECK(arcs.minimal.arcs.empty());
    CHECK(arcs.minimal.ring_refs.empty());
    CHECK_EQ(arcs.arcs.size(), size_t(4));
    CHECK(blocks.set(ArcSet::Minimal).ring_blocks.empty());
    for (const AdminArcPolygon& p : blocks.polygons) {
        CHECK(p.blocks[0].size > 0);
        CHECK_EQ(p.blocks[1].size, 0u);
    }
    // The exact tier stores every raw vertex.
    size_t raw = 0;
    for (const Arc& a : arcs.arcs) raw += a.view.count;
    CHECK_EQ(arcs.tiers[1].counts.stored_vertices, raw);
    CHECK(log.find("ARC_CHECK set=water junctions=") != std::string::npos);
    CHECK(log.find(" way_ends=2 ") != std::string::npos);
    CHECK(log.find("ARC_CHECK tier=water/all/simplified set=water ") != std::string::npos);
    CHECK(log.find("ARC_CHECK tier=water/all/exact set=water ") != std::string::npos);
    CHECK(log.find("set=minimal") == std::string::npos);
    CHECK(log.find("set=full") == std::string::npos);
    CHECK(log.find("  Water arcs: ") != std::string::npos);

    // A subset in slots of its own: its sidecars where the stage keeps them.
    const ScratchDir dir("water-arcs");
    AdminArcBlocks one = subset_admin_arcs(blocks, {1}, 1);
    CHECK(one.stage == &kWaterArcStage);
    CHECK(one.set(ArcSet::Minimal).ring_refs.empty());
    CHECK(!one.tiers[1].bytes.empty());
    // One of the simplified tier alone (water's major tier) copies no other
    // tier's blocks.
    const AdminArcBlocks simplified = subset_admin_arcs(blocks, {1}, 1, 1u << 0);
    CHECK(simplified.tiers[0].bytes == one.tiers[0].bytes);
    CHECK(simplified.tiers[0].at == one.tiers[0].at);
    CHECK(simplified.tiers[1].bytes.empty());
    CHECK(simplified.tiers[1].at.empty());
    {
        const gctest::CapturedStderr captured;
        assign_region_arc_slots(one, "", "oceania", 1);
        log = captured.str();
    }
    CHECK_EQ(one.set(ArcSet::Full).arcs(), size_t(2));
    CHECK(one.polygons[0].blocks[0].size > 0);
    CHECK(log.find("minimal") == std::string::npos);
    CHECK(arc_sidecar_path(dir.path(), ArcSet::Full, kWaterArcStage) ==
          dir.path() + "/water/all/exact/arc_index.osm_ids");
    CHECK(arc_sidecar_path(dir.path(), ArcSet::Full) == dir.path() + "/quality/exact/arc_index.osm_ids");
}

TEST(a_pond_within_one_step_of_the_simplified_grid_keeps_its_shore_on_the_e7_grid) {
    // A pond 30 e7 units a side, every vertex of which snaps to one point of
    // the 100 grid; a strip 30 wide, which the grid flattens to a line; and
    // a lake, whose shore the grid holds.
    const int32_t lat = 470000000, lng = 80000000;
    AdminExactRings store;
    store.append(closed({pt(lat + 10, lng + 10), pt(lat + 10, lng + 40), pt(lat + 40, lng + 40), pt(lat + 40, lng + 10)}), {});
    store.append(closed({pt(lat + 1010, lng + 10), pt(lat + 1010, lng + 5010), pt(lat + 1040, lng + 5010),
                         pt(lat + 1040, lng + 10)}),
                 {});
    store.append(closed({pt(lat + 10000, lng), pt(lat + 10000, lng + 50000), pt(lat + 60000, lng + 50000),
                         pt(lat + 60000, lng)}),
                 {});
    const AdminArcs arcs = build_arc_topology(store, 1, {}, ArcCuts::JunctionsAndWayEnds);
    const std::vector<bool> nests = exact_rings_nest(store, 1);
    const ArcTierState water = build_arc_tier(arcs, store, water_simplified(), nests, 1);
    CHECK_EQ(water.counts.degenerate_rings, size_t(0));
    CHECK_EQ(water.counts.e7_grid_arcs, size_t(2));
    // Each holds water: the pond and the lake their middles, the strip a
    // shore of raw vertices round an area.
    std::vector<PackedLocation> ring, raw;
    std::vector<StoredVertex> stored;
    tier_ring(arcs, store, water_simplified(), water, 0, ring, raw, stored);
    CHECK(exact_ring_contains(ring.data(), static_cast<uint32_t>(ring.size()), pt(lat + 25, lng + 25)));
    tier_ring(arcs, store, water_simplified(), water, 1, ring, raw, stored);
    CHECK(exact_ring_area2(ring.data(), static_cast<uint32_t>(ring.size())) != 0);
    for (const PackedLocation& v : ring) CHECK(v.lat_e7 == lat + 1010 || v.lat_e7 == lat + 1040);
    tier_ring(arcs, store, water_simplified(), water, 2, ring, raw, stored);
    CHECK(exact_ring_contains(ring.data(), static_cast<uint32_t>(ring.size()), pt(lat + 35000, lng + 25000)));
    // The lake's shore stays on the grid.
    for (const PackedLocation& v : ring) CHECK(v.lat_e7 % 100 == 0 && v.lon_e7 % 100 == 0);
    // The boundary tiers' grid is as it was.
    CHECK_EQ(build_arc_tier(arcs, store, kArcTiers[0], nests, 1).counts.degenerate_rings, size_t(2));
}

TEST(a_ring_the_ponds_shore_on_the_e7_grid_would_flatten_keeps_its_own_shore_on_the_e7_grid) {
    // A pond within one point of the 100 grid, and a triangle meeting its
    // shore at a vertex. Snapped whole the triangle holds an area; with that
    // vertex on the e7 grid, where the pond keeps it, and its others snapped,
    // its three vertices lie on one line.
    const int32_t lat = 470000000, lng = 80000000;
    AdminExactRings store;
    store.append(closed({pt(lat + 10, lng + 20), pt(lat + 10, lng + 45), pt(lat + 40, lng + 45), pt(lat + 40, lng + 20)}), {});
    store.append(closed({pt(lat + 40, lng + 20), pt(lat + 190, lng + 510), pt(lat + 110, lng + 190)}), {});
    const AdminArcs arcs = build_arc_topology(store, 1, {}, ArcCuts::JunctionsAndWayEnds);
    const std::vector<bool> nests = exact_rings_nest(store, 1);
    const ArcTierState water = build_arc_tier(arcs, store, water_simplified(), nests, 1);
    CHECK_EQ(water.counts.degenerate_rings, size_t(0));
    CHECK_EQ(water.counts.e7_collapsing_rings, size_t(2));
    std::vector<PackedLocation> ring, raw;
    std::vector<StoredVertex> stored;
    tier_ring(arcs, store, water_simplified(), water, 1, ring, raw, stored);
    CHECK(exact_ring_area2(ring.data(), static_cast<uint32_t>(ring.size())) != 0);
    for (const PackedLocation& v : ring) CHECK(v.lat_e7 % 100 != 0);
}

TEST(a_ring_the_nesting_pass_flattens_at_a_junction_it_moves_keeps_its_own_shore_on_the_e7_grid) {
    // A lake whose island the 100 grid moves across its shore, and a
    // triangle meeting the lake's shore at a vertex. Snapped whole every
    // ring holds an area; on the grid the island crosses the shore, with no
    // vertex to give back, so the nesting pass puts the lake and that vertex
    // on the e7 grid, where the triangle's three vertices lie on one line.
    const int32_t lat = 470000000, lng = 80000000;
    AdminExactRings store;
    // The lake's west shore runs along lng = lat / 2 from (40, 20); the
    // island's corner (1149, 551) is just west of it, and snaps east of it.
    store.append(closed({pt(lat + 40, lng + 20), pt(lat + 100000, lng + 50000), pt(lat + 100000, lng - 50000),
                         pt(lat, lng - 50000)}),
                 {closed({pt(lat + 1149, lng + 551), pt(lat + 1149, lng + 351), pt(lat + 1349, lng + 351),
                          pt(lat + 1349, lng + 551)})});
    store.append(closed({pt(lat + 40, lng + 20), pt(lat + 110, lng + 190), pt(lat + 190, lng + 510)}), {});
    const AdminArcs arcs = build_arc_topology(store, 1, {}, ArcCuts::JunctionsAndWayEnds);
    const std::vector<bool> nests = exact_rings_nest(store, 1);
    REQUIRE(nests[0] && nests[1]);
    const ArcTierState water = build_arc_tier(arcs, store, water_simplified(), nests, 1);
    CHECK(water.counts.e7_grid_rounds >= 1);
    CHECK_EQ(water.counts.rings_nest_introduced, size_t(0));
    CHECK_EQ(water.counts.e7_collapsing_rings, size_t(1));
    std::vector<PackedLocation> ring, raw;
    std::vector<StoredVertex> stored;
    // The lake's shore holds its raw vertex where the two meet.
    tier_ring(arcs, store, water_simplified(), water, 0, ring, raw, stored);
    CHECK(std::find(ring.begin(), ring.end(), pt(lat + 40, lng + 20)) != ring.end());
    // The triangle holds an area, on its raw vertices.
    tier_ring(arcs, store, water_simplified(), water, 2, ring, raw, stored);
    CHECK(exact_ring_area2(ring.data(), static_cast<uint32_t>(ring.size())) != 0);
    for (const PackedLocation& v : ring) CHECK(v.lat_e7 % 100 != 0);
}
