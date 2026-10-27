// Unit tests for assemble_outer_rings (ring_assembly.cpp).
//
// Ring assembly stitches relation member ways into closed outer rings using
// coordinate-keyed endpoint matching (split at shared internal nodes, then
// greedy extension with a backtracking retry pass). It is geometry-critical:
// it decides which polygons admin/area boundaries become, so these tests lock
// in the CURRENT behaviour (single closed way, two-way stitch, role filtering,
// open/unclosable -> no ring) as a regression net for a refactor.
#include "ring_assembly.h"

#include <cmath>
#include <string>
#include <utility>
#include <vector>

#include "polygon_rings.h"
#include "test_framework.h"

namespace {

using Coord = std::pair<double, double>;
using Ring = std::vector<Coord>;

// Adds a way from a coordinate list. The node-id fields are not used by
// assemble_outer_rings (it works purely on coordinates), so any value is
// fine; we set them deterministically anyway.
void add_way(WayGeometries& geoms, int64_t way_id, const std::vector<Coord>& coords) {
    geoms.ways[way_id] = {geoms.coords.size(), static_cast<uint32_t>(coords.size()), 0, 0};
    geoms.coords.insert(geoms.coords.end(), coords.begin(), coords.end());
}

// Are two coordinates equal to within float tolerance?
bool coord_eq(const Coord& a, const Coord& b) {
    return std::fabs(a.first - b.first) < 1e-9 &&
           std::fabs(a.second - b.second) < 1e-9;
}

bool ring_is_closed(const Ring& r) {
    return r.size() >= 4 && coord_eq(r.front(), r.back());
}

}  // namespace

// A single already-closed way (a unit square, first==last) is emitted as one
// ring in the greedy pass, unchanged.
TEST(ring_assembly_single_closed_way) {
    WayGeometries geoms;
    add_way(geoms, 1, {{0.0, 0.0}, {0.0, 1.0}, {1.0, 1.0}, {1.0, 0.0}, {0.0, 0.0}});

    std::vector<std::pair<int64_t, std::string>> members = {{1, "outer"}};
    auto rings = assemble_outer_rings(members, geoms);

    REQUIRE(rings.size() == size_t(1));
    CHECK(ring_is_closed(rings[0]));
    // The ring is the way's coordinates verbatim (5 points, closed).
    CHECK_EQ(rings[0].size(), size_t(5));
    CHECK(coord_eq(rings[0].front(), Coord(0.0, 0.0)));
}

// An empty role string is treated like "outer" (the filter only skips named
// non-outer roles), so an empty-role closed way still produces a ring.
TEST(ring_assembly_empty_role_is_outer) {
    WayGeometries geoms;
    add_way(geoms, 1, {{0.0, 0.0}, {0.0, 1.0}, {1.0, 1.0}, {1.0, 0.0}, {0.0, 0.0}});

    std::vector<std::pair<int64_t, std::string>> members = {{1, ""}};
    auto rings = assemble_outer_rings(members, geoms);

    REQUIRE(rings.size() == size_t(1));
    CHECK(ring_is_closed(rings[0]));
}

// Two open ways sharing both endpoints stitch into one closed ring.
//   way 1: bottom + right edges  (0,0) -> (1,0) -> (1,1)
//   way 2: top + left edges      (1,1) -> (0,1) -> (0,0)
// Stitched they form a closed square.
TEST(ring_assembly_two_ways_stitch) {
    WayGeometries geoms;
    add_way(geoms, 1, {{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0}});
    add_way(geoms, 2, {{1.0, 1.0}, {0.0, 1.0}, {0.0, 0.0}});

    std::vector<std::pair<int64_t, std::string>> members = {{1, "outer"}, {2, "outer"}};
    auto rings = assemble_outer_rings(members, geoms);

    REQUIRE(rings.size() == size_t(1));
    CHECK(ring_is_closed(rings[0]));
    // 3 + 3 coords stitched at the shared endpoint -> 5 distinct points,
    // closed back to start.
    CHECK_EQ(rings[0].size(), size_t(5));
}

// A non-"outer" named role (e.g. "inner") is filtered out, so the way is never
// considered and no ring is produced.
TEST(ring_assembly_inner_role_filtered) {
    WayGeometries geoms;
    add_way(geoms, 1, {{0.0, 0.0}, {0.0, 1.0}, {1.0, 1.0}, {1.0, 0.0}, {0.0, 0.0}});

    std::vector<std::pair<int64_t, std::string>> members = {{1, "inner"}};
    auto rings = assemble_outer_rings(members, geoms);

    CHECK_EQ(rings.size(), size_t(0));
}

// include_all_roles=true overrides the role filter: an "inner" closed way is
// then assembled into a ring.
TEST(ring_assembly_include_all_roles_overrides_filter) {
    WayGeometries geoms;
    add_way(geoms, 1, {{0.0, 0.0}, {0.0, 1.0}, {1.0, 1.0}, {1.0, 0.0}, {0.0, 0.0}});

    std::vector<std::pair<int64_t, std::string>> members = {{1, "inner"}};
    auto rings = assemble_outer_rings(members, geoms, /*include_all_roles=*/true);

    REQUIRE(rings.size() == size_t(1));
    CHECK(ring_is_closed(rings[0]));
}

// An open, unclosable set of ways (a single open polyline whose endpoints
// don't meet) produces no ring.
TEST(ring_assembly_open_unclosable_no_ring) {
    WayGeometries geoms;
    add_way(geoms, 1, {{0.0, 0.0}, {1.0, 0.0}, {2.0, 0.0}});

    std::vector<std::pair<int64_t, std::string>> members = {{1, "outer"}};
    auto rings = assemble_outer_rings(members, geoms);

    CHECK_EQ(rings.size(), size_t(0));
}

// Two ways that share only ONE endpoint (the other ends dangle apart) cannot
// close, so no ring is produced.
TEST(ring_assembly_dangling_chain_no_ring) {
    WayGeometries geoms;
    add_way(geoms, 1, {{0.0, 0.0}, {1.0, 0.0}});
    add_way(geoms, 2, {{1.0, 0.0}, {2.0, 0.0}});

    std::vector<std::pair<int64_t, std::string>> members = {{1, "outer"}, {2, "outer"}};
    auto rings = assemble_outer_rings(members, geoms);

    CHECK_EQ(rings.size(), size_t(0));
}

// A member referencing a way with no geometry (missing from the map) is
// skipped; a sibling closed way still assembles fine.
TEST(ring_assembly_missing_geometry_skipped) {
    WayGeometries geoms;
    add_way(geoms, 1, {{0.0, 0.0}, {0.0, 1.0}, {1.0, 1.0}, {1.0, 0.0}, {0.0, 0.0}});
    // way 2 has no geometry entry.

    std::vector<std::pair<int64_t, std::string>> members = {{2, "outer"}, {1, "outer"}};
    auto rings = assemble_outer_rings(members, geoms);

    REQUIRE(rings.size() == size_t(1));
    CHECK(ring_is_closed(rings[0]));
}

// Empty member list -> no rings (and no crash).
TEST(ring_assembly_empty_members_no_ring) {
    WayGeometries geoms;
    std::vector<std::pair<int64_t, std::string>> members;
    auto rings = assemble_outer_rings(members, geoms);
    CHECK_EQ(rings.size(), size_t(0));
}

// Two independent closed squares (disjoint, no shared coords) each become their
// own ring -> two rings out.
TEST(ring_assembly_two_disjoint_closed_ways) {
    WayGeometries geoms;
    add_way(geoms, 1, {{0.0, 0.0}, {0.0, 1.0}, {1.0, 1.0}, {1.0, 0.0}, {0.0, 0.0}});
    add_way(geoms, 2, {{5.0, 5.0}, {5.0, 6.0}, {6.0, 6.0}, {6.0, 5.0}, {5.0, 5.0}});

    std::vector<std::pair<int64_t, std::string>> members = {{1, "outer"}, {2, "outer"}};
    auto rings = assemble_outer_rings(members, geoms);

    CHECK_EQ(rings.size(), size_t(2));
    CHECK(ring_is_closed(rings[0]));
    CHECK(ring_is_closed(rings[1]));
}

// NOTE: a single pre-closed way is emitted verbatim by the greedy pass with
// NO self-intersection check — only backtrack-assembled (pass 2) rings are
// validated. This test locks in that (long-standing) behaviour; the
// intersection predicate itself is unit-tested in test_geometry.cpp for both
// the brute-force and sweep-line branches.
TEST(ring_assembly_preclosed_bowtie_passes_through) {
    WayGeometries geoms;
    add_way(geoms, 1, {{0.0, 0.0}, {1.0, 1.0}, {1.0, 0.0}, {0.0, 1.0}, {0.0, 0.0}});
    std::vector<std::pair<int64_t, std::string>> members = {{1, "outer"}};
    auto rings = assemble_outer_rings(members, geoms);
    CHECK_EQ(rings.size(), size_t(1));
}

// Two ways whose shared endpoints only line up when the second is walked in
// reverse: the stitcher must handle reversed segments.
TEST(ring_assembly_reversed_way_stitch) {
    WayGeometries geoms;
    // Way 1: (0,0) -> (0,1) -> (1,1). Way 2 REVERSED: (0,0) -> (1,0) -> (1,1)
    // (i.e. its stored direction runs from way 1's START to way 1's END).
    add_way(geoms, 1, {{0.0, 0.0}, {0.0, 1.0}, {1.0, 1.0}});
    add_way(geoms, 2, {{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0}});
    std::vector<std::pair<int64_t, std::string>> members = {{1, "outer"}, {2, "outer"}};
    auto rings = assemble_outer_rings(members, geoms);
    REQUIRE(rings.size() == size_t(1));
    CHECK(ring_is_closed(rings[0]));
}

// Greedy fails (three candidate continuations from a junction, greedy picks a
// dead end first) but the backtracking pass closes the ring.
TEST(ring_assembly_backtrack_closes_after_greedy_dead_end) {
    WayGeometries geoms;
    // Square via two arcs plus a dead-end spur sharing the junction (1,1):
    add_way(geoms, 1, {{0.0, 0.0}, {0.0, 1.0}, {1.0, 1.0}});   // arc A
    add_way(geoms, 2, {{1.0, 1.0}, {1.0, 0.0}, {0.0, 0.0}});   // arc B (closes)
    add_way(geoms, 3, {{1.0, 1.0}, {2.0, 2.0}});               // dead-end spur
    std::vector<std::pair<int64_t, std::string>> members =
        {{1, "outer"}, {2, "outer"}, {3, "outer"}};
    auto rings = assemble_outer_rings(members, geoms);
    REQUIRE(rings.size() >= size_t(1));
    CHECK(ring_is_closed(rings[0]));
}

// assemble_area_rings: every member way, whatever its role, as osmium's
// assembler takes them.

TEST(area_rings_ignore_roles) {
    WayGeometries geoms;
    add_way(geoms, 1, {{0.0, 0.0}, {0.0, 10.0}, {10.0, 10.0}, {10.0, 0.0}, {0.0, 0.0}});
    add_way(geoms, 2, {{2.0, 2.0}, {2.0, 4.0}, {4.0, 4.0}});
    add_way(geoms, 3, {{4.0, 4.0}, {4.0, 2.0}, {2.0, 2.0}});
    // Roles swapped: the outline tagged inner, the enclave's halves outer and unset.
    const std::vector<std::pair<int64_t, std::string>> members = {{1, "inner"}, {2, "outer"}, {3, ""}};
    const AreaRings a = assemble_area_rings(members, geoms);
    REQUIRE(a.rings.size() == size_t(2));
    CHECK(ring_is_closed(a.rings[0]));
    CHECK_EQ(a.rings[0].size(), size_t(5));
    CHECK(ring_is_closed(a.rings[1]));
    CHECK_EQ(a.rings[1].size(), size_t(5));
    CHECK(a.roles == std::vector<uint8_t>({kRoleInner, kRoleOuter}));
}

// The enclave touches the outline at a node, and the walk along the outline
// reaches that node before the outline closes: it comes back to a coordinate
// it has passed, so the enclave closes there as its own ring instead of
// making a figure eight with the outline.
TEST(area_rings_split_rings_touching_at_a_node) {
    WayGeometries geoms;
    add_way(geoms, 1, {{0.0, 0.0}, {0.0, 10.0}, {10.0, 10.0}, {10.0, 0.0}, {5.0, 0.0}});
    add_way(geoms, 2, {{5.0, 0.0}, {7.0, 3.0}, {3.0, 3.0}, {5.0, 0.0}});
    add_way(geoms, 3, {{5.0, 0.0}, {0.0, 0.0}});
    const std::vector<std::pair<int64_t, std::string>> members = {{1, "outer"}, {2, "inner"}, {3, "outer"}};
    const AreaRings a = assemble_area_rings(members, geoms);
    REQUIRE(a.rings.size() == size_t(2));
    CHECK_EQ(a.rings[0].size(), size_t(4));  // the enclave, closed first
    CHECK_EQ(a.rings[1].size(), size_t(6));
    for (const auto& r : a.rings) {
        CHECK(ring_is_closed(r));
        for (size_t i = 1; i + 1 < r.size(); i++) CHECK(!coord_eq(r[i], r.front()));
    }
}

// A spur hanging off the ring leaves a coordinate with an odd number of ends:
// osmium builds no area at all then, so neither does this.
TEST(area_rings_fail_on_an_open_ring) {
    WayGeometries geoms;
    add_way(geoms, 1, {{0.0, 0.0}, {0.0, 1.0}, {1.0, 1.0}});
    add_way(geoms, 3, {{1.0, 1.0}, {2.0, 2.0}});
    add_way(geoms, 2, {{1.0, 1.0}, {1.0, 0.0}, {0.0, 0.0}});
    const std::vector<std::pair<int64_t, std::string>> members = {{1, "outer"}, {3, "outer"}, {2, "outer"}};
    const AreaRings a = assemble_area_rings(members, geoms);
    CHECK(a.rings.empty());
    CHECK(a.failure == AreaFailure::OpenRing);
}

// A way listed twice cancels out, as osmium drops duplicate segments in
// pairs: two squares sharing a side, with the shared side in the relation
// twice, are one rectangle.
TEST(area_rings_cancel_ways_listed_twice) {
    WayGeometries geoms;
    add_way(geoms, 1, {{0.0, 1.0}, {0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0}});
    add_way(geoms, 2, {{0.0, 1.0}, {0.0, 2.0}, {1.0, 2.0}, {1.0, 1.0}});
    add_way(geoms, 3, {{0.0, 1.0}, {1.0, 1.0}});
    const std::vector<std::pair<int64_t, std::string>> members = {{1, "outer"}, {3, "outer"}, {2, "outer"}, {3, "outer"}};
    const AreaRings a = assemble_area_rings(members, geoms);
    REQUIRE(a.rings.size() == size_t(1));
    CHECK_EQ(a.rings[0].size(), size_t(7));
    CHECK_EQ(a.cancelled_sub_ways, 2u);
}

// Crossing segments fail the area too, whether in one ring (stitched or a
// closed way) or between two rings.
TEST(area_rings_fail_when_segments_cross) {
    WayGeometries geoms;
    add_way(geoms, 1, {{0.0, 0.0}, {1.0, 1.0}, {1.0, 0.0}});
    add_way(geoms, 2, {{1.0, 0.0}, {0.0, 1.0}, {0.0, 0.0}});
    add_way(geoms, 3, {{5.0, 5.0}, {6.0, 6.0}, {6.0, 5.0}, {5.0, 6.0}, {5.0, 5.0}});
    add_way(geoms, 4, {{10.0, 10.0}, {10.0, 12.0}, {12.0, 12.0}, {12.0, 10.0}, {10.0, 10.0}});
    add_way(geoms, 5, {{11.0, 11.0}, {11.0, 13.0}, {13.0, 13.0}, {13.0, 11.0}, {11.0, 11.0}});
    for (const auto& members : std::vector<std::vector<std::pair<int64_t, std::string>>>{
             {{1, "outer"}, {2, "outer"}}, {{3, "outer"}}, {{4, "outer"}, {5, "outer"}}}) {
        const AreaRings a = assemble_area_rings(members, geoms);
        CHECK(a.rings.empty());
        CHECK(a.failure == AreaFailure::Crossing);
    }
}

// Rings may touch at up to 100 coordinates (osmium's max_split_locations): a
// chain of squares corner to corner.
TEST(area_rings_fail_past_100_touching_coordinates) {
    WayGeometries geoms;
    std::vector<std::pair<int64_t, std::string>> members;
    for (int i = 0; i < 102; i++) {
        const double d = i;
        add_way(geoms, i + 1, {{d, d}, {d, d + 1}, {d + 1, d + 1}, {d + 1, d}, {d, d}});
        members.emplace_back(i + 1, "outer");
    }
    const AreaRings all = assemble_area_rings(members, geoms);
    CHECK(all.rings.empty());
    CHECK(all.failure == AreaFailure::TooManyTouches);
    members.pop_back();
    const AreaRings fewer = assemble_area_rings(members, geoms);
    CHECK_EQ(fewer.rings.size(), size_t(101));
    CHECK(fewer.failure == AreaFailure::None);
}

namespace {

// The polygons nest_rings makes of a relation's rings, nested as the builder
// nests them, and how many of them hold a point (inside the outer ring and
// outside its holes).
std::vector<NestedPolygon> nested(const AreaRings& a) {
    const std::vector<int> around = enclosing_rings(a.rings);
    return nest_rings(a.rings, around, ring_depths(around));
}

int polygons_holding(const std::vector<NestedPolygon>& polygons, double lat, double lng) {
    int n = 0;
    for (const auto& p : polygons) {
        bool in = ring_contains_latlng(p.outer, lat, lng);
        for (const auto& h : p.holes) in = in && !ring_contains_latlng(h, lat, lng);
        n += in;
    }
    return n;
}

}  // namespace

// Two squares overlapping, each closed way noded at the two points where
// their outlines meet: no segments cross, but walked in member order the
// rings are the two squares, which cross at those nodes. osmium joins the
// ways' halves into rings that don't cross (two L-shapes), so the overlap is
// in neither: the area is the even-odd one of the outlines.
TEST(area_rings_rings_crossing_at_shared_nodes_take_the_even_odd_area) {
    WayGeometries geoms;
    add_way(geoms, 1, {{0.0, 0.0}, {0.0, 2.0}, {1.0, 2.0}, {2.0, 2.0}, {2.0, 1.0}, {2.0, 0.0}, {0.0, 0.0}});
    add_way(geoms, 2, {{1.0, 1.0}, {1.0, 2.0}, {1.0, 3.0}, {3.0, 3.0}, {3.0, 1.0}, {2.0, 1.0}, {1.0, 1.0}});
    for (const auto& members : std::vector<std::vector<std::pair<int64_t, std::string>>>{
             {{1, "outer"}, {2, "outer"}}, {{2, "outer"}, {1, "inner"}}}) {
        const AreaRings a = assemble_area_rings(members, geoms);
        CHECK(a.failure == AreaFailure::None);
        CHECK_EQ(a.rings.size(), size_t(2));
        const std::vector<NestedPolygon> polygons = nested(a);
        CHECK_EQ(polygons_holding(polygons, 1.5, 1.5), 0);  // the overlap
        CHECK_EQ(polygons_holding(polygons, 0.5, 0.5), 1);
        CHECK_EQ(polygons_holding(polygons, 0.5, 1.5), 1);
        CHECK_EQ(polygons_holding(polygons, 2.5, 2.5), 1);
        CHECK_EQ(polygons_holding(polygons, 2.5, 1.5), 1);
        CHECK_EQ(polygons_holding(polygons, 3.5, 3.5), 0);
    }
}

// Segments that cross fail the area before rings crossing at shared nodes
// are looked at, as osmium finds intersections before it joins rings (The
// Ledges, r19640968): no area, and no node counted as re-paired.
TEST(area_rings_crossing_segments_fail_before_rings_are_re_paired) {
    WayGeometries geoms;
    add_way(geoms, 1, {{0.0, 0.0}, {0.0, 2.0}, {1.0, 2.0}, {2.0, 2.0}, {2.0, 1.0}, {2.0, 0.0}, {0.0, 0.0}});
    add_way(geoms, 2, {{1.0, 1.0}, {1.0, 2.0}, {1.0, 3.0}, {3.0, 3.0}, {3.0, 1.0}, {2.0, 1.0}, {1.0, 1.0}});
    add_way(geoms, 3, {{5.0, 5.0}, {6.0, 6.0}, {6.0, 5.0}, {5.0, 6.0}, {5.0, 5.0}});
    const AreaRings a = assemble_area_rings({{1, "outer"}, {2, "outer"}, {3, "outer"}}, geoms);
    CHECK(a.rings.empty());
    CHECK(a.failure == AreaFailure::Crossing);
    CHECK_EQ(a.crossing_coordinates, 0u);
}

// Two enclaves overlapping each other, crossing at two shared nodes
// (Louisville, r1804307): their overlap is in an odd number of rings, so it
// is part of the area again, an island as osmium builds it.
TEST(area_rings_enclaves_crossing_at_shared_nodes_leave_their_overlap_in) {
    WayGeometries geoms;
    add_way(geoms, 1, {{0.0, 0.0}, {0.0, 10.0}, {10.0, 10.0}, {10.0, 0.0}, {0.0, 0.0}});
    add_way(geoms, 2, {{2.0, 2.0}, {2.0, 6.0}, {4.0, 6.0}, {6.0, 6.0}, {6.0, 4.0}, {6.0, 2.0}, {2.0, 2.0}});
    add_way(geoms, 3, {{4.0, 4.0}, {4.0, 6.0}, {4.0, 8.0}, {8.0, 8.0}, {8.0, 4.0}, {6.0, 4.0}, {4.0, 4.0}});
    const AreaRings a = assemble_area_rings({{1, "outer"}, {2, "inner"}, {3, "inner"}}, geoms);
    CHECK(a.failure == AreaFailure::None);
    const std::vector<NestedPolygon> polygons = nested(a);
    CHECK_EQ(polygons_holding(polygons, 5.0, 5.0), 1);  // the overlap
    CHECK_EQ(polygons_holding(polygons, 3.0, 3.0), 0);
    CHECK_EQ(polygons_holding(polygons, 7.0, 7.0), 0);
    CHECK_EQ(polygons_holding(polygons, 1.0, 1.0), 1);
    CHECK_EQ(polygons_holding(polygons, 9.0, 1.0), 1);
}

// Rings that only touch at a node keep their own rings, in either member
// order: two squares corner to corner are two polygons, and an enclave
// touching the outline from inside is a hole in it.
TEST(area_rings_rings_touching_at_a_node_stay_as_they_are) {
    WayGeometries geoms;
    add_way(geoms, 1, {{0.0, 0.0}, {0.0, 1.0}, {1.0, 1.0}, {1.0, 0.0}, {0.0, 0.0}});
    add_way(geoms, 2, {{1.0, 1.0}, {1.0, 2.0}, {2.0, 2.0}, {2.0, 1.0}, {1.0, 1.0}});
    add_way(geoms, 3, {{0.0, 0.0}, {0.0, 5.0}, {0.0, 10.0}, {10.0, 10.0}, {10.0, 0.0}, {0.0, 0.0}});
    add_way(geoms, 4, {{0.0, 5.0}, {3.0, 7.0}, {6.0, 5.0}, {3.0, 3.0}, {0.0, 5.0}});
    using Members = std::vector<std::pair<int64_t, std::string>>;
    for (const Members& members : {Members{{1, "outer"}, {2, "outer"}}, Members{{2, "outer"}, {1, "outer"}}}) {
        const AreaRings a = assemble_area_rings(members, geoms);
        CHECK(a.failure == AreaFailure::None);
        REQUIRE(a.rings.size() == size_t(2));
        CHECK_EQ(a.rings[0].size(), size_t(5));
        CHECK_EQ(a.rings[1].size(), size_t(5));
        const std::vector<NestedPolygon> polygons = nested(a);
        CHECK_EQ(polygons.size(), size_t(2));
        CHECK_EQ(polygons_holding(polygons, 0.5, 0.5), 1);
        CHECK_EQ(polygons_holding(polygons, 1.5, 1.5), 1);
        CHECK_EQ(polygons_holding(polygons, 0.5, 1.5), 0);
    }
    for (const Members& members : {Members{{3, "outer"}, {4, "inner"}}, Members{{4, "inner"}, {3, "outer"}}}) {
        const AreaRings a = assemble_area_rings(members, geoms);
        CHECK(a.failure == AreaFailure::None);
        REQUIRE(a.rings.size() == size_t(2));
        const std::vector<NestedPolygon> polygons = nested(a);
        REQUIRE(polygons.size() == size_t(1));
        CHECK_EQ(polygons[0].holes.size(), size_t(1));
        CHECK_EQ(polygons_holding(polygons, 3.0, 5.0), 0);
        CHECK_EQ(polygons_holding(polygons, 8.0, 8.0), 1);
    }
}
