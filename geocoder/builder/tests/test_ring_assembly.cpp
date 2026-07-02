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

#include "test_framework.h"

namespace {

using Coord = std::pair<double, double>;
using Ring = std::vector<Coord>;

// Build a WayGeometry from a coordinate list. The node-id fields are not used
// by assemble_outer_rings (it works purely on coordinates), so any value is
// fine; we set them deterministically anyway.
ParsedData::WayGeometry make_way(const std::vector<Coord>& coords) {
    ParsedData::WayGeometry wg;
    wg.coords = coords;
    wg.first_node_id = 0;
    wg.last_node_id = 0;
    return wg;
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
    std::unordered_map<int64_t, ParsedData::WayGeometry> geoms;
    geoms[1] = make_way({{0.0, 0.0}, {0.0, 1.0}, {1.0, 1.0}, {1.0, 0.0}, {0.0, 0.0}});

    std::vector<std::pair<int64_t, std::string>> members = {{1, "outer"}};
    auto rings = assemble_outer_rings(members, geoms);

    CHECK_EQ(rings.size(), size_t(1));
    CHECK(ring_is_closed(rings[0]));
    // The ring is the way's coordinates verbatim (5 points, closed).
    CHECK_EQ(rings[0].size(), size_t(5));
    CHECK(coord_eq(rings[0].front(), Coord(0.0, 0.0)));
}

// An empty role string is treated like "outer" (the filter only skips named
// non-outer roles), so an empty-role closed way still produces a ring.
TEST(ring_assembly_empty_role_is_outer) {
    std::unordered_map<int64_t, ParsedData::WayGeometry> geoms;
    geoms[1] = make_way({{0.0, 0.0}, {0.0, 1.0}, {1.0, 1.0}, {1.0, 0.0}, {0.0, 0.0}});

    std::vector<std::pair<int64_t, std::string>> members = {{1, ""}};
    auto rings = assemble_outer_rings(members, geoms);

    CHECK_EQ(rings.size(), size_t(1));
    CHECK(ring_is_closed(rings[0]));
}

// Two open ways sharing both endpoints stitch into one closed ring.
//   way 1: bottom + right edges  (0,0) -> (1,0) -> (1,1)
//   way 2: top + left edges      (1,1) -> (0,1) -> (0,0)
// Stitched they form a closed square.
TEST(ring_assembly_two_ways_stitch) {
    std::unordered_map<int64_t, ParsedData::WayGeometry> geoms;
    geoms[1] = make_way({{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0}});
    geoms[2] = make_way({{1.0, 1.0}, {0.0, 1.0}, {0.0, 0.0}});

    std::vector<std::pair<int64_t, std::string>> members = {{1, "outer"}, {2, "outer"}};
    auto rings = assemble_outer_rings(members, geoms);

    CHECK_EQ(rings.size(), size_t(1));
    CHECK(ring_is_closed(rings[0]));
    // 3 + 3 coords stitched at the shared endpoint -> 5 distinct points,
    // closed back to start.
    CHECK_EQ(rings[0].size(), size_t(5));
}

// A non-"outer" named role (e.g. "inner") is filtered out, so the way is never
// considered and no ring is produced.
TEST(ring_assembly_inner_role_filtered) {
    std::unordered_map<int64_t, ParsedData::WayGeometry> geoms;
    geoms[1] = make_way({{0.0, 0.0}, {0.0, 1.0}, {1.0, 1.0}, {1.0, 0.0}, {0.0, 0.0}});

    std::vector<std::pair<int64_t, std::string>> members = {{1, "inner"}};
    auto rings = assemble_outer_rings(members, geoms);

    CHECK_EQ(rings.size(), size_t(0));
}

// include_all_roles=true overrides the role filter: an "inner" closed way is
// then assembled into a ring.
TEST(ring_assembly_include_all_roles_overrides_filter) {
    std::unordered_map<int64_t, ParsedData::WayGeometry> geoms;
    geoms[1] = make_way({{0.0, 0.0}, {0.0, 1.0}, {1.0, 1.0}, {1.0, 0.0}, {0.0, 0.0}});

    std::vector<std::pair<int64_t, std::string>> members = {{1, "inner"}};
    auto rings = assemble_outer_rings(members, geoms, /*include_all_roles=*/true);

    CHECK_EQ(rings.size(), size_t(1));
    CHECK(ring_is_closed(rings[0]));
}

// An open, unclosable set of ways (a single open polyline whose endpoints
// don't meet) produces no ring.
TEST(ring_assembly_open_unclosable_no_ring) {
    std::unordered_map<int64_t, ParsedData::WayGeometry> geoms;
    geoms[1] = make_way({{0.0, 0.0}, {1.0, 0.0}, {2.0, 0.0}});

    std::vector<std::pair<int64_t, std::string>> members = {{1, "outer"}};
    auto rings = assemble_outer_rings(members, geoms);

    CHECK_EQ(rings.size(), size_t(0));
}

// Two ways that share only ONE endpoint (the other ends dangle apart) cannot
// close, so no ring is produced.
TEST(ring_assembly_dangling_chain_no_ring) {
    std::unordered_map<int64_t, ParsedData::WayGeometry> geoms;
    geoms[1] = make_way({{0.0, 0.0}, {1.0, 0.0}});
    geoms[2] = make_way({{1.0, 0.0}, {2.0, 0.0}});

    std::vector<std::pair<int64_t, std::string>> members = {{1, "outer"}, {2, "outer"}};
    auto rings = assemble_outer_rings(members, geoms);

    CHECK_EQ(rings.size(), size_t(0));
}

// A member referencing a way with no geometry (missing from the map) is
// skipped; a sibling closed way still assembles fine.
TEST(ring_assembly_missing_geometry_skipped) {
    std::unordered_map<int64_t, ParsedData::WayGeometry> geoms;
    geoms[1] = make_way({{0.0, 0.0}, {0.0, 1.0}, {1.0, 1.0}, {1.0, 0.0}, {0.0, 0.0}});
    // way 2 has no geometry entry.

    std::vector<std::pair<int64_t, std::string>> members = {{2, "outer"}, {1, "outer"}};
    auto rings = assemble_outer_rings(members, geoms);

    CHECK_EQ(rings.size(), size_t(1));
    CHECK(ring_is_closed(rings[0]));
}

// Empty member list -> no rings (and no crash).
TEST(ring_assembly_empty_members_no_ring) {
    std::unordered_map<int64_t, ParsedData::WayGeometry> geoms;
    std::vector<std::pair<int64_t, std::string>> members;
    auto rings = assemble_outer_rings(members, geoms);
    CHECK_EQ(rings.size(), size_t(0));
}

// Two independent closed squares (disjoint, no shared coords) each become their
// own ring -> two rings out.
TEST(ring_assembly_two_disjoint_closed_ways) {
    std::unordered_map<int64_t, ParsedData::WayGeometry> geoms;
    geoms[1] = make_way({{0.0, 0.0}, {0.0, 1.0}, {1.0, 1.0}, {1.0, 0.0}, {0.0, 0.0}});
    geoms[2] = make_way({{5.0, 5.0}, {5.0, 6.0}, {6.0, 6.0}, {6.0, 5.0}, {5.0, 5.0}});

    std::vector<std::pair<int64_t, std::string>> members = {{1, "outer"}, {2, "outer"}};
    auto rings = assemble_outer_rings(members, geoms);

    CHECK_EQ(rings.size(), size_t(2));
    CHECK(ring_is_closed(rings[0]));
    CHECK(ring_is_closed(rings[1]));
}
