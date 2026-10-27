// polygon_rings.h: holes kept in an admin polygon's vertex range so that the
// crossing-number tests honour them.
#include "polygon_rings.h"

#include <cmath>
#include <random>
#include <vector>

#include "continent_boundaries.h"
#include "geometry.h"
#include "ring_assembly.h"
#include "ring_contains.h"
#include "test_framework.h"

namespace {

using Ring = std::vector<LatLng>;

// Closed axis-aligned square ring (lat0, lng0)-(lat1, lng1), counter-clockwise
// in (lng, lat), starting at its lexicographically smallest corner.
Ring square(double lat0, double lng0, double lat1, double lng1) {
    return {{lat0, lng0}, {lat0, lng1}, {lat1, lng1}, {lat1, lng0}, {lat0, lng0}};
}

Ring reversed(Ring r) {
    std::reverse(r.begin(), r.end());
    return r;
}

std::vector<NodeCoord> as_nodes(const Ring& r) {
    std::vector<NodeCoord> out;
    for (const auto& [lat, lng] : r) out.push_back({static_cast<float>(lat), static_cast<float>(lng)});
    return out;
}

// Italy-like outer ring with two enclaves, the second touching nothing.
const Ring kOuter = square(0, 0, 10, 10);
const std::vector<Ring> kHoles = {reversed(square(2, 2, 4, 4)), reversed(square(6, 5, 7, 8))};

bool range_contains(const std::vector<NodeCoord>& v, double lat, double lng) {
    return ring_contains(v.data(), static_cast<uint32_t>(v.size()), static_cast<float>(lat), static_cast<float>(lng));
}

}  // namespace

TEST(encode_rings_splits_back_into_the_same_rings) {
    const Ring range = encode_rings(kOuter, kHoles);
    CHECK_EQ(range.size(), kOuter.size() + kHoles[0].size() + kHoles[1].size() + 2 * kExcursionConnectors);
    std::vector<RingSpan> spans;
    CHECK(split_rings(range.data(), static_cast<uint32_t>(range.size()), 2, spans));
    CHECK_EQ(spans.size(), 3u);
    CHECK_EQ(spans[0].begin, 0u);
    CHECK_EQ(spans[0].count, static_cast<uint32_t>(kOuter.size()));
    for (size_t k = 0; k < kHoles.size(); k++) {
        CHECK_EQ(spans[k + 1].count, static_cast<uint32_t>(kHoles[k].size()));
        for (uint32_t j = 0; j < spans[k + 1].count; j++) CHECK(range[spans[k + 1].begin + j] == kHoles[k][j]);
    }
}

TEST(split_rings_without_holes_is_one_ring) {
    std::vector<RingSpan> spans;
    CHECK(split_rings(kOuter.data(), static_cast<uint32_t>(kOuter.size()), 0, spans));
    CHECK_EQ(spans.size(), 1u);
    CHECK_EQ(spans[0].count, static_cast<uint32_t>(kOuter.size()));
    CHECK_EQ(outer_ring_count(kOuter.data(), static_cast<uint32_t>(kOuter.size()), 0),
             static_cast<uint32_t>(kOuter.size()));
}

TEST(split_rings_rejects_a_range_without_the_holes_it_claims) {
    std::vector<RingSpan> spans;
    CHECK(!split_rings(kOuter.data(), static_cast<uint32_t>(kOuter.size()), 1, spans));
    CHECK_EQ(spans.size(), 1u);
    CHECK_EQ(spans[0].count, static_cast<uint32_t>(kOuter.size()));
}

TEST(crossing_number_tests_leave_out_the_holes) {
    const auto v = as_nodes(encode_rings(kOuter, kHoles));
    CHECK(range_contains(v, 1, 1));       // outer ring only
    CHECK(range_contains(v, 5, 9));
    CHECK(!range_contains(v, 3, 3));      // first hole
    CHECK(!range_contains(v, 6.5, 6));    // second hole
    CHECK(!range_contains(v, 11, 5));     // outside
    // Along the connectors: v0's latitude and the holes' first longitudes.
    CHECK(range_contains(v, 0.5, 2));
    CHECK(range_contains(v, 1, 5));
    CHECK(range_contains(v, 5, 2));
    // The axis-swapped test (place-node containment) agrees.
    CHECK(point_in_polygon_nc(1, 1, v.data(), v.size()));
    CHECK(!point_in_polygon_nc(3, 3, v.data(), v.size()));
    CHECK(!point_in_polygon_nc(6.5f, 6, v.data(), v.size()));
}

TEST(crossing_number_matches_outer_minus_holes_at_random_points) {
    // Every connector edge walked both ways cancels exactly, wherever the
    // point is: compare against ring-by-ring tests on a fine random sample.
    const auto v = as_nodes(encode_rings(kOuter, kHoles));
    const auto outer = as_nodes(kOuter);
    std::vector<std::vector<NodeCoord>> holes;
    for (const auto& h : kHoles) holes.push_back(as_nodes(h));
    std::mt19937_64 rng(7);
    std::uniform_real_distribution<double> coord(-1, 11);
    for (int i = 0; i < 20000; i++) {
        // Snap to a coarse grid so many points land on connector lines.
        const float lat = static_cast<float>(std::round(coord(rng) * 4) / 4);
        const float lng = static_cast<float>(std::round(coord(rng) * 4) / 4);
        bool want = range_contains(outer, lat, lng);
        for (const auto& h : holes)
            if (range_contains(h, lat, lng)) want = !want;
        CHECK_EQ(range_contains(v, lat, lng), want);
    }
}

TEST(shoelace_area_subtracts_holes) {
    const Ring range = encode_rings(kOuter, kHoles);
    CHECK_NEAR(polygon_area(range), 100.0 - 4.0 - 3.0, 1e-4);
}

TEST(interior_point_lies_inside_a_concave_ring) {
    // A C-shape: its vertex average (about (5, 3.6)) is in the notch.
    const Ring c = {{0, 0}, {0, 10}, {2, 10}, {2, 2}, {8, 2}, {8, 10}, {10, 10}, {10, 0}, {0, 0}};
    const std::vector<RingSpan> whole = {{0, static_cast<uint32_t>(c.size())}};
    const auto [lat, lng] = interior_point(c.data(), whole);
    CHECK(ring_contains_latlng(c, lat, lng));
    double avg_lat = 0, avg_lng = 0;
    for (const auto& [la, ln] : c) {
        avg_lat += la;
        avg_lng += ln;
    }
    CHECK(!ring_contains_latlng(c, avg_lat / c.size(), avg_lng / c.size()));
}

TEST(interior_point_avoids_holes) {
    // The scan line through the middle meets the central hole: the point
    // must sit in one of the stretches beside it.
    const Ring range = encode_rings(square(0, 0, 10, 10), {reversed(square(1, 3, 9, 7))});
    const auto [lat, lng] = polygon_interior_point(range.data(), static_cast<uint32_t>(range.size()), 1);
    CHECK(ring_contains_latlng(square(0, 0, 10, 10), lat, lng));
    CHECK(!ring_contains_latlng(square(1, 3, 9, 7), lat, lng));
}

TEST(interior_point_of_a_degenerate_ring_is_its_first_vertex) {
    const Ring flat = {{1, 1}, {1, 2}, {1, 3}, {1, 1}};
    const std::vector<RingSpan> whole = {{0, 4}};
    const auto p = interior_point(flat.data(), whole);
    CHECK(p == LatLng(1, 1));
}

TEST(assign_holes_picks_the_smallest_outer_ring_larger_than_the_hole) {
    // The Netherlands' main ring holds a Belgian enclave; a Dutch
    // counter-enclave inside that enclave is a smaller outer ring and must
    // not take the hole.
    const std::vector<Ring> outers = {square(0, 0, 10, 10), square(2.5, 2.5, 3.5, 3.5), square(20, 20, 21, 21)};
    const std::vector<Ring> inners = {square(2, 2, 4, 4), square(20.2, 20.2, 20.4, 20.4), square(50, 50, 51, 51)};
    const std::vector<int> owner = assign_holes(outers, inners);
    CHECK_EQ(owner.size(), 3u);
    CHECK_EQ(owner[0], 0);
    CHECK_EQ(owner[1], 2);
    CHECK_EQ(owner[2], -1);  // inside no outer ring
}

TEST(simplify_rings_turns_holes_against_the_outer_ring_and_drops_collapsed_ones) {
    const auto identity = [](const Ring& r, double) { return r; };
    // Holes given with the outer ring's orientation, one too small to keep.
    const std::vector<Ring> holes = {square(2, 2, 4, 4), {{6, 6}, {6, 7}, {6, 6}}};
    const SimplifiedRings s = simplify_rings(kOuter, holes, identity);
    CHECK_EQ(s.holes, 1u);
    CHECK_EQ(s.halvings, 0u);
    CHECK_NEAR(polygon_area(s.range), 96.0, 1e-4);
    const auto v = as_nodes(s.range);
    CHECK(!range_contains(v, 3, 3));
    CHECK(range_contains(v, 6.5, 6.5));
}

TEST(simplify_rings_without_holes_is_the_simplified_outer_ring) {
    const auto shrink = [](const Ring& r, double) { return Ring(r.begin(), r.end()); };
    const SimplifiedRings s = simplify_rings(kOuter, {}, shrink);
    CHECK_EQ(s.holes, 0u);
    CHECK(s.range == kOuter);
}

TEST(simplify_rings_orders_holes_by_first_vertex) {
    const auto identity = [](const Ring& r, double) { return r; };
    const std::vector<Ring> holes = {kHoles[1], kHoles[0]};
    const SimplifiedRings s = simplify_rings(kOuter, holes, identity);
    CHECK_EQ(s.holes, 2u);
    CHECK(s.range == encode_rings(kOuter, kHoles));
}

namespace {

// Douglas-Peucker at eps degrees times simplify_rings' tolerance scale, as
// the builder's quality variants simplify.
auto dp(double eps) {
    return [eps](const Ring& r, double scale) { return simplify_polygon_epsilon(r, eps * scale); };
}

}  // namespace

TEST(simplify_rings_keeps_a_hole_inside_the_outer_ring_it_simplifies) {
    // The outer ring's top edge has a 0.2 high bump at lng 5, and an
    // enclave reaches up into it. At 0.3 the bump goes, the outer ring's top
    // edge straightens to lat 10, and the enclave's top (lat 10.15) pokes
    // out of it: the crossing test would count that strip outside the outer
    // ring as inside the polygon. At half the tolerance the bump stays.
    const Ring outer = {{0, 0}, {0, 10}, {10, 10}, {10.2, 5}, {10, 0}, {0, 0}};
    const Ring hole = reversed(square(9.5, 4.8, 10.15, 5.2));
    CHECK_EQ(dp(0.3)(outer, 1).size(), 5u);  // the bump simplified away
    CHECK_EQ(dp(0.3)(hole, 1).size(), 5u);   // the enclave as it was
    const SimplifiedRings s = simplify_rings(outer, {hole}, dp(0.3));
    CHECK_EQ(s.holes, 1u);
    CHECK_EQ(s.halvings, 1u);
    CHECK(s.nests);
    const auto v = as_nodes(s.range);
    CHECK(!range_contains(v, 10.1, 5));  // above lat 10, in the enclave
    CHECK(!range_contains(v, 9.8, 5));   // in the enclave
    CHECK(range_contains(v, 5, 5));
    CHECK(!range_contains(v, 10.5, 5));  // above the bump
}

TEST(simplify_rings_keeps_bordering_holes_apart) {
    // Two enclaves share a border with a 0.2 bump into the second. The
    // first straightens it at 0.3; the second keeps the bump but cuts its
    // corner at (4, 4), so near there the two overlap, and the crossing test
    // would count the overlap inside the polygon. Only at 0.3/16 does the
    // second keep (4, 4).
    const Ring a = {{2, 2}, {2, 4}, {3, 4.2}, {4, 4}, {4, 2}, {2, 2}};
    const Ring b = {{2, 4}, {2, 6}, {6, 6}, {6, 3.5}, {4, 4}, {3, 4.2}, {2, 4}};
    CHECK_EQ(dp(0.3)(a, 1).size(), 5u);
    CHECK_EQ(dp(0.3)(b, 1).size(), 6u);
    const SimplifiedRings s = simplify_rings(kOuter, {a, b}, dp(0.3));
    CHECK_EQ(s.holes, 2u);
    CHECK_EQ(s.halvings, 4u);
    const auto v = as_nodes(s.range);
    CHECK(!range_contains(v, 3.98, 3.995));  // in the overlap the first pass made
    CHECK(!range_contains(v, 3, 3));
    CHECK(!range_contains(v, 3, 5));
    CHECK(range_contains(v, 1, 1));
}

TEST(simplify_rings_keeps_the_rings_as_given_when_no_tolerance_nests_them) {
    // A simplification that moves the hole across the outer ring's edge at
    // any tolerance.
    const auto shove = [](const Ring& r, double) {
        Ring out = r;
        if (r.front().first > 1) for (auto& p : out) p.first += 7;
        return out;
    };
    const Ring hole = reversed(square(2, 2, 4, 4));
    const SimplifiedRings s = simplify_rings(kOuter, {hole}, shove);
    CHECK_EQ(s.halvings, kKeptUnsimplified);
    CHECK(s.nests);
    CHECK(s.range == encode_rings(kOuter, {hole}));
}

TEST(simplify_rings_reports_rings_that_do_not_nest_even_as_given) {
    const auto identity = [](const Ring& r, double) { return r; };
    const SimplifiedRings s = simplify_rings(kOuter, {reversed(square(9, 2, 11, 4))}, identity);
    CHECK_EQ(s.halvings, kKeptUnsimplified);
    CHECK(!s.nests);
    CHECK_EQ(s.holes, 1u);
    HoleTopologyCounts counts;
    counts.add(s.halvings, s.nests);
    counts.add(2, true);
    counts.add(0, true);
    CHECK(counts.summary() ==
          "1 polygons simplified again (tolerance/2 0, /4 1, /8 0, /16 0), 1 kept unsimplified (1 not nesting even so)");
}

TEST(rings_nest_takes_holes_inside_the_outer_ring) {
    CHECK(rings_nest(kOuter, kHoles));
    CHECK(rings_nest(kOuter, {}));
    // Orientation doesn't matter.
    CHECK(rings_nest(reversed(kOuter), {reversed(kHoles[0]), kHoles[1]}));
}

TEST(rings_nest_takes_rings_that_touch) {
    // A hole touching the outer ring at a vertex inside one of its edges.
    CHECK(rings_nest(kOuter, {{{10, 5}, {7, 6}, {7, 4}, {10, 5}}}));
    // A hole sharing an edge with the outer ring.
    const Ring notched = {{0, 0}, {0, 10}, {10, 10}, {10, 6}, {10, 4}, {10, 0}, {0, 0}};
    CHECK(rings_nest(notched, {square(7, 4, 10, 6)}));
    // A slanted shared edge, whose middle needn't round onto it.
    const Ring slanted = {{0, 0}, {0.3, 10}, {10.7, 9.1}, {9.9, 0.1}, {0, 0}};
    CHECK(rings_nest(slanted, {{{0.3, 10}, {10.7, 9.1}, {5.1, 5.3}, {0.3, 10}}}));
    // Holes sharing an edge, and touching at a corner.
    CHECK(rings_nest(kOuter, {square(2, 2, 4, 4), square(2, 4, 4, 6), square(4, 6, 5, 7)}));
}

TEST(rings_nest_rejects_holes_outside_the_outer_ring) {
    CHECK(!rings_nest(kOuter, {square(9, 2, 11, 4)}));    // crossing it
    CHECK(!rings_nest(kOuter, {square(12, 2, 14, 4)}));   // beside it
    CHECK(!rings_nest(square(4, 4, 5, 5), {kOuter}));     // around it
    CHECK(!rings_nest(kOuter, {square(10, 2, 12, 4)}));   // touching it from outside
    // Touching the outer ring at two vertices, the edge between them
    // cutting across a notch in it, without crossing any edge.
    const Ring notched = {{0, 0}, {0, 10}, {10, 10}, {10, 6}, {8, 5}, {10, 4}, {10, 0}, {0, 0}};
    CHECK(!rings_nest(notched, {square(6, 4, 10, 6)}));
    CHECK(rings_nest(notched, {square(6, 4, 7.5, 6)}));
}

TEST(rings_nest_rejects_a_hole_edge_leaving_the_outer_ring_where_it_touches) {
    // The outer ring pinches at (5, 9) around a notch whose west side
    // (lng 7) crosses the hole's edge (5, 1)-(5, 9) at its vertex (5, 7):
    // the edge's stretch east of it lies in the notch, though its end
    // points and its middle (5, 5) do not.
    const Ring outer = {{0, 0}, {0, 9}, {5, 9}, {4, 7}, {5, 7}, {6, 7}, {5, 9}, {10, 9}, {10, 0}, {0, 0}};
    const Ring hole = {{5, 1}, {5, 9}, {2, 5}, {5, 1}};
    CHECK(!rings_nest(outer, {hole}));
    const auto v = as_nodes(encode_rings(outer, {hole}));
    CHECK(range_contains(v, 4.6, 7.8));  // in the notch, yet counted inside
}

TEST(rings_nest_rejects_overlapping_holes) {
    CHECK(!rings_nest(kOuter, {square(2, 2, 5, 5), square(4, 4, 7, 7)}));  // crossing
    CHECK(!rings_nest(kOuter, {square(2, 2, 8, 8), square(4, 4, 6, 6)}));  // one inside the other
    CHECK(!rings_nest(kOuter, {square(4, 4, 6, 6), square(2, 2, 8, 8)}));
    CHECK(!rings_nest(kOuter, {square(2, 2, 4, 4), reversed(square(2, 2, 4, 4))}));  // the same twice
    CHECK(!rings_nest(kOuter, {square(2, 2, 6, 6), square(2, 2, 4, 4)}));  // inside, sharing a corner
}

TEST(assemble_inner_rings_takes_only_inner_ways) {
    WayGeometries geoms;
    auto add = [&](int64_t id, const Ring& coords) {
        geoms.ways[id] = {geoms.coords.size(), static_cast<uint32_t>(coords.size()), 0, 0};
        geoms.coords.insert(geoms.coords.end(), coords.begin(), coords.end());
    };
    add(1, square(0, 0, 10, 10));
    // The enclave's boundary in two halves.
    add(2, {{2, 2}, {2, 4}, {4, 4}});
    add(3, {{4, 4}, {4, 2}, {2, 2}});
    const std::vector<std::pair<int64_t, std::string>> members = {{1, "outer"}, {2, "inner"}, {3, "inner"}};
    const auto inners = assemble_inner_rings(members, geoms);
    CHECK_EQ(inners.size(), 1u);
    CHECK_NEAR(polygon_area(inners[0]), 4.0, 1e-6);
    const auto outers = assemble_outer_rings(members, geoms);
    CHECK_EQ(outers.size(), 1u);
    CHECK_NEAR(polygon_area(outers[0]), 100.0, 1e-6);
}
