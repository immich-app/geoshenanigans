// polygon_rings.h: an admin relation's rings nested into polygons, a
// polygon's interior point, and whether its rings nest.
#include "polygon_rings.h"

#include <cmath>
#include <random>
#include <vector>

#include "geometry.h"
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

// Italy-like outer ring with two enclaves, the second touching nothing.
const Ring kOuter = square(0, 0, 10, 10);
const std::vector<Ring> kHoles = {reversed(square(2, 2, 4, 4)), reversed(square(6, 5, 7, 8))};

}  // namespace

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
    Ring rings = square(0, 0, 10, 10);
    const Ring hole = reversed(square(1, 3, 9, 7));
    rings.insert(rings.end(), hole.begin(), hole.end());
    const std::vector<RingSpan> spans = {{0, 5}, {5, 5}};
    const auto [lat, lng] = interior_point(rings.data(), spans);
    CHECK(ring_contains_latlng(square(0, 0, 10, 10), lat, lng));
    CHECK(!ring_contains_latlng(square(1, 3, 9, 7), lat, lng));
}

TEST(interior_point_of_a_degenerate_ring_is_its_first_vertex) {
    const Ring flat = {{1, 1}, {1, 2}, {1, 3}, {1, 1}};
    const std::vector<RingSpan> whole = {{0, 4}};
    const auto p = interior_point(flat.data(), whole);
    CHECK(p == LatLng(1, 1));
}

// Baarle: the Netherlands' main ring holds a Belgian enclave (a hole), and a
// Dutch counter-enclave inside that is an outer ring again; an island with a
// lake, and a ring apart.
const std::vector<Ring> kBaarle = {square(0, 0, 10, 10), square(2, 2, 4, 4), square(2.5, 2.5, 3.5, 3.5),
                                   square(20, 20, 21, 21), square(20.2, 20.2, 20.4, 20.4), square(50, 50, 51, 51)};

TEST(enclosing_rings_finds_the_ring_immediately_around_each) {
    const std::vector<int> around = enclosing_rings(kBaarle);
    CHECK(around == std::vector<int>({-1, 0, 1, -1, 3, -1}));
    CHECK(ring_depths(around) == std::vector<int>({0, 1, 2, 0, 1, 0}));
}

TEST(enclosing_rings_ignores_ring_order) {
    const std::vector<Ring> rings = {kBaarle[2], kBaarle[4], kBaarle[1], kBaarle[5], kBaarle[0], kBaarle[3]};
    CHECK(enclosing_rings(rings) == std::vector<int>({2, 5, 4, -1, -1, -1}));
}

TEST(enclosing_rings_needs_the_interior_point_inside_not_just_the_box) {
    // A small square in the notch of a C: inside the C's box, outside the C.
    const Ring c = {{0, 0}, {0, 10}, {2, 10}, {2, 2}, {8, 2}, {8, 10}, {10, 10}, {10, 0}, {0, 0}};
    CHECK(enclosing_rings(std::vector<Ring>{c, square(4, 5, 6, 7)}) == std::vector<int>({-1, -1}));
}

TEST(enclosing_rings_nests_a_hole_touching_its_outer_ring) {
    // The enclave's corner sits on the outer ring (the rings share a node).
    const Ring outer = {{0, 0}, {0, 10}, {10, 10}, {10, 0}, {5, 0}, {0, 0}};
    const Ring touching = {{5, 0}, {7, 3}, {3, 3}, {5, 0}};
    CHECK(enclosing_rings(std::vector<Ring>{outer, touching}) == std::vector<int>({-1, 0}));
}

TEST(nest_rings_makes_even_depths_outer_rings_and_odd_depths_their_holes) {
    const std::vector<int> around = enclosing_rings(kBaarle);
    const std::vector<NestedPolygon<LatLng>> polygons = nest_rings(kBaarle, around, ring_depths(around));
    CHECK_EQ(polygons.size(), 4u);
    CHECK(polygons[0].outer == kBaarle[0]);
    CHECK(polygons[0].holes == std::vector<Ring>({kBaarle[1]}));
    CHECK(polygons[1].outer == kBaarle[2]);  // the counter-enclave
    CHECK(polygons[1].holes.empty());
    CHECK(polygons[2].outer == kBaarle[3]);
    CHECK(polygons[2].holes == std::vector<Ring>({kBaarle[4]}));
    CHECK(polygons[3].outer == kBaarle[5]);
    CHECK(polygons[3].holes.empty());
}

// The builder nests packed rings; every double it computes with is lat() or
// lon() of a vertex, so the answers are those of the rings' doubles.
TEST(packed_rings_nest_and_measure_as_their_doubles) {
    std::mt19937 rng(7);
    std::uniform_int_distribution<int32_t> jitter(-50000, 50000);
    std::vector<std::vector<PackedLocation>> packed;
    for (const Ring& r : kBaarle) {
        std::vector<PackedLocation> p;
        for (const auto& [lat, lng] : r) p.push_back(pack_location(lat, lng));
        for (size_t i = 0; i + 1 < p.size(); i++) {
            p[i].lat_e7 += jitter(rng);
            p[i].lon_e7 += jitter(rng);
        }
        p.back() = p.front();
        packed.push_back(std::move(p));
    }
    std::vector<Ring> doubles;
    for (const auto& p : packed) doubles.push_back(unpack_ring(p));

    const std::vector<int> around = enclosing_rings(packed);
    CHECK(around == std::vector<int>({-1, 0, 1, -1, 3, -1}));
    CHECK(around == enclosing_rings(doubles));
    const std::vector<RingSpan> whole = {{0, 5}};
    for (size_t i = 0; i < packed.size(); i++) {
        CHECK(signed_area2(packed[i]) == signed_area2(doubles[i]));
        CHECK(interior_point(packed[i].data(), whole) == interior_point(doubles[i].data(), whole));
    }
    const std::vector<NestedPolygon<PackedLocation>> polygons = nest_rings(packed, around, ring_depths(around));
    REQUIRE(polygons.size() == 4u);
    CHECK(polygons[0].outer == packed[0]);
    CHECK(polygons[0].holes == std::vector<std::vector<PackedLocation>>({packed[1]}));
    CHECK(polygons[2].holes == std::vector<std::vector<PackedLocation>>({packed[4]}));
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
    // In the notch, yet counted inside: outside the outer ring, inside the hole.
    CHECK(!ring_contains_latlng(outer, 4.6, 7.8) && ring_contains_latlng(hole, 4.6, 7.8));
}

TEST(rings_nest_rejects_overlapping_holes) {
    CHECK(!rings_nest(kOuter, {square(2, 2, 5, 5), square(4, 4, 7, 7)}));  // crossing
    CHECK(!rings_nest(kOuter, {square(2, 2, 8, 8), square(4, 4, 6, 6)}));  // one inside the other
    CHECK(!rings_nest(kOuter, {square(4, 4, 6, 6), square(2, 2, 8, 8)}));
    CHECK(!rings_nest(kOuter, {square(2, 2, 4, 4), reversed(square(2, 2, 4, 4))}));  // the same twice
    CHECK(!rings_nest(kOuter, {square(2, 2, 6, 6), square(2, 2, 4, 4)}));  // inside, sharing a corner
}
