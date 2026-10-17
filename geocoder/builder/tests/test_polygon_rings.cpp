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
    const auto identity = [](const Ring& r) { return r; };
    // Holes given with the outer ring's orientation, one too small to keep.
    const std::vector<Ring> holes = {square(2, 2, 4, 4), {{6, 6}, {6, 7}, {6, 6}}};
    const auto [range, count] = simplify_rings(kOuter, holes, identity);
    CHECK_EQ(count, 1u);
    CHECK_NEAR(polygon_area(range), 96.0, 1e-4);
    const auto v = as_nodes(range);
    CHECK(!range_contains(v, 3, 3));
    CHECK(range_contains(v, 6.5, 6.5));
}

TEST(simplify_rings_without_holes_is_the_simplified_outer_ring) {
    const auto shrink = [](const Ring& r) { return Ring(r.begin(), r.end()); };
    const auto [range, count] = simplify_rings(kOuter, {}, shrink);
    CHECK_EQ(count, 0u);
    CHECK(range == kOuter);
}

TEST(simplify_rings_orders_holes_by_first_vertex) {
    const auto identity = [](const Ring& r) { return r; };
    const std::vector<Ring> holes = {kHoles[1], kHoles[0]};
    const auto [range, count] = simplify_rings(kOuter, holes, identity);
    CHECK_EQ(count, 2u);
    CHECK(range == encode_rings(kOuter, kHoles));
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
