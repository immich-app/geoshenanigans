// polygon_relations.h: ST_Contains and ST_Equals on the exact e7 rings, in
// integers, as the rank pass asks them of PostGIS's answers: holes either
// side, shared borders, touching vertices, multipolygons, rings rotated and
// reversed; the point locator and the edge grid against plain scans.
#include "polygon_relations.h"

#include <algorithm>
#include <cmath>
#include <random>
#include <vector>

#include "parsed_data.h"
#include "test_framework.h"

namespace {

using Ring = std::vector<PackedLocation>;

// A closed ring through (lat, lng) pairs given in whole e7 units.
Ring ring_of(std::initializer_list<std::pair<int32_t, int32_t>> points) {
    Ring ring;
    for (const auto& [lat, lng] : points) ring.push_back({lat, lng});
    ring.push_back(ring.front());
    return ring;
}

// The square (south, west, north, east), counterclockwise from its south-west corner.
Ring square(int32_t s, int32_t w, int32_t n, int32_t e) { return ring_of({{s, w}, {s, e}, {n, e}, {n, w}}); }

// Polygons of an outer ring and its holes, held as AdminExactRings holds them.
struct Polygons {
    AdminExactRings rings;

    uint32_t add(const Ring& outer, const std::vector<Ring>& holes = {}) {
        rings.append(outer, holes);
        return static_cast<uint32_t>(rings.polygons.size() - 1);
    }
    RingPolygon operator[](uint32_t pid) const {
        return {rings.vertices.data(), rings.rings_of(pid), rings.polygons[pid].ring_count};
    }
    bool contains(uint32_t c, uint32_t b) const { return polygon_contains(PolygonIndex((*this)[c]), PolygonIndex((*this)[b])); }
    bool equal(std::vector<uint32_t> a, std::vector<uint32_t> b) const {
        std::vector<RingPolygon> pa, pb;
        for (uint32_t p : a) pa.push_back((*this)[p]);
        for (uint32_t p : b) pb.push_back((*this)[p]);
        return polygons_equal(pa, pb);
    }
};

}  // namespace

TEST(a_polygon_contains_one_inside_it_and_not_one_across_its_border) {
    Polygons p;
    const uint32_t outer = p.add(square(0, 0, 1000, 1000));
    const uint32_t inside = p.add(square(200, 200, 800, 800));
    const uint32_t across = p.add(square(500, 500, 1500, 1500));
    const uint32_t apart = p.add(square(2000, 2000, 3000, 3000));
    CHECK(p.contains(outer, inside));
    CHECK(!p.contains(inside, outer));
    CHECK(!p.contains(outer, across));
    CHECK(!p.contains(outer, apart));
}

TEST(a_polygon_contains_one_on_its_border_from_inside_and_itself) {
    // ST_Contains holds where the inner polygon shares edges with the outer one, and of a polygon
    // and itself: the rank pass leaves out equal polygons itself (polygons_equal).
    Polygons p;
    const uint32_t outer = p.add(square(0, 0, 1000, 1000));
    const uint32_t corner = p.add(square(0, 0, 500, 500));
    const uint32_t strip = p.add(ring_of({{0, 0}, {0, 300}, {0, 1000}, {1000, 1000}, {1000, 0}}));
    CHECK(p.contains(outer, corner));
    CHECK(p.contains(outer, outer));
    // The same square with an extra vertex on its south side.
    CHECK(p.contains(outer, strip));
    CHECK(p.contains(strip, outer));
}

TEST(a_polygon_does_not_contain_its_neighbour_across_a_shared_border) {
    Polygons p;
    const uint32_t west = p.add(square(0, 0, 1000, 1000));
    const uint32_t east = p.add(square(0, 1000, 1000, 2000));
    const uint32_t east_half = p.add(square(250, 1000, 750, 1500));
    CHECK(!p.contains(west, east));
    CHECK(!p.contains(west, east_half));
    CHECK(p.contains(east, east_half));
}

TEST(a_hole_inside_the_inner_polygon_breaks_containment_and_one_beside_it_does_not) {
    Polygons p;
    const uint32_t holed = p.add(square(0, 0, 1000, 1000), {square(400, 400, 600, 600)});
    const uint32_t round_hole = p.add(square(300, 300, 700, 700));
    const uint32_t beside_hole = p.add(square(100, 100, 300, 300));
    const uint32_t the_hole = p.add(square(400, 400, 600, 600));
    // The hole's corner touches the inner square's corner.
    const uint32_t touching_hole = p.add(square(200, 200, 400, 400));
    CHECK(!p.contains(holed, round_hole));
    CHECK(p.contains(holed, beside_hole));
    CHECK(!p.contains(holed, the_hole));
    CHECK(p.contains(holed, touching_hole));
}

TEST(a_hole_whose_edges_lie_on_the_inner_polygon_still_breaks_containment) {
    // The hole's corners all sit on the inner square's border, so no vertex of the hole is inside
    // it, but the hole's edges cut through it.
    Polygons p;
    const uint32_t holed = p.add(square(0, 0, 1000, 1000), {ring_of({{400, 500}, {500, 600}, {600, 500}, {500, 400}})});
    const uint32_t inner = p.add(square(400, 400, 600, 600));
    CHECK(!p.contains(holed, inner));
}

TEST(an_inner_polygon_with_a_hole_lies_in_an_outer_one_without) {
    Polygons p;
    const uint32_t outer = p.add(square(0, 0, 1000, 1000));
    const uint32_t ring = p.add(square(100, 100, 900, 900), {square(300, 300, 700, 700)});
    CHECK(p.contains(outer, ring));
    CHECK(!p.contains(ring, outer));
}

TEST(a_polygon_holds_a_triangle_with_a_corner_on_its_reflex_vertex_and_not_one_past_it) {
    // An L, the square (0, 0, 1000, 1000) without its north-east quarter, whose reflex vertex is
    // (500, 500). A triangle with a corner there lies inside it; one whose edge runs from (0,
    // 1000) through it to (1000, 0) takes in the missing quarter.
    Polygons p;
    const uint32_t l = p.add(ring_of({{0, 0}, {0, 1000}, {500, 1000}, {500, 500}, {1000, 500}, {1000, 0}}));
    const uint32_t notch = p.add(ring_of({{200, 800}, {500, 500}, {200, 200}}));
    const uint32_t under = p.add(ring_of({{0, 0}, {0, 1000}, {1000, 0}}));
    const uint32_t through = p.add(ring_of({{0, 1000}, {1000, 1000}, {1000, 0}}));
    CHECK(p.contains(l, notch));
    CHECK(p.contains(l, under));
    CHECK(!p.contains(l, through));
}

TEST(an_edge_through_two_reflex_vertices_leaves_the_polygon_without_crossing_an_edge) {
    // A square with a V-shaped notch from its north side, whose bottom is (400, 500) at lng 500
    // and whose sides pass (500, 400) and (500, 600). The triangle's edge along lat 500 from lng
    // 200 to 800 touches the notch's border at those two vertices and crosses no edge of it, its
    // vertices all inside the square, but between them it runs through the notch.
    Polygons p;
    const uint32_t notched = p.add(
        ring_of({{0, 0}, {0, 1000}, {1000, 1000}, {1000, 700}, {500, 600}, {400, 500}, {500, 400}, {1000, 300}, {1000, 0}}));
    const uint32_t across = p.add(ring_of({{500, 200}, {100, 500}, {500, 800}}));
    const uint32_t below = p.add(ring_of({{300, 200}, {100, 500}, {300, 800}}));
    CHECK(!p.contains(notched, across));
    CHECK(p.contains(notched, below));
}

TEST(a_polygon_is_not_inside_the_hole_it_fills) {
    Polygons p;
    const uint32_t holed = p.add(square(0, 0, 1000, 1000), {square(300, 300, 700, 700)});
    const uint32_t plug = p.add(square(300, 300, 700, 700));
    const uint32_t plug_holed = p.add(square(0, 0, 1000, 1000), {square(300, 300, 700, 700)});
    CHECK(!p.contains(holed, plug));
    CHECK(!p.contains(plug, holed));
    CHECK(p.contains(holed, plug_holed));
}

TEST(a_multipolygon_contains_one_whose_every_part_lies_in_one_of_its_parts) {
    Polygons p;
    const uint32_t west = p.add(square(0, 0, 1000, 1000)), east = p.add(square(0, 2000, 1000, 3000));
    const uint32_t in_west = p.add(square(100, 100, 200, 200)), in_east = p.add(square(100, 2100, 200, 2200));
    const uint32_t between = p.add(square(100, 1100, 200, 1200));
    std::vector<PolygonIndex> index;
    for (uint32_t k = 0; k <= between; k++) index.emplace_back(p[k]);
    auto parts = [&](std::initializer_list<uint32_t> ids) {
        std::vector<const PolygonIndex*> out;
        for (uint32_t k : ids) out.push_back(&index[k]);
        return out;
    };
    CHECK(multipolygon_contains(parts({west, east}), parts({in_west, in_east})));
    CHECK(!multipolygon_contains(parts({west, east}), parts({in_west, between})));
    CHECK(!multipolygon_contains(parts({in_west, in_east}), parts({west, east})));
}

TEST(polygons_are_equal_with_the_same_rings_whatever_their_rotation_and_direction) {
    Polygons p;
    const uint32_t a = p.add(ring_of({{0, 0}, {0, 1000}, {1000, 1000}, {1000, 0}}), {square(300, 300, 700, 700)});
    const uint32_t rotated = p.add(ring_of({{1000, 1000}, {1000, 0}, {0, 0}, {0, 1000}}), {square(300, 300, 700, 700)});
    const uint32_t reversed = p.add(ring_of({{1000, 0}, {1000, 1000}, {0, 1000}, {0, 0}}),
                                    {ring_of({{700, 300}, {700, 700}, {300, 700}, {300, 300}})});
    const uint32_t no_hole = p.add(square(0, 0, 1000, 1000));
    // An extra vertex on a side makes the same area a different ring: PostGIS's ST_Equals would
    // call them equal, the rank pass doesn't.
    const uint32_t extra_vertex = p.add(ring_of({{0, 0}, {0, 500}, {0, 1000}, {1000, 1000}, {1000, 0}}),
                                        {square(300, 300, 700, 700)});
    const uint32_t moved = p.add(ring_of({{0, 0}, {0, 1000}, {1000, 1000}, {1000, 1}}), {square(300, 300, 700, 700)});
    CHECK(p.equal({a}, {rotated}));
    CHECK(p.equal({a}, {reversed}));
    CHECK(!p.equal({a}, {no_hole}));
    CHECK(!p.equal({a}, {extra_vertex}));
    CHECK(!p.equal({a}, {moved}));
    // Multipolygons: the same parts in another order.
    const uint32_t far = p.add(square(5000, 5000, 6000, 6000)), far_again = p.add(square(5000, 5000, 6000, 6000));
    CHECK(p.equal({a, far}, {far_again, rotated}));
    CHECK(!p.equal({a, far}, {rotated}));
    CHECK(!p.equal({a, far}, {a, a}));
}

TEST(the_point_locator_agrees_with_a_plain_scan_on_random_rings) {
    std::mt19937_64 rng(20261123);
    for (int round = 0; round < 40; round++) {
        Polygons p;
        // A star of up to 300 vertices round (0, 0) with a smaller star inside as its hole, on a
        // grid of 100 so that points fall on edges and vertices too.
        const uint32_t n = 3 + static_cast<uint32_t>(rng() % 300);
        auto star = [&](uint32_t count, int64_t radius) {
            Ring ring;
            for (uint32_t k = 0; k < count; k++) {
                const double a = 2 * M_PI * (k + 0.5 * (rng() % 1000) / 1000.0) / count;
                const double r = radius * (0.6 + 0.4 * (rng() % 1000) / 1000.0);
                ring.push_back({static_cast<int32_t>(std::lround(r * std::sin(a) / 100) * 100),
                                static_cast<int32_t>(std::lround(r * std::cos(a) / 100) * 100)});
            }
            ring.push_back(ring.front());
            return ring;
        };
        const uint32_t pid = p.add(star(n, 100000), {star(3 + n / 4, 30000)});
        const RingPolygon polygon = p[pid];
        const PolygonIndex index(polygon);
        for (int k = 0; k < 400; k++) {
            const HalfPoint at{static_cast<int64_t>(rng() % 4401) * 50 - 110000, static_cast<int64_t>(rng() % 4401) * 50 - 110000};
            // The plain scan: on an edge, else the crossings of every ring.
            bool on = false, inside = false;
            for (uint32_t r = 0; r < polygon.ring_count; r++) {
                const PackedLocation* v = polygon.v + polygon.rings[r].begin;
                for (uint32_t a = 1; a < polygon.rings[r].count; a++) {
                    on |= on_edge_half(v[a - 1], v[a], at);
                    inside ^= edge_crosses_half(v[a - 1], v[a], at);
                }
            }
            const Where expected = on ? Where::BOUNDARY : inside ? Where::INSIDE : Where::OUTSIDE;
            CHECK(index.locate(at) == expected);
        }
    }
}

TEST(the_edge_grid_hands_every_segment_whose_box_meets_the_query_box) {
    std::mt19937_64 rng(7);
    for (int round = 0; round < 20; round++) {
        const uint32_t count = 1 + static_cast<uint32_t>(rng() % 2000);
        std::vector<ExactRingBox> boxes(count);
        for (auto& b : boxes) {
            const int32_t lat = static_cast<int32_t>(rng() % 100000), lng = static_cast<int32_t>(rng() % 100000);
            // Mostly short, some long.
            const int32_t span = rng() % 10 == 0 ? static_cast<int32_t>(rng() % 60000) : static_cast<int32_t>(rng() % 2000);
            b = {lat, lat + static_cast<int32_t>(rng() % (span + 1)), lng, lng + span};
        }
        const ExactRingBox bounds{0, 160000, 0, 160000};
        const EdgeGrid grid(bounds, count, [&](uint32_t k) { return boxes[k]; });
        std::vector<uint32_t> scratch;
        for (int q = 0; q < 50; q++) {
            const int32_t lat = static_cast<int32_t>(rng() % 170000) - 5000, lng = static_cast<int32_t>(rng() % 170000) - 5000;
            const ExactRingBox query{lat, lat + static_cast<int32_t>(rng() % 5000), lng, lng + static_cast<int32_t>(rng() % 5000)};
            std::vector<uint32_t> got;
            grid.for_each_meeting(query, scratch, [&](uint32_t k) { got.push_back(k); });
            std::vector<uint32_t> sorted = got;
            std::sort(sorted.begin(), sorted.end());
            CHECK(std::adjacent_find(sorted.begin(), sorted.end()) == sorted.end());
            for (uint32_t k = 0; k < count; k++) {
                const ExactRingBox& b = boxes[k];
                const bool meets = b.lat_lo <= query.lat_hi && query.lat_lo <= b.lat_hi && b.lng_lo <= query.lng_hi &&
                                   query.lng_lo <= b.lng_hi;
                if (meets) CHECK(std::binary_search(sorted.begin(), sorted.end(), k));
            }
        }
    }
}

TEST(the_edge_grid_files_a_long_edge_in_few_cells_and_still_hands_it) {
    // 2048 boxes nested across nearly the whole bounds, as the long edges of a star of a ring
    // are, and 2048 short ones: filed in every cell they meet, the long ones alone would make
    // some 2048 * 1024 postings. The grid keeps to a few cells an edge, and still hands every
    // box that meets a query.
    std::mt19937_64 rng(11);
    const uint32_t count = 4096;
    std::vector<ExactRingBox> boxes(count);
    for (uint32_t k = 0; k < count; k++) {
        if (k % 2 == 0) {
            const int32_t in = static_cast<int32_t>(k);
            boxes[k] = {in, 100000 - in, in, 100000 - in};
        } else {
            const int32_t lat = static_cast<int32_t>(rng() % 99000), lng = static_cast<int32_t>(rng() % 99000);
            boxes[k] = {lat, lat + static_cast<int32_t>(rng() % 1000), lng, lng + static_cast<int32_t>(rng() % 1000)};
        }
    }
    const EdgeGrid grid(ExactRingBox{0, 100000, 0, 100000}, count, [&](uint32_t k) { return boxes[k]; });
    CHECK(grid.postings() <= size_t(16) * count);
    std::vector<uint32_t> scratch;
    for (int q = 0; q < 200; q++) {
        const int32_t lat = static_cast<int32_t>(rng() % 102000) - 1000, lng = static_cast<int32_t>(rng() % 102000) - 1000;
        const int32_t size = q % 2 ? 0 : static_cast<int32_t>(rng() % 3000);
        const ExactRingBox query{lat, lat + size, lng, lng + size};
        std::vector<uint32_t> got;
        grid.for_each_meeting(query, scratch, [&](uint32_t k) { got.push_back(k); });
        std::sort(got.begin(), got.end());
        CHECK(std::adjacent_find(got.begin(), got.end()) == got.end());
        for (uint32_t k = 0; k < count; k++)
            if (EdgeGrid::meets(boxes[k], query)) CHECK(std::binary_search(got.begin(), got.end(), k));
    }
}
