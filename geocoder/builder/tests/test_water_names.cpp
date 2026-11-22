// water_names.h: a segment's length and its share inside rings, and the
// choice of a river area's name from the lengths its candidate lines credit
// it with.
#include "water_names.h"

#include <cmath>
#include <vector>

#include "test_framework.h"

using namespace water;

namespace {

using Ring = std::vector<PackedLocation>;

// A closed ring of the box [lat, lat + h] x [lng, lng + w], counterclockwise.
Ring box(int32_t lat, int32_t lng, int32_t h, int32_t w) {
    return {{lat, lng}, {lat, lng + w}, {lat + h, lng + w}, {lat + h, lng}, {lat, lng}};
}

// The share of segment a-b inside `rings` (even-odd), every edge fed to the
// clip.
long double share(const std::vector<Ring>& rings, PackedLocation a, PackedLocation b) {
    SegmentClip clip(a, b);
    bool inside = false;
    for (const Ring& r : rings) {
        bool odd = false;
        for (size_t i = 1; i < r.size(); i++) {
            odd ^= clip.start_crossed_by(r[i - 1], r[i]);
            clip.add_crossing(r[i - 1], r[i]);
        }
        inside ^= odd;
    }
    return clip.share(inside);
}

// The share either way along the segment, which must agree.
long double both_ways(const std::vector<Ring>& rings, PackedLocation a, PackedLocation b) {
    const long double there = share(rings, a, b), back = share(rings, b, a);
    CHECK(there == back);
    return there;
}

bool near(long double x, long double want) { return std::fabs(static_cast<double>(x - want)) < 1e-15; }

}  // namespace

TEST(a_segments_length_is_whole_millimetres_on_the_sphere) {
    // 0.001 degrees of latitude: 111.195 m on a sphere of 6,371,008.8 m.
    CHECK_EQ(segment_mm({0, 0}, {10000, 0}), uint64_t(111195));
    CHECK_EQ(segment_mm({10000, 0}, {0, 0}), uint64_t(111195));
    CHECK_EQ(segment_mm({450000000, 70000000}, {450000000, 70000000}), uint64_t(0));
    // A degree of longitude at 60 degrees is half one at the equator, less
    // the half metre the great circle cuts off the parallel.
    const uint64_t equator = segment_mm({0, 0}, {0, 10000000}), sixty = segment_mm({600000000, 0}, {600000000, 10000000});
    CHECK(sixty < equator / 2 && sixty + 1000 > equator / 2);
}

TEST(a_segment_credits_the_share_of_it_inside_the_rings_with_both_ends_and_its_middle_outside) {
    const std::vector<Ring> square = {box(0, 0, 100, 100)};
    // All inside, all outside, and across it with its ends and middle out:
    // the 100 of 700 inside.
    CHECK(both_ways(square, {10, 10}, {90, 60}) == 1);
    CHECK(both_ways(square, {200, 10}, {300, 60}) == 0);
    CHECK(near(both_ways(square, {50, -500}, {50, 200}), 100.0L / 700));
    // Askew through two sides, and in from a side to a point inside.
    CHECK(near(both_ways(square, {-50, 25}, {150, 75}), 0.5L));
    CHECK(near(both_ways(square, {50, -100}, {50, 50}), 50.0L / 150));
    // A hole takes its share off, and a ring inside the hole puts it back.
    const std::vector<Ring> holed = {box(0, 0, 100, 100), box(40, 40, 20, 20), box(45, 45, 10, 10)};
    CHECK(near(both_ways(holed, {50, 0}, {50, 100}), 90.0L / 100));
    // Its share in whole millimetres: the segment's own when it is all in.
    CHECK_EQ(share_mm({0, 0}, {10000, 0}, 1), segment_mm({0, 0}, {10000, 0}));
    CHECK_EQ(share_mm({0, 0}, {10000, 0}, 0.25L), uint64_t(std::llround(segment_length_mm({0, 0}, {10000, 0}) / 4)));
    CHECK_EQ(share_mm({0, 0}, {10000, 0}, 0), uint64_t(0));
}

TEST(a_segment_through_a_vertex_or_along_an_edge_is_inside_where_the_side_to_its_left_is) {
    const std::vector<Ring> square = {box(0, 0, 100, 100)};
    // Through the corner (0, 0) from outside, on into the square, and past
    // the corner touching it alone.
    CHECK(near(both_ways(square, {-100, -100}, {100, 100}), 0.5L));
    CHECK(both_ways(square, {-100, 100}, {100, -100}) == 0);
    // From a vertex or a side into it, or away from it.
    CHECK(both_ways(square, {0, 0}, {50, 50}) == 1);
    CHECK(both_ways(square, {0, 50}, {50, 50}) == 1);
    CHECK(both_ways(square, {0, 50}, {-50, 50}) == 0);
    // Along its sides, run from the end of smaller latitude: the east side
    // (the square to the left, west) in, the west side out; along the south
    // side (run east, the square to the left, north) in, the north side out.
    CHECK(both_ways(square, {0, 100}, {100, 100}) == 1);
    CHECK(both_ways(square, {0, 0}, {100, 0}) == 0);
    CHECK(both_ways(square, {0, 0}, {0, 100}) == 1);
    CHECK(both_ways(square, {100, 0}, {100, 100}) == 0);
    // Along a side past both its ends: the side's length alone, of 300.
    CHECK(near(both_ways(square, {-100, 100}, {200, 100}), 1.0L / 3));
    // Two squares sharing a side: along it, one of them to its left either
    // way, so all of it.
    const std::vector<Ring> pair = {box(0, 0, 100, 100), box(0, 100, 100, 100)};
    CHECK(both_ways(pair, {0, 100}, {100, 100}) == 1);
}

TEST(a_segment_whose_ends_lie_more_than_half_the_world_apart_wraps) {
    CHECK(segment_wraps({0, 1799998000}, {10, -1799999000}));
    CHECK(segment_wraps({10, -1799999000}, {0, 1799998000}));
    CHECK(!segment_wraps({0, -900000000}, {0, 900000000}));
    CHECK(!segment_wraps({0, 1790000000}, {0, 1800000000}));
}

TEST(an_area_takes_the_key_of_most_length_and_that_keys_display_of_most_length) {
    // Object 0: key 1 (500 + 400) beats key 0 (800); key 1's display 3 has
    // 500 against display 2's 400. Object 1: keys tie, the smaller wins;
    // displays tie within it, the smaller wins. Object 2: no credit.
    std::vector<Credit> credits = {
        {0, 0, 1, 800}, {0, 1, 3, 300}, {0, 1, 2, 400}, {0, 1, 3, 200},
        {1, 5, 7, 100}, {1, 4, 9, 50},  {1, 4, 8, 50},
    };
    const std::vector<NameChoice> choices = pick_names(credits, 3, 2);
    REQUIRE(choices.size() == 3u);
    CHECK_EQ(choices[0].key, 1u);
    CHECK_EQ(choices[0].display, 3u);
    CHECK_EQ(choices[0].mm, uint64_t(900));
    CHECK_EQ(choices[0].runner_up_mm, uint64_t(800));
    CHECK_EQ(choices[1].key, 4u);
    CHECK_EQ(choices[1].display, 8u);
    CHECK_EQ(choices[1].mm, uint64_t(100));
    CHECK_EQ(choices[1].runner_up_mm, uint64_t(100));
    CHECK(!choices[2].named());
    CHECK(credits.empty());
}

TEST(the_choice_is_the_same_whatever_order_the_credits_arrive_in) {
    // Enough credits that eight threads sort them in chunks and merge those
    // (parallel_sort), where one sorts them whole.
    std::vector<Credit> credits;
    for (uint32_t i = 0; i < 200000; i++)
        credits.push_back({i % 7, (i * 13) % 5, (i * 7) % 3, uint64_t(i % 11) * 17, (i * 3) % 13});
    std::vector<Credit> reversed(credits.rbegin(), credits.rend());
    const std::vector<NameChoice> a = pick_names(credits, 7, 1), b = pick_names(reversed, 7, 8);
    for (size_t o = 0; o < 7; o++) {
        CHECK(a[o].named());
        CHECK_EQ(a[o].key, b[o].key);
        CHECK_EQ(a[o].display, b[o].display);
        CHECK_EQ(a[o].mm, b[o].mm);
        CHECK_EQ(a[o].runner_up_mm, b[o].runner_up_mm);
        CHECK_EQ(a[o].qid, b[o].qid);
    }
}

TEST(a_choice_carries_the_lowest_wikidata_item_of_its_keys_lines_in_the_object) {
    // Object 0 takes key 1, whose lines in it carry Q9 and Q4 (and none);
    // key 0's Q2 is another river's. Object 1's key has no item.
    std::vector<Credit> credits = {
        {0, 1, 3, 300, 9}, {0, 1, 2, 400, 4}, {0, 1, 3, 200, 0}, {0, 0, 1, 800, 2}, {1, 5, 7, 100, 0},
    };
    const std::vector<NameChoice> choices = pick_names(credits, 2, 1);
    CHECK_EQ(choices[0].key, 1u);
    CHECK_EQ(choices[0].qid, 4u);
    CHECK_EQ(choices[1].qid, 0u);
}
