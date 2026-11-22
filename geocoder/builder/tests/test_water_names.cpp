// water_names.h: a segment's length and midpoint, and the choice of a river
// area's name from the lengths its candidate lines credit it with.
#include "water_names.h"

#include <vector>

#include "test_framework.h"

using namespace water;

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

TEST(a_segments_midpoint_is_on_the_e7_grid_rounded_down) {
    CHECK(segment_midpoint({0, 0}, {3, -3}) == PackedLocation({1, -2}));
    CHECK(segment_midpoint({-5, 7}, {-6, 8}) == PackedLocation({-6, 7}));
    CHECK(segment_midpoint({899999999, 1799999999}, {899999999, 1799999999}) == PackedLocation({899999999, 1799999999}));
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
    std::vector<Credit> credits;
    for (uint32_t i = 0; i < 2000; i++) credits.push_back({i % 7, (i * 13) % 5, (i * 7) % 3, uint64_t(i % 11) * 17});
    std::vector<Credit> reversed(credits.rbegin(), credits.rend());
    const std::vector<NameChoice> a = pick_names(credits, 7, 1), b = pick_names(reversed, 7, 8);
    for (size_t o = 0; o < 7; o++) {
        CHECK_EQ(a[o].key, b[o].key);
        CHECK_EQ(a[o].display, b[o].display);
        CHECK_EQ(a[o].mm, b[o].mm);
    }
}
