// water_split.h: a polygon of many holes cut along the grid into pieces of
// few holes each, which hold what it holds, each point once.
#include "water_split.h"

#include <map>
#include <random>
#include <set>
#include <vector>

#include "test_framework.h"

using namespace water;

namespace {

PackedLocation pt(int32_t lat, int32_t lng) { return {lat, lng}; }

// A ring of the box [lat, lat + h] x [lng, lng + w], counterclockwise, each
// edge one way: `way` + 0..3.
SplitRing box_ring(int32_t lat, int32_t lng, int32_t h, int32_t w, int64_t way) {
    SplitRing r;
    r.v = {pt(lat, lng), pt(lat, lng + w), pt(lat + h, lng + w), pt(lat + h, lng)};
    r.way = {way, way + 1, way + 2, way + 3};
    return r;
}

// A lake of 1.6 x 1.6 degrees from (47, 8) whose shore wiggles, with an n x n
// grid of square islands, each `side` e7 units, and `extra` holes besides.
SplitPolygon lake(int n, int32_t side, std::vector<SplitRing> extra = {}) {
    SplitPolygon p;
    const int32_t lat0 = 470000000, lng0 = 80000000, size = 16000000;
    std::mt19937 rng(7);
    std::uniform_int_distribution<int32_t> jitter(-20000, 20000);
    const PackedLocation corners[] = {pt(lat0, lng0), pt(lat0, lng0 + size), pt(lat0 + size, lng0 + size),
                                      pt(lat0 + size, lng0)};
    for (int e = 0; e < 4; e++)
        for (int k = 0; k < 50; k++) {
            const PackedLocation a = corners[e], b = corners[(e + 1) % 4];
            const int32_t off = k == 0 ? 0 : jitter(rng);
            const int32_t lat = a.lat_e7 + (b.lat_e7 - a.lat_e7) / 50 * k, lng = a.lon_e7 + (b.lon_e7 - a.lon_e7) / 50 * k;
            p.outer.v.push_back(a.lat_e7 == b.lat_e7 ? pt(lat + off, lng) : pt(lat, lng + off));
            p.outer.way.push_back(1000 + e);  // the shore, four ways
        }
    const int32_t pitch = (size - 400000) / n;
    for (int i = 0; i < n; i++)
        for (int j = 0; j < n; j++) {
            SplitRing h = box_ring(lat0 + 200000 + i * pitch + 1234, lng0 + 200000 + j * pitch + 4321, side, side,
                                   10 * (i * n + j) + 10);
            std::reverse(h.v.begin(), h.v.end());
            p.holes.push_back(h);
        }
    for (SplitRing& h : extra) p.holes.push_back(h);
    return p;
}

// How many of `pieces` hold p.
int holders(const std::vector<SplitPolygon>& pieces, PackedLocation p) {
    int n = 0;
    for (const SplitPolygon& q : pieces) n += split_detail::polygon_contains(q, p);
    return n;
}

}  // namespace

TEST(a_polygon_of_few_holes_stays_whole) {
    const SplitPolygon p = lake(3, 100000);
    const std::vector<SplitPolygon> pieces = split_polygon(p, 64);
    REQUIRE(pieces.size() == 1u);
    CHECK_EQ(pieces[0].key, uint64_t(0));
    CHECK_EQ(pieces[0].holes.size(), size_t(9));
}

TEST(a_lake_of_many_islands_is_cut_into_pieces_of_few_holes_holding_what_it_holds) {
    const SplitPolygon p = lake(20, 100000);
    size_t lines = 0;
    const std::vector<SplitPolygon> pieces = split_polygon(p, 64, &lines);
    CHECK(pieces.size() > 6);
    CHECK(lines > 0);
    size_t holes = 0;
    std::set<uint64_t> keys;
    for (const SplitPolygon& q : pieces) {
        CHECK(q.holes.size() <= 64u);
        holes += q.holes.size();
        keys.insert(q.key);
        CHECK(q.key != 0);
    }
    CHECK_EQ(keys.size(), pieces.size());
    // An island a cut crosses is part of the pieces' outer rings.
    CHECK(holes <= 400u && holes > 200u);
    // Their area is the lake's, to the rounding of the cut vertices, and
    // each point the lake holds one holds.
    __int128 area = 0;
    for (const SplitPolygon& q : pieces) area += split_detail::polygon_area2(q);
    const __int128 whole = split_detail::polygon_area2(p), diff = area > whole ? area - whole : whole - area;
    CHECK(diff * 1000000 < whole);
    std::mt19937 rng(11);
    std::uniform_int_distribution<int32_t> lat(469000000, 487000000), lng(79000000, 97000000);
    for (int k = 0; k < 20000; k++) {
        const PackedLocation q = pt(lat(rng), lng(rng));
        CHECK_EQ(holders(pieces, q), split_detail::polygon_contains(p, q) ? 1 : 0);
    }
}

TEST(an_island_a_cut_crosses_is_cut_with_its_lake_and_each_stretch_of_cut_is_shared) {
    // 81 islands, and a long one between two of their rows across the whole
    // lake, which every cut along a meridian crosses.
    std::vector<SplitRing> long_isle = {box_ring(475800000, 80500000, 1000000, 15000000, 9000)};
    std::reverse(long_isle[0].v.begin(), long_isle[0].v.end());
    const SplitPolygon p = lake(9, 100000, long_isle);
    const std::vector<SplitPolygon> pieces = split_polygon(p, 32);
    REQUIRE(pieces.size() > 1);
    size_t holes = 0;
    for (const SplitPolygon& q : pieces) holes += q.holes.size();
    // The long island became part of the pieces' outer rings.
    CHECK(holes < 82u);
    std::mt19937 rng(12);
    std::uniform_int_distribution<int32_t> lat(469000000, 487000000), lng(79000000, 97000000);
    for (int k = 0; k < 20000; k++) {
        const PackedLocation q = pt(lat(rng), lng(rng));
        CHECK_EQ(holders(pieces, q), split_detail::polygon_contains(p, q) ? 1 : 0);
    }
    // Each edge keeps its way or is a cut (a negative way), and every cut
    // edge of one piece is a cut edge of another, walked the other way.
    std::map<std::pair<uint64_t, uint64_t>, int> cut_edges;
    auto key = [](PackedLocation a) { return uint64_t(uint32_t(a.lat_e7)) << 32 | uint32_t(a.lon_e7); };
    for (const SplitPolygon& q : pieces)
        for (const SplitRing* r : [&] {
                 std::vector<const SplitRing*> rings = {&q.outer};
                 for (const SplitRing& h : q.holes) rings.push_back(&h);
                 return rings;
             }()) {
            REQUIRE(r->v.size() == r->way.size());
            for (size_t i = 0; i < r->v.size(); i++) {
                const int64_t w = r->way[i];
                CHECK(w < 0 || (w >= 1000 && w <= 1003) || w >= 10);
                if (w < 0) cut_edges[{key(r->v[i]), key(r->v[(i + 1) % r->v.size()])}]++;
            }
        }
    CHECK(!cut_edges.empty());
    for (const auto& [edge, n] : cut_edges) {
        CHECK_EQ(n, 1);
        CHECK(cut_edges.count({edge.second, edge.first}));
    }
}

TEST(a_hole_outside_its_outer_ring_leaves_the_polygon_whole) {
    std::vector<SplitRing> stray = {box_ring(400000000, 80000000, 100000, 100000, 7000)};
    std::reverse(stray[0].v.begin(), stray[0].v.end());
    const SplitPolygon p = lake(20, 100000, stray);
    const std::vector<SplitPolygon> pieces = split_polygon(p, 64);
    REQUIRE(pieces.size() == 1u);
    CHECK_EQ(pieces[0].key, uint64_t(0));
    CHECK_EQ(pieces[0].holes.size(), size_t(401));
}

TEST(the_same_polygon_cuts_the_same_way_whatever_its_rings_start_and_run) {
    SplitPolygon p = lake(20, 100000), q = p;
    std::rotate(q.outer.v.begin(), q.outer.v.begin() + 17, q.outer.v.end());
    std::rotate(q.outer.way.begin(), q.outer.way.begin() + 17, q.outer.way.end());
    const std::vector<SplitPolygon> a = split_polygon(p, 64), b = split_polygon(q, 64);
    REQUIRE(a.size() == b.size());
    std::multiset<uint64_t> ka, kb;
    for (const SplitPolygon& x : a) ka.insert(x.key);
    for (const SplitPolygon& x : b) kb.insert(x.key);
    CHECK(ka == kb);
}
