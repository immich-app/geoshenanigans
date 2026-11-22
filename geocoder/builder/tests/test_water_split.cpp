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
    // edge of one piece is a cut edge of another.
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
                if (w < 0) cut_edges[std::minmax(key(r->v[i]), key(r->v[(i + 1) % r->v.size()]))]++;
            }
        }
    CHECK(!cut_edges.empty());
    for (const auto& [edge, n] : cut_edges) CHECK_EQ(n, 2);
}

TEST(rings_that_cross_or_run_either_way_are_cut_by_what_they_hold_even_odd) {
    // Holes across the shore and across one another, and holes and an
    // outer ring that run the other way: the pieces hold, even-odd, what the
    // rings hold.
    std::vector<SplitRing> crossing = {box_ring(469900000, 84000000, 300000, 300000, 7000),
                                       box_ring(475000000, 85000000, 400000, 400000, 7100),
                                       box_ring(475200000, 85200000, 400000, 400000, 7200)};
    SplitPolygon p = lake(20, 100000, crossing);
    std::reverse(p.outer.v.begin(), p.outer.v.end());
    for (size_t h = 0; h < p.holes.size(); h += 3) std::reverse(p.holes[h].v.begin(), p.holes[h].v.end());
    const std::vector<SplitPolygon> pieces = split_polygon(p, 64);
    REQUIRE(pieces.size() > 6);
    std::mt19937 rng(13);
    std::uniform_int_distribution<int32_t> lat(469000000, 487000000), lng(79000000, 97000000);
    for (int k = 0; k < 20000; k++) {
        const PackedLocation q = pt(lat(rng), lng(rng));
        CHECK_EQ(holders(pieces, q), split_detail::polygon_contains(p, q) ? 1 : 0);
    }
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

TEST(a_cut_puts_its_vertex_where_it_does_with_the_edge_run_the_other_way) {
    // Half way between two units it rounds away from zero, whichever end
    // the edge starts at and on either side of zero.
    using split_detail::P;
    auto both = [](P a, P b, int64_t line) {
        const P there = split_detail::crossing(a, b, line), back = split_detail::crossing(b, a, line);
        CHECK(there == back);
        CHECK_EQ(there.y, line);
        return there.x;
    };
    CHECK_EQ(both({0, 0}, {1, 2}, 1), int64_t(1));
    CHECK_EQ(both({0, 0}, {-1, 2}, 1), int64_t(-1));
    CHECK_EQ(both({-3, -2}, {-2, 0}, -1), int64_t(-3));
    CHECK_EQ(both({5, -1}, {2, 1}, 0), int64_t(4));
    CHECK_EQ(both({-7, 10}, {6, 12}, 11), int64_t(-1));
    CHECK_EQ(both({1800000000, -900000000}, {1799999999, 900000000}, 0), int64_t(1800000000));
    // Elsewhere the nearest unit.
    CHECK_EQ(both({0, 0}, {10, 3}, 1), int64_t(3));
    CHECK_EQ(both({0, 0}, {-10, 3}, 2), int64_t(-7));
}

TEST(each_stretch_of_cut_is_one_edge_of_both_pieces_beside_it_when_one_side_is_cut_again) {
    // A lake of 1.6 x 1.6 degrees whose 20 islands all lie west of its first
    // cut, a meridian: the west side is cut again along a parallel, the east
    // side not, and the east side's edge along the meridian takes the
    // vertex where the parallel meets it.
    SplitPolygon p;
    p.outer = box_ring(470000000, 80000000, 16000000, 16000000, 1000);
    for (int i = 0; i < 10; i++)
        for (int j = 0; j < 2; j++) {
            SplitRing h = box_ring(470500000 + i * 1500000, 80500000 + j * 1500000, 100000, 100000, 10 * (i * 2 + j) + 10);
            std::reverse(h.v.begin(), h.v.end());
            p.holes.push_back(h);
        }
    size_t lines = 0;
    const std::vector<SplitPolygon> pieces = split_polygon(p, 12, &lines);
    REQUIRE(pieces.size() == 3u);
    CHECK_EQ(lines, size_t(2));
    std::map<std::pair<uint64_t, uint64_t>, int> cut_edges;
    auto key = [](PackedLocation a) { return uint64_t(uint32_t(a.lat_e7)) << 32 | uint32_t(a.lon_e7); };
    for (const SplitPolygon& q : pieces) {
        REQUIRE(q.outer.v.size() == q.outer.way.size());
        for (size_t i = 0; i < q.outer.v.size(); i++)
            if (q.outer.way[i] < 0) cut_edges[std::minmax(key(q.outer.v[i]), key(q.outer.v[(i + 1) % q.outer.v.size()]))]++;
    }
    CHECK_EQ(cut_edges.size(), size_t(3));
    for (const auto& [edge, n] : cut_edges) CHECK_EQ(n, 2);
    std::mt19937 rng(14);
    std::uniform_int_distribution<int32_t> lat(469000000, 487000000), lng(79000000, 97000000);
    for (int k = 0; k < 20000; k++) {
        const PackedLocation q = pt(lat(rng), lng(rng));
        CHECK_EQ(holders(pieces, q), split_detail::polygon_contains(p, q) ? 1 : 0);
    }
}

TEST(pieces_that_hold_other_points_pass_the_check_only_where_those_lie_on_a_cut) {
    // A square of 8 e7 units a side, every unit a vertex, its edges a way of
    // a marine coast's (negative, as cuts are).
    auto square = [](int32_t lat, int32_t lat_hi, int64_t south, int64_t north) {
        SplitRing r;
        for (int32_t g = 0; g < 8; g++) r.v.push_back(pt(lat, g)), r.way.push_back(south);
        for (int32_t a = lat; a < lat_hi; a++) r.v.push_back(pt(a, 8)), r.way.push_back(-7);
        for (int32_t g = 8; g > 0; g--) r.v.push_back(pt(lat_hi, g)), r.way.push_back(north);
        for (int32_t a = lat_hi; a > lat; a--) r.v.push_back(pt(a, 0)), r.way.push_back(-7);
        return r;
    };
    SplitPolygon whole, south, north;
    whole.outer = square(0, 8, -7, -7);
    // Its south half alone: the points north of it are the polygon's and no
    // piece's, though each shares its longitude with a vertex of the piece.
    south.outer = square(0, 4, -7, -7);
    CHECK(!split_detail::pieces_hold(whole, {south}, {}));
    // Pieces that both hold latitude 4, the north one's cut along it: only
    // the points there are held twice, and they lie on the cut.
    const int64_t cut = split_detail::cut_way_of(4, false);
    south.outer = square(0, 5, -7, -7);
    north.outer = square(4, 8, cut, -7);
    CHECK(split_detail::pieces_hold(whole, {south, north}, {cut}));
    // The same pieces, the cut another line's: the points held twice are on
    // none of this split's cuts.
    CHECK(!split_detail::pieces_hold(whole, {south, north}, {split_detail::cut_way_of(5, false)}));
}
