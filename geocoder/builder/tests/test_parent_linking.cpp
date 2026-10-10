// parent_linking.h: helpers the parent-linking passes share.
#include "parent_linking.h"

#include <algorithm>
#include <cmath>
#include <random>
#include <vector>

#include "test_framework.h"

namespace {

// Closed unit square in (lat, lng).
const std::vector<NodeCoord> kSquare = {{0, 0}, {0, 1}, {1, 1}, {1, 0}, {0, 0}};

bool square_contains(float lat, float lng) {
    return ring_contains(kSquare.data(), static_cast<uint32_t>(kSquare.size()), lat, lng);
}

// A coordinate on OSM's 1e-7 degree grid, stored as float like admin_vertices.
float grid(double deg) {
    return static_cast<float>(std::round(deg * 1e7) / 1e7);
}

// Random closed ring around (lat, lng) with radius up to `radius` degrees.
std::vector<NodeCoord> new_ring(std::mt19937_64& rng, double lat, double lng, double radius) {
    std::uniform_int_distribution<int> count(3, 40);
    std::uniform_real_distribution<double> unit(0.0, 1.0);
    int n = count(rng);
    std::vector<NodeCoord> ring;
    for (int i = 0; i < n; i++) {
        double angle = 2 * M_PI * unit(rng);
        double r = radius * unit(rng);
        ring.push_back({grid(std::clamp(lat + r * std::sin(angle), -90.0, 90.0)),
                        grid(std::clamp(lng + r * std::cos(angle), -180.0, 180.0))});
    }
    ring.push_back(ring.front());
    return ring;
}

}  // namespace

TEST(ring_contains_interior_and_exterior) {
    CHECK(square_contains(0.5f, 0.5f));
    CHECK(!square_contains(1.5f, 0.5f));
    CHECK(!square_contains(0.5f, -0.5f));
}

TEST(ring_contains_keeps_south_and_west_edges_only) {
    CHECK(square_contains(0.0f, 0.5f));
    CHECK(square_contains(0.5f, 0.0f));
    CHECK(!square_contains(1.0f, 0.5f));
    CHECK(!square_contains(0.5f, 1.0f));
}

TEST(ring_contains_empty_ring_contains_nothing) {
    CHECK(!ring_contains(kSquare.data(), 0, 0.5f, 0.5f));
}

TEST(ring_box_spans_the_vertices_with_latitude_pad) {
    RingBox box = ring_box(kSquare.data(), static_cast<uint32_t>(kSquare.size()));
    CHECK(box.lng_lo == 0.0f);
    CHECK(box.lng_hi == 1.0f);
    CHECK(box.lat_lo == -kRingBoxLatPad);
    CHECK(box.lat_hi == 1.0f + kRingBoxLatPad);
    CHECK(!ring_box_excludes(box, 0.5f, 0.5f));
    CHECK(ring_box_excludes(box, 0.5f, 1.0f));
    CHECK(ring_box_excludes(box, 0.5f, -1e-7f));
    CHECK(ring_box_excludes(box, 1.0f + kRingBoxLatPad, 0.5f));
    CHECK(ring_box_excludes(box, -0.01f, 0.5f));
}

TEST(ring_box_of_an_empty_ring_excludes_everything) {
    RingBox box = ring_box(kSquare.data(), 0);
    CHECK(ring_box_excludes(box, 0.5f, 0.5f));
}

// The pad must cover the interpolation's rounding: hunt for the worst
// overshoot of the latitude ring_contains computes past an edge's ends, on
// long thin edges anywhere on the globe.
TEST(ring_contains_crossing_latitude_stays_within_pad) {
    std::mt19937_64 rng(11);
    std::uniform_real_distribution<double> lat(-90, 90), lng(-180, 180), unit(0, 1);
    float worst = 0;
    for (int i = 0; i < 2000000; i++) {
        NodeCoord a{grid(lat(rng)), grid(lng(rng))};
        NodeCoord b{grid(lat(rng)), grid(a.lng + (unit(rng) < 0.5 ? 1e-7 : 1e-3) * (unit(rng) * 2 - 1))};
        if (a.lng == b.lng) continue;
        // ring_contains interpolates the edge when lo <= p < hi.
        float lo = std::min(a.lng, b.lng), hi = std::max(a.lng, b.lng);
        float p = lo + static_cast<float>(unit(rng)) * (hi - lo);
        if (p >= hi) p = std::nextafter(hi, lo);
        if (p < lo) p = lo;
        float x = (b.lat - a.lat) * (p - a.lng) / (b.lng - a.lng) + a.lat;
        worst = std::max({worst, x - std::max(a.lat, b.lat), std::min(a.lat, b.lat) - x});
    }
    CHECK(worst < 4e-5f);
    CHECK(worst < kRingBoxLatPad / 10);
}

TEST(ring_box_excludes_only_points_outside_the_ring) {
    std::mt19937_64 rng(5);
    std::uniform_real_distribution<double> lat(-89, 89), lng(-179, 179), unit(0, 1);
    int excluded = 0;
    for (int r = 0; r < 3000; r++) {
        double radius = unit(rng) < 0.5 ? 1e-4 : 2.0;
        auto ring = new_ring(rng, lat(rng), lng(rng), radius);
        uint32_t n = static_cast<uint32_t>(ring.size());
        RingBox box = ring_box(ring.data(), n);
        std::vector<NodeCoord> probes;
        for (const auto& v : ring) {
            probes.push_back(v);
            probes.push_back({std::nextafter(v.lat, 100.0f), v.lng});
            probes.push_back({v.lat, std::nextafter(v.lng, -200.0f)});
        }
        for (float plat : {box.lat_lo, box.lat_hi, std::nextafter(box.lat_lo, -100.0f),
                           std::nextafter(box.lat_hi, -100.0f)})
            for (const auto& v : ring) probes.push_back({plat, v.lng});
        for (int k = 0; k < 200; k++) {
            float plat = box.lat_lo + static_cast<float>(unit(rng) * 1.2 - 0.1) * (box.lat_hi - box.lat_lo);
            float plng = box.lng_lo + static_cast<float>(unit(rng) * 1.2 - 0.1) * (box.lng_hi - box.lng_lo);
            probes.push_back({plat, plng});
        }
        for (const auto& p : probes) {
            if (!ring_box_excludes(box, p.lat, p.lng)) continue;
            excluded++;
            CHECK(!ring_contains(ring.data(), n, p.lat, p.lng));
        }
    }
    CHECK(excluded > 100000);
}

namespace {

struct Poly {
    uint8_t level;
    float area;
    bool contains;
};

uint32_t first_containing(const std::vector<uint32_t>& ranked, const std::vector<Poly>& polys) {
    for (uint32_t id : ranked)
        if (polys[id].contains) return id;
    return NO_DATA;
}

}  // namespace

TEST(rank_candidates_without_order_keeps_first_occurrences) {
    std::vector<uint32_t> ids = {7, 3, 7, 9, 3, 1, 9};
    std::vector<std::pair<uint32_t, uint32_t>> scratch;
    rank_candidates(ids, [](uint32_t, uint32_t) { return false; }, scratch);
    CHECK((ids == std::vector<uint32_t>{7, 3, 9, 1}));
}

// The parent passes used to scan every listed id and keep a containing one
// only when strictly better; the ranked first-containing pick must agree on
// every sequence, ties and repeats included.
TEST(rank_candidates_first_containing_matches_strict_improvement_scan) {
    std::mt19937_64 rng(17);
    std::vector<std::pair<uint32_t, uint32_t>> scratch;
    for (int round = 0; round < 20000; round++) {
        uint32_t k = 1 + rng() % 30;
        std::vector<Poly> polys(k);
        for (auto& p : polys) p = {static_cast<uint8_t>(rng() % 5), static_cast<float>(rng() % 6), rng() % 3 == 0};
        std::vector<uint32_t> seq(rng() % 60);
        for (auto& id : seq) id = static_cast<uint32_t>(rng() % k);

        // Smallest area (POI parents).
        float best_area = 1e18f;
        uint32_t expect = NO_DATA;
        for (uint32_t id : seq) {
            if (polys[id].area >= best_area) continue;
            if (polys[id].contains) { best_area = polys[id].area; expect = id; }
        }
        auto ids = seq;
        rank_candidates(ids, [&](uint32_t a, uint32_t b) { return polys[a].area < polys[b].area; }, scratch);
        CHECK_EQ(first_containing(ids, polys), expect);

        // Highest level, then smallest area (way and admin parents).
        uint8_t best_level = 0;
        best_area = 1e18f;
        expect = NO_DATA;
        for (uint32_t id : seq) {
            const auto& p = polys[id];
            if (p.level < best_level || (p.level == best_level && p.area >= best_area)) continue;
            if (p.contains) { best_level = p.level; best_area = p.area; expect = id; }
        }
        ids = seq;
        rank_candidates(ids, [&](uint32_t a, uint32_t b) {
            if (polys[a].level != polys[b].level) return polys[a].level > polys[b].level;
            return polys[a].area < polys[b].area;
        }, scratch);
        CHECK_EQ(first_containing(ids, polys), expect);
    }
}

namespace {

struct Street {
    double d2;
    int64_t osm;
    uint32_t listed;
    bool matches;
};

struct StreetKey {
    double d2;
    int64_t osm;
    uint32_t listed;
    bool operator<(const StreetKey& o) const {
        if (d2 != o.d2) return d2 < o.d2;
        if (osm != o.osm) return osm < o.osm;
        return listed < o.listed;
    }
};

// Streets whose bounds are their d2 less up to `slack`, never below 0.
std::vector<Street> new_streets(std::mt19937_64& rng, double* bounds, double slack) {
    std::vector<Street> streets(rng() % 40);
    for (uint32_t k = 0; k < streets.size(); k++) {
        streets[k] = {static_cast<double>(rng() % 5), static_cast<int64_t>(rng() % 4), k, rng() % 3 == 0};
        bounds[k] = std::max(0.0, streets[k].d2 - slack * static_cast<double>(rng() % 3));
    }
    return streets;
}

}  // namespace

TEST(lazy_ascending_of_nothing_hands_out_nothing) {
    LazyAscending<StreetKey> order;
    order.reset(0, [](uint32_t) { return 0.0; });
    uint32_t item;
    StreetKey key;
    CHECK(!order.next(item, key, [](uint32_t) { return StreetKey{}; }, [](double, const StreetKey&) { return false; }));
}

TEST(lazy_ascending_hands_out_every_item_in_key_order) {
    std::mt19937_64 rng(19);
    LazyAscending<StreetKey> order;
    double bounds[40];
    for (int round = 0; round < 5000; round++) {
        const auto streets = new_streets(rng, bounds, 1.0);
        order.reset(static_cast<uint32_t>(streets.size()), [&](uint32_t k) { return bounds[k]; });
        std::vector<StreetKey> expect, got;
        for (const auto& s : streets) expect.push_back({s.d2, s.osm, s.listed});
        std::sort(expect.begin(), expect.end());
        uint32_t item;
        StreetKey key;
        auto key_of = [&](uint32_t k) { return StreetKey{streets[k].d2, streets[k].osm, k}; };
        auto beyond = [](double bound, const StreetKey& k) { return bound > k.d2; };
        while (order.next(item, key, key_of, beyond)) got.push_back(key);
        CHECK(got.size() == expect.size());
        for (size_t i = 0; i < got.size() && i < expect.size(); i++) CHECK(!(got[i] < expect[i]) && !(expect[i] < got[i]));
    }
}

// Exact bounds: handing out the first item computes the keys of the items
// sharing the least d2 and of no other.
TEST(lazy_ascending_computes_only_the_keys_that_could_come_first) {
    std::mt19937_64 rng(29);
    LazyAscending<StreetKey> order;
    double bounds[40];
    for (int round = 0; round < 5000; round++) {
        const auto streets = new_streets(rng, bounds, 0.0);
        if (streets.empty()) continue;
        order.reset(static_cast<uint32_t>(streets.size()), [&](uint32_t k) { return bounds[k]; });
        int computed = 0;
        uint32_t item;
        StreetKey key;
        CHECK(order.next(item, key, [&](uint32_t k) {
            computed++;
            return StreetKey{streets[k].d2, streets[k].osm, k};
        }, [](double bound, const StreetKey& k) { return bound > k.d2; }));
        double least = 1e18;
        for (const auto& s : streets) least = std::min(least, s.d2);
        int sharing = 0;
        for (const auto& s : streets) sharing += s.d2 == least;
        CHECK_EQ(computed, sharing);
    }
}

// The token-matched street used to come from a visit-order scan keeping a
// matching street only when strictly nearer by (d2, osm_id); the first
// matching street handed out must be the same one, ties included, after
// handing out exactly the streets ahead of it.
TEST(lazy_ascending_first_match_matches_strict_improvement_scan) {
    std::mt19937_64 rng(23);
    LazyAscending<StreetKey> order;
    double bounds[40];
    for (int round = 0; round < 20000; round++) {
        const auto visits = new_streets(rng, bounds, 1.0);

        double best_d2 = 1e18;
        int64_t best_osm = INT64_MAX;
        int64_t expect = -1;
        for (const auto& v : visits) {
            if (!(v.d2 < best_d2 || (v.d2 == best_d2 && v.osm < best_osm)) || !v.matches) continue;
            best_d2 = v.d2;
            best_osm = v.osm;
            expect = v.listed;
        }

        order.reset(static_cast<uint32_t>(visits.size()), [&](uint32_t k) { return bounds[k]; });
        int64_t got = -1;
        int handed = 0;
        uint32_t item;
        StreetKey key;
        while (order.next(item, key, [&](uint32_t k) { return StreetKey{visits[k].d2, visits[k].osm, k}; },
                          [](double bound, const StreetKey& k) { return bound > k.d2; })) {
            handed++;
            if (visits[item].matches) {
                got = item;
                break;
            }
        }
        CHECK_EQ(got, expect);
        if (got >= 0) {
            const StreetKey match{visits[got].d2, visits[got].osm, static_cast<uint32_t>(got)};
            int ahead = 0;
            for (const auto& v : visits) ahead += StreetKey{v.d2, v.osm, v.listed} < match;
            CHECK_EQ(handed, ahead + 1);
        }
    }
}

TEST(keep_first_occurrences_matches_unordered_ranking) {
    std::mt19937_64 rng(37);
    std::vector<std::pair<uint32_t, uint32_t>> scratch;
    std::vector<uint32_t> table;
    for (int round = 0; round < 5000; round++) {
        std::vector<uint32_t> ids(rng() % 300);
        uint32_t range = 1 + static_cast<uint32_t>(rng() % 400);
        for (auto& id : ids) id = rng() % 2 ? static_cast<uint32_t>(rng() % range) : 0xFFFFFFFEu - static_cast<uint32_t>(rng() % range);
        auto expect = ids;
        rank_candidates(expect, [](uint32_t, uint32_t) { return false; }, scratch);
        keep_first_occurrences(ids, table);
        CHECK(ids == expect);
    }
}

TEST(gallop_lower_bound_matches_lower_bound) {
    std::mt19937_64 rng(31);
    for (int round = 0; round < 2000; round++) {
        std::vector<uint32_t> v(rng() % 300);
        for (auto& x : v) x = static_cast<uint32_t>(rng() % 50);
        std::sort(v.begin(), v.end());
        size_t from = v.empty() ? 0 : rng() % (v.size() + 1);
        for (uint32_t value = 0; value <= 51; value++) {
            auto expect = std::lower_bound(v.begin() + from, v.end(), value);
            auto got = gallop_lower_bound(v.begin() + from, v.end(), value, std::less<uint32_t>());
            CHECK(got == expect);
        }
    }
}

TEST(street_d2_is_zero_on_the_street_and_grows_off_it) {
    const std::vector<NodeCoord> street = {{0, 0}, {0, 1}, {1, 1}};
    const double cos_lat = std::cos(0.5 * M_PI / 180.0);
    CHECK(street_d2(street.data(), 3, 0.0f, 0.5f, 1.0) == 0.0);
    CHECK(street_d2(street.data(), 3, 0.5f, 1.0f, 1.0) == 0.0);
    CHECK(street_d2(street.data(), 3, 0.5f, 0.5f, cos_lat) > 0.0);
    CHECK(street_d2(street.data(), 1, 0.0f, 0.0f, 1.0) == INFINITY);
}

// The sweep skips a street when its box is farther than a street already
// met by kStreetBoxSlack: sound only if no box ever lies farther than its
// street by that much, near the antimeridian, the equator and the poles too.
TEST(street_box_d2_never_exceeds_street_d2_by_the_slack) {
    std::mt19937_64 rng(41);
    std::uniform_real_distribution<double> unit(0.0, 1.0);
    double worst = 0;
    for (int round = 0; round < 200000; round++) {
        const double lat0 = round % 4 == 0 ? 0.0 : -89.0 + 178.0 * unit(rng);
        const double lng0 = round % 4 == 1 ? 179.99 : (round % 4 == 2 ? 0.0 : -180.0 + 360.0 * unit(rng));
        const double reach = std::pow(10.0, -6 + 5 * unit(rng));
        std::vector<NodeCoord> street(2 + rng() % 8);
        for (auto& nd : street)
            nd = {static_cast<float>(std::clamp(lat0 + reach * (2 * unit(rng) - 1), -90.0, 90.0)),
                  static_cast<float>(std::clamp(lng0 + reach * (2 * unit(rng) - 1), -180.0, 180.0))};
        const float plat = static_cast<float>(std::clamp(lat0 + reach * (2 * unit(rng) - 1), -90.0, 90.0));
        const float plng = static_cast<float>(std::clamp(lng0 + reach * (2 * unit(rng) - 1), -180.0, 180.0));
        const double cos_lat = std::cos(plat * M_PI / 180.0);
        const uint16_t n = static_cast<uint16_t>(street.size());
        const double box = street_box_d2(street_box(street.data(), n), plat, plng, cos_lat);
        const double d2 = street_d2(street.data(), n, plat, plng, cos_lat);
        worst = std::max(worst, std::sqrt(box) - std::sqrt(d2));
    }
    CHECK(worst < kStreetBoxSlack / 100);
}
