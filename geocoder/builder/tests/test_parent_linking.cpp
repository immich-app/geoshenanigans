// parent_linking.h: helpers the parent-linking passes share.
#include "parent_linking.h"

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
