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
