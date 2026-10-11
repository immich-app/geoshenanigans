// RingEdgeIndex (ring_contains.h) and PolygonIndex (continent_boundaries.h)
// answer point-in-ring tests from a subset of the edges; they must give the
// full scan's answer for every point.
#include "continent_boundaries.h"
#include "ring_contains.h"

#include <cmath>
#include <limits>
#include <random>

#include "test_framework.h"

TEST(ring_edge_index_matches_the_full_scan) {
    std::mt19937 rng(8);
    std::uniform_real_distribution<float> step(-0.02f, 0.02f);
    size_t inside = 0, outside = 0;
    for (int round = 0; round < 60; round++) {
        // A wandering ring (self-crossings and all), sometimes with a jump
        // across most of the longitude range like an antimeridian edge.
        uint32_t n = 3 + rng() % (round % 10 == 0 ? 20000 : 400);
        std::vector<NodeCoord> ring(n);
        float lat = 10, lng = 20;
        for (auto& v : ring) {
            lat += step(rng);
            lng += step(rng);
            if (rng() % 500 == 0) lng += rng() % 2 ? 300.0f : -300.0f;
            v = {lat, lng};
        }
        RingEdgeIndex index(ring.data(), n);
        float lat_lo = ring[0].lat, lat_hi = lat_lo, lng_lo = ring[0].lng, lng_hi = lng_lo;
        for (const auto& v : ring) {
            lat_lo = std::min(lat_lo, v.lat); lat_hi = std::max(lat_hi, v.lat);
            lng_lo = std::min(lng_lo, v.lng); lng_hi = std::max(lng_hi, v.lng);
        }
        std::uniform_real_distribution<float> plat(lat_lo - 0.1f, lat_hi + 0.1f), plng(lng_lo - 0.1f, lng_hi + 0.1f);
        for (int q = 0; q < 2000; q++) {
            float qlat = plat(rng), qlng = plng(rng);
            if (q % 7 == 0) qlng = ring[rng() % n].lng;  // on a vertex's longitude
            if (q % 11 == 0) qlat = ring[rng() % n].lat;
            bool want = ring_contains(ring.data(), n, qlat, qlng);
            CHECK_EQ(index.contains(qlat, qlng), want);
            (want ? inside : outside)++;
        }
        const float nan = std::numeric_limits<float>::quiet_NaN();
        CHECK_EQ(index.contains(lat, nan), ring_contains(ring.data(), n, lat, nan));
    }
    CHECK(inside > 1000 && outside > 1000);
}

TEST(polygon_index_matches_point_in_polygon) {
    std::mt19937 rng(9);
    std::uniform_real_distribution<double> step(-3.0, 3.0);
    size_t inside = 0, outside = 0;
    for (int round = 0; round < 60; round++) {
        uint32_t n = 3 + rng() % 150;
        std::vector<std::pair<double, double>> poly(n);
        double lat = 10, lng = 20;
        for (auto& v : poly) {
            lat += step(rng);
            lng += step(rng);
            v = {lat, lng};
        }
        // Closed like the continent polygons: the last vertex repeats the first.
        if (round % 2) poly.push_back(poly.front());
        PolygonIndex index(poly);
        double lat_lo = poly[0].first, lat_hi = lat_lo, lng_lo = poly[0].second, lng_hi = lng_lo;
        for (const auto& v : poly) {
            lat_lo = std::min(lat_lo, v.first); lat_hi = std::max(lat_hi, v.first);
            lng_lo = std::min(lng_lo, v.second); lng_hi = std::max(lng_hi, v.second);
        }
        std::uniform_real_distribution<double> plat(lat_lo - 1, lat_hi + 1), plng(lng_lo - 1, lng_hi + 1);
        for (int q = 0; q < 3000; q++) {
            double qlat = plat(rng), qlng = plng(rng);
            if (q % 7 == 0) qlat = poly[rng() % poly.size()].first;  // on a vertex's latitude
            if (q % 11 == 0) qlng = poly[rng() % poly.size()].second;
            bool want = point_in_polygon(qlat, qlng, poly);
            CHECK_EQ(index.contains(qlat, qlng), want);
            (want ? inside : outside)++;
        }
        const double nan = std::numeric_limits<double>::quiet_NaN();
        CHECK_EQ(index.contains(nan, lng), point_in_polygon(nan, lng, poly));
        CHECK_EQ(index.contains(lat, nan), point_in_polygon(lat, nan, poly));
    }
    CHECK(inside > 1000 && outside > 1000);
    const std::vector<std::pair<double, double>> none;
    CHECK_EQ(PolygonIndex(none).contains(1, 2), point_in_polygon(1, 2, none));
}

TEST(ring_edge_index_handles_a_ring_of_one_longitude) {
    const std::vector<NodeCoord> ring = {{0, 5}, {1, 5}, {2, 5}};
    RingEdgeIndex index(ring.data(), 3);
    CHECK_EQ(index.contains(1, 5), ring_contains(ring.data(), 3, 1, 5));
    CHECK_EQ(index.contains(1, 4), false);
}
