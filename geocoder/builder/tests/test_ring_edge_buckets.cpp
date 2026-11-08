// PolygonIndex (continent_boundaries.h) and ExactRingEdgeIndex (exact_pip.h),
// the indexes over RingEdgeBuckets (ring_edge_buckets.h), answer point-in-ring
// tests from a subset of the edges; they must give the full scan's answer for
// every point.
#include "continent_boundaries.h"
#include "exact_pip.h"

#include <cmath>
#include <limits>
#include <random>

#include "test_framework.h"

namespace {

// The full scan: ray casting over every edge.
bool reference_point_in_polygon(double lat, double lng, const std::vector<std::pair<double, double>>& poly) {
    bool inside = false;
    size_t n = poly.size();
    for (size_t i = 0, j = n - 1; i < n; j = i++)
        if (polygon_edge_crosses(poly[i], poly[j], lat, lng)) inside = !inside;
    return inside;
}

}  // namespace

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
            bool want = reference_point_in_polygon(qlat, qlng, poly);
            CHECK_EQ(index.contains(qlat, qlng), want);
            (want ? inside : outside)++;
        }
        const double nan = std::numeric_limits<double>::quiet_NaN();
        CHECK_EQ(index.contains(nan, lng), reference_point_in_polygon(nan, lng, poly));
        CHECK_EQ(index.contains(lat, nan), reference_point_in_polygon(lat, nan, poly));
    }
    CHECK(inside > 1000 && outside > 1000);
    const std::vector<std::pair<double, double>> none;
    CHECK_EQ(PolygonIndex(none).contains(1, 2), reference_point_in_polygon(1, 2, none));
}

TEST(exact_ring_edge_index_handles_a_ring_of_one_longitude) {
    const std::vector<PackedLocation> ring = {{0, 5}, {1, 5}, {2, 5}};
    const ExactRingEdgeIndex index(ring.data(), 3);
    CHECK_EQ(index.contains({1, 5}), exact_ring_contains(ring.data(), 3, {1, 5}));
    CHECK_EQ(index.contains({1, 4}), false);
}
