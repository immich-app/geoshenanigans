// exact_pip.h: the e7 ray cast against the shared vectors in geocoder/testdata
// (which the reader is held to as well), its edge index and box against the
// plain scan, and the exact area. In the sanitized suite: the products reach
// 6.5e18 at the poles and the antimeridian.
#include "exact_pip.h"

#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "test_framework.h"

namespace {

constexpr int32_t kLat = 900000000, kLng = 1800000000;

// The data rows of a tab-separated file in geocoder/testdata: '#' lines are
// comments, the first other line names the columns.
std::vector<std::vector<std::string>> testdata_rows(const std::string& name) {
    std::ifstream f(std::string(GEOCODER_TESTDATA_DIR) + "/" + name);
    REQUIRE(f.good());
    std::vector<std::vector<std::string>> rows;
    std::string line;
    bool header = true;
    while (std::getline(f, line)) {
        if (line.empty() || line[0] == '#') continue;
        if (header) {
            header = false;
            continue;
        }
        std::vector<std::string> fields;
        std::stringstream fields_of(line);
        for (std::string field; std::getline(fields_of, field, '\t');) fields.push_back(field);
        rows.push_back(std::move(fields));
    }
    return rows;
}

int32_t e7(const std::string& field) { return static_cast<int32_t>(std::stoll(field)); }

// A closed ring of n distinct-ish vertices round (lat, lng) within `radius`
// e7 units, clamped to the globe: a star when `star`, else a random walk
// that crosses itself.
std::vector<PackedLocation> random_ring(std::mt19937_64& rng, uint32_t n, int32_t lat, int32_t lng, int64_t radius,
                                        bool star) {
    std::uniform_real_distribution<double> unit(0, 1);
    std::vector<PackedLocation> ring;
    auto clamp = [](int64_t v, int32_t lim) { return static_cast<int32_t>(std::clamp<int64_t>(v, -lim, lim)); };
    int64_t walk_lat = lat, walk_lng = lng;
    for (uint32_t k = 0; k < n; k++) {
        if (star) {
            const double a = 2 * M_PI * (k + 0.8 * unit(rng)) / n;
            const double r = radius * (0.1 + unit(rng));
            ring.push_back({clamp(lat + static_cast<int64_t>(r * std::sin(a)), kLat),
                            clamp(lng + static_cast<int64_t>(r * std::cos(a)), kLng)});
        } else {
            walk_lat += static_cast<int64_t>((unit(rng) - 0.5) * radius / 8);
            walk_lng += static_cast<int64_t>((unit(rng) - 0.5) * radius / 8);
            ring.push_back({clamp(walk_lat, kLat), clamp(walk_lng, kLng)});
        }
    }
    ring.push_back(ring.front());
    return ring;
}

// Points to test against a ring: its vertices and their neighbours one step
// off, grid points on its edges and beside them, and points over its box.
std::vector<PackedLocation> probes(std::mt19937_64& rng, const std::vector<PackedLocation>& ring, size_t random) {
    std::vector<PackedLocation> out;
    for (size_t k = 0; k + 1 < ring.size(); k++) {
        const PackedLocation a = ring[k], b = ring[k + 1];
        for (int dlat = -1; dlat <= 1; dlat++)
            for (int dlng = -1; dlng <= 1; dlng++) out.push_back({a.lat_e7 + dlat, a.lon_e7 + dlng});
        // The grid point half-way along the edge, when there is one.
        const int64_t slat = int64_t(a.lat_e7) + b.lat_e7, slng = int64_t(a.lon_e7) + b.lon_e7;
        if (slat % 2 == 0 && slng % 2 == 0) {
            const int32_t mlat = static_cast<int32_t>(slat / 2), mlng = static_cast<int32_t>(slng / 2);
            out.push_back({mlat, mlng});
            out.push_back({mlat + 1, mlng});
            out.push_back({mlat - 1, mlng});
        }
    }
    const ExactRingBox box = exact_ring_box(ring.data(), static_cast<uint32_t>(ring.size()));
    std::uniform_int_distribution<int64_t> lat(box.lat_lo - 10, box.lat_hi + 10), lng(box.lng_lo - 10, box.lng_hi + 10);
    for (size_t k = 0; k < random; k++)
        out.push_back({static_cast<int32_t>(std::clamp<int64_t>(lat(rng), -kLat, kLat)),
                       static_cast<int32_t>(std::clamp<int64_t>(lng(rng), -kLng, kLng))});
    return out;
}

}  // namespace

TEST(exact_edge_crosses_as_the_shared_vectors_say_either_way_round) {
    const auto rows = testdata_rows("pip_edges.tsv");
    CHECK(rows.size() > 500);
    size_t crossing = 0;
    for (const auto& r : rows) {
        REQUIRE(r.size() == 7);
        const PackedLocation a{e7(r[0]), e7(r[1])}, b{e7(r[2]), e7(r[3])}, p{e7(r[4]), e7(r[5])};
        const bool want = r[6] == "1";
        crossing += want;
        CHECK(exact_edge_crosses(a, b, p) == want);
        CHECK(exact_edge_crosses(b, a, p) == want);
    }
    CHECK(crossing > 100);
    CHECK(crossing < rows.size() - 100);
}

TEST(to_e7_rounds_as_the_shared_vectors_say) {
    const auto rows = testdata_rows("point_e7.tsv");
    CHECK(rows.size() > 60);
    for (const auto& r : rows) {
        REQUIRE(r.size() == 3);
        const double value = r[1] == "f32" ? static_cast<double>(std::strtof(r[0].c_str(), nullptr))
                                           : std::strtod(r[0].c_str(), nullptr);
        REQUIRE(r[1] == "f32" || r[1] == "f64");
        CHECK_EQ(to_e7(value), e7(r[2]));
    }
}

TEST(a_point_on_an_edge_two_rings_share_is_inside_exactly_one) {
    // Two quads sharing the slanted edge (0, 0)-(30, 10): every grid point
    // on it, and beside it, is in one of them.
    const std::vector<PackedLocation> west = {{0, 0}, {30, 10}, {30, -20}, {0, -20}, {0, 0}};
    const std::vector<PackedLocation> east = {{0, 0}, {0, 40}, {30, 40}, {30, 10}, {0, 0}};
    auto inside_one = [](const std::vector<PackedLocation>& a, const std::vector<PackedLocation>& b, PackedLocation p) {
        return exact_ring_contains(a.data(), static_cast<uint32_t>(a.size()), p) !=
               exact_ring_contains(b.data(), static_cast<uint32_t>(b.size()), p);
    };
    for (int32_t k = 1; k < 10; k++)
        for (int32_t d = -2; d <= 2; d++) CHECK(inside_one(west, east, {3 * k + d, k}));
    // The same two neighbours at Bern's coordinates, where f32 steps are 3.8e-6 deg.
    const PackedLocation o{469480000, 74470000};
    auto shift = [&](std::vector<PackedLocation> ring) {
        for (auto& v : ring) v = {v.lat_e7 + o.lat_e7, v.lon_e7 + o.lon_e7};
        return ring;
    };
    for (int32_t k = 1; k < 10; k++)
        for (int32_t d = -2; d <= 2; d++)
            CHECK(inside_one(shift(west), shift(east), {o.lat_e7 + 3 * k + d, o.lon_e7 + k}));
}

TEST(exact_ring_edge_index_matches_the_scan_on_edges_and_vertices) {
    std::mt19937_64 rng(20261102);
    size_t inside = 0, tested = 0;
    for (int round = 0; round < 40; round++) {
        const uint32_t n = round % 4 == 0 ? 64 : 64 + static_cast<uint32_t>(rng() % 10000);
        const int64_t radius = round % 3 == 0 ? 100 : (round % 3 == 1 ? 1000000 : 300000000);
        const int32_t lat = static_cast<int32_t>(static_cast<int64_t>(rng() % (2 * kLat)) - kLat);
        const int32_t lng = static_cast<int32_t>(static_cast<int64_t>(rng() % (2 * uint64_t(kLng))) - kLng);
        const auto ring = random_ring(rng, n, lat, lng, radius, round % 2 == 0);
        const ExactRingEdgeIndex index(ring.data(), static_cast<uint32_t>(ring.size()));
        for (const PackedLocation& p : probes(rng, ring, 2000)) {
            const bool want = exact_ring_contains(ring.data(), static_cast<uint32_t>(ring.size()), p);
            CHECK(index.contains(p) == want);
            inside += want;
            tested++;
        }
    }
    CHECK(inside > tested / 20);
}

TEST(exact_ring_box_spans_the_vertices) {
    const std::vector<PackedLocation> square = {{0, 0}, {0, 10}, {10, 10}, {10, 0}, {0, 0}};
    const ExactRingBox box = exact_ring_box(square.data(), static_cast<uint32_t>(square.size()));
    CHECK(box.lat_lo == 0 && box.lat_hi == 10 && box.lng_lo == 0 && box.lng_hi == 10);
    CHECK(!exact_ring_box_excludes(box, {5, 5}));
    CHECK(!exact_ring_box_excludes(box, {0, 0}));  // the south-west corner is inside
    CHECK(exact_ring_box_excludes(box, {5, 10}));
    CHECK(exact_ring_box_excludes(box, {10, 5}));
    CHECK(exact_ring_box_excludes(box, {-1, 5}));
    CHECK(exact_ring_box_excludes(box, {5, -1}));
    CHECK(exact_ring_contains(square.data(), static_cast<uint32_t>(square.size()), {0, 0}));
}

TEST(exact_ring_box_of_an_empty_ring_excludes_everything) {
    const ExactRingBox box = exact_ring_box(nullptr, 0);
    for (const PackedLocation p : {PackedLocation{0, 0}, PackedLocation{kLat, kLng}, PackedLocation{-kLat, -kLng}})
        CHECK(exact_ring_box_excludes(box, p));
}

TEST(exact_ring_box_excludes_only_points_outside_the_ring) {
    std::mt19937_64 rng(5);
    size_t excluded = 0;
    for (int round = 0; round < 300; round++) {
        const uint32_t n = 3 + static_cast<uint32_t>(rng() % 60);
        const int32_t lat = static_cast<int32_t>(static_cast<int64_t>(rng() % (2 * kLat)) - kLat);
        const int32_t lng = static_cast<int32_t>(static_cast<int64_t>(rng() % (2 * uint64_t(kLng))) - kLng);
        const auto ring = random_ring(rng, n, lat, lng, round % 2 ? 50 : 50000000, round % 3 != 0);
        const uint32_t count = static_cast<uint32_t>(ring.size());
        const ExactRingBox box = exact_ring_box(ring.data(), count);
        std::vector<PackedLocation> points = probes(rng, ring, 200);
        // Along the box's sides, one step either way.
        for (const PackedLocation& v : ring)
            for (int32_t d : {-1, 0, 1}) {
                points.push_back({box.lat_lo + d, v.lon_e7});
                points.push_back({box.lat_hi + d, v.lon_e7});
                points.push_back({v.lat_e7, box.lng_lo + d});
                points.push_back({v.lat_e7, box.lng_hi + d});
            }
        for (const PackedLocation& p : points) {
            if (!exact_ring_box_excludes(box, p)) continue;
            excluded++;
            CHECK(!exact_ring_contains(ring.data(), count, p));
        }
    }
    CHECK(excluded > 50000);
}

TEST(exact_polygon_area_takes_the_holes_off_whatever_the_rotation) {
    // A 10 x 10 degree square with a 2 x 3 hole, the rings as e7 points.
    const int32_t d = 10000000;
    std::vector<PackedLocation> v = {{0, 0}, {0, 10 * d}, {10 * d, 10 * d}, {10 * d, 0}, {0, 0},
                                     {d, d}, {3 * d, d}, {3 * d, 4 * d}, {d, 4 * d}, {d, d}};
    const std::vector<RingSpan> rings = {{0, 5}, {5, 5}};
    CHECK_EQ(exact_polygon_area(v.data(), rings.data(), 2), 94.0f);
    CHECK_EQ(exact_polygon_area(v.data(), rings.data(), 1), 100.0f);
    // Reversed and rotated rings give the same float.
    std::vector<PackedLocation> w = {{10 * d, 10 * d}, {0, 10 * d}, {0, 0}, {10 * d, 0}, {10 * d, 10 * d},
                                     {3 * d, d}, {d, d}, {d, 4 * d}, {3 * d, 4 * d}, {3 * d, d}};
    CHECK_EQ(exact_polygon_area(w.data(), rings.data(), 2), 94.0f);
    // The whole globe as a rectangle: 360 * 180 degrees, past 64 bits doubled.
    const std::vector<PackedLocation> globe = {{-kLat, -kLng}, {-kLat, kLng}, {kLat, kLng}, {kLat, -kLng}, {-kLat, -kLng}};
    const std::vector<RingSpan> one = {{0, 5}};
    CHECK_EQ(exact_polygon_area(globe.data(), one.data(), 1), 64800.0f);
    // Holes larger than their ring leave no negative area.
    const std::vector<RingSpan> swapped = {{5, 5}, {0, 5}};
    CHECK_EQ(exact_polygon_area(v.data(), swapped.data(), 2), 0.0f);
}
