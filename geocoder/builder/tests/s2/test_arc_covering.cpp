// The admin covering's parts (admin_cover.h): an arc's cells (admin_arc_cells)
// are its edges' cells whichever way it runs, and a polygon's cells
// (admin_polygon_cells) are refused from a boundary out of order.
#include <algorithm>
#include <cstdint>
#include <random>
#include <stdexcept>
#include <vector>

#include "admin_cover.h"
#include "exact_pip.h"
#include "test_framework.h"
#include "types.h"

namespace {

using Ring = std::vector<PackedLocation>;

}  // namespace

TEST(an_arcs_cells_are_its_edges_cells_whichever_way_it_runs) {
    std::mt19937_64 rng(1108);
    std::uniform_real_distribution<double> unit(0, 1);
    size_t cells = 0;
    for (int round = 0; round < 200; round++) {
        // A random walk: short and long steps, repeated vertices, the
        // antimeridian taken the long way round, up to the poles.
        Ring line;
        double lat = -89 + 178 * unit(rng), lng = -179 + 358 * unit(rng);
        const double step = round % 4 == 0 ? 2.0 : round % 4 == 1 ? 0.05 : 0.002;
        const int n = 1 + static_cast<int>(rng() % 30);
        for (int k = 0; k < n; k++) {
            line.push_back(point_e7(std::clamp(lat, -90.0, 90.0), std::clamp(lng, -180.0, 180.0)));
            if (rng() % 8 == 0) line.push_back(line.back());
            lat += step * (2 * unit(rng) - 1);
            lng += step * (2 * unit(rng) - 1);
            if (round % 10 == 0 && k == n / 2) lng = lng > 0 ? -179.5 : 179.5;
        }
        // The long way round at coarse levels only: 358 degrees of 600 m cells is slow.
        const int level = 6 + static_cast<int>(rng() % (round % 10 == 0 ? 5 : 9));
        std::vector<uint64_t> whole, backwards, edges;
        admin_arc_cells(line.data(), static_cast<uint32_t>(line.size()), level, ADMIN_COVER_REACH_RAD, whole);
        Ring reversed(line.rbegin(), line.rend());
        admin_arc_cells(reversed.data(), static_cast<uint32_t>(reversed.size()), level, ADMIN_COVER_REACH_RAD,
                        backwards);
        for (size_t k = 1; k < line.size(); k++)
            admin_arc_cells(line.data() + k - 1, 2, level, ADMIN_COVER_REACH_RAD, edges);
        std::sort(edges.begin(), edges.end());
        edges.erase(std::unique(edges.begin(), edges.end()), edges.end());
        CHECK(std::is_sorted(whole.begin(), whole.end()));
        CHECK(std::adjacent_find(whole.begin(), whole.end()) == whole.end());
        CHECK(whole == edges);
        CHECK(whole == backwards);
        // Appended after what `out` holds already, which stays.
        std::vector<uint64_t> after{7, 3};
        admin_arc_cells(line.data(), static_cast<uint32_t>(line.size()), level, ADMIN_COVER_REACH_RAD, after);
        CHECK(std::vector<uint64_t>(after.begin(), after.begin() + 2) == std::vector<uint64_t>({7, 3}));
        CHECK(std::vector<uint64_t>(after.begin() + 2, after.end()) == whole);
        cells += whole.size();
    }
    CHECK(cells > 2000);
    // One vertex, or one point twice, has no edge and no cells.
    std::vector<uint64_t> none;
    const Ring point{point_e7(10, 20), point_e7(10, 20)};
    admin_arc_cells(point.data(), 1, 10, ADMIN_COVER_REACH_RAD, none);
    admin_arc_cells(point.data(), 2, 10, ADMIN_COVER_REACH_RAD, none);
    CHECK(none.empty());
}

TEST(a_polygons_cells_are_refused_from_a_boundary_out_of_order) {
    const Ring ring{point_e7(10, 20), point_e7(10, 20.1), point_e7(10.1, 20.1), point_e7(10, 20)};
    const RingSpan span{0, static_cast<uint32_t>(ring.size())};
    std::vector<uint64_t> boundary;
    admin_arc_cells(ring.data(), static_cast<uint32_t>(ring.size()), 10, ADMIN_COVER_REACH_RAD, boundary);
    REQUIRE(boundary.size() >= 2);
    CHECK(!admin_polygon_cells(boundary, ring.data(), &span, 1, 10).empty());
    for (const std::vector<uint64_t>& bad : {std::vector<uint64_t>{boundary[1], boundary[0]},
                                             std::vector<uint64_t>{boundary[0], boundary[0]}}) {
        bool refused = false;
        try {
            admin_polygon_cells(bad, ring.data(), &span, 1, 10);
        } catch (const std::logic_error&) {
            refused = true;
        }
        CHECK(refused);
    }
    CHECK(admin_polygon_cells(boundary, ring.data(), &span, 0, 10).empty());
}
