// lune_diag <ring.txt> < points : why a ring that contains a point (the reader's straight-line
// lat/lng test) can be missing from the point's level-10 cell (the builder's S2 covering).
// ring.txt: "lat lng" per vertex (the ring as the reader decodes it). For each point prints:
//   lat lng cell in_cover interior s2_cell_meets_ring s2_contains planar_pip
//   then, for each ring edge whose planar and great-circle versions put the point on different
//   sides (the point is inside that edge's lune): edge index, length km, the planar and
//   great-circle latitude at the point's longitude, the point's distance to the great-circle
//   edge (km) and the lune's depth at that longitude (km).
// cover_polygon is the builder's (geocoder/builder/src/s2_helpers.cpp) at level 10, 200 cells.
#include <cmath>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <memory>
#include <set>
#include <sstream>
#include <unordered_set>
#include <vector>

#include <s2/s1chord_angle.h>
#include <s2/s2cell.h>
#include <s2/s2edge_distances.h>
#include <s2/s2latlng.h>
#include <s2/s2loop.h>
#include <s2/s2polygon.h>
#include <s2/s2region_coverer.h>

static const int kLevel = 10;
static const double kEarthKm = 6371.0;

static std::vector<std::pair<S2CellId, bool>> cover_polygon(const std::vector<std::pair<double, double>>& vertices,
                                                            std::unique_ptr<S2Polygon>& out_poly) {
    std::vector<S2Point> points;
    for (const auto& [lat, lng] : vertices) {
        S2Point p = S2LatLng::FromDegrees(lat, lng).ToPoint();
        if (!points.empty() && points.back() == p) continue;
        points.push_back(p);
    }
    if (points.size() > 1 && points.front() == points.back()) points.pop_back();
    if (points.size() < 3) return {};
    auto loop = std::make_unique<S2Loop>(points, S2Debug::DISABLE);
    loop->Normalize();
    out_poly = std::make_unique<S2Polygon>(std::move(loop), S2Debug::DISABLE);
    S2Error error;
    if (out_poly->FindValidationError(&error)) {
        std::cerr << "invalid ring: " << error.text() << "\n";
        return {};
    }
    S2RegionCoverer::Options options;
    options.set_max_level(kLevel);
    options.set_max_cells(200);
    S2RegionCoverer coverer(options);
    S2CellUnion covering = coverer.GetCovering(*out_poly);
    S2CellUnion interior = coverer.GetInteriorCovering(*out_poly);
    std::unordered_set<uint64_t> interior_set;
    for (const auto& cell : interior.cell_ids()) {
        if (cell.level() <= kLevel) {
            auto end = cell.range_max().parent(kLevel);
            for (auto c = cell.range_min().parent(kLevel); c != end; c = c.next()) interior_set.insert(c.id());
            interior_set.insert(end.id());
        } else {
            interior_set.insert(cell.parent(kLevel).id());
        }
    }
    std::vector<std::pair<S2CellId, bool>> result;
    for (const auto& cell : covering.cell_ids()) {
        if (cell.level() <= kLevel) {
            auto end = cell.range_max().parent(kLevel);
            for (auto c = cell.range_min().parent(kLevel); c != end; c = c.next())
                result.emplace_back(c, interior_set.count(c.id()) > 0);
            result.emplace_back(end, interior_set.count(end.id()) > 0);
        } else {
            auto parent = cell.parent(kLevel);
            result.emplace_back(parent, interior_set.count(parent.id()) > 0);
        }
    }
    return result;
}

// Latitude (deg) of the great circle through a and b at longitude lng (deg).
static double gc_lat_at(const S2Point& a, const S2Point& b, double lng) {
    S2Point n = a.CrossProd(b);
    double l = lng * M_PI / 180;
    return std::atan(-(n.x() * std::cos(l) + n.y() * std::sin(l)) / n.z()) * 180 / M_PI;
}

int main(int argc, char** argv) {
    if (argc < 2) { std::cerr << "usage: lune_diag ring.txt < points\n"; return 2; }
    std::vector<std::pair<double, double>> ring;
    {
        std::ifstream f(argv[1]);
        double a, b;
        while (f >> a >> b) ring.emplace_back(a, b);
    }
    std::unique_ptr<S2Polygon> poly;
    auto cover = cover_polygon(ring, poly);
    std::set<uint64_t> in_cover, interior;
    for (auto& [c, i] : cover) { in_cover.insert(c.id()); if (i) interior.insert(c.id()); }
    std::cerr << "ring vertices " << ring.size() << ", covering cells " << in_cover.size() << "\n";
    double lat, lng;
    while (std::cin >> lat >> lng) {
        S2Point q = S2LatLng::FromDegrees(lat, lng).ToPoint();
        S2CellId cell = S2CellId(q).parent(kLevel);
        bool meets = poly && poly->MayIntersect(S2Cell(cell));
        bool contains = poly && poly->Contains(q);
        // planar ray cast, the reader's formula in double precision
        bool inside = false;
        size_t n = ring.size();
        for (size_t i = 0, j = n - 1; i < n; j = i++) {
            auto [ilat, ilng] = ring[i];
            auto [jlat, jlng] = ring[j];
            if (((ilng > lng) != (jlng > lng)) && (lat < (jlat - ilat) * (lng - ilng) / (jlng - ilng) + ilat)) inside = !inside;
        }
        printf("%.7f %.7f %016llx in_cover=%d interior=%d s2_cell_meets_ring=%d s2_contains=%d planar_pip=%d\n", lat, lng,
               (unsigned long long)cell.id(), (int)in_cover.count(cell.id()), (int)interior.count(cell.id()), (int)meets, (int)contains,
               (int)inside);
        for (size_t i = 0, j = n - 1; i < n; j = i++) {
            auto [alat, alng] = ring[j];
            auto [blat, blng] = ring[i];
            if (!((alng > lng) != (blng > lng))) continue;
            double plat = (blat - alat) * (lng - alng) / (blng - alng) + alat;
            S2Point a = S2LatLng::FromDegrees(alat, alng).ToPoint();
            S2Point b = S2LatLng::FromDegrees(blat, blng).ToPoint();
            double glat = gc_lat_at(a, b, lng);
            bool in_lune = (lat - plat) * (lat - glat) < 0;
            if (!in_lune) continue;
            double len = S1ChordAngle(a, b).ToAngle().radians() * kEarthKm;
            double dist = S2::GetDistance(q, a, b).radians() * kEarthKm;
            double depth = std::fabs(glat - plat) * M_PI / 180 * kEarthKm;
            printf("  lune edge %zu (%.5f,%.5f)-(%.5f,%.5f) length_km=%.1f planar_lat=%.6f gc_lat=%.6f dist_to_gc_edge_km=%.3f lune_depth_km=%.3f\n",
                   j, alat, alng, blat, blng, len, plat, glat, dist, depth);
        }
    }
}
