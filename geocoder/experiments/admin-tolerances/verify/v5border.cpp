// v5border <dump dir> <n> <seed> <lo> <hi> > pts.txt : verify5's own border sampler (independent of
// tiers3 borders2). Each point: a level drawn uniformly from lo..hi, then a ring edge of that level
// with probability proportional to its length in metres (cumulative table + binary search), a uniform
// position along it and a perpendicular offset uniform in [-200, 200] m. Rings from the builder's
// final admin arrays (GC_TIER_DUMP = uncapped truth geometry), split with the builder's split_rings
// (keyhole connectors never sampled). Reader-visible polygons only: area > 0, level 2 with a country.
// Output "lat lng level".
#include <algorithm>
#include <cmath>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "types.h"
#include "polygon_rings.h"

template <class T>
static std::vector<T> slurp(const std::string& p) {
    std::ifstream f(p, std::ios::binary | std::ios::ate);
    if (!f) { std::cerr << "cannot open " << p << "\n"; std::exit(1); }
    size_t n = static_cast<size_t>(f.tellg());
    std::vector<T> b(n / sizeof(T));
    f.seekg(0);
    f.read(reinterpret_cast<char*>(b.data()), static_cast<std::streamsize>(n));
    return b;
}

static constexpr double kMPerDeg = 111320.0;

int main(int argc, char** argv) {
    if (argc != 6) { std::cerr << "usage: v5border <dump> <n> <seed> <lo> <hi>\n"; return 1; }
    const std::string d = argv[1];
    const long n = std::atol(argv[2]);
    std::mt19937_64 rng(std::strtoull(argv[3], nullptr, 10));
    const int lo = std::atoi(argv[4]), hi = std::atoi(argv[5]);
    auto polys = slurp<AdminPolygon>(d + "/admin_polygons.raw");
    auto verts = slurp<NodeCoord>(d + "/admin_vertices.raw");
    struct Edge { NodeCoord a, b; };
    std::vector<std::vector<Edge>> edges(16);
    std::vector<std::vector<double>> cum(16);
    std::vector<RingSpan> rings;
    for (const auto& p : polys) {
        int l = p.admin_level;
        if (l < lo || l > hi || p.vertex_count < kMinClosedRing || !(p.area > 0)) continue;
        if (l == 2 && p.country_code == 0) continue;
        const NodeCoord* v = verts.data() + p.vertex_offset;
        split_rings(v, p.vertex_count, p.hole_count, rings);
        for (const auto& r : rings)
            for (uint32_t k = 1; k < r.count; k++) {
                const NodeCoord& a = v[r.begin + k - 1];
                const NodeCoord& b = v[r.begin + k];
                double dl = double(b.lng) - a.lng;
                if (std::fabs(dl) > 180) continue;
                double c = std::cos((double(a.lat) + b.lat) / 2 * M_PI / 180);
                double len = std::hypot(dl * kMPerDeg * c, (double(b.lat) - a.lat) * kMPerDeg);
                if (!(len > 0)) continue;
                edges[l].push_back({a, b});
                cum[l].push_back((cum[l].empty() ? 0 : cum[l].back()) + len);
            }
    }
    std::vector<int> levels;
    for (int l = lo; l <= hi; l++) {
        std::cerr << "level " << l << " edges " << edges[l].size() << " km " << (cum[l].empty() ? 0 : cum[l].back() / 1000) << "\n";
        if (!edges[l].empty()) levels.push_back(l);
    }
    std::uniform_int_distribution<size_t> pick_level(0, levels.size() - 1);
    std::uniform_real_distribution<double> u01(0, 1), uoff(-200, 200);
    for (long i = 0; i < n; i++) {
        int l = levels[pick_level(rng)];
        double t = u01(rng) * cum[l].back();
        size_t e = std::upper_bound(cum[l].begin(), cum[l].end(), t) - cum[l].begin();
        if (e >= edges[l].size()) e = edges[l].size() - 1;
        const Edge& E = edges[l][e];
        double s = u01(rng);
        double c = std::cos((double(E.a.lat) + E.b.lat) / 2 * M_PI / 180);
        double dx = (double(E.b.lng) - E.a.lng) * kMPerDeg * c, dy = (double(E.b.lat) - E.a.lat) * kMPerDeg;
        double len = std::hypot(dx, dy), off = uoff(rng);
        double lat = E.a.lat + s * (double(E.b.lat) - E.a.lat) + off * (dx / len) / kMPerDeg;
        double lng = E.a.lng + s * (double(E.b.lng) - E.a.lng) - off * (dy / len) / (kMPerDeg * c);
        lat = std::max(-89.9, std::min(89.9, lat));
        if (lng > 180) lng -= 360;
        if (lng < -180) lng += 360;
        std::printf("%.7f %.7f %d\n", lat, lng, l);
    }
    return 0;
}
