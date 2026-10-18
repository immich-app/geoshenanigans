// borders <dump dir> <n> <seed> > pts.txt : n fresh border test points for the tiers2 verify track.
// Each point: admin level uniform over 2..8 (levels with reader-visible rings), a ring edge of that
// level with probability proportional to its length in metres, a uniform position along it and a
// perpendicular offset uniform in [-200, 200] m. Rings come from the builder's final planet admin
// arrays (GC_TIER_DUMP, the uncapped truth geometry), split with the builder's own split_rings so
// keyhole connector edges are never sampled. Reader-visible: area > 0, and at level 2 a country code.
// Output: "lat lng level" per line.
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

static constexpr double kM = 111320.0;
static constexpr double kOffsetM = 200.0;

static bool visible(const AdminPolygon& p) {
    if (p.admin_level < 2 || p.admin_level > 8 || p.vertex_count < kMinClosedRing || !(p.area > 0)) return false;
    return p.admin_level != 2 || p.country_code != 0;
}

static double edge_len(const NodeCoord& a, const NodeCoord& b) {
    double dl = double(b.lng) - a.lng;
    if (std::fabs(dl) > 180) return 0;  // antimeridian wrap: not a real edge length
    double c = std::cos((double(a.lat) + b.lat) / 2 * M_PI / 180);
    return std::hypot(dl * kM * c, (double(b.lat) - a.lat) * kM);
}

template <class F>
static void for_each_edge(const std::vector<AdminPolygon>& polys, const std::vector<NodeCoord>& verts, F f) {
    std::vector<RingSpan> rings;
    for (const auto& p : polys) {
        if (!visible(p)) continue;
        const NodeCoord* v = verts.data() + p.vertex_offset;
        split_rings(v, p.vertex_count, p.hole_count, rings);
        for (const auto& r : rings)
            for (uint32_t k = 1; k < r.count; k++) f(p.admin_level, v[r.begin + k - 1], v[r.begin + k]);
    }
}

int main(int argc, char** argv) {
    if (argc != 4) { std::cerr << "usage: borders <dump> <n> <seed>\n"; return 1; }
    std::string d = argv[1];
    const int n = std::atoi(argv[2]);
    std::mt19937_64 rng(std::strtoull(argv[3], nullptr, 10));
    auto polys = slurp<AdminPolygon>(d + "/admin_polygons.raw");
    auto verts = slurp<NodeCoord>(d + "/admin_vertices.raw");
    double total[16] = {0};
    for_each_edge(polys, verts, [&](int l, const NodeCoord& a, const NodeCoord& b) { total[l] += edge_len(a, b); });
    std::vector<int> levels;
    for (int l = 2; l <= 8; l++) {
        std::cerr << "level " << l << " boundary length " << total[l] / 1000 << " km\n";
        if (total[l] > 0) levels.push_back(l);
    }
    std::uniform_int_distribution<size_t> pick_level(0, levels.size() - 1);
    std::uniform_real_distribution<double> u01(0, 1), uoff(-kOffsetM, kOffsetM);
    std::vector<std::vector<double>> targets(16);
    for (int i = 0; i < n; i++) { int l = levels[pick_level(rng)]; targets[l].push_back(u01(rng) * total[l]); }
    for (auto& t : targets) std::sort(t.begin(), t.end());
    size_t next[16] = {0};
    double cum[16] = {0};
    struct Pt { double lat, lng; int l; };
    std::vector<Pt> out;
    for_each_edge(polys, verts, [&](int l, const NodeCoord& a, const NodeCoord& b) {
        double len = edge_len(a, b);
        if (len <= 0) return;
        auto& t = targets[l];
        while (next[l] < t.size() && t[next[l]] < cum[l] + len) {
            double s = (t[next[l]] - cum[l]) / len;
            double c = std::cos((double(a.lat) + b.lat) / 2 * M_PI / 180);
            double dx = (double(b.lng) - a.lng) * kM * c, dy = (double(b.lat) - a.lat) * kM;
            double nx = -dy / len, ny = dx / len, off = uoff(rng);
            double lat = a.lat + s * (double(b.lat) - a.lat) + off * ny / kM;
            double lng = a.lng + s * (double(b.lng) - a.lng) + off * nx / (kM * c);
            out.push_back({lat, lng, l});
            next[l]++;
        }
        cum[l] += len;
    });
    std::shuffle(out.begin(), out.end(), rng);
    for (const auto& p : out) std::printf("%.7f %.7f %d\n", std::max(-89.9, std::min(89.9, p.lat)), p.lng, p.l);
    std::cerr << "points " << out.size() << "\n";
    return 0;
}
