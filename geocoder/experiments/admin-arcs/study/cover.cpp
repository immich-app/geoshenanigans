// Gaps and overlaps between same-level admin polygons on a set of points.
//
//   cover <truth set dir> <variant set dir> <points file> [levels, default 2-8]
//
// For every point and admin level, counts the polygons of that level that contain the point
// (the reader's f32 crossing-number test over each polygon's keyhole vertex range; every vertex
// encoding 0-4 decoded as the reader decodes it), in the truth set and in the variant set.
// gap     = the truth has exactly one polygon there, the variant none
// overlap = the variant has two or more polygons there, the truth at most one
// Level-2 polygons without a country code are left out (the reader ignores them).
// Prints one JSON line: per level n (truth exactly one), gap, overlap, and the truth's own
// multi-cover count.
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>
#include <omp.h>

struct Poly { uint32_t off, cnt, name; uint8_t lvl, pto, p2, p3; float area; uint16_t cc, holes; };
static_assert(sizeof(Poly) == 24, "AdminPolygon is 24 bytes");
struct V { float lat, lng; };

static std::vector<uint8_t> slurp(const std::string& p) {
    std::ifstream f(p, std::ios::binary | std::ios::ate);
    if (!f) { std::cerr << "cannot open " << p << "\n"; std::exit(1); }
    std::vector<uint8_t> b(static_cast<size_t>(f.tellg()));
    f.seekg(0); f.read(reinterpret_cast<char*>(b.data()), b.size());
    return b;
}

struct Set {
    std::vector<Poly> polys;
    std::vector<std::vector<V>> verts;
    std::vector<float> bb;  // per polygon min_lat, max_lat, min_lng, max_lng
    std::unordered_map<int64_t, std::vector<uint32_t>> cells[16];  // per level, 1-degree cells
};

static void load(Set& s, const std::string& dir, int lo, int hi) {
    auto pb = slurp(dir + "/admin_polygons.bin");
    auto vb = slurp(dir + "/admin_vertices.bin");
    s.polys.resize(pb.size() / sizeof(Poly));
    std::memcpy(s.polys.data(), pb.data(), pb.size());
    const size_t n = s.polys.size();
    s.verts.resize(n);
    s.bb.assign(n * 4, 0);
#pragma omp parallel for schedule(dynamic, 256)
    for (size_t i = 0; i < n; i++) {
        const Poly& p = s.polys[i];
        if (p.cnt < 3 || p.off == 0xFFFFFFFF || p.lvl < lo || p.lvl > hi || p.area <= 0) continue;
        if (p.lvl == 2 && p.cc == 0) continue;
        const uint8_t* h = vb.data() + p.off;
        const uint8_t enc = h[0];
        float bml, bmg;
        std::memcpy(&bml, h + 2, 4);
        std::memcpy(&bmg, h + 6, 4);
        const double scale = enc == 0 ? 1e-5 : enc == 1 ? 1e-4 : enc == 2 ? 1e-6 : 1e-7;
        auto& out = s.verts[i];
        out.resize(p.cnt);
        const uint8_t* q = h + 10;
        for (uint32_t k = 0; k < p.cnt; k++) {
            if (enc == 4) {
                std::memcpy(&out[k].lat, q + 8 * k, 4);
                std::memcpy(&out[k].lng, q + 8 * k + 4, 4);
            } else if (enc == 3) {
                uint32_t a, b;
                std::memcpy(&a, q + 8 * k, 4);
                std::memcpy(&b, q + 8 * k + 4, 4);
                out[k] = {float(double(bml) + double(a) * scale), float(double(bmg) + double(b) * scale)};
            } else {
                uint16_t a, b;
                std::memcpy(&a, q + 4 * k, 2);
                std::memcpy(&b, q + 4 * k + 2, 2);
                out[k] = {float(double(bml) + double(a) * scale), float(double(bmg) + double(b) * scale)};
            }
        }
        float a = INFINITY, b = -INFINITY, c = INFINITY, d = -INFINITY;
        for (auto& v : out) { a = std::min(a, v.lat); b = std::max(b, v.lat); c = std::min(c, v.lng); d = std::max(d, v.lng); }
        s.bb[i * 4] = a; s.bb[i * 4 + 1] = b; s.bb[i * 4 + 2] = c; s.bb[i * 4 + 3] = d;
    }
    for (size_t i = 0; i < n; i++) {
        if (s.verts[i].empty()) continue;
        int la0 = int(std::floor(s.bb[i * 4])), la1 = int(std::floor(s.bb[i * 4 + 1]));
        int lg0 = int(std::floor(s.bb[i * 4 + 2])), lg1 = int(std::floor(s.bb[i * 4 + 3]));
        for (int y = la0; y <= la1; y++)
            for (int x = lg0; x <= lg1; x++) s.cells[s.polys[i].lvl][int64_t(y) * 1000 + x].push_back(static_cast<uint32_t>(i));
    }
}

// The reader's point_in_polygon (f32).
static bool pip(float lat, float lng, const std::vector<V>& v) {
    bool inside = false;
    const size_t n = v.size();
    size_t j = n - 1;
    for (size_t i = 0; i < n; i++) {
        const V& vi = v[i];
        const V& vj = v[j];
        if (((vi.lng > lng) != (vj.lng > lng)) && (lat < (vj.lat - vi.lat) * (lng - vi.lng) / (vj.lng - vi.lng) + vi.lat))
            inside = !inside;
        j = i;
    }
    return inside;
}

static int count(const Set& s, int lvl, float lat, float lng) {
    auto it = s.cells[lvl].find(int64_t(std::floor(lat)) * 1000 + int64_t(std::floor(lng)));
    if (it == s.cells[lvl].end()) return 0;
    int c = 0;
    for (uint32_t i : it->second) {
        const float* b = &s.bb[i * 4];
        if (lat < b[0] || lat > b[1] || lng < b[2] || lng > b[3]) continue;
        c += pip(lat, lng, s.verts[i]);
    }
    return c;
}

int main(int argc, char** argv) {
    if (argc < 4) { std::cerr << "usage: cover <truth set> <variant set> <points> [lo-hi]\n"; return 1; }
    int lo = 2, hi = 8;
    if (argc > 4) std::sscanf(argv[4], "%d-%d", &lo, &hi);
    Set t, v;
    load(t, argv[1], lo, hi);
    load(v, argv[2], lo, hi);
    std::vector<std::pair<float, float>> pts;
    {
        std::ifstream f(argv[3]);
        double a, b;
        while (f >> a >> b) pts.push_back({float(a), float(b)});
    }
    long n1[16] = {0}, gap[16] = {0}, over[16] = {0}, tmulti[16] = {0}, vany[16] = {0};
#pragma omp parallel for schedule(dynamic, 256) reduction(+ : n1[:16], gap[:16], over[:16], tmulti[:16], vany[:16])
    for (size_t k = 0; k < pts.size(); k++) {
        for (int l = lo; l <= hi; l++) {
            int ct = count(t, l, pts[k].first, pts[k].second);
            int cv = count(v, l, pts[k].first, pts[k].second);
            tmulti[l] += ct >= 2;
            vany[l] += cv >= 1;
            if (ct == 1) { n1[l]++; gap[l] += cv == 0; }
            if (cv >= 2 && ct <= 1) over[l]++;
        }
    }
    std::printf("{\"points\":%zu,\"levels\":{", pts.size());
    for (int l = lo; l <= hi; l++)
        std::printf("%s\"%d\":{\"truth_one\":%ld,\"gap\":%ld,\"overlap\":%ld,\"truth_multi\":%ld}", l == lo ? "" : ",", l, n1[l], gap[l],
                    over[l], tmulti[l]);
    std::printf("}}\n");
    return 0;
}
