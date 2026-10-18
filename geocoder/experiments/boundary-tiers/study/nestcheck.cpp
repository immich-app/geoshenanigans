// Does each admin polygon nest in its parent? Build-time check for the chain
// reader (candidate g): the chain may stand in for point-in-polygon at the
// coarse ranks only where the polygon lies inside its parent, its parent
// inside the grandparent, and so on (Nominatim tests each rank's full
// geometry, so the two agree exactly there).
//
//   nestcheck <dump> <out flags> [eps_m]
//
// Geometry: the dump's full-resolution f32 rings (the vertices the builder's
// parent chain was computed from). P nests in its parent Q when every vertex
// of P's outer ring is inside Q (crossing number over Q's keyhole range, so
// Q's holes count) or within eps_m (default 2 m) of an edge of Q (shared
// borders: both rings carry the same OSM nodes, minus the 0.1 m DP base).
// Flags, one byte per polygon: bit0 P nests in its parent (or has none),
// bit1 every link of P's chain nests (chain_ok), bit2 P has a parent.
// Prints per level: polygons with a parent, nested, chain_ok.
#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <omp.h>

#include "types.h"
#include "polygon_rings.h"

template <class T>
static std::vector<T> slurp(const std::string& p) {
    std::ifstream f(p, std::ios::binary | std::ios::ate);
    if (!f) { std::cerr << "cannot open " << p << "\n"; std::exit(1); }
    size_t n = static_cast<size_t>(f.tellg());
    std::vector<T> b(n / sizeof(T));
    f.seekg(0);
    f.read(reinterpret_cast<char*>(b.data()), n);
    return b;
}

struct Index {  // latitude bands over the edges of one polygon's vertex range
    double lat0 = 0, lat1 = 0, inv = 0;
    uint32_t bands = 0;
    std::vector<uint32_t> start, edges;  // CSR: band -> edge ids (edge k: vertex k -> k+1, last wraps)
};

static Index build(const NodeCoord* v, uint32_t n) {
    Index ix;
    ix.lat0 = 1e9; ix.lat1 = -1e9;
    for (uint32_t k = 0; k < n; k++) { ix.lat0 = std::min<double>(ix.lat0, v[k].lat); ix.lat1 = std::max<double>(ix.lat1, v[k].lat); }
    ix.bands = std::max<uint32_t>(1, std::min<uint32_t>(1u << 16, n / 4));
    ix.inv = ix.lat1 > ix.lat0 ? ix.bands / (ix.lat1 - ix.lat0) : 0;
    auto band = [&](double y) { long b = static_cast<long>((y - ix.lat0) * ix.inv); return static_cast<uint32_t>(std::clamp<long>(b, 0, ix.bands - 1)); };
    std::vector<uint32_t> cnt(ix.bands + 1, 0);
    for (uint32_t k = 0; k < n; k++) {
        const NodeCoord& a = v[k]; const NodeCoord& b = v[k + 1 < n ? k + 1 : 0];
        uint32_t b0 = band(std::min(a.lat, b.lat)), b1 = band(std::max(a.lat, b.lat));
        for (uint32_t x = b0; x <= b1; x++) cnt[x + 1]++;
    }
    for (uint32_t x = 0; x < ix.bands; x++) cnt[x + 1] += cnt[x];
    ix.start = cnt;
    ix.edges.resize(cnt[ix.bands]);
    std::vector<uint32_t> fill(cnt.begin(), cnt.end() - 1);
    for (uint32_t k = 0; k < n; k++) {
        const NodeCoord& a = v[k]; const NodeCoord& b = v[k + 1 < n ? k + 1 : 0];
        uint32_t b0 = band(std::min(a.lat, b.lat)), b1 = band(std::max(a.lat, b.lat));
        for (uint32_t x = b0; x <= b1; x++) ix.edges[fill[x]++] = k;
    }
    return ix;
}

static bool inside(const Index& ix, const NodeCoord* v, uint32_t n, double y, double x) {
    if (y < ix.lat0 || y > ix.lat1) return false;
    long b = static_cast<long>((y - ix.lat0) * ix.inv);
    b = std::clamp<long>(b, 0, ix.bands - 1);
    bool in = false;
    for (uint32_t e = ix.start[b]; e < ix.start[b + 1]; e++) {
        uint32_t k = ix.edges[e];
        const NodeCoord& a = v[k]; const NodeCoord& c = v[k + 1 < n ? k + 1 : 0];
        double ya = a.lat, yc = c.lat, xa = a.lng, xc = c.lng;
        if ((ya > y) != (yc > y) && x < (xc - xa) * (y - ya) / (yc - ya) + xa) in = !in;
    }
    return in;
}

static bool near(const Index& ix, const NodeCoord* v, uint32_t n, double y, double x, double eps_m) {
    double dlat = eps_m / 111320.0;
    if (y < ix.lat0 - dlat || y > ix.lat1 + dlat) return false;
    long b0 = std::clamp<long>(static_cast<long>((y - dlat - ix.lat0) * ix.inv), 0, ix.bands - 1);
    long b1 = std::clamp<long>(static_cast<long>((y + dlat - ix.lat0) * ix.inv), 0, ix.bands - 1);
    double kx = 111320.0 * std::cos(y * M_PI / 180.0), ky = 111320.0;
    for (long b = b0; b <= b1; b++)
        for (uint32_t e = ix.start[b]; e < ix.start[b + 1]; e++) {
            uint32_t k = ix.edges[e];
            const NodeCoord& a = v[k]; const NodeCoord& c = v[k + 1 < n ? k + 1 : 0];
            double ax = (a.lng - x) * kx, ay = (a.lat - y) * ky, cx = (c.lng - x) * kx, cy = (c.lat - y) * ky;
            double dx = cx - ax, dy = cy - ay, L = dx * dx + dy * dy;
            double t = L > 0 ? std::clamp(-(ax * dx + ay * dy) / L, 0.0, 1.0) : 0.0;
            double px = ax + t * dx, py = ay + t * dy;
            if (px * px + py * py <= eps_m * eps_m) return true;
        }
    return false;
}

int main(int argc, char** argv) {
    if (argc < 3) { std::cerr << "usage: nestcheck <dump> <out flags> [eps_m]\n"; return 1; }
    std::string d = argv[1];
    double eps = argc > 3 ? std::atof(argv[3]) : 2.0;
    auto polys = slurp<AdminPolygon>(d + "/admin_polygons.raw");
    auto verts = slurp<NodeCoord>(d + "/admin_vertices.raw");
    auto par = slurp<uint32_t>(d + "/admin_parent_ids.raw");
    const size_t n = polys.size();
    std::vector<uint8_t> is_parent(n, 0);
    for (size_t i = 0; i < n; i++) if (par[i] != NO_DATA && par[i] < n) is_parent[par[i]] = 1;
    std::vector<uint32_t> parents;
    for (size_t i = 0; i < n; i++) if (is_parent[i] && polys[i].vertex_count >= 3) parents.push_back(static_cast<uint32_t>(i));
    std::vector<Index> idx(n);
    std::vector<int32_t> slot(n, -1);
#pragma omp parallel for schedule(dynamic, 1)
    for (size_t j = 0; j < parents.size(); j++) {
        const AdminPolygon& q = polys[parents[j]];
        idx[parents[j]] = build(verts.data() + q.vertex_offset, q.vertex_count);
    }
    std::vector<uint8_t> flags(n, 0);
    std::vector<uint64_t> outside_v(n, 0);
#pragma omp parallel for schedule(dynamic, 64)
    for (size_t i = 0; i < n; i++) {
        const AdminPolygon& p = polys[i];
        uint32_t q = par[i];
        if (q == NO_DATA || q >= n || p.vertex_count < 3 || polys[q].vertex_count < 3) { flags[i] = 1; continue; }
        flags[i] = 4;
        const NodeCoord* pv = verts.data() + p.vertex_offset;
        uint32_t outer = outer_ring_count(pv, p.vertex_count, p.hole_count);
        const NodeCoord* qv = verts.data() + polys[q].vertex_offset;
        uint32_t qn = polys[q].vertex_count;
        uint64_t out = 0;
        for (uint32_t k = 0; k < outer; k++) {
            double y = pv[k].lat, x = pv[k].lng;
            if (inside(idx[q], qv, qn, y, x)) continue;
            if (near(idx[q], qv, qn, y, x, eps)) continue;
            out++;
        }
        outside_v[i] = out;
        if (out == 0) flags[i] |= 1;
    }
    // chain_ok: every link up the chain nests (memoised walk).
    std::vector<int8_t> ok(n, -1);
    for (size_t i = 0; i < n; i++) {
        std::vector<uint32_t> path;
        uint32_t c = static_cast<uint32_t>(i);
        int8_t res = 1;
        while (true) {
            if (ok[c] >= 0) { res = ok[c]; break; }
            path.push_back(c);
            if (!(flags[c] & 1)) { res = 0; break; }
            if (par[c] == NO_DATA || par[c] >= n || !(flags[c] & 4)) { res = 1; break; }
            c = par[c];
            if (path.size() > 40) { res = 0; break; }
        }
        // A broken link makes every polygon below it not ok; above it stays to be found.
        for (uint32_t x : path) { if (ok[x] < 0) ok[x] = res; }
    }
    for (size_t i = 0; i < n; i++) if (ok[i] == 1) flags[i] |= 2;
    std::ofstream(argv[2], std::ios::binary).write(reinterpret_cast<const char*>(flags.data()), n);
    uint64_t L[16][4] = {{0}};
    for (size_t i = 0; i < n; i++) {
        int l = std::min<int>(polys[i].admin_level, 15);
        if (polys[i].vertex_count < 3) continue;
        L[l][0]++;
        if (flags[i] & 4) L[l][1]++;
        if ((flags[i] & 4) && (flags[i] & 1)) L[l][2]++;
        if (flags[i] & 2) L[l][3]++;
    }
    std::printf("eps %.2f m\n", eps);
    for (int l = 0; l < 16; l++)
        if (L[l][0]) std::printf("L%d: %llu live, %llu with parent, %llu nest in parent, %llu chain_ok\n", l,
                                 (unsigned long long)L[l][0], (unsigned long long)L[l][1], (unsigned long long)L[l][2],
                                 (unsigned long long)L[l][3]);
    return 0;
}
