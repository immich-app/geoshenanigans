// tierscan: per-level reader picks of admin simplification schemes against the uncapped truth.
//
//   tierscan <dump> <pools file> <out tsv> <spec> [<spec> ...]
//
// <dump>: the builder's final planet admin arrays (GC_TIER_DUMP: admin_polygons.raw,
// admin_vertices.raw). <pools file>: lines "name path", each path "lat lng ..." per line.
//
// Reader semantics (server lib.rs find_admin, per admin level): the pick is the smallest-area polygon
// whose decoded f32 vertices (the whole keyhole range) contain the point by the crossing-number test
// point_in_polygon; level 2 needs a country code; area <= 0 never picks. Candidates here are every
// polygon whose bbox holds the point (the reader's cell index only narrows that; ignored).
// Truth = every vertex packed and decoded as the reader sees quality/uncapped.
//
// <spec>: ';'-separated items, applied per ring (outer and each hole) of one level's polygons:
//   L=<level>     the level (required)
//   t=<m>         tolerance in metres (default q2.5 = 2.5 x kAdminEpsilonMeters)
//   cap=<f>       tolerance at most f x sqrt(ring area) (9b55d28 ring_tolerance_degrees)
//   iso=1         Douglas-Peucker in a locally isotropic frame (lng x cos(lat0)): t is metres in every
//                 direction; the builder's DP measures in raw degrees with t converted at lat0 as a
//                 longitude distance, so north-south it allows t / cos(lat0)
//   rel=k,lo      tolerance min(t, max(lo, k x sqrt(ring area in m^2)))
//   relx=f,lo,hi  tolerance clamp(f x sqrt(ring area in m^2), lo, hi), t ignored
//   none=1        no simplification (= truth; a control: mismatch must be 0)
// Output (TSV, one row per spec x pool): spec level polygons vertices bytes pool n truth_has
//   mismatch gap extra swap overlap; overlap = swaps where the variant still contains the truth
//   polygon (a smaller neighbour now overlaps the point).
#include <algorithm>
#include <array>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <omp.h>

#include "types.h"
#include "geometry.h"
#include "polygon_rings.h"
#include "vertex_pack.h"

using Pts = std::vector<std::pair<double, double>>;
static constexpr int kMaxLevel = 10;
static constexpr uint32_t kNone = 0xFFFFFFFFu;

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

// ---- reader-exact geometry ----

struct Geo {
    std::vector<NodeCoord> v;      // decoded f32 vertices, whole keyhole range
    float minlat = 0, maxlat = 0, minlng = 0, maxlng = 0;
    float area = 0;                // as stored (f32 of polygon_area of the packed points)
    // lng bands: edges (i-1 -> i, wrap at 0) whose lng range meets the band
    uint32_t bands = 0;
    double band_inv = 0;
    std::vector<uint32_t> boff, bedge;
    bool ok = false;
};

static std::vector<NodeCoord> decode(const std::vector<uint8_t>& b, uint32_t n) {
    std::vector<NodeCoord> out(n);
    uint8_t enc = b[0];
    float bml, bmg;
    std::memcpy(&bml, &b[2], 4);
    std::memcpy(&bmg, &b[6], 4);
    double scale = enc == 0 ? 1e-5 : enc == 1 ? 1e-4 : enc == 2 ? 1e-6 : 1e-7;
    const uint8_t* p = b.data() + POLY_HEADER_BYTES;
    for (uint32_t i = 0; i < n; i++) {
        double dl, dg;
        if (enc == 3) { uint32_t a, c; std::memcpy(&a, p, 4); std::memcpy(&c, p + 4, 4); p += 8; dl = a; dg = c; }
        else { uint16_t a, c; std::memcpy(&a, p, 2); std::memcpy(&c, p + 2, 2); p += 4; dl = a; dg = c; }
        out[i].lat = static_cast<float>(static_cast<double>(bml) + dl * scale);
        out[i].lng = static_cast<float>(static_cast<double>(bmg) + dg * scale);
    }
    return out;
}

static inline uint32_t band_of(const Geo& g, float x) {
    double b = (static_cast<double>(x) - g.minlng) * g.band_inv;
    if (b < 0) b = 0;
    uint32_t i = static_cast<uint32_t>(b);
    return i >= g.bands ? g.bands - 1 : i;
}

static void finish(Geo& g) {
    const auto& v = g.v;
    const size_t n = v.size();
    if (n < 3) return;
    g.minlat = g.maxlat = v[0].lat;
    g.minlng = g.maxlng = v[0].lng;
    for (const auto& c : v) {
        g.minlat = std::min(g.minlat, c.lat); g.maxlat = std::max(g.maxlat, c.lat);
        g.minlng = std::min(g.minlng, c.lng); g.maxlng = std::max(g.maxlng, c.lng);
    }
    g.bands = n < 64 ? 1 : static_cast<uint32_t>(std::min<size_t>(n / 8, 8192));
    double w = (static_cast<double>(g.maxlng) - g.minlng) / g.bands;
    if (!(w > 0)) { g.bands = 1; w = 1; }
    g.band_inv = 1.0 / w;
    std::vector<uint32_t> cnt(g.bands + 1, 0);
    for (size_t i = 0; i < n; i++) {
        const NodeCoord& a = v[i == 0 ? n - 1 : i - 1];
        const NodeCoord& b = v[i];
        uint32_t b0 = band_of(g, std::min(a.lng, b.lng)), b1 = band_of(g, std::max(a.lng, b.lng));
        for (uint32_t k = b0; k <= b1; k++) cnt[k + 1]++;
    }
    for (uint32_t k = 0; k < g.bands; k++) cnt[k + 1] += cnt[k];
    g.boff = cnt;
    g.bedge.resize(cnt[g.bands]);
    std::vector<uint32_t> pos(cnt.begin(), cnt.end() - 1);
    for (size_t i = 0; i < n; i++) {
        const NodeCoord& a = v[i == 0 ? n - 1 : i - 1];
        const NodeCoord& b = v[i];
        uint32_t b0 = band_of(g, std::min(a.lng, b.lng)), b1 = band_of(g, std::max(a.lng, b.lng));
        for (uint32_t k = b0; k <= b1; k++) g.bedge[pos[k]++] = static_cast<uint32_t>(i);
    }
    g.ok = true;
}

// lib.rs point_in_polygon over the edges of the point's band (the others cannot straddle its lng).
static bool pip(const Geo& g, float lat, float lng) {
    if (lat < g.minlat || lat > g.maxlat || lng < g.minlng || lng > g.maxlng) {
        // Outside the bbox no edge straddles lng (lng outside) or every crossing is south (lat > max)
        // or the count is even (lat < min: the ray crosses the whole polygon). Exact for lng; for lat
        // the crossing test can still count edges, so fall through only for the lat case.
        if (lng < g.minlng || lng > g.maxlng) return false;
    }
    const size_t n = g.v.size();
    uint32_t b = band_of(g, lng);
    bool inside = false;
    for (uint32_t k = g.boff[b]; k < g.boff[b + 1]; k++) {
        uint32_t i = g.bedge[k];
        const NodeCoord& vi = g.v[i];
        const NodeCoord& vj = g.v[i == 0 ? n - 1 : i - 1];
        if (((vi.lng > lng) != (vj.lng > lng)) && (lat < (vj.lat - vi.lat) * (lng - vi.lng) / (vj.lng - vi.lng) + vi.lat))
            inside = !inside;
    }
    return inside;
}

// ---- simplification (tiervar pack(), with the iso and rel options) ----

struct Spec {
    std::string text;
    int level = -1;
    double t = -1, cap = 0, rel_k = 0, rel_lo = 0, rx_f = 0, rx_lo = 0, rx_hi = 0;
    bool iso = false, none = false;
};

static Spec parse(const std::string& s) {
    Spec sp;
    sp.text = s;
    std::stringstream ss(s);
    std::string kv;
    while (std::getline(ss, kv, ';')) {
        if (kv.empty()) continue;
        auto eq = kv.find('=');
        if (eq == std::string::npos) { std::cerr << "bad spec item " << kv << "\n"; std::exit(1); }
        std::string k = kv.substr(0, eq), v = kv.substr(eq + 1);
        if (k == "L") sp.level = std::atoi(v.c_str());
        else if (k == "t") sp.t = std::atof(v.c_str());
        else if (k == "cap") sp.cap = std::atof(v.c_str());
        else if (k == "iso") sp.iso = std::atoi(v.c_str()) != 0;
        else if (k == "none") sp.none = std::atoi(v.c_str()) != 0;
        else if (k == "relx") { if (std::sscanf(v.c_str(), "%lf,%lf,%lf", &sp.rx_f, &sp.rx_lo, &sp.rx_hi) != 3) { std::cerr << "relx=f,lo,hi\n"; std::exit(1); } }
        else if (k == "rel") { if (std::sscanf(v.c_str(), "%lf,%lf", &sp.rel_k, &sp.rel_lo) != 2) { std::cerr << "rel=k,lo\n"; std::exit(1); } }
        else { std::cerr << "unknown spec key " << k << "\n"; std::exit(1); }
    }
    if (sp.level < 2 || sp.level > kMaxLevel) { std::cerr << "spec needs L=2.." << kMaxLevel << "\n"; std::exit(1); }
    if (sp.t < 0) sp.t = admin_epsilon_meters(static_cast<uint8_t>(sp.level)) * 2.5;
    return sp;
}

static double ring_area_m2(const Pts& r) {
    if (r.size() < 3) return 0;
    double lat0 = r[0].first, k = std::cos(lat0 * M_PI / 180.0);
    double a = 0;
    for (size_t i = 0, j = r.size() - 1; i < r.size(); j = i++)
        a += (r[j].second * k) * r[i].first - (r[i].second * k) * r[j].first;
    return std::fabs(a) / 2 * 111320.0 * 111320.0;
}

// The builder's dp_simplify / simplify_polygon_epsilon on transformed points q, returning kept indices.
static void dp(const Pts& q, size_t s, size_t e, double eps, std::vector<bool>& keep) {
    if (e <= s + 1) return;
    double ax = q[s].first, ay = q[s].second, bx = q[e].first, by = q[e].second;
    double dx = bx - ax, dy = by - ay, len_sq = dx * dx + dy * dy, md = 0;
    size_t mi = s;
    for (size_t i = s + 1; i < e; i++) {
        double px = q[i].first - ax, py = q[i].second - ay, d;
        if (len_sq == 0) d = std::sqrt(px * px + py * py);
        else {
            double t = std::max(0.0, std::min(1.0, (px * dx + py * dy) / len_sq));
            double ox = t * dx - px, oy = t * dy - py;
            d = std::sqrt(ox * ox + oy * oy);
        }
        if (d > md) { md = d; mi = i; }
    }
    if (md > eps) { keep[mi] = true; dp(q, s, mi, eps, keep); dp(q, mi, e, eps, keep); }
}
static std::vector<bool> dp_keep(const Pts& q, double eps, size_t min_vertices = 4) {
    std::vector<bool> keep(q.size(), true);
    if (q.size() <= min_vertices) return keep;
    std::fill(keep.begin(), keep.end(), false);
    keep[0] = keep[q.size() - 1] = true;
    dp(q, 0, q.size() - 1, eps, keep);
    size_t count = std::count(keep.begin(), keep.end(), true);
    if (count < min_vertices) {
        double lo = 0, hi = eps;
        for (int it = 0; it < 20; it++) {
            double mid = (lo + hi) / 2;
            std::vector<bool> tk(q.size(), false);
            tk[0] = tk[q.size() - 1] = true;
            dp(q, 0, q.size() - 1, mid, tk);
            size_t c = std::count(tk.begin(), tk.end(), true);
            if (c >= min_vertices) { lo = mid; keep = tk; } else hi = mid;
        }
    }
    return keep;
}

static Pts simplify_ring(const Pts& ring, const Spec& sp) {
    double lat = ring.empty() ? 0.0 : ring[0].first;
    double m = sp.t;
    if (sp.rel_k > 0) m = std::min(m, std::max(sp.rel_lo, sp.rel_k * std::sqrt(ring_area_m2(ring))));
    if (sp.rx_f > 0) m = std::max(sp.rx_lo, std::min(sp.rx_hi, sp.rx_f * std::sqrt(ring_area_m2(ring))));
    if (!sp.iso) {
        double deg = meters_to_degrees(m, lat);
        if (sp.cap > 0) deg = std::min(deg, sp.cap * std::sqrt(polygon_area(ring)));
        return simplify_polygon_epsilon(ring, deg);
    }
    double k = std::cos(lat * M_PI / 180.0);
    if (k < 0.01) k = 0.01;
    Pts q(ring.size());
    for (size_t i = 0; i < ring.size(); i++) q[i] = {ring[i].first, ring[i].second * k};
    double eps = m / 111320.0;
    if (sp.cap > 0) eps = std::min(eps, sp.cap * std::sqrt(polygon_area(ring) * k));
    std::vector<bool> keep = dp_keep(q, eps);
    Pts out;
    for (size_t i = 0; i < ring.size(); i++) if (keep[i]) out.push_back(ring[i]);
    return out;
}

struct Built { Geo g; uint32_t count = 0; size_t bytes = 0; };

static Built build(const AdminPolygon& ap, const NodeCoord* all, const Spec* sp) {
    Built out;
    if (ap.vertex_count < 3) return out;
    const NodeCoord* verts = all + ap.vertex_offset;
    auto points = [&](const RingSpan& r) {
        Pts pts;
        pts.reserve(r.count);
        for (uint32_t j = 0; j < r.count; j++) pts.emplace_back(verts[r.begin + j].lat, verts[r.begin + j].lng);
        return pts;
    };
    Pts pts;
    if (sp && !sp->none) {
        std::vector<RingSpan> rings;
        split_rings(verts, ap.vertex_count, ap.hole_count, rings);
        std::vector<Pts> holes;
        for (size_t r = 1; r < rings.size(); r++) holes.push_back(points(rings[r]));
        pts = simplify_rings(points(rings[0]), holes, [&](const Pts& ring) { return simplify_ring(ring, *sp); }).first;
    } else {
        pts = points(RingSpan{0, ap.vertex_count});
    }
    if (pts.size() < 3) return out;
    PolygonPacking pk = plan_polygon(pts.data(), pts.size());
    std::vector<uint8_t> bytes(pk.bytes);
    pack_polygon(bytes.data(), pk, pts.data(), pts.size());
    out.count = static_cast<uint32_t>(pts.size());
    out.bytes = pk.bytes;
    out.g.v = decode(bytes, out.count);
    out.g.area = static_cast<float>(polygon_area(pts));
    finish(out.g);
    return out;
}

// ---- candidate grid per level ----

struct Grid {
    static constexpr double kCell = 0.25;
    std::unordered_map<uint64_t, std::vector<uint32_t>> cells;
    std::vector<uint32_t> big;
    static uint64_t key(int a, int b) { return (static_cast<uint64_t>(static_cast<uint32_t>(a)) << 32) | static_cast<uint32_t>(b); }
    void add(uint32_t id, const Geo& g) {
        int a0 = static_cast<int>(std::floor(g.minlat / kCell)), a1 = static_cast<int>(std::floor(g.maxlat / kCell));
        int b0 = static_cast<int>(std::floor(g.minlng / kCell)), b1 = static_cast<int>(std::floor(g.maxlng / kCell));
        if (static_cast<long>(a1 - a0 + 1) * (b1 - b0 + 1) > 2048) { big.push_back(id); return; }
        for (int a = a0; a <= a1; a++) for (int b = b0; b <= b1; b++) cells[key(a, b)].push_back(id);
    }
    template <class F> void each(float lat, float lng, F f) const {
        auto it = cells.find(key(static_cast<int>(std::floor(lat / kCell)), static_cast<int>(std::floor(lng / kCell))));
        if (it != cells.end()) for (uint32_t id : it->second) f(id);
        for (uint32_t id : big) f(id);
    }
};

static bool visible(const AdminPolygon& ap, int level) {
    return ap.admin_level == level && !(level == 2 && ap.country_code == 0);
}

// Smallest-area containing polygon of geometry set `geo` (indexed like polys) via `grid`.
static uint32_t pick(const std::vector<Geo*>& geo, const Grid& grid, float lat, float lng) {
    uint32_t best = kNone;
    float ba = 0;
    grid.each(lat, lng, [&](uint32_t id) {
        const Geo& g = *geo[id];
        if (!g.ok || !(g.area > 0)) return;
        if (best != kNone && (g.area > ba || (g.area == ba && id > best))) return;
        if (!pip(g, lat, lng)) return;
        best = id;
        ba = g.area;
    });
    return best;
}

int main(int argc, char** argv) {
    if (argc < 5) { std::cerr << "usage: tierscan <dump> <pools file> <out tsv> <spec>...\n"; return 1; }
    std::string dump = argv[1];
    auto polys = slurp<AdminPolygon>(dump + "/admin_polygons.raw");
    auto verts = slurp<NodeCoord>(dump + "/admin_vertices.raw");
    std::vector<std::string> pool_names;
    std::vector<int> pool_of;
    std::vector<float> plat, plng;
    {
        std::ifstream pf(argv[2]);
        std::string name, path;
        while (pf >> name >> path) {
            std::ifstream f(path);
            if (!f) { std::cerr << "cannot open " << path << "\n"; return 1; }
            std::string line;
            int id = static_cast<int>(pool_names.size());
            pool_names.push_back(name);
            while (std::getline(f, line)) {
                double la, ln;
                if (std::sscanf(line.c_str(), "%lf %lf", &la, &ln) != 2) continue;
                plat.push_back(static_cast<float>(la));
                plng.push_back(static_cast<float>(ln));
                pool_of.push_back(id);
            }
        }
    }
    const size_t np = plat.size(), P = pool_names.size();
    std::cerr << "points " << np << " in " << P << " pools\n";

    // Truth per level (cached next to the output: <out>.truth, keyed by point count).
    std::vector<uint32_t> truth(np * (kMaxLevel + 1), kNone);
    std::string cache = std::string(argv[3]) + ".truth";
    bool have = false;
    {
        std::ifstream c(cache, std::ios::binary | std::ios::ate);
        if (c && static_cast<size_t>(c.tellg()) == truth.size() * 4) {
            c.seekg(0);
            c.read(reinterpret_cast<char*>(truth.data()), static_cast<std::streamsize>(truth.size() * 4));
            have = true;
            std::cerr << "truth from cache\n";
        }
    }
    if (!have) {
        for (int L = 2; L <= kMaxLevel; L++) {
            std::vector<uint32_t> ids;
            for (uint32_t i = 0; i < polys.size(); i++) if (visible(polys[i], L)) ids.push_back(i);
            std::vector<Built> b(ids.size());
#pragma omp parallel for schedule(dynamic, 16)
            for (size_t k = 0; k < ids.size(); k++) b[k] = build(polys[ids[k]], verts.data(), nullptr);
            std::vector<Geo*> geo(polys.size(), nullptr);
            Geo empty;
            for (auto& g : geo) g = &empty;
            Grid grid;
            for (size_t k = 0; k < ids.size(); k++) { geo[ids[k]] = &b[k].g; if (b[k].g.ok) grid.add(ids[k], b[k].g); }
#pragma omp parallel for schedule(dynamic, 256)
            for (size_t p = 0; p < np; p++) truth[p * (kMaxLevel + 1) + L] = pick(geo, grid, plat[p], plng[p]);
            std::cerr << "truth level " << L << ": " << ids.size() << " polygons\n";
        }
        std::ofstream c(cache, std::ios::binary);
        c.write(reinterpret_cast<const char*>(truth.data()), static_cast<std::streamsize>(truth.size() * 4));
    }

    FILE* out = std::fopen(argv[3], "a");
    for (int s = 4; s < argc; s++) {
        Spec sp = parse(argv[s]);
        const int L = sp.level;
        std::vector<uint32_t> ids;
        for (uint32_t i = 0; i < polys.size(); i++) if (visible(polys[i], L)) ids.push_back(i);
        std::vector<Built> b(ids.size());
#pragma omp parallel for schedule(dynamic, 16)
        for (size_t k = 0; k < ids.size(); k++) b[k] = build(polys[ids[k]], verts.data(), &sp);
        std::vector<Geo*> geo(polys.size(), nullptr);
        Geo empty;
        for (auto& g : geo) g = &empty;
        Grid grid;
        uint64_t nv = 0, nb = 0, npoly = 0;
        for (size_t k = 0; k < ids.size(); k++) {
            geo[ids[k]] = &b[k].g;
            nv += b[k].count; nb += b[k].bytes; npoly += b[k].count > 0;
            if (b[k].g.ok) grid.add(ids[k], b[k].g);
        }
        // per pool: n, truth_has, mismatch, gap, extra, swap, overlap
        std::vector<std::array<uint64_t, 7>> st(P);
        for (auto& a : st) a.fill(0);
        int nt = omp_get_max_threads();
        std::vector<std::vector<std::array<uint64_t, 7>>> loc(nt, st);
#pragma omp parallel for schedule(dynamic, 256)
        for (size_t p = 0; p < np; p++) {
            auto& a = loc[omp_get_thread_num()][pool_of[p]];
            uint32_t t = truth[p * (kMaxLevel + 1) + L];
            uint32_t v = pick(geo, grid, plat[p], plng[p]);
            a[0]++;
            a[1] += t != kNone;
            if (t == v) continue;
            a[2]++;
            if (v == kNone) a[3]++;
            else if (t == kNone) a[4]++;
            else {
                a[5]++;
                const Geo& tg = *geo[t];
                a[6] += tg.ok && tg.area > 0 && pip(tg, plat[p], plng[p]);
            }
        }
        for (auto& l : loc) for (size_t q = 0; q < P; q++) for (int k = 0; k < 7; k++) st[q][k] += l[q][k];
        for (size_t q = 0; q < P; q++)
            std::fprintf(out, "%s\t%d\t%llu\t%llu\t%llu\t%s\t%llu\t%llu\t%llu\t%llu\t%llu\t%llu\t%llu\n", sp.text.c_str(), L,
                         (unsigned long long)npoly, (unsigned long long)nv, (unsigned long long)nb, pool_names[q].c_str(),
                         (unsigned long long)st[q][0], (unsigned long long)st[q][1], (unsigned long long)st[q][2],
                         (unsigned long long)st[q][3], (unsigned long long)st[q][4], (unsigned long long)st[q][5],
                         (unsigned long long)st[q][6]);
        std::fflush(out);
        std::cerr << "spec " << sp.text << " done: " << nv << " vertices, " << nb << " bytes\n";
    }
    std::fclose(out);
    return 0;
}
