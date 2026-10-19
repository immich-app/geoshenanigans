// Shared-arc (topology) admin boundary study.
//
//   arcs <minimal|quality> <dump> <arcdump> <admin_dir|-> <out> <scheme>
//
// <dump>     the planet's final admin arrays (GC_TIER_DUMP of the arcs-dump branch): which
//            polygons exist, in which slot, with which level / name / country / osm id.
// <arcdump>  every admin polygon's rings exactly as assembled from its OSM member ways
//            (GC_ARC_DUMP): exact 1e-7 coordinates and the member way of every edge.
// A polygon of the dump without an arcdump record (none expected) falls back to its dump rings.
//
// Arcs come from the OSM structure: neighbouring polygons and the levels above them share
// member ways, so their rings run over the same nodes. A vertex is a junction where the union
// of all rings' edges meets more than two other vertices (or fewer): between junctions every
// edge has the same set of rings, so each maximal junction-to-junction run is one arc, stored
// once whichever rings (outer or hole, any level) use it and in which direction. A ring with no
// junction is one closed arc (canonical rotation and direction, so the same ring at several
// levels is stored once).
//
// Each arc is simplified ONCE (Douglas-Peucker, the builder's dp_simplify metric) at the finest
// tolerance among the rings using it (scheme tolerance of the ring's level, and the ring cap
// 0.1 x sqrt(ring area) when cap= is set), endpoints fixed; closed arcs as the builder's
// simplify_polygon_epsilon (min 4 vertices). Kept vertices snap to a global grid (q=), so a
// junction is bit-identical in every arc. Then:
//   - every ring must keep >= 3 distinct vertices and a non-zero area (else its arcs get their
//     farthest dropped vertex back, one at a time);
//   - repair=1: segments of all arcs that cross or touch away from a shared vertex get the
//     farthest dropped vertex of their span back, until none are left (or 40 rounds).
//
// Writes <out>/arc/ (the arc format, sizes measured by sizes_arcs.py) and <out>/dec/ (the
// arcs decoded back from the encoded bytes into today's admin files, encoding 4 = raw f32 per
// vertex so every shared vertex is the same f32 in every polygon, for the existing reader):
//   minimal: dec/admin_polygons.bin admin_vertices.bin admin_cells.bin admin_entries.bin
//            admin_parents.bin (tiervar minimal's slots, cell remap and parents)
//   quality: dec/admin_polygons.bin admin_vertices.bin postal_polygons.bin postal_vertices.bin
// arc format files:
//   arc_vertices.bin  per arc: u8 flags (bits 0-1 delta width 1/2/4 B, bit 7 bbox follows),
//                     i32 lat, i32 lng of the first vertex (grid units), [bbox: i32 min_lat,
//                     max_lat, min_lng, max_lng], then n-1 zigzag deltas as byte planes
//                     (lat b0 | lat b1 .. | lng b0 | ..). bbox only on arcs of >= bbox= vertices.
//   arc_index.bin     u32 byte offset of every arc + the end offset (n derivable from it)
//   poly_arcs.bin     per polygon (slot order): per ring u32 arc count, then u32 refs
//                     (arc << 1 | reversed); outer ring first
//   poly_arcs_v.bin   the same refs as varints (ring count, then zigzag(arc - previous arc) << 1
//                     | reversed) - a compact alternative
//   admin_polygons.bin  AdminPolygon per slot: vertex_offset = byte offset into poly_arcs.bin,
//                     vertex_count = refs, hole_count = rings - 1 (rest as today)
//   postal_polygons.bin (quality) the level-11 subset, same offsets
//   arc_keys.bin, poly_keys.bin  per arc / per polygon hashes for the day-to-day patch study
//
// scheme: ';'-separated, as tiervar: none | scale=s | uni=m | t<L>=m | max=m | min=m | cap=f |
//   capmin=m, plus q=<grid deg> (default 1e-5), repair=0|1 (default 1), bbox=<n> (default 16,
//   0 = never), order=hilbert|first (default hilbert).
// Prints one JSON line of stats.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <numeric>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <dirent.h>
#include <omp.h>
#include <parallel/algorithm>

#include "types.h"
#include "geometry.h"
#include "polygon_rings.h"

using Pts = std::vector<std::pair<double, double>>;
static double now() { return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count(); }

template <class T>
static std::vector<T> slurp(const std::string& p) {
    std::ifstream f(p, std::ios::binary | std::ios::ate);
    if (!f) { std::cerr << "cannot open " << p << "\n"; std::exit(1); }
    size_t n = static_cast<size_t>(f.tellg());
    if (n % sizeof(T)) { std::cerr << p << ": size not a multiple of " << sizeof(T) << "\n"; std::exit(1); }
    std::vector<T> b(n / sizeof(T));
    f.seekg(0);
    f.read(reinterpret_cast<char*>(b.data()), n);
    return b;
}
static void spit(const std::string& p, const void* d, size_t n) {
    std::ofstream f(p, std::ios::binary);
    f.write(static_cast<const char*>(d), static_cast<std::streamsize>(n));
    f.flush();
    if (!f) { std::cerr << "failed to write " << p << "\n"; std::exit(1); }
}

// ---------------------------------------------------------------- scheme
struct Scheme {
    bool none = false;
    double level_m[16];
    double cap = 0, capmin = 0;
    double q = 1e-5;
    bool repair = true;
    uint32_t bbox_min_n = 16;
    uint32_t chunk = 0;  // refs per chunk bbox in poly_arcs (0: none)
    bool hilbert = true;
    std::string text;
};
static Scheme parse(const std::string& s) {
    Scheme sc;
    sc.text = s;
    for (int l = 0; l < 16; l++) sc.level_m[l] = admin_epsilon_meters(static_cast<uint8_t>(l)) * 2.5;
    std::stringstream ss(s);
    std::string kv;
    double mx = 1e18, mn = 0;
    std::map<int, double> over;
    while (std::getline(ss, kv, ';')) {
        if (kv.empty()) continue;
        if (kv == "none") { sc.none = true; continue; }
        auto eq = kv.find('=');
        if (eq == std::string::npos) { std::cerr << "bad scheme item " << kv << "\n"; std::exit(1); }
        std::string k = kv.substr(0, eq), v = kv.substr(eq + 1);
        if (k == "scale") { double f = std::atof(v.c_str()); for (int l = 0; l < 16; l++) sc.level_m[l] = admin_epsilon_meters(static_cast<uint8_t>(l)) * f; }
        else if (k == "uni") { double m = std::atof(v.c_str()); for (int l = 0; l < 16; l++) sc.level_m[l] = m; }
        else if (k[0] == 't' && k.size() > 1 && std::isdigit(static_cast<unsigned char>(k[1]))) over[std::atoi(k.c_str() + 1)] = std::atof(v.c_str());
        else if (k == "max") mx = std::atof(v.c_str());
        else if (k == "min") mn = std::atof(v.c_str());
        else if (k == "cap") sc.cap = std::atof(v.c_str());
        else if (k == "capmin") sc.capmin = std::atof(v.c_str());
        else if (k == "q") sc.q = std::atof(v.c_str());
        else if (k == "repair") sc.repair = std::atoi(v.c_str()) != 0;
        else if (k == "bbox") sc.bbox_min_n = static_cast<uint32_t>(std::atoi(v.c_str()));
        else if (k == "chunk") sc.chunk = static_cast<uint32_t>(std::atoi(v.c_str()));
        else if (k == "order") sc.hilbert = v != "first";
        else { std::cerr << "unknown scheme key " << k << "\n"; std::exit(1); }
    }
    for (auto& [l, m] : over) if (l >= 0 && l < 16) sc.level_m[l] = m;
    for (int l = 0; l < 16; l++) sc.level_m[l] = std::max(mn, std::min(mx, sc.level_m[l]));
    return sc;
}

// ---------------------------------------------------------------- input
struct Dump {
    std::vector<AdminPolygon> polys;
    std::vector<NodeCoord> verts;
    std::vector<uint64_t> osm;
    std::vector<uint32_t> parents;
};

// Raw ring records of the arcdump, by packed osm key (as admin_osm_ids).
struct ArcDump {
    std::vector<std::vector<uint8_t>> files;
    std::unordered_map<uint64_t, std::pair<uint32_t, size_t>> at;  // key -> (file, offset)
};
template <class T>
static T rd(const uint8_t* p) { T v; std::memcpy(&v, p, sizeof(T)); return v; }

static ArcDump load_arcdump(const std::string& dir) {
    ArcDump a;
    DIR* d = opendir(dir.c_str());
    if (!d) { std::cerr << "cannot open " << dir << "\n"; std::exit(1); }
    std::vector<std::string> names;
    while (dirent* e = readdir(d)) {
        std::string n = e->d_name;
        if (n.rfind("rings.", 0) == 0) names.push_back(n);
    }
    closedir(d);
    std::sort(names.begin(), names.end());
    a.files.resize(names.size());
#pragma omp parallel for schedule(dynamic, 1)
    for (size_t i = 0; i < names.size(); i++) a.files[i] = slurp<uint8_t>(dir + "/" + names[i]);
    size_t dup = 0;
    for (uint32_t fi = 0; fi < a.files.size(); fi++) {
        const auto& b = a.files[fi];
        size_t o = 0;
        while (o < b.size()) {
            const uint8_t type = b[o];
            const int64_t id = rd<int64_t>(&b[o + 1]);
            const uint16_t ring = rd<uint16_t>(&b[o + 9]);
            const uint16_t holes = rd<uint16_t>(&b[o + 12]);
            uint64_t key = (uint64_t(type) << 56) |
                           ((type == 3 ? ((uint64_t(id) << 16) | ring) : uint64_t(id)) & 0x00FFFFFFFFFFFFFFull);
            if (!a.at.emplace(key, std::make_pair(fi, o)).second) dup++;
            size_t p = o + 14;
            for (uint32_t r = 0; r <= holes; r++) {
                const uint32_t n = rd<uint32_t>(&b[p]);
                p += 4 + size_t(n) * 8 + size_t(n ? n - 1 : 0) * 8;
            }
            o = p;
        }
    }
    if (dup) std::cerr << "arcdump: " << dup << " duplicate keys (first kept)\n";
    return a;
}

// ---------------------------------------------------------------- topology
struct Ring {
    uint32_t poly;   // index into the selected polygon list
    uint32_t start;  // into the occurrence arrays (closing vertex dropped)
    uint32_t len;
    uint8_t level;
    bool hole;
    double cap_deg;  // 0: no cap
};
struct ArcRef { uint32_t arc; bool rev; };

static inline uint64_t mix(uint64_t h, uint64_t v) {
    h ^= v + 0x9E3779B97F4A7C15ull + (h << 6) + (h >> 2);
    return h * 0xff51afd7ed558ccdull;
}

static uint64_t hilbert_d(uint32_t n, uint32_t x, uint32_t y) {
    uint64_t d = 0;
    for (uint32_t s = n / 2; s > 0; s /= 2) {
        uint32_t rx = (x & s) > 0, ry = (y & s) > 0;
        d += uint64_t(s) * s * ((3 * rx) ^ ry);
        if (ry == 0) {
            if (rx == 1) { x = s - 1 - x; y = s - 1 - y; }
            std::swap(x, y);
        }
    }
    return d;
}

static inline int64_t floordiv(int64_t a, int64_t b) { int64_t q = a / b; if ((a % b != 0) && ((a < 0) != (b < 0))) q--; return q; }

// Perpendicular distance in degrees, dp_simplify's metric.
static inline double seg_dist(double ax, double ay, double bx, double by, double x, double y) {
    double dx = bx - ax, dy = by - ay, px = x - ax, py = y - ay;
    double len_sq = dx * dx + dy * dy;
    if (len_sq == 0) return std::sqrt(px * px + py * py);
    double t = std::max(0.0, std::min(1.0, (px * dx + py * dy) / len_sq));
    double qx = t * dx - px, qy = t * dy - py;
    return std::sqrt(qx * qx + qy * qy);
}

static inline __int128 orient(int64_t ax, int64_t ay, int64_t bx, int64_t by, int64_t cx, int64_t cy) {
    return (__int128)(bx - ax) * (cy - ay) - (__int128)(by - ay) * (cx - ax);
}
static inline int sgn(__int128 v) { return (v > 0) - (v < 0); }
static inline bool on_seg(int64_t ax, int64_t ay, int64_t bx, int64_t by, int64_t cx, int64_t cy) {
    return std::min(ax, bx) <= cx && cx <= std::max(ax, bx) && std::min(ay, by) <= cy && cy <= std::max(ay, by);
}
// Segments a-b and c-d meet anywhere but at a shared endpoint position.
static bool seg_conflict(int64_t ax, int64_t ay, int64_t bx, int64_t by, int64_t cx, int64_t cy, int64_t dx, int64_t dy) {
    auto same = [](int64_t px, int64_t py, int64_t qx, int64_t qy) { return px == qx && py == qy; };
    const bool sh_ac = same(ax, ay, cx, cy), sh_ad = same(ax, ay, dx, dy), sh_bc = same(bx, by, cx, cy), sh_bd = same(bx, by, dx, dy);
    const int shared = sh_ac + sh_ad + sh_bc + sh_bd;
    if (shared >= 2) return false;  // the same segment (either way) or a degenerate one: not a crossing
    int o1 = sgn(orient(ax, ay, bx, by, cx, cy)), o2 = sgn(orient(ax, ay, bx, by, dx, dy));
    int o3 = sgn(orient(cx, cy, dx, dy, ax, ay)), o4 = sgn(orient(cx, cy, dx, dy, bx, by));
    if (shared == 1) {
        // Meeting at one shared endpoint is fine unless they overlap collinearly beyond it.
        if (o1 == 0 && o2 == 0) {
            // collinear: overlap if the non-shared endpoint of one lies on the other
            if (sh_ac || sh_ad) { if (on_seg(cx, cy, dx, dy, bx, by) && !(sh_bc || sh_bd)) return true; }
            if (sh_bc || sh_bd) { if (on_seg(cx, cy, dx, dy, ax, ay) && !(sh_ac || sh_ad)) return true; }
            if (sh_ac || sh_bc) { if (on_seg(ax, ay, bx, by, dx, dy)) return true; }
            if (sh_ad || sh_bd) { if (on_seg(ax, ay, bx, by, cx, cy)) return true; }
        }
        return false;
    }
    if (o1 != o2 && o3 != o4) return true;
    if (o1 == 0 && on_seg(ax, ay, bx, by, cx, cy)) return true;
    if (o2 == 0 && on_seg(ax, ay, bx, by, dx, dy)) return true;
    if (o3 == 0 && on_seg(cx, cy, dx, dy, ax, ay)) return true;
    if (o4 == 0 && on_seg(cx, cy, dx, dy, bx, by)) return true;
    return false;
}

static inline uint64_t zz(int64_t x) { return x >= 0 ? uint64_t(x) << 1 : (uint64_t(-x) << 1) - 1; }
static inline int64_t unzz(uint64_t z) { return (z & 1) ? -int64_t((z + 1) >> 1) : int64_t(z >> 1); }
static inline void put_varint(std::vector<uint8_t>& o, uint64_t v) {
    while (v >= 0x80) { o.push_back(uint8_t(v) | 0x80); v >>= 7; }
    o.push_back(uint8_t(v));
}
template <class T>
static void put(std::vector<uint8_t>& b, T v) { size_t o = b.size(); b.resize(o + sizeof(T)); std::memcpy(&b[o], &v, sizeof(T)); }

// Packs a keyhole range as encoding 4 (raw f32 lat/lng per vertex).
static void pack_f32(std::vector<uint8_t>& out, const Pts& pts) {
    float ml = INFINITY, mg = INFINITY;
    for (auto& [a, b] : pts) { ml = std::min(ml, float(a)); mg = std::min(mg, float(b)); }
    out.push_back(4);
    out.push_back(0);
    put<float>(out, ml);
    put<float>(out, mg);
    for (auto& [a, b] : pts) { put<float>(out, float(a)); put<float>(out, float(b)); }
}

int main(int argc, char** argv) {
    if (argc != 7) {
        std::cerr << "usage: arcs <minimal|quality> <dump> <arcdump> <admin_dir|-> <out> <scheme>\n";
        return 1;
    }
    const std::string mode = argv[1], dump_dir = argv[2], arc_dir = argv[3], admin_dir = argv[4], out = argv[5];
    const Scheme sc = parse(argv[6]);
    const bool minimal = mode == "minimal";
    if (!minimal && mode != "quality") { std::cerr << "mode must be minimal or quality\n"; return 1; }
    const int64_t F = std::llround(sc.q / 1e-7);  // grid step in 1e-7 units
    if (F < 1 || std::fabs(F * 1e-7 - sc.q) > 1e-12) { std::cerr << "q must be a multiple of 1e-7\n"; return 1; }
    std::string cmd = "mkdir -p '" + out + "/arc' '" + out + "/dec'";
    if (std::system(cmd.c_str()) != 0) return 1;
    double t0 = now(), tl;

    // ---- load
    Dump x;
    x.polys = slurp<AdminPolygon>(dump_dir + "/admin_polygons.raw");
    x.verts = slurp<NodeCoord>(dump_dir + "/admin_vertices.raw");
    x.osm = slurp<uint64_t>(dump_dir + "/admin_osm_ids.raw");
    x.parents = slurp<uint32_t>(dump_dir + "/admin_parent_ids.raw");
    const size_t n_all = x.polys.size();
    ArcDump ad = load_arcdump(arc_dir);
    tl = now() - t0;

    // Selected polygons: minimal = levels 2-8 in dump order; quality = all.
    std::vector<uint32_t> sel;
    for (size_t i = 0; i < n_all; i++) {
        if (x.polys[i].vertex_count < 3) continue;
        if (minimal && (x.polys[i].admin_level < 2 || x.polys[i].admin_level > 8)) continue;
        sel.push_back(static_cast<uint32_t>(i));
    }

    // ---- rings into occurrence arrays
    std::vector<int32_t> olat, olng;
    std::vector<int64_t> oway;
    std::vector<Ring> rings;
    std::vector<uint32_t> poly_ring0(sel.size() + 1, 0);
    size_t fallback = 0, dropped_rings = 0;
    std::vector<int32_t> tla, tln;
    std::vector<int64_t> twy;
    for (size_t s = 0; s < sel.size(); s++) {
        const AdminPolygon& ap = x.polys[sel[s]];
        poly_ring0[s] = static_cast<uint32_t>(rings.size());
        auto add_ring = [&](const std::vector<int32_t>& la, const std::vector<int32_t>& ln, const std::vector<int64_t>& wy, bool hole) {
            // la/ln closed (last == first); wy per edge. Drop the closing vertex and repeats.
            size_t n = la.size();
            if (n >= 2 && la[0] == la[n - 1] && ln[0] == ln[n - 1]) n--;
            const uint32_t start = static_cast<uint32_t>(olat.size());
            for (size_t k = 0; k < n; k++) {
                if (olat.size() > start && olat.back() == la[k] && olng.back() == ln[k]) {
                    continue;
                }
                olat.push_back(la[k]);
                olng.push_back(ln[k]);
                oway.push_back(k < wy.size() ? wy[k] : 0);
            }
            while (olat.size() - start >= 2 && olat.back() == olat[start] && olng.back() == olng[start]) {
                olat.pop_back(); olng.pop_back(); oway.pop_back();
            }
            uint32_t len = static_cast<uint32_t>(olat.size() - start);
            if (len < 3) {
                olat.resize(start); olng.resize(start); oway.resize(start);
                dropped_rings++;
                return;
            }
            Ring r{static_cast<uint32_t>(s), start, len, ap.admin_level, hole, 0};
            if (sc.cap > 0) {
                Pts p(len + 1);
                for (uint32_t k = 0; k < len; k++) p[k] = {olat[start + k] * 1e-7, olng[start + k] * 1e-7};
                p[len] = p[0];
                r.cap_deg = sc.cap * std::sqrt(polygon_area(p));
            }
            rings.push_back(r);
        };
        auto it = ad.at.find(x.osm[sel[s]]);
        if (it != ad.at.end()) {
            const uint8_t* b = ad.files[it->second.first].data() + it->second.second;
            const uint16_t holes = rd<uint16_t>(b + 12);
            const uint8_t* p = b + 14;
            for (uint32_t r = 0; r <= holes; r++) {
                const uint32_t n = rd<uint32_t>(p);
                tla.resize(n); tln.resize(n); twy.resize(n ? n - 1 : 0);
                std::memcpy(tla.data(), p + 4, size_t(n) * 4);
                std::memcpy(tln.data(), p + 4 + size_t(n) * 4, size_t(n) * 4);
                if (n) std::memcpy(twy.data(), p + 4 + size_t(n) * 8, size_t(n - 1) * 8);
                p += 4 + size_t(n) * 8 + size_t(n ? n - 1 : 0) * 8;
                add_ring(tla, tln, twy, r > 0);
            }
        } else {
            fallback++;
            std::vector<RingSpan> spans;
            const NodeCoord* v = x.verts.data() + ap.vertex_offset;
            split_rings(v, ap.vertex_count, ap.hole_count, spans);
            for (size_t r = 0; r < spans.size(); r++) {
                tla.clear(); tln.clear(); twy.clear();
                for (uint32_t k = 0; k < spans[r].count; k++) {
                    tla.push_back(static_cast<int32_t>(std::lround(double(v[spans[r].begin + k].lat) * 1e7)));
                    tln.push_back(static_cast<int32_t>(std::lround(double(v[spans[r].begin + k].lng) * 1e7)));
                }
                add_ring(tla, tln, twy, r > 0);
            }
        }
    }
    poly_ring0[sel.size()] = static_cast<uint32_t>(rings.size());
    ad.files.clear(); ad.files.shrink_to_fit(); ad.at.clear();
    const size_t N = olat.size(), R = rings.size();
    std::vector<uint32_t> ring_of(N);
#pragma omp parallel for schedule(dynamic, 64)
    for (size_t r = 0; r < R; r++)
        for (uint32_t k = 0; k < rings[r].len; k++) ring_of[rings[r].start + k] = static_cast<uint32_t>(r);
    const double t_load = now() - t0;

    // ---- vertex ids
    std::vector<uint32_t> vid(N);
    std::vector<int32_t> vlat, vlng;
    {
        std::vector<std::pair<uint64_t, uint32_t>> k(N);
#pragma omp parallel for
        for (size_t i = 0; i < N; i++)
            k[i] = {(uint64_t(uint32_t(olat[i])) << 32) | uint32_t(olng[i]), static_cast<uint32_t>(i)};
        __gnu_parallel::sort(k.begin(), k.end());
        uint32_t id = 0;
        for (size_t i = 0; i < N; i++) {
            if (i && k[i].first != k[i - 1].first) id++;
            vid[k[i].second] = id;
            if (i == 0 || k[i].first != k[i - 1].first) { vlat.push_back(olat[k[i].second]); vlng.push_back(olng[k[i].second]); }
        }
    }
    const size_t V = vlat.size();

    // ---- junctions: a vertex whose union-graph degree is not 2
    std::vector<uint32_t> nb1(V, UINT32_MAX), nb2(V, UINT32_MAX);
    std::vector<uint8_t> junction(V, 0);
    std::vector<uint16_t> vuse(V, 0);  // rings through the vertex (saturating)
    for (size_t r = 0; r < R; r++) {
        const Ring& g = rings[r];
        for (uint32_t k = 0; k < g.len; k++) {
            uint32_t v = vid[g.start + k];
            uint32_t a = vid[g.start + (k + g.len - 1) % g.len], b = vid[g.start + (k + 1) % g.len];
            if (vuse[v] < 65535) vuse[v]++;
            for (uint32_t w : {a, b}) {
                if (w == nb1[v] || w == nb2[v]) continue;
                if (nb1[v] == UINT32_MAX) nb1[v] = w;
                else if (nb2[v] == UINT32_MAX) nb2[v] = w;
                else junction[v] = 1;
            }
        }
    }
    size_t n_junction = 0;
    for (size_t v = 0; v < V; v++) {
        if (nb2[v] == UINT32_MAX) junction[v] = 1;
        n_junction += junction[v];
    }
    nb1.clear(); nb1.shrink_to_fit(); nb2.clear(); nb2.shrink_to_fit();
    const double t_vid = now() - t0;

    // ---- pieces -> arcs
    struct Piece { uint32_t ring, pos, len; bool rev; bool closed; uint64_t h; };
    std::vector<std::vector<Piece>> ring_pieces(R);
#pragma omp parallel for schedule(dynamic, 64)
    for (size_t r = 0; r < R; r++) {
        const Ring& g = rings[r];
        auto V_ = [&](uint32_t k) { return vid[g.start + (k % g.len)]; };
        std::vector<uint32_t> js;
        for (uint32_t k = 0; k < g.len; k++) if (junction[V_(k)]) js.push_back(k);
        auto& out_p = ring_pieces[r];
        if (js.empty()) {
            // closed arc: canonical rotation (smallest vid first), direction by the second vertex
            uint32_t m = 0;
            for (uint32_t k = 1; k < g.len; k++) if (V_(k) < V_(m)) m = k;
            bool rev = V_(m + g.len - 1) < V_(m + 1);
            // forward walk m, m+1, .., m; reversed walk m, m-1, .., m
            uint64_t h = 0x12345;
            for (uint32_t i = 0; i <= g.len; i++) h = mix(h, rev ? V_(m + g.len * 2 - i) : V_(m + i));
            out_p.push_back({static_cast<uint32_t>(r), m, g.len + 1, rev, true, h});
            continue;
        }
        for (size_t j = 0; j < js.size(); j++) {
            uint32_t a = js[j], b = j + 1 < js.size() ? js[j + 1] : js[0] + g.len;
            uint32_t len = b - a + 1;
            uint32_t f = V_(a), l = V_(b);
            bool rev = f > l || (f == l && len > 2 && V_(a + 1) > V_(b - 1));
            uint64_t h = 0x6789;
            for (uint32_t i = 0; i < len; i++) h = mix(h, rev ? V_(b + g.len - i) : V_(a + i));
            out_p.push_back({static_cast<uint32_t>(r), a, len, rev, false, h});
        }
    }
    // Sequence of a piece in canonical direction.
    auto piece_vid = [&](const Piece& p, uint32_t i) -> uint32_t {
        const Ring& g = rings[p.ring];
        if (!p.closed) {
            uint32_t k = p.rev ? (p.pos + p.len - 1 - i) : (p.pos + i);
            return vid[g.start + (k % g.len)];
        }
        uint32_t k = p.rev ? (p.pos + 2 * g.len - i) : (p.pos + i);
        return vid[g.start + (k % g.len)];
    };
    auto piece_way = [&](const Piece& p, uint32_t i) -> int64_t {  // edge i -> i+1 in canonical direction
        const Ring& g = rings[p.ring];
        if (!p.closed) {
            uint32_t k = p.rev ? (p.pos + p.len - 2 - i) : (p.pos + i);
            return oway[g.start + (k % g.len)];
        }
        uint32_t k = p.rev ? (p.pos + 2 * g.len - i - 1) : (p.pos + i);
        return oway[g.start + (k % g.len)];
    };
    std::vector<std::pair<uint64_t, uint64_t>> order;  // (hash, ring<<20 | piece idx)
    for (size_t r = 0; r < R; r++)
        for (size_t j = 0; j < ring_pieces[r].size(); j++) order.push_back({ring_pieces[r][j].h, (uint64_t(r) << 24) | j});
    __gnu_parallel::sort(order.begin(), order.end());
    auto piece_at = [&](uint64_t code) -> Piece& { return ring_pieces[code >> 24][code & 0xFFFFFF]; };
    std::vector<std::vector<ArcRef>> refs(R);
    for (size_t r = 0; r < R; r++) refs[r].resize(ring_pieces[r].size());
    struct Arc { uint64_t piece; uint32_t len; bool closed; uint32_t users; double tol_m; double cap_deg; uint8_t minlvl, maxlvl; bool way_agree; };
    std::vector<Arc> arcs;
    size_t collisions = 0, way_disagree_arcs = 0, shared_arcs = 0;
    {
        size_t i = 0;
        std::vector<uint32_t> group_arcs;
        while (i < order.size()) {
            size_t j = i;
            while (j < order.size() && order[j].first == order[i].first) j++;
            group_arcs.clear();
            for (size_t k = i; k < j; k++) {
                const Piece& p = piece_at(order[k].second);
                uint32_t found = UINT32_MAX;
                for (uint32_t a : group_arcs) {
                    const Piece& q = piece_at(arcs[a].piece);
                    if (q.len != p.len || q.closed != p.closed) continue;
                    bool eq = true;
                    for (uint32_t t = 0; t < p.len && eq; t++) eq = piece_vid(p, t) == piece_vid(q, t);
                    if (eq) { found = a; break; }
                }
                if (found == UINT32_MAX) {
                    if (!group_arcs.empty()) collisions++;
                    found = static_cast<uint32_t>(arcs.size());
                    arcs.push_back({order[k].second, p.len, p.closed, 0, 1e18, 1e18, 255, 0, true});
                    group_arcs.push_back(found);
                } else {
                    // same arc from another ring: same member ways?
                    const Piece& q = piece_at(arcs[found].piece);
                    if (arcs[found].way_agree)
                        for (uint32_t t = 0; t + 1 < p.len; t++)
                            if (piece_way(p, t) != piece_way(q, t)) { arcs[found].way_agree = false; break; }
                }
                Arc& A = arcs[found];
                A.users++;
                const Ring& g = rings[p.ring];
                A.tol_m = std::min(A.tol_m, sc.level_m[g.level < 16 ? g.level : 15]);
                if (g.cap_deg > 0) A.cap_deg = std::min(A.cap_deg, g.cap_deg);
                A.minlvl = std::min(A.minlvl, g.level);
                A.maxlvl = std::max(A.maxlvl, g.level);
                refs[order[k].second >> 24][order[k].second & 0xFFFFFF] = {found, p.rev};
            }
            i = j;
        }
    }
    for (auto& a : arcs) { shared_arcs += a.users > 1; way_disagree_arcs += a.users > 1 && !a.way_agree; }
    order.clear(); order.shrink_to_fit();
    const size_t A = arcs.size();
    // Arc vertex sequences (vids, canonical direction) and way ids per edge.
    std::vector<uint64_t> arc_v0(A + 1, 0);
    for (size_t a = 0; a < A; a++) arc_v0[a + 1] = arc_v0[a] + arcs[a].len;
    std::vector<uint32_t> av(arc_v0[A]);
#pragma omp parallel for schedule(dynamic, 256)
    for (size_t a = 0; a < A; a++) {
        const Piece& p = piece_at(arcs[a].piece);
        for (uint32_t t = 0; t < p.len; t++) av[arc_v0[a] + t] = piece_vid(p, t);
    }
    // Way ids of each arc's edges, for the patch study's stable key.
    std::vector<uint64_t> arc_wayhash(A, 0);
#pragma omp parallel for schedule(dynamic, 256)
    for (size_t a = 0; a < A; a++) {
        const Piece& p = piece_at(arcs[a].piece);
        std::vector<int64_t> w;
        for (uint32_t t = 0; t + 1 < p.len; t++) w.push_back(piece_way(p, t));
        std::sort(w.begin(), w.end());
        w.erase(std::unique(w.begin(), w.end()), w.end());
        uint64_t h = 0xabc;
        for (int64_t v : w) h = mix(h, uint64_t(v));
        arc_wayhash[a] = h;
    }
    ring_pieces.clear(); ring_pieces.shrink_to_fit();
    const double t_arcs = now() - t0;

    // ---- storage order
    std::vector<uint32_t> perm(A);  // new -> old
    std::iota(perm.begin(), perm.end(), 0);
    if (sc.hilbert) {
        std::vector<uint64_t> hk(A);
#pragma omp parallel for
        for (size_t a = 0; a < A; a++) {
            uint32_t v0 = av[arc_v0[a]];
            uint32_t xq = uint32_t((int64_t(vlng[v0]) + 1800000000ll) * 65535 / 3600000000ll);
            uint32_t yq = uint32_t((int64_t(vlat[v0]) + 900000000ll) * 65535 / 1800000000ll);
            hk[a] = (hilbert_d(65536, xq, yq) << 20) | (a & 0xFFFFF);
        }
        __gnu_parallel::sort(perm.begin(), perm.end(), [&](uint32_t p, uint32_t q) { return hk[p] != hk[q] ? hk[p] < hk[q] : p < q; });
    } else {
        // first use in slot order: arcs are already numbered by hash; renumber by first ref
        std::vector<uint32_t> first(A, UINT32_MAX);
        uint32_t c = 0;
        for (size_t r = 0; r < R; r++) for (auto& f : refs[r]) if (first[f.arc] == UINT32_MAX) first[f.arc] = c++;
        std::sort(perm.begin(), perm.end(), [&](uint32_t p, uint32_t q) { return first[p] < first[q]; });
    }
    std::vector<uint32_t> inv(A);
    for (size_t k = 0; k < A; k++) inv[perm[k]] = static_cast<uint32_t>(k);

    // ---- simplify each arc once
    // keep[] per arc over its raw vertices.
    std::vector<uint8_t> keep(arc_v0[A], 0);
    std::vector<double> arc_tol(A);
    auto raw_pt = [&](size_t a, uint32_t i) {
        uint32_t v = av[arc_v0[a] + i];
        return std::pair<double, double>(vlat[v] * 1e-7, vlng[v] * 1e-7);
    };
#pragma omp parallel for schedule(dynamic, 64)
    for (size_t a = 0; a < A; a++) {
        const uint32_t n = arcs[a].len;
        uint8_t* kp = &keep[arc_v0[a]];
        if (sc.none) { std::fill(kp, kp + n, 1); arc_tol[a] = 0; continue; }
        Pts pts(n);
        for (uint32_t i = 0; i < n; i++) pts[i] = raw_pt(a, i);
        double eps = meters_to_degrees(arcs[a].tol_m, pts[0].first);
        if (sc.cap > 0 && arcs[a].cap_deg < 1e17) {
            double c = arcs[a].cap_deg;
            if (sc.capmin > 0) c = std::max(c, meters_to_degrees(sc.capmin, pts[0].first));
            eps = std::min(eps, c);
        }
        arc_tol[a] = eps;
        const bool loop = pts.front() == pts.back();
        std::vector<bool> kb(n, false);
        kb[0] = kb[n - 1] = true;
        if (n > 2) dp_simplify(pts, 0, n - 1, eps, kb);
        if (loop) {
            // as simplify_polygon_epsilon: at least 4 (closed), lowering epsilon by bisection
            const size_t min_v = 4;
            size_t cnt = 0;
            for (bool b : kb) cnt += b;
            if (n > min_v && cnt < min_v) {
                double lo = 0, hi = eps;
                for (int it = 0; it < 20; it++) {
                    double mid = (lo + hi) / 2;
                    std::vector<bool> tk(n, false);
                    tk[0] = tk[n - 1] = true;
                    dp_simplify(pts, 0, n - 1, mid, tk);
                    size_t c = 0;
                    for (bool b : tk) c += b;
                    if (c >= min_v) { lo = mid; kb = tk; cnt = c; } else hi = mid;
                }
            }
        }
        for (uint32_t i = 0; i < n; i++) kp[i] = kb[i];
    }
    const double t_simpl = now() - t0;

    // Quantized kept vertices of an arc (grid units), consecutive repeats dropped.
    auto qv = [&](int32_t c) { return floordiv(int64_t(c) + F / 2, F); };
    auto arc_q = [&](size_t a, std::vector<int64_t>& la, std::vector<int64_t>& ln, std::vector<uint32_t>* idx) {
        la.clear(); ln.clear(); if (idx) idx->clear();
        const uint32_t n = arcs[a].len;
        for (uint32_t i = 0; i < n; i++) {
            if (!keep[arc_v0[a] + i]) continue;
            uint32_t v = av[arc_v0[a] + i];
            int64_t y = qv(vlat[v]), xx = qv(vlng[v]);
            if (!la.empty() && la.back() == y && ln.back() == xx) {
                // a repeat after snapping: the same position, so only the raw index can matter
                // (the endpoint's, so spans still end at the arc's last vertex)
                if (i == n - 1 && idx && la.size() > 1) idx->back() = i;
                else if (i == n - 1 && la.size() == 1) { la.push_back(y); ln.push_back(xx); if (idx) idx->push_back(i); }
                continue;
            }
            la.push_back(y); ln.push_back(xx); if (idx) idx->push_back(i);
        }
    };
    // Add back the farthest dropped raw vertex of arc a (in any span); false if none.
    auto refine_arc = [&](size_t a) -> bool {
        const uint32_t n = arcs[a].len;
        uint8_t* kp = &keep[arc_v0[a]];
        double best = -1; uint32_t bi = 0;
        uint32_t prev = 0;
        for (uint32_t i = 1; i < n; i++) {
            if (!kp[i]) continue;
            if (i > prev + 1) {
                auto pa = raw_pt(a, prev), pb = raw_pt(a, i);
                for (uint32_t k = prev + 1; k < i; k++) {
                    auto p = raw_pt(a, k);
                    double d = seg_dist(pa.first, pa.second, pb.first, pb.second, p.first, p.second);
                    if (d > best) { best = d; bi = k; }
                }
            }
            prev = i;
        }
        if (best < 0) return false;
        kp[bi] = 1;
        return true;
    };
    auto refine_span = [&](size_t a, uint32_t i0, uint32_t i1) -> bool {  // raw indices of a kept segment
        if (i1 <= i0 + 1) return false;
        auto pa = raw_pt(a, i0), pb = raw_pt(a, i1);
        double best = -1; uint32_t bi = 0;
        for (uint32_t k = i0 + 1; k < i1; k++) {
            if (keep[arc_v0[a] + k]) return false;  // already refined this round
            auto p = raw_pt(a, k);
            double d = seg_dist(pa.first, pa.second, pb.first, pb.second, p.first, p.second);
            if (d > best) { best = d; bi = k; }
        }
        keep[arc_v0[a] + bi] = 1;
        return true;
    };

    // ---- ring validity: >= 3 distinct vertices and a non-zero area after quantization
    size_t ring_fix_rounds = 0, ring_fix_added = 0, rings_degenerate = 0;
    auto ring_q = [&](size_t r, Pts& pts) {
        pts.clear();
        std::vector<int64_t> la, ln;
        for (const ArcRef& f : refs[r]) {
            arc_q(f.arc, la, ln, nullptr);
            size_t m = la.size();
            for (size_t i = 0; i < m; i++) {
                size_t k = f.rev ? m - 1 - i : i;
                std::pair<double, double> p{double(la[k]) * sc.q, double(ln[k]) * sc.q};
                if (!pts.empty() && pts.back() == p) continue;
                pts.push_back(p);
            }
        }
        if (pts.size() >= 2 && pts.front() == pts.back()) pts.pop_back();
    };
    auto ring_ok = [&](const Pts& pts) {
        if (pts.size() < 3) return false;
        Pts c = pts; c.push_back(c.front());
        return polygon_area(c) > 0;
    };
    {
        std::vector<uint32_t> bad;
        for (int round = 0; round < 64; round++) {
            bad.clear();
            std::mutex mu;
#pragma omp parallel
            {
                Pts pts;
                std::vector<uint32_t> mine;
#pragma omp for schedule(dynamic, 256) nowait
                for (size_t r = 0; r < R; r++) { ring_q(r, pts); if (!ring_ok(pts)) mine.push_back(static_cast<uint32_t>(r)); }
                std::lock_guard<std::mutex> lk(mu);
                bad.insert(bad.end(), mine.begin(), mine.end());
            }
            if (bad.empty()) break;
            ring_fix_rounds++;
            size_t added = 0;
            std::sort(bad.begin(), bad.end());
            for (uint32_t r : bad) {
                // refine the ring's arc with the largest remaining deviation
                double best = -1; size_t ba = SIZE_MAX;
                for (const ArcRef& f : refs[r]) {
                    const uint32_t n = arcs[f.arc].len;
                    uint32_t prev = 0;
                    for (uint32_t i = 1; i < n; i++) {
                        if (!keep[arc_v0[f.arc] + i]) continue;
                        if (i > prev + 1) {
                            auto pa = raw_pt(f.arc, prev), pb = raw_pt(f.arc, i);
                            for (uint32_t k = prev + 1; k < i; k++) {
                                auto p = raw_pt(f.arc, k);
                                double d = seg_dist(pa.first, pa.second, pb.first, pb.second, p.first, p.second);
                                if (d > best) { best = d; ba = f.arc; }
                            }
                        }
                        prev = i;
                    }
                }
                if (ba != SIZE_MAX && refine_arc(ba)) added++;
            }
            ring_fix_added += added;
            if (!added) { rings_degenerate = bad.size(); break; }
        }
    }
    const double t_valid = now() - t0;

    // ---- topology repair: no two simplified segments may meet away from a shared vertex
    // Rounds test the pairs with a segment of an arc changed in the previous round (all arcs in
    // round 0); a last full round counts what is left. A conflict whose raw spans already meet
    // is in the data (overlapping polygons, postcode or place areas crossing admin borders):
    // simplification did not make it, so it is not repaired. Spans too long to compare are
    // treated as new (they shrink as they are refined).
    size_t cross0 = 0, cross_last = 0, repair_rounds = 0, repair_added = 0, unrepairable = 0, cross_real0 = 0, cross_real_last = 0;
    {
        const int64_t CELL = std::max<int64_t>(1, std::llround(0.02 / sc.q));
        struct Seg { uint32_t arc, i0, i1; int64_t ay, ax, by, bx; };
        std::unordered_set<uint64_t> known_real;
        auto pair_key = [&](const Seg& p, const Seg& q) {
            return mix(mix(mix(mix(mix(mix(0x31, p.arc), p.i0), p.i1), q.arc), q.i0), q.i1);
        };
        auto cell_key = [](int64_t yy, int64_t xx) { return (uint64_t(yy + (1 << 30)) << 32) | uint64_t(xx + (1 << 30)); };
        std::vector<uint8_t> dirty(A, 1);
        const int max_rounds = sc.none ? 0 : sc.repair ? 60 : 1;
        bool final_round = !sc.repair;
        for (int round = 0; round < max_rounds + 1; round++) {
            if (round == max_rounds) final_round = true;
            if (final_round) std::fill(dirty.begin(), dirty.end(), 1);
            std::vector<std::vector<Seg>> tsegs(omp_get_max_threads());
#pragma omp parallel
            {
                auto& S = tsegs[omp_get_thread_num()];
                std::vector<int64_t> la, ln;
                std::vector<uint32_t> idx;
#pragma omp for schedule(dynamic, 256)
                for (size_t a = 0; a < A; a++) {
                    arc_q(a, la, ln, &idx);
                    for (size_t k = 1; k < la.size(); k++)
                        S.push_back({static_cast<uint32_t>(a), idx[k - 1], idx[k], la[k - 1], ln[k - 1], la[k], ln[k]});
                }
            }
            std::vector<Seg> segs;
            for (auto& t : tsegs) { segs.insert(segs.end(), t.begin(), t.end()); t.clear(); t.shrink_to_fit(); }
            auto cells_of = [&](const Seg& g, auto&& fn) {
                int64_t y0 = floordiv(std::min(g.ay, g.by), CELL), y1 = floordiv(std::max(g.ay, g.by), CELL);
                int64_t x0 = floordiv(std::min(g.ax, g.bx), CELL), x1 = floordiv(std::max(g.ax, g.bx), CELL);
                for (int64_t yy = y0; yy <= y1; yy++)
                    for (int64_t xx = x0; xx <= x1; xx++) fn(cell_key(yy, xx));
            };
            const bool all_dirty = round == 0 || final_round;
            std::vector<uint64_t> hot;  // cells holding a segment of a dirty arc
            if (!all_dirty) {
                std::vector<std::vector<uint64_t>> th(omp_get_max_threads());
#pragma omp parallel for schedule(dynamic, 4096)
                for (size_t s = 0; s < segs.size(); s++)
                    if (dirty[segs[s].arc]) cells_of(segs[s], [&](uint64_t k) { th[omp_get_thread_num()].push_back(k); });
                for (auto& t : th) hot.insert(hot.end(), t.begin(), t.end());
                __gnu_parallel::sort(hot.begin(), hot.end());
                hot.erase(std::unique(hot.begin(), hot.end()), hot.end());
            }
            std::vector<std::pair<uint64_t, uint32_t>> ce;
            {
                std::vector<std::vector<std::pair<uint64_t, uint32_t>>> tce(omp_get_max_threads());
#pragma omp parallel for schedule(dynamic, 4096)
                for (size_t s = 0; s < segs.size(); s++) {
                    auto& T = tce[omp_get_thread_num()];
                    cells_of(segs[s], [&](uint64_t k) {
                        if (all_dirty || std::binary_search(hot.begin(), hot.end(), k)) T.push_back({k, static_cast<uint32_t>(s)});
                    });
                }
                for (auto& t : tce) { ce.insert(ce.end(), t.begin(), t.end()); t.clear(); t.shrink_to_fit(); }
            }
            __gnu_parallel::sort(ce.begin(), ce.end());
            std::vector<size_t> starts;
            for (size_t i = 0; i < ce.size(); i++) if (i == 0 || ce[i].first != ce[i - 1].first) starts.push_back(i);
            starts.push_back(ce.size());
            std::vector<std::vector<std::pair<uint32_t, uint32_t>>> tconf(omp_get_max_threads());
            const size_t ncell = starts.size() - 1;
#pragma omp parallel for schedule(dynamic, 64)
            for (size_t c = 0; c < ncell; c++) {
                auto& T = tconf[omp_get_thread_num()];
                for (size_t i = starts[c]; i < starts[c + 1]; i++) {
                    const Seg& p = segs[ce[i].second];
                    for (size_t j = i + 1; j < starts[c + 1]; j++) {
                        const Seg& q = segs[ce[j].second];
                        if (!dirty[p.arc] && !dirty[q.arc]) continue;
                        if (std::max(p.ay, p.by) < std::min(q.ay, q.by) || std::max(q.ay, q.by) < std::min(p.ay, p.by)) continue;
                        if (std::max(p.ax, p.bx) < std::min(q.ax, q.bx) || std::max(q.ax, q.bx) < std::min(p.ax, p.bx)) continue;
                        if (p.arc == q.arc && (p.i1 == q.i0 || q.i1 == p.i0)) continue;  // adjacent in one arc
                        if (seg_conflict(p.ay, p.ax, p.by, p.bx, q.ay, q.ax, q.by, q.bx))
                            T.push_back({std::min(ce[i].second, ce[j].second), std::max(ce[i].second, ce[j].second)});
                    }
                }
            }
            std::vector<std::pair<uint32_t, uint32_t>> conf;
            for (auto& t : tconf) conf.insert(conf.end(), t.begin(), t.end());
            std::sort(conf.begin(), conf.end());
            conf.erase(std::unique(conf.begin(), conf.end()), conf.end());
            std::vector<uint8_t> real(conf.size(), 0);
#pragma omp parallel for schedule(dynamic, 16)
            for (size_t c = 0; c < conf.size(); c++) {
                const Seg& p = segs[conf[c].first];
                const Seg& q = segs[conf[c].second];
                if (known_real.count(pair_key(p, q))) { real[c] = 1; continue; }
                if (uint64_t(p.i1 - p.i0) * (q.i1 - q.i0) > 4000000ull) continue;
                bool hit = false;
                for (uint32_t i = p.i0; i < p.i1 && !hit; i++) {
                    uint32_t u0 = av[arc_v0[p.arc] + i], u1 = av[arc_v0[p.arc] + i + 1];
                    int64_t ay = vlat[u0], ax = vlng[u0], by = vlat[u1], bx = vlng[u1];
                    for (uint32_t j = q.i0; j < q.i1 && !hit; j++) {
                        if (p.arc == q.arc && (j + 1 == i || i + 1 == j || i == j)) continue;
                        uint32_t w0 = av[arc_v0[q.arc] + j], w1 = av[arc_v0[q.arc] + j + 1];
                        int64_t cy = vlat[w0], cx = vlng[w0], dy = vlat[w1], dx = vlng[w1];
                        if (std::max(ay, by) < std::min(cy, dy) || std::max(cy, dy) < std::min(ay, by)) continue;
                        if (std::max(ax, bx) < std::min(cx, dx) || std::max(cx, dx) < std::min(ax, bx)) continue;
                        hit = seg_conflict(ay, ax, by, bx, cy, cx, dy, dx);
                    }
                }
                real[c] = hit;
            }
            size_t n_real = 0;
            for (size_t c = 0; c < conf.size(); c++) {
                n_real += real[c];
                if (real[c]) known_real.insert(pair_key(segs[conf[c].first], segs[conf[c].second]));
            }
            if (round == 0) { cross0 = conf.size() - n_real; cross_real0 = n_real; }
            if (final_round) { cross_last = conf.size() - n_real; cross_real_last = n_real; break; }
            if (conf.size() == n_real) { final_round = true; continue; }
            repair_rounds++;
            std::vector<uint32_t> fix;
            for (size_t c = 0; c < conf.size(); c++)
                if (!real[c]) { fix.push_back(conf[c].first); fix.push_back(conf[c].second); }
            std::sort(fix.begin(), fix.end());
            fix.erase(std::unique(fix.begin(), fix.end()), fix.end());
            std::fill(dirty.begin(), dirty.end(), 0);
            size_t added = 0, stuck = 0;
            for (uint32_t s : fix) {
                if (refine_span(segs[s].arc, segs[s].i0, segs[s].i1)) { added++; dirty[segs[s].arc] = 1; }
                else if (segs[s].i1 == segs[s].i0 + 1) stuck++;
            }
            repair_added += added;
            unrepairable = stuck;
            if (!added) final_round = true;
        }
    }
    const double t_repair = now() - t0;

    // ---- encode the arc format (storage order)
    std::vector<uint8_t> av_bytes;
    std::vector<uint32_t> arc_off(A + 1);
    std::vector<uint64_t> arc_keys(A * 3);  // raw content hash, block hash, way hash (storage order)
    std::vector<uint32_t> arc_n(A);
    uint64_t stored_vertices = 0, bbox_arcs = 0, width_count[3] = {0, 0, 0};
    std::vector<uint64_t> lvl_vertices(16, 0);  // stored vertices by the lowest admin_level (highest rank) using the arc
    std::vector<int32_t> abox(A * 3);           // per arc (storage order): min lng, max lng, max lat (grid units)
    {
        std::vector<std::vector<uint8_t>> blocks(A);
#pragma omp parallel
        {
            std::vector<int64_t> la, ln;
#pragma omp for schedule(dynamic, 256)
            for (size_t k = 0; k < A; k++) {
                const uint32_t a = perm[k];
                arc_q(a, la, ln, nullptr);
                const size_t n = la.size();
                arc_n[k] = static_cast<uint32_t>(n);
                int64_t maxd = 0;
                for (size_t i = 1; i < n; i++) maxd = std::max<int64_t>({maxd, std::llabs(la[i] - la[i - 1]), std::llabs(ln[i] - ln[i - 1])});
                int wc = maxd <= 127 ? 0 : maxd <= 32767 ? 1 : 2;
                const size_t w = size_t(1) << wc;
                const bool bb = sc.bbox_min_n && n >= sc.bbox_min_n;
                auto& B = blocks[k];
                B.push_back(uint8_t(wc | (bb ? 0x80 : 0)));
                put<int32_t>(B, int32_t(la[0]));
                put<int32_t>(B, int32_t(ln[0]));
                if (bb) {
                    put<int32_t>(B, int32_t(*std::min_element(la.begin(), la.end())));
                    put<int32_t>(B, int32_t(*std::max_element(la.begin(), la.end())));
                    put<int32_t>(B, int32_t(*std::min_element(ln.begin(), ln.end())));
                    put<int32_t>(B, int32_t(*std::max_element(ln.begin(), ln.end())));
                }
                const size_t m = n - 1, base = B.size();
                B.resize(base + 2 * w * m);
                for (int c = 0; c < 2; c++) {
                    const auto& v = c ? ln : la;
                    for (size_t i = 0; i < m; i++) {
                        uint64_t z = zz(v[i + 1] - v[i]);
                        for (size_t b = 0; b < w; b++) B[base + (c * w + b) * m + i] = uint8_t(z >> (8 * b));
                    }
                }
                uint64_t h = 0x55;
                for (size_t i = 0; i < arcs[a].len; i++) { uint32_t vv = av[arc_v0[a] + i]; h = mix(h, (uint64_t(uint32_t(vlat[vv])) << 32) | uint32_t(vlng[vv])); }
                arc_keys[k * 3] = h;
                uint64_t hb = 0x77;
                for (uint8_t c : B) hb = mix(hb, c);
                arc_keys[k * 3 + 1] = hb;
                arc_keys[k * 3 + 2] = arc_wayhash[a];
                abox[k * 3] = int32_t(*std::min_element(ln.begin(), ln.end()));
                abox[k * 3 + 1] = int32_t(*std::max_element(ln.begin(), ln.end()));
                abox[k * 3 + 2] = int32_t(*std::max_element(la.begin(), la.end()));
            }
        }
        for (size_t k = 0; k < A; k++) {
            arc_off[k] = static_cast<uint32_t>(av_bytes.size());
            if (av_bytes.size() + blocks[k].size() > UINT32_MAX) { std::cerr << "arc_vertices exceeds u32 offsets\n"; return 1; }
            av_bytes.insert(av_bytes.end(), blocks[k].begin(), blocks[k].end());
            stored_vertices += arc_n[k];
            bbox_arcs += (blocks[k][0] & 0x80) != 0;
            width_count[blocks[k][0] & 3]++;
            lvl_vertices[std::min<int>(arcs[perm[k]].minlvl, 15)] += arc_n[k];
            blocks[k].clear(); blocks[k].shrink_to_fit();
        }
        arc_off[A] = static_cast<uint32_t>(av_bytes.size());
    }
    // ring refs per polygon (slot order), polygon records
    std::vector<uint8_t> pa_bytes, pav_bytes;
    std::vector<uint32_t> pa_off(sel.size(), NO_DATA);
    std::vector<uint64_t> poly_keys;  // osm key, refs hash (content hashes of arcs, in ring order), per written record
    std::vector<uint64_t> poly_hash(sel.size(), 0);
    uint64_t n_refs = 0;
    for (size_t s = 0; s < sel.size(); s++) {
        if (poly_ring0[s] == poly_ring0[s + 1]) continue;
        pa_off[s] = static_cast<uint32_t>(pa_bytes.size());
        uint64_t prev = 0, h = 0x99;
        put_varint(pav_bytes, poly_ring0[s + 1] - poly_ring0[s]);
        for (uint32_t r = poly_ring0[s]; r < poly_ring0[s + 1]; r++) {
            put<uint32_t>(pa_bytes, static_cast<uint32_t>(refs[r].size()));
            put_varint(pav_bytes, refs[r].size());
            const bool chunked = sc.chunk && refs[r].size() > sc.chunk;
            for (size_t j = 0; j < refs[r].size(); j++) {
                const ArcRef& f = refs[r][j];
                if (chunked && j % sc.chunk == 0) {
                    // chunk bbox: the union of the next <= chunk arcs' (min lng, max lng, max lat)
                    int32_t a = INT32_MAX, b = INT32_MIN, c = INT32_MIN;
                    for (size_t t = j; t < std::min<size_t>(j + sc.chunk, refs[r].size()); t++) {
                        const uint32_t k = inv[refs[r][t].arc];
                        a = std::min(a, abox[k * 3]); b = std::max(b, abox[k * 3 + 1]); c = std::max(c, abox[k * 3 + 2]);
                    }
                    put<int32_t>(pa_bytes, a); put<int32_t>(pa_bytes, b); put<int32_t>(pa_bytes, c);
                }
                uint32_t id = inv[f.arc];
                put<uint32_t>(pa_bytes, (id << 1) | (f.rev ? 1 : 0));
                put_varint(pav_bytes, (zz(int64_t(id) - int64_t(prev)) << 1) | (f.rev ? 1 : 0));
                prev = id;
                h = mix(mix(h, arc_keys[size_t(id) * 3]), f.rev);
                n_refs++;
            }
            h = mix(h, 0xFFFF);
        }
        poly_hash[s] = h;
    }
    if (A >= (1u << 31)) { std::cerr << "too many arcs for 31-bit refs\n"; return 1; }
    const double t_encode = now() - t0;

    // ---- decode from the encoded bytes and write today's files (encoding 4)
    auto decode_arc = [&](uint32_t k, std::vector<int64_t>& la, std::vector<int64_t>& ln) {
        const uint8_t* B = av_bytes.data() + arc_off[k];
        const size_t sz = arc_off[k + 1] - arc_off[k];
        const int wc = B[0] & 3;
        const size_t w = size_t(1) << wc;
        const bool bb = B[0] & 0x80;
        const size_t hdr = 9 + (bb ? 16 : 0);
        const size_t m = (sz - hdr) / (2 * w);
        la.assign(m + 1, 0); ln.assign(m + 1, 0);
        la[0] = rd<int32_t>(B + 1); ln[0] = rd<int32_t>(B + 5);
        for (int c = 0; c < 2; c++) {
            auto& v = c ? ln : la;
            for (size_t i = 0; i < m; i++) {
                uint64_t z = 0;
                for (size_t b = 0; b < w; b++) z |= uint64_t(B[hdr + (c * w + b) * m + i]) << (8 * b);
                v[i + 1] = v[i] + unzz(z);
            }
        }
    };
    size_t rt_mismatch = 0;
    {
        // round trip check of every arc
#pragma omp parallel
        {
            std::vector<int64_t> la, ln, qa, qn;
#pragma omp for schedule(dynamic, 1024) reduction(+ : rt_mismatch)
            for (size_t k = 0; k < A; k++) {
                decode_arc(static_cast<uint32_t>(k), la, ln);
                arc_q(perm[k], qa, qn, nullptr);
                if (la != qa || ln != qn) rt_mismatch++;
            }
        }
    }
    std::vector<std::vector<uint8_t>> packed(sel.size());
    std::vector<float> parea(sel.size(), 0);
    std::vector<uint16_t> pholes(sel.size(), 0);
    std::vector<uint32_t> pcount(sel.size(), 0);
    size_t dec_dropped = 0;
#pragma omp parallel
    {
        std::vector<int64_t> la, ln;
#pragma omp for schedule(dynamic, 64) reduction(+ : dec_dropped)
        for (size_t s = 0; s < sel.size(); s++) {
            if (pa_off[s] == NO_DATA) { dec_dropped++; continue; }
            const uint8_t* p = pa_bytes.data() + pa_off[s];
            const uint32_t nr = poly_ring0[s + 1] - poly_ring0[s];
            std::vector<Pts> rs;
            for (uint32_t r = 0; r < nr; r++) {
                uint32_t cnt = rd<uint32_t>(p); p += 4;
                const bool chunked = sc.chunk && cnt > sc.chunk;
                Pts ring;
                for (uint32_t j = 0; j < cnt; j++) {
                    if (chunked && j % sc.chunk == 0) p += 12;  // chunk bbox
                    uint32_t ref = rd<uint32_t>(p); p += 4;
                    decode_arc(ref >> 1, la, ln);
                    size_t m = la.size();
                    for (size_t i = 0; i < m; i++) {
                        size_t k = (ref & 1) ? m - 1 - i : i;
                        std::pair<double, double> q{double(la[k]) * sc.q, double(ln[k]) * sc.q};
                        if (!ring.empty() && ring.back() == q) continue;
                        ring.push_back(q);
                    }
                }
                if (ring.size() >= 2 && ring.front() == ring.back()) ring.pop_back();
                if (ring.size() < 3) continue;
                ring.push_back(ring.front());
                rs.push_back(std::move(ring));
            }
            if (rs.empty()) { dec_dropped++; continue; }
            std::vector<Pts> holes(rs.begin() + 1, rs.end());
            auto [range, nh] = simplify_rings(rs[0], holes, [](const Pts& r) { return r; });
            if (range.size() < 3) { dec_dropped++; continue; }
            pack_f32(packed[s], range);
            parea[s] = polygon_area(range);
            pholes[s] = nh;
            pcount[s] = static_cast<uint32_t>(range.size());
        }
    }
    const double t_decode = now() - t0;

    // admin_polygons (arc format and decoded), postal subset, cells for minimal
    std::vector<AdminPolygon> arc_polys, dec_polys, arc_postal, dec_postal;
    std::vector<uint32_t> remap(n_all, NO_DATA), slot_src;
    std::ofstream dv(out + "/dec/admin_vertices.bin", std::ios::binary);
    std::vector<uint8_t> postal_v;
    size_t vb = 0;
    if (minimal) {
        for (size_t s = 0; s < sel.size(); s++) {
            if (packed[s].empty()) continue;
            AdminPolygon a = x.polys[sel[s]];
            a.vertex_offset = static_cast<uint32_t>(vb);
            a.vertex_count = pcount[s];
            a.area = parea[s];
            a.hole_count = pholes[s];
            remap[sel[s]] = static_cast<uint32_t>(dec_polys.size());
            dec_polys.push_back(a);
            slot_src.push_back(sel[s]);
            dv.write(reinterpret_cast<const char*>(packed[s].data()), packed[s].size());
            vb += packed[s].size();
            AdminPolygon b = a;
            b.vertex_offset = pa_off[s];
            b.vertex_count = 0;
            for (uint32_t r = poly_ring0[s]; r < poly_ring0[s + 1]; r++) b.vertex_count += refs[r].size();
            b.hole_count = static_cast<uint16_t>(poly_ring0[s + 1] - poly_ring0[s] - 1);
            arc_polys.push_back(b);
            poly_keys.push_back(x.osm[sel[s]]);
            poly_keys.push_back(poly_hash[s]);
        }
    } else {
        std::vector<int64_t> sel_of(n_all, -1);
        for (size_t s = 0; s < sel.size(); s++) sel_of[sel[s]] = static_cast<int64_t>(s);
        dec_polys.assign(n_all, admin_polygon_tombstone());
        arc_polys.assign(n_all, admin_polygon_tombstone());
        size_t pb = 0;
        for (size_t i = 0; i < n_all; i++) {
            int64_t s = sel_of[i];
            if (s < 0 || packed[s].empty()) continue;
            AdminPolygon a = x.polys[i];
            a.vertex_offset = static_cast<uint32_t>(vb);
            a.vertex_count = pcount[s];
            a.area = parea[s];
            a.hole_count = pholes[s];
            dec_polys[i] = a;
            dv.write(reinterpret_cast<const char*>(packed[s].data()), packed[s].size());
            vb += packed[s].size();
            AdminPolygon b = a;
            b.vertex_offset = pa_off[s];
            b.vertex_count = 0;
            for (uint32_t r = poly_ring0[s]; r < poly_ring0[s + 1]; r++) b.vertex_count += refs[r].size();
            b.hole_count = static_cast<uint16_t>(poly_ring0[s + 1] - poly_ring0[s] - 1);
            arc_polys[i] = b;
            poly_keys.push_back(x.osm[i]);
            poly_keys.push_back(poly_hash[s]);
            if (a.admin_level != 11) continue;
            AdminPolygon c = a;
            c.vertex_offset = static_cast<uint32_t>(pb);
            dec_postal.push_back(c);
            postal_v.insert(postal_v.end(), packed[s].begin(), packed[s].end());
            pb += packed[s].size();
            arc_postal.push_back(b);
        }
    }
    dv.close();
    if (vb > UINT32_MAX) { std::cerr << "decoded admin_vertices exceeds u32 offsets\n"; return 1; }
    spit(out + "/dec/admin_polygons.bin", dec_polys.data(), dec_polys.size() * sizeof(AdminPolygon));
    if (!minimal && !dec_postal.empty()) {
        spit(out + "/dec/postal_polygons.bin", dec_postal.data(), dec_postal.size() * sizeof(AdminPolygon));
        spit(out + "/dec/postal_vertices.bin", postal_v.data(), postal_v.size());
        spit(out + "/arc/postal_polygons.bin", arc_postal.data(), arc_postal.size() * sizeof(AdminPolygon));
    }
    spit(out + "/arc/admin_polygons.bin", arc_polys.data(), arc_polys.size() * sizeof(AdminPolygon));
    spit(out + "/arc/arc_vertices.bin", av_bytes.data(), av_bytes.size());
    spit(out + "/arc/arc_index.bin", arc_off.data(), arc_off.size() * 4);
    spit(out + "/arc/poly_arcs.bin", pa_bytes.data(), pa_bytes.size());
    spit(out + "/arc/poly_arcs_v.bin", pav_bytes.data(), pav_bytes.size());
    spit(out + "/arc/arc_keys.bin", arc_keys.data(), arc_keys.size() * 8);
    spit(out + "/arc/poly_keys.bin", poly_keys.data(), poly_keys.size() * 8);
    {
        // grid quantum for readers
        std::ofstream m(out + "/arc/arc_meta.json");
        m << "{\"q\":" << sc.q << ",\"arcs\":" << A << ",\"bbox_min_n\":" << sc.bbox_min_n << ",\"chunk\":" << sc.chunk << "}\n";
    }
    if (minimal) {
        // parents in minimal slots (closest drawn ancestor), as tiervar minimal
        std::vector<uint32_t> par(dec_polys.size(), NO_DATA);
        for (size_t s = 0; s < dec_polys.size(); s++) {
            uint32_t c = x.parents.size() > slot_src[s] ? x.parents[slot_src[s]] : NO_DATA;
            for (int guard = 0; guard < 32 && c != NO_DATA && c < n_all; guard++) {
                if (remap[c] != NO_DATA) { par[s] = remap[c]; break; }
                c = x.parents[c];
            }
        }
        spit(out + "/dec/admin_parents.bin", par.data(), par.size() * 4);
        std::vector<uint8_t> rows = slurp<uint8_t>(admin_dir + "/admin_cells.bin");
        std::vector<uint8_t> ent = slurp<uint8_t>(admin_dir + "/admin_entries.bin");
        const size_t RR = 12, cells = rows.size() / RR;
        std::vector<uint8_t> orow, oent;
        std::vector<uint32_t> ids;
        for (size_t c = 0; c < cells; c++) {
            uint64_t id; uint32_t off;
            std::memcpy(&id, &rows[c * RR], 8);
            std::memcpy(&off, &rows[c * RR + 8], 4);
            uint16_t cnt;
            std::memcpy(&cnt, &ent[off], 2);
            ids.clear();
            for (uint16_t j = 0; j < cnt; j++) {
                uint32_t v;
                std::memcpy(&v, &ent[off + 2 + 4 * size_t(j)], 4);
                uint32_t pid = v & ID_MASK;
                if (pid >= n_all || remap[pid] == NO_DATA) continue;
                ids.push_back(remap[pid] | (v & INTERIOR_FLAG));
            }
            if (ids.empty()) continue;
            std::sort(ids.begin(), ids.end());
            uint32_t o = static_cast<uint32_t>(oent.size());
            uint16_t k = static_cast<uint16_t>(ids.size());
            size_t r0 = orow.size();
            orow.resize(r0 + RR);
            std::memcpy(&orow[r0], &id, 8);
            std::memcpy(&orow[r0 + 8], &o, 4);
            size_t e0 = oent.size();
            oent.resize(e0 + 2 + 4 * ids.size());
            std::memcpy(&oent[e0], &k, 2);
            std::memcpy(&oent[e0 + 2], ids.data(), 4 * ids.size());
        }
        spit(out + "/dec/admin_cells.bin", orow.data(), orow.size());
        spit(out + "/dec/admin_entries.bin", oent.data(), oent.size());
    }
    const double t_end = now() - t0;

    // ---- stats
    uint64_t lvl_rings[16] = {0};
    for (auto& r : rings) lvl_rings[std::min<int>(r.level, 15)]++;
    uint64_t users_hist[6] = {0};
    uint64_t raw_arc_vertices = arc_v0[A];
    uint64_t multi_level_arcs = 0;
    for (auto& a : arcs) { users_hist[std::min<uint32_t>(a.users, 5)]++; multi_level_arcs += a.minlvl != a.maxlvl; }
    uint64_t sum_ring_occ = N;
    std::printf("{\"mode\":\"%s\",\"scheme\":\"%s\",\"q\":%g,\"polygons\":%zu,\"rings\":%zu,\"fallback_polygons\":%zu,"
                "\"dropped_rings\":%zu,\"occurrences\":%llu,\"vertices\":%zu,\"junctions\":%zu,\"arcs\":%zu,"
                "\"arc_raw_vertices\":%llu,\"shared_arcs\":%zu,\"multi_level_arcs\":%llu,\"way_disagree_arcs\":%zu,"
                "\"hash_collisions\":%zu,\"users_hist\":[%llu,%llu,%llu,%llu,%llu],"
                "\"stored_vertices\":%llu,\"refs\":%llu,\"bbox_arcs\":%llu,\"width\":[%llu,%llu,%llu],"
                "\"ring_fix_rounds\":%zu,\"ring_fix_added\":%zu,\"rings_degenerate\":%zu,"
                "\"crossings_before\":%zu,\"crossings_after\":%zu,\"crossings_in_raw_data\":[%zu,%zu],\"repair_rounds\":%zu,\"repair_added\":%zu,\"unrepairable_segments\":%zu,"
                "\"roundtrip_mismatch\":%zu,\"dec_dropped\":%zu,"
                "\"bytes\":{\"arc_vertices\":%zu,\"arc_index\":%zu,\"poly_arcs\":%zu,\"poly_arcs_v\":%zu,\"admin_polygons\":%zu,\"postal_polygons\":%zu},"
                "\"stored_by_finest_level\":{",
                mode.c_str(), sc.text.c_str(), sc.q, sel.size(), R, fallback, dropped_rings, (unsigned long long)sum_ring_occ, V,
                n_junction, A, (unsigned long long)raw_arc_vertices, shared_arcs, (unsigned long long)multi_level_arcs,
                way_disagree_arcs, collisions, (unsigned long long)users_hist[1], (unsigned long long)users_hist[2],
                (unsigned long long)users_hist[3], (unsigned long long)users_hist[4], (unsigned long long)users_hist[5],
                (unsigned long long)stored_vertices, (unsigned long long)n_refs, (unsigned long long)bbox_arcs,
                (unsigned long long)width_count[0], (unsigned long long)width_count[1], (unsigned long long)width_count[2],
                ring_fix_rounds, ring_fix_added, rings_degenerate, cross0, cross_last, cross_real0, cross_real_last, repair_rounds, repair_added, unrepairable,
                rt_mismatch, dec_dropped, av_bytes.size(), arc_off.size() * 4, pa_bytes.size(), pav_bytes.size(),
                arc_polys.size() * sizeof(AdminPolygon), arc_postal.size() * sizeof(AdminPolygon));
    bool first = true;
    for (int l = 0; l < 16; l++) {
        if (!lvl_vertices[l]) continue;
        std::printf("%s\"%d\":%llu", first ? "" : ",", l, (unsigned long long)lvl_vertices[l]);
        first = false;
    }
    std::printf("},\"rings_by_level\":{");
    first = true;
    for (int l = 0; l < 16; l++) {
        if (!lvl_rings[l]) continue;
        std::printf("%s\"%d\":%llu", first ? "" : ",", l, (unsigned long long)lvl_rings[l]);
        first = false;
    }
    std::printf("},\"threads\":%d,\"seconds\":{\"load\":%.1f,\"vids_junctions\":%.1f,\"arcs\":%.1f,\"simplify\":%.1f,"
                "\"ring_validity\":%.1f,\"repair\":%.1f,\"encode\":%.1f,\"decode_write\":%.1f,\"total\":%.1f,\"read_inputs\":%.1f}}\n",
                omp_get_max_threads(), t_load, t_vid - t_load, t_arcs - t_vid, t_simpl - t_arcs, t_valid - t_simpl,
                t_repair - t_valid, t_encode - t_repair, t_decode - t_encode, t_end, tl);
    return 0;
}
