// Admin boundary tier variants re-simplified from the builder's own final
// planet admin arrays (GC_TIER_DUMP of the tiers2-dump branch: fix/admin-holes
// 97c732c + a dump hook), with the builder's own code paths: split_rings /
// simplify_rings (polygon_rings.h), simplify_polygon_epsilon / meters_to_degrees
// / polygon_area (geometry.h), plan_polygon / pack_polygon (vertex_pack.h),
// write_cell_index's file layout (cell_index_io.h). pack() below is
// cell_index.cpp pack_admin_polygon with the tolerance made a parameter.
//
//   tiervar quality <dump> <out> <scheme>
//       = write_quality_variant: admin_polygons.bin (slot-aligned with the
//         dump), admin_vertices.bin, postal_polygons.bin, postal_vertices.bin
//   tiervar minimal <dump> <admin_mode_dir> <out> <scheme>
//       = write_admin_minimal_polygons (cold build: slot = order of the drawn
//         level 2-8 polygons) + its cell index, rebuilt from the admin mode's
//         admin_cells/admin_entries (= data.cell_to_admin) through the remap,
//         + admin_parents.bin (the dump's parent chain in minimal slots,
//         skipping ancestors minimal doesn't draw; for the chain reader)
//   Prints one JSON line of stats (per level rings / vertices / bytes).
//
// <scheme>: ';'-separated key=value, applied per ring (outer and each hole):
//   none          no simplification (= quality/uncapped)
//   scale=s       level tolerance kAdminEpsilonMeters[level] * s  (q2.5: s=2.5)
//   uni=m         every level m metres
//   t<L>=m        level L tolerance m metres (after scale/uni)
//   max=m         every level's tolerance at most m metres
//   min=m         every level's tolerance at least m metres
//   rel=f,lo,hi   ignore the level: tol = clamp(f * sqrt(ring area in m^2), lo, hi)
//   cap=f         degrees tolerance at most f * sqrt(polygon_area(ring))
//                 (9b55d28 ring_tolerance_degrees with kRingToleranceFraction = f)
//   capmin=m      ... but never below m metres (cap floor)
//   grid=g        snap every simplified ring to the global g-degree grid,
//                 drop consecutive duplicates; a ring left < 4 vertices,
//                 open, or with zero area keeps its unsnapped vertices
//   u32=1         pack every polygon as encoding 3 (u32 @ 1e-7)
//   levels=a-b    only polygons of admin_level a..b get the scheme; others
//                 take scale=2.5 (only meaningful for quality)
#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <vector>
#include <omp.h>

#include "types.h"
#include "geometry.h"
#include "polygon_rings.h"
#include "vertex_pack.h"

using Pts = std::vector<std::pair<double, double>>;

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

struct Scheme {
    bool none = false;
    double level_m[16];
    bool rel = false; double rel_f = 0, rel_lo = 0, rel_hi = 1e18;
    double cap = 0, capmin = 0;
    double grid = 0;
    bool u32 = false;
    int lv_lo = 0, lv_hi = 255;
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
        else if (k == "rel") { sc.rel = true; if (std::sscanf(v.c_str(), "%lf,%lf,%lf", &sc.rel_f, &sc.rel_lo, &sc.rel_hi) != 3) { std::cerr << "rel=f,lo,hi\n"; std::exit(1); } }
        else if (k == "cap") sc.cap = std::atof(v.c_str());
        else if (k == "capmin") sc.capmin = std::atof(v.c_str());
        else if (k == "grid") sc.grid = std::atof(v.c_str());
        else if (k == "u32") sc.u32 = std::atoi(v.c_str()) != 0;
        else if (k == "levels") { if (std::sscanf(v.c_str(), "%d-%d", &sc.lv_lo, &sc.lv_hi) != 2) { std::cerr << "levels=a-b\n"; std::exit(1); } }
        else { std::cerr << "unknown scheme key " << k << "\n"; std::exit(1); }
    }
    for (auto& [l, m] : over) if (l >= 0 && l < 16) sc.level_m[l] = m;
    for (int l = 0; l < 16; l++) sc.level_m[l] = std::max(mn, std::min(mx, sc.level_m[l]));
    return sc;
}

// Ring area in square metres (shoelace on a local equirectangular projection).
static double ring_area_m2(const Pts& r) {
    if (r.size() < 3) return 0;
    double lat0 = r[0].first, k = std::cos(lat0 * M_PI / 180.0);
    double a = 0;
    for (size_t i = 0, j = r.size() - 1; i < r.size(); j = i++)
        a += (r[j].second * k) * r[i].first - (r[i].second * k) * r[j].first;
    return std::fabs(a) / 2 * 111320.0 * 111320.0;
}

struct Packed {
    std::vector<uint8_t> bytes;
    uint32_t count = 0;
    float area = 0;
    uint16_t holes = 0;
    uint32_t holes_in = 0, snap_kept = 0;
};

static Pts snap(const Pts& r, double g, uint32_t& kept) {
    Pts s;
    s.reserve(r.size());
    for (const auto& p : r) {
        std::pair<double, double> q{std::round(p.first / g) * g, std::round(p.second / g) * g};
        if (!s.empty() && s.back() == q) continue;
        s.push_back(q);
    }
    if (s.size() < kMinClosedRing || s.front() != s.back() || polygon_area(s) <= 0) { kept++; return r; }
    return s;
}

// cell_index.cpp pack_admin_polygon with the tolerance from `sc`.
static Packed pack(const AdminPolygon& ap, const NodeCoord* all, const Scheme& sc_in, const Scheme& q25) {
    Packed out;
    if (ap.vertex_count < 3) return out;
    const Scheme& sc = (ap.admin_level >= sc_in.lv_lo && ap.admin_level <= sc_in.lv_hi) ? sc_in : q25;
    const NodeCoord* verts = all + ap.vertex_offset;
    auto points = [&](const RingSpan& r) {
        Pts pts;
        pts.reserve(r.count);
        for (uint32_t j = 0; j < r.count; j++) pts.emplace_back(verts[r.begin + j].lat, verts[r.begin + j].lng);
        return pts;
    };
    std::vector<RingSpan> rings;
    const bool parsed = split_rings(verts, ap.vertex_count, ap.hole_count, rings);
    Pts pts;
    uint16_t holes = parsed ? ap.hole_count : 0;
    out.holes_in = holes;
    if (!sc.none) {
        double eps_m = sc.level_m[ap.admin_level < 16 ? ap.admin_level : 15];
        std::vector<Pts> hole_rings;
        for (size_t r = 1; r < rings.size(); r++) hole_rings.push_back(points(rings[r]));
        std::tie(pts, holes) = simplify_rings(points(rings[0]), hole_rings, [&](const Pts& ring) {
            double lat = ring.empty() ? 0.0 : ring[0].first;
            double m = eps_m;
            if (sc.rel) m = std::max(sc.rel_lo, std::min(sc.rel_hi, sc.rel_f * std::sqrt(ring_area_m2(ring))));
            double deg = meters_to_degrees(m, lat);
            if (sc.cap > 0) {
                double c = sc.cap * std::sqrt(polygon_area(ring));
                if (sc.capmin > 0) c = std::max(c, meters_to_degrees(sc.capmin, lat));
                deg = std::min(deg, c);
            }
            Pts s = simplify_polygon_epsilon(ring, deg);
            if (sc.grid > 0) s = snap(s, sc.grid, out.snap_kept);
            return s;
        });
    } else {
        pts = points(RingSpan{0, ap.vertex_count});
    }
    if (pts.size() < 3) return out;
    PolygonPacking pk = plan_polygon(pts.data(), pts.size());
    if (sc.u32) {
        pk.enc = VertexEncoding::U32_1CM;
        pk.scale = 1e-7;
        pk.bytes = POLY_HEADER_BYTES + pts.size() * VERTEX_STRIDE[3];
    }
    out.bytes.resize(pk.bytes);
    pack_polygon(out.bytes.data(), pk, pts.data(), pts.size());
    out.count = static_cast<uint32_t>(pts.size());
    out.area = polygon_area(pts);
    out.holes = holes;
    return out;
}

struct Dump {
    std::vector<AdminPolygon> polys;
    std::vector<NodeCoord> verts;
    std::vector<uint32_t> parents;
};
static Dump load_dump(const std::string& d) {
    Dump x;
    x.polys = slurp<AdminPolygon>(d + "/admin_polygons.raw");
    x.verts = slurp<NodeCoord>(d + "/admin_vertices.raw");
    x.parents = slurp<uint32_t>(d + "/admin_parent_ids.raw");
    return x;
}

static std::vector<Packed> pack_all(const Dump& x, const std::vector<uint32_t>& idx, const Scheme& sc) {
    Scheme q25 = parse("scale=2.5");
    std::vector<Packed> out(idx.size());
#pragma omp parallel for schedule(dynamic, 1)
    for (size_t k = 0; k < idx.size(); k++) out[k] = pack(x.polys[idx[k]], x.verts.data(), sc, q25);
    return out;
}

static void stats(const char* mode, const Scheme& sc, const Dump& x, const std::vector<uint32_t>& idx,
                  const std::vector<Packed>& p, size_t vbytes, size_t slots) {
    uint64_t rings[16] = {0}, verts[16] = {0}, bytes[16] = {0}, holes_in = 0, holes_out = 0, snap_kept = 0, dropped = 0;
    uint64_t enc_v[4] = {0};
    for (size_t k = 0; k < idx.size(); k++) {
        int l = std::min<int>(x.polys[idx[k]].admin_level, 15);
        holes_in += p[k].holes_in;
        snap_kept += p[k].snap_kept;
        if (p[k].bytes.empty()) { dropped += x.polys[idx[k]].vertex_count >= 3; continue; }
        rings[l]++; verts[l] += p[k].count; bytes[l] += p[k].bytes.size(); holes_out += p[k].holes;
        enc_v[p[k].bytes[0]] += p[k].count;
    }
    std::printf("{\"mode\":\"%s\",\"scheme\":\"%s\",\"slots\":%zu,\"vertex_bytes\":%zu,\"holes_in\":%llu,\"holes_out\":%llu,"
                "\"dropped\":%llu,\"snap_kept_unsnapped\":%llu,\"enc_vertices\":[%llu,%llu,%llu,%llu],\"levels\":{",
                mode, sc.text.c_str(), slots, vbytes, (unsigned long long)holes_in, (unsigned long long)holes_out,
                (unsigned long long)dropped, (unsigned long long)snap_kept, (unsigned long long)enc_v[0],
                (unsigned long long)enc_v[1], (unsigned long long)enc_v[2], (unsigned long long)enc_v[3]);
    bool first = true;
    for (int l = 0; l < 16; l++) {
        if (!rings[l]) continue;
        std::printf("%s\"%d\":[%llu,%llu,%llu]", first ? "" : ",", l, (unsigned long long)rings[l],
                    (unsigned long long)verts[l], (unsigned long long)bytes[l]);
        first = false;
    }
    std::printf("}}\n");
}

static int run_quality(const std::string& dump, const std::string& out, const Scheme& sc) {
    Dump x = load_dump(dump);
    const size_t n = x.polys.size();
    std::vector<uint32_t> idx(n);
    for (size_t i = 0; i < n; i++) idx[i] = static_cast<uint32_t>(i);
    std::vector<Packed> p = pack_all(x, idx, sc);
    std::vector<AdminPolygon> np(n, admin_polygon_tombstone()), postal;
    std::vector<uint32_t> postal_idx;
    size_t vb = 0, pb = 0;
    for (size_t i = 0; i < n; i++) {
        if (p[i].bytes.empty()) continue;
        AdminPolygon a = x.polys[i];
        a.vertex_offset = static_cast<uint32_t>(vb);
        a.vertex_count = p[i].count;
        a.area = p[i].area;
        a.hole_count = p[i].holes;
        np[i] = a;
        vb += p[i].bytes.size();
        if (a.admin_level != 11) continue;
        a.vertex_offset = static_cast<uint32_t>(pb);
        postal.push_back(a);
        postal_idx.push_back(static_cast<uint32_t>(i));
        pb += p[i].bytes.size();
    }
    if (vb > UINT32_MAX) { std::cerr << "admin_vertices exceeds u32 offsets\n"; return 1; }
    spit(out + "/admin_polygons.bin", np.data(), np.size() * sizeof(AdminPolygon));
    {
        std::ofstream f(out + "/admin_vertices.bin", std::ios::binary);
        for (size_t i = 0; i < n; i++) f.write(reinterpret_cast<const char*>(p[i].bytes.data()), static_cast<std::streamsize>(p[i].bytes.size()));
        f.flush();
        if (!f) { std::cerr << "write failed\n"; return 1; }
    }
    if (!postal.empty()) {
        spit(out + "/postal_polygons.bin", postal.data(), postal.size() * sizeof(AdminPolygon));
        std::ofstream f(out + "/postal_vertices.bin", std::ios::binary);
        for (uint32_t i : postal_idx) f.write(reinterpret_cast<const char*>(p[i].bytes.data()), static_cast<std::streamsize>(p[i].bytes.size()));
    }
    stats("quality", sc, x, idx, p, vb, n);
    return 0;
}

static int run_minimal(const std::string& dump, const std::string& admin_dir, const std::string& out, const Scheme& sc) {
    Dump x = load_dump(dump);
    const size_t n = x.polys.size();
    std::vector<uint32_t> kept;
    for (size_t i = 0; i < n; i++)
        if (x.polys[i].admin_level >= 2 && x.polys[i].admin_level <= 8) kept.push_back(static_cast<uint32_t>(i));
    std::vector<Packed> p = pack_all(x, kept, sc);
    std::vector<uint32_t> remap(n, NO_DATA);
    std::vector<AdminPolygon> np;
    std::vector<uint32_t> slot_src;  // slot -> full id
    size_t vb = 0;
    for (size_t k = 0; k < kept.size(); k++) {
        if (p[k].bytes.empty()) continue;
        AdminPolygon a = x.polys[kept[k]];
        a.vertex_offset = static_cast<uint32_t>(vb);
        a.vertex_count = p[k].count;
        a.area = p[k].area;
        a.hole_count = p[k].holes;
        remap[kept[k]] = static_cast<uint32_t>(np.size());
        np.push_back(a);
        slot_src.push_back(kept[k]);
        vb += p[k].bytes.size();
    }
    spit(out + "/admin_polygons.bin", np.data(), np.size() * sizeof(AdminPolygon));
    {
        std::ofstream f(out + "/admin_vertices.bin", std::ios::binary);
        for (size_t k = 0; k < kept.size(); k++) f.write(reinterpret_cast<const char*>(p[k].bytes.data()), static_cast<std::streamsize>(p[k].bytes.size()));
        f.flush();
        if (!f) { std::cerr << "write failed\n"; return 1; }
    }
    // Parent chain in minimal slots: the closest drawn ancestor. With
    // TIERS2_NEST=<nestcheck flags>, a polygon whose chain doesn't nest all the
    // way up (flag bit1 clear) gets no parent, so the chain reader falls back
    // to point-in-polygon for it.
    {
        std::vector<uint8_t> nest;
        if (const char* nf = std::getenv("TIERS2_NEST")) nest = slurp<uint8_t>(nf);
        std::vector<uint32_t> par(np.size(), NO_DATA);
        for (size_t s = 0; s < np.size(); s++) {
            if (!nest.empty() && !(nest[slot_src[s]] & 2)) continue;
            uint32_t c = x.parents.size() > slot_src[s] ? x.parents[slot_src[s]] : NO_DATA;
            for (int guard = 0; guard < 32 && c != NO_DATA && c < n; guard++) {
                if (remap[c] != NO_DATA) { par[s] = remap[c]; break; }
                c = x.parents[c];
            }
        }
        spit(out + "/admin_parents.bin", par.data(), par.size() * sizeof(uint32_t));
    }
    // Cell index: the admin mode's (= data.cell_to_admin) through the remap.
    {
        std::vector<uint8_t> rows = slurp<uint8_t>(admin_dir + "/admin_cells.bin");
        std::vector<uint8_t> ent = slurp<uint8_t>(admin_dir + "/admin_entries.bin");
        const size_t R = 12, cells = rows.size() / R;
        std::vector<uint8_t> orow, oent;
        orow.reserve(rows.size());
        oent.reserve(ent.size());
        std::vector<uint32_t> ids;
        for (size_t c = 0; c < cells; c++) {
            uint64_t id; uint32_t off;
            std::memcpy(&id, &rows[c * R], 8);
            std::memcpy(&off, &rows[c * R + 8], 4);
            uint16_t cnt;
            std::memcpy(&cnt, &ent[off], 2);
            ids.clear();
            for (uint16_t j = 0; j < cnt; j++) {
                uint32_t v;
                std::memcpy(&v, &ent[off + 2 + 4 * size_t(j)], 4);
                uint32_t pid = v & ID_MASK;
                if (pid >= n || remap[pid] == NO_DATA) continue;
                ids.push_back(remap[pid] | (v & INTERIOR_FLAG));
            }
            if (ids.empty()) continue;
            std::sort(ids.begin(), ids.end());
            uint32_t o = static_cast<uint32_t>(oent.size());
            uint16_t k = static_cast<uint16_t>(ids.size());
            size_t r0 = orow.size();
            orow.resize(r0 + R);
            std::memcpy(&orow[r0], &id, 8);
            std::memcpy(&orow[r0 + 8], &o, 4);
            size_t e0 = oent.size();
            oent.resize(e0 + 2 + 4 * ids.size());
            std::memcpy(&oent[e0], &k, 2);
            std::memcpy(&oent[e0 + 2], ids.data(), 4 * ids.size());
        }
        spit(out + "/admin_cells.bin", orow.data(), orow.size());
        spit(out + "/admin_entries.bin", oent.data(), oent.size());
    }
    stats("minimal", sc, x, kept, p, vb, np.size());
    return 0;
}

// The full set's admin_parents.bin with no parent where the nestcheck flags
// say the chain doesn't nest (bit1 clear).
static int run_parents(const std::string& dump, const std::string& flags, const std::string& out) {
    auto par = slurp<uint32_t>(dump + "/admin_parent_ids.raw");
    auto nest = slurp<uint8_t>(flags);
    if (nest.size() != par.size()) { std::cerr << "flags/parents size mismatch\n"; return 1; }
    size_t cut = 0;
    for (size_t i = 0; i < par.size(); i++)
        if (!(nest[i] & 2) && par[i] != NO_DATA) { par[i] = NO_DATA; cut++; }
    spit(out, par.data(), par.size() * sizeof(uint32_t));
    std::printf("{\"mode\":\"parents\",\"cut\":%zu}\n", cut);
    return 0;
}

int main(int argc, char** argv) {
    std::string mode = argc > 1 ? argv[1] : "";
    if (mode == "parents" && argc == 5) return run_parents(argv[2], argv[3], argv[4]);
    if (mode == "quality" && argc == 5) return run_quality(argv[2], argv[3], parse(argv[4]));
    if (mode == "minimal" && argc == 6) return run_minimal(argv[2], argv[3], argv[4], parse(argv[5]));
    std::cerr << "usage: tiervar quality <dump> <out> <scheme> | tiervar minimal <dump> <admin_dir> <out> <scheme>\n";
    return 1;
}
