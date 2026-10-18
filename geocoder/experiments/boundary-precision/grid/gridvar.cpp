// Admin boundary tier variants written in TODAY's on-disk format, so the real
// reader (grid_query) answers queries against them. Uses the builder's own
// simplify_polygon_epsilon / meters_to_degrees / polygon_area / plan_polygon /
// pack_polygon headers.
//
//   gridvar var    <src_dir> <out_dir> <dp_metres> <grid_deg> [legacy]
//   gridvar filter <src_dir> <out_dir> <min_level> <max_level>
//
// var: every live ring is decoded exactly as the reader does (f32 results).
//   dp_metres > 0  re-simplify the ring with simplify_polygon_epsilon at a
//                  uniform tolerance (builder: meters_to_degrees at the ring's
//                  first vertex), whatever its admin level.
//   grid_deg > 0   snap every vertex to the global grid round(v / G) * G and
//                  drop consecutive duplicates. Only rings whose storage grid
//                  (plan_polygon: 1e-6 / 1e-5 / 1e-4 / 1e-7 by bbox span) is
//                  finer than G are snapped; a ring already stored on a grid
//                  >= G keeps its bytes. A ring left with < 3 distinct
//                  vertices or area <= 0 after the snap keeps its pre-snap
//                  geometry (the reader skips area <= 0 rings). Closed rings
//                  stay closed: first and last vertex snap to the same point.
//   legacy         snap every ring, keep duplicates, no collapse fallback:
//                  must reproduce /ssd/shrink/vertices/variant.cpp "snap"
//                  byte for byte (regression check of this tool).
//   Unchanged rings keep their original bytes and area; changed ones are
//   repacked like the builder (area from the new double coordinates).
//   Prints one JSON line of stats.
// filter: keep only rings with min_level <= admin_level <= max_level, bytes
//   copied verbatim and laid back to back; other slots get vertex_count 0 and
//   vertex_offset NO_DATA (slot count unchanged).
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
#include "geometry.h"
#include "vertex_pack.h"

using Pts = std::vector<std::pair<double, double>>;

static std::vector<uint8_t> slurp(const std::string& p) {
    std::ifstream f(p, std::ios::binary | std::ios::ate);
    if (!f) { std::cerr << "cannot open " << p << "\n"; std::exit(1); }
    std::vector<uint8_t> b(static_cast<size_t>(f.tellg()));
    f.seekg(0); f.read(reinterpret_cast<char*>(b.data()), b.size());
    return b;
}

static const double SCALE[4] = {1e-5, 1e-4, 1e-6, 1e-7};

static size_t block_bytes(const uint8_t* h, uint32_t cnt) { return POLY_HEADER_BYTES + size_t(cnt) * VERTEX_STRIDE[h[0]]; }

// Exactly the reader's decode (server/src/lib.rs decode_polygon_verts): f32(f64(bbox_min_f32) + q * scale).
static Pts decode(const uint8_t* h, uint32_t cnt) {
    uint8_t enc = h[0];
    if (enc > 3) { std::cerr << "unexpected encoding " << int(enc) << "\n"; std::exit(1); }
    float bml, bmg; std::memcpy(&bml, h + 2, 4); std::memcpy(&bmg, h + 6, 4);
    size_t stride = VERTEX_STRIDE[enc];
    Pts pts(cnt);
    for (uint32_t k = 0; k < cnt; k++) {
        const uint8_t* q = h + POLY_HEADER_BYTES + k * stride;
        double dl, dg;
        if (enc == 3) { uint32_t a, b; std::memcpy(&a, q, 4); std::memcpy(&b, q + 4, 4); dl = a; dg = b; }
        else { uint16_t a, b; std::memcpy(&a, q, 2); std::memcpy(&b, q + 2, 2); dl = a; dg = b; }
        pts[k] = {static_cast<float>(static_cast<double>(bml) + dl * SCALE[enc]),
                  static_cast<float>(static_cast<double>(bmg) + dg * SCALE[enc])};
    }
    return pts;
}

static double metres(const std::pair<double, double>& a, const std::pair<double, double>& b) {
    double dy = (a.first - b.first) * 111320.0;
    double dx = (a.second - b.second) * 111320.0 * std::cos(a.first * M_PI / 180.0);
    return std::hypot(dx, dy);
}

struct Res {
    std::vector<uint8_t> bytes;  // empty = copy source bytes
    uint32_t count = 0;
    float area = 0;
    uint8_t status = 0;          // 0 dead, 1 untouched, 2 changed, 3 snap collapsed -> pre-snap kept
    uint64_t v_in = 0, v_dp = 0, v_out = 0, dups = 0;
    double max_disp = 0, max_dlat = 0, max_dlng = 0;
    bool snapped = false, open_in = false, open_out = false, storage_jump = false;
};

static int run_filter(const std::string& src, const std::string& out, int lo, int hi) {
    auto pb = slurp(src + "/admin_polygons.bin");
    auto vb = slurp(src + "/admin_vertices.bin");
    size_t n = pb.size() / sizeof(AdminPolygon);
    std::vector<AdminPolygon> polys(n);
    std::memcpy(polys.data(), pb.data(), pb.size());
    std::vector<uint8_t> outv;
    size_t kept = 0; uint64_t verts = 0;
    for (auto& p : polys) {
        if (p.vertex_count == 0 || p.vertex_offset == NO_DATA) continue;
        if (p.admin_level < lo || p.admin_level > hi) { p.vertex_count = 0; p.vertex_offset = NO_DATA; continue; }
        const uint8_t* h = vb.data() + p.vertex_offset;
        size_t bytes = block_bytes(h, p.vertex_count);
        uint32_t off = static_cast<uint32_t>(outv.size());
        outv.insert(outv.end(), h, h + bytes);
        p.vertex_offset = off;
        kept++; verts += p.vertex_count;
    }
    std::ofstream(out + "/admin_polygons.bin", std::ios::binary).write(reinterpret_cast<const char*>(polys.data()), n * sizeof(AdminPolygon));
    std::ofstream(out + "/admin_vertices.bin", std::ios::binary).write(reinterpret_cast<const char*>(outv.data()), outv.size());
    std::printf("{\"mode\":\"filter\",\"levels\":[%d,%d],\"rings\":%zu,\"vertices\":%llu,\"bytes\":%zu}\n", lo, hi, kept,
                (unsigned long long)verts, outv.size());
    return 0;
}

int main(int argc, char** argv) {
    if (argc >= 6 && std::string(argv[1]) == "filter") return run_filter(argv[2], argv[3], std::atoi(argv[4]), std::atoi(argv[5]));
    if (argc < 6 || std::string(argv[1]) != "var") {
        std::cerr << "usage: gridvar var <src> <out> <dp_metres> <grid_deg> [legacy] | gridvar filter <src> <out> <lo> <hi>\n";
        return 1;
    }
    std::string src = argv[2], out = argv[3];
    double dp_m = std::atof(argv[4]), G = std::atof(argv[5]);
    bool legacy = argc > 6 && std::string(argv[6]) == "legacy";
    auto pb = slurp(src + "/admin_polygons.bin");
    auto vb = slurp(src + "/admin_vertices.bin");
    size_t n = pb.size() / sizeof(AdminPolygon);
    std::vector<AdminPolygon> polys(n);
    std::memcpy(polys.data(), pb.data(), pb.size());
    std::vector<Res> res(n);

#pragma omp parallel for schedule(dynamic, 16)
    for (size_t i = 0; i < n; i++) {
        const AdminPolygon& p = polys[i];
        Res& r = res[i];
        if (p.vertex_count == 0 || p.vertex_offset == NO_DATA) continue;
        const uint8_t* h = vb.data() + p.vertex_offset;
        Pts pts = decode(h, p.vertex_count);
        r.v_in = pts.size();
        r.open_in = pts.front() != pts.back();
        Pts work = pts;
        bool changed = false;
        if (dp_m > 0 && pts.size() >= 3) {
            work = simplify_polygon_epsilon(work, meters_to_degrees(dp_m, work[0].first));
            changed = work.size() != pts.size();
        }
        r.v_dp = work.size();
        Pts snapped;
        std::vector<uint32_t> into;  // pre-snap vertex -> snapped vertex index
        if (G > 0 && work.size() >= 3) {
            PolygonPacking pk = plan_polygon(work.data(), work.size());
            if (legacy || pk.scale < G * (1 - 1e-9)) {
                snapped.reserve(work.size()); into.resize(work.size());
                long long la = 0, lg = 0;
                for (size_t k = 0; k < work.size(); k++) {
                    long long a = std::llround(work[k].first / G), b = std::llround(work[k].second / G);
                    if (!legacy && !snapped.empty() && a == la && b == lg) { r.dups++; into[k] = snapped.size() - 1; continue; }
                    into[k] = snapped.size();
                    snapped.emplace_back(static_cast<double>(a) * G, static_cast<double>(b) * G);
                    la = a; lg = b;
                }
                size_t distinct = snapped.size() - (snapped.size() > 1 && snapped.front() == snapped.back() ? 1 : 0);
                // A snap can grow the bbox span past a plan_polygon threshold; if the new
                // storage grid is coarser than G (only possible for G = 5e-5 -> 1e-4) the
                // packer would re-quantize the snapped vertices, so keep the pre-snap ring.
                bool coarser = !legacy && distinct >= 3 &&
                               plan_polygon(snapped.data(), snapped.size()).scale > G * (1 + 1e-9);
                if (coarser) r.storage_jump = true;
                if (!legacy && (distinct < 3 || coarser || polygon_area(snapped) <= 0)) {
                    r.status = 3; r.dups = 0; snapped.clear();
                } else {
                    r.snapped = true;
                }
            }
        }
        if (r.status != 3 && !r.snapped && !changed) {
            r.status = 1; r.count = p.vertex_count; r.area = p.area; r.v_out = p.vertex_count;
            r.open_out = r.open_in;
            continue;
        }
        Pts& fin = r.snapped ? snapped : work;
        if (r.status != 3) r.status = 2;
        if (r.status == 3 && !changed) {  // collapsed under the snap and no DP: original bytes
            r.count = p.vertex_count; r.area = p.area; r.v_out = p.vertex_count; r.open_out = r.open_in;
            continue;
        }
        PolygonPacking pk = plan_polygon(fin.data(), fin.size());
        r.bytes.resize(pk.bytes);
        pack_polygon(r.bytes.data(), pk, fin.data(), fin.size());
        r.count = static_cast<uint32_t>(fin.size());
        r.area = polygon_area(fin);
        r.v_out = fin.size();
        Pts dec = decode(r.bytes.data(), r.count);
        r.open_out = dec.front() != dec.back();
        if (r.snapped) {
            for (size_t k = 0; k < work.size(); k++) {
                const auto& a = work[k]; const auto& b = dec[into[k]];
                r.max_disp = std::max(r.max_disp, metres(a, b));
                r.max_dlat = std::max(r.max_dlat, std::fabs(a.first - b.first));
                r.max_dlng = std::max(r.max_dlng, std::fabs(a.second - b.second));
            }
        }
    }

    std::vector<uint8_t> outv;
    outv.reserve(vb.size());
    uint64_t v_in = 0, v_dp = 0, v_out = 0, dups = 0;
    size_t live = 0, untouched = 0, changed = 0, collapsed = 0, snapped = 0, open_in = 0, open_out = 0, area_le0 = 0, jumps = 0;
    double max_disp = 0, max_dlat = 0, max_dlng = 0;
    for (size_t i = 0; i < n; i++) {
        AdminPolygon& p = polys[i];
        Res& r = res[i];
        if (r.status == 0) continue;
        live++;
        uint32_t off = static_cast<uint32_t>(outv.size());
        if (r.bytes.empty()) {
            const uint8_t* h = vb.data() + p.vertex_offset;
            outv.insert(outv.end(), h, h + block_bytes(h, p.vertex_count));
        } else {
            outv.insert(outv.end(), r.bytes.begin(), r.bytes.end());
        }
        p.vertex_offset = off; p.vertex_count = r.count; p.area = r.area;
        v_in += r.v_in; v_dp += r.v_dp; v_out += r.v_out; dups += r.dups;
        untouched += r.status == 1; changed += r.status == 2; collapsed += r.status == 3; snapped += r.snapped;
        open_in += r.open_in; open_out += r.open_out; area_le0 += !(r.area > 0); jumps += r.storage_jump;
        if (r.max_disp > max_disp) max_disp = r.max_disp;
        max_dlat = std::max(max_dlat, r.max_dlat); max_dlng = std::max(max_dlng, r.max_dlng);
        std::vector<uint8_t>().swap(r.bytes);
    }
    if (outv.size() > UINT32_MAX) { std::cerr << "output exceeds u32 offsets\n"; return 1; }
    std::ofstream(out + "/admin_polygons.bin", std::ios::binary).write(reinterpret_cast<const char*>(polys.data()), n * sizeof(AdminPolygon));
    std::ofstream(out + "/admin_vertices.bin", std::ios::binary).write(reinterpret_cast<const char*>(outv.data()), outv.size());
    std::printf("{\"mode\":\"var\",\"src\":\"%s\",\"dp_m\":%g,\"grid\":%g,\"legacy\":%d,\"live\":%zu,\"untouched\":%zu,"
                "\"changed\":%zu,\"snapped\":%zu,\"collapsed_kept_presnap\":%zu,\"of_which_storage_grid_jump\":%zu,\"v_in\":%llu,\"v_after_dp\":%llu,"
                "\"dups_dropped\":%llu,\"v_out\":%llu,\"bytes_in\":%zu,\"bytes_out\":%zu,\"open_rings_in\":%zu,"
                "\"open_rings_out\":%zu,\"area_le0_out\":%zu,\"max_snap_disp_m\":%.3f,\"max_snap_dlat_deg\":%.3e,"
                "\"max_snap_dlng_deg\":%.3e,\"threads\":%d}\n",
                src.c_str(), dp_m, G, legacy, live, untouched, changed, snapped, collapsed, jumps, (unsigned long long)v_in,
                (unsigned long long)v_dp, (unsigned long long)dups, (unsigned long long)v_out, vb.size(), outv.size(),
                open_in, open_out, area_le0, max_disp, max_dlat, max_dlng, omp_get_max_threads());
    return 0;
}
