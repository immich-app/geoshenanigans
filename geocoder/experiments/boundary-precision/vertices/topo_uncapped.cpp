// Topology-aware (shared-arc) simplification estimate for the admin-minimal
// polygon set, built from the planet's UNCAPPED admin polygons (the builder's
// own source for admin-minimal: write_admin_minimal_polygons re-simplifies
// these rings per polygon at admin_epsilon_meters(level) * 2.5).
//
//   topo_uncapped <uncapped dir> <out dir> [grid_deg=1e-5]
//
// 1. Decode every L2..L8 ring exactly as the reader does (f32 results).
// 2. Per-polygon baseline: simplify_polygon_epsilon per ring at q2.5 (what
//    the builder does today) and at floors / scales (lossy options).
// 3. Cluster vertex occurrences of different polygons that are the same
//    point (within both polygons' half-grid + 1e-5 deg slack) with a greedy
//    sweep (each occurrence joins the nearest earlier representative of a
//    cluster that does not already hold its polygon).
// 4. TopoJSON arcs: junction = cluster whose occurrences disagree on their
//    {prev,next} neighbour clusters; rings cut at junctions; arcs deduped
//    in either direction (closed junction-free rings canonically rotated).
//    Mode "global": arcs shared across levels, simplified once at the
//    smallest epsilon of the levels using them. Mode "perlevel": arcs only
//    shared within one level, simplified at that level's epsilon.
// 5. Writes coordinate / structure streams on a global grid for zstd.
#include <algorithm>
#include <atomic>
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
#include <tuple>
#include "types.h"
#include "geometry.h"

using Pts = std::vector<std::pair<double, double>>;

static std::vector<uint8_t> slurp(const std::string& p) {
    std::ifstream f(p, std::ios::binary | std::ios::ate);
    if (!f) { std::cerr << "cannot open " << p << "\n"; std::exit(1); }
    std::vector<uint8_t> b(static_cast<size_t>(f.tellg()));
    f.seekg(0); f.read(reinterpret_cast<char*>(b.data()), b.size());
    return b;
}

static double LEVEL_EPS(uint8_t lvl, double scale) { return admin_epsilon_meters(lvl) * scale; }

// ---- byte stream helpers ----
static inline uint64_t zz(int64_t x) { return x >= 0 ? uint64_t(x) << 1 : (uint64_t(-x) << 1) - 1; }
static inline void put_varint(std::vector<uint8_t>& o, uint64_t v) {
    while (v >= 0x80) { o.push_back(uint8_t(v) | 0x80); v >>= 7; }
    o.push_back(uint8_t(v));
}
static void write_file(const std::string& p, const std::vector<uint8_t>& b) {
    std::ofstream(p, std::ios::binary).write(reinterpret_cast<const char*>(b.data()), b.size());
}
// Coordinate sequences (each sequence = one ring or one arc) on a global grid:
// first vertex delta from the previous sequence's first vertex, then deltas.
struct CoordStreams {
    std::vector<uint8_t> vlat, vlng;                  // zigzag varints, split streams
    std::vector<uint8_t> width;                       // 1 byte per sequence: 2 or 4
    std::vector<uint8_t> p16[4], p32[8];              // byte planes: lat lo..hi, lng lo..hi
    int64_t prev_first_a = 0, prev_first_b = 0;
    uint64_t seqs = 0, verts = 0;
    void add(const std::vector<int64_t>& A, const std::vector<int64_t>& B) {
        size_t n = A.size(); if (!n) return;
        std::vector<int64_t> da(n), db(n);
        da[0] = A[0] - prev_first_a; db[0] = B[0] - prev_first_b;
        prev_first_a = A[0]; prev_first_b = B[0];
        bool wide = false;
        for (size_t k = 0; k < n; k++) {
            if (k) { da[k] = A[k] - A[k - 1]; db[k] = B[k] - B[k - 1]; }
            put_varint(vlat, zz(da[k])); put_varint(vlng, zz(db[k]));
            if (k && (std::llabs(da[k]) > 32767 || std::llabs(db[k]) > 32767)) wide = true;
        }
        // first vertex always 32-bit (absolute-ish jump); rest by width flag
        width.push_back(wide ? 4 : 2);
        for (int c = 0; c < 4; c++) { p32[c].push_back(uint8_t(zz(da[0]) >> (8 * c))); p32[4 + c].push_back(uint8_t(zz(db[0]) >> (8 * c))); }
        for (size_t k = 1; k < n; k++) {
            uint64_t a = zz(da[k]), b = zz(db[k]);
            if (wide) { for (int c = 0; c < 4; c++) { p32[c].push_back(uint8_t(a >> (8 * c))); p32[4 + c].push_back(uint8_t(b >> (8 * c))); } }
            else { p16[0].push_back(uint8_t(a)); p16[1].push_back(uint8_t(a >> 8)); p16[2].push_back(uint8_t(b)); p16[3].push_back(uint8_t(b >> 8)); }
        }
        seqs++; verts += n;
    }
    void write(const std::string& prefix) const {
        std::vector<uint8_t> v = vlat; v.insert(v.end(), vlng.begin(), vlng.end());
        write_file(prefix + "_coords_varint.bin", v);
        std::vector<uint8_t> s = width;
        for (auto& p : p16) s.insert(s.end(), p.begin(), p.end());
        for (auto& p : p32) s.insert(s.end(), p.begin(), p.end());
        write_file(prefix + "_coords_shuffle.bin", s);
        std::printf("    %s: %llu seqs, %llu stored vertices, varint %zu B, shuffle %zu B\n", prefix.c_str(),
                    (unsigned long long)seqs, (unsigned long long)verts, v.size(), s.size());
    }
};

struct Ring { size_t start, len; uint8_t level; uint8_t enc; };

int main(int argc, char** argv) {
    if (argc < 3) { std::cerr << "usage: topo_uncapped <uncapped dir> <out dir> [grid]\n"; return 1; }
    std::string src = argv[1], out = argv[2];
    double G = argc > 3 ? std::atof(argv[3]) : 1e-5;
    const double SCALE = 2.5;  // kAdminMinimalEpsilonScale (build_index.cpp:1682)
    auto pb = slurp(src + "/admin_polygons.bin");
    auto vb = slurp(src + "/admin_vertices.bin");
    size_t np = pb.size() / sizeof(AdminPolygon);
    const AdminPolygon* polys = reinterpret_cast<const AdminPolygon*>(pb.data());

    // ---- 1. decode ----
    std::vector<Ring> rings;
    std::vector<double> lat, lng;
    std::vector<float> half;
    std::vector<uint32_t> ring_of;
    size_t enc_v[4] = {0, 0, 0, 0}, open_dropped = 0;
    for (size_t i = 0; i < np; i++) {
        const AdminPolygon& p = polys[i];
        if (p.vertex_count < 3 || p.vertex_offset == NO_DATA) continue;
        if (p.admin_level < 2 || p.admin_level > 8) continue;
        const uint8_t* h = vb.data() + p.vertex_offset;
        uint8_t enc = h[0];
        float bml, bmg; std::memcpy(&bml, h + 2, 4); std::memcpy(&bmg, h + 6, 4);
        size_t stride = VERTEX_STRIDE[enc];
        double sc = enc == 0 ? 1e-5 : enc == 1 ? 1e-4 : enc == 2 ? 1e-6 : 1e-7;
        size_t n = p.vertex_count;
        Pts pts(n);
        for (size_t k = 0; k < n; k++) {
            const uint8_t* q = h + POLY_HEADER_BYTES + k * stride;
            double dl, dg;
            if (enc == 3) { uint32_t a, b; std::memcpy(&a, q, 4); std::memcpy(&b, q + 4, 4); dl = a; dg = b; }
            else { uint16_t a, b; std::memcpy(&a, q, 2); std::memcpy(&b, q + 2, 2); dl = a; dg = b; }
            pts[k] = {static_cast<float>(double(bml) + dl * sc), static_cast<float>(double(bmg) + dg * sc)};
        }
        if (pts.front() == pts.back()) { pts.pop_back(); open_dropped++; }
        Ring r{lat.size(), pts.size(), p.admin_level, enc};
        for (auto& [a, b] : pts) {
            lat.push_back(a); lng.push_back(b);
            half.push_back(float(0.5 * sc + 1e-5));
            ring_of.push_back(uint32_t(rings.size()));
        }
        enc_v[enc] += pts.size();
        rings.push_back(r);
    }
    vb.clear(); vb.shrink_to_fit();
    const size_t N = lat.size(), R = rings.size();
    std::printf("L2-8 rings %zu (closing vertex dropped on %zu), open-ring vertex occurrences %zu\n", R, open_dropped, N);
    std::printf("  by encoding: u16@1e-5 %zu, u16@1e-4 %zu, u16@1e-6 %zu, u32@1e-7 %zu\n", enc_v[0], enc_v[1], enc_v[2], enc_v[3]);
    {
        size_t lv[16] = {0}, lr[16] = {0};
        for (auto& r : rings) { lv[r.level] += r.len; lr[r.level]++; }
        for (int l = 2; l <= 8; l++) std::printf("  level %d: rings %zu, open vertices %zu\n", l, lr[l], lv[l]);
    }
    auto ring_pts = [&](size_t r, bool closed) {
        Pts p(rings[r].len + (closed ? 1 : 0));
        for (size_t k = 0; k < rings[r].len; k++) p[k] = {lat[rings[r].start + k], lng[rings[r].start + k]};
        if (closed) p.back() = p.front();
        return p;
    };
    auto gq = [&](double v) { return int64_t(std::llround(v / G)); };

    // ---- 2. per-polygon baselines ----
    struct Variant { const char* name; double floor_m; double mult; };
    const Variant variants[] = {{"q2.5 (today)", 0, 1}, {"floor75", 75, 1}, {"floor150", 150, 1}, {"x2", 0, 2}, {"x4", 0, 4}};
    const bool runsonly = argc > 5 && std::string(argv[5]) == "runsonly";
    for (const auto& V : variants) {
        if (runsonly) break;
        std::vector<uint32_t> kept(R, 0);
        #pragma omp parallel for schedule(dynamic, 16)
        for (size_t r = 0; r < R; r++) {
            Pts p = ring_pts(r, true);
            double eps_m = std::max(LEVEL_EPS(rings[r].level, SCALE) * V.mult, V.floor_m);
            kept[r] = uint32_t(simplify_polygon_epsilon(p, meters_to_degrees(eps_m, p[0].first)).size());
        }
        size_t tot = 0, surv = 0, lv[16] = {0};
        for (size_t r = 0; r < R; r++) if (kept[r] >= 3) { tot += kept[r]; surv++; lv[rings[r].level] += kept[r]; }
        std::printf("per-polygon %-13s: rings>=3 %zu, stored vertices (closed rings) %zu  [L2 %zu L3 %zu L4 %zu L5 %zu L6 %zu L7 %zu L8 %zu]\n",
                    V.name, surv, tot, lv[2], lv[3], lv[4], lv[5], lv[6], lv[7], lv[8]);
        if (V.floor_m == 0 && V.mult == 1) {
            // write today-equivalent per-polygon streams on the global grid (open rings)
            CoordStreams cs; std::vector<uint8_t> st;
            for (size_t r = 0; r < R; r++) {
                Pts p = ring_pts(r, true);
                double eps_m = LEVEL_EPS(rings[r].level, SCALE);
                Pts s = simplify_polygon_epsilon(p, meters_to_degrees(eps_m, p[0].first));
                if (s.size() < 3) continue;
                s.pop_back();
                std::vector<int64_t> A, B;
                for (auto& [a, b] : s) { A.push_back(gq(a)); B.push_back(gq(b)); }
                cs.add(A, B); put_varint(st, s.size());
            }
            cs.write(out + "/perpoly");
            write_file(out + "/perpoly_struct.bin", st);
            std::printf("    perpoly struct (ring lengths) %zu B\n", st.size());
        }
    }

    // ---- 3. clustering sweep ----
    const double C = 1.5e-4;
    std::vector<uint64_t> key(N);
    auto cellx = [&](double a) { return int64_t(std::floor(a / C)) + 700000; };
    auto celly = [&](double b) { return int64_t(std::floor(b / C)) + 1300000; };
    for (size_t i = 0; i < N; i++) key[i] = uint64_t(cellx(lat[i])) * 4000000ull + uint64_t(celly(lng[i]));
    std::vector<uint32_t> ord(N);
    for (size_t i = 0; i < N; i++) ord[i] = uint32_t(i);
    std::sort(ord.begin(), ord.end(), [&](uint32_t a, uint32_t b) {
        if (key[a] != key[b]) return key[a] < key[b];
        if (half[a] != half[b]) return half[a] < half[b];
        return a < b;
    });
    std::vector<uint64_t> skey(N);
    for (size_t i = 0; i < N; i++) skey[i] = key[ord[i]];
    key.clear(); key.shrink_to_fit();
    std::vector<uint32_t> clus(N, UINT32_MAX), rep_of_cluster, next_member(N, UINT32_MAX), head;
    std::vector<uint8_t> is_rep(N, 0);
    size_t chained_guard = 0;
    uint64_t prev_cx = UINT64_MAX; size_t ptr[2] = {0, 0};
    for (size_t si = 0; si < N; si++) {
        uint32_t v = ord[si];
        uint64_t k = skey[si];
        uint64_t cx = k / 4000000ull, cy = k % 4000000ull;
        if (cx != prev_cx) {
            ptr[0] = std::lower_bound(skey.begin(), skey.begin() + si, (cx - 1) * 4000000ull) - skey.begin();
            ptr[1] = si; prev_cx = cx;
        }
        double best = 1e9; uint32_t bestc = UINT32_MAX;
        for (int dx = -1; dx <= 0; dx++) {
            uint64_t lo = (cx + dx) * 4000000ull + (cy - 1);
            uint64_t hi = (cx + dx) * 4000000ull + (cy + 1);
            size_t& a = ptr[dx + 1];
            while (a < si && skey[a] < lo) a++;
            for (size_t j = a; j < si && skey[j] <= hi; j++) {
                uint32_t u = ord[j];
                if (!is_rep[u]) continue;
                if (ring_of[u] == ring_of[v]) continue;
                double tol = double(half[u]) + double(half[v]);
                double d1 = std::fabs(lat[u] - lat[v]), d2 = std::fabs(lng[u] - lng[v]);
                if (d1 > tol || d2 > tol) continue;
                double d = std::max(d1, d2);
                if (d >= best) continue;
                // guard: cluster must not already hold this ring
                bool has = false;
                for (uint32_t m = head[clus[u]]; m != UINT32_MAX; m = next_member[m]) if (ring_of[m] == ring_of[v]) { has = true; break; }
                if (has) { chained_guard++; continue; }
                best = d; bestc = clus[u];
            }
        }
        if (bestc == UINT32_MAX) {
            bestc = uint32_t(rep_of_cluster.size());
            rep_of_cluster.push_back(v); head.push_back(UINT32_MAX); is_rep[v] = 1;
        }
        clus[v] = bestc; next_member[v] = head[bestc]; head[bestc] = v;
    }
    skey.clear(); skey.shrink_to_fit(); ord.clear(); ord.shrink_to_fit();
    const size_t NC = rep_of_cluster.size();
    // sharing stats
    size_t shared_any = 0, shared_same = 0;
    for (size_t c = 0; c < NC; c++) {
        size_t cnt = 0; bool multi = false; uint32_t r0 = UINT32_MAX;
        for (uint32_t m = head[c]; m != UINT32_MAX; m = next_member[m]) { cnt++; if (r0 == UINT32_MAX) r0 = ring_of[m]; else if (ring_of[m] != r0) multi = true; }
        if (multi) {
            shared_any += cnt;
            for (uint32_t m = head[c]; m != UINT32_MAX; m = next_member[m]) {
                bool same = false;
                for (uint32_t o = head[c]; o != UINT32_MAX; o = next_member[o])
                    if (o != m && ring_of[o] != ring_of[m] && rings[ring_of[o]].level == rings[ring_of[m]].level) { same = true; break; }
                shared_same += same;
            }
        }
    }
    std::printf("clusters (unique positions) %zu = %.1f%% of occurrences; occurrences shared with another ring %.1f%%, with a same-level ring %.1f%% (guard rejections %zu)\n",
                NC, 100.0 * NC / N, 100.0 * shared_any / N, 100.0 * shared_same / N, chained_guard);
    next_member.clear(); next_member.shrink_to_fit(); head.clear(); head.shrink_to_fit(); is_rep.clear(); is_rep.shrink_to_fit();

    // ring cluster sequences (consecutive duplicates collapsed)
    std::vector<std::vector<uint32_t>> rseq(R);
    for (size_t r = 0; r < R; r++) {
        auto& s = rseq[r];
        for (size_t k = 0; k < rings[r].len; k++) { uint32_t c = clus[rings[r].start + k]; if (s.empty() || s.back() != c) s.push_back(c); }
        while (s.size() > 1 && s.back() == s.front()) s.pop_back();
    }
    clus.clear(); clus.shrink_to_fit();

    {   // unique positions within each level (unsimplified)
        std::vector<uint8_t> seenc(NC, 0);
        for (int L = 2; L <= 8; L++) {
            size_t occ = 0, uniq = 0; std::vector<uint32_t> t;
            for (size_t r = 0; r < R; r++) if (rings[r].level == L) for (uint32_t c : rseq[r]) { occ++; if (!seenc[c]) { seenc[c] = 1; uniq++; t.push_back(c); } }
            for (uint32_t c : t) seenc[c] = 0;
            std::printf("  level %d: occurrences %zu, unique positions within level %zu (%.1f%%)\n", L, occ, uniq, 100.0 * uniq / occ);
        }
    }
    // ---- 3b. repair: the uncapped rings are themselves DP-simplified per
    // polygon at 0.1 m, so a near-collinear node can survive in ring A but
    // not in its neighbour B. That breaks the {prev,next} junction rule
    // (two spurious junctions per dropped node). Re-insert such runs (up to
    // 8 clusters) into B where B's edge p->r spans A's p, x1..xm, r.
    const bool repair = argc > 4 && std::string(argv[4]) == "repair";
    if (repair) {
        std::vector<uint64_t> cstart(NC + 1, 0);
        for (size_t r = 0; r < R; r++) for (uint32_t c : rseq[r]) cstart[c + 1]++;
        for (size_t c = 0; c < NC; c++) cstart[c + 1] += cstart[c];
        std::vector<uint32_t> cring(cstart[NC]), cpos(cstart[NC]);
        { std::vector<uint64_t> fillp(cstart.begin(), cstart.end() - 1);
          for (size_t r = 0; r < R; r++) for (size_t k = 0; k < rseq[r].size(); k++) { uint32_t c = rseq[r][k]; cring[fillp[c]] = uint32_t(r); cpos[fillp[c]] = uint32_t(k); fillp[c]++; } }
        auto pos_in = [&](uint32_t c, uint32_t ring) -> int64_t {
            for (uint64_t i = cstart[c]; i < cstart[c + 1]; i++) if (cring[i] == ring) return cpos[i];
            return -1;
        };
        int nt = omp_get_max_threads();
        std::vector<std::vector<std::tuple<uint32_t, uint32_t, std::vector<uint32_t>>>> props(nt);
        #pragma omp parallel for schedule(dynamic, 64)
        for (size_t A = 0; A < R; A++) {
            auto& s = rseq[A]; size_t n = s.size(); auto& mine = props[omp_get_thread_num()];
            for (size_t k = 0; k < n; k++) {
                uint32_t p = s[k];
                for (uint64_t i = cstart[p]; i < cstart[p + 1]; i++) {
                    uint32_t B = cring[i]; if (B == A) continue;
                    if (pos_in(s[(k + 1) % n], B) >= 0) continue;
                    size_t m = 1;
                    while (m <= 8 && pos_in(s[(k + m + 1) % n], B) < 0) m++;
                    if (m > 8) continue;
                    uint32_t r = s[(k + m + 1) % n];
                    int64_t pb = cpos[i], pr = pos_in(r, B); size_t nb = rseq[B].size();
                    std::vector<uint32_t> run;
                    for (size_t t = 1; t <= m; t++) run.push_back(s[(k + t) % n]);
                    if (uint64_t(pr) == (uint64_t(pb) + 1) % nb) mine.emplace_back(B, uint32_t(pb), run);
                    else if (uint64_t(pb) == (uint64_t(pr) + 1) % nb) { std::reverse(run.begin(), run.end()); mine.emplace_back(B, uint32_t(pr), run); }
                }
            }
        }
        std::unordered_map<uint64_t, std::vector<uint32_t>> ins;
        size_t proposals = 0, inserted = 0;
        for (auto& v : props) for (auto& [B, after, run] : v) { proposals++; ins.emplace((uint64_t(B) << 32) | after, std::move(run)); }
        std::vector<std::vector<std::pair<uint32_t, const std::vector<uint32_t>*>>> per_ring(R);
        for (auto& [k, run] : ins) per_ring[k >> 32].push_back({uint32_t(k & 0xffffffff), &run});
        for (size_t B = 0; B < R; B++) {
            if (per_ring[B].empty()) continue;
            std::sort(per_ring[B].begin(), per_ring[B].end());
            std::vector<uint32_t> ns; size_t q = 0;
            for (size_t k = 0; k < rseq[B].size(); k++) {
                ns.push_back(rseq[B][k]);
                while (q < per_ring[B].size() && per_ring[B][q].first == k) { for (uint32_t c : *per_ring[B][q].second) { ns.push_back(c); inserted++; } q++; }
            }
            std::vector<uint32_t> s2;
            for (uint32_t c : ns) if (s2.empty() || s2.back() != c) s2.push_back(c);
            while (s2.size() > 1 && s2.back() == s2.front()) s2.pop_back();
            rseq[B].swap(s2);
        }
        size_t occ = 0; for (auto& s : rseq) occ += s.size();
        std::printf("repair: %zu proposals, %zu distinct edges, %zu clusters inserted; ring occurrences now %zu\n", proposals, ins.size(), inserted, occ);
    }

    // ---- 4/5. arcs per mode ----
    for (int mode = 0; mode < (runsonly ? 0 : 2); mode++) {
        const char* mname = mode == 0 ? "global" : "perlevel";
        size_t tot_arcs = 0, tot_arc_occ = 0, tot_stored = 0, tot_unsimpl_arc_verts = 0, tot_ring_verts = 0, degenerate = 0;
        size_t lv_ring_verts[16] = {0}, lv_stored[16] = {0};
        CoordStreams cs; std::vector<uint8_t> st_len, st_refcnt, st_refs;
        std::vector<uint8_t> isJ(NC, 0), seen(NC, 0);
        std::vector<uint64_t> pairk(NC, 0);
        for (int L = (mode == 0 ? 0 : 2); L <= (mode == 0 ? 0 : 8); L++) {
            auto use = [&](size_t r) { return mode == 0 || rings[r].level == L; };
            // junctions
            std::vector<uint32_t> touched;
            for (size_t r = 0; r < R; r++) {
                if (!use(r)) continue;
                auto& s = rseq[r]; size_t n = s.size(); if (n < 2) continue;
                for (size_t k = 0; k < n; k++) {
                    uint32_t c = s[k], a = s[(k + n - 1) % n], b = s[(k + 1) % n];
                    uint64_t pk = (uint64_t(std::min(a, b)) << 32) | std::max(a, b);
                    if (!seen[c]) { seen[c] = 1; pairk[c] = pk; touched.push_back(c); }
                    else if (pairk[c] != pk) isJ[c] = 1;
                }
            }
            // arcs
            std::unordered_map<uint64_t, std::vector<uint32_t>> idx;
            std::vector<size_t> arc_off; std::vector<uint32_t> arc_data; std::vector<uint8_t> arc_closed; std::vector<double> arc_eps;
            std::vector<std::vector<std::pair<uint32_t, bool>>> refs(R);
            auto intern = [&](std::vector<uint32_t>& seq, bool closed, double eps) -> std::pair<uint32_t, bool> {
                bool rev = false;
                if (closed) {
                    // rotate to min, direction by second element
                    auto it = std::min_element(seq.begin(), seq.end()); std::rotate(seq.begin(), it, seq.end());
                    if (seq.size() > 2 && seq.back() < seq[1]) { std::reverse(seq.begin() + 1, seq.end()); rev = true; }
                } else {
                    std::vector<uint32_t> r2(seq.rbegin(), seq.rend());
                    if (r2 < seq) { seq.swap(r2); rev = true; }
                }
                uint64_t h = 1469598103934665603ull ^ (closed ? 0x9e37 : 0);
                for (uint32_t c : seq) { h ^= c; h *= 1099511628211ull; h ^= h >> 29; }
                auto& bucket = idx[h];
                for (uint32_t id : bucket) {
                    size_t o = arc_off[id], len = arc_off[id + 1] - o;
                    if (len == seq.size() && arc_closed[id] == closed && std::equal(seq.begin(), seq.end(), arc_data.begin() + o)) {
                        arc_eps[id] = std::min(arc_eps[id], eps); return {id, rev};
                    }
                }
                uint32_t id = uint32_t(arc_closed.size());
                if (arc_off.empty()) arc_off.push_back(0);
                arc_data.insert(arc_data.end(), seq.begin(), seq.end()); arc_off.push_back(arc_data.size());
                arc_closed.push_back(closed); arc_eps.push_back(eps); bucket.push_back(id);
                return {id, rev};
            };
            for (size_t r = 0; r < R; r++) {
                if (!use(r)) continue;
                auto& s = rseq[r]; size_t n = s.size(); if (n < 3) { degenerate++; continue; }
                double eps = LEVEL_EPS(rings[r].level, SCALE);
                size_t j0 = SIZE_MAX;
                for (size_t k = 0; k < n; k++) if (isJ[s[k]]) { j0 = k; break; }
                if (j0 == SIZE_MAX) {
                    std::vector<uint32_t> seq = s;
                    refs[r].push_back(intern(seq, true, eps));
                    continue;
                }
                std::vector<uint32_t> cur{s[j0]};
                for (size_t t = 1; t <= n; t++) {
                    uint32_t c = s[(j0 + t) % n];
                    cur.push_back(c);
                    if (isJ[c]) { std::vector<uint32_t> seq = cur; refs[r].push_back(intern(seq, false, eps)); cur.assign(1, c); }
                }
            }
            for (uint32_t c : touched) { isJ[c] = 0; seen[c] = 0; }
            const size_t NA = arc_closed.size();
            // simplify arcs
            std::vector<std::vector<uint32_t>> kept(NA);
            #pragma omp parallel for schedule(dynamic, 64)
            for (size_t a = 0; a < NA; a++) {
                size_t o = arc_off[a], len = arc_off[a + 1] - o;
                Pts p(len);
                for (size_t k = 0; k < len; k++) { uint32_t v = rep_of_cluster[arc_data[o + k]]; p[k] = {lat[v], lng[v]}; }
                double eps_deg = meters_to_degrees(arc_eps[a], p[0].first);
                std::vector<uint32_t> K;
                if (arc_closed[a]) {
                    p.push_back(p.front());
                    // same as simplify_polygon_epsilon but returning indices
                    std::vector<bool> keep(p.size(), false); keep[0] = keep[p.size() - 1] = true;
                    if (p.size() > 4) dp_simplify(p, 0, p.size() - 1, eps_deg, keep); else std::fill(keep.begin(), keep.end(), true);
                    size_t c = 0; for (bool b : keep) c += b;
                    if (c < 4 && p.size() > 4) {  // builder's min_vertices=4 rescue
                        double lo = 0, hi = eps_deg;
                        for (int it = 0; it < 20; it++) {
                            double mid = (lo + hi) / 2; std::vector<bool> tk(p.size(), false); tk[0] = tk[p.size() - 1] = true;
                            dp_simplify(p, 0, p.size() - 1, mid, tk); size_t cc = 0; for (bool b : tk) cc += b;
                            if (cc >= 4) { lo = mid; keep = tk; } else hi = mid;
                        }
                    }
                    for (size_t k = 0; k + 1 < p.size(); k++) if (keep[k]) K.push_back(uint32_t(k));  // open storage
                } else {
                    std::vector<bool> keep(p.size(), false); keep[0] = keep[p.size() - 1] = true;
                    dp_simplify(p, 0, p.size() - 1, eps_deg, keep);
                    for (size_t k = 0; k < p.size(); k++) if (keep[k]) K.push_back(uint32_t(k));
                }
                kept[a] = std::move(K);
            }
            // streams + stats
            std::vector<uint32_t> new_id(NA, UINT32_MAX); uint32_t next_id = 0;
            for (size_t r = 0; r < R; r++) {
                if (!use(r) || refs[r].empty()) continue;
                put_varint(st_refcnt, refs[r].size());
                int64_t prev = 0; size_t rv = 0;
                for (auto [a, rev] : refs[r]) {
                    if (new_id[a] == UINT32_MAX) {
                        new_id[a] = next_id++;
                        size_t o = arc_off[a];
                        std::vector<int64_t> A, B;
                        for (uint32_t k : kept[a]) { uint32_t v = rep_of_cluster[arc_data[o + k]]; A.push_back(gq(lat[v])); B.push_back(gq(lng[v])); }
                        cs.add(A, B); put_varint(st_len, kept[a].size());
                        tot_stored += kept[a].size(); lv_stored[rings[r].level] += kept[a].size();
                        tot_unsimpl_arc_verts += arc_off[a + 1] - o;
                    }
                    int64_t ref = int64_t(new_id[a]) * 2 + (rev ? 1 : 0);
                    put_varint(st_refs, zz(ref - prev)); prev = ref;
                    rv += arc_closed[a] ? kept[a].size() : kept[a].size() - 1;
                    tot_arc_occ++;
                }
                tot_ring_verts += rv + 1; lv_ring_verts[rings[r].level] += rv + 1;
            }
            tot_arcs += NA;
        }
        std::printf("topo %s: unique arcs %zu, arc refs %zu (%.1f refs/ring), unsimplified arc vertices %zu (%.1f%% of occurrences), "
                    "stored after simplify %zu, ring vertices reconstructed (closed) %zu, rings <3 clusters %zu\n",
                    mname, tot_arcs, tot_arc_occ, double(tot_arc_occ) / R, tot_unsimpl_arc_verts, 100.0 * tot_unsimpl_arc_verts / N,
                    tot_stored, tot_ring_verts, degenerate);
        std::printf("  ring vertices by level: L2 %zu L3 %zu L4 %zu L5 %zu L6 %zu L7 %zu L8 %zu\n",
                    lv_ring_verts[2], lv_ring_verts[3], lv_ring_verts[4], lv_ring_verts[5], lv_ring_verts[6], lv_ring_verts[7], lv_ring_verts[8]);
        cs.write(out + "/topo_" + mname);
        std::vector<uint8_t> st = st_len; st.insert(st.end(), st_refcnt.begin(), st_refcnt.end()); st.insert(st.end(), st_refs.begin(), st_refs.end());
        write_file(out + "/topo_" + std::string(mname) + "_struct.bin", st);
        std::printf("    struct: arc lengths %zu B, ref counts %zu B, refs %zu B\n", st_len.size(), st_refcnt.size(), st_refs.size());
    }

    // ---- 6. "runs" estimator: robust to near-duplicate vertex lists.
    // For each ring position, its partner set = other in-use rings holding
    // the same cluster. Rings are cut where the partner set changes; runs
    // shorter than 3 vertices whose two neighbour runs agree are absorbed
    // (a node one polygon kept and its neighbour dropped). A run is stored
    // once, by the lowest ring id among {ring} + partners, simplified with
    // fixed endpoints at the smallest epsilon of those rings' levels; the
    // other rings reference the owner's run (found via a shared cluster).
    auto keep_open = [](const Pts& p, double eps_deg) {
        std::vector<bool> keep(p.size(), false); keep[0] = keep[p.size() - 1] = true;
        dp_simplify(p, 0, p.size() - 1, eps_deg, keep);
        std::vector<uint32_t> K; for (size_t t = 0; t < p.size(); t++) if (keep[t]) K.push_back(uint32_t(t));
        return K;
    };
    auto keep_closed = [](Pts p, double eps_deg) {  // simplify_polygon_epsilon, as indices, open storage
        p.push_back(p.front());
        std::vector<bool> keep(p.size(), true);
        if (p.size() > 4) {
            std::fill(keep.begin(), keep.end(), false); keep[0] = keep[p.size() - 1] = true;
            dp_simplify(p, 0, p.size() - 1, eps_deg, keep);
            size_t c = 0; for (bool b : keep) c += b;
            if (c < 4) {
                double lo = 0, hi = eps_deg;
                for (int it = 0; it < 20; it++) {
                    double mid = (lo + hi) / 2; std::vector<bool> tk(p.size(), false); tk[0] = tk[p.size() - 1] = true;
                    dp_simplify(p, 0, p.size() - 1, mid, tk); size_t cc = 0; for (bool b : tk) cc += b;
                    if (cc >= 4) { lo = mid; keep = tk; } else hi = mid;
                }
            }
        }
        std::vector<uint32_t> K; for (size_t t = 0; t + 1 < p.size(); t++) if (keep[t]) K.push_back(uint32_t(t));
        return K;
    };
    {
        std::vector<uint64_t> cstart(NC + 1, 0);
        for (size_t r = 0; r < R; r++) for (uint32_t c : rseq[r]) cstart[c + 1]++;
        for (size_t c = 0; c < NC; c++) cstart[c + 1] += cstart[c];
        std::vector<uint32_t> cring(cstart[NC]), cpos(cstart[NC]);
        { std::vector<uint64_t> fillp(cstart.begin(), cstart.end() - 1);
          for (size_t r = 0; r < R; r++) for (size_t k = 0; k < rseq[r].size(); k++) { uint32_t c = rseq[r][k]; cring[fillp[c]] = uint32_t(r); cpos[fillp[c]] = uint32_t(k); fillp[c]++; } }
        auto pos_in = [&](uint32_t c, uint32_t ring) -> int64_t {
            for (uint64_t i = cstart[c]; i < cstart[c + 1]; i++) if (cring[i] == ring) return cpos[i];
            return -1;
        };
        struct Run { uint32_t start, len; uint64_t h; uint32_t owner; float eps; bool closed; };
        for (int mode = 0; mode < 2; mode++) {
            const char* mname = mode == 0 ? "global" : "perlevel";
            auto use2 = [&](uint32_t a, uint32_t b) { return mode == 0 || rings[a].level == rings[b].level; };
            std::vector<std::vector<Run>> runs(R);
            std::vector<std::vector<uint32_t>> runat(R);
            #pragma omp parallel for schedule(dynamic, 64)
            for (size_t A = 0; A < R; A++) {
                auto& s = rseq[A]; size_t n = s.size(); if (n < 3) continue;
                std::vector<uint64_t> h(n); std::vector<uint32_t> own(n); std::vector<float> ep(n);
                for (size_t k = 0; k < n; k++) {
                    uint32_t c = s[k]; std::vector<uint32_t> P;
                    for (uint64_t i = cstart[c]; i < cstart[c + 1]; i++) if (cring[i] != A && use2(uint32_t(A), cring[i])) P.push_back(cring[i]);
                    std::sort(P.begin(), P.end()); P.erase(std::unique(P.begin(), P.end()), P.end());
                    uint64_t hh = 1469598103934665603ull; uint32_t o = uint32_t(A); double e = LEVEL_EPS(rings[A].level, SCALE);
                    for (uint32_t q : P) { hh ^= q; hh *= 1099511628211ull; hh ^= hh >> 31; o = std::min(o, q); e = std::min(e, LEVEL_EPS(rings[q].level, SCALE)); }
                    h[k] = P.empty() ? 0 : hh; own[k] = o; ep[k] = float(e);
                }
                size_t k0 = SIZE_MAX;
                for (size_t k = 0; k < n; k++) if (h[k] != h[(k + n - 1) % n]) { k0 = k; break; }
                std::vector<Run> rr;
                if (k0 == SIZE_MAX) rr.push_back({0, uint32_t(n), h[0], own[0], ep[0], true});
                else {
                    for (size_t t = 0; t < n; t++) {
                        size_t k = (k0 + t) % n;
                        if (rr.empty() || rr.back().h != h[k]) rr.push_back({uint32_t(k), 1, h[k], own[k], ep[k], false});
                        else rr.back().len++;
                    }
                    for (int pass = 0; pass < 2 && rr.size() > 2; pass++) {
                        size_t m = rr.size();
                        for (size_t i = 0; i < m; i++) {
                            Run& a = rr[(i + m - 1) % m]; Run& b = rr[i]; Run& c = rr[(i + 1) % m];
                            if (b.len < 3 && a.h == c.h && b.h != a.h) { b.h = a.h; b.owner = a.owner; b.eps = a.eps; }
                        }
                        std::vector<Run> mg;
                        for (auto& x : rr) { if (!mg.empty() && mg.back().h == x.h) mg.back().len += x.len; else mg.push_back(x); }
                        if (mg.size() > 1 && mg.front().h == mg.back().h) { mg.back().len += mg.front().len; mg.erase(mg.begin()); }
                        rr.swap(mg);
                    }
                    if (rr.size() == 1) { rr[0].closed = true; rr[0].len = uint32_t(n); }
                }
                std::vector<uint32_t> at(n, 0);
                for (size_t i = 0; i < rr.size(); i++) for (uint32_t t = 0; t < rr[i].len; t++) at[(rr[i].start + t) % n] = uint32_t(i);
                runs[A].swap(rr); runat[A].swap(at);
            }
            std::vector<uint64_t> base(R + 1, 0);
            for (size_t A = 0; A < R; A++) base[A + 1] = base[A] + runs[A].size();
            std::vector<uint32_t> kept_n(base[R], 0);
            std::vector<std::vector<uint32_t>> kept_idx(base[R]);
            #pragma omp parallel for schedule(dynamic, 64)
            for (size_t A = 0; A < R; A++) {
                auto& s = rseq[A]; size_t n = s.size();
                for (size_t i = 0; i < runs[A].size(); i++) {
                    const Run& ru = runs[A][i];
                    size_t L = ru.closed ? n : ru.len + 1;
                    Pts p(L);
                    for (size_t t = 0; t < L; t++) { uint32_t v = rep_of_cluster[s[(ru.start + t) % n]]; p[t] = {lat[v], lng[v]}; }
                    double eps_deg = meters_to_degrees(ru.eps, p[0].first);
                    std::vector<uint32_t> K = ru.closed ? keep_closed(p, eps_deg) : keep_open(p, eps_deg);
                    kept_n[base[A] + i] = uint32_t(K.size());
                    if (ru.owner == A) kept_idx[base[A] + i].swap(K);
                }
            }
            CoordStreams cs; std::vector<uint8_t> st_len, st_refcnt, st_refs;
            std::vector<uint32_t> arc_id(base[R], UINT32_MAX); uint32_t nid = 0;
            size_t owner_runs = 0, stored = 0, refs = 0, unmatched = 0, ring_verts = 0, lvv[16] = {0}, lvs[16] = {0};
            for (size_t A = 0; A < R; A++) for (size_t i = 0; i < runs[A].size(); i++) if (runs[A][i].owner == A) {
                arc_id[base[A] + i] = nid++; owner_runs++; stored += kept_idx[base[A] + i].size(); lvs[rings[A].level] += kept_idx[base[A] + i].size();
                auto& s = rseq[A]; size_t n = s.size(); const Run& ru = runs[A][i];
                std::vector<int64_t> AA, BB;
                for (uint32_t t : kept_idx[base[A] + i]) { uint32_t v = rep_of_cluster[s[(ru.start + t) % n]]; AA.push_back(gq(lat[v])); BB.push_back(gq(lng[v])); }
                cs.add(AA, BB); put_varint(st_len, kept_idx[base[A] + i].size());
            }
            for (size_t A = 0; A < R; A++) {
                if (runs[A].empty()) continue;
                auto& s = rseq[A]; size_t n = s.size();
                put_varint(st_refcnt, runs[A].size());
                int64_t prev = 0; size_t rv = 0;
                for (size_t i = 0; i < runs[A].size(); i++) {
                    const Run& ru = runs[A][i];
                    uint64_t id = UINT64_MAX; uint32_t kn = kept_n[base[A] + i]; bool rev = false;
                    if (ru.owner == A) id = arc_id[base[A] + i];
                    else {
                        uint32_t O = ru.owner;
                        size_t L = ru.closed ? n : ru.len;
                        for (size_t t = 0; t < L && id == UINT64_MAX; t++) {
                            size_t k = (ru.start + (L / 2 + t) % L) % n;
                            int64_t po = pos_in(s[k], O);
                            if (po < 0) continue;
                            uint32_t ri = runat[O][po];
                            if (runs[O][ri].owner != O) continue;
                            id = arc_id[base[O] + ri]; kn = kept_n[base[O] + ri];
                            int64_t pn = pos_in(s[(k + 1) % n], O);
                            rev = !(pn >= 0 && uint64_t(pn) == (uint64_t(po) + 1) % rseq[O].size());
                        }
                        if (id == UINT64_MAX) { unmatched++; id = nid + unmatched; }
                    }
                    int64_t ref = int64_t(id) * 2 + rev;
                    put_varint(st_refs, zz(ref - prev)); prev = ref; refs++;
                    rv += ru.closed ? kn : (kn ? kn - 1 : 0);
                }
                ring_verts += rv + 1; lvv[rings[A].level] += rv + 1;
            }
            std::printf("runs %s: owner runs (stored arcs) %zu, refs %zu (%.1f/ring), unmatched refs %zu, stored vertices %zu, ring vertices reconstructed (closed) %zu\n",
                        mname, owner_runs, refs, double(refs) / R, unmatched, stored, ring_verts);
            std::printf("  stored by owner level: L2 %zu L3 %zu L4 %zu L5 %zu L6 %zu L7 %zu L8 %zu\n", lvs[2], lvs[3], lvs[4], lvs[5], lvs[6], lvs[7], lvs[8]);
            std::printf("  ring vertices by level: L2 %zu L3 %zu L4 %zu L5 %zu L6 %zu L7 %zu L8 %zu\n", lvv[2], lvv[3], lvv[4], lvv[5], lvv[6], lvv[7], lvv[8]);
            cs.write(out + "/runs_" + mname);
            std::vector<uint8_t> st = st_len; st.insert(st.end(), st_refcnt.begin(), st_refcnt.end()); st.insert(st.end(), st_refs.begin(), st_refs.end());
            write_file(out + "/runs_" + std::string(mname) + "_struct.bin", st);
            std::printf("    struct: arc lengths %zu B, ref counts %zu B, refs %zu B\n", st_len.size(), st_refcnt.size(), st_refs.size());
        }
    }
    return 0;
}
