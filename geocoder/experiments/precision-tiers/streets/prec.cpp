// prec: precision study tools for street / interpolation nodes and address points.
//
//   prec permute SRC_FULL SORTED META DUMPS OUT     exact values in the canon_sort order (+ checks)
//   prec stats   SORTED EXACT                       residual / displacement statistics
//   prec encways SORTED EXACT OUT street|interp     packed-way files at e5 / e7 + refinements
//   prec encaddr SORTED EXACT OUT                   address point lat/lng files + refinement
//   prec snap    SRC_FULL DUMPS OUT                 builder-order f32 files with 1.1 m values
//   prec churn   D1 D2 META1 META2 OUT              changed records between two sorted days
//   prec synth   STREAM.f32 OUTPREFIX SEED          e5 / e7 / residual streams from f32 values
//   prec synthval SORTED EXACT OUT                  real vs synthesized exact values (zstd check)
#include "common.h"
#include <omp.h>
#include <random>
#include <unordered_map>

// ---------------------------------------------------------------- permute
static void permute(const std::string& src, const std::string& sorted, const std::string& meta, const std::string& dumps,
                    const std::string& out) {
    for (int fam = 0; fam < 2; fam++) {
        const bool street = fam == 0;
        const std::string w = street ? "street_ways.bin" : "interp_ways.bin";
        const std::string nn = street ? "street_nodes" : "interp_nodes";
        Ways S, T;
        S.open(src + "/" + w, street);
        T.open(sorted + "/" + w, street);
        MF slot = map_ro(meta + (street ? "/street.slot.u32" : "/interp.slot.u32"));
        MF E = map_ro(dumps + "/" + nn + ".e7");
        MF F = map_ro(sorted + "/" + nn + ".bin");
        const uint32_t* sl = slot.as<uint32_t>();
        const E7* e = E.as<E7>();
        const NodeCoord* f = F.as<NodeCoord>();
        if (slot.count<uint32_t>() != T.n) die("%s slot count", nn.c_str());
        if (E.n != F.n) die("%s node count", nn.c_str());
        std::vector<E7> o(E.count<E7>());
        std::vector<uint8_t> tiger;
        MF side;
        if (!street) {
            side = map_ro(src + "/interp_ways.osm_ids");
            if (side.n != 12 + 12 * S.n) die("interp sidecar size %zu", side.n);
            tiger.resize(T.n);
        }
        uint64_t bad = 0, nodes = 0;
#pragma omp parallel for schedule(dynamic, 4096) reduction(+ : bad, nodes)
        for (size_t j = 0; j < T.n; j++) {
            const uint32_t s = sl[j];
            const uint16_t c = T.cnt(j);
            if (S.cnt(s) != c) die("%s way %zu count", nn.c_str(), j);
            const uint32_t a = S.off(s), b = T.off(j);
            for (uint32_t k = 0; k < c; k++) {
                o[b + k] = e[a + k];
                if (f32_of(e[a + k].lat) != f[b + k].lat || f32_of(e[a + k].lng) != f[b + k].lng) bad++;
            }
            nodes += c;
            if (!street) tiger[j] = side.p[12 + 12 * size_t(s)] == 5;
        }
        printf("%s: ways %zu nodes %llu f32 mismatches %llu\n", nn.c_str(), T.n, (unsigned long long)nodes,
               (unsigned long long)bad);
        if (bad) die("mismatch");
        write_file(out + "/" + nn + ".e7", o.data(), o.size() * 8);
        if (!street) {
            uint64_t t = 0;
            for (auto x : tiger) t += x;
            printf("interp: TIGER (synthetic) ways %llu of %zu\n", (unsigned long long)t, tiger.size());
            write_file(out + "/interp_tiger.u8", tiger.data(), tiger.size());
        }
    }
    MF slot = map_ro(meta + "/addr.slot.u32");
    MF E = map_ro(dumps + "/addr_points.e7");
    MF K = map_ro(dumps + "/addr_points.kind");
    MF P = map_ro(sorted + "/addr_points.bin");
    const size_t n = P.count<AddrPoint>();
    if (slot.count<uint32_t>() != n || E.count<E7>() != n || K.n != n) die("addr counts");
    const uint32_t* sl = slot.as<uint32_t>();
    const E7* e = E.as<E7>();
    const AddrPoint* p = P.as<AddrPoint>();
    std::vector<E7> o(n);
    std::vector<uint8_t> kind(n);
    uint64_t bad0 = 0, cnt[256] = {0}, ulpdiff[4][3] = {{0}};
    double maxd[3] = {0, 0, 0};
    for (size_t j = 0; j < n; j++) {
        const uint32_t s = sl[j];
        o[j] = e[s];
        kind[j] = K.p[s];
        cnt[kind[j]]++;
        const float a = f32_of(e[s].lat), b = f32_of(e[s].lng);
        const int k = kind[j] > 2 ? 2 : kind[j];
        if (kind[j] == 0 && (a != p[j].lat || b != p[j].lng)) bad0++;
        const int64_t ua = std::llabs(int64_t(int32_t(fbits(a))) - int64_t(int32_t(fbits(p[j].lat))));
        const int64_t ub = std::llabs(int64_t(int32_t(fbits(b))) - int64_t(int32_t(fbits(p[j].lng))));
        const int64_t u = std::max(ua, ub);
        ulpdiff[u > 3 ? 3 : u][k]++;
        maxd[k] = std::max(maxd[k], dist_m(p[j].lat, p[j].lng, a, b));
    }
    printf("addr: points %zu kind0 %llu kind1 %llu kind2 %llu other %llu; kind0 f32 mismatches %llu\n", n,
           (unsigned long long)cnt[0], (unsigned long long)cnt[1], (unsigned long long)cnt[2],
           (unsigned long long)(n - cnt[0] - cnt[1] - cnt[2]), (unsigned long long)bad0);
    for (int k = 0; k < 3; k++)
        printf("  kind %d: f32(E7) vs shipped f32, max ulp diff 0/1/2/>=3: %llu %llu %llu %llu, max distance %.4f m\n", k,
               (unsigned long long)ulpdiff[0][k], (unsigned long long)ulpdiff[1][k], (unsigned long long)ulpdiff[2][k],
               (unsigned long long)ulpdiff[3][k], maxd[k]);
    if (bad0) die("addr kind0 mismatch");
    write_file(out + "/addr_points.e7", o.data(), n * 8);
    write_file(out + "/addr_points.kind", kind.data(), n);
}

// ---------------------------------------------------------------- stats
struct ResStats {
    uint64_t hl[100] = {0}, hg[100] = {0}, n = 0, m10 = 0, m100 = 0;
    void add(int32_t e1, int32_t e2) {
        const int a = res_of(e1), b = res_of(e2);
        hl[a + 50]++;
        hg[b + 50]++;
        n++;
        m10 += (a % 10 == 0) && (b % 10 == 0);
        m100 += a == 0 && b == 0;
    }
    void merge(const ResStats& o) {
        for (int i = 0; i < 100; i++) { hl[i] += o.hl[i]; hg[i] += o.hg[i]; }
        n += o.n;
        m10 += o.m10;
        m100 += o.m100;
    }
    static double ent(const uint64_t* h, uint64_t n) {
        double s = 0;
        for (int i = 0; i < 100; i++)
            if (h[i]) { const double p = double(h[i]) / double(n); s -= p * log2(p); }
        return s;
    }
    void print(const char* name) const {
        uint64_t mn = ~0ull, mx = 0;
        for (int i = 0; i < 100; i++) { mn = std::min(mn, hl[i]); mx = std::max(mx, hl[i]); }
        printf("%-22s coords(pairs) %llu: both residuals multiple of 10: %.3f%%, both 0: %.3f%%; entropy lat %.3f lng %.3f "
               "bits (uniform 6.644); lat bin max/min %.3f\n",
               name, (unsigned long long)n, 100.0 * double(m10) / double(n), 100.0 * double(m100) / double(n),
               ent(hl, n), ent(hg, n), double(mx) / double(mn ? mn : 1));
    }
};

struct DispStats {
    Hist f32, e5, f32e5, f32e5_vs_f32;
    void add(int32_t la, int32_t lg, float fa, float fg) {
        const double ela = la / 1e7, elg = lg / 1e7;
        const int32_t ka = e5_of(la), kg = e5_of(lg);
        f32.add(dist_m(ela, elg, fa, fg));
        e5.add(dist_m(ela, elg, ka / 1e5, kg / 1e5));
        const float sa = f32_of_e5(ka), sg = f32_of_e5(kg);
        f32e5.add(dist_m(ela, elg, sa, sg));
        f32e5_vs_f32.add(dist_m(fa, fg, sa, sg));
    }
    void merge(const DispStats& o) {
        f32.merge(o.f32);
        e5.merge(o.e5);
        f32e5.merge(o.f32e5);
        f32e5_vs_f32.merge(o.f32e5_vs_f32);
    }
    void print(const std::string& name) const {
        f32.print((name + " f32 vs exact").c_str());
        e5.print((name + " 1e-5 vs exact").c_str());
        f32e5.print((name + " f32(1e-5) vs exact").c_str());
        f32e5_vs_f32.print((name + " f32(1e-5) vs f32").c_str());
    }
};

// Per-way residual unit: 100 if every residual is 0, 10 if every residual is a multiple of 10, else 1.
static int way_unit(const E7* e, size_t c) {
    bool all0 = true, all10 = true;
    for (size_t k = 0; k < c; k++) {
        const int a = res_of(e[k].lat), b = res_of(e[k].lng);
        all0 &= a == 0 && b == 0;
        all10 &= a % 10 == 0 && b % 10 == 0;
    }
    return all0 ? 100 : all10 ? 10 : 1;
}
static int unit_bits(int u) { return u == 100 ? 0 : u == 10 ? 8 : 14; }

static void stats(const std::string& sorted, const std::string& exact) {
    for (int fam = 0; fam < 2; fam++) {
        const bool street = fam == 0;
        Ways W;
        W.open(sorted + (street ? "/street_ways.bin" : "/interp_ways.bin"), street);
        MF E = map_ro(exact + (street ? "/street_nodes.e7" : "/interp_nodes.e7"));
        MF F = map_ro(sorted + (street ? "/street_nodes.bin" : "/interp_nodes.bin"));
        MF T;
        if (!street) T = map_ro(exact + "/interp_tiger.u8");
        const E7* e = E.as<E7>();
        const NodeCoord* f = F.as<NodeCoord>();
        const int classes = street ? 1 : 2;
        std::vector<ResStats> rs(classes);
        std::vector<DispStats> ds(classes);
        // [class][unit idx]: ways, nodes
        uint64_t wu[2][3] = {{0}}, nu[2][3] = {{0}};
        // per-group unit (max over the group's ways) bits
        uint64_t group_bits_way = 0, group_bits_grp = 0, fixed_bits = 0;
#pragma omp parallel
        {
            std::vector<ResStats> lrs(classes);
            std::vector<DispStats> lds(classes);
            uint64_t lwu[2][3] = {{0}}, lnu[2][3] = {{0}}, gbw = 0, gbg = 0, fb = 0;
#pragma omp for schedule(dynamic, 64)
            for (size_t g = 0; g < (W.n + 15) / 16; g++) {
                int gunit = 100;
                uint64_t gnodes = 0;
                for (size_t j = g * 16; j < std::min(W.n, g * 16 + 16); j++) {
                    const size_t c = W.cnt(j), o = W.off(j);
                    const int cls = street ? 0 : (T.p[j] ? 1 : 0);
                    for (size_t k = 0; k < c; k++) {
                        lrs[cls].add(e[o + k].lat, e[o + k].lng);
                        if ((o + k) % 4 == 0) lds[cls].add(e[o + k].lat, e[o + k].lng, f[o + k].lat, f[o + k].lng);
                    }
                    if (!c) continue;
                    const int u = way_unit(e + o, c);
                    const int ui = u == 100 ? 2 : u == 10 ? 1 : 0;
                    lwu[cls][ui]++;
                    lnu[cls][ui] += c;
                    gbw += uint64_t(c) * unit_bits(u);
                    gunit = std::min(gunit, u);
                    gnodes += c;
                    fb += 14 * uint64_t(c);
                }
                gbg += gnodes * unit_bits(gunit);
            }
#pragma omp critical
            {
                for (int c = 0; c < classes; c++) { rs[c].merge(lrs[c]); ds[c].merge(lds[c]); }
                for (int c = 0; c < 2; c++)
                    for (int u = 0; u < 3; u++) { wu[c][u] += lwu[c][u]; nu[c][u] += lnu[c][u]; }
                group_bits_way += gbw;
                group_bits_grp += gbg;
                fixed_bits += fb;
            }
        }
        const char* nm[2] = {street ? "streets" : "interp OSM", "interp TIGER"};
        for (int c = 0; c < classes; c++) {
            rs[c].print(nm[c]);
            printf("  %s ways by residual unit 1 / 10 / 100: %llu / %llu / %llu (nodes %llu / %llu / %llu)\n", nm[c],
                   (unsigned long long)wu[c][0], (unsigned long long)wu[c][1], (unsigned long long)wu[c][2],
                   (unsigned long long)nu[c][0], (unsigned long long)nu[c][1], (unsigned long long)nu[c][2]);
            ds[c].print(std::string(nm[c]) + " (1 in 4 nodes)");
        }
        const uint64_t groups = (W.n + 15) / 16;
        printf("%s refinement estimate: fixed 14 bits %.1f MB (+%.1f MB u32 group table); per-way unit %.1f MB (+%.1f MB "
               "6 B group table); per-group unit %.1f MB (+%.1f MB)\n",
               street ? "streets" : "interps", fixed_bits / 8e6, groups * 4 / 1e6, group_bits_way / 8e6, groups * 6 / 1e6,
               group_bits_grp / 8e6, groups * 5 / 1e6);
    }
    MF E = map_ro(exact + "/addr_points.e7"), K = map_ro(exact + "/addr_points.kind"), P = map_ro(sorted + "/addr_points.bin");
    const size_t n = P.count<AddrPoint>();
    const E7* e = E.as<E7>();
    const AddrPoint* p = P.as<AddrPoint>();
    std::vector<ResStats> rs(3);
    std::vector<DispStats> ds(3);
#pragma omp parallel
    {
        std::vector<ResStats> lrs(3);
        std::vector<DispStats> lds(3);
#pragma omp for schedule(static)
        for (size_t j = 0; j < n; j++) {
            const int k = K.p[j] > 2 ? 2 : K.p[j];
            lrs[k].add(e[j].lat, e[j].lng);
            if (j % 2 == 0) lds[k].add(e[j].lat, e[j].lng, p[j].lat, p[j].lng);
        }
#pragma omp critical
        for (int c = 0; c < 3; c++) { rs[c].merge(lrs[c]); ds[c].merge(lds[c]); }
    }
    const char* nm[3] = {"addr OSM nodes", "addr bldg centroid", "addr rel centroid"};
    for (int c = 0; c < 3; c++) {
        rs[c].print(nm[c]);
        ds[c].print(std::string(nm[c]) + " (1 in 2)");
    }
}

// ---------------------------------------------------------------- packed ways at an integer key
// groups: per 16 ways u32 nodes2 offset | i32 anchor lat key | i32 anchor lng key
// code (u16 per way): wl | wg << 6 | unit10 << 12 | wf << 13
// nodes2 per way: first node zz(k - anchor) at wf bytes (lat, lng); then (cnt-1) lat values at wl bits and
// (cnt-1) lng values at wg bits: value 1 the first-order delta, then second-order deltas (u32 wrap), divided
// by 10 when unit10.
static const size_t G = 16;
static inline size_t block_size(uint16_t cnt, uint16_t code) {
    if (!cnt) return 0;
    const size_t wl = code & 63, wg = (code >> 6) & 63, wf = (code >> 13) & 7;
    return 2 * wf + ((size_t(cnt) - 1) * (wl + wg) + 7) / 8;
}

static uint16_t encode_way(const int32_t* kl0, const int32_t* kg0, size_t cnt, int32_t al, int32_t ag, bool allow10,
                           std::vector<uint8_t>& blk, std::vector<uint32_t>& kl, std::vector<uint32_t>& kg) {
    const uint32_t zfl = zz(uint32_t(kl0[0]) - uint32_t(al)), zfg = zz(uint32_t(kg0[0]) - uint32_t(ag));
    const int wf = std::max(bytelen(zfl), bytelen(zfg));
    for (int b = 0; b < wf; b++) blk.push_back(uint8_t(zfl >> (8 * b)));
    for (int b = 0; b < wf; b++) blk.push_back(uint8_t(zfg >> (8 * b)));
    kl.resize(cnt);
    kg.resize(cnt);
    for (size_t j = 0; j < cnt; j++) { kl[j] = uint32_t(kl0[j]); kg[j] = uint32_t(kg0[j]); }
    for (size_t j = cnt - 1; j >= 1; j--) { kl[j] -= kl[j - 1]; kg[j] -= kg[j - 1]; }
    for (size_t j = cnt - 1; j >= 2; j--) { kl[j] -= kl[j - 1]; kg[j] -= kg[j - 1]; }
    bool u10 = allow10 && cnt > 1;
    for (size_t j = 1; j < cnt && u10; j++) u10 = int32_t(kl[j]) % 10 == 0 && int32_t(kg[j]) % 10 == 0;
    uint32_t ml = 0, mg = 0;
    for (size_t j = 1; j < cnt; j++) {
        if (u10) { kl[j] = uint32_t(int32_t(kl[j]) / 10); kg[j] = uint32_t(int32_t(kg[j]) / 10); }
        kl[j] = zz(kl[j]);
        kg[j] = zz(kg[j]);
        ml |= kl[j];
        mg |= kg[j];
    }
    const int wl = bitlen(ml), wg = bitlen(mg);
    BitW bw(blk);
    for (size_t j = 1; j < cnt; j++) bw.put(kl[j], wl);
    for (size_t j = 1; j < cnt; j++) bw.put(kg[j], wg);
    bw.flush();
    return uint16_t(wl | (wg << 6) | (u10 ? 1 << 12 : 0) | (wf << 13));
}

// Decodes one way's keys into out (2 * cnt int32: lat, lng interleaved).
static inline void decode_way(const uint8_t* q, uint16_t cnt, uint16_t code, int32_t al, int32_t ag, int32_t* out) {
    const int wl = code & 63, wg = (code >> 6) & 63, wf = (code >> 13) & 7;
    const bool u10 = (code >> 12) & 1;
    uint32_t zl = 0, zg = 0;
    for (int b = 0; b < wf; b++) zl |= uint32_t(q[b]) << (8 * b);
    for (int b = 0; b < wf; b++) zg |= uint32_t(q[wf + b]) << (8 * b);
    const uint8_t* bp = q + 2 * wf;
    uint32_t kl = uint32_t(al) + unzz(zl), kg = uint32_t(ag) + unzz(zg);
    out[0] = int32_t(kl);
    out[1] = int32_t(kg);
    const size_t m = size_t(cnt) - 1;
    uint64_t gl = 0, gg = m * size_t(wl);
    uint32_t dl = 0, dg = 0;
    for (size_t j = 0; j < m; j++) {
        uint32_t vl = unzz(getbits(bp, gl, wl)), vg = unzz(getbits(bp, gg, wg));
        if (u10) { vl *= 10; vg *= 10; }
        gl += wl;
        gg += wg;
        if (j == 0) { dl = vl; dg = vg; } else { dl += vl; dg += vg; }
        kl += dl;
        kg += dg;
        out[2 * (j + 1)] = int32_t(kl);
        out[2 * (j + 1) + 1] = int32_t(kg);
    }
}

struct Buf {
    std::vector<uint8_t> v;
    void u16(uint16_t x) { v.insert(v.end(), (uint8_t*)&x, (uint8_t*)&x + 2); }
    void u32(uint32_t x) { v.insert(v.end(), (uint8_t*)&x, (uint8_t*)&x + 4); }
    void i32(int32_t x) { u32(uint32_t(x)); }
    void pad8() { v.resize(v.size() + 8, 0); }
};

static void encways(const std::string& sorted, const std::string& exact, const std::string& out, bool street) {
    Ways W;
    W.open(sorted + (street ? "/street_ways.bin" : "/interp_ways.bin"), street);
    MF E = map_ro(exact + (street ? "/street_nodes.e7" : "/interp_nodes.e7"));
    const E7* e = E.as<E7>();
    const std::string pre = out + "/" + (street ? "street" : "interp");
    // shared count column
    {
        Buf c;
        for (size_t j = 0; j < W.n; j++) c.u16(W.cnt(j));
        write_file(pre + ".cnt", c.v.data(), c.v.size());
    }
    const char* only = getenv("PREC_VARIANTS");  // e.g. "e6"
    for (int variant = 0; variant < 4; variant++) {
        // 0: e5 base, 1: e7 exact (unit-10 ways), 2: e7 exact without the unit-10 trick, 3: e6 (11 cm)
        const bool e7 = variant == 1 || variant == 2, allow10 = variant == 1;
        const char* vn = variant == 0 ? "e5" : variant == 1 ? "e7" : variant == 2 ? "e7plain" : "e6";
        if (only ? !strstr(only, vn) || (variant == 1 && strstr(only, "e7plain") && !strstr(only, "e7,")) : variant == 3)
            continue;
        auto key = [&](int32_t v) { return e7 ? v : variant == 3 ? e6_of(v) : e5_of(v); };
        Buf groups, code, nodes2;
        std::vector<uint8_t> blk;
        std::vector<uint32_t> kl, kg;
        std::vector<int32_t> al, ag, dec;
        uint64_t u10ways = 0, wfh[5] = {0};
        for (size_t g = 0; g < (W.n + G - 1) / G; g++) {
            int32_t ancl = 0, ancg = 0;
            const size_t j0 = g * G, j1 = std::min(W.n, j0 + G);
            for (size_t j = j0; j < j1; j++)
                if (W.cnt(j)) {
                    const E7 a = e[W.off(j)];
                    ancl = key(a.lat);
                    ancg = key(a.lng);
                    break;
                }
            if (nodes2.v.size() > 0xFFFFFFFFull) die("nodes2 overflow");
            groups.u32(uint32_t(nodes2.v.size()));
            groups.i32(ancl);
            groups.i32(ancg);
            for (size_t j = j0; j < j1; j++) {
                const size_t c = W.cnt(j), o = W.off(j);
                if (!c) { code.u16(0); continue; }
                al.resize(c);
                ag.resize(c);
                for (size_t k = 0; k < c; k++) {
                    al[k] = key(e[o + k].lat);
                    ag[k] = key(e[o + k].lng);
                }
                blk.clear();
                const uint16_t cd = encode_way(al.data(), ag.data(), c, ancl, ancg, allow10, blk, kl, kg);
                if (block_size(uint16_t(c), cd) != blk.size()) die("block size");
                // round-trip check
                blk.resize(blk.size() + 8, 0);
                dec.resize(2 * c);
                decode_way(blk.data(), uint16_t(c), cd, ancl, ancg, dec.data());
                for (size_t k = 0; k < c; k++)
                    if (dec[2 * k] != al[k] || dec[2 * k + 1] != ag[k]) die("%s way %zu round trip", vn, j);
                blk.resize(blk.size() - 8);
                nodes2.v.insert(nodes2.v.end(), blk.begin(), blk.end());
                code.u16(cd);
                u10ways += (cd >> 12) & 1;
                wfh[(cd >> 13) & 7]++;
            }
        }
        nodes2.pad8();
        write_file(pre + "." + vn + ".groups", groups.v.data(), groups.v.size());
        write_file(pre + "." + vn + ".code", code.v.data(), code.v.size());
        write_file(pre + "." + vn + ".nodes2", nodes2.v.data(), nodes2.v.size());
        printf("%s %s: groups %zu B, code %zu B, nodes2 %zu B, total %zu B; unit-10 ways %llu; wf 0..4 %llu %llu %llu %llu %llu\n",
               street ? "street" : "interp", vn, groups.v.size(), code.v.size(), nodes2.v.size(),
               groups.v.size() + code.v.size() + nodes2.v.size(), (unsigned long long)u10ways, (unsigned long long)wfh[0],
               (unsigned long long)wfh[1], (unsigned long long)wfh[2], (unsigned long long)wfh[3],
               (unsigned long long)wfh[4]);
    }
    if (only) return;
    // Refinements against the e5 base.
    // ref14: residual + 50 at 7 bits, lat then lng, node i at bit 14 i; ref14.idx: u32 first node index per group.
    // refA:  per group byte-aligned; per way unit (1: 7+7 bits, 10: (r/10 + 5) at 4+4 bits, 100: nothing);
    //        refA.idx: per group u32 byte offset + u32 unit codes (2 bits per way).
    {
        Buf idx;
        std::vector<uint8_t> bits;
        BitW bw(bits);
        uint64_t node = 0;
        for (size_t j = 0; j < W.n; j++) {
            if (j % G == 0) idx.u32(uint32_t(node));
            const size_t c = W.cnt(j), o = W.off(j);
            for (size_t k = 0; k < c; k++) {
                bw.put(uint32_t(res_of(e[o + k].lat) + 50), 7);
                bw.put(uint32_t(res_of(e[o + k].lng) + 50), 7);
            }
            node += c;
        }
        bw.flush();
        bits.resize(bits.size() + 8, 0);
        write_file(pre + ".ref14.idx", idx.v.data(), idx.v.size());
        write_file(pre + ".ref14", bits.data(), bits.size());
        printf("%s ref14: bits %zu B, idx %zu B\n", street ? "street" : "interp", bits.size(), idx.v.size());
    }
    {
        Buf idx;
        std::vector<uint8_t> bits;
        uint64_t nu[3] = {0, 0, 0};
        for (size_t g = 0; g < (W.n + G - 1) / G; g++) {
            const size_t j0 = g * G, j1 = std::min(W.n, j0 + G);
            idx.u32(uint32_t(bits.size()));
            if (bits.size() > 0xFFFFFFFFull) die("refA overflow");
            uint32_t units = 0;
            BitW bw(bits);
            for (size_t j = j0; j < j1; j++) {
                const size_t c = W.cnt(j), o = W.off(j);
                const int u = c ? way_unit(e + o, c) : 100;
                const uint32_t ui = u == 100 ? 2 : u == 10 ? 1 : 0;
                units |= ui << (2 * (j - j0));
                nu[ui] += c;
                for (size_t k = 0; k < c && u != 100; k++) {
                    const int a = res_of(e[o + k].lat), b = res_of(e[o + k].lng);
                    if (u == 10) { bw.put(uint32_t(a / 10 + 5), 4); bw.put(uint32_t(b / 10 + 5), 4); }
                    else { bw.put(uint32_t(a + 50), 7); bw.put(uint32_t(b + 50), 7); }
                }
            }
            bw.flush();
            idx.u32(units);
        }
        bits.resize(bits.size() + 8, 0);
        write_file(pre + ".refA.idx", idx.v.data(), idx.v.size());
        write_file(pre + ".refA", bits.data(), bits.size());
        printf("%s refA: bits %zu B, idx %zu B; nodes by unit 1/10/100: %llu %llu %llu\n", street ? "street" : "interp",
               bits.size(), idx.v.size(), (unsigned long long)nu[0], (unsigned long long)nu[1], (unsigned long long)nu[2]);
    }
}

// ---------------------------------------------------------------- address points
// planes.<k>: lat b0..b3 planes then lng b0..b3 planes (as gc's 'col' lat/lng region), k = f32 bits | e5 | e7.
// for<B>.<k>: per block of B points a 16 B header (i32 lat min, i32 lng min, u32 byte offset, u8 wl, u8 wg, u16 0)
//             and, at that offset, B (lat - min, lng - min) pairs at wl + wg bits (point i at bit (i % B)(wl + wg)).
//             f32 values use the ordered ulp key (monotone u32) as the integer.
// ref14: point i's residuals (+50) at bits 14 i (lat 7 bits, lng 7 bits).
static inline uint32_t ulp_key(uint32_t b) { return (b & 0x80000000u) ? ~b : (b | 0x80000000u); }
static inline uint32_t ulp_unkey(uint32_t k) { return (k & 0x80000000u) ? (k & 0x7fffffffu) : ~k; }

static void encaddr(const std::string& sorted, const std::string& exact, const std::string& out) {
    MF E = map_ro(exact + "/addr_points.e7"), P = map_ro(sorted + "/addr_points.bin");
    const size_t n = P.count<AddrPoint>();
    const E7* e = E.as<E7>();
    const AddrPoint* p = P.as<AddrPoint>();
    std::vector<uint32_t> kl(n), kg(n);
    const char* only = getenv("PREC_VARIANTS");
    for (int kind = 0; kind < 4; kind++) {
        const char* kn = kind == 0 ? "f32" : kind == 1 ? "e5" : kind == 2 ? "e7" : "e6";
        if (only ? !strstr(only, kn) : kind == 3) continue;
        for (size_t i = 0; i < n; i++) {
            if (kind == 0) { kl[i] = fbits(p[i].lat); kg[i] = fbits(p[i].lng); }
            else if (kind == 1) { kl[i] = uint32_t(e5_of(e[i].lat)); kg[i] = uint32_t(e5_of(e[i].lng)); }
            else if (kind == 3) { kl[i] = uint32_t(e6_of(e[i].lat)); kg[i] = uint32_t(e6_of(e[i].lng)); }
            else { kl[i] = uint32_t(e[i].lat); kg[i] = uint32_t(e[i].lng); }
        }
        {
            std::vector<uint8_t> pl(8 * n);
            for (size_t i = 0; i < n; i++)
                for (int b = 0; b < 4; b++) {
                    pl[b * n + i] = uint8_t(kl[i] >> (8 * b));
                    pl[(4 + b) * n + i] = uint8_t(kg[i] >> (8 * b));
                }
            write_file(out + "/addr.planes." + kn, pl.data(), pl.size());
        }
        for (size_t B : {64, 128, 256}) {
            Buf hdr;
            std::vector<uint8_t> bits;
            // ordered integer view: f32 -> ulp key, ints -> offset binary
            auto ord = [&](uint32_t v) { return kind == 0 ? ulp_key(v) : v ^ 0x80000000u; };
            for (size_t b0 = 0; b0 < n; b0 += B) {
                const size_t b1 = std::min(n, b0 + B);
                uint32_t mnl = ~0u, mng = ~0u, mxl = 0, mxg = 0;
                for (size_t i = b0; i < b1; i++) {
                    mnl = std::min(mnl, ord(kl[i])); mxl = std::max(mxl, ord(kl[i]));
                    mng = std::min(mng, ord(kg[i])); mxg = std::max(mxg, ord(kg[i]));
                }
                const int wl = bitlen(mxl - mnl), wg = bitlen(mxg - mng);
                hdr.u32(mnl);
                hdr.u32(mng);
                hdr.u32(uint32_t(bits.size()));
                hdr.v.push_back(uint8_t(wl));
                hdr.v.push_back(uint8_t(wg));
                hdr.u16(0);
                BitW bw(bits);
                for (size_t i = b0; i < b1; i++) { bw.put(ord(kl[i]) - mnl, wl); bw.put(ord(kg[i]) - mng, wg); }
                bw.flush();
            }
            bits.resize(bits.size() + 8, 0);
            const std::string base = out + "/addr.for" + std::to_string(B) + "." + kn;
            write_file(base + ".hdr", hdr.v.data(), hdr.v.size());
            write_file(base + ".bits", bits.data(), bits.size());
            printf("addr for%zu %s: hdr %zu B bits %zu B total %zu B (%.3f B/point)\n", B, kn, hdr.v.size(), bits.size(),
                   hdr.v.size() + bits.size(), double(hdr.v.size() + bits.size()) / double(n));
        }
    }
    if (only) return;
    std::vector<uint8_t> bits;
    BitW bw(bits);
    for (size_t i = 0; i < n; i++) { bw.put(uint32_t(res_of(e[i].lat) + 50), 7); bw.put(uint32_t(res_of(e[i].lng) + 50), 7); }
    bw.flush();
    bits.resize(bits.size() + 8, 0);
    write_file(out + "/addr.ref14", bits.data(), bits.size());
    printf("addr ref14: %zu B (%.3f B/point)\n", bits.size(), double(bits.size()) / double(n));
}

// ---------------------------------------------------------------- snap (builder order, today's f32 format)
static void snap(const std::string& src, const std::string& dumps, const std::string& out) {
    for (const char* nn : {"street_nodes", "interp_nodes"}) {
        MF E = map_ro(dumps + "/" + nn + ".e7"), F = map_ro(src + "/" + nn + ".bin");
        const size_t n = E.count<E7>();
        if (F.count<NodeCoord>() != n) die("count");
        const E7* e = E.as<E7>();
        const NodeCoord* f = F.as<NodeCoord>();
        std::vector<NodeCoord> o(n);
        uint64_t same = 0;
#pragma omp parallel for reduction(+ : same)
        for (size_t i = 0; i < n; i++) {
            o[i] = {f32_of_e5(e5_of(e[i].lat)), f32_of_e5(e5_of(e[i].lng))};
            same += o[i].lat == f[i].lat && o[i].lng == f[i].lng;
        }
        write_file(out + "/" + nn + ".bin", o.data(), n * 8);
        printf("%s: %zu nodes, unchanged f32 %llu\n", nn, n, (unsigned long long)same);
    }
    MF E = map_ro(dumps + "/addr_points.e7"), P = map_ro(src + "/addr_points.bin");
    const size_t n = P.count<AddrPoint>();
    const E7* e = E.as<E7>();
    std::vector<AddrPoint> o(P.as<AddrPoint>(), P.as<AddrPoint>() + n);
    uint64_t same = 0;
    for (size_t i = 0; i < n; i++) {
        const float a = f32_of_e5(e5_of(e[i].lat)), b = f32_of_e5(e5_of(e[i].lng));
        same += a == o[i].lat && b == o[i].lng;
        o[i].lat = a;
        o[i].lng = b;
    }
    write_file(out + "/addr_points.bin", o.data(), n * 28);
    printf("addr_points: %zu, unchanged f32 %llu\n", n, (unsigned long long)same);
}

// ---------------------------------------------------------------- churn between two sorted days
// Writes, in day-2 order, the f32 (lat, lng) of every node / point a patch has to carry:
//   streets / interps: all nodes of ways whose slot is new; for kept ways whose nodes changed, the nodes outside
//   the longest common head and tail (the patcher keeps an edited way's unchanged head and tail);
//   addr points: points whose slot is new or whose lat / lng changed.
static void churn(const std::string& d1, const std::string& d2, const std::string& m1, const std::string& m2,
                  const std::string& out) {
    for (int fam = 0; fam < 3; fam++) {
        const char* sn = fam == 0 ? "street" : fam == 1 ? "interp" : "addr";
        MF s1 = map_ro(m1 + "/" + sn + ".slot.u32"), s2 = map_ro(m2 + "/" + sn + ".slot.u32");
        const uint32_t* a = s1.as<uint32_t>();
        const uint32_t* b = s2.as<uint32_t>();
        const size_t n1 = s1.count<uint32_t>(), n2 = s2.count<uint32_t>();
        uint32_t mx = 0;
        for (size_t i = 0; i < n1; i++) mx = std::max(mx, a[i]);
        for (size_t i = 0; i < n2; i++) mx = std::max(mx, b[i]);
        std::vector<uint32_t> pos1(size_t(mx) + 1, 0xFFFFFFFFu);
        for (size_t i = 0; i < n1; i++) pos1[a[i]] = uint32_t(i);
        std::vector<NodeCoord> stream;
        uint64_t recs_new = 0, recs_changed = 0, recs_same = 0, nodes_new = 0, nodes_changed_ins = 0, nodes_total = 0;
        if (fam < 2) {
            const bool street = fam == 0;
            Ways W1, W2;
            W1.open(d1 + (street ? "/street_ways.bin" : "/interp_ways.bin"), street);
            W2.open(d2 + (street ? "/street_ways.bin" : "/interp_ways.bin"), street);
            MF N1 = map_ro(d1 + (street ? "/street_nodes.bin" : "/interp_nodes.bin"));
            MF N2 = map_ro(d2 + (street ? "/street_nodes.bin" : "/interp_nodes.bin"));
            const uint64_t* x1 = N1.as<uint64_t>();
            const uint64_t* x2 = N2.as<uint64_t>();
            const NodeCoord* c2 = N2.as<NodeCoord>();
            for (size_t j = 0; j < n2; j++) {
                const size_t c = W2.cnt(j), o = W2.off(j);
                nodes_total += c;
                const uint32_t i = pos1[b[j]];
                if (i == 0xFFFFFFFFu) {
                    recs_new++;
                    nodes_new += c;
                    for (size_t k = 0; k < c; k++) stream.push_back(c2[o + k]);
                    continue;
                }
                const size_t cp = W1.cnt(i), op = W1.off(i);
                if (cp == c && !memcmp(x1 + op, x2 + o, 8 * c)) { recs_same++; continue; }
                recs_changed++;
                size_t h = 0, t = 0;
                while (h < c && h < cp && x1[op + h] == x2[o + h]) h++;
                while (t < c - h && t < cp - h && x1[op + cp - 1 - t] == x2[o + c - 1 - t]) t++;
                for (size_t k = h; k < c - t; k++) stream.push_back(c2[o + k]);
                nodes_changed_ins += c - t - h;
            }
        } else {
            MF P1 = map_ro(d1 + "/addr_points.bin"), P2 = map_ro(d2 + "/addr_points.bin");
            const AddrPoint* p1 = P1.as<AddrPoint>();
            const AddrPoint* p2 = P2.as<AddrPoint>();
            for (size_t j = 0; j < n2; j++) {
                nodes_total++;
                const uint32_t i = pos1[b[j]];
                if (i == 0xFFFFFFFFu) {
                    recs_new++;
                    nodes_new++;
                    stream.push_back({p2[j].lat, p2[j].lng});
                    continue;
                }
                if (fbits(p1[i].lat) == fbits(p2[j].lat) && fbits(p1[i].lng) == fbits(p2[j].lng)) { recs_same++; continue; }
                recs_changed++;
                nodes_changed_ins++;
                stream.push_back({p2[j].lat, p2[j].lng});
            }
        }
        printf("%s: day1 %zu day2 %zu records; new %llu, coordinates changed %llu, same %llu; nodes/points day2 %llu, carried "
               "%zu (new records %llu, changed records %llu)\n",
               sn, n1, n2, (unsigned long long)recs_new, (unsigned long long)recs_changed, (unsigned long long)recs_same,
               (unsigned long long)nodes_total, stream.size(), (unsigned long long)nodes_new,
               (unsigned long long)nodes_changed_ins);
        write_file(out + "/" + sn + ".f32", stream.data(), stream.size() * 8);
    }
}

// ---------------------------------------------------------------- synthesized exact values
// A uniformly drawn 1e-7 integer among those whose f32 is v (the low digits f32 dropped are close to uniform).
// A value that is not the f32 of any 1e-7 integer (a building centroid, the f32 of a double) draws from the
// integers within half an f32 ulp (+ half a 1e-7 step) of it instead.
static uint64_t g_no_preimage = 0;
static int32_t synth_e7(float v, std::mt19937_64& rng) {
    const double c = std::nearbyint(double(v) * 1e7);
    int64_t lo = int64_t(c), hi = int64_t(c);
    if (f32_of(int32_t(c)) != v) {
        g_no_preimage++;
        const double half = (double(std::nextafter(std::fabs(v), INFINITY)) - std::fabs(double(v))) / 2 * 1e7 + 0.5;
        lo = int64_t(std::ceil(double(v) * 1e7 - half));
        hi = int64_t(std::floor(double(v) * 1e7 + half));
        return int32_t(lo + int64_t(rng() % uint64_t(hi - lo + 1)));
    }
    while (f32_of(int32_t(lo - 1)) == v) lo--;
    while (f32_of(int32_t(hi + 1)) == v) hi++;
    return int32_t(lo + int64_t(rng() % uint64_t(hi - lo + 1)));
}

// Writes the streams a patch would carry for the same nodes in each representation:
//   .f32 (input, 8 B/node) | .e5 (i32 pairs) | .e7 (i32 pairs) | .res8 (u8 r+50 pairs) | .res14 (14-bit packed)
static void write_streams(const std::string& pre, const std::vector<E7>& ex) {
    std::vector<E7> k5(ex.size());
    std::vector<uint8_t> r8(2 * ex.size()), r14;
    BitW bw(r14);
    for (size_t i = 0; i < ex.size(); i++) {
        k5[i] = {e5_of(ex[i].lat), e5_of(ex[i].lng)};
        r8[2 * i] = uint8_t(res_of(ex[i].lat) + 50);
        r8[2 * i + 1] = uint8_t(res_of(ex[i].lng) + 50);
        bw.put(r8[2 * i], 7);
        bw.put(r8[2 * i + 1], 7);
    }
    bw.flush();
    write_file(pre + ".e5", k5.data(), k5.size() * 8);
    write_file(pre + ".e7", ex.data(), ex.size() * 8);
    write_file(pre + ".res8", r8.data(), r8.size());
    write_file(pre + ".res14", r14.data(), r14.size());
}

static void synth(const std::string& in, const std::string& pre, uint64_t seed) {
    MF F = map_ro(in);
    const size_t n = F.count<NodeCoord>();
    const NodeCoord* f = F.as<NodeCoord>();
    std::mt19937_64 rng(seed);
    std::vector<E7> ex(n);
    for (size_t i = 0; i < n; i++) ex[i] = {synth_e7(f[i].lat, rng), synth_e7(f[i].lng, rng)};
    write_streams(pre, ex);
    printf("synth %s: %zu nodes (%llu values without an exact 1e-7 preimage)\n", in.c_str(), n,
           (unsigned long long)g_no_preimage);
}

// Validation: runs of real planet nodes (whole ways from random positions, like a patch's inserted ways) with
// their real exact values, and the same runs with exact values synthesized from their f32.
static void synthval(const std::string& sorted, const std::string& exact, const std::string& out) {
    Ways W;
    W.open(sorted + "/street_ways.bin", true);
    MF E = map_ro(exact + "/street_nodes.e7"), F = map_ro(sorted + "/street_nodes.bin");
    const E7* e = E.as<E7>();
    const NodeCoord* f = F.as<NodeCoord>();
    std::mt19937_64 rng(42);
    std::vector<E7> real, syn;
    std::vector<NodeCoord> fl;
    while (real.size() < 220000) {
        size_t j = rng() % W.n;
        for (size_t r = 0; r < 8 && j < W.n; r++, j++) {
            const size_t c = W.cnt(j), o = W.off(j);
            for (size_t k = 0; k < c; k++) {
                real.push_back(e[o + k]);
                fl.push_back(f[o + k]);
                syn.push_back({synth_e7(f[o + k].lat, rng), synth_e7(f[o + k].lng, rng)});
            }
        }
    }
    write_file(out + "/val.f32", fl.data(), fl.size() * 8);
    write_streams(out + "/val_real", real);
    write_streams(out + "/val_syn", syn);
    printf("synthval: %zu nodes\n", real.size());
    // address points: runs of 16 consecutive points (sorted order) from random positions
    MF AE = map_ro(exact + "/addr_points.e7"), AP = map_ro(sorted + "/addr_points.bin");
    const E7* ae = AE.as<E7>();
    const AddrPoint* ap = AP.as<AddrPoint>();
    const size_t n = AP.count<AddrPoint>();
    real.clear();
    syn.clear();
    fl.clear();
    while (real.size() < 50000) {
        size_t i = rng() % (n - 16);
        for (size_t r = 0; r < 16; r++, i++) {
            real.push_back(ae[i]);
            fl.push_back({ap[i].lat, ap[i].lng});
            syn.push_back({synth_e7(ap[i].lat, rng), synth_e7(ap[i].lng, rng)});
        }
    }
    write_file(out + "/val_addr.f32", fl.data(), fl.size() * 8);
    write_streams(out + "/val_addr_real", real);
    write_streams(out + "/val_addr_syn", syn);
    printf("synthval addr: %zu points\n", real.size());
}

int main(int argc, char** argv) {
    if (argc < 2) die("usage: see source");
    const std::string cmd = argv[1];
    auto need = [&](int k) { if (argc < k + 2) die("%s: %d arguments", cmd.c_str(), k); };
    if (cmd == "permute") { need(5); permute(argv[2], argv[3], argv[4], argv[5], argv[6]); }
    else if (cmd == "stats") { need(2); stats(argv[2], argv[3]); }
    else if (cmd == "encways") { need(4); encways(argv[2], argv[3], argv[4], std::string(argv[5]) == "street"); }
    else if (cmd == "encaddr") { need(3); encaddr(argv[2], argv[3], argv[4]); }
    else if (cmd == "snap") { need(3); snap(argv[2], argv[3], argv[4]); }
    else if (cmd == "churn") { need(5); churn(argv[2], argv[3], argv[4], argv[5], argv[6]); }
    else if (cmd == "synth") { need(3); synth(argv[2], argv[3], strtoull(argv[4], nullptr, 10)); }
    else if (cmd == "synthval") { need(3); synthval(argv[2], argv[3], argv[4]); }
    else die("unknown command %s", cmd.c_str());
    return 0;
}
