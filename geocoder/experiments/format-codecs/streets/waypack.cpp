// "Packed ways" on-disk layout for street / interp ways (lossless, random access per way).
//
//   <p>.ways2  per way:  street: cnt u16 | code u16 | name u32                      (8 B)
//                        interp: cnt u16 | code u16 | street u32 | start u32 | end u32 | interpolation u8 (17 B)
//   <p>.groups per G ways: block byte offset u32 | anchor lat f32 bits | anchor lng f32 bits (12 B)
//                        anchor = first node of the first non-empty way of the group
//   <p>.nodes2 per way block:
//        first node: zigzag(ulp_key(v) - ulp_key(anchor)) for lat then lng, wf bytes each
//        then (cnt-1) lat values (wl bits each) and (cnt-1) lng values (wg bits each),
//        LSB-first bit stream, byte aligned at the end of the block; value 1 is the
//        first-order delta, values 2.. are second-order deltas of the integer key
//        (hybrid fixed-point key when every value of the way round-trips, else ulp key)
//   code: bits 0-5 wl, 6-11 wg, 12 hybrid, 13-15 wf (0..4)
//   block size = 2*wf + ceil((cnt-1)*(wl+wg)/8), so way i's offset is the group offset
//   plus the sizes of the earlier ways of its group (read from their 8-byte headers).
//
//   waypack enc   <stride> <ways.bin> <nodes.bin> <G> <out_prefix> [sample]
//   waypack bench <stride> <ways.bin> <nodes.bin> <G> <out_prefix> <n_ways>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <string>
#include <vector>
#include <random>
#include <chrono>
#include <algorithm>
#include <stdexcept>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

struct Map {
    const uint8_t* p = nullptr; size_t n = 0;
    explicit Map(const std::string& path) {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("open " + path);
        struct stat st; fstat(fd, &st); n = st.st_size;
        p = (const uint8_t*)mmap(nullptr, n ? n : 1, PROT_READ, MAP_SHARED, fd, 0);
        close(fd);
    }
};
static inline uint32_t rd32(const uint8_t* p) { uint32_t v; memcpy(&v, p, 4); return v; }
static inline uint16_t rd16(const uint8_t* p) { uint16_t v; memcpy(&v, p, 2); return v; }
static inline uint64_t rd64(const uint8_t* p) { uint64_t v; memcpy(&v, p, 8); return v; }
static inline void wr32(uint8_t* p, uint32_t v) { memcpy(p, &v, 4); }
static inline void wr16(uint8_t* p, uint16_t v) { memcpy(p, &v, 2); }

static inline uint32_t ulp_key(uint32_t b) { return (b & 0x80000000u) ? ~b : (b | 0x80000000u); }
static inline uint32_t ulp_unkey(uint32_t k) { return (k & 0x80000000u) ? (k & 0x7fffffffu) : ~k; }
static constexpr uint32_t ONE_BITS = 0x3f800000u, TEN_MILLION = 10000000u;
static inline uint32_t hyb_decode(uint32_t k) {
    int32_t s = (int32_t)k;
    uint32_t sign = s < 0 ? 0x80000000u : 0;
    uint32_t mag = s < 0 ? (uint32_t)(-(int64_t)s) : (uint32_t)s;
    if (mag >= TEN_MILLION) return (mag - TEN_MILLION + ONE_BITS) | sign;
    float f = (float)((double)mag / 1e7);
    uint32_t b; memcpy(&b, &f, 4);
    return b | sign;
}
static inline bool hyb_key(uint32_t b, uint32_t& k) {
    uint32_t a = b & 0x7fffffffu, mag;
    if (a >= ONE_BITS) {
        uint64_t m = (uint64_t)a - ONE_BITS + TEN_MILLION;
        if (m > 0x7fffffffu) return false;
        mag = (uint32_t)m;
    } else {
        float f; memcpy(&f, &a, 4);
        double n = std::nearbyint((double)f * 1e7);
        if (n >= TEN_MILLION) return false;
        mag = (uint32_t)n;
    }
    if ((b >> 31) && mag == 0) return false;
    k = (b >> 31) ? (uint32_t)(-(int64_t)mag) : mag;
    return hyb_decode(k) == b;
}
static inline uint32_t zz(uint32_t d) { return (d << 1) ^ (uint32_t)((int32_t)d >> 31); }
static inline uint32_t unzz(uint32_t z) { return (z >> 1) ^ (uint32_t)(-(int32_t)(z & 1)); }
static inline int bitlen(uint32_t v) { return v ? 32 - __builtin_clz(v) : 0; }
static inline int bytelen(uint32_t v) { return (bitlen(v) + 7) / 8; }

static inline size_t block_size(uint16_t cnt, uint16_t code) {
    if (!cnt) return 0;
    const size_t wl = code & 63, wg = (code >> 6) & 63, wf = (code >> 13) & 7;
    return 2 * wf + ((size_t)(cnt - 1) * (wl + wg) + 7) / 8;
}

struct Layout { size_t in_stride, out_stride; };
static Layout layout_for(size_t stride) { return stride == 12 ? Layout{12, 8} : Layout{24, 17}; }

// header of way i in the packed ways file
static inline void hdr(const uint8_t* ways2, size_t os, size_t i, uint16_t& cnt, uint16_t& code) {
    cnt = rd16(ways2 + i * os); code = rd16(ways2 + i * os + 2);
}

// Decode way i (random access). Returns node count; fills out (2 u32 per node).
static inline size_t decode(const uint8_t* ways2, size_t os, const uint8_t* groups, const uint8_t* blocks,
                            size_t G, size_t i, uint32_t* out, const uint32_t* known_off = nullptr) {
    const size_t g = i / G;
    const uint8_t* gr = groups + 12 * g;
    size_t off;
    if (known_off) off = known_off[i];
    else {
        off = rd32(gr);
        for (size_t j = g * G; j < i; j++) { uint16_t c, k; hdr(ways2, os, j, c, k); off += block_size(c, k); }
    }
    uint16_t cnt, code; hdr(ways2, os, i, cnt, code);
    if (!cnt) return 0;
    const int wl = code & 63, wg = (code >> 6) & 63, wf = (code >> 13) & 7;
    const bool hyb = (code >> 12) & 1;
    const uint8_t* p = blocks + off;
    uint32_t zl = 0, zgv = 0;
    for (int b = 0; b < wf; b++) zl |= (uint32_t)p[b] << (8 * b);
    for (int b = 0; b < wf; b++) zgv |= (uint32_t)p[wf + b] << (8 * b);
    p += 2 * wf;
    const uint32_t fl = ulp_unkey(ulp_key(rd32(gr + 4)) + unzz(zl));
    const uint32_t fg = ulp_unkey(ulp_key(rd32(gr + 8)) + unzz(zgv));
    out[0] = fl; out[1] = fg;
    if (cnt == 1) return 1;
    uint32_t kl, kg;
    if (hyb) { hyb_key(fl, kl); hyb_key(fg, kg); } else { kl = ulp_key(fl); kg = ulp_key(fg); }
    const size_t m = cnt - 1;
    const uint64_t mlk = wl == 32 ? 0xffffffffull : ((1ull << wl) - 1);
    const uint64_t mgk = wg == 32 ? 0xffffffffull : ((1ull << wg) - 1);
    size_t gl = 0, gg = m * wl;
    // pass 1: integer keys (two independent recurrences); pass 2: keys -> f32 bits
    uint32_t dl = unzz((uint32_t)((rd64(p) & mlk))), dg = unzz((uint32_t)((rd64(p + (gg >> 3)) >> (gg & 7)) & mgk));
    gl += wl; gg += wg;
    kl += dl; kg += dg;
    out[2] = kl; out[3] = kg;
    for (size_t j = 1; j < m; j++) {
        dl += unzz((uint32_t)((rd64(p + (gl >> 3)) >> (gl & 7)) & mlk));
        dg += unzz((uint32_t)((rd64(p + (gg >> 3)) >> (gg & 7)) & mgk));
        gl += wl; gg += wg;
        kl += dl; kg += dg;
        out[2 * (j + 1)] = kl; out[2 * (j + 1) + 1] = kg;
    }
    uint32_t* o = out + 2;
    const size_t nv = 2 * m;
    if (hyb) for (size_t j = 0; j < nv; j++) o[j] = hyb_decode(o[j]);
    else for (size_t j = 0; j < nv; j++) o[j] = ulp_unkey(o[j]);
    return cnt;
}

static void write_file(const std::string& path, const void* p, size_t n) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("write " + path);
    if (n && fwrite(p, 1, n, f) != n) throw std::runtime_error("short write " + path);
    fclose(f);
}

int main(int argc, char** argv) {
    const std::string mode = argv[1];
    const size_t S = std::stoul(argv[2]);
    Map ways(argv[3]), nodes(argv[4]);
    const size_t G = std::stoul(argv[5]);
    const std::string pre = argv[6];
    const Layout L = layout_for(S);
    const size_t W = ways.n / S, N = nodes.n / 8;
    const uint32_t* nb = (const uint32_t*)nodes.p;

    if (mode == "enc") {
        std::vector<uint8_t> w2(W * L.out_stride), groups(12 * ((W + G - 1) / G)), blocks;
        blocks.reserve(nodes.n / 2);
        std::vector<uint32_t> kl, kg;
        size_t hyb_ways = 0, wf_hist[5] = {0};
        for (size_t g = 0; g * G < W; g++) {
            const size_t w0 = g * G, w1 = std::min(W, w0 + G);
            uint32_t al = 0, ag = 0;
            for (size_t w = w0; w < w1; w++) if (rd16(ways.p + w * S + 4)) {
                uint32_t o = rd32(ways.p + w * S); al = nb[2 * (size_t)o]; ag = nb[2 * (size_t)o + 1]; break;
            }
            if (blocks.size() > 0xffffffffull) throw std::runtime_error("u32 offset overflow");
            wr32(&groups[12 * g], (uint32_t)blocks.size()); wr32(&groups[12 * g + 4], al); wr32(&groups[12 * g + 8], ag);
            for (size_t w = w0; w < w1; w++) {
                const uint8_t* r = ways.p + w * S;
                uint8_t* o2 = &w2[w * L.out_stride];
                const uint32_t off = rd32(r); const uint16_t cnt = rd16(r + 4);
                if (S == 12) memcpy(o2 + 4, r + 8, 4);
                else { memcpy(o2 + 4, r + 8, 12); o2[16] = r[20]; }
                uint16_t code = 0;
                if (cnt) {
                    const uint32_t* bits = nb + 2 * (size_t)off;
                    uint32_t zfl = zz(ulp_key(bits[0]) - ulp_key(al)), zfg = zz(ulp_key(bits[1]) - ulp_key(ag));
                    int wf = std::max(bytelen(zfl), bytelen(zfg));
                    wf_hist[wf]++;
                    for (int b = 0; b < wf; b++) blocks.push_back((uint8_t)(zfl >> (8 * b)));
                    for (int b = 0; b < wf; b++) blocks.push_back((uint8_t)(zfg >> (8 * b)));
                    kl.resize(cnt); kg.resize(cnt);
                    bool hyb = true;
                    for (size_t j = 0; j < cnt && hyb; j++) hyb = hyb_key(bits[2 * j], kl[j]) && hyb_key(bits[2 * j + 1], kg[j]);
                    if (!hyb) for (size_t j = 0; j < cnt; j++) { kl[j] = ulp_key(bits[2 * j]); kg[j] = ulp_key(bits[2 * j + 1]); }
                    hyb_ways += hyb;
                    for (size_t j = cnt - 1; j >= 1; j--) { kl[j] -= kl[j - 1]; kg[j] -= kg[j - 1]; }
                    for (size_t j = cnt - 1; j >= 2; j--) { kl[j] -= kl[j - 1]; kg[j] -= kg[j - 1]; }
                    uint32_t ml = 0, mg = 0;
                    for (size_t j = 1; j < cnt; j++) { kl[j] = zz(kl[j]); kg[j] = zz(kg[j]); ml |= kl[j]; mg |= kg[j]; }
                    const int wl = bitlen(ml), wg = bitlen(mg);
                    const size_t m = cnt - 1, base = blocks.size();
                    blocks.resize(base + (m * (wl + wg) + 7) / 8, 0);
                    uint8_t* bo = &blocks[base];
                    size_t pos = 0;
                    auto put = [&](uint32_t v, int wdt) {
                        for (int b = 0; b < wdt; b++, pos++) if ((v >> b) & 1) bo[pos >> 3] |= (uint8_t)(1u << (pos & 7));
                    };
                    for (size_t j = 1; j < cnt; j++) put(kl[j], wl);
                    for (size_t j = 1; j < cnt; j++) put(kg[j], wg);
                    code = (uint16_t)(wl | (wg << 6) | (hyb ? 1 << 12 : 0) | (wf << 13));
                }
                wr16(o2, cnt); wr16(o2 + 2, code);
            }
        }
        const size_t payload = blocks.size();
        blocks.resize(payload + 8, 0);
        // verify every way through the random-access decode path
        std::vector<uint32_t> buf(2 * 65536);
        size_t bad = 0, checked = 0;
        for (size_t w = 0; w < W; w++) {
            const uint8_t* r = ways.p + w * S;
            const uint16_t cnt = rd16(r + 4);
            size_t n = decode(w2.data(), L.out_stride, groups.data(), blocks.data(), G, w, buf.data());
            if (n != cnt || (cnt && memcmp(buf.data(), nb + 2 * (size_t)rd32(r), 8 * (size_t)cnt) != 0)) bad++;
            const uint8_t* o2 = &w2[w * L.out_stride];
            if (S == 12 ? memcmp(o2 + 4, r + 8, 4) != 0 : (memcmp(o2 + 4, r + 8, 12) != 0 || o2[16] != r[20])) bad++;
            checked += cnt;
        }
        // the dropped bytes of today's records: node_offset (== prefix sum, checked
        // here) and the zero padding
        size_t expect = 0, offs_bad = 0, pad_bad = 0;
        for (size_t w = 0; w < W; w++) {
            const uint8_t* r = ways.p + w * S;
            uint16_t c = rd16(r + 4);
            if (c && rd32(r) != expect) offs_bad++;
            if (!c && rd32(r) != 0) offs_bad++;
            expect += c;
            if (rd16(r + 6) || (S == 24 && (r[21] | r[22] | r[23]))) pad_bad++;
        }
        if (!getenv("NOFULL")) {
            write_file(pre + ".ways2", w2.data(), w2.size());
            write_file(pre + ".groups", groups.data(), groups.size());
            write_file(pre + ".nodes2", blocks.data(), payload);
        }
        printf("%s\tG %zu\tways %zu\tnodes %zu\tways2 %zu\tgroups %zu\tnodes2 %zu\ttotal %zu\ttoday %zu\tverified_nodes %zu\tbad %zu\tdropped_offsets_not_prefix_sum %zu\tnonzero_pad %zu\thyb_ways %zu\twf_hist %zu/%zu/%zu/%zu/%zu\n",
               pre.c_str(), G, W, N, w2.size(), groups.size(), payload, w2.size() + groups.size() + payload,
               ways.n + nodes.n, checked, bad, offs_bad, pad_bad, hyb_ways, wf_hist[0], wf_hist[1], wf_hist[2], wf_hist[3], wf_hist[4]);
        if (argc > 7) {
            // sample: 8 slices x 0.75% of ways (rounded to whole groups): ways2 + groups + nodes2 concatenated per file
            size_t per = (size_t)(W * 0.06 / 8) / G * G;
            std::vector<uint8_t> sw, sg, sn, tw, tn;
            for (int s = 0; s < 8; s++) {
                size_t w0 = ((size_t)((double)W * (s + 0.5) / 8) - per / 2) / G * G, w1 = w0 + per;
                sw.insert(sw.end(), w2.begin() + w0 * L.out_stride, w2.begin() + w1 * L.out_stride);
                sg.insert(sg.end(), groups.begin() + 12 * (w0 / G), groups.begin() + 12 * (w1 / G));
                size_t b0 = rd32(&groups[12 * (w0 / G)]), b1 = w1 < W ? rd32(&groups[12 * (w1 / G)]) : payload;
                sn.insert(sn.end(), blocks.begin() + b0, blocks.begin() + b1);
                tw.insert(tw.end(), ways.p + w0 * S, ways.p + w1 * S);
                size_t o0 = rd32(ways.p + w0 * S), o1 = w1 < W ? rd32(ways.p + w1 * S) : N;
                tn.insert(tn.end(), nodes.p + 8 * o0, nodes.p + 8 * o1);
            }
            write_file(pre + ".ways2.sample", sw.data(), sw.size());
            write_file(pre + ".groups.sample", sg.data(), sg.size());
            write_file(pre + ".nodes2.sample", sn.data(), sn.size());
            write_file(pre + ".todayways.sample", tw.data(), tw.size());
            write_file(pre + ".todaynodes.sample", tn.data(), tn.size());
        }
        return bad || offs_bad || pad_bad ? 1 : 0;
    }

    if (mode == "bench") {
        Map w2(pre + ".ways2"), gr(pre + ".groups"), bl(pre + ".nodes2");
        std::vector<uint8_t> blocks(bl.p, bl.p + bl.n); blocks.resize(bl.n + 8, 0);
        const size_t want = std::stoul(argv[7]);
        std::mt19937_64 rng(42);
        std::vector<uint32_t> ids;
        // HOT=k: k distinct random ways repeated up to `want` lookups (cache-resident: pure decode cost)
        const size_t hot = getenv("HOT") ? std::stoul(getenv("HOT")) : 0;
        while (ids.size() < (hot ? hot : want)) { uint32_t w = rng() % W; if (rd16(ways.p + (size_t)w * S + 4) >= 2) ids.push_back(w); }
        if (hot) { size_t k = 0; while (ids.size() < want) ids.push_back(ids[k++]); }
        volatile uint64_t sink = 0;
        for (size_t i = 0; i < nodes.n; i += 4096) sink += nodes.p[i];
        for (size_t i = 0; i < w2.n; i += 4096) sink += w2.p[i];
        std::vector<uint32_t> buf(2 * 65536);
        auto segdist = [](const float* c, size_t n, double lat, double lng) {
            double best = 1e300, cs = cos(lat * M_PI / 180);
            for (size_t i = 0; i + 1 < n; i++) {
                double ax = c[2*i], ay = c[2*i+1], bx = c[2*i+2], by = c[2*i+3];
                double dx = bx - ax, dy = (by - ay) * cs, px = lat - ax, py = (lng - ay) * cs;
                double l2 = dx*dx + dy*dy, t = l2 > 0 ? (px*dx + py*dy) / l2 : 0;
                t = t < 0 ? 0 : t > 1 ? 1 : t;
                double ex = px - t*dx, ey = py - t*dy, d = ex*ex + ey*ey;
                if (d < best) best = d;
            }
            return best;
        };
        std::vector<uint32_t> offs(W);
        { size_t o = 0; for (size_t i = 0; i < W; i++) { if (i % G == 0) o = rd32(gr.p + 12 * (i / G)); offs[i] = (uint32_t)o;
            uint16_t c, k; hdr(w2.p, L.out_stride, i, c, k); o += block_size(c, k); } }
        double acc = 0; size_t total = 0;
        for (int rep = 0; rep < 3; rep++) {
            auto t0 = std::chrono::steady_clock::now();
            total = 0;
            for (uint32_t w : ids) {
                const uint8_t* r = ways.p + (size_t)w * S;
                uint32_t off = rd32(r); uint16_t cnt = rd16(r + 4);
                const float* c = (const float*)(nodes.p + 8 * (size_t)off);
                acc += segdist(c, cnt, c[0] + 1e-4, c[1] + 1e-4);
                total += cnt;
            }
            auto t1 = std::chrono::steady_clock::now();
            for (uint32_t w : ids) {
                size_t n = decode(w2.p, L.out_stride, gr.p, blocks.data(), G, w, buf.data());
                const float* c = (const float*)buf.data();
                acc += segdist(c, n, c[0] + 1e-4, c[1] + 1e-4);
            }
            auto t2 = std::chrono::steady_clock::now();
            for (uint32_t w : ids) acc += buf[decode(w2.p, L.out_stride, gr.p, blocks.data(), G, w, buf.data())];
            auto t3 = std::chrono::steady_clock::now();
            for (uint32_t w : ids) acc += buf[decode(w2.p, L.out_stride, gr.p, blocks.data(), G, w, buf.data(), offs.data())];
            auto t4 = std::chrono::steady_clock::now();
            double e = std::chrono::duration<double, std::nano>(t4 - t3).count();
            if (rep == 2) printf("  decode-only with per-way offsets (no group scan) %.2f ns/node (%.1f ns/way)\n", e / total, e / ids.size());
            double a = std::chrono::duration<double, std::nano>(t1 - t0).count();
            double b = std::chrono::duration<double, std::nano>(t2 - t1).count();
            double c = std::chrono::duration<double, std::nano>(t3 - t2).count();
            if (rep == 2)
                printf("%s G=%zu\tways %zu\tnodes %zu\ttoday+dist %.2f ns/node (%.1f ns/way)\tpacked+dist %.2f ns/node (%.1f ns/way)\tdecode-only %.2f ns/node (%.1f ns/way)\n",
                       pre.c_str(), G, ids.size(), total, a / total, a / ids.size(), b / total, b / ids.size(), c / total, c / ids.size());
        }
        fprintf(stderr, "%g %llu\n", acc, (unsigned long long)sink);
        return 0;
    }
    return 2;
}
