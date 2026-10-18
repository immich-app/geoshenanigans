// Lossless per-way re-encodings of street_nodes.bin / interp_nodes.bin.
//
// Every way keeps one contiguous block, addressed by a byte offset that
// replaces node_offset in the way record; the 2-byte padding after
// node_count carries a per-way code (widths + mapping). The first node is
// stored as raw f32 bits; the rest as deltas of an integer key derived from
// the f32 bit pattern, so decoding reproduces the identical bits.
//
//   nodeenc enc <stride> <ways.bin> <nodes.bin> <variant> <out_dir> [sample_slices sample_frac]
//     stride 12 = WayHeader (street), 24 = InterpWay
//     writes out_dir/<variant>.nodes, out_dir/<variant>.ways, verifies the
//     whole file decodes bit-identically, prints sizes; with sample args also
//     writes <variant>.nodes.sample (and today.sample) over the same ways.
//   nodeenc bench <stride> <ways.bin> <nodes.bin> <variant> <out_dir> <n_ways>
//     random-way decode + segment-distance loop, today vs variant, ns/node.
//
// variants: <order>-<layout>-<mapping>
//   order  : d1 (first-order delta) | d2 (delta of delta)
//   layout : vi (LEB128 zigzag, lat/lng interleaved) | vs (LEB128, lat run then lng run)
//            fi (per-way byte width 0..4, interleaved) | fp (per-way byte width, byte planes)
//            bp (per-way bit width 0..32, lat bits then lng bits)
//   mapping: ulp (orderable f32 bits) | hyb (1e-7 fixed point below |v|<1, ulps above;
//            ways where any value does not round-trip fall back to ulp)
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <string>
#include <vector>
#include <chrono>
#include <random>
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

// ---------------------------------------------------------------- mappings
static inline uint32_t ulp_key(uint32_t b) { return (b & 0x80000000u) ? ~b : (b | 0x80000000u); }
static inline uint32_t ulp_unkey(uint32_t k) { return (k & 0x80000000u) ? (k & 0x7fffffffu) : ~k; }

static constexpr uint32_t ONE_BITS = 0x3f800000u;
static constexpr uint32_t TEN_MILLION = 10000000u;
static inline uint32_t hyb_decode(uint32_t k) {
    int32_t s = (int32_t)k;
    uint32_t sign = s < 0 ? 0x80000000u : 0;
    uint32_t mag = s < 0 ? (uint32_t)(-(int64_t)s) : (uint32_t)s;
    if (mag >= TEN_MILLION) return (mag - TEN_MILLION + ONE_BITS) | sign;
    float f = (float)((double)mag / 1e7);
    uint32_t b; memcpy(&b, &f, 4);
    return b | sign;
}
// false when the value has no hybrid key (decode would not reproduce it)
static inline bool hyb_key(uint32_t b, uint32_t& k) {
    uint32_t a = b & 0x7fffffffu;
    uint32_t mag;
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
    if ((b >> 31) && mag == 0) return false;  // -0.0
    k = (b >> 31) ? (uint32_t)(-(int64_t)mag) : mag;
    return hyb_decode(k) == b;
}

static inline uint32_t zz(uint32_t d) { return (d << 1) ^ (uint32_t)((int32_t)d >> 31); }
static inline uint32_t unzz(uint32_t z) { return (z >> 1) ^ (uint32_t)(-(int32_t)(z & 1)); }
static inline int bitlen(uint32_t v) { return v ? 32 - __builtin_clz(v) : 0; }
static inline int bytelen(uint32_t v) { return (bitlen(v) + 7) / 8; }

// anchor > 0: the first node is a LEB128 zigzag delta (ulp keys) from the
// first node of the first way in its group of `anchor` ways (anchors file).
struct Variant { int order; std::string layout; bool hyb; int anchor; };
static Variant parse_variant(const std::string& s) {
    Variant v;
    std::vector<std::string> parts;
    size_t st = 0;
    while (true) { size_t e = s.find('-', st); parts.push_back(s.substr(st, e - st)); if (e == std::string::npos) break; st = e + 1; }
    v.order = parts[0] == "d2" ? 2 : 1;
    v.layout = parts[1];
    v.hyb = parts[2] == "hyb";
    v.anchor = parts.size() > 3 ? atoi(parts[3].c_str() + 1) : 0;
    return v;
}

// code: bits 0-5 lat width, 6-11 lng width, 12 hybrid mapping used
static inline uint16_t mkcode(int wl, int wg, bool hyb) { return (uint16_t)(wl | (wg << 6) | (hyb ? 1 << 12 : 0)); }

static inline void put_varint(std::vector<uint8_t>& out, uint32_t v) {
    while (v >= 0x80) { out.push_back((uint8_t)(v | 0x80)); v >>= 7; }
    out.push_back((uint8_t)v);
}
static inline uint32_t get_varint(const uint8_t*& p) {
    uint32_t v = 0; int s = 0;
    while (true) { uint8_t c = *p++; v |= (uint32_t)(c & 0x7f) << s; if (!(c & 0x80)) return v; s += 7; }
}

// Encode one way (n >= 1 nodes, bits[2*i], bits[2*i+1] = lat, lng) into out.
static uint16_t encode_way(const Variant& V, const uint32_t* bits, size_t n,
                           std::vector<uint8_t>& out, std::vector<uint32_t>& zl, std::vector<uint32_t>& zg,
                           const uint32_t* anc) {
    bool hyb = false;
    std::vector<uint32_t>& kl = zl; std::vector<uint32_t>& kg = zg;  // reused buffers
    kl.resize(n); kg.resize(n);
    if (V.hyb) {
        hyb = true;
        for (size_t i = 0; i < n && hyb; i++)
            hyb = hyb_key(bits[2 * i], kl[i]) && hyb_key(bits[2 * i + 1], kg[i]);
    }
    if (!hyb) for (size_t i = 0; i < n; i++) { kl[i] = ulp_key(bits[2 * i]); kg[i] = ulp_key(bits[2 * i + 1]); }
    if (anc) {
        put_varint(out, zz(ulp_key(bits[0]) - ulp_key(anc[0])));
        put_varint(out, zz(ulp_key(bits[1]) - ulp_key(anc[1])));
    } else {
        size_t at = out.size();
        out.resize(at + 8);
        wr32(&out[at], bits[0]); wr32(&out[at + 4], bits[1]);
    }
    if (n == 1) return mkcode(0, 0, hyb);
    // deltas in place (from the back), then optional second order, then zigzag
    for (size_t i = n - 1; i >= 1; i--) { kl[i] -= kl[i - 1]; kg[i] -= kg[i - 1]; }
    if (V.order == 2)
        for (size_t i = n - 1; i >= 2; i--) { kl[i] -= kl[i - 1]; kg[i] -= kg[i - 1]; }
    uint32_t ml = 0, mg = 0;
    for (size_t i = 1; i < n; i++) { kl[i] = zz(kl[i]); kg[i] = zz(kg[i]); ml |= kl[i]; mg |= kg[i]; }
    const std::string& L = V.layout;
    if (L == "vi") {
        for (size_t i = 1; i < n; i++) { put_varint(out, kl[i]); put_varint(out, kg[i]); }
        return mkcode(0, 0, hyb);
    }
    if (L == "vs") {
        for (size_t i = 1; i < n; i++) put_varint(out, kl[i]);
        for (size_t i = 1; i < n; i++) put_varint(out, kg[i]);
        return mkcode(0, 0, hyb);
    }
    if (L == "fi" || L == "fp") {
        int wl = bytelen(ml), wg = bytelen(mg);
        size_t m = n - 1, base = out.size();
        out.resize(base + m * (wl + wg));
        uint8_t* o = &out[base];
        if (L == "fi") {
            for (size_t j = 0; j < m; j++) {
                for (int b = 0; b < wl; b++) *o++ = (uint8_t)(kl[j + 1] >> (8 * b));
                for (int b = 0; b < wg; b++) *o++ = (uint8_t)(kg[j + 1] >> (8 * b));
            }
        } else {
            for (int b = 0; b < wl; b++) for (size_t j = 0; j < m; j++) *o++ = (uint8_t)(kl[j + 1] >> (8 * b));
            for (int b = 0; b < wg; b++) for (size_t j = 0; j < m; j++) *o++ = (uint8_t)(kg[j + 1] >> (8 * b));
        }
        return mkcode(wl, wg, hyb);
    }
    if (L == "bx" || L == "fx") {
        // first delta as a LEB128 pair; widths cover the remaining values only
        put_varint(out, kl[1]); put_varint(out, kg[1]);
        uint32_t rl = 0, rg = 0;
        for (size_t i = 2; i < n; i++) { rl |= kl[i]; rg |= kg[i]; }
        size_t m = n - 2, base = out.size();
        if (L == "fx") {
            int wl = bytelen(rl), wg = bytelen(rg);
            out.resize(base + m * (wl + wg));
            uint8_t* o = &out[base];
            for (int b = 0; b < wl; b++) for (size_t j = 0; j < m; j++) *o++ = (uint8_t)(kl[j + 2] >> (8 * b));
            for (int b = 0; b < wg; b++) for (size_t j = 0; j < m; j++) *o++ = (uint8_t)(kg[j + 2] >> (8 * b));
            return mkcode(wl, wg, hyb);
        }
        int wl = bitlen(rl), wg = bitlen(rg);
        size_t nbytes = (m * (wl + wg) + 7) / 8;
        out.resize(base + nbytes, 0);
        uint8_t* o = &out[base];
        size_t pos = 0;
        auto put = [&](uint32_t v, int w) {
            for (int b = 0; b < w; b++, pos++) if ((v >> b) & 1) o[pos >> 3] |= (uint8_t)(1u << (pos & 7));
        };
        for (size_t j = 0; j < m; j++) put(kl[j + 2], wl);
        for (size_t j = 0; j < m; j++) put(kg[j + 2], wg);
        return mkcode(wl, wg, hyb);
    }
    if (L == "bp") {
        int wl = bitlen(ml), wg = bitlen(mg);
        size_t m = n - 1;
        size_t nbits = m * (wl + wg), nbytes = (nbits + 7) / 8;
        size_t base = out.size();
        out.resize(base + nbytes, 0);
        uint8_t* o = &out[base];
        size_t pos = 0;
        auto put = [&](uint32_t v, int w) {
            for (int b = 0; b < w; b++, pos++) if ((v >> b) & 1) o[pos >> 3] |= (uint8_t)(1u << (pos & 7));
        };
        for (size_t j = 0; j < m; j++) put(kl[j + 1], wl);
        for (size_t j = 0; j < m; j++) put(kg[j + 1], wg);
        return mkcode(wl, wg, hyb);
    }
    throw std::runtime_error("layout " + L);
}

// Decode n nodes of one way into out (bits, 2 per node). Returns bytes consumed.
static size_t decode_way(const Variant& V, const uint8_t* p, size_t n, uint16_t code, uint32_t* out,
                         const uint32_t* anc) {
    const uint8_t* p0 = p;
    const int wl = code & 63, wg = (code >> 6) & 63;
    const bool hyb = (code >> 12) & 1;
    uint32_t fl, fg;
    if (anc) {
        fl = ulp_unkey(ulp_key(anc[0]) + unzz(get_varint(p)));
        fg = ulp_unkey(ulp_key(anc[1]) + unzz(get_varint(p)));
    } else { fl = rd32(p); fg = rd32(p + 4); p += 8; }
    out[0] = fl; out[1] = fg;
    if (n == 1) return p - p0;
    uint32_t kl = hyb ? 0 : ulp_key(fl), kg = hyb ? 0 : ulp_key(fg);
    if (hyb) { hyb_key(fl, kl); hyb_key(fg, kg); }
    uint32_t dl = 0, dg = 0;
    const size_t m = n - 1;
    auto emit = [&](size_t j, uint32_t zl, uint32_t zgv) {
        uint32_t a = unzz(zl), b = unzz(zgv);
        if (V.order == 2 && j > 0) { dl += a; dg += b; } else { dl = a; dg = b; }
        kl += dl; kg += dg;
        out[2 * (j + 1)] = hyb ? hyb_decode(kl) : ulp_unkey(kl);
        out[2 * (j + 1) + 1] = hyb ? hyb_decode(kg) : ulp_unkey(kg);
    };
    const std::string& L = V.layout;
    if (L == "vi") {
        for (size_t j = 0; j < m; j++) { uint32_t a = get_varint(p); uint32_t b = get_varint(p); emit(j, a, b); }
        return p - p0;
    }
    if (L == "vs") {
        // lat run first: skip-scan to find the lng run start
        const uint8_t* q = p;
        for (size_t j = 0; j < m; j++) { while (*q & 0x80) q++; q++; }
        for (size_t j = 0; j < m; j++) { uint32_t a = get_varint(p); uint32_t b = get_varint(q); emit(j, a, b); }
        return q - p0;
    }
    if (L == "fi") {
        for (size_t j = 0; j < m; j++) {
            uint32_t a = 0, b = 0;
            for (int k = 0; k < wl; k++) a |= (uint32_t)*p++ << (8 * k);
            for (int k = 0; k < wg; k++) b |= (uint32_t)*p++ << (8 * k);
            emit(j, a, b);
        }
        return p - p0;
    }
    if (L == "fp") {
        for (size_t j = 0; j < m; j++) {
            uint32_t a = 0, b = 0;
            for (int k = 0; k < wl; k++) a |= (uint32_t)p[k * m + j] << (8 * k);
            for (int k = 0; k < wg; k++) b |= (uint32_t)p[(wl + k) * m + j] << (8 * k);
            emit(j, a, b);
        }
        return (p - p0) + m * (wl + wg);
    }
    if (L == "bx" || L == "fx") {
        uint32_t a0 = get_varint(p), b0 = get_varint(p);
        emit(0, a0, b0);
        const size_t r = m - 1;
        if (L == "fx") {
            for (size_t j = 0; j < r; j++) {
                uint32_t a = 0, b = 0;
                for (int k = 0; k < wl; k++) a |= (uint32_t)p[k * r + j] << (8 * k);
                for (int k = 0; k < wg; k++) b |= (uint32_t)p[(wl + k) * r + j] << (8 * k);
                emit(j + 1, a, b);
            }
            return (p - p0) + r * (wl + wg);
        }
        const uint64_t mlk = wl == 32 ? 0xffffffffull : ((1ull << wl) - 1);
        const uint64_t mgk = wg == 32 ? 0xffffffffull : ((1ull << wg) - 1);
        size_t gl = 0, gg = r * wl;
        for (size_t j = 0; j < r; j++) {
            uint32_t a = (uint32_t)((rd64(p + (gl >> 3)) >> (gl & 7)) & mlk);
            uint32_t b = (uint32_t)((rd64(p + (gg >> 3)) >> (gg & 7)) & mgk);
            gl += wl; gg += wg;
            emit(j + 1, a, b);
        }
        return (p - p0) + (r * (wl + wg) + 7) / 8;
    }
    if (L == "bp") {
        const uint64_t mlk = wl == 32 ? 0xffffffffull : ((1ull << wl) - 1);
        const uint64_t mgk = wg == 32 ? 0xffffffffull : ((1ull << wg) - 1);
        size_t gl = 0, gg = m * wl;
        for (size_t j = 0; j < m; j++) {
            uint32_t a = (uint32_t)((rd64(p + (gl >> 3)) >> (gl & 7)) & mlk);
            uint32_t b = (uint32_t)((rd64(p + (gg >> 3)) >> (gg & 7)) & mgk);
            gl += wl; gg += wg;
            emit(j, a, b);
        }
        return (p - p0) + (m * (wl + wg) + 7) / 8;
    }
    throw std::runtime_error("layout " + L);
}

static void write_file(const std::string& path, const void* p, size_t n) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("write " + path);
    if (n && fwrite(p, 1, n, f) != n) throw std::runtime_error("short write " + path);
    fclose(f);
}

int main(int argc, char** argv) {
    if (argc < 7) { fprintf(stderr, "usage: see header\n"); return 2; }
    std::string mode = argv[1];
    const size_t stride = std::stoul(argv[2]);
    Map ways(argv[3]), nodes(argv[4]);
    const std::string vname = argv[5], outdir = argv[6];
    const Variant V = parse_variant(vname);
    const size_t W = ways.n / stride;
    const uint32_t* nb = (const uint32_t*)nodes.p;
    const size_t N = nodes.n / 8;

    if (mode == "enc") {
        std::vector<uint8_t> blocks; blocks.reserve(nodes.n);
        std::vector<uint8_t> nw(ways.p, ways.p + ways.n);
        std::vector<uint32_t> z1, z2;
        size_t hyb_fallback = 0, hyb_ways = 0;
        const size_t A = V.anchor;
        std::vector<uint32_t> anchors;
        if (A) {
            anchors.assign(2 * ((W + A - 1) / A), 0);
            for (size_t g = 0; g * A < W; g++)
                for (size_t w = g * A; w < std::min(W, (g + 1) * A); w++) {
                    if (!rd16(ways.p + w * stride + 4)) continue;
                    uint32_t o = rd32(ways.p + w * stride);
                    anchors[2 * g] = nb[2 * (size_t)o]; anchors[2 * g + 1] = nb[2 * (size_t)o + 1];
                    break;
                }
        }
        auto anc_of = [&](size_t w) -> const uint32_t* { return A ? &anchors[2 * (w / A)] : nullptr; };
        for (size_t w = 0; w < W; w++) {
            uint8_t* rec = &nw[w * stride];
            uint32_t off = rd32(rec); uint16_t cnt = rd16(rec + 4);
            if (blocks.size() > 0xffffffffull) throw std::runtime_error("block offset overflows u32");
            wr32(rec, (uint32_t)blocks.size());
            uint16_t code = 0;
            if (cnt) {
                if ((size_t)off + cnt > N) throw std::runtime_error("way out of range");
                code = encode_way(V, nb + 2 * (size_t)off, cnt, blocks, z1, z2, anc_of(w));
                if (V.hyb && cnt > 1) { hyb_ways++; if (!((code >> 12) & 1)) hyb_fallback++; }
            }
            wr16(rec + 6, code);
        }
        const size_t payload = blocks.size();
        blocks.resize(payload + 8, 0);  // bit-unpack reads 8 bytes
        // verify: decode every way, compare to the original node bits
        std::vector<uint32_t> buf(2 * 65536);
        size_t bad = 0, checked = 0;
        for (size_t w = 0; w < W; w++) {
            const uint8_t* rec = &nw[w * stride];
            uint32_t boff = rd32(rec); uint16_t cnt = rd16(rec + 4), code = rd16(rec + 6);
            if (!cnt) continue;
            uint32_t off = rd32(ways.p + w * stride);
            size_t used = decode_way(V, &blocks[boff], cnt, code, buf.data(), anc_of(w));
            size_t next = (w + 1 < W) ? rd32(&nw[(w + 1) * stride]) : payload;
            if (memcmp(buf.data(), nb + 2 * (size_t)off, 8 * (size_t)cnt) != 0 || boff + used != next) bad++;
            checked += cnt;
            // the non-node fields are copied verbatim
            if (memcmp(rec + 8, ways.p + w * stride + 8, stride - 8) != 0) bad++;
        }
        if (!getenv("NOFULL")) {
            write_file(outdir + "/" + vname + ".nodes", blocks.data(), payload);
            write_file(outdir + "/" + vname + ".ways", nw.data(), nw.size());
            if (A) write_file(outdir + "/" + vname + ".anchors", anchors.data(), anchors.size() * 4);
        }
        if (A) printf("anchors\t%zu bytes\n", anchors.size() * 4);
        printf("%s\tways %zu\tnodes %zu\tbytes %zu\tB/node %.4f\tverified_nodes %zu\tmismatched_ways %zu\thyb_ways %zu\thyb_fallback %zu\n",
               vname.c_str(), W, N, payload, (double)payload / N, checked, bad, hyb_ways, hyb_fallback);
        if (argc >= 9) {
            // sample: `slices` evenly spaced runs of ways covering `frac` of all ways
            int slices = atoi(argv[7]); double frac = atof(argv[8]);
            size_t per = (size_t)(W * frac / slices);
            std::vector<uint8_t> s_new, s_today;
            size_t s_nodes = 0;
            for (int s = 0; s < slices; s++) {
                size_t w0 = (size_t)((double)W * (s + 0.5) / slices) - per / 2, w1 = w0 + per;
                uint32_t b0 = rd32(&nw[w0 * stride]);
                uint32_t b1 = w1 < W ? rd32(&nw[w1 * stride]) : payload;
                s_new.insert(s_new.end(), blocks.begin() + b0, blocks.begin() + b1);
                // today's nodes for the same ways
                uint32_t o0 = rd32(ways.p + w0 * stride);
                uint32_t o1 = w1 < W ? rd32(ways.p + w1 * stride) : N;
                s_today.insert(s_today.end(), nodes.p + 8 * (size_t)o0, nodes.p + 8 * (size_t)o1);
                s_nodes += o1 - o0;
                if (A) {
                    const uint8_t* ap = (const uint8_t*)&anchors[2 * (w0 / A)];
                    s_new.insert(s_new.end(), ap, ap + 8 * ((w1 - 1) / A - w0 / A + 1));
                }
            }
            write_file(outdir + "/" + vname + ".nodes.sample", s_new.data(), s_new.size());
            write_file(outdir + "/today.nodes.sample", s_today.data(), s_today.size());
            printf("sample\tslices %d\tways %zu\tnodes %zu\tnew %zu\ttoday %zu\n", slices, per * slices, s_nodes, s_new.size(), s_today.size());
        }
        return bad ? 1 : 0;
    }

    if (mode == "bench") {
        // random ways (cnt >= 2); both paths run the reader's per-segment
        // distance loop so the difference is the decode.
        Map nways((outdir + "/" + vname + ".ways"));
        Map nblk((outdir + "/" + vname + ".nodes"));
        std::vector<uint8_t> blk(nblk.p, nblk.p + nblk.n); blk.resize(nblk.n + 8, 0);
        std::vector<uint32_t> anchors;
        if (V.anchor) { Map am(outdir + "/" + vname + ".anchors"); anchors.assign((const uint32_t*)am.p, (const uint32_t*)(am.p + am.n)); }
        auto anc_of = [&](size_t w) -> const uint32_t* { return V.anchor ? &anchors[2 * (w / V.anchor)] : nullptr; };
        size_t want = std::stoul(argv[7]);
        std::mt19937_64 rng(42);
        std::vector<uint32_t> ids;
        while (ids.size() < want) {
            uint32_t w = rng() % W;
            if (rd16(ways.p + w * stride + 4) >= 2) ids.push_back(w);
        }
        // touch everything once so both runs are page-cache hot
        volatile uint64_t sink = 0;
        for (size_t i = 0; i < nodes.n; i += 4096) sink += nodes.p[i];
        for (size_t i = 0; i < nblk.n; i += 4096) sink += nblk.p[i];
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
        size_t total_nodes = 0;
        double acc = 0;
        for (int rep = 0; rep < 2; rep++) {
            // today
            auto t0 = std::chrono::steady_clock::now();
            total_nodes = 0;
            for (uint32_t w : ids) {
                uint32_t off = rd32(ways.p + w * stride); uint16_t cnt = rd16(ways.p + w * stride + 4);
                const float* c = (const float*)(nodes.p + 8 * (size_t)off);
                acc += segdist(c, cnt, c[0] + 1e-4, c[1] + 1e-4);
                total_nodes += cnt;
            }
            auto t1 = std::chrono::steady_clock::now();
            for (uint32_t w : ids) {
                const uint8_t* rec = nways.p + w * stride;
                uint32_t boff = rd32(rec); uint16_t cnt = rd16(rec + 4), code = rd16(rec + 6);
                decode_way(V, &blk[boff], cnt, code, buf.data(), anc_of(w));
                const float* c = (const float*)buf.data();
                acc += segdist(c, cnt, c[0] + 1e-4, c[1] + 1e-4);
            }
            auto t2 = std::chrono::steady_clock::now();
            for (uint32_t w : ids) {
                const uint8_t* rec = nways.p + w * stride;
                uint32_t boff = rd32(rec); uint16_t cnt = rd16(rec + 4), code = rd16(rec + 6);
                decode_way(V, &blk[boff], cnt, code, buf.data(), anc_of(w));
                acc += buf[cnt];
            }
            auto t3 = std::chrono::steady_clock::now();
            double a = std::chrono::duration<double, std::nano>(t1 - t0).count();
            double b = std::chrono::duration<double, std::nano>(t2 - t1).count();
            double c = std::chrono::duration<double, std::nano>(t3 - t2).count();
            if (rep == 1)
                printf("%s\tways %zu\tnodes %zu\ttoday+dist %.2f ns/node\tvariant+dist %.2f ns/node\tdecode-only %.2f ns/node\t(+%.2f ns/node)\n",
                       vname.c_str(), ids.size(), total_nodes, a / total_nodes, b / total_nodes, c / total_nodes, (b - a) / total_nodes);
        }
        fprintf(stderr, "%g %llu\n", acc, (unsigned long long)sink);
        return 0;
    }
    return 2;
}
