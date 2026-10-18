// Download-only (whole-file) codecs for street / interp ways + nodes.
// The download form is a set of column streams concatenated into one file;
// install decodes it back to today's street_ways/street_nodes (or the new
// on-disk layout, which is derived from the same values). Lossless: the
// inverse is run here and compared byte-for-byte with today's files.
//
//   nodedl <stride 12|24> <ways.bin> <nodes.bin> <out> <opts> [slices frac]
//   opts: comma list of
//     d2      second-order deltas inside a way (else first-order)
//     hyb     hybrid fixed-point/ulp key (else ulp key)
//     fd      first node of each way as a zigzag delta of the previous way's first node
//     planes  per-way byte widths, every byte plane its own global stream (else LEB128 streams)
//   Streams: cnt(u16 planes) | name(delta zigzag u32 planes) | [interp: start, end-start zz, interp]
//            | first lat, first lng (u32 planes) | lat deltas | lng deltas | width codes
//   With slices/frac only that subset of ways is emitted (sample).
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <string>
#include <vector>
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
static inline int bytelen(uint32_t v) { return v ? (32 - __builtin_clz(v) + 7) / 8 : 0; }

struct Streams {
    std::vector<std::vector<uint8_t>> s;
    explicit Streams(size_t n) : s(n) {}
    void u32planes(size_t base, uint32_t v) { for (int b = 0; b < 4; b++) s[base + b].push_back((uint8_t)(v >> (8 * b))); }
    void u16planes(size_t base, uint16_t v) { for (int b = 0; b < 2; b++) s[base + b].push_back((uint8_t)(v >> (8 * b))); }
    void varint(size_t i, uint32_t v) { while (v >= 0x80) { s[i].push_back((uint8_t)(v | 0x80)); v >>= 7; } s[i].push_back((uint8_t)v); }
};
struct Reader {
    const std::vector<std::vector<uint8_t>>& s; std::vector<size_t> at;
    explicit Reader(const std::vector<std::vector<uint8_t>>& ss) : s(ss), at(ss.size(), 0) {}
    uint8_t byte(size_t i) { return s[i][at[i]++]; }
    uint32_t u32planes(size_t base) { uint32_t v = 0; for (int b = 0; b < 4; b++) v |= (uint32_t)byte(base + b) << (8 * b); return v; }
    uint16_t u16planes(size_t base) { uint16_t v = 0; for (int b = 0; b < 2; b++) v |= (uint16_t)(byte(base + b) << (8 * b)); return v; }
    uint32_t varint(size_t i) { uint32_t v = 0; int sh = 0; while (true) { uint8_t c = byte(i); v |= (uint32_t)(c & 0x7f) << sh; if (!(c & 0x80)) return v; sh += 7; } }
};

// stream ids
enum : size_t { CNT = 0, NAME = 2, START = 6, SPAN = 10, INTERP = 14, FLAT = 15, FLNG = 19,
                CODE = 23, DLAT = 24, DLNG = 28, NSTREAMS = 32 };

int main(int argc, char** argv) {
    const size_t S = std::stoul(argv[1]);
    Map ways(argv[2]), nodes(argv[3]);
    const std::string out = argv[4], opts = "," + std::string(argv[5]) + ",";
    const bool d2 = opts.find(",d2,") != std::string::npos, hyb = opts.find(",hyb,") != std::string::npos,
               fd = opts.find(",fd,") != std::string::npos, planes = opts.find(",planes,") != std::string::npos;
    const size_t W = ways.n / S;
    const uint32_t* nb = (const uint32_t*)nodes.p;
    // way ranges to emit
    std::vector<std::pair<size_t, size_t>> ranges;
    if (argc >= 8) {
        int slices = atoi(argv[6]); double frac = atof(argv[7]);
        size_t per = (size_t)(W * frac / slices);
        for (int s = 0; s < slices; s++) {
            size_t w0 = (size_t)((double)W * (s + 0.5) / slices) - per / 2;
            ranges.push_back({w0, w0 + per});
        }
    } else ranges.push_back({0, W});

    Streams st(NSTREAMS);
    std::vector<uint32_t> kl, kg;
    uint32_t prev_name = 0, prev_fl = 0, prev_fg = 0, prev_start = 0;
    size_t hyb_fail = 0;
    auto key = [&](uint32_t b, bool h) { uint32_t k; if (h && hyb_key(b, k)) return k; return ulp_key(b); };
    for (auto [w0, w1] : ranges) for (size_t w = w0; w < w1; w++) {
        const uint8_t* r = ways.p + w * S;
        uint32_t off = rd32(r); uint16_t c = rd16(r + 4); uint32_t name = rd32(r + 8);
        st.u16planes(CNT, c);
        st.u32planes(NAME, zz(name - prev_name)); prev_name = name;
        if (S == 24) {
            uint32_t s0 = rd32(r + 12), e0 = rd32(r + 16);
            st.u32planes(START, zz(s0 - prev_start)); prev_start = s0;
            st.u32planes(SPAN, zz(e0 - s0));
            st.s[INTERP].push_back(r[20]);
        }
        if (!c) continue;
        // per-way mapping: hybrid only if every value round-trips (else flag 0)
        bool h = hyb;
        kl.resize(c); kg.resize(c);
        for (size_t j = 0; j < c && h; j++) h = hyb_key(nb[2 * (off + j)], kl[j]) && hyb_key(nb[2 * (off + j) + 1], kg[j]);
        if (hyb && !h) hyb_fail++;
        if (!h) for (size_t j = 0; j < c; j++) { kl[j] = ulp_key(nb[2 * (off + j)]); kg[j] = ulp_key(nb[2 * (off + j) + 1]); }
        uint32_t fl = nb[2 * off], fg = nb[2 * off + 1];
        if (fd) {
            uint32_t a = ulp_key(fl), b = ulp_key(fg);
            st.u32planes(FLAT, zz(a - prev_fl)); st.u32planes(FLNG, zz(b - prev_fg));
            prev_fl = a; prev_fg = b;
        } else { st.u32planes(FLAT, fl); st.u32planes(FLNG, fg); }
        for (size_t j = c - 1; j >= 1; j--) { kl[j] -= kl[j - 1]; kg[j] -= kg[j - 1]; }
        if (d2) for (size_t j = c - 1; j >= 2; j--) { kl[j] -= kl[j - 1]; kg[j] -= kg[j - 1]; }
        uint32_t ml = 0, mg = 0;
        for (size_t j = 1; j < c; j++) { kl[j] = zz(kl[j]); kg[j] = zz(kg[j]); ml |= kl[j]; mg |= kg[j]; }
        int wl = bytelen(ml), wg = bytelen(mg);
        if (c > 1) st.s[CODE].push_back((uint8_t)(wl | (wg << 3) | (h ? 64 : 0)));
        else st.s[CODE].push_back(h ? 64 : 0);
        for (size_t j = 1; j < c; j++) {
            if (planes) {
                for (int b = 0; b < wl; b++) st.s[DLAT + b].push_back((uint8_t)(kl[j] >> (8 * b)));
                for (int b = 0; b < wg; b++) st.s[DLNG + b].push_back((uint8_t)(kg[j] >> (8 * b)));
            } else { st.varint(DLAT, kl[j]); st.varint(DLNG, kg[j]); }
        }
    }
    // write + per-stream sizes
    FILE* f = fopen(out.c_str(), "wb");
    size_t total = 0;
    std::string sizes;
    for (size_t i = 0; i < NSTREAMS; i++) {
        if (st.s[i].empty()) continue;
        fwrite(st.s[i].data(), 1, st.s[i].size(), f);
        total += st.s[i].size();
        sizes += " " + std::to_string(i) + ":" + std::to_string(st.s[i].size());
    }
    fclose(f);

    // inverse: rebuild today's way records' (cnt, name, ...) and node bits, compare
    Reader rd(st.s);
    size_t bad = 0, nodes_checked = 0;
    prev_name = prev_fl = prev_fg = prev_start = 0;
    std::vector<uint32_t> buf(2 * 65536);
    for (auto [w0, w1] : ranges) for (size_t w = w0; w < w1; w++) {
        const uint8_t* r = ways.p + w * S;
        uint16_t c = rd.u16planes(CNT);
        uint32_t name = prev_name + unzz(rd.u32planes(NAME)); prev_name = name;
        if (c != rd16(r + 4) || name != rd32(r + 8)) bad++;
        if (S == 24) {
            uint32_t s0 = prev_start + unzz(rd.u32planes(START)); prev_start = s0;
            uint32_t e0 = s0 + unzz(rd.u32planes(SPAN));
            uint8_t ip = rd.byte(INTERP);
            if (s0 != rd32(r + 12) || e0 != rd32(r + 16) || ip != r[20]) bad++;
        }
        if (!c) continue;
        uint32_t fl, fg;
        if (fd) {
            uint32_t a = prev_fl + unzz(rd.u32planes(FLAT)), b = prev_fg + unzz(rd.u32planes(FLNG));
            prev_fl = a; prev_fg = b; fl = ulp_unkey(a); fg = ulp_unkey(b);
        } else { fl = rd.u32planes(FLAT); fg = rd.u32planes(FLNG); }
        uint8_t code = rd.byte(CODE);
        int wl = code & 7, wg = (code >> 3) & 7; bool h = code & 64;
        uint32_t a = 0, b = 0;
        if (h) { hyb_key(fl, a); hyb_key(fg, b); } else { a = ulp_key(fl); b = ulp_key(fg); }
        buf[0] = fl; buf[1] = fg;
        uint32_t dl = 0, dg = 0;
        for (size_t j = 1; j < c; j++) {
            uint32_t zl = 0, zgv = 0;
            if (planes) {
                for (int k = 0; k < wl; k++) zl |= (uint32_t)rd.byte(DLAT + k) << (8 * k);
                for (int k = 0; k < wg; k++) zgv |= (uint32_t)rd.byte(DLNG + k) << (8 * k);
            } else { zl = rd.varint(DLAT); zgv = rd.varint(DLNG); }
            uint32_t x = unzz(zl), y = unzz(zgv);
            if (d2 && j > 1) { dl += x; dg += y; } else { dl = x; dg = y; }
            a += dl; b += dg;
            buf[2 * j] = h ? hyb_decode(a) : ulp_unkey(a);
            buf[2 * j + 1] = h ? hyb_decode(b) : ulp_unkey(b);
        }
        if (memcmp(buf.data(), nb + 2 * (size_t)rd32(r), 8 * (size_t)c) != 0) bad++;
        nodes_checked += c;
    }
    printf("%s\topts %s\tbytes %zu\tnodes_checked %zu\tbad %zu\thyb_fail %zu\tstreams%s\n",
           out.c_str(), argv[5], total, nodes_checked, bad, hyb_fail, sizes.c_str());
    return bad ? 1 : 0;
}
