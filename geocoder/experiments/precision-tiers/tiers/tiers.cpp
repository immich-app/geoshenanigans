// tiers: precision-tier variants of the planet's admin boundaries and building outlines, built
// from the exact-coordinate dumps (/ssd/precision/build/dumps, aligned with out-nocap/planet).
// Study tool, single-threaded, everything through mmap.
//
// Ring / outline sets are "e7 sets": IN.e7 (i32 lat, i32 lng per vertex, degrees x 1e7) plus
// IN.idx (u64 x (N + 1) CSR vertex offsets), as dumps/README.md describes.
//
//   tiers snap      IN.e7 IN.idx DIV OUT.e7 OUT.idx
//       Snap every vertex to the global grid DIV x 1e-7 deg (round half away from zero) and drop
//       consecutive duplicates. A ring left with < 3 distinct vertices, or zero area when the exact
//       ring has some, keeps its exact vertices (the PB7 per-ring divisor stores it at 1e-7).
//   tiers today-admin IN.e7 IN.idx SHIPPED_POLYS OUT_DIR
//       Today's format for the real reader: admin_polygons.bin (the shipped records, vertex
//       offset/count and area replaced; area = the builder's polygon_area over the ring's doubles)
//       and admin_vertices.bin, every ring as encoding 3 (u32 @ 1e-7 from an f32 bbox_min).
//   tiers pb-enc    IN.e7 IN.idx SHIPPED_POLYS OUT_BLOCKS OUT_POLYS [--single] [--nogcd] [--order2]
//   tiers pb-dec    BLOCKS POLYS OUT.e7 OUT.idx
//       PB7, the new boundary format (layout B generalised to 1e-7 with a per-ring divisor).
//       One block per ring at the byte offset its polygon record holds (random access per ring):
//         u8 tag = (wl - 1) | (wg - 1) << 2 | closed << 4 | order2 << 5
//         LEB128 g: the ring's divisor (gcd of its deltas; 1 with --nogcd)
//         i32 lat0, i32 lng0: first vertex, degrees x 1e7
//         m - 1 lat deltas / g as wrapping zigzag i32 at wl bytes, byte planes (b0 of every delta,
//         then b1, ...), then m - 1 lng deltas at wg bytes (--single: wl = wg). order2: the planes
//         hold second-order deltas (mod 2^32).
//       m = vertex_count - closed; closed (first == last) drops the closing vertex.
//   tiers ab-enc    PTS.e7 IN.e7 IN.idx DIV OUT_PREFIX [--planes] [--delta]
//   tiers ab-verify PTS.e7 IN.e7 IN.idx DIV OUT_PREFIX [--planes] [--delta]
//       AB7, the new building outline format (abs2 widened to any grid DIV x 1e-7). Outlines in
//       address-point order, one blob per point with an outline (OUT.blobs):
//         u8 tag = wcode | small << 2 | closed << 3 | planes << 4 | delta << 5  (w = wcode + 1 bytes)
//         LEB128 m (stored vertices; closed drops the closing vertex)
//         bbox min B: small -> i8 residuals against the prediction from the shared address point
//           (on the uniform 1e-5 grid: round(point / 100) x 100) minus the mean of q over the closed
//           ring; else i32 B lat, i32 B lng (units of DIV x 1e-7)
//         q = v / DIV - B for the m lat values then the m lng values at w bytes LE (--planes: byte
//         planes; --delta: wrapping zigzag deltas of q instead of q)
//       Index (OUT.i2): per group of 64 points a u64 mask (bit j: point 64 g + j has an outline),
//       then per group the u32 byte offset of the group's first blob; a lookup reads the group's
//       offset and skips popcount(mask below j) blob headers. OUT.i1: today's per-point u32
//       poly_offset column as 4 byte planes (NO_DATA for none), the gc 'col' layout, for comparison.
//       ab-verify decodes every blob sequentially and through the index (all points) and compares
//       each vertex with the encoder's input (v snapped to DIV).
//   tiers today-bldg PTS_BIN IN.e7 IN.idx DIV OUT_DIR [EXACT_MASK.u8]
//       Today's format for the real reader: addr_points.bin (vertex_offset rewritten) and
//       addr_vertices.bin with every outline snapped to the global grid DIV x 1e-7, no duplicate
//       removal: encoding 2 (u16 @ 1e-6 from an f32 bbox_min) when it fits, else encoding 3
//       (u32 @ 1e-7). EXACT_MASK (u8 per point): those outlines exact, as encoding 3 (all of them
//       would not fit the 4 GiB u32 offsets).
//   tiers admin-err SHIPPED_POLYS SHIPPED_VERTS IN.e7 IN.idx [DIV]
//       Vertex position error (m) of the shipped vertices as the reader decodes them (f32), and of
//       the DIV-snapped exact vertices, against IN (the exact rings the shipped ones come from).
#include <algorithm>
#include <cerrno>
#include <cstdarg>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <numeric>
#include <random>
#include <string>
#include <utility>
#include <vector>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "geometry.h"
#include "types.h"
#include "vertex_pack.h"

static const uint32_t NO = 0xFFFFFFFFu;

[[noreturn]] static void die(const char* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    fprintf(stderr, "tiers: FATAL: ");
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n");
    va_end(ap);
    exit(2);
}

struct Map {
    const uint8_t* p = nullptr;
    size_t n = 0;
};
static Map map_file(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) die("open %s: %s", path.c_str(), strerror(errno));
    struct stat st;
    fstat(fd, &st);
    Map m;
    m.n = size_t(st.st_size);
    if (m.n) {
        void* a = mmap(nullptr, m.n, PROT_READ, MAP_SHARED, fd, 0);
        if (a == MAP_FAILED) die("mmap %s", path.c_str());
        madvise(a, m.n, MADV_SEQUENTIAL);
        m.p = static_cast<const uint8_t*>(a);
    }
    close(fd);
    return m;
}

struct Out {
    FILE* f;
    std::string path;
    uint64_t pos = 0;
    explicit Out(const std::string& p) : path(p) {
        f = fopen(p.c_str(), "wb");
        if (!f) die("create %s: %s", p.c_str(), strerror(errno));
        setvbuf(f, nullptr, _IOFBF, 16 << 20);
    }
    ~Out() { close(); }
    void close() {
        if (f && fclose(f)) die("close %s", path.c_str());
        f = nullptr;
    }
    void put(const void* d, size_t n) {
        if (n && fwrite(d, 1, n, f) != n) die("write %s", path.c_str());
        pos += n;
    }
    void put(const std::vector<uint8_t>& v) { put(v.data(), v.size()); }
    template <class T> void pod(const T& v) { put(&v, sizeof v); }
};

struct Rings {
    const uint64_t* idx;
    size_t n;
    const int32_t* v;  // 2 per vertex
    uint64_t nv;
    uint64_t cnt(size_t i) const { return idx[i + 1] - idx[i]; }
    const int32_t* at(size_t i) const { return v + 2 * idx[i]; }
};
static Rings load_rings(const std::string& e7, const std::string& idx) {
    Map mi = map_file(idx), mv = map_file(e7);
    if (mi.n % 8 || mi.n < 8) die("%s size", idx.c_str());
    Rings r;
    r.idx = reinterpret_cast<const uint64_t*>(mi.p);
    r.n = mi.n / 8 - 1;
    r.v = reinterpret_cast<const int32_t*>(mv.p);
    r.nv = mv.n / 8;
    if (r.idx[r.n] != r.nv || r.idx[0] != 0) die("%s does not cover %s", idx.c_str(), e7.c_str());
    return r;
}

static inline int64_t snap(int64_t x, int64_t d) {
    if (d == 1) return x;
    return x >= 0 ? (x + d / 2) / d : -((-x + d / 2) / d);
}
static inline uint32_t zz32(int32_t d) { return (uint32_t(d) << 1) ^ uint32_t(d >> 31); }
static inline int32_t unzz32(uint32_t z) { return int32_t((z >> 1) ^ uint32_t(-int32_t(z & 1))); }
static inline int bytes_for(uint64_t x) { return x < (1ull << 8) ? 1 : x < (1ull << 16) ? 2 : x < (1ull << 24) ? 3 : 4; }
static inline void put_uv(std::vector<uint8_t>& o, uint64_t v) {
    while (v >= 0x80) { o.push_back(uint8_t(v) | 0x80); v >>= 7; }
    o.push_back(uint8_t(v));
}
static inline uint64_t get_uv(const uint8_t*& p) {
    uint64_t v = 0;
    int sh = 0;
    for (;;) {
        const uint8_t c = *p++;
        v |= uint64_t(c & 0x7f) << sh;
        if (!(c & 0x80)) return v;
        sh += 7;
        if (sh > 63) die("bad varint");
    }
}
static std::vector<std::pair<double, double>> as_doubles(const int32_t* v, size_t n) {
    std::vector<std::pair<double, double>> d(n);
    for (size_t k = 0; k < n; k++) d[k] = {double(v[2 * k]) * 1e-7, double(v[2 * k + 1]) * 1e-7};
    return d;
}

// AdminPolygon as shipped (types.h, 24 bytes)
struct Poly {
    uint32_t off, cnt, name;
    uint8_t lvl, pto, p2, p3;
    float area;
    uint16_t cc, p4;
};
static_assert(sizeof(Poly) == 24, "AdminPolygon size");

// ------------------------------------------------------------------------------------- snap
static int cmd_snap(int argc, char** argv) {
    if (argc != 7) die("usage: snap IN.e7 IN.idx DIV OUT.e7 OUT.idx");
    const Rings R = load_rings(argv[2], argv[3]);
    const int64_t D = atoll(argv[4]);
    Out ov(argv[5]), oi(argv[6]);
    uint64_t pos = 0, kept_exact = 0, dups = 0;
    oi.pod(pos);
    std::vector<int32_t> s;
    for (size_t i = 0; i < R.n; i++) {
        const int32_t* v = R.at(i);
        const size_t n = R.cnt(i);
        s.clear();
        for (size_t k = 0; k < n; k++) {
            const int32_t la = int32_t(snap(v[2 * k], D) * D), lo = int32_t(snap(v[2 * k + 1], D) * D);
            if (!s.empty() && s[s.size() - 2] == la && s.back() == lo) { dups++; continue; }
            s.push_back(la);
            s.push_back(lo);
        }
        const size_t m = s.size() / 2;
        const bool closed = m >= 2 && s[0] == s[2 * m - 2] && s[1] == s[2 * m - 1];
        const size_t distinct = m - (closed ? 1 : 0);
        bool keep = distinct < 3;
        if (!keep) {
            const float a0 = polygon_area(as_doubles(v, n)), a1 = polygon_area(as_doubles(s.data(), m));
            keep = a0 > 0 && a1 <= 0;
        }
        if (keep) {
            kept_exact++;
            dups -= n - m;
            ov.put(v, 8 * n);
            pos += n;
        } else {
            ov.put(s.data(), 8 * m);
            pos += m;
        }
        oi.pod(pos);
    }
    printf("{\"cmd\":\"snap\",\"div\":%lld,\"rings\":%zu,\"vertices_in\":%llu,\"vertices_out\":%llu,"
           "\"duplicates_dropped\":%llu,\"rings_kept_exact\":%llu}\n",
           (long long)D, R.n, (unsigned long long)R.nv, (unsigned long long)pos, (unsigned long long)dups,
           (unsigned long long)kept_exact);
    return 0;
}

// ------------------------------------------------------------------------------- today-admin
static float f32_floor(double x) {  // largest float <= x
    float f = float(x);
    if (double(f) > x) f = std::nextafter(f, -INFINITY);
    return f;
}

static int cmd_today_admin(int argc, char** argv) {
    if (argc != 6) die("usage: today-admin IN.e7 IN.idx SHIPPED_POLYS OUT_DIR");
    const Rings R = load_rings(argv[2], argv[3]);
    const Map mp = map_file(argv[4]);
    if (mp.n != R.n * sizeof(Poly)) die("records do not match rings");
    const std::string od = argv[5];
    mkdir(od.c_str(), 0755);
    Out op(od + "/admin_polygons.bin"), ov(od + "/admin_vertices.bin");
    const Poly* P = reinterpret_cast<const Poly*>(mp.p);
    uint64_t verts = 0, f32_same = 0, f32_diff = 0;
    std::vector<uint8_t> blk;
    for (size_t i = 0; i < R.n; i++) {
        Poly p = P[i];
        const int32_t* v = R.at(i);
        const size_t n = R.cnt(i);
        int32_t mla = v[0], mlo = v[1];
        for (size_t k = 1; k < n; k++) { mla = std::min(mla, v[2 * k]); mlo = std::min(mlo, v[2 * k + 1]); }
        const float bl = f32_floor(double(mla) * 1e-7), bg = f32_floor(double(mlo) * 1e-7);
        blk.assign(POLY_HEADER_BYTES + 8 * n, 0);
        blk[0] = 3;
        memcpy(&blk[2], &bl, 4);
        memcpy(&blk[6], &bg, 4);
        for (size_t k = 0; k < n; k++) {
            const double la = double(v[2 * k]) * 1e-7, lo = double(v[2 * k + 1]) * 1e-7;
            // the q nearest the exact value whose reader decode is f32(exact), when one exists
            // within an f32 ulp (it does except near 0, where 1e-7 steps are coarser than an ulp)
            auto pick = [](double x, float b) {
                const long long q0 = std::llround((x - double(b)) / 1e-7);
                const float want = float(x);
                for (long long d = 0; d <= 80; d++)
                    for (long long q : {q0 - d, q0 + d})
                        if (q >= 0 && float(double(b) + double(q) * 1e-7) == want) return q;
                return q0;
            };
            const long long ql = pick(la, bl), qg = pick(lo, bg);
            if (ql < 0 || qg < 0 || ql > 0xFFFFFFFFll || qg > 0xFFFFFFFFll) die("ring %zu: u32 overflow", i);
            const uint32_t a = uint32_t(ql), b = uint32_t(qg);
            memcpy(&blk[POLY_HEADER_BYTES + 8 * k], &a, 4);
            memcpy(&blk[POLY_HEADER_BYTES + 8 * k + 4], &b, 4);
            // the reader's decode (server/src/lib.rs decode_polygon_verts) against f32 of the exact value
            const float rl = float(double(bl) + double(a) * 1e-7), rg = float(double(bg) + double(b) * 1e-7);
            if (rl == float(la) && rg == float(lo)) f32_same++;
            else f32_diff++;
        }
        if (ov.pos + blk.size() > 0xFFFFFFFFull) die("admin_vertices.bin over 4 GiB");
        p.off = uint32_t(ov.pos);
        p.cnt = uint32_t(n);
        p.area = polygon_area(as_doubles(v, n));
        ov.put(blk);
        op.pod(p);
        verts += n;
    }
    printf("{\"cmd\":\"today-admin\",\"rings\":%zu,\"vertices\":%llu,\"bytes\":%llu,\"reader_f32_equal_f32_exact\":%llu,"
           "\"reader_f32_differs\":%llu}\n",
           R.n, (unsigned long long)verts, (unsigned long long)ov.pos, (unsigned long long)f32_same,
           (unsigned long long)f32_diff);
    return 0;
}

// ------------------------------------------------------------------------------------- PB7
struct PbOpt {
    bool single = false, gcd = true, order2 = false;
};

static size_t pb_block(const int32_t* v, size_t n, const PbOpt& o, std::vector<uint8_t>& out, uint64_t* whist, uint64_t& g_not1) {
    const bool closed = n >= 2 && v[0] == v[2 * n - 2] && v[1] == v[2 * n - 1];
    const size_t m = n - (closed ? 1 : 0);
    uint64_t g = 0;
    if (o.gcd)
        for (size_t k = 1; k < m; k++)
            for (int c = 0; c < 2; c++) g = std::gcd(g, uint64_t(std::llabs(int64_t(v[2 * k + c]) - v[2 * k - 2 + c])));
    if (g == 0) g = 1;
    if (g != 1) g_not1++;
    std::vector<uint32_t> z[2];
    uint32_t mx[2] = {0, 0};
    for (int c = 0; c < 2; c++) {
        z[c].resize(m ? m - 1 : 0);
        uint32_t prevd = 0;
        for (size_t k = 1; k < m; k++) {
            const int64_t d = (int64_t(v[2 * k + c]) - v[2 * k - 2 + c]) / int64_t(g);
            const uint32_t dw = uint32_t(int32_t(uint32_t(uint64_t(d))));  // wrap mod 2^32
            const uint32_t val = o.order2 ? dw - prevd : dw;
            prevd = dw;
            z[c][k - 1] = zz32(int32_t(val));
            mx[c] = std::max(mx[c], z[c][k - 1]);
        }
    }
    int w[2] = {bytes_for(mx[0]), bytes_for(mx[1])};
    if (o.single) w[0] = w[1] = std::max(w[0], w[1]);
    whist[w[0] - 1]++;
    whist[4 + w[1] - 1]++;
    out.push_back(uint8_t((w[0] - 1) | (w[1] - 1) << 2 | (closed ? 1 : 0) << 4 | (o.order2 ? 1 : 0) << 5));
    put_uv(out, g);
    const size_t h = out.size();
    out.resize(h + 8);
    memcpy(&out[h], &v[0], 4);
    memcpy(&out[h + 4], &v[1], 4);
    const size_t header = out.size();
    for (int c = 0; c < 2; c++)
        for (int b = 0; b < w[c]; b++)
            for (size_t k = 0; k + 1 < m; k++) out.push_back(uint8_t(z[c][k] >> (8 * b)));
    return header;
}

// Decodes the block at p for a ring of n vertices (closing vertex included) into dst (2 n i32).
static const uint8_t* pb_decode(const uint8_t* p, size_t n, int32_t* dst) {
    const uint8_t tag = *p++;
    const int w[2] = {(tag & 3) + 1, ((tag >> 2) & 3) + 1};
    const bool closed = (tag >> 4) & 1, order2 = (tag >> 5) & 1;
    if (tag >> 6) die("pb: bad tag");
    const uint64_t g = get_uv(p);
    const size_t m = n - (closed ? 1 : 0);
    int32_t first[2];
    memcpy(first, p, 8);
    p += 8;
    for (int c = 0; c < 2; c++) {
        uint32_t cur = uint32_t(first[c]), prevd = 0;
        dst[c] = first[c];
        for (size_t k = 1; k < m; k++) {
            uint32_t zv = 0;
            for (int b = 0; b < w[c]; b++) zv |= uint32_t(p[size_t(b) * (m - 1) + (k - 1)]) << (8 * b);
            uint32_t val = uint32_t(unzz32(zv));
            if (order2) { val += prevd; prevd = val; }
            cur += val * uint32_t(g);
            dst[2 * k + c] = int32_t(cur);
        }
        p += size_t(w[c]) * (m ? m - 1 : 0);
    }
    if (closed) { dst[2 * n - 2] = dst[0]; dst[2 * n - 1] = dst[1]; }
    return p;
}

static int cmd_pb_enc(int argc, char** argv) {
    if (argc < 7) die("usage: pb-enc IN.e7 IN.idx SHIPPED_POLYS OUT_BLOCKS OUT_POLYS [--single] [--nogcd] [--order2]");
    PbOpt o;
    for (int i = 7; i < argc; i++) {
        const std::string a = argv[i];
        if (a == "--single") o.single = true;
        else if (a == "--nogcd") o.gcd = false;
        else if (a == "--order2") o.order2 = true;
        else die("bad option %s", a.c_str());
    }
    const Rings R = load_rings(argv[2], argv[3]);
    const Map mp = map_file(argv[4]);
    if (mp.n != R.n * sizeof(Poly)) die("records do not match rings");
    const Poly* P = reinterpret_cast<const Poly*>(mp.p);
    Out ob(argv[5]), op(argv[6]);
    std::vector<uint8_t> blk;
    uint64_t whist[8] = {0}, g_not1 = 0, closed = 0, hdr = 0;
    for (size_t i = 0; i < R.n; i++) {
        const int32_t* v = R.at(i);
        const size_t n = R.cnt(i);
        blk.clear();
        hdr += pb_block(v, n, o, blk, whist, g_not1);
        closed += (blk[0] >> 4) & 1;
        Poly p = P[i];
        if (ob.pos > 0xFFFFFFFFull) die("blocks over 4 GiB");
        p.off = uint32_t(ob.pos);
        p.cnt = uint32_t(n);
        p.area = polygon_area(as_doubles(v, n));
        ob.put(blk);
        op.pod(p);
    }
    printf("{\"cmd\":\"pb-enc\",\"single\":%d,\"gcd\":%d,\"order2\":%d,\"rings\":%zu,\"vertices\":%llu,\"bytes\":%llu,"
           "\"header_bytes\":%llu,\"closed\":%llu,\"rings_with_divisor\":%llu,\"wl\":[%llu,%llu,%llu,%llu],\"wg\":[%llu,%llu,%llu,%llu]}\n",
           o.single, o.gcd, o.order2, R.n, (unsigned long long)R.nv, (unsigned long long)ob.pos, (unsigned long long)hdr,
           (unsigned long long)closed, (unsigned long long)g_not1, (unsigned long long)whist[0], (unsigned long long)whist[1],
           (unsigned long long)whist[2], (unsigned long long)whist[3], (unsigned long long)whist[4], (unsigned long long)whist[5],
           (unsigned long long)whist[6], (unsigned long long)whist[7]);
    return 0;
}

static int cmd_pb_dec(int argc, char** argv) {
    if (argc != 6) die("usage: pb-dec BLOCKS POLYS OUT.e7 OUT.idx");
    const Map mb = map_file(argv[2]), mp = map_file(argv[3]);
    const size_t N = mp.n / sizeof(Poly);
    const Poly* P = reinterpret_cast<const Poly*>(mp.p);
    Out ov(argv[4]), oi(argv[5]);
    uint64_t pos = 0;
    oi.pod(pos);
    std::vector<int32_t> buf;
    uint64_t consumed = 0;
    for (size_t i = 0; i < N; i++) {
        buf.resize(2 * size_t(P[i].cnt));
        const uint8_t* end = pb_decode(mb.p + P[i].off, P[i].cnt, buf.data());
        consumed += uint64_t(end - (mb.p + P[i].off));
        ov.put(buf.data(), 4 * buf.size());
        pos += P[i].cnt;
        oi.pod(pos);
    }
    if (consumed != mb.n) die("pb-dec: blocks consumed %llu of %zu bytes", (unsigned long long)consumed, mb.n);
    printf("{\"cmd\":\"pb-dec\",\"rings\":%zu,\"vertices\":%llu}\n", N, (unsigned long long)pos);
    return 0;
}

// ------------------------------------------------------------------------------------- AB7
struct AbOpt {
    bool planes = false, delta = false;
};

// Shared address point on the uniform 1e-5 grid, in units of DIV x 1e-7 (as double).
static inline double shared_unit(int32_t e7, int64_t D) { return double(snap(e7, 100) * 100) / double(D); }

static void ab_blob(const int32_t* v, size_t n, const int32_t* pt, int64_t D, const AbOpt& o, std::vector<uint8_t>& out,
                    uint64_t* st) {
    // st: 0 small, 1 closed, 2..5 width 1..4
    std::vector<int64_t> V[2];
    int64_t B[2] = {INT64_MAX, INT64_MAX};
    for (int c = 0; c < 2; c++) {
        V[c].resize(n);
        for (size_t k = 0; k < n; k++) {
            V[c][k] = snap(v[2 * k + c], D);
            B[c] = std::min(B[c], V[c][k]);
        }
    }
    const bool closed = n >= 2 && V[0][0] == V[0][n - 1] && V[1][0] == V[1][n - 1];
    const size_t m = n - (closed ? 1 : 0);
    int64_t r[2];
    uint64_t span = 0;
    std::vector<uint32_t> q[2];
    for (int c = 0; c < 2; c++) {
        int64_t sum = 0;
        for (size_t k = 0; k < n; k++) sum += V[c][k] - B[c];
        const int64_t pred = std::llround(shared_unit(pt[c], D) - double(sum) / double(n));
        r[c] = B[c] - pred;
        q[c].resize(m);
        uint32_t prev = 0;
        for (size_t k = 0; k < m; k++) {
            const uint64_t a = uint64_t(V[c][k] - B[c]);
            if (a > 0xFFFFFFFFull) die("ab: outline span over u32");
            if (o.delta) {
                q[c][k] = zz32(int32_t(uint32_t(a) - prev));
                prev = uint32_t(a);
            } else {
                q[c][k] = uint32_t(a);
            }
            span = std::max<uint64_t>(span, q[c][k]);
        }
    }
    const bool small = r[0] >= -128 && r[0] <= 127 && r[1] >= -128 && r[1] <= 127;
    const int w = bytes_for(span);
    st[0] += small;
    st[1] += closed;
    st[1 + w]++;
    out.push_back(uint8_t((w - 1) | (small ? 1 : 0) << 2 | (closed ? 1 : 0) << 3 | (o.planes ? 1 : 0) << 4 | (o.delta ? 1 : 0) << 5));
    put_uv(out, m);
    if (small) {
        out.push_back(uint8_t(int8_t(r[0])));
        out.push_back(uint8_t(int8_t(r[1])));
    } else {
        for (int c = 0; c < 2; c++) {
            if (B[c] < INT32_MIN || B[c] > INT32_MAX) die("ab: B over i32");
            const int32_t b = int32_t(B[c]);
            const size_t h = out.size();
            out.resize(h + 4);
            memcpy(&out[h], &b, 4);
        }
    }
    for (int c = 0; c < 2; c++) {
        if (o.planes) {
            for (int b = 0; b < w; b++)
                for (size_t k = 0; k < m; k++) out.push_back(uint8_t(q[c][k] >> (8 * b)));
        } else {
            for (size_t k = 0; k < m; k++)
                for (int b = 0; b < w; b++) out.push_back(uint8_t(q[c][k] >> (8 * b)));
        }
    }
}

// Blob size from its header (for skipping), and decode into dst (2 n units x D as e7; n incl closing).
static size_t ab_size(const uint8_t* p) {
    const uint8_t* s = p;
    const uint8_t tag = *p++;
    const size_t w = (tag & 3) + 1;
    const uint64_t m = get_uv(p);
    p += ((tag >> 2) & 1) ? 2 : 8;
    return size_t(p - s) + 2 * m * w;
}
static const uint8_t* ab_decode(const uint8_t* p, const int32_t* pt, int64_t D, std::vector<int32_t>& dst) {
    const uint8_t tag = *p++;
    if (tag >> 6) die("ab: bad tag");
    const int w = (tag & 3) + 1;
    const bool small = (tag >> 2) & 1, closed = (tag >> 3) & 1, planes = (tag >> 4) & 1, delta = (tag >> 5) & 1;
    const uint64_t m = get_uv(p);
    const size_t n = m + (closed ? 1 : 0);
    int8_t rr[2] = {0, 0};
    int32_t bb[2] = {0, 0};
    if (small) { rr[0] = int8_t(p[0]); rr[1] = int8_t(p[1]); p += 2; }
    else { memcpy(bb, p, 8); p += 8; }
    dst.resize(2 * n);
    std::vector<uint32_t> q(m);
    for (int c = 0; c < 2; c++) {
        int64_t sum = 0;
        uint32_t prev = 0;
        for (uint64_t k = 0; k < m; k++) {
            uint32_t x = 0;
            for (int b = 0; b < w; b++) x |= uint32_t(planes ? p[size_t(b) * m + k] : p[k * size_t(w) + size_t(b)]) << (8 * b);
            if (delta) { x = prev + uint32_t(unzz32(x)); prev = x; }
            q[k] = x;
            sum += x;
        }
        p += size_t(w) * m;
        if (closed) sum += q[0];
        const int64_t B = small ? std::llround(shared_unit(pt[c], D) - double(sum) / double(n)) + rr[c] : bb[c];
        for (uint64_t k = 0; k < m; k++) dst[2 * k + c] = int32_t((B + int64_t(q[k])) * D);
        if (closed) dst[2 * m + c] = dst[c];
    }
    return p;
}

struct AbFiles {
    std::string blobs, i2, i1;
};
static AbFiles ab_files(const std::string& prefix) { return {prefix + ".blobs", prefix + ".i2", prefix + ".i1"}; }

static void ab_parse(int argc, char** argv, AbOpt& o) {
    for (int i = 7; i < argc; i++) {
        const std::string a = argv[i];
        if (a == "--planes") o.planes = true;
        else if (a == "--delta") o.delta = true;
        else die("bad option %s", a.c_str());
    }
}

static int cmd_ab_enc(int argc, char** argv) {
    if (argc < 7) die("usage: ab-enc PTS.e7 IN.e7 IN.idx DIV OUT_PREFIX [--planes] [--delta]");
    AbOpt o;
    ab_parse(argc, argv, o);
    const Map mpt = map_file(argv[2]);
    const Rings R = load_rings(argv[3], argv[4]);
    const int64_t D = atoll(argv[5]);
    if (mpt.n != R.n * 8) die("points do not match outlines");
    const int32_t* PT = reinterpret_cast<const int32_t*>(mpt.p);
    const AbFiles F = ab_files(argv[6]);
    Out ob(F.blobs);
    const size_t G = (R.n + 63) / 64;
    std::vector<uint64_t> mask(G, 0);
    std::vector<uint32_t> goff(G, 0), poff(R.n, NO);
    std::vector<uint8_t> blob;
    uint64_t st[6] = {0}, outlines = 0, verts = 0;
    for (size_t i = 0; i < R.n; i++) {
        if (i % 64 == 0) {
            if (ob.pos > 0xFFFFFFFFull) die("blobs over 4 GiB");
            goff[i / 64] = uint32_t(ob.pos);
        }
        const size_t n = R.cnt(i);
        if (!n) continue;
        mask[i / 64] |= 1ull << (i % 64);
        poff[i] = uint32_t(ob.pos);
        blob.clear();
        ab_blob(R.at(i), n, PT + 2 * i, D, o, blob, st);
        ob.put(blob);
        outlines++;
        verts += n;
    }
    if (ob.pos > 0xFFFFFFFFull) die("blobs over 4 GiB");
    {
        Out oi(F.i2);
        oi.put(mask.data(), 8 * G);
        oi.put(goff.data(), 4 * G);
    }
    {
        Out oi(F.i1);
        std::vector<uint8_t> plane(R.n);
        for (int b = 0; b < 4; b++) {
            for (size_t i = 0; i < R.n; i++) plane[i] = uint8_t(poff[i] >> (8 * b));
            oi.put(plane);
        }
    }
    printf("{\"cmd\":\"ab-enc\",\"div\":%lld,\"planes\":%d,\"delta\":%d,\"points\":%zu,\"outlines\":%llu,\"vertices\":%llu,"
           "\"blob_bytes\":%llu,\"small\":%llu,\"closed\":%llu,\"w\":[%llu,%llu,%llu,%llu],\"i2_bytes\":%zu,\"i1_bytes\":%zu}\n",
           (long long)D, o.planes, o.delta, R.n, (unsigned long long)outlines, (unsigned long long)verts,
           (unsigned long long)ob.pos, (unsigned long long)st[0], (unsigned long long)st[1], (unsigned long long)st[2],
           (unsigned long long)st[3], (unsigned long long)st[4], (unsigned long long)st[5], 12 * G, 4 * R.n);
    return 0;
}

static int cmd_ab_verify(int argc, char** argv) {
    if (argc < 7) die("usage: ab-verify PTS.e7 IN.e7 IN.idx DIV OUT_PREFIX");
    const Map mpt = map_file(argv[2]);
    const Rings R = load_rings(argv[3], argv[4]);
    const int64_t D = atoll(argv[5]);
    const int32_t* PT = reinterpret_cast<const int32_t*>(mpt.p);
    const AbFiles F = ab_files(argv[6]);
    const Map mb = map_file(F.blobs), mi = map_file(F.i2);
    const size_t G = (R.n + 63) / 64;
    if (mi.n != 12 * G) die("i2 size");
    const uint64_t* mask = reinterpret_cast<const uint64_t*>(mi.p);
    const uint32_t* goff = reinterpret_cast<const uint32_t*>(mi.p + 8 * G);
    // 1) sequential decode of every blob, every vertex against the snapped input
    std::vector<int32_t> dec;
    const uint8_t* p = mb.p;
    uint64_t verts = 0, outlines = 0;
    for (size_t i = 0; i < R.n; i++) {
        const size_t n = R.cnt(i);
        const bool has = (mask[i / 64] >> (i % 64)) & 1;
        if (has != (n > 0)) die("point %zu: mask bit %d, outline vertices %zu", i, int(has), n);
        if (!n) continue;
        const uint8_t* q = ab_decode(p, PT + 2 * i, D, dec);
        if (size_t(q - p) != ab_size(p)) die("point %zu: blob size", i);
        p = q;
        if (dec.size() != 2 * n) die("point %zu: decoded %zu vertices, expected %zu", i, dec.size() / 2, n);
        const int32_t* v = R.at(i);
        for (size_t k = 0; k < 2 * n; k++)
            if (dec[k] != int32_t(snap(v[k], D) * D)) die("point %zu vertex %zu: decoded %d expected %lld", i, k / 2, dec[k], (long long)(snap(v[k], D) * D));
        verts += n;
        outlines++;
    }
    if (size_t(p - mb.p) != mb.n) die("blobs not fully consumed");
    // 2) every point through the group index (random-access path), 3) timed random lookups
    auto locate = [&](size_t i, size_t& skipped) -> const uint8_t* {
        const uint64_t mk = mask[i / 64];
        const unsigned j = unsigned(i % 64);
        if (!((mk >> j) & 1)) return nullptr;
        const uint8_t* b = mb.p + goff[i / 64];
        const int before = __builtin_popcountll(mk & ((1ull << j) - 1));
        for (int s = 0; s < before; s++) b += ab_size(b);
        skipped += size_t(before);
        return b;
    };
    const uint8_t* expect = mb.p;
    size_t skipped = 0;
    for (size_t i = 0; i < R.n; i++) {
        if (!R.cnt(i)) continue;
        const uint8_t* b = locate(i, skipped);
        if (b != expect) die("point %zu: index locates offset %zu, sequential %zu", i, size_t(b - mb.p), size_t(expect - mb.p));
        expect += ab_size(b);
    }
    std::mt19937_64 rng(20261014);
    const size_t L = 2000000;
    std::vector<size_t> pick(L);
    for (auto& x : pick) x = size_t(rng() % R.n);
    size_t found = 0, sk = 0;
    uint64_t chk = 0;
    const auto t0 = std::chrono::steady_clock::now();
    for (size_t x : pick) {
        const uint8_t* b = locate(x, sk);
        if (!b) continue;
        ab_decode(b, PT + 2 * x, D, dec);
        chk += uint64_t(dec[0]) + dec.size();
        found++;
    }
    const double ns = std::chrono::duration<double, std::nano>(std::chrono::steady_clock::now() - t0).count() / double(L);
    // the same lookups through the per-point offset column (.i1, 4 byte planes): no header skipping
    const Map m1 = map_file(F.i1);
    if (m1.n != 4 * R.n) die("i1 size");
    const auto t1 = std::chrono::steady_clock::now();
    size_t found1 = 0;
    for (size_t x : pick) {
        const uint32_t off = uint32_t(m1.p[x]) | uint32_t(m1.p[R.n + x]) << 8 | uint32_t(m1.p[2 * R.n + x]) << 16 |
                             uint32_t(m1.p[3 * R.n + x]) << 24;
        if (off == NO) continue;
        ab_decode(mb.p + off, PT + 2 * x, D, dec);
        chk += uint64_t(dec[0]) + dec.size();
        found1++;
    }
    const double ns1 = std::chrono::duration<double, std::nano>(std::chrono::steady_clock::now() - t1).count() / double(L);
    if (found1 != found) die("i1 and i2 lookups disagree");
    printf("{\"cmd\":\"ab-verify\",\"div\":%lld,\"outlines\":%llu,\"vertices\":%llu,\"all_vertices_equal\":true,"
           "\"index_locates_every_blob\":true,\"index_mean_headers_skipped\":%.2f,\"random_lookups\":%zu,\"with_outline\":%zu,"
           "\"ns_per_lookup_decode_warm_i2\":%.0f,\"ns_per_lookup_decode_warm_i1\":%.0f,\"chk\":%llu}\n",
           (long long)D, (unsigned long long)outlines, (unsigned long long)verts, double(skipped) / double(outlines), L, found,
           ns, ns1, (unsigned long long)chk);
    return 0;
}

// ------------------------------------------------------------------------------ decode benches
// Warm decode cost of a whole boundary file to f64 degrees (sequential, every ring) and of 300k
// random rings, PB7 (pb) or gc layout B (lb, today's grids, the reader's f32 semantics).
static void lb_decode(const uint8_t* h, size_t n, std::vector<double>& out) {
    static const double SCALE[4] = {1e-5, 1e-4, 1e-6, 1e-7};
    const uint8_t e = h[0];
    const int w = e == 3 ? 4 : 2;
    float bm[2];
    memcpy(bm, h + 2, 8);
    const uint8_t* s = h + 10;
    out.resize(2 * n);
    const uint64_t mask = w == 4 ? 0xFFFFFFFFull : 0xFFFFull;
    for (int c = 0; c < 2; c++) {
        uint32_t prev = 0;
        for (size_t k = 0; k < n; k++) {
            uint32_t z = 0;
            for (int b = 0; b < w; b++) z |= uint32_t(s[(size_t(c) * w + b) * n + k]) << (8 * b);
            const uint32_t q = uint32_t((uint64_t(prev) + uint32_t(unzz32(z))) & mask);
            prev = q;
            out[2 * k + c] = double(float(double(bm[c]) + double(q) * SCALE[e]));
        }
    }
}

static int cmd_bench(int argc, char** argv) {
    if (argc != 4) die("usage: pb-bench|lb-bench BLOCKS POLYS");
    const bool pb = std::string(argv[1]) == "pb-bench";
    const Map mb = map_file(argv[2]), mp = map_file(argv[3]);
    madvise(const_cast<uint8_t*>(mb.p), mb.n, MADV_WILLNEED);
    const size_t N = mp.n / sizeof(Poly);
    const Poly* P = reinterpret_cast<const Poly*>(mp.p);
    std::vector<int32_t> e7;
    std::vector<double> out;
    double chk = 0;
    auto one = [&](size_t i) {
        if (pb) {
            e7.resize(2 * size_t(P[i].cnt));
            pb_decode(mb.p + P[i].off, P[i].cnt, e7.data());
            out.resize(e7.size());
            for (size_t k = 0; k < e7.size(); k++) out[k] = double(e7[k]) * 1e-7;
        } else {
            lb_decode(mb.p + P[i].off, P[i].cnt, out);
        }
        chk += out[0];
    };
    uint64_t verts = 0;
    for (size_t i = 0; i < N; i++) { one(i); verts += P[i].cnt; }  // warm-up pass (page cache)
    const auto t0 = std::chrono::steady_clock::now();
    for (size_t i = 0; i < N; i++) one(i);
    const double seq = std::chrono::duration<double, std::nano>(std::chrono::steady_clock::now() - t0).count();
    std::mt19937_64 rng(20261014);
    std::vector<size_t> pick(300000);
    uint64_t rv = 0;
    for (auto& x : pick) { x = size_t(rng() % N); rv += P[x].cnt; }
    const auto t1 = std::chrono::steady_clock::now();
    for (size_t x : pick) one(x);
    const double rnd = std::chrono::duration<double, std::nano>(std::chrono::steady_clock::now() - t1).count();
    printf("{\"cmd\":\"%s\",\"rings\":%zu,\"vertices\":%llu,\"seq_s\":%.2f,\"seq_ns_per_vertex\":%.2f,\"random_rings\":%zu,"
           "\"random_ns_per_vertex\":%.2f,\"random_us_per_ring\":%.2f,\"chk\":%.3f}\n",
           argv[1], N, (unsigned long long)verts, seq / 1e9, seq / double(verts), pick.size(), rnd / double(rv),
           rnd / 1e3 / double(pick.size()), chk);
    return 0;
}

// ------------------------------------------------------------------------------- today-bldg
struct AddrRec {
    float lat, lng;
    uint32_t hn, street, pw, voff, vcnt;
};
static_assert(sizeof(AddrRec) == 28, "AddrPoint size");

static int cmd_today_bldg(int argc, char** argv) {
    if (argc != 7 && argc != 8) die("usage: today-bldg PTS_BIN IN.e7 IN.idx DIV OUT_DIR [EXACT_MASK.u8]");
    const Map mp = map_file(argv[2]);
    const Rings R = load_rings(argv[3], argv[4]);
    const int64_t D0 = atoll(argv[5]);
    const Map mm = argc == 8 ? map_file(argv[7]) : Map{};
    if (argc == 8 && mm.n != R.n) die("mask size");
    uint64_t exact_outlines = 0;
    if (mp.n != R.n * sizeof(AddrRec)) die("addr_points.bin does not match outlines");
    const AddrRec* P = reinterpret_cast<const AddrRec*>(mp.p);
    const std::string od = argv[6];
    mkdir(od.c_str(), 0755);
    Out op(od + "/addr_points.bin"), ov(od + "/addr_vertices.bin");
    std::vector<uint8_t> blk;
    uint64_t by_enc[4] = {0}, verts = 0;
    for (size_t i = 0; i < R.n; i++) {
        AddrRec r = P[i];
        const size_t n = R.cnt(i);
        if (n != r.vcnt) die("point %zu: shipped vertex_count %u, dump %zu (expects the uncapped build)", i, r.vcnt, n);
        if (!n) { op.pod(r); continue; }
        const int32_t* v = R.at(i);
        // masked points: their exact outline (DIV 1) as encoding 3, the q picked so the reader decodes
        // f32(exact) (as today-admin)
        const bool exact = mm.p && mm.p[i];
        const int64_t D = exact ? 1 : D0;
        const double U = double(D) * 1e-7;
        exact_outlines += exact;
        int64_t B[2] = {INT64_MAX, INT64_MAX};
        for (size_t k = 0; k < n; k++)
            for (int c = 0; c < 2; c++) B[c] = std::min(B[c], snap(v[2 * k + c], D));
        const float bf[2] = {f32_floor(double(B[0]) * U), f32_floor(double(B[1]) * U)};
        // encoding 2 (u16 @ 1e-6) when the whole outline fits, else encoding 3 (u32 @ 1e-7)
        int enc = exact ? 3 : 2;
        double scale = exact ? 1e-7 : 1e-6;
        for (int pass = 0; pass < 2; pass++) {
            bool fits = true;
            blk.assign(POLY_HEADER_BYTES + n * VERTEX_STRIDE[enc], 0);
            blk[0] = uint8_t(enc);
            memcpy(&blk[2], &bf[0], 4);
            memcpy(&blk[6], &bf[1], 4);
            for (size_t k = 0; k < n && fits; k++)
                for (int c = 0; c < 2; c++) {
                    const double x = double(snap(v[2 * k + c], D)) * U;
                    long long q = std::llround((x - double(bf[c])) / scale);
                    if (exact)
                        for (long long d = 0, found = 0; d <= 80 && !found; d++)
                            for (long long t : {q - d, q + d})
                                if (t >= 0 && float(double(bf[c]) + double(t) * 1e-7) == float(x)) { q = t; found = 1; break; }
                    if (q < 0 || q > (enc == 2 ? 0xFFFFll : 0xFFFFFFFFll)) { fits = false; break; }
                    if (enc == 2) { const uint16_t x = uint16_t(q); memcpy(&blk[POLY_HEADER_BYTES + 4 * k + 2 * c], &x, 2); }
                    else { const uint32_t x = uint32_t(q); memcpy(&blk[POLY_HEADER_BYTES + 8 * k + 4 * c], &x, 4); }
                }
            if (fits) break;
            if (enc == 3) die("point %zu: does not fit encoding 3", i);
            enc = 3;
            scale = 1e-7;
        }
        if (ov.pos + blk.size() > 0xFFFFFFFFull) die("addr_vertices.bin over 4 GiB");
        r.voff = uint32_t(ov.pos);
        ov.put(blk);
        op.pod(r);
        by_enc[enc]++;
        verts += n;
    }
    printf("{\"cmd\":\"today-bldg\",\"div\":%lld,\"exact_outlines\":%llu,\"points\":%zu,\"outlines_enc2\":%llu,\"outlines_enc3\":%llu,\"vertices\":%llu,"
           "\"bytes\":%llu}\n",
           (long long)D0, (unsigned long long)exact_outlines, R.n, (unsigned long long)by_enc[2], (unsigned long long)by_enc[3], (unsigned long long)verts,
           (unsigned long long)ov.pos);
    return 0;
}

// --------------------------------------------------------------------------------- admin-err
struct Hist {
    // error in mm, log-ish buckets: exact mm below 100 m
    std::vector<uint64_t> b = std::vector<uint64_t>(100001, 0);
    uint64_t n = 0;
    double mx = 0;
    void add(double m) {
        const double mm = m * 1000.0;
        b[size_t(std::min(mm, 100000.0))]++;
        n++;
        mx = std::max(mx, m);
    }
    double q(double f) const {
        const uint64_t t = uint64_t(std::ceil(f * double(n)));
        uint64_t s = 0;
        for (size_t i = 0; i < b.size(); i++) {
            s += b[i];
            if (s >= t) return double(i) / 1000.0;
        }
        return mx;
    }
    void print(const char* name) const {
        printf("\"%s\":{\"n\":%llu,\"p50_m\":%.3f,\"p90_m\":%.3f,\"p99_m\":%.3f,\"p999_m\":%.3f,\"max_m\":%.3f}", name, (unsigned long long)n,
               q(0.5), q(0.9), q(0.99), q(0.999), mx);
    }
};
static inline double err_m(double la, double lo, double ea, double eo) {
    const double dy = (la - ea) * 111320.0, dx = (lo - eo) * 111320.0 * std::cos(ea * M_PI / 180.0);
    return std::sqrt(dx * dx + dy * dy);
}

static int cmd_admin_err(int argc, char** argv) {
    if (argc < 6) die("usage: admin-err SHIPPED_POLYS SHIPPED_VERTS IN.e7 IN.idx [DIV]");
    const Map mp = map_file(argv[2]), mv = map_file(argv[3]);
    const Rings R = load_rings(argv[4], argv[5]);
    const int64_t D = argc > 6 ? atoll(argv[6]) : 0;
    const Poly* P = reinterpret_cast<const Poly*>(mp.p);
    if (mp.n != R.n * sizeof(Poly)) die("records do not match rings");
    static const double SCALE[4] = {1e-5, 1e-4, 1e-6, 1e-7};
    Hist shipped[4], all, snapped, snapped_f32, exact_f32;
    for (size_t i = 0; i < R.n; i++) {
        const uint8_t* h = mv.p + P[i].off;
        const uint8_t e = h[0];
        if (e > 3) die("encoding %u", e);
        if (P[i].cnt != R.cnt(i)) die("ring %zu count", i);
        float bl, bg;
        memcpy(&bl, h + 2, 4);
        memcpy(&bg, h + 6, 4);
        const int32_t* v = R.at(i);
        for (size_t k = 0; k < P[i].cnt; k++) {
            uint32_t ql, qg;
            if (e == 3) { memcpy(&ql, h + 10 + 8 * k, 4); memcpy(&qg, h + 14 + 8 * k, 4); }
            else { uint16_t a, b; memcpy(&a, h + 10 + 4 * k, 2); memcpy(&b, h + 12 + 4 * k, 2); ql = a; qg = b; }
            const float la = float(double(bl) + double(ql) * SCALE[e]), lo = float(double(bg) + double(qg) * SCALE[e]);
            const double ea = double(v[2 * k]) * 1e-7, eo = double(v[2 * k + 1]) * 1e-7;
            const double m = err_m(la, lo, ea, eo);
            shipped[e].add(m);
            all.add(m);
            exact_f32.add(err_m(float(ea), float(eo), ea, eo));
            if (D) {
                const double sa = double(snap(v[2 * k], D) * D) * 1e-7, so = double(snap(v[2 * k + 1], D) * D) * 1e-7;
                snapped.add(err_m(sa, so, ea, eo));
                snapped_f32.add(err_m(float(sa), float(so), ea, eo));
            }
        }
    }
    printf("{\"cmd\":\"admin-err\",");
    all.print("shipped_as_read");
    const char* nm[4] = {"shipped_enc0_1e-5", "shipped_enc1_1e-4", "shipped_enc2_1e-6", "shipped_enc3_1e-7"};
    for (int e = 0; e < 4; e++) { printf(","); shipped[e].print(nm[e]); }
    printf(",");
    exact_f32.print("exact_as_read_f32");
    if (D) {
        printf(",");
        snapped.print("snapped_exact_double");
        printf(",");
        snapped_f32.print("snapped_as_read_f32");
    }
    printf("}\n");
    return 0;
}

int main(int argc, char** argv) {
    if (argc < 2) die("usage: tiers snap|today-admin|pb-enc|pb-dec|ab-enc|ab-verify|today-bldg|admin-err ...");
    const std::string c = argv[1];
    if (c == "snap") return cmd_snap(argc, argv);
    if (c == "today-admin") return cmd_today_admin(argc, argv);
    if (c == "pb-enc") return cmd_pb_enc(argc, argv);
    if (c == "pb-dec") return cmd_pb_dec(argc, argv);
    if (c == "ab-enc") return cmd_ab_enc(argc, argv);
    if (c == "ab-verify") return cmd_ab_verify(argc, argv);
    if (c == "today-bldg") return cmd_today_bldg(argc, argv);
    if (c == "admin-err") return cmd_admin_err(argc, argv);
    if (c == "pb-bench" || c == "lb-bench") return cmd_bench(argc, argv);
    die("unknown command %s", c.c_str());
}
