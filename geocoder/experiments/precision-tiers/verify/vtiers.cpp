// vtiers: independent decoder (verify step) for the tiers step's PB7 boundary blocks and AB7 building
// blobs, written from the format description; compares every decoded vertex with the exact dumps.
//   vtiers pb   BLOCKS POLYS EXACT.e7 EXACT.idx DIV   (DIV 1: must equal EXACT; DIV 100: must equal my
//                                                      own snap of EXACT, consecutive duplicates dropped,
//                                                      or else EXACT itself; counts both)
//   vtiers ab   BLOBS I2 PTS.e7 OUT.e7 OUT.idx DIV
#include <cmath>
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

static void fail(const char* m, unsigned long long a = 0, unsigned long long b = 0) {
    fprintf(stderr, "FAIL %s %llu %llu\n", m, a, b);
    exit(1);
}
struct M { const uint8_t* p; size_t n; };
static M mapf(const char* path) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) fail(path);
    struct stat st; fstat(fd, &st);
    M m{nullptr, size_t(st.st_size)};
    if (m.n) {
        void* a = mmap(nullptr, m.n, PROT_READ, MAP_SHARED, fd, 0);
        if (a == MAP_FAILED) fail("mmap");
        madvise(a, m.n, MADV_SEQUENTIAL);
        m.p = (const uint8_t*)a;
    }
    close(fd);
    return m;
}
static uint64_t leb(const uint8_t*& p) {
    uint64_t v = 0; int s = 0;
    while (true) { uint8_t c = *p++; v |= uint64_t(c & 127) << s; if (c < 128) return v; s += 7; }
}
static int32_t rdi32(const uint8_t* p) { int32_t v; memcpy(&v, p, 4); return v; }
static uint32_t rdu32(const uint8_t* p) { uint32_t v; memcpy(&v, p, 4); return v; }
static int32_t unzig(uint32_t z) { return (z & 1) ? -int32_t(z >> 1) - 1 : int32_t(z >> 1); }
// round half away from zero to a multiple of d
static int64_t rnd(int64_t x, int64_t d) {
    if (d == 1) return x;
    int64_t a = x < 0 ? -x : x;
    int64_t q = (a + d / 2) / d;
    return (x < 0 ? -q : q) * d;
}

static int cmd_pb(char** a) {
    M blk = mapf(a[2]), pol = mapf(a[3]), ex = mapf(a[4]), ei = mapf(a[5]);
    const int64_t D = atoll(a[6]);
    const size_t N = pol.n / 24;
    const uint64_t* idx = (const uint64_t*)ei.p;
    const int32_t* E = (const int32_t*)ex.p;
    if (ei.n / 8 != N + 1) fail("ring count", ei.n / 8, N);
    uint64_t verts = 0, exact_rings = 0, snapped_rings = 0, consumed = 0, maxoff = 0;
    std::vector<int64_t> dec, ref;
    for (size_t i = 0; i < N; i++) {
        const uint32_t off = rdu32(pol.p + 24 * i), cnt = rdu32(pol.p + 24 * i + 4);
        const uint8_t* p = blk.p + off;
        const uint8_t* s = p;
        const uint8_t tag = *p++;
        const int wl = (tag & 3) + 1, wg = ((tag >> 2) & 3) + 1;
        const bool closed = (tag >> 4) & 1, o2 = (tag >> 5) & 1;
        const uint64_t g = leb(p);
        const size_t m = cnt - (closed ? 1 : 0);
        dec.assign(2 * size_t(cnt), 0);
        for (int c = 0; c < 2; c++) {
            const int32_t first = rdi32(p + 4 * c);
            dec[c] = first;
        }
        p += 8;
        for (int c = 0; c < 2; c++) {
            const int w = c == 0 ? wl : wg;
            uint32_t acc = uint32_t(dec[c]), d = 0;
            for (size_t k = 1; k < m; k++) {
                uint32_t z = 0;
                for (int b = 0; b < w; b++) z |= uint32_t(p[size_t(b) * (m - 1) + (k - 1)]) << (8 * b);
                uint32_t v = uint32_t(unzig(z));
                d = o2 ? d + v : v;
                acc += d * uint32_t(g);
                dec[2 * k + c] = int32_t(acc);
            }
            p += size_t(w) * (m > 0 ? m - 1 : 0);
        }
        if (closed) { dec[2 * m] = dec[0]; dec[2 * m + 1] = dec[1]; }
        consumed += size_t(p - s);
        maxoff = std::max<uint64_t>(maxoff, off);
        // reference: exact ring, and my own snapped ring
        const size_t ne = idx[i + 1] - idx[i];
        const int32_t* e = E + 2 * idx[i];
        bool eq_exact = ne == cnt;
        for (size_t k = 0; eq_exact && k < 2 * ne; k++) eq_exact = dec[k] == e[k];
        bool eq_snap = false;
        if (D != 1) {
            ref.clear();
            for (size_t k = 0; k < ne; k++) {
                int64_t la = rnd(e[2 * k], D), lo = rnd(e[2 * k + 1], D);
                if (!ref.empty() && ref[ref.size() - 2] == la && ref.back() == lo) continue;
                ref.push_back(la); ref.push_back(lo);
            }
            eq_snap = ref.size() == 2 * size_t(cnt);
            for (size_t k = 0; eq_snap && k < ref.size(); k++) eq_snap = dec[k] == ref[k];
        }
        if (D == 1 ? !eq_exact : !(eq_snap || eq_exact)) fail("ring mismatch", i, cnt);
        if (D != 1 && eq_snap) snapped_rings++;
        else exact_rings++;
        verts += cnt;
    }
    if (consumed != blk.n) fail("blocks not fully consumed", consumed, blk.n);
    printf("pb D=%lld rings %zu vertices %llu: equal to my snap %llu, equal to exact (kept exact or D=1) %llu; bytes %zu all consumed\n",
           (long long)D, N, (unsigned long long)verts, (unsigned long long)snapped_rings, (unsigned long long)exact_rings, blk.n);
    return 0;
}

static int cmd_ab(char** a) {
    M bl = mapf(a[2]), i2 = mapf(a[3]), pt = mapf(a[4]), ex = mapf(a[5]), ei = mapf(a[6]);
    const int64_t D = atoll(a[7]);
    const size_t N = pt.n / 8;
    const uint64_t* idx = (const uint64_t*)ei.p;
    if (ei.n / 8 != N + 1) fail("point count");
    const size_t G = (N + 63) / 64;
    if (i2.n != 12 * G) fail("i2 size", i2.n, 12 * G);
    const uint64_t* mask = (const uint64_t*)i2.p;
    const uint32_t* goff = (const uint32_t*)(i2.p + 8 * G);
    const int32_t* P = (const int32_t*)pt.p;
    const int32_t* E = (const int32_t*)ex.p;
    const uint8_t* p = bl.p;
    uint64_t outlines = 0, verts = 0, small = 0;
    std::vector<uint32_t> q[2];
    for (size_t i = 0; i < N; i++) {
        if (i % 64 == 0 && goff[i / 64] != uint64_t(p - bl.p)) fail("group offset", i, goff[i / 64]);
        const size_t ne = idx[i + 1] - idx[i];
        const bool bit = (mask[i / 64] >> (i % 64)) & 1;
        if (bit != (ne > 0)) fail("mask", i);
        if (!ne) continue;
        const uint8_t tag = *p++;
        const int w = (tag & 3) + 1;
        const bool sm = (tag >> 2) & 1, closed = (tag >> 3) & 1, planes = (tag >> 4) & 1, delta = (tag >> 5) & 1;
        const uint64_t m = leb(p);
        const size_t n = m + (closed ? 1 : 0);
        if (n != ne) fail("vertex count", i, n);
        int64_t B[2], r[2] = {0, 0};
        if (sm) { r[0] = int8_t(p[0]); r[1] = int8_t(p[1]); p += 2; small++; }
        else { B[0] = rdi32(p); B[1] = rdi32(p + 4); p += 8; }
        for (int c = 0; c < 2; c++) {
            q[c].assign(m, 0);
            for (uint64_t k = 0; k < m; k++)
                for (int b = 0; b < w; b++)
                    q[c][k] |= uint32_t(planes ? p[size_t(b) * m + k] : p[k * w + b]) << (8 * b);
            if (delta) { uint32_t acc = 0; for (uint64_t k = 0; k < m; k++) { acc += uint32_t(unzig(q[c][k])); q[c][k] = acc; } }
            p += size_t(w) * m;
            if (sm) {
                int64_t sum = 0;
                for (uint64_t k = 0; k < m; k++) sum += q[c][k];
                if (closed) sum += q[c][0];
                const double unit = double(rnd(P[2 * i + c], 100)) / double(D);
                B[c] = std::llround(unit - double(sum) / double(n)) + r[c];
            }
        }
        const int32_t* e = E + 2 * idx[i];
        for (size_t k = 0; k < n; k++)
            for (int c = 0; c < 2; c++) {
                const int64_t got = (B[c] + int64_t(q[c][k < m ? k : 0])) * D;
                if (got != rnd(e[2 * k + c], D)) fail("vertex", i, k);
            }
        outlines++;
        verts += n;
    }
    if (size_t(p - bl.p) != bl.n) fail("blobs not consumed", size_t(p - bl.p), bl.n);
    printf("ab D=%lld points %zu outlines %llu vertices %llu small %llu: every vertex equals round(exact/D)*D; group offsets ok; bytes %zu all consumed\n",
           (long long)D, N, (unsigned long long)outlines, (unsigned long long)verts, (unsigned long long)small, bl.n);
    return 0;
}

int main(int argc, char** argv) {
    if (argc < 2) fail("usage");
    std::string c = argv[1];
    if (c == "pb" && argc == 7) return cmd_pb(argv);
    if (c == "ab" && argc == 8) return cmd_ab(argv);
    fail("usage");
}
