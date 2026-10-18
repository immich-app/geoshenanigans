// vstreets: independent decoder (verify step) for the streets step's packed-way base/exact files,
// refA refinements and address for64 + ref14 files. Checks every node against work/exact.
//   vstreets ways  WAYS.bin STRIDE EXACT.e7 PREFIX   (PREFIX.e5.*, PREFIX.e7.*, PREFIX.refA*)
//   vstreets addr  EXACT.e7 ENCDIR
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
static M mapf(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) fail(path.c_str());
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
template <class T> static T rd(const uint8_t* p) { T v; memcpy(&v, p, sizeof v); return v; }
static int32_t unzig(uint32_t z) { return (z & 1) ? -int32_t(z >> 1) - 1 : int32_t(z >> 1); }
// floor((e + 50) / 100), written as floor division
static int32_t base_key(int32_t e) {
    int64_t x = int64_t(e) + 50, q = x / 100;
    if (x % 100 != 0 && x < 0) q--;
    return int32_t(q);
}
// LSB-first bit reader
struct Bits {
    const uint8_t* b; uint64_t pos;
    uint32_t get(int w) {
        uint32_t v = 0;
        for (int i = 0; i < w; i++, pos++) v |= uint32_t((b[pos >> 3] >> (pos & 7)) & 1) << i;
        return v;
    }
};

// decodes a packed-way variant; key(e) is the expected key of an exact value
template <class K>
static uint64_t check_packed(const std::string& pre, const std::string& vn, const M& ways, size_t stride, const int32_t* E, K key) {
    M grp = mapf(pre + "." + vn + ".groups"), code = mapf(pre + "." + vn + ".code"), nodes = mapf(pre + "." + vn + ".nodes2");
    const size_t W = ways.n / stride, G = 16;
    if (code.n != 2 * W) fail("code size");
    if (grp.n != 12 * ((W + G - 1) / G)) fail("groups size");
    uint64_t checked = 0, u10 = 0, end = 0;
    for (size_t g = 0; g * G < W; g++) {
        const uint8_t* p = nodes.p + rd<uint32_t>(grp.p + 12 * g);
        const int32_t al = rd<int32_t>(grp.p + 12 * g + 4), ag = rd<int32_t>(grp.p + 12 * g + 8);
        for (size_t j = g * G; j < std::min(W, g * G + G); j++) {
            const uint32_t off = rd<uint32_t>(ways.p + stride * j);
            const uint16_t c = rd<uint16_t>(ways.p + stride * j + 4);
            const uint16_t cd = rd<uint16_t>(code.p + 2 * j);
            if (!c) continue;
            const int wl = cd & 63, wg = (cd >> 6) & 63, wf = (cd >> 13) & 7;
            const bool t10 = (cd >> 12) & 1;
            u10 += t10;
            uint32_t zl = 0, zg = 0;
            for (int b = 0; b < wf; b++) { zl |= uint32_t(p[b]) << (8 * b); zg |= uint32_t(p[wf + b]) << (8 * b); }
            uint32_t kl = uint32_t(al) + uint32_t(unzig(zl)), kg = uint32_t(ag) + uint32_t(unzig(zg));
            Bits bl{p + 2 * wf, 0}, bg{p + 2 * wf, uint64_t(c - 1) * wl};
            uint32_t dl = 0, dg = 0;
            for (size_t k = 0; k < c; k++) {
                if (k) {
                    uint32_t vl = uint32_t(unzig(bl.get(wl))), vg = uint32_t(unzig(bg.get(wg)));
                    if (t10) { vl *= 10; vg *= 10; }
                    dl = k == 1 ? vl : dl + vl;
                    dg = k == 1 ? vg : dg + vg;
                    kl += dl; kg += dg;
                }
                if (int32_t(kl) != key(E[2 * (off + k)]) || int32_t(kg) != key(E[2 * (off + k) + 1])) fail((vn + " node").c_str(), j, k);
                checked++;
            }
            p += 2 * wf + (uint64_t(c - 1) * (wl + wg) + 7) / 8;
            end = std::max<uint64_t>(end, uint64_t(p - nodes.p));
        }
    }
    printf("%s.%s: ways %zu nodes %llu all equal (unit-10 ways %llu); nodes2 bytes used %llu of %zu\n", pre.c_str(), vn.c_str(), W,
           (unsigned long long)checked, (unsigned long long)u10, (unsigned long long)end, nodes.n);
    return checked;
}

static int cmd_ways(char** a) {
    M ways = mapf(a[2]);
    const size_t stride = atoi(a[3]);
    M ex = mapf(a[4]);
    const int32_t* E = (const int32_t*)ex.p;
    const std::string pre = a[5];
    check_packed(pre, "e5", ways, stride, E, base_key);
    check_packed(pre, "e7", ways, stride, E, [](int32_t e) { return e; });
    // refA: base * 100 + residual == exact for every node
    M ri = mapf(pre + ".refA.idx"), rb = mapf(pre + ".refA");
    const size_t W = ways.n / stride, G = 16;
    uint64_t nodes = 0, by[3] = {0, 0, 0};
    for (size_t g = 0; g * G < W; g++) {
        Bits bits{rb.p + rd<uint32_t>(ri.p + 8 * g), 0};
        const uint32_t units = rd<uint32_t>(ri.p + 8 * g + 4);
        for (size_t j = g * G; j < std::min(W, g * G + G); j++) {
            const uint32_t off = rd<uint32_t>(ways.p + stride * j);
            const uint16_t c = rd<uint16_t>(ways.p + stride * j + 4);
            const int u = (units >> (2 * (j - g * G))) & 3;
            for (size_t k = 0; k < c; k++) {
                int32_t r[2] = {0, 0};
                if (u == 0) { r[0] = int32_t(bits.get(7)) - 50; r[1] = int32_t(bits.get(7)) - 50; }
                else if (u == 1) { r[0] = (int32_t(bits.get(4)) - 5) * 10; r[1] = (int32_t(bits.get(4)) - 5) * 10; }
                for (int cc = 0; cc < 2; cc++) {
                    const int32_t e = E[2 * (off + k) + cc];
                    if (int64_t(base_key(e)) * 100 + r[cc] != e) fail("refA node", j, k);
                }
                nodes++;
                by[u]++;
            }
        }
    }
    printf("%s.refA: nodes %llu: base*100 + residual == exact for all (units 1/10/100: %llu %llu %llu)\n", pre.c_str(),
           (unsigned long long)nodes, (unsigned long long)by[0], (unsigned long long)by[1], (unsigned long long)by[2]);
    return 0;
}

static int cmd_addr(char** a) {
    M ex = mapf(a[2]);
    const int32_t* E = (const int32_t*)ex.p;
    const size_t N = ex.n / 8;
    const std::string d = a[3];
    for (int v = 0; v < 2; v++) {
        const char* vn = v == 0 ? "e5" : "e7";
        M h = mapf(d + "/addr.for64." + vn + ".hdr"), b = mapf(d + "/addr.for64." + vn + ".bits");
        if (h.n != 16 * ((N + 63) / 64)) fail("hdr size");
        for (size_t i = 0; i < N; i++) {
            const uint8_t* hp = h.p + 16 * (i / 64);
            const uint32_t mnl = rd<uint32_t>(hp), mng = rd<uint32_t>(hp + 4), off = rd<uint32_t>(hp + 8);
            const int wl = hp[12], wg = hp[13];
            Bits bits{b.p + off, uint64_t(i % 64) * (wl + wg)};
            const int32_t la = int32_t((mnl + bits.get(wl)) ^ 0x80000000u), lo = int32_t((mng + bits.get(wg)) ^ 0x80000000u);
            const int32_t xl = v == 0 ? base_key(E[2 * i]) : E[2 * i], xg = v == 0 ? base_key(E[2 * i + 1]) : E[2 * i + 1];
            if (la != xl || lo != xg) fail("addr for64", v, i);
        }
        printf("addr.for64.%s: %zu points all equal\n", vn, N);
    }
    M r = mapf(d + "/addr.ref14");
    Bits bits{r.p, 0};
    for (size_t i = 0; i < N; i++)
        for (int c = 0; c < 2; c++) {
            const int32_t res = int32_t(bits.get(7)) - 50, e = E[2 * i + c];
            if (int64_t(base_key(e)) * 100 + res != e) fail("ref14", i);
        }
    printf("addr.ref14: %zu points: base*100 + residual == exact for all\n", N);
    return 0;
}

int main(int argc, char** argv) {
    if (argc < 2) fail("usage");
    std::string c = argv[1];
    if (c == "ways" && argc == 6) return cmd_ways(argv);
    if (c == "addr" && argc == 4) return cmd_addr(argv);
    fail("usage");
}
