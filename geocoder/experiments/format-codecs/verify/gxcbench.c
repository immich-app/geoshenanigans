// Warm lookup cost: today's geo index (binary search over 20-byte geo_cells rows + u16/u32 lists)
// vs the combined block index (gxc b128 delta inline): binary search over gxc_top, decode up to B rows,
// parse the varint lists passed on the way. Both return (sum of street ids, sum of interp ids, addr
// count) for the same random present cells; results are cross-checked through the remap tables
// (sums of remapped ids). Single thread, data in page cache.
// usage: gxcbench <gxc_dir> <B> <today_dir> <remap_s> <remap_i> <n_lookups>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

static const uint8_t* map2(const char* d, const char* f, size_t* n) {
    char p[4096];
    if (*f) snprintf(p, sizeof p, "%s/%s", d, f); else snprintf(p, sizeof p, "%s", d);
    int fd = open(p, O_RDONLY); if (fd < 0) { perror(p); exit(2); }
    struct stat st; fstat(fd, &st); *n = st.st_size;
    const uint8_t* m = mmap(0, *n ? *n : 1, PROT_READ, MAP_SHARED, fd, 0); close(fd);
    volatile uint64_t s = 0; for (size_t i = 0; i < *n; i += 4096) s += m[i];  // warm
    return m;
}
static inline uint32_t rd32(const uint8_t* p) { uint32_t v; memcpy(&v, p, 4); return v; }
static inline uint64_t rd64(const uint8_t* p) { uint64_t v; memcpy(&v, p, 8); return v; }
static inline uint16_t rd16(const uint8_t* p) { uint16_t v; memcpy(&v, p, 2); return v; }
static inline uint64_t uv(const uint8_t* s, uint64_t* p) {
    uint64_t v = 0; int sh = 0;
    for (;;) { uint8_t c = s[(*p)++]; v |= (uint64_t)(c & 0x7f) << sh; if (!(c & 0x80)) return v; sh += 7; }
}
typedef struct { uint64_t s, i, a; } Res;
static double now(void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return t.tv_sec + t.tv_nsec * 1e-9; }

static const uint8_t *G, *E[3], *TOP, *ROWS, *SS, *IS;
static size_t R, NB, B;
static const uint32_t *RMS, *RMI;

static Res today(uint64_t cell) {
    Res r = {0, 0, 0};
    size_t lo = 0, hi = R;
    while (lo < hi) { size_t m = (lo + hi) / 2; if (rd64(G + 20 * m) < cell) lo = m + 1; else hi = m; }
    if (lo == R || rd64(G + 20 * lo) != cell) return r;
    const uint8_t* g = G + 20 * lo;
    for (int k = 0; k < 3; k++) {
        uint32_t o = rd32(g + 8 + 4 * k);
        if (o == 0xFFFFFFFFu) continue;
        uint16_t c = rd16(E[k] + o);
        for (uint16_t x = 0; x < c; x++) {
            uint32_t id = rd32(E[k] + o + 2 + 4 * x);
            if (k == 0) r.s += RMS[id]; else if (k == 2) r.i += RMI[id]; else r.a++;
        }
    }
    return r;
}

// parse a stored list at *p (advancing it); first id is a zigzag delta from *pf (updated)
static inline uint64_t list_sum(const uint8_t* s, uint64_t* p, uint32_t* pf, int known, uint32_t first_known) {
    uint32_t c = (uint32_t)uv(s, p) + 1;
    uint64_t f = uv(s, p);
    uint32_t x = known ? first_known : (uint32_t)((int64_t)*pf + ((f & 1) ? -(int64_t)((f + 1) >> 1) : (int64_t)(f >> 1)));
    *pf = x;
    uint64_t sum = x;
    for (uint32_t k = 1; k < c; k++) { x += (uint32_t)uv(s, p) + 1; sum += x; }
    return sum;
}

static Res gxc(uint64_t cell) {
    Res r = {0, 0, 0};
    size_t lo = 0, hi = NB;   // last block with first cell <= cell
    while (lo < hi) { size_t m = (lo + hi) / 2; if (rd64(TOP + 32 * m) <= cell) lo = m + 1; else hi = m; }
    if (lo == 0) return r;
    size_t bk = lo - 1;
    const uint8_t* tp = TOP + 32 * bk;
    uint64_t c = rd64(tp), p = rd32(tp + 8);
    uint64_t cur[2] = {rd32(tp + 12), rd32(tp + 16)};
    uint64_t addr = rd32(tp + 20);
    uint32_t pf[2] = {rd32(tp + 24), rd32(tp + 28)};
    const uint8_t* S[2] = {SS, IS};
    uint64_t sum[2] = {0, 0}, end[2] = {0, 0};
    int loaded[2] = {0, 0};
    size_t r1 = (bk + 1) * B < R ? (bk + 1) * B : R;
    for (size_t row = bk * B; row < r1; row++) {
        uint8_t h = ROWS[p++];
        int d = h >> 6;
        uint64_t step = d + 1;
        if (d == 3) step = uv(ROWS, &p) + 4;
        if (row != bk * B) c += step << 27;
        if (c > cell) return r;
        uint64_t ac = 0;
        if ((h >> 2) & 1) ac = uv(ROWS, &p);
        for (int k = 0; k < 2; k++) {
            int st = (h >> (k ? 4 : 0)) & 3;
            if (st == 0) continue;
            if (!loaded[k] && cur[k] != 0xFFFFFFFFull) {   // the list in effect at block start
                uint64_t q = cur[k]; sum[k] = list_sum(S[k], &q, &pf[k], 1, pf[k]); end[k] = q;
            }
            if (st == 1) {
                uint64_t q = cur[k] == 0xFFFFFFFFull ? 0 : end[k];
                cur[k] = q;
                sum[k] = list_sum(S[k], &q, &pf[k], 0, 0); end[k] = q;
            }
            loaded[k] = 1;
        }
        if (c == cell) {
            if ((h & 3)) r.s = sum[0];
            if (((h >> 4) & 3)) r.i = sum[1];
            r.a = ac;
            return r;
        }
        addr += ac;
    }
    return r;
}

int main(int argc, char** argv) {
    if (argc != 7) { fprintf(stderr, "usage\n"); return 2; }
    size_t n, nt, nr, ns, ni, ng, ne;
    B = strtoul(argv[2], 0, 10);
    TOP = map2(argv[1], "gxc_top.bin", &nt); ROWS = map2(argv[1], "gxc_rows.bin", &nr);
    SS = map2(argv[1], "gxc_street.bin", &ns); IS = map2(argv[1], "gxc_interp.bin", &ni);
    G = map2(argv[3], "geo_cells.bin", &ng);
    const char* TF[3] = {"street_entries.bin", "addr_entries.bin", "interp_entries.bin"};
    for (int k = 0; k < 3; k++) E[k] = map2(argv[3], TF[k], &ne);
    RMS = (const uint32_t*)map2(argv[4], "", &n); RMI = (const uint32_t*)map2(argv[5], "", &n);
    R = ng / 20; NB = nt / 32;
    size_t L = strtoul(argv[6], 0, 10);
    uint64_t* cells = malloc(8 * L);
    uint64_t seed = 88172645463325252ull;
    for (size_t k = 0; k < L; k++) { seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17; cells[k] = rd64(G + 20 * (seed % R)); }
    uint64_t mism = 0, acc = 0;
    for (size_t k = 0; k < L; k++) {   // correctness: street / interp sums through the remaps; addr counts may
        Res a = today(cells[k]), b = gxc(cells[k]);  // differ only where today's list has repeated ids
        if (a.s != b.s || a.i != b.i) mism++;
    }
    for (int rep = 0; rep < 2; rep++) {
        double t0 = now();
        for (size_t k = 0; k < L; k++) { Res a = today(cells[k]); acc += a.s + a.i + a.a; }
        double t1 = now();
        for (size_t k = 0; k < L; k++) { Res b = gxc(cells[k]); acc += b.s + b.i + b.a; }
        double t2 = now();
        if (rep == 1) printf("lookups %zu | today %.0f ns/lookup | combined block index B=%zu %.0f ns/lookup | street/interp mismatches %llu (%llu)\n",
                             L, (t1 - t0) * 1e9 / L, B, (t2 - t1) * 1e9 / L, (unsigned long long)mism, (unsigned long long)(acc & 1));
    }
    return 0;
}
