// Read-cost + exactness check of layout B against today's admin_vertices format.
// For every live polygon, decodes today's block exactly like server/src/lib.rs decode_polygon_verts
// (f32 = (f64)bbox_min + q * scale, cast to f32) and the layout-B block (byte planes, zigzag
// wrapping deltas, prefix sum at the polygon's own width), and compares the two f32 arrays BIT FOR
// BIT. Then times both decoders: a sequential pass over every polygon, and N random polygon
// decodes (query-like access), best of 3.
// usage: bench_b <admin_polygons.bin> <today admin_vertices.bin> <B file> [random decodes]
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>

typedef struct { uint32_t off, cnt, name; uint8_t lvl, pto, p2, p3; float area; uint16_t cc, p4; } Poly;

static const uint8_t* map(const char* p, size_t* n) {
    int fd = open(p, O_RDONLY);
    if (fd < 0) { perror(p); exit(1); }
    struct stat st; fstat(fd, &st); *n = st.st_size;
    void* m = mmap(0, *n, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (m == MAP_FAILED) { perror("mmap"); exit(1); }
    return m;
}
static double now(void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return t.tv_sec + t.tv_nsec * 1e-9; }
static double scale_of(uint8_t e) { return e == 0 ? 1e-5 : e == 1 ? 1e-4 : e == 2 ? 1e-6 : 1e-7; }

static void dec_today(const uint8_t* b, uint32_t c, float* lat, float* lng) {
    uint8_t e = b[0]; float bl, bg; memcpy(&bl, b + 2, 4); memcpy(&bg, b + 6, 4);
    double s = scale_of(e), dl = bl, dg = bg; const uint8_t* v = b + 10;
    if (e == 3) {
        for (uint32_t k = 0; k < c; k++) { uint32_t a, g; memcpy(&a, v + 8 * k, 4); memcpy(&g, v + 8 * k + 4, 4);
            lat[k] = (float)(dl + a * s); lng[k] = (float)(dg + g * s); }
    } else {
        for (uint32_t k = 0; k < c; k++) { uint16_t a, g; memcpy(&a, v + 4 * k, 2); memcpy(&g, v + 4 * k + 2, 2);
            lat[k] = (float)(dl + a * s); lng[k] = (float)(dg + g * s); }
    }
}

static void dec_B(const uint8_t* b, uint32_t c, float* lat, float* lng) {
    uint8_t e = b[0]; float bl, bg; memcpy(&bl, b + 2, 4); memcpy(&bg, b + 6, 4);
    double s = scale_of(e), dl = bl, dg = bg; const uint8_t* p = b + 10;
    if (e == 3) {
        const uint8_t *l0 = p, *l1 = p + c, *l2 = p + 2 * c, *l3 = p + 3 * c;
        const uint8_t *g0 = p + 4 * c, *g1 = p + 5 * c, *g2 = p + 6 * c, *g3 = p + 7 * c;
        uint32_t ql = 0, qg = 0;
        for (uint32_t k = 0; k < c; k++) {
            uint32_t zl = l0[k] | l1[k] << 8 | l2[k] << 16 | (uint32_t)l3[k] << 24;
            uint32_t zg = g0[k] | g1[k] << 8 | g2[k] << 16 | (uint32_t)g3[k] << 24;
            ql += (zl >> 1) ^ (0u - (zl & 1)); qg += (zg >> 1) ^ (0u - (zg & 1));
            lat[k] = (float)(dl + ql * s); lng[k] = (float)(dg + qg * s);
        }
    } else {
        const uint8_t *l0 = p, *l1 = p + c, *g0 = p + 2 * c, *g1 = p + 3 * c;
        uint16_t ql = 0, qg = 0;
        for (uint32_t k = 0; k < c; k++) {
            uint16_t zl = l0[k] | l1[k] << 8, zg = g0[k] | g1[k] << 8;
            ql += (uint16_t)((zl >> 1) ^ (uint16_t)(0u - (zl & 1))); qg += (uint16_t)((zg >> 1) ^ (uint16_t)(0u - (zg & 1)));
            lat[k] = (float)(dl + ql * s); lng[k] = (float)(dg + qg * s);
        }
    }
}

int main(int argc, char** argv) {
    size_t np, nt, nb;
    const Poly* P = (const Poly*)map(argv[1], &np);
    const uint8_t* T = map(argv[2], &nt);
    const uint8_t* B = map(argv[3], &nb);
    long R = argc > 4 ? atol(argv[4]) : 2000000;
    size_t n = np / sizeof(Poly);
    if (nt != nb) { printf("size differs %zu vs %zu\n", nt, nb); return 1; }
    uint32_t* live = malloc(n * 4); size_t L = 0; uint32_t maxc = 0; uint64_t verts = 0;
    for (size_t i = 0; i < n; i++)
        if (P[i].cnt && P[i].off != 0xFFFFFFFFu) { live[L++] = i; if (P[i].cnt > maxc) maxc = P[i].cnt; verts += P[i].cnt; }
    float *a = malloc(maxc * 4), *b = malloc(maxc * 4), *c = malloc(maxc * 4), *d = malloc(maxc * 4);
    uint64_t mism = 0, mpoly = 0;
    for (size_t j = 0; j < L; j++) {
        const Poly* p = &P[live[j]];
        dec_today(T + p->off, p->cnt, a, b); dec_B(B + p->off, p->cnt, c, d);
        uint64_t m = 0;
        for (uint32_t k = 0; k < p->cnt; k++) m += (memcmp(a + k, c + k, 4) != 0) + (memcmp(b + k, d + k, 4) != 0);
        mism += m; mpoly += m != 0;
    }
    printf("live polygons %zu, vertices %llu: f32 bit-pattern mismatches today vs B: %llu values in %llu polygons\n",
           L, (unsigned long long)verts, (unsigned long long)mism, (unsigned long long)mpoly);
    uint32_t* rnd = malloc(R * 4); uint64_t rv = 0; srand(1);
    for (long r = 0; r < R; r++) { rnd[r] = live[((uint64_t)rand() * 2654435761u + rand()) % L]; rv += P[rnd[r]].cnt; }
    double best[4] = {1e9, 1e9, 1e9, 1e9}; volatile float sink = 0;
    for (int rep = 0; rep < 3; rep++) {
        for (int f = 0; f < 2; f++) {
            void (*dec)(const uint8_t*, uint32_t, float*, float*) = f ? dec_B : dec_today;
            const uint8_t* src = f ? B : T;
            double t = now();
            for (size_t j = 0; j < L; j++) { const Poly* p = &P[live[j]]; dec(src + p->off, p->cnt, a, b); sink += a[0]; }
            t = now() - t; if (t < best[f]) best[f] = t;
            t = now();
            for (long r = 0; r < R; r++) { const Poly* p = &P[rnd[r]]; dec(src + p->off, p->cnt, a, b); sink += b[0]; }
            t = now() - t; if (t < best[2 + f]) best[2 + f] = t;
        }
    }
    printf("sequential pass, all polygons: today %.3f s (%.2f ns/vertex), B %.3f s (%.2f ns/vertex), B/today %.2fx\n",
           best[0], best[0] * 1e9 / verts, best[1], best[1] * 1e9 / verts, best[1] / best[0]);
    printf("%ld random polygon decodes (%llu vertices): today %.3f s (%.2f us/decode), B %.3f s (%.2f us/decode), B/today %.2fx\n",
           R, (unsigned long long)rv, best[2], best[2] * 1e6 / R, best[3], best[3] * 1e6 / R, best[3] / best[2]);
    return mism != 0;
}
