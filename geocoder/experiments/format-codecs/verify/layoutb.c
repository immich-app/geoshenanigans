// Independent re-implementation of "layout B" (four-changes / stack layouts.py), written from the
// format description only:
//   per polygon block (same offset and size as today): 10-byte header verbatim, then the
//   wrapping zigzag deltas (first vertex absolute) of q_lat then q_lng at the polygon's own width
//   w (2 B for enc 0/1/2, 4 B for enc 3), as byte planes: lat b0 | lat b1 [| b2 | b3] | lng b0 | ...
// usage: layoutb enc|dec <admin_polygons.bin> <in> <out>
//   enc: in = today's admin_vertices.bin, out = layout B; dec: in = layout B, out = today's format.
// Also prints vertex/encoding counts. Reads ONLY the polygons file and <in>.
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct { uint64_t off, cnt; } Poly;
static int cmp_poly(const void* a, const void* b) {
    const Poly* x = a; const Poly* y = b;
    return x->off < y->off ? -1 : x->off > y->off;
}
static uint8_t* slurp(const char* p, size_t* n) {
    FILE* f = fopen(p, "rb"); if (!f) { perror(p); exit(2); }
    fseek(f, 0, SEEK_END); *n = ftell(f); fseek(f, 0, SEEK_SET);
    uint8_t* b = malloc(*n ? *n : 1);
    if (fread(b, 1, *n, f) != *n) { perror("read"); exit(2); }
    fclose(f); return b;
}
static inline uint32_t zz(uint32_t d, int bits) {  // d is a wrapped delta in [0, 2^bits)
    uint32_t half = bits == 32 ? 0x80000000u : (1u << (bits - 1));
    int64_t s = d >= half ? (int64_t)d - (bits == 32 ? 0x100000000LL : (1LL << bits)) : (int64_t)d;
    return (uint32_t)(s >= 0 ? 2 * s : -2 * s - 1);
}
static inline uint32_t unzz(uint32_t z) { return (z >> 1) ^ (uint32_t)(-(int32_t)(z & 1)); }

int main(int argc, char** argv) {
    if (argc != 5) { fprintf(stderr, "usage\n"); return 2; }
    int enc_mode = strcmp(argv[1], "enc") == 0;
    size_t np, nv;
    uint8_t* pb = slurp(argv[2], &np);
    uint8_t* in = slurp(argv[3], &nv);
    uint8_t* out = calloc(nv ? nv : 1, 1);
    size_t P = np / 24, L = 0;
    Poly* ps = malloc(sizeof(Poly) * P);
    for (size_t i = 0; i < P; i++) {
        uint32_t vo, vc; memcpy(&vo, pb + 24 * i, 4); memcpy(&vc, pb + 24 * i + 4, 4);
        if (vc > 0 && vo != 0xFFFFFFFFu) { ps[L].off = vo; ps[L].cnt = vc; L++; }
    }
    qsort(ps, L, sizeof(Poly), cmp_poly);
    uint64_t pos = 0, verts = 0, by_enc[4] = {0}, vby_enc[4] = {0};
    for (size_t j = 0; j < L; j++) {
        uint64_t off = ps[j].off, c = ps[j].cnt;
        if (off != pos) { fprintf(stderr, "gap/overlap at polygon %zu: off %llu expected %llu\n", j, (unsigned long long)off, (unsigned long long)pos); return 3; }
        uint8_t e = in[off];
        if (e > 3) { fprintf(stderr, "bad enc %u\n", e); return 3; }
        int w = e == 3 ? 4 : 2, bits = 8 * w;
        uint64_t mask = bits == 32 ? 0xFFFFFFFFull : ((1ull << bits) - 1);
        memcpy(out + off, in + off, 10);
        const uint8_t* src = in + off + 10;
        uint8_t* dst = out + off + 10;
        for (int comp = 0; comp < 2; comp++) {
            uint32_t prev = 0;
            for (uint64_t k = 0; k < c; k++) {
                if (enc_mode) {
                    uint32_t q = 0;
                    for (int b = 0; b < w; b++) q |= (uint32_t)src[k * 2 * w + comp * w + b] << (8 * b);
                    uint32_t d = (uint32_t)(((uint64_t)q - prev) & mask);
                    uint32_t z = zz(d, bits);
                    for (int b = 0; b < w; b++) dst[(comp * w + b) * c + k] = (uint8_t)(z >> (8 * b));
                    prev = q;
                } else {
                    uint32_t z = 0;
                    for (int b = 0; b < w; b++) z |= (uint32_t)src[(comp * w + b) * c + k] << (8 * b);
                    uint32_t q = (uint32_t)(((uint64_t)prev + unzz(z)) & mask);
                    for (int b = 0; b < w; b++) dst[k * 2 * w + comp * w + b] = (uint8_t)(q >> (8 * b));
                    prev = q;
                }
            }
        }
        pos = off + 10 + c * 2 * w;
        verts += c; by_enc[e]++; vby_enc[e] += c;
    }
    if (pos != nv) { fprintf(stderr, "blocks end at %llu, file %zu\n", (unsigned long long)pos, nv); return 3; }
    FILE* f = fopen(argv[4], "wb"); fwrite(out, 1, nv, f); fclose(f);
    printf("%s: polygons %zu vertices %llu bytes %zu | enc0 %llu/%llu enc1 %llu/%llu enc2 %llu/%llu enc3 %llu/%llu (polys/verts)\n",
           argv[1], L, (unsigned long long)verts, nv,
           (unsigned long long)by_enc[0], (unsigned long long)vby_enc[0], (unsigned long long)by_enc[1], (unsigned long long)vby_enc[1],
           (unsigned long long)by_enc[2], (unsigned long long)vby_enc[2], (unsigned long long)by_enc[3], (unsigned long long)vby_enc[3]);
    return 0;
}
