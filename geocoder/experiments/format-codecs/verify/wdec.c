// Independent decoder for the streets track's packed ways (waypack.cpp format), written from the
// format header only. Rebuilds today's-format ways + nodes files from <p>.ways2/.groups/.nodes2
// and writes them so they can be cmp'd with the (reordered) today's-format files.
//   ways2 : street: cnt u16 | code u16 | name u32 (8 B); interp: cnt | code | street u32 | start u32 |
//           end u32 | interpolation u8 (17 B)
//   groups: per G ways: block byte offset u32 | anchor lat bits u32 | anchor lng bits u32
//   nodes2: per way: first node zigzag(ulpkey(v) - ulpkey(anchor)) at wf bytes (lat then lng), then
//           (cnt-1) lat values at wl bits, (cnt-1) lng values at wg bits, LSB-first, byte aligned;
//           value 1 = first-order delta, 2.. = second-order deltas of the integer key
//           (hybrid key when code bit 12 set, else ulp key). code: wl bits 0-5, wg 6-11, hyb 12, wf 13-15.
// Sequential decode, but every group's stored offset is checked against the running sum (so the
// random-access path's only input, the group offset, is verified for every group).
// usage: wdec street|interp <G> <prefix> <out_ways> <out_nodes>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

static uint8_t* slurp(const char* p, size_t* n, size_t pad) {
    FILE* f = fopen(p, "rb"); if (!f) { perror(p); exit(2); }
    fseek(f, 0, SEEK_END); *n = ftell(f); fseek(f, 0, SEEK_SET);
    uint8_t* b = calloc(*n + pad, 1);
    if (fread(b, 1, *n, f) != *n) { perror("read"); exit(2); }
    fclose(f); return b;
}
static inline uint32_t rd32(const uint8_t* p) { uint32_t v; memcpy(&v, p, 4); return v; }
static inline uint16_t rd16(const uint8_t* p) { uint16_t v; memcpy(&v, p, 2); return v; }
static inline uint32_t ulpkey(uint32_t b) { return (b & 0x80000000u) ? ~b : (b | 0x80000000u); }
static inline uint32_t ulpunkey(uint32_t k) { return (k & 0x80000000u) ? (k & 0x7fffffffu) : ~k; }
static inline int32_t unzz(uint32_t z) { return (int32_t)((z >> 1) ^ (uint32_t)(-(int32_t)(z & 1))); }
// hybrid key -> f32 bits: |key| < 1e7 means value = key / 1e7 (rounded to f32), else f32 step count
// above 1.0 (bits = key - 1e7 + bits(1.0f)); sign of the key is the sign of the value.
static inline uint32_t hybdec(uint32_t k) {
    int32_t s = (int32_t)k;
    uint32_t sign = s < 0 ? 0x80000000u : 0;
    uint32_t mag = s < 0 ? (uint32_t)(-(int64_t)s) : (uint32_t)s;
    if (mag >= 10000000u) return (mag - 10000000u + 0x3f800000u) | sign;
    float f = (float)((double)mag / 1e7);
    uint32_t b; memcpy(&b, &f, 4);
    return b | sign;
}
static inline uint32_t hybenc(uint32_t b) {  // inverse for values the encoder accepted (checked by the caller)
    uint32_t a = b & 0x7fffffffu, mag;
    if (a >= 0x3f800000u) mag = a - 0x3f800000u + 10000000u;
    else { float f; memcpy(&f, &a, 4); mag = (uint32_t)nearbyint((double)f * 1e7); }
    return (b >> 31) ? (uint32_t)(-(int64_t)mag) : mag;
}
static uint64_t getbits(const uint8_t* p, uint64_t pos, int w) {
    uint64_t v = 0;
    for (int b = 0; b < w; b++, pos++) v |= (uint64_t)((p[pos >> 3] >> (pos & 7)) & 1) << b;
    return v;
}

int main(int argc, char** argv) {
    if (argc != 6) { fprintf(stderr, "usage\n"); return 2; }
    int street = strcmp(argv[1], "street") == 0;
    size_t G = strtoul(argv[2], 0, 10), nw2, ng, nn;
    char path[4096];
    snprintf(path, sizeof path, "%s.ways2", argv[3]); uint8_t* w2 = slurp(path, &nw2, 0);
    snprintf(path, sizeof path, "%s.groups", argv[3]); uint8_t* gr = slurp(path, &ng, 0);
    snprintf(path, sizeof path, "%s.nodes2", argv[3]); uint8_t* bl = slurp(path, &nn, 16);
    size_t os = street ? 8 : 17, is = street ? 12 : 24;
    size_t W = nw2 / os;
    if (W * os != nw2 || ng != 12 * ((W + G - 1) / G)) { fprintf(stderr, "size mismatch\n"); return 3; }
    FILE* fw = fopen(argv[4], "wb"); FILE* fn = fopen(argv[5], "wb");
    uint32_t* buf = malloc(8 * 65536);
    uint8_t rec[24];
    uint64_t off = 0, node_off = 0, bad_group = 0, nodes = 0, hyb_ways = 0;
    for (size_t i = 0; i < W; i++) {
        const uint8_t* g = gr + 12 * (i / G);
        if (i % G == 0 && rd32(g) != off) bad_group++;
        uint16_t cnt = rd16(w2 + i * os), code = rd16(w2 + i * os + 2);
        memset(rec, 0, sizeof rec);
        uint32_t no = (uint32_t)node_off;
        memcpy(rec, &no, 4); memcpy(rec + 4, &cnt, 2);
        if (street) memcpy(rec + 8, w2 + i * os + 4, 4);
        else { memcpy(rec + 8, w2 + i * os + 4, 12); rec[20] = w2[i * os + 16]; }
        fwrite(rec, 1, is, fw);
        if (!cnt) continue;
        int wl = code & 63, wg = (code >> 6) & 63, hyb = (code >> 12) & 1, wf = (code >> 13) & 7;
        const uint8_t* p = bl + off;
        uint32_t zl = 0, zg = 0;
        for (int b = 0; b < wf; b++) { zl |= (uint32_t)p[b] << (8 * b); zg |= (uint32_t)p[wf + b] << (8 * b); }
        uint32_t fl = ulpunkey(ulpkey(rd32(g + 4)) + (uint32_t)unzz(zl));
        uint32_t fg = ulpunkey(ulpkey(rd32(g + 8)) + (uint32_t)unzz(zg));
        buf[0] = fl; buf[1] = fg;
        const uint8_t* bits = p + 2 * wf;
        uint64_t m = cnt - 1;
        uint32_t kl = hyb ? hybenc(fl) : ulpkey(fl), kg = hyb ? hybenc(fg) : ulpkey(fg);
        uint32_t dl = 0, dg = 0;
        for (uint64_t j = 0; j < m; j++) {
            int32_t vl = unzz((uint32_t)getbits(bits, j * wl, wl));
            int32_t vg = unzz((uint32_t)getbits(bits, m * wl + j * wg, wg));
            if (j == 0) { dl = (uint32_t)vl; dg = (uint32_t)vg; } else { dl += (uint32_t)vl; dg += (uint32_t)vg; }
            kl += dl; kg += dg;
            buf[2 * (j + 1)] = hyb ? hybdec(kl) : ulpunkey(kl);
            buf[2 * (j + 1) + 1] = hyb ? hybdec(kg) : ulpunkey(kg);
        }
        if (hyb && (hybdec(hybenc(fl)) != fl || hybdec(hybenc(fg)) != fg)) bad_group++;
        fwrite(buf, 8, cnt, fn);
        off += 2 * wf + (m * (wl + wg) + 7) / 8;
        node_off += cnt; nodes += cnt; hyb_ways += hyb;
    }
    fclose(fw); fclose(fn);
    printf("%s: ways %zu nodes %llu hyb_ways %llu blocks_end %llu nodes2_size %zu bad_group_offsets %llu\n", argv[1], W,
           (unsigned long long)nodes, (unsigned long long)hyb_ways, (unsigned long long)off, nn, (unsigned long long)bad_group);
    return (bad_group || off != nn) ? 1 : 0;
}
