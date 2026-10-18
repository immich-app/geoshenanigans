// Independent decoder for the addresses track's Tier B "abs2" (written from the format description):
//   addr_points (columns, N u32 each): lat | lng | housenumber | street | parent_way | poly_offset
//     (poly_offset = byte offset of the record's footprint blob in addr_vertices, NO_DATA = none)
//   addr_vertices, one blob per footprint:
//     tag u8 = enc | wide<<2 | small<<3 | closed<<4; count = LEB128 (stored = open-ring vertices);
//     bmin: small -> i8 lat, i8 lng residuals in f32-order-key units against the prediction
//           pred = f32( f64(point f32) - (sum of q over the closed ring / n) * scale ) [f64 math],
//           else raw f32 lat, f32 lng; then count q_lat values, then count q_lng values, u8 (wide=0)
//           or u16 LE (wide=1). closed: the decoder re-appends the first vertex.
// Output: today's 28-byte AddrPoint records + today's addr_vertices blob (10-byte header + u16 pairs),
// footprints laid out back to back in record order (vertex_offset = running sum, vertex_count = full count).
// usage: abs2dec <tb_addr_points> <tb_addr_vertices> <out_points> <out_vertices>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

static const uint8_t* map(const char* p, size_t* n) {
    int fd = open(p, O_RDONLY); if (fd < 0) { perror(p); exit(2); }
    struct stat st; fstat(fd, &st); *n = st.st_size;
    const uint8_t* m = mmap(0, *n ? *n : 1, PROT_READ, MAP_SHARED, fd, 0); close(fd); return m;
}
static inline int64_t fkey(uint32_t b) { return (b & 0x80000000u) ? (int64_t)0xFFFFFFFFu - b : (int64_t)(b ^ 0x80000000u); }
static inline uint32_t unfkey(int64_t k) { return (k & 0x80000000) ? (uint32_t)(k ^ 0x80000000) : (uint32_t)(0xFFFFFFFFu - k); }
static const double SCALE[3] = {1e-5, 1e-4, 1e-6};

int main(int argc, char** argv) {
    if (argc != 5) { fprintf(stderr, "usage\n"); return 2; }
    size_t np_, nv;
    const uint32_t* col = (const uint32_t*)map(argv[1], &np_);
    const uint8_t* s = map(argv[2], &nv);
    size_t N = np_ / 24;
    const uint32_t *lat = col, *lng = col + N, *hn = col + 2 * N, *st = col + 3 * N, *pw = col + 4 * N, *vo = col + 5 * N;
    FILE* fp = fopen(argv[3], "wb"); FILE* fv = fopen(argv[4], "wb");
    uint32_t* ql = malloc(4 * 70000); uint32_t* qg = malloc(4 * 70000);
    uint8_t* blob = malloc(10 + 4 * 70000);
    uint64_t out_off = 0, polys = 0, verts = 0, small_n = 0, closed_n = 0, max_end = 0, bad = 0;
    for (size_t i = 0; i < N; i++) {
        uint32_t rec[7] = {lat[i], lng[i], hn[i], st[i], pw[i], 0xFFFFFFFFu, 0};
        if (vo[i] != 0xFFFFFFFFu) {
            size_t p = vo[i];
            uint8_t tag = s[p++];
            int enc = tag & 3, wide = (tag >> 2) & 1, small = (tag >> 3) & 1, closed = (tag >> 4) & 1;
            if (enc > 2) { bad++; continue; }
            uint32_t cnt = 0; int sh = 0;
            for (;;) { uint8_t c = s[p++]; cnt |= (uint32_t)(c & 0x7f) << sh; if (!(c & 0x80)) break; sh += 7; }
            if (cnt + closed > 69999) { bad++; continue; }
            size_t bm = p;
            p += small ? 2 : 8;
            int64_t suml = 0, sumg = 0;
            for (uint32_t k = 0; k < cnt; k++) {
                ql[k] = wide ? (uint32_t)s[p + 2 * k] | ((uint32_t)s[p + 2 * k + 1] << 8) : s[p + k];
                suml += ql[k];
            }
            p += (size_t)cnt * (wide ? 2 : 1);
            for (uint32_t k = 0; k < cnt; k++) {
                qg[k] = wide ? (uint32_t)s[p + 2 * k] | ((uint32_t)s[p + 2 * k + 1] << 8) : s[p + k];
                sumg += qg[k];
            }
            p += (size_t)cnt * (wide ? 2 : 1);
            if (p > max_end) max_end = p;
            uint32_t full = cnt + closed;
            if (closed) { ql[cnt] = ql[0]; qg[cnt] = qg[0]; suml += ql[0]; sumg += qg[0]; }
            uint32_t bl, bg;
            if (small) {
                float pf, gf; memcpy(&pf, &lat[i], 4); memcpy(&gf, &lng[i], 4);
                float predl = (float)((double)pf - ((double)suml / (double)full) * SCALE[enc]);
                float predg = (float)((double)gf - ((double)sumg / (double)full) * SCALE[enc]);
                uint32_t pl, pg; memcpy(&pl, &predl, 4); memcpy(&pg, &predg, 4);
                bl = unfkey(fkey(pl) + (int8_t)s[bm]);
                bg = unfkey(fkey(pg) + (int8_t)s[bm + 1]);
                small_n++;
            } else { memcpy(&bl, s + bm, 4); memcpy(&bg, s + bm + 4, 4); }
            memset(blob, 0, 10);
            blob[0] = (uint8_t)enc;
            memcpy(blob + 2, &bl, 4); memcpy(blob + 6, &bg, 4);
            for (uint32_t k = 0; k < full; k++) {
                uint16_t a = (uint16_t)ql[k], b = (uint16_t)qg[k];
                memcpy(blob + 10 + 4 * k, &a, 2); memcpy(blob + 12 + 4 * k, &b, 2);
            }
            fwrite(blob, 1, 10 + 4 * (size_t)full, fv);
            rec[5] = (uint32_t)out_off; rec[6] = full;
            out_off += 10 + 4 * (uint64_t)full;
            polys++; verts += full; closed_n += closed;
        }
        fwrite(rec, 4, 7, fp);
    }
    fclose(fp); fclose(fv);
    printf("records %zu footprints %llu vertices %llu small_bmin %llu closed %llu stream_end %llu stream_size %zu bad %llu\n", N,
           (unsigned long long)polys, (unsigned long long)verts, (unsigned long long)small_n, (unsigned long long)closed_n,
           (unsigned long long)max_end, nv, (unsigned long long)bad);
    return bad ? 1 : 0;
}
