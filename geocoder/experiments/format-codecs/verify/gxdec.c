// Independent full decode of the index track's block geo index ("gx adj", from geo_gx.py's format
// description) compared row by row with today's geo_cells.bin + entries files.
//   gx_top.bin : per block of B rows: u64 first cell | u32 byte offset of the block in gx_rows |
//                u32 cur street | u32 cur addr | u32 cur interp (offset of the list in effect at block
//                start in that table's entries file, NO_DATA if none yet)
//   gx_rows.bin: per row a header byte s | a<<2 | i<<4 | d<<6 (+ LEB128(step-4) when d == 3);
//                table state 0 absent, 1 next stored list (cur + 2 + 4*count, or 0 for the first),
//                2 same list as this table's previous list; d: cell step in level-17 units (2^27) is
//                d+1, first row of a block takes its cell from gx_top
//   <t>_entries: [u16 count][u32 ids] lists back to back (consecutive repeats stored once)
// Today's lists are compared after dropping repeated ids inside a list (first copy kept), which is
// the clean-up the track applied (make_clean.py).
// usage: gxdec <B> <gx_dir> <today_dir>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

static const uint8_t* map(const char* d, const char* f, size_t* n) {
    char p[4096]; snprintf(p, sizeof p, "%s/%s", d, f);
    int fd = open(p, O_RDONLY); if (fd < 0) { perror(p); exit(2); }
    struct stat st; fstat(fd, &st); *n = st.st_size;
    const uint8_t* m = mmap(0, *n ? *n : 1, PROT_READ, MAP_SHARED, fd, 0); close(fd); return m;
}
static inline uint32_t rd32(const uint8_t* p) { uint32_t v; memcpy(&v, p, 4); return v; }
static inline uint64_t rd64(const uint8_t* p) { uint64_t v; memcpy(&v, p, 8); return v; }
static inline uint16_t rd16(const uint8_t* p) { uint16_t v; memcpy(&v, p, 2); return v; }
static const char* TF[3] = {"street_entries.bin", "addr_entries.bin", "interp_entries.bin"};

int main(int argc, char** argv) {
    if (argc != 4) { fprintf(stderr, "usage\n"); return 2; }
    size_t B = strtoul(argv[1], 0, 10), ntop, nrows, ng, ne[3], te[3];
    const uint8_t* top = map(argv[2], "gx_top.bin", &ntop);
    const uint8_t* rows = map(argv[2], "gx_rows.bin", &nrows);
    const uint8_t* g = map(argv[3], "geo_cells.bin", &ng);
    const uint8_t *e[3], *t[3];
    for (int k = 0; k < 3; k++) { e[k] = map(argv[2], TF[k], &ne[k]); t[k] = map(argv[3], TF[k], &te[k]); }
    size_t R = ng / 20, NB = ntop / 24;
    if (NB != (R + B - 1) / B) { fprintf(stderr, "block count %zu vs rows %zu\n", NB, R); return 3; }
    uint64_t bad_cell = 0, bad_list = 0, bad_state = 0, lists[3] = {0}, new_lists[3] = {0}, maxend[3] = {0};
    uint32_t* clean = malloc(4 * 65536);
    size_t p = 0;
    for (size_t bk = 0; bk < NB; bk++) {
        const uint8_t* tp = top + 24 * bk;
        uint64_t cell = rd64(tp);
        if (rd32(tp + 8) != p) { fprintf(stderr, "block %zu row offset %u vs %zu\n", bk, rd32(tp + 8), p); return 3; }
        uint32_t cur[3] = {rd32(tp + 12), rd32(tp + 16), rd32(tp + 20)};
        size_t r1 = (bk + 1) * B < R ? (bk + 1) * B : R;
        for (size_t r = bk * B; r < r1; r++) {
            uint8_t h = rows[p++];
            int d = h >> 6;
            uint64_t step = d + 1;
            if (d == 3) { uint64_t v = 0; int s = 0; for (;;) { uint8_t c = rows[p++]; v |= (uint64_t)(c & 0x7f) << s; if (!(c & 0x80)) break; s += 7; } step = v + 4; }
            if (r != bk * B) cell += step << 27;
            const uint8_t* gr = g + 20 * r;
            if (cell != rd64(gr)) bad_cell++;
            for (int k = 0; k < 3; k++) {
                int st = (h >> (2 * k)) & 3;
                uint32_t to = rd32(gr + 8 + 4 * k);
                if (st == 0) { if (to != 0xFFFFFFFFu) bad_state++; continue; }
                if (st == 1) { cur[k] = cur[k] == 0xFFFFFFFFu ? 0 : cur[k] + 2 + 4 * rd16(e[k] + cur[k]); new_lists[k]++; }
                else if (st != 2) { bad_state++; continue; }
                if (to == 0xFFFFFFFFu) { bad_state++; continue; }
                lists[k]++;
                // today's list, repeated ids dropped (first copy kept)
                uint16_t c = rd16(t[k] + to), m = 0;
                for (uint16_t a = 0; a < c; a++) {
                    uint32_t id = rd32(t[k] + to + 2 + 4 * a); int dup = 0;
                    for (uint16_t b2 = 0; b2 < m; b2++) if (clean[b2] == id) { dup = 1; break; }
                    if (!dup) clean[m++] = id;
                }
                uint16_t c2 = rd16(e[k] + cur[k]);
                if (c2 != m || memcmp(e[k] + cur[k] + 2, clean, 4 * (size_t)m) != 0) bad_list++;
                uint64_t end = (uint64_t)cur[k] + 2 + 4 * c2;
                if (end > maxend[k]) maxend[k] = end;
            }
        }
    }
    printf("rows %zu blocks %zu rows_bytes %zu/%zu | lists s/a/i %llu/%llu/%llu new %llu/%llu/%llu | entries end==size s %d a %d i %d | bad cell %llu state %llu list %llu\n",
           R, NB, p, nrows, (unsigned long long)lists[0], (unsigned long long)lists[1], (unsigned long long)lists[2],
           (unsigned long long)new_lists[0], (unsigned long long)new_lists[1], (unsigned long long)new_lists[2],
           maxend[0] == ne[0], maxend[1] == ne[1], maxend[2] == ne[2],
           (unsigned long long)bad_cell, (unsigned long long)bad_state, (unsigned long long)bad_list);
    return (bad_cell || bad_state || bad_list || p != nrows) ? 1 : 0;
}
