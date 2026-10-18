// Full decode of the combined geo index (gxc.py) compared row by row with today's geo_cells.bin +
// entries files: cell ids; street / interp lists == sorted(unique(remap(today's list))); addr count ==
// number of distinct ids in today's list. Decodes every block from its gx_top record only (so every
// block's random-access entry state is checked).
// usage: gxcdec <gxc_dir> <B> <abs|delta> <inline|sep> <today_dir> <remap_street> <remap_interp>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

static const uint8_t* map2(const char* d, const char* f, size_t* n) {
    char p[4096];
    if (*f) snprintf(p, sizeof p, "%s/%s", d, f); else snprintf(p, sizeof p, "%s", d);
    int fd = open(p, O_RDONLY); if (fd < 0) { perror(p); exit(2); }
    struct stat st; fstat(fd, &st); *n = st.st_size;
    const uint8_t* m = mmap(0, *n ? *n : 1, PROT_READ, MAP_SHARED, fd, 0); close(fd); return m;
}
static inline uint32_t rd32(const uint8_t* p) { uint32_t v; memcpy(&v, p, 4); return v; }
static inline uint64_t rd64(const uint8_t* p) { uint64_t v; memcpy(&v, p, 8); return v; }
static inline uint16_t rd16(const uint8_t* p) { uint16_t v; memcpy(&v, p, 2); return v; }
static inline uint64_t uv(const uint8_t* s, uint64_t* p) {
    uint64_t v = 0; int sh = 0;
    for (;;) { uint8_t c = s[(*p)++]; v |= (uint64_t)(c & 0x7f) << sh; if (!(c & 0x80)) return v; sh += 7; }
}
static int cmpu(const void* a, const void* b) { uint32_t x = *(const uint32_t*)a, y = *(const uint32_t*)b; return x < y ? -1 : x > y; }

typedef struct { const uint8_t* s; size_t n; uint64_t cur; uint32_t prevfirst; uint32_t ids[65536]; uint32_t cnt; uint64_t maxend; } Tab;
// decode the list at offset off; first id: abs value, or delta from prevfirst; known_first >= 0 overrides
static uint64_t dec_list(Tab* t, uint64_t off, int delta, int64_t known_first) {
    uint64_t p = off;
    uint32_t c = (uint32_t)uv(t->s, &p) + 1;
    uint64_t f = uv(t->s, &p);
    uint32_t first;
    if (known_first >= 0) first = (uint32_t)known_first;
    else if (delta) { int64_t d = (f & 1) ? -(int64_t)((f + 1) >> 1) : (int64_t)(f >> 1); first = (uint32_t)((int64_t)t->prevfirst + d); }
    else first = (uint32_t)f;
    t->ids[0] = first;
    for (uint32_t k = 1; k < c; k++) t->ids[k] = t->ids[k - 1] + (uint32_t)uv(t->s, &p) + 1;
    t->cnt = c;
    t->prevfirst = first;
    if (p > t->maxend) t->maxend = p;
    return p;  // end of this list = start of the next stored list
}

int main(int argc, char** argv) {
    if (argc != 8) { fprintf(stderr, "usage\n"); return 2; }
    const char* gd = argv[1]; size_t B = strtoul(argv[2], 0, 10);
    int delta = strcmp(argv[3], "delta") == 0, sep = strcmp(argv[4], "sep") == 0;
    size_t ntop, nrows, ng, nr0, nr2, te[3], nac = 0;
    const uint8_t* top = map2(gd, "gxc_top.bin", &ntop);
    const uint8_t* rows = map2(gd, "gxc_rows.bin", &nrows);
    const uint8_t* acs = sep ? map2(gd, "gxc_acnt.bin", &nac) : 0;
    Tab* ts = calloc(1, sizeof(Tab)); Tab* ti = calloc(1, sizeof(Tab));
    ts->s = map2(gd, "gxc_street.bin", &ts->n); ti->s = map2(gd, "gxc_interp.bin", &ti->n);
    const uint8_t* g = map2(argv[5], "geo_cells.bin", &ng);
    const char* TF[3] = {"street_entries.bin", "addr_entries.bin", "interp_entries.bin"};
    const uint8_t* t[3];
    for (int k = 0; k < 3; k++) t[k] = map2(argv[5], TF[k], &te[k]);
    const uint32_t* rm[3];
    rm[0] = (const uint32_t*)map2(argv[6], "", &nr0); rm[2] = (const uint32_t*)map2(argv[7], "", &nr2);
    size_t R = ng / 20, rs = 24 + (sep ? 4 : 0) + (delta ? 8 : 0), NB = ntop / rs;
    if (NB != (R + B - 1) / B || NB * rs != ntop) { fprintf(stderr, "top size\n"); return 3; }
    uint32_t* a = malloc(4 * 65536);
    uint64_t bad_cell = 0, bad_s = 0, bad_i = 0, bad_a = 0, bad_state = 0, addr_total = 0, p = 0, ap = 0;
    Tab* tabs[3] = {ts, 0, ti};
    for (size_t bk = 0; bk < NB; bk++) {
        const uint8_t* tp = top + rs * bk;
        uint64_t cell = rd64(tp);
        if (rd32(tp + 8) != p) { fprintf(stderr, "block %zu row offset mismatch\n", bk); return 3; }
        uint32_t cur[3] = {rd32(tp + 12), 0, rd32(tp + 16)};
        uint64_t aid = rd32(tp + 20);
        if (aid != (uint32_t)addr_total) bad_a++;
        size_t q = 24;
        if (sep) { if (rd32(tp + q) != ap) bad_a++; q += 4; }
        uint32_t pf[3] = {0, 0, 0};
        if (delta) { pf[0] = rd32(tp + q); pf[2] = rd32(tp + q + 4); }
        int loaded[3] = {0, 0, 0};
        for (int k = 0; k < 3; k += 2) { tabs[k]->cur = cur[k]; tabs[k]->prevfirst = pf[k]; }
        size_t r1 = (bk + 1) * B < R ? (bk + 1) * B : R;
        for (size_t r = bk * B; r < r1; r++) {
            uint8_t h = rows[p++];
            int d = h >> 6;
            uint64_t step = d + 1;
            if (d == 3) step = uv(rows, &p) + 4;
            if (r != bk * B) cell += step << 27;
            const uint8_t* gr = g + 20 * r;
            if (cell != rd64(gr)) bad_cell++;
            int as = (h >> 2) & 3;
            uint32_t ao = rd32(gr + 12);
            if (as == 1) {
                uint64_t c = sep ? uv(acs, &ap) : uv(rows, &p);
                if (ao == 0xFFFFFFFFu) bad_state++;
                else {
                    uint16_t tc = rd16(t[1] + ao);
                    for (uint16_t x = 0; x < tc; x++) a[x] = rd32(t[1] + ao + 2 + 4 * x);
                    qsort(a, tc, 4, cmpu);
                    uint64_t m = tc ? 1 : 0;
                    for (uint16_t x = 1; x < tc; x++) if (a[x] != a[x - 1]) m++;
                    if (m != c) bad_a++;
                }
                addr_total += c;
            } else if (as != 0 || ao != 0xFFFFFFFFu) bad_state++;
            for (int k = 0; k < 3; k += 2) {
                Tab* T = tabs[k];
                int st = (h >> (2 * k)) & 3;
                uint32_t to = rd32(gr + 8 + 4 * k);
                if (st == 0) { if (to != 0xFFFFFFFFu) bad_state++; continue; }
                if (to == 0xFFFFFFFFu || st == 3) { bad_state++; continue; }
                if (st == 1) {
                    uint64_t next = 0;
                    if (T->cur != 0xFFFFFFFFull) {
                        if (!loaded[k]) next = dec_list(T, T->cur, delta, delta ? (int64_t)T->prevfirst : -1);
                        else { uint64_t pp = T->cur; uint64_t c = uv(T->s, &pp) + 1; uv(T->s, &pp); for (uint64_t x = 1; x < c; x++) uv(T->s, &pp); next = pp; }
                    }
                    if (!loaded[k] && T->cur != 0xFFFFFFFFull) { /* prevfirst now = cur list's first */ }
                    dec_list(T, next, delta, -1);
                    T->cur = next; loaded[k] = 1;
                } else if (!loaded[k]) {
                    dec_list(T, T->cur, delta, delta ? (int64_t)T->prevfirst : -1);
                    loaded[k] = 1;
                }
                uint16_t tc = rd16(t[k] + to);
                uint32_t m = 0;
                for (uint16_t x = 0; x < tc; x++) a[x] = rm[k][rd32(t[k] + to + 2 + 4 * x)];
                qsort(a, tc, 4, cmpu);
                for (uint16_t x = 0; x < tc; x++) if (x == 0 || a[x] != a[x - 1]) a[m++] = a[x];
                if (m != T->cnt || memcmp(a, T->ids, 4 * (size_t)m) != 0) { if (k == 0) bad_s++; else bad_i++; }
            }
        }
    }
    printf("rows %zu blocks %zu | rows stream %llu/%zu acnt %llu/%zu street end %llu/%zu interp end %llu/%zu | addr records %llu | bad cell %llu state %llu street %llu interp %llu addr %llu\n",
           R, NB, (unsigned long long)p, nrows, (unsigned long long)ap, nac, (unsigned long long)ts->maxend, ts->n,
           (unsigned long long)ti->maxend, ti->n, (unsigned long long)addr_total, (unsigned long long)bad_cell,
           (unsigned long long)bad_state, (unsigned long long)bad_s, (unsigned long long)bad_i, (unsigned long long)bad_a);
    int ok = !bad_cell && !bad_state && !bad_s && !bad_i && !bad_a && p == nrows && ts->maxend == ts->n && ti->maxend == ti->n && (!sep || ap == nac);
    return ok ? 0 : 1;
}
