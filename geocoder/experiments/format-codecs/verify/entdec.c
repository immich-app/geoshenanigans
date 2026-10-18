// Independent check of the streets track's cell-list encoding (entenc "gv", INLINE, DEDUP):
//   new geo_cells column value: NO_DATA = absent; bit 31 set = single id inline (low 31 bits);
//   else byte offset into the new entries file of a list: LEB128 count, LEB128 first id,
//   LEB128 (gap - 1) for the rest.
// For every geo_cells row compares the decoded list with today's list mapped through remap
// (old -> new id) and sorted. Also checks the cell ids and the addr column are unchanged.
// usage: entdec <today geo_cells> <col 0|2> <today entries> <remap.u32> <new geo_cells> <new entries>
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
static inline uint32_t rd32(const uint8_t* p) { uint32_t v; memcpy(&v, p, 4); return v; }
static inline uint16_t rd16(const uint8_t* p) { uint16_t v; memcpy(&v, p, 2); return v; }
static int cmpu(const void* a, const void* b) { uint32_t x = *(const uint32_t*)a, y = *(const uint32_t*)b; return x < y ? -1 : x > y; }
static uint32_t varint(const uint8_t* e, size_t n, size_t* p, int* err) {
    uint32_t v = 0; int s = 0;
    for (;;) { if (*p >= n || s > 28) { *err = 1; return 0; } uint8_t c = e[(*p)++]; v |= (uint32_t)(c & 0x7f) << s; if (!(c & 0x80)) return v; s += 7; }
}

int main(int argc, char** argv) {
    if (argc != 7) { fprintf(stderr, "usage\n"); return 2; }
    size_t ng, ne, nr, nng, nne;
    const uint8_t* g = map(argv[1], &ng);
    int col = atoi(argv[2]);
    const uint8_t* e = map(argv[3], &ne);
    const uint32_t* rm = (const uint32_t*)map(argv[4], &nr);
    const uint8_t* g2 = map(argv[5], &nng);
    const uint8_t* e2 = map(argv[6], &nne);
    if (ng != nng) { fprintf(stderr, "geo_cells size differs\n"); return 3; }
    size_t R = ng / 20, M = nr / 4;
    uint32_t* a = malloc(4 * 65536); uint32_t* b = malloc(4 * 65536);
    uint64_t rows = 0, lists = 0, inl = 0, bad = 0, other_bad = 0, ids = 0, maxend = 0;
    for (size_t i = 0; i < R; i++) {
        const uint8_t* r = g + 20 * i; const uint8_t* r2 = g2 + 20 * i;
        // cell id and the addr column must be untouched (the street and interp columns both change)
        if (memcmp(r, r2, 8) != 0 || memcmp(r + 12, r2 + 12, 4) != 0) other_bad++;
        uint32_t o = rd32(r + 8 + 4 * col), v = rd32(r2 + 8 + 4 * col);
        rows++;
        if (o == 0xFFFFFFFFu) { if (v != 0xFFFFFFFFu) bad++; continue; }
        lists++;
        uint16_t c = rd16(e + o);
        for (uint16_t k = 0; k < c; k++) { uint32_t id = rd32(e + o + 2 + 4 * k); if (id >= M) { bad++; id = 0; } a[k] = rm[id]; }
        qsort(a, c, 4, cmpu);
        ids += c;
        uint32_t n2;
        if (v & 0x80000000u) { n2 = 1; b[0] = v & 0x7fffffffu; inl++; }
        else {
            size_t p = v; int err = 0;
            n2 = varint(e2, nne, &p, &err);
            if (n2 > 65535) { bad++; continue; }
            uint32_t x = 0;
            for (uint32_t k = 0; k < n2; k++) { uint32_t d = varint(e2, nne, &p, &err); x = k == 0 ? d : x + d + 1; b[k] = x; }
            if (err) { bad++; continue; }
            if (p > maxend) maxend = p;
        }
        if (n2 != c || memcmp(a, b, 4 * (size_t)c) != 0) bad++;
    }
    printf("col %d: rows %llu lists %llu inline %llu ids %llu bad %llu other_columns_changed %llu entries_bytes %zu max_list_end %llu\n",
           col, (unsigned long long)rows, (unsigned long long)lists, (unsigned long long)inl, (unsigned long long)ids,
           (unsigned long long)bad, (unsigned long long)other_bad, nne, (unsigned long long)maxend);
    return bad ? 1 : 0;
}
