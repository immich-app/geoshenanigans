// Re-encode a cell entries file (street_entries / interp_entries): each list
// is today u16 count + count x u32 sorted ids, addressed by a u32 byte offset
// in geo_cells.bin (20-byte rows: u64 cell, u32 street, u32 addr, u32 interp).
//
//   entenc <geo_cells.bin> <col 0|1|2> <entries.bin> <variant> <out> [remap.u32]
//     variant: raw  (today: u16 count + u32 ids)
//              gv   (LEB128 count, LEB128 first id, LEB128 gap-1 ...)
//              gfp  (u16 count, u8 width, u32 first, gaps-1 at width bytes, byte planes)
//     remap  : optional old->new id table (u32 per old id); ids are mapped and
//              re-sorted per list (for the reordered-ways variant).
//   Writes <out> (new entries file) and <out>.cells (geo_cells with offsets
//   rewritten), verifies every list decodes to the (remapped, sorted) ids, and
//   prints sizes and list stats.
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <stdexcept>
#include <unordered_map>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

struct Map {
    const uint8_t* p = nullptr; size_t n = 0;
    explicit Map(const std::string& path) {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("open " + path);
        struct stat st; fstat(fd, &st); n = st.st_size;
        p = (const uint8_t*)mmap(nullptr, n ? n : 1, PROT_READ, MAP_SHARED, fd, 0);
        close(fd);
    }
};
static inline uint32_t rd32(const uint8_t* p) { uint32_t v; memcpy(&v, p, 4); return v; }
static inline uint16_t rd16(const uint8_t* p) { uint16_t v; memcpy(&v, p, 2); return v; }
static inline void put_varint(std::vector<uint8_t>& out, uint32_t v) {
    while (v >= 0x80) { out.push_back((uint8_t)(v | 0x80)); v >>= 7; }
    out.push_back((uint8_t)v);
}
static inline uint32_t get_varint(const uint8_t*& p) {
    uint32_t v = 0; int s = 0;
    while (true) { uint8_t c = *p++; v |= (uint32_t)(c & 0x7f) << s; if (!(c & 0x80)) return v; s += 7; }
}
static const uint32_t NO_DATA = 0xffffffffu;

static void encode(const std::string& V, const std::vector<uint32_t>& ids, std::vector<uint8_t>& out) {
    const size_t n = ids.size();
    if (V == "raw") {
        uint16_t c = (uint16_t)n; out.insert(out.end(), (uint8_t*)&c, (uint8_t*)&c + 2);
        for (uint32_t id : ids) out.insert(out.end(), (uint8_t*)&id, (uint8_t*)&id + 4);
        return;
    }
    if (V == "gv") {
        put_varint(out, (uint32_t)n);
        if (!n) return;
        put_varint(out, ids[0]);
        for (size_t i = 1; i < n; i++) put_varint(out, ids[i] - ids[i - 1] - 1);
        return;
    }
    if (V == "gfp") {
        uint16_t c = (uint16_t)n; out.insert(out.end(), (uint8_t*)&c, (uint8_t*)&c + 2);
        if (!n) return;
        uint32_t mx = 0;
        for (size_t i = 1; i < n; i++) mx |= ids[i] - ids[i - 1] - 1;
        int w = mx ? (32 - __builtin_clz(mx) + 7) / 8 : 0;
        out.push_back((uint8_t)w);
        uint32_t f = ids[0]; out.insert(out.end(), (uint8_t*)&f, (uint8_t*)&f + 4);
        for (int b = 0; b < w; b++)
            for (size_t i = 1; i < n; i++) out.push_back((uint8_t)((ids[i] - ids[i - 1] - 1) >> (8 * b)));
        return;
    }
    throw std::runtime_error("variant " + V);
}

static void decode(const std::string& V, const uint8_t* p, std::vector<uint32_t>& ids) {
    ids.clear();
    if (V == "raw") {
        uint16_t c = rd16(p); p += 2;
        for (uint16_t i = 0; i < c; i++) ids.push_back(rd32(p + 4 * i));
        return;
    }
    if (V == "gv") {
        uint32_t c = get_varint(p);
        if (!c) return;
        uint32_t v = get_varint(p); ids.push_back(v);
        for (uint32_t i = 1; i < c; i++) { v += get_varint(p) + 1; ids.push_back(v); }
        return;
    }
    if (V == "gfp") {
        uint16_t c = rd16(p); p += 2;
        if (!c) return;
        int w = *p++;
        uint32_t v = rd32(p); p += 4;
        ids.push_back(v);
        const size_t m = c - 1;
        for (size_t i = 0; i < m; i++) {
            uint32_t g = 0;
            for (int b = 0; b < w; b++) g |= (uint32_t)p[b * m + i] << (8 * b);
            v += g + 1; ids.push_back(v);
        }
        return;
    }
}

// Env options: INLINE=1 stores a one-id list in the geo_cells column as
// 0x80000000|id (no entries bytes); DEDUP=1 stores identical encoded lists
// once; CELLS_IN=<path> reads the geo_cells to rewrite from there (to stack
// the street and interp column changes); SAMPLE=1 also writes 8 x 0.75% row
// slices of the old and new geo_cells.
int main(int argc, char** argv) {
    if (argc < 6) { fprintf(stderr, "usage: see header\n"); return 2; }
    Map cells(argv[1]);
    const int col = atoi(argv[2]);
    Map ent(argv[3]);
    const std::string V = argv[4], out = argv[5];
    const bool inl = getenv("INLINE") != nullptr, dedup = getenv("DEDUP") != nullptr;
    const uint32_t* remap = nullptr; size_t remap_n = 0;
    Map* rm = nullptr;
    if (argc >= 7) { rm = new Map(argv[6]); remap = (const uint32_t*)rm->p; remap_n = rm->n / 4; }
    const size_t C = cells.n / 20;
    Map* cin = getenv("CELLS_IN") ? new Map(getenv("CELLS_IN")) : nullptr;
    std::vector<uint8_t> nc(cin ? cin->p : cells.p, (cin ? cin->p : cells.p) + cells.n);

    std::vector<uint8_t> enc; enc.reserve(ent.n);
    std::unordered_map<std::string, uint32_t> seen;
    std::vector<uint32_t> ids, back;
    size_t refs = 0, total_ids = 0, bad = 0, unsorted = 0, dangling = 0, inlined = 0, shared = 0, last_off = 0, nonmono = 0;
    uint64_t hist[8] = {0};
    std::vector<uint8_t> tmp;
    for (size_t i = 0; i < C; i++) {
        uint32_t o = rd32(cells.p + 20 * i + 8 + 4 * col);
        if (o == NO_DATA) continue;
        refs++;
        if (o < last_off) nonmono++;
        last_off = o;
        uint16_t c = rd16(ent.p + o);
        ids.resize(c);
        for (uint16_t k = 0; k < c; k++) ids[k] = rd32(ent.p + o + 2 + 4 * k);
        for (uint16_t k = 1; k < c; k++) if (ids[k] <= ids[k - 1]) unsorted++;
        if (remap) {
            for (auto& v : ids) { if (v >= remap_n) { dangling++; continue; } v = remap[v]; }
            std::sort(ids.begin(), ids.end());
        }
        total_ids += c;
        int b = c < 2 ? 0 : c < 4 ? 1 : c < 8 ? 2 : c < 16 ? 3 : c < 32 ? 4 : c < 64 ? 5 : c < 256 ? 6 : 7;
        hist[b]++;
        uint32_t nv;
        if (inl && c == 1 && ids[0] < 0x7fffffffu) { nv = 0x80000000u | ids[0]; inlined++; }
        else {
            tmp.clear();
            encode(V, ids, tmp);
            if (dedup) {
                std::string key((const char*)tmp.data(), tmp.size());
                auto it = seen.find(key);
                if (it != seen.end()) { nv = it->second; shared++; }
                else { nv = (uint32_t)enc.size(); seen.emplace(std::move(key), nv); enc.insert(enc.end(), tmp.begin(), tmp.end()); }
            } else { nv = (uint32_t)enc.size(); enc.insert(enc.end(), tmp.begin(), tmp.end()); }
        }
        memcpy(&nc[20 * i + 8 + 4 * col], &nv, 4);
    }
    enc.resize(enc.size() + 8, 0);
    // verify every cell through the new column + entries
    for (size_t i = 0; i < C; i++) {
        uint32_t o = rd32(cells.p + 20 * i + 8 + 4 * col);
        uint32_t nv = rd32(&nc[20 * i + 8 + 4 * col]);
        if (o == NO_DATA) { if (nv != NO_DATA) bad++; continue; }
        uint16_t c = rd16(ent.p + o);
        ids.resize(c);
        for (uint16_t k = 0; k < c; k++) ids[k] = rd32(ent.p + o + 2 + 4 * k);
        if (remap) { for (auto& v : ids) if (v < remap_n) v = remap[v]; std::sort(ids.begin(), ids.end()); }
        if (nv & 0x80000000u) { back.assign(1, nv & 0x7fffffffu); }
        else decode(V, enc.data() + nv, back);
        if (back != ids) bad++;
    }
    const size_t out_bytes = enc.size() - 8;
    FILE* f = fopen(out.c_str(), "wb"); fwrite(enc.data(), 1, out_bytes, f); fclose(f);
    if (!getenv("NOCELLS")) { f = fopen((out + ".cells").c_str(), "wb"); fwrite(nc.data(), 1, nc.size(), f); fclose(f); }
    if (getenv("SAMPLE")) {
        size_t per = (size_t)(C * 0.06 / 8);
        std::vector<uint8_t> sn, so;
        for (int k = 0; k < 8; k++) {
            size_t r0 = (size_t)((double)C * (k + 0.5) / 8) - per / 2;
            sn.insert(sn.end(), nc.begin() + 20 * r0, nc.begin() + 20 * (r0 + per));
            so.insert(so.end(), cells.p + 20 * r0, cells.p + 20 * (r0 + per));
        }
        f = fopen((out + ".cells.sample").c_str(), "wb"); fwrite(sn.data(), 1, sn.size(), f); fclose(f);
        f = fopen((out + ".todaycells.sample").c_str(), "wb"); fwrite(so.data(), 1, so.size(), f); fclose(f);
    }
    printf("%s inline=%d dedup=%d\tcells %zu\trefs %zu\tids %zu\tin_bytes %zu\tunsorted_pairs %zu\tnonmono_offsets %zu\tdangling %zu\tinlined %zu\tshared %zu\tout_bytes %zu\tbad %zu\n",
           V.c_str(), inl, dedup, C, refs, total_ids, ent.n, unsorted, nonmono, dangling, inlined, shared, out_bytes, bad);
    printf("list size hist <2,<4,<8,<16,<32,<64,<256,>=256:");
    for (int k = 0; k < 8; k++) printf(" %llu", (unsigned long long)hist[k]);
    printf("\n");
    return bad ? 1 : 0;
}
