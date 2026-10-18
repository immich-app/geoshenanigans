// GX geo index (geo_gx.py): (1) full sequential decode back to today's format, compared
// byte-for-byte with the today's-format reference set (geo_cells.bin + 3 entries files);
// (2) random-access lookups (binary search gx_top + one block decode) compared with today's
// binary-search lookup for sampled present and absent cells; (3) warm lookup timing of both.
// usage: gx_check <gx dir> <adj|global> <B> <reference dir> [<geo_cells.bin for cell ids>]
// With the 5th argument the reference dir holds only the entries files; cells and presence come
// from that geo_cells.bin and the reference offsets are today's (back-to-back, row order).
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <random>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

static const uint32_t NO = 0xFFFFFFFFu;
struct Map { const uint8_t* p = nullptr; size_t n = 0; };
static Map map_file(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    Map m; m.n = st.st_size;
    m.p = static_cast<const uint8_t*>(mmap(nullptr, m.n, PROT_READ, MAP_PRIVATE, fd, 0));
    close(fd);
    return m;
}
template <class T> static T rd(const uint8_t* p) { T v; std::memcpy(&v, p, sizeof v); return v; }
static uint64_t varint(const uint8_t*& p) {
    uint64_t v = 0; int s = 0;
    for (;;) { uint8_t b = *p++; v |= uint64_t(b & 0x7F) << s; if (!(b & 0x80)) return v; s += 7; }
}
static double now() { return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count(); }

struct GX {
    Map top, rows, ent[3];
    bool global; size_t B, topsz, nblocks;
    uint64_t top_cell(size_t b) const { return rd<uint64_t>(top.p + b * topsz); }
    size_t list_bytes(int t, uint32_t off) const { return 2 + 4 * size_t(rd<uint16_t>(ent[t].p + off)); }
    // Lookup: offsets (into this GX's entries files) of the cell's lists, NO_DATA when absent.
    bool lookup(uint64_t target, uint32_t out[3]) const {
        out[0] = out[1] = out[2] = NO;
        size_t lo = 0, hi = nblocks;            // last block whose first cell <= target
        while (lo < hi) { size_t mid = (lo + hi) / 2; if (top_cell(mid) <= target) lo = mid + 1; else hi = mid; }
        if (lo == 0) return false;
        size_t b = lo - 1;
        const uint8_t* t = top.p + b * topsz;
        uint64_t cell = rd<uint64_t>(t);
        const uint8_t* p = rows.p + rd<uint32_t>(t + 8);
        uint32_t cur[3], nxt[3];
        for (int k = 0; k < 3; k++) {
            if (global) { cur[k] = rd<uint32_t>(t + 12 + 8 * k); nxt[k] = rd<uint32_t>(t + 16 + 8 * k); }
            else { cur[k] = rd<uint32_t>(t + 12 + 4 * k); nxt[k] = cur[k] == NO ? 0 : uint32_t(cur[k] + list_bytes(k, cur[k])); }
        }
        size_t end = (b + 1) * B;
        for (size_t r = b * B; ; r++) {
            if (r >= end || (r - b * B) >= B) return false;
            if (p >= rows.p + rows.n) return false;
            uint8_t h = *p++;
            int d = h >> 6;
            if (r != b * B) {
                uint64_t du = d == 3 ? varint(p) + 4 : uint64_t(d) + 1;
                cell += du << 27;
            }
            if (cell > target) return false;
            int st[3] = {h & 3, (h >> 2) & 3, (h >> 4) & 3};
            for (int k = 0; k < 3; k++) {
                if (st[k] == 1) { cur[k] = nxt[k]; nxt[k] += uint32_t(list_bytes(k, cur[k])); }
                else if (st[k] == 3) { cur[k] = nxt[k] - uint32_t(varint(p)); }
            }
            if (cell == target) { for (int k = 0; k < 3; k++) out[k] = st[k] ? cur[k] : NO; return true; }
        }
    }
};

int main(int argc, char** argv) {
    if (argc < 5) { fprintf(stderr, "usage: gx_check <gx dir> <adj|global> <B> <ref dir>\n"); return 2; }
    std::string gd = argv[1], ref = argv[4];
    const char* names[3] = {"street_entries.bin", "addr_entries.bin", "interp_entries.bin"};
    GX gx;
    gx.global = std::string(argv[2]) == "global";
    gx.B = std::strtoul(argv[3], nullptr, 10);
    gx.topsz = gx.global ? 36 : 24;
    gx.top = map_file(gd + "/gx_top.bin");
    gx.rows = map_file(gd + "/gx_rows.bin");
    gx.nblocks = gx.top.n / gx.topsz;
    const bool ext_geo = argc > 5;
    Map rgeo = map_file(ext_geo ? std::string(argv[5]) : ref + "/geo_cells.bin"), rent[3];
    for (int k = 0; k < 3; k++) { gx.ent[k] = map_file(gd + "/" + names[k]); rent[k] = map_file(ref + "/" + names[k]); }
    const size_t R = rgeo.n / 20;

    // (1) full sequential decode -> today's layout, compared on the fly
    double t0 = now();
    const uint8_t* p = gx.rows.p;
    uint64_t cell = 0;
    uint32_t cur[3] = {NO, NO, NO}, nxt[3] = {0, 0, 0};
    uint64_t outpos[3] = {0, 0, 0};
    size_t bad = 0;
    for (size_t r = 0; r < R; r++) {
        if (r % gx.B == 0) {
            const uint8_t* t = gx.top.p + (r / gx.B) * gx.topsz;
            if (rd<uint32_t>(t + 8) != uint32_t(p - gx.rows.p)) { bad++; }
        }
        uint8_t h = *p++;
        int d = h >> 6;
        if (r % gx.B == 0) cell = rd<uint64_t>(gx.top.p + (r / gx.B) * gx.topsz);
        else cell += (d == 3 ? varint(p) + 4 : uint64_t(d) + 1) << 27;
        int st[3] = {h & 3, (h >> 2) & 3, (h >> 4) & 3};
        const uint8_t* rr = rgeo.p + r * 20;
        if (rd<uint64_t>(rr) != cell) { if (bad++ < 5) fprintf(stderr, "row %zu cell mismatch\n", r); }
        for (int k = 0; k < 3; k++) {
            if (st[k] == 1) { cur[k] = nxt[k]; nxt[k] += uint32_t(gx.list_bytes(k, cur[k])); }
            else if (st[k] == 3) { cur[k] = nxt[k] - uint32_t(varint(p)); }
            uint32_t roff = rd<uint32_t>(rr + 8 + 4 * k);
            if (!st[k]) { if (roff != NO) bad++; continue; }
            // today's layout: lists back to back in row order -> this list sits at outpos
            size_t nbytes = gx.list_bytes(k, cur[k]);
            if ((!ext_geo && roff != uint32_t(outpos[k])) || std::memcmp(gx.ent[k].p + cur[k], rent[k].p + outpos[k], nbytes) != 0) {
                if (bad++ < 5) fprintf(stderr, "row %zu table %d list mismatch\n", r, k);
            }
            outpos[k] += nbytes;
        }
    }
    bool sizes_ok = p == gx.rows.p + gx.rows.n;
    for (int k = 0; k < 3; k++) sizes_ok &= outpos[k] == rent[k].n;
    printf("full decode of %zu rows -> today's geo_cells + entries: %s (%zu mismatches, all bytes consumed/produced: %s) %.1fs\n",
           R, bad == 0 && sizes_ok ? "BYTE-IDENTICAL" : "DIFFERENT", bad, sizes_ok ? "yes" : "no", now() - t0);

    // (2) random access vs today's lookup (reference offsets: with an external geo_cells they are
    // recomputed as today's back-to-back offsets of the reference entries)
    std::vector<uint32_t> refoff;
    if (ext_geo) {
        refoff.assign(R * 3, NO);
        uint64_t pos[3] = {0, 0, 0};
        for (size_t r = 0; r < R; r++)
            for (int k = 0; k < 3; k++)
                if (rd<uint32_t>(rgeo.p + r * 20 + 8 + 4 * k) != NO) {
                    refoff[r * 3 + k] = uint32_t(pos[k]);
                    pos[k] += 2 + 4 * size_t(rd<uint16_t>(rent[k].p + pos[k]));
                }
    }
    auto ref_lookup = [&](uint64_t target, uint32_t out[3]) {
        size_t lo = 0, hi = R;
        while (lo < hi) { size_t mid = (lo + hi) / 2; uint64_t c = rd<uint64_t>(rgeo.p + mid * 20);
            if (c == target) { for (int k = 0; k < 3; k++) out[k] = ext_geo ? refoff[mid * 3 + k] : rd<uint32_t>(rgeo.p + mid * 20 + 8 + 4 * k); return true; }
            if (c < target) lo = mid + 1; else hi = mid; }
        out[0] = out[1] = out[2] = NO; return false;
    };
    std::mt19937_64 rng(20261013);
    std::vector<uint64_t> targets;
    for (int i = 0; i < 2000000; i++) {
        uint64_t c = rd<uint64_t>(rgeo.p + (rng() % R) * 20);
        int64_t shift = int64_t(rng() % 9) - 4;            // the cell or a nearby (often absent) one
        targets.push_back(c + (uint64_t(shift) << 27));
    }
    size_t mism = 0, present = 0;
    for (uint64_t c : targets) {
        uint32_t a[3], b[3];
        bool fa = gx.lookup(c, a), fb = ref_lookup(c, b);
        present += fb;
        if (fa != fb) { mism++; continue; }
        for (int k = 0; k < 3; k++) {
            if ((a[k] == NO) != (b[k] == NO)) { mism++; break; }
            if (a[k] == NO) continue;
            size_t n = gx.list_bytes(k, a[k]);
            if (n != 2 + 4 * size_t(rd<uint16_t>(rent[k].p + b[k])) || std::memcmp(gx.ent[k].p + a[k], rent[k].p + b[k], n)) { mism++; break; }
        }
    }
    printf("random access: %zu lookups (%zu present cells), %zu mismatches vs today's lookup\n", targets.size(), present, mism);
    // (3) warm timing
    volatile uint64_t sink = 0;
    double ta = now();
    for (uint64_t c : targets) { uint32_t a[3]; gx.lookup(c, a); sink += a[0]; }
    double tb = now();
    for (uint64_t c : targets) { uint32_t b[3]; ref_lookup(c, b); sink += b[0]; }
    double tc = now();
    printf("warm lookup: GX %.0f ns, today %.0f ns per cell (block size %zu)\n",
           (tb - ta) / targets.size() * 1e9, (tc - tb) / targets.size() * 1e9, gx.B);
    return (bad == 0 && sizes_ok && mism == 0) ? 0 : 1;
}
