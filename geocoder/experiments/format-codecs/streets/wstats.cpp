// Structural facts about street / interp ways in today's format.
//   wstats <stride 12|24> <ways.bin> <nodes.bin> <geo_cells.bin> <col 0|2> <entries.bin> [other_nodes.bin]
// Prints tombstones, short ways, offset packing, padding, unreferenced ways,
// dead entry ids, f32 specials, duplicate consecutive nodes, ways whose whole
// node run duplicates another way's run, distinct coordinates, and (with
// other_nodes) how many nodes also occur exactly in the other file.
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cmath>
#include <string>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <stdexcept>
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
static inline uint64_t rd64(const uint8_t* p) { uint64_t v; memcpy(&v, p, 8); return v; }
static const uint32_t NO_DATA = 0xffffffffu;

int main(int argc, char** argv) {
    const size_t S = std::stoul(argv[1]);
    Map ways(argv[2]), nodes(argv[3]), cells(argv[4]);
    const int col = atoi(argv[5]);
    Map ent(argv[6]);
    const size_t W = ways.n / S, N = nodes.n / 8;
    const uint64_t* nb = (const uint64_t*)nodes.p;
    size_t tomb = 0, one = 0, maxc = 0, at_max = 0, pad_nz = 0, not_packed = 0, name_nd = 0, tail_nz = 0;
    size_t expect = 0;
    for (size_t w = 0; w < W; w++) {
        const uint8_t* r = ways.p + w * S;
        uint32_t off = rd32(r); uint16_t c = rd16(r + 4);
        if (rd16(r + 6)) pad_nz++;
        if (rd32(r + 8) == NO_DATA) name_nd++;
        if (S == 24 && (r[21] | r[22] | r[23])) tail_nz++;
        if (!c) { tomb++; continue; }
        if (c == 1) one++;
        if (c > maxc) maxc = c;
        if (c == 65535) at_max++;
        if (off != expect) not_packed++;
        expect = off + c;
    }
    printf("ways %zu nodes %zu tombstones(cnt0) %zu cnt1 %zu max_cnt %zu cnt==65535 %zu live_not_packed %zu end==N %d pad_nonzero %zu name_NO_DATA %zu tailpad_nonzero %zu\n",
           W, N, tomb, one, maxc, at_max, not_packed, expect == N, pad_nz, name_nd, tail_nz);
    if (S == 24) {
        size_t hist[256] = {0}, s_gt_e = 0, big = 0, eq = 0;
        for (size_t w = 0; w < W; w++) {
            const uint8_t* r = ways.p + w * S;
            if (!rd16(r + 4)) continue;
            hist[r[20]]++;
            uint32_t s = rd32(r + 12), e = rd32(r + 16);
            if (s > e) s_gt_e++;
            if (s == e) eq++;
            if (e - s > 65535) big++;
        }
        printf("interpolation:"); for (int i = 0; i < 256; i++) if (hist[i]) printf(" %d:%zu", i, hist[i]); printf("\n");
        printf("start>end %zu start==end %zu end-start>65535 %zu\n", s_gt_e, eq, big);
    }
    // references from cell entry lists
    const size_t C = cells.n / 20;
    std::vector<uint8_t> ref(W, 0);
    size_t ids = 0, oob = 0, to_short = 0, lists = 0;
    for (size_t i = 0; i < C; i++) {
        uint32_t o = rd32(cells.p + 20 * i + 8 + 4 * col);
        if (o == NO_DATA) continue;
        lists++;
        uint16_t c = rd16(ent.p + o);
        for (uint16_t k = 0; k < c; k++) {
            uint32_t id = rd32(ent.p + o + 2 + 4 * k);
            ids++;
            if (id >= W) { oob++; continue; }
            ref[id] = 1;
            if (rd16(ways.p + (size_t)id * S + 4) < 2) to_short++;
        }
    }
    size_t unref = 0, unref_nodes = 0, unref_live = 0;
    for (size_t w = 0; w < W; w++) if (!ref[w]) {
        uint16_t c = rd16(ways.p + w * S + 4);
        unref++; unref_nodes += c; if (c >= 2) unref_live++;
    }
    printf("cell lists %zu ids %zu ids>=W %zu ids->cnt<2 %zu | ways never referenced %zu (cnt>=2: %zu, nodes %zu)\n",
           lists, ids, oob, to_short, unref, unref_live, unref_nodes);
    // f32 specials, consecutive duplicates
    size_t nan = 0, inf = 0, negz = 0, sub = 0, dupc = 0, small_lat = 0, small_lng = 0;
    for (size_t w = 0; w < W; w++) {
        const uint8_t* r = ways.p + w * S;
        uint32_t off = rd32(r); uint16_t c = rd16(r + 4);
        for (size_t j = 0; j < c; j++) {
            uint64_t v = nb[off + j];
            for (int h = 0; h < 2; h++) {
                uint32_t b = (uint32_t)(v >> (32 * h));
                float f; memcpy(&f, &b, 4);
                if (std::isnan(f)) nan++;
                if (std::isinf(f)) inf++;
                if (b == 0x80000000u) negz++;
                if ((b & 0x7f800000u) == 0 && (b & 0x7fffffu)) sub++;
                if (std::fabs(f) < 1.0f) (h ? small_lng : small_lat)++;
            }
            if (j && nb[off + j] == nb[off + j - 1]) dupc++;
        }
    }
    printf("nan %zu inf %zu -0.0 %zu subnormal %zu |lat|<1 %zu |lng|<1 %zu consecutive identical nodes %zu\n",
           nan, inf, negz, sub, small_lat, small_lng, dupc);
    // duplicate whole runs (cnt >= 2)
    std::vector<std::pair<uint64_t, uint32_t>> hs;
    hs.reserve(W);
    for (size_t w = 0; w < W; w++) {
        const uint8_t* r = ways.p + w * S;
        uint32_t off = rd32(r); uint16_t c = rd16(r + 4);
        if (c < 2) continue;
        uint64_t h = 1469598103934665603ull ^ c;
        for (size_t j = 0; j < c; j++) { h ^= nb[off + j]; h *= 1099511628211ull; h ^= h >> 29; }
        hs.push_back({h, (uint32_t)w});
    }
    std::sort(hs.begin(), hs.end());
    size_t dup_ways = 0, dup_nodes = 0, reversed = 0;
    for (size_t i = 1; i < hs.size(); i++) {
        if (hs[i].first != hs[i - 1].first) continue;
        const uint8_t* a = ways.p + (size_t)hs[i].second * S;
        // compare against the first of this hash run
        size_t k = i; while (k > 0 && hs[k - 1].first == hs[i].first) k--;
        const uint8_t* b = ways.p + (size_t)hs[k].second * S;
        uint16_t ca = rd16(a + 4), cb = rd16(b + 4);
        if (ca == cb && memcmp(nodes.p + 8 * (size_t)rd32(a), nodes.p + 8 * (size_t)rd32(b), 8 * (size_t)ca) == 0) {
            dup_ways++; dup_nodes += ca;
        }
    }
    printf("ways whose node run exactly duplicates an earlier way's run: %zu (nodes %zu = %.2f%% of nodes)\n",
           dup_ways, dup_nodes, 100.0 * dup_nodes / N);
    // distinct coordinates
    std::vector<uint64_t> all(nb, nb + N);
    std::sort(all.begin(), all.end());
    size_t distinct = std::unique(all.begin(), all.end()) - all.begin();
    all.resize(distinct);
    printf("distinct (lat,lng) %zu of %zu nodes (%.2f%%)\n", distinct, N, 100.0 * distinct / N);
    if (argc > 7) {
        Map other(argv[7]);
        const uint64_t* ob = (const uint64_t*)other.p;
        size_t M = other.n / 8;
        // count other-file nodes present in this file
        size_t hits = 0;
        for (size_t i = 0; i < M; i++) if (std::binary_search(all.begin(), all.end(), ob[i])) hits++;
        printf("other file nodes %zu found exactly in this file: %zu (%.2f%%)\n", M, hits, 100.0 * hits / M);
    }
    return 0;
}
