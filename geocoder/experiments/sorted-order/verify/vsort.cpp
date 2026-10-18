// vsort: independent check of a canonical sorted day against the published day.
//
//   vsort PUB_FULL SRT_FULL META_DIR OUT_PREFIX
//
// For streets, interps and addresses:
//   - tombstones: count in PUB; none left in SRT; n_pub - tombs == n_srt
//   - META/<fam>.slot.u32 (new id -> published slot) is a bijection onto the
//     live published records
//   - record content equal (node arrays / vertex blobs compared byte for byte),
//     node_offset / vertex_offset repacked contiguously, addr parent_way_id ==
//     street inverse map of the published parent, sidecars permuted
//   - order: the stated content key, recomputed here, is non-decreasing; equal
//     keys are in slot order (the history tiebreak) -- counted
//   - interps: descents of the key, n - LIS, and the stated constraint (lines
//     sharing a vertex that the reader can tell apart keep slot order)
// Cell lists: every geo row's sorted list == sort(new ids of the published list).
// Writes OUT_PREFIX.street_ties.txt / .interp_shared.txt (lat lng of a sample of
// tie-class first nodes / shared interp vertices) for targeted queries.
#include <algorithm>
#include <cinttypes>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <omp.h>
#include <parallel/algorithm>
#include <random>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

static const uint32_t NO = 0xFFFFFFFFu;
struct WayHeader { uint32_t off; uint16_t cnt; uint16_t pad; uint32_t name; };
struct InterpWay { uint32_t off; uint16_t cnt; uint16_t pad1; uint32_t street, start, end; uint8_t interp, p4, p5, p6; };
struct AddrPoint { float lat, lng; uint32_t hn, street, parent, voff, vcnt; };
struct Node { float lat, lng; };
struct __attribute__((packed)) GeoRow { uint64_t cell; uint32_t off[3]; };
static_assert(sizeof(WayHeader) == 12 && sizeof(InterpWay) == 24 && sizeof(AddrPoint) == 28 && sizeof(GeoRow) == 20, "");

static int g_fail = 0;
#define CHECK(cond, ...) do { if (!(cond)) { printf("FAIL: " __VA_ARGS__); printf("\n"); g_fail++; } } while (0)

struct Map { const uint8_t* p = nullptr; size_t n = 0; template <class T> const T* as() const { return (const T*)p; } template <class T> size_t cnt() const { return n / sizeof(T); } };
static Map mapf(const std::string& path) {
    Map m; int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { fprintf(stderr, "open %s\n", path.c_str()); exit(2); }
    struct stat st; fstat(fd, &st); m.n = st.st_size;
    if (m.n) { void* q = mmap(nullptr, m.n, PROT_READ, MAP_SHARED, fd, 0); if (q == MAP_FAILED) { perror("mmap"); exit(2); } m.p = (const uint8_t*)q; madvise(q, m.n, MADV_WILLNEED); }
    close(fd); return m;
}
static uint32_t fbits(float f) { uint32_t b; memcpy(&b, &f, 4); return b; }
// unsigned order == numeric order
static uint32_t ford(float f) { uint32_t b = fbits(f); return (b >> 31) ? ~b : (b | 0x80000000u); }

// Wikipedia xy2d on a 2^24 grid, x = lng, y = lat.
static uint64_t hilb(float lat, float lng) {
    const int64_t n = int64_t(1) << 24;
    auto q = [&](double v, double lo, double span) { int64_t x = (int64_t)std::floor((v - lo) / span * (double)n); return x < 0 ? 0 : (x > n - 1 ? n - 1 : x); };
    int64_t x = q((double)lng, -180.0, 360.0), y = q((double)lat, -90.0, 180.0);
    uint64_t d = 0;
    for (int64_t s = n / 2; s > 0; s /= 2) {
        int64_t rx = (x & s) > 0, ry = (y & s) > 0;
        d += (uint64_t)s * (uint64_t)s * (uint64_t)((3 * rx) ^ ry);
        if (ry == 0) { if (rx == 1) { x = n - 1 - x; y = n - 1 - y; } int64_t t = x; x = y; y = t; }
    }
    return d;
}

struct Bij { std::vector<uint32_t> inv; bool ok = true; };
// slot: new -> pub; dead(i): pub slot i is a tombstone
template <class F>
static Bij check_bijection(const char* fam, const uint32_t* slot, size_t n_srt, size_t n_pub, F dead) {
    Bij b; b.inv.assign(n_pub, NO);
    uint64_t oob = 0, dup = 0, to_dead = 0;
    for (size_t k = 0; k < n_srt; k++) {
        uint32_t s = slot[k];
        if (s >= n_pub) { oob++; continue; }
        if (b.inv[s] != NO) { dup++; continue; }
        b.inv[s] = (uint32_t)k;
        if (dead(s)) to_dead++;
    }
    uint64_t live_missing = 0;
    for (size_t s = 0; s < n_pub; s++) if (b.inv[s] == NO && !dead(s)) live_missing++;
    printf("%s slot map: %zu entries, out-of-range %" PRIu64 ", duplicate %" PRIu64 ", to tombstone %" PRIu64 ", live published not mapped %" PRIu64 "\n",
           fam, n_srt, oob, dup, to_dead, live_missing);
    CHECK(!oob && !dup && !to_dead && !live_missing, "%s slot map is not a bijection onto live records", fam);
    return b;
}

static void sidecar_perm(const char* name, const std::string& pub, const std::string& srt, const uint32_t* slot, size_t n_srt, size_t n_pub) {
    Map a = mapf(pub + "/" + name), b = mapf(srt + "/" + name);
    CHECK(a.cnt<uint32_t>() == n_pub, "%s pub length %zu != %zu", name, a.cnt<uint32_t>(), n_pub);
    CHECK(b.cnt<uint32_t>() == n_srt, "%s srt length %zu != %zu", name, b.cnt<uint32_t>(), n_srt);
    const uint32_t *pa = a.as<uint32_t>(), *pb = b.as<uint32_t>();
    uint64_t bad = 0;
#pragma omp parallel for reduction(+ : bad)
    for (size_t k = 0; k < n_srt; k++) if (pb[k] != pa[slot[k]]) bad++;
    printf("  %s permuted by slot map: %" PRIu64 " mismatches\n", name, bad);
    CHECK(!bad, "%s not a permutation of the published sidecar", name);
}

struct WK { uint64_t h; uint32_t name, la, ln; };
static int wk_cmp(const WK& a, const WK& b) {
    if (a.h != b.h) return a.h < b.h ? -1 : 1;
    if (a.name != b.name) return a.name < b.name ? -1 : 1;
    if (a.la != b.la) return a.la < b.la ? -1 : 1;
    if (a.ln != b.ln) return a.ln < b.ln ? -1 : 1;
    return 0;
}

template <class Rec>
static uint32_t rname(const Rec& r) { if constexpr (sizeof(Rec) == 12) return r.name; else return r.street; }

template <class Rec>
static bool rtomb(const Rec& r) {
    if constexpr (sizeof(Rec) == 12) return r.cnt == 0 && r.off == 0 && r.name == NO && r.pad == 0;
    else return r.cnt == 0 && r.off == 0 && r.street == NO && r.start == 0 && r.end == 0 && r.interp == 0;
}

template <class Rec>
struct WayRes { std::vector<uint32_t> inv; size_t n_srt; };

template <class Rec>
static WayRes<Rec> check_ways(const char* fam, const std::string& pub, const std::string& srt, const std::string& meta,
                              const char* rf, const char* nf, std::vector<const char*> sidecars, const std::string& outp) {
    printf("== %s\n", fam);
    Map pr = mapf(pub + "/" + rf), pn = mapf(pub + "/" + nf), sr = mapf(srt + "/" + rf), sn = mapf(srt + "/" + nf),
        sm = mapf(meta + "/" + fam + ".slot.u32");
    const Rec *P = pr.as<Rec>(), *S = sr.as<Rec>();
    const Node *PN = pn.as<Node>(), *SN = sn.as<Node>();
    const size_t np = pr.cnt<Rec>(), ns = sr.cnt<Rec>();
    const uint32_t* slot = sm.as<uint32_t>();
    CHECK(sm.cnt<uint32_t>() == ns, "%s slot map length", fam);
    uint64_t tp = 0, ts = 0;
#pragma omp parallel for reduction(+ : tp)
    for (size_t i = 0; i < np; i++) tp += rtomb(P[i]);
#pragma omp parallel for reduction(+ : ts)
    for (size_t i = 0; i < ns; i++) ts += rtomb(S[i]);
    printf("%s: published %zu records (%" PRIu64 " tombstones), sorted %zu records (%" PRIu64 " tombstones); pub - tombs - sorted = %" PRId64 "\n",
           fam, np, tp, ns, ts, (int64_t)np - (int64_t)tp - (int64_t)ns);
    CHECK(ts == 0 && np - tp == ns, "%s tombstones not dropped exactly", fam);
    Bij b = check_bijection(fam, slot, ns, np, [&](size_t s) { return rtomb(P[s]); });
    // content + packing
    uint64_t at = 0, packbad = 0;
    for (size_t k = 0; k < ns; k++) { if (S[k].cnt) { if (S[k].off != at) packbad++; at += S[k].cnt; } }
    CHECK(at == sn.cnt<Node>(), "%s node file length %zu != packed sum %" PRIu64, fam, sn.cnt<Node>(), at);
    uint64_t cbad = 0, nbad = 0;
#pragma omp parallel for schedule(dynamic, 65536) reduction(+ : cbad, nbad)
    for (size_t k = 0; k < ns; k++) {
        const Rec &a = S[k], &o = P[slot[k]];
        Rec a2 = a, o2 = o; a2.off = 0; o2.off = 0;
        if (memcmp(&a2, &o2, sizeof(Rec))) cbad++;
        if (a.cnt && memcmp(SN + a.off, PN + o.off, size_t(a.cnt) * sizeof(Node))) nbad++;
    }
    printf("  content: %" PRIu64 " records differ outside node_offset, %" PRIu64 " node arrays differ, %" PRIu64 " packing breaks\n", cbad, nbad, packbad);
    CHECK(!cbad && !nbad && !packbad, "%s content/packing", fam);
    for (const char* sc : sidecars) sidecar_perm(sc, pub, srt, slot, ns, np);
    // order
    std::vector<WK> key(ns);
#pragma omp parallel for schedule(static)
    for (size_t k = 0; k < ns; k++) {
        const Rec& a = S[k];
        if (!a.cnt) { key[k] = WK{~0ull, rname(a), 0, 0}; continue; }
        const Node& f = SN[a.off];
        key[k] = WK{hilb(f.lat, f.lng), rname(a), ford(f.lat), ford(f.lng)};
    }
    uint64_t desc = 0, tie_groups = 0, tie_members = 0, tie_slot_bad = 0, cnt0 = 0;
    for (size_t k = 0; k < ns; k++) cnt0 += S[k].cnt == 0;
    {
        size_t i = 0;
        while (i < ns) {
            size_t j = i + 1;
            while (j < ns && wk_cmp(key[j], key[i]) == 0) { if (slot[j] < slot[j - 1]) tie_slot_bad++; j++; }
            if (j - i > 1) { tie_groups++; tie_members += j - i; }
            if (j < ns && wk_cmp(key[j], key[j - 1]) < 0) desc++;
            i = j;
        }
    }
    // exact tie (equal key) groups only count when adjacent; equal keys split apart
    // by a constraint would show up as descents.
    printf("  key (hilbert24 first node, name, first lat, first lng): descents %" PRIu64 ", equal-key runs %" PRIu64 " / %" PRIu64
           " records, equal-key runs not in slot order %" PRIu64 ", node-less records %" PRIu64 "\n",
           desc, tie_groups, tie_members, tie_slot_bad, cnt0);
    if (sizeof(Rec) == 12) CHECK(desc == 0 && tie_slot_bad == 0, "%s not in key order", fam);
    if constexpr (sizeof(Rec) == 24) {
        // n - LIS of the (key, slot) rank sequence
        std::vector<uint32_t> idx(ns);
        for (size_t k = 0; k < ns; k++) idx[k] = (uint32_t)k;
        __gnu_parallel::sort(idx.begin(), idx.end(), [&](uint32_t a, uint32_t c) { int r = wk_cmp(key[a], key[c]); return r ? r < 0 : slot[a] < slot[c]; });
        std::vector<uint32_t> rank(ns);
        for (size_t r = 0; r < ns; r++) rank[idx[r]] = (uint32_t)r;
        std::vector<uint32_t> tails;
        uint64_t far = 0, shifted = 0;
        for (size_t k = 0; k < ns; k++) {
            uint32_t r = rank[k];
            if (r != k) shifted++;
            if ((r > k ? r - k : k - r) > 1000) far++;
            auto it = std::lower_bound(tails.begin(), tails.end(), r);
            if (it == tails.end()) tails.push_back(r); else *it = r;
        }
        printf("  interp: n - LIS (lines out of key order) %zu, lines > 1000 places from key position %" PRIu64 ", lines not at key position %" PRIu64 "\n",
               ns - tails.size(), far, shifted);
        // constraint check: shared vertices, distinguishable pairs keep slot order
        Map pcm = mapf(srt + "/interp_postcodes.bin");
        const uint32_t* pc = pcm.as<uint32_t>();
        struct Inc { uint64_t v; uint32_t k; };
        std::vector<uint64_t> off(ns + 1, 0);
        for (size_t k = 0; k < ns; k++) off[k + 1] = off[k] + S[k].cnt;
        std::vector<Inc> inc(off[ns]);
#pragma omp parallel for schedule(dynamic, 65536)
        for (size_t k = 0; k < ns; k++)
            for (uint32_t j = 0; j < S[k].cnt; j++) {
                const Node& nd = SN[S[k].off + j];
                inc[off[k] + j] = Inc{(uint64_t(fbits(nd.lat)) << 32) | fbits(nd.lng), (uint32_t)k};
            }
        __gnu_parallel::sort(inc.begin(), inc.end(), [](const Inc& a, const Inc& c) { return a.v != c.v ? a.v < c.v : a.k < c.k; });
        uint64_t shared = 0, pairs_obs = 0, viol = 0, pairs_all = 0;
        std::vector<uint32_t> mem;
        FILE* fo = fopen((outp + ".interp_shared.txt").c_str(), "w");
        std::mt19937_64 rng(777);
        for (size_t i = 0; i < inc.size();) {
            size_t j = i + 1;
            while (j < inc.size() && inc[j].v == inc[i].v) j++;
            mem.clear();
            for (size_t a = i; a < j; a++) if (mem.empty() || mem.back() != inc[a].k) mem.push_back(inc[a].k);
            if (mem.size() > 1) {
                shared++;
                bool any = false;
                for (size_t a = 0; a < mem.size(); a++)
                    for (size_t c = a + 1; c < mem.size(); c++) {
                        const InterpWay &x = (const InterpWay&)S[mem[a]], &y = (const InterpWay&)S[mem[c]];
                        pairs_all++;
                        bool obs = x.street != y.street || pc[mem[a]] != pc[mem[c]] || (x.start == y.start && x.interp == y.interp);
                        if (!obs) continue;
                        pairs_obs++; any = true;
                        if (slot[mem[a]] > slot[mem[c]]) viol++;
                    }
                if (any && (rng() % 8) == 0) {
                    float la, ln; uint32_t hi = uint32_t(inc[i].v >> 32), lo = uint32_t(inc[i].v);
                    memcpy(&la, &hi, 4); memcpy(&ln, &lo, 4);
                    fprintf(fo, "%.8f %.8f\n", la, ln);
                }
            }
            i = j;
        }
        fclose(fo);
        printf("  interp shared vertices %" PRIu64 ", line pairs %" PRIu64 ", distinguishable pairs %" PRIu64 ", of those out of slot order %" PRIu64 "\n",
               shared, pairs_all, pairs_obs, viol);
        CHECK(viol == 0, "interp distinguishable pairs not in slot order");
    } else {
        // street tie classes sample
        FILE* fo = fopen((outp + ".street_ties.txt").c_str(), "w");
        std::mt19937_64 rng(4242);
        size_t i = 0;
        while (i < ns) {
            size_t j = i + 1;
            while (j < ns && wk_cmp(key[j], key[i]) == 0) j++;
            if (j - i > 1 && (rng() % 400) == 0 && S[i].cnt) {
                const Node& f = SN[S[i].off];
                fprintf(fo, "%.8f %.8f\n", f.lat, f.lng);
            }
            i = j;
        }
        fclose(fo);
    }
    WayRes<Rec> res; res.inv = std::move(b.inv); res.n_srt = ns;
    return res;
}

static size_t poly_len(const uint8_t* v, size_t vn, uint32_t off, uint32_t cnt) {
    if (off >= vn) return 0;
    uint8_t e = v[off];
    return 10 + size_t(cnt) * (e >= 3 ? 8 : 4);
}

struct AK { uint32_t row; uint64_t h; uint32_t la, ln, hn, st; };
static int ak_cmp(const AK& a, const AK& b) {
    if (a.row != b.row) return a.row < b.row ? -1 : 1;
    if (a.h != b.h) return a.h < b.h ? -1 : 1;
    if (a.la != b.la) return a.la < b.la ? -1 : 1;
    if (a.ln != b.ln) return a.ln < b.ln ? -1 : 1;
    if (a.hn != b.hn) return a.hn < b.hn ? -1 : 1;
    if (a.st != b.st) return a.st < b.st ? -1 : 1;
    return 0;
}
static bool atomb(const AddrPoint& a) {
    return fbits(a.lat) == 0 && fbits(a.lng) == 0 && a.hn == 0 && a.street == 0 && a.parent == NO && a.voff == NO && a.vcnt == 0;
}

int main(int argc, char** argv) {
    if (argc < 5) { fprintf(stderr, "usage: vsort PUB_FULL SRT_FULL META OUT_PREFIX\n"); return 1; }
    const std::string pub = argv[1], srt = argv[2], meta = argv[3], outp = argv[4];
    setvbuf(stdout, nullptr, _IOLBF, 0);
    auto st = check_ways<WayHeader>("street", pub, srt, meta, "street_ways.bin", "street_nodes.bin", {"way_parents.bin", "way_postcodes.bin"}, outp);
    auto it = check_ways<InterpWay>("interp", pub, srt, meta, "interp_ways.bin", "interp_nodes.bin", {"interp_postcodes.bin"}, outp);

    // ---------------- addresses
    printf("== addr\n");
    Map pa = mapf(pub + "/addr_points.bin"), sa = mapf(srt + "/addr_points.bin"), pv = mapf(pub + "/addr_vertices.bin"),
        sv = mapf(srt + "/addr_vertices.bin"), am = mapf(meta + "/addr.slot.u32");
    const AddrPoint *P = pa.as<AddrPoint>(), *S = sa.as<AddrPoint>();
    const size_t np = pa.cnt<AddrPoint>(), ns = sa.cnt<AddrPoint>();
    const uint32_t* aslot = am.as<uint32_t>();
    CHECK(am.cnt<uint32_t>() == ns, "addr slot map length");
    uint64_t tp = 0, ts = 0;
#pragma omp parallel for reduction(+ : tp)
    for (size_t i = 0; i < np; i++) tp += atomb(P[i]);
#pragma omp parallel for reduction(+ : ts)
    for (size_t i = 0; i < ns; i++) ts += atomb(S[i]);
    printf("addr: published %zu (%" PRIu64 " tombstones), sorted %zu (%" PRIu64 " tombstones); pub - tombs - sorted = %" PRId64 "\n", np, tp, ns, ts,
           (int64_t)np - (int64_t)tp - (int64_t)ns);
    CHECK(ts == 0 && np - tp == ns, "addr tombstones not dropped exactly");
    Bij ab = check_bijection("addr", aslot, ns, np, [&](size_t s) { return atomb(P[s]); });
    uint64_t vat = 0, vbad = 0, pbad = 0, fbad = 0, blobbad = 0, parents = 0;
    for (size_t k = 0; k < ns; k++) {
        const AddrPoint& a = S[k];
        if (a.vcnt && a.voff != NO) { if (a.voff != vat) vbad++; vat += poly_len(sv.p, sv.n, a.voff, a.vcnt); }
    }
    CHECK(vat == sv.n, "addr_vertices length %zu != packed %" PRIu64, sv.n, vat);
#pragma omp parallel for schedule(dynamic, 65536) reduction(+ : pbad, fbad, blobbad, parents)
    for (size_t k = 0; k < ns; k++) {
        const AddrPoint &a = S[k], &o = P[aslot[k]];
        if (fbits(a.lat) != fbits(o.lat) || fbits(a.lng) != fbits(o.lng) || a.hn != o.hn || a.street != o.street || a.vcnt != o.vcnt) fbad++;
        if (o.parent == NO) { if (a.parent != NO) pbad++; }
        else { parents++; if (o.parent >= st.inv.size() || a.parent != st.inv[o.parent]) pbad++; }
        if (o.vcnt && o.voff != NO) {
            size_t la = poly_len(pv.p, pv.n, o.voff, o.vcnt);
            if (a.voff == NO || size_t(a.voff) + la > sv.n || memcmp(sv.p + a.voff, pv.p + o.voff, la)) blobbad++;
        } else if (a.voff != o.voff) blobbad++;
    }
    printf("  content: %" PRIu64 " field mismatches, parent_way_id %" PRIu64 " checked / %" PRIu64 " not equal to remapped published parent, %" PRIu64
           " vertex blobs differ, %" PRIu64 " packing breaks\n", fbad, parents, pbad, blobbad, vbad);
    CHECK(!fbad && !pbad && !blobbad && !vbad, "addr content");
    sidecar_perm("addr_postcodes.bin", pub, srt, aslot, ns, np);

    // ---------------- geo index: lists are the published lists relabelled
    printf("== cell lists\n");
    Map pg = mapf(pub + "/geo_cells.bin"), sg = mapf(srt + "/geo_cells.bin");
    CHECK(pg.n == sg.n && !memcmp(pg.p, sg.p, pg.n), "geo_cells.bin differs");
    const GeoRow* G = sg.as<GeoRow>();
    const size_t rows = sg.cnt<GeoRow>();
    const char* EF[3] = {"street_entries.bin", "addr_entries.bin", "interp_entries.bin"};
    const std::vector<uint32_t>* inv[3] = {&st.inv, &ab.inv, &it.inv};
    std::vector<uint32_t> first_row(ns, NO);
    for (int t = 0; t < 3; t++) {
        Map pe = mapf(pub + "/" + EF[t]), se = mapf(srt + "/" + EF[t]);
        CHECK(pe.n == se.n, "%s size", EF[t]);
        uint64_t lists = 0, ids = 0, bad = 0, unsorted = 0, unmapped = 0;
#pragma omp parallel reduction(+ : lists, ids, bad, unsorted, unmapped)
        {
            std::vector<uint32_t> a, b;
#pragma omp for schedule(dynamic, 65536)
            for (size_t i = 0; i < rows; i++) {
                uint32_t off = G[i].off[t];
                if (off == NO) continue;
                uint16_t c, c2; memcpy(&c, pe.p + off, 2); memcpy(&c2, se.p + off, 2);
                if (c != c2) { bad++; continue; }
                a.resize(c); b.resize(c);
                memcpy(a.data(), pe.p + off + 2, size_t(c) * 4); memcpy(b.data(), se.p + off + 2, size_t(c) * 4);
                for (auto& x : a) { uint32_t m = x < inv[t]->size() ? (*inv[t])[x] : NO; if (m == NO) unmapped++; x = m; }
                std::sort(a.begin(), a.end());
                if (!std::is_sorted(b.begin(), b.end())) unsorted++;
                if (a != b) bad++;
                lists++; ids += c;
            }
        }
        printf("  %s: %" PRIu64 " lists, %" PRIu64 " ids; lists != relabelled published %" PRIu64 ", unsorted %" PRIu64 ", ids of dropped records %" PRIu64 "\n",
               EF[t], lists, ids, bad, unsorted, unmapped);
        CHECK(!bad && !unsorted && !unmapped, "%s", EF[t]);
        if (t == 1) {
            for (size_t i = 0; i < rows; i++) {
                uint32_t off = G[i].off[1];
                if (off == NO) continue;
                uint16_t c; memcpy(&c, se.p + off, 2);
                const uint8_t* q = se.p + off + 2;
                for (uint32_t k = 0; k < c; k++) { uint32_t id; memcpy(&id, q + 4 * k, 4); if (id < ns && first_row[id] == NO) first_row[id] = (uint32_t)i; }
            }
        }
    }
    // address order
    uint64_t desc = 0, tg = 0, tm = 0, tslot = 0, unlisted = 0, unl_not_last = 0;
    {
        AK prev{};
        size_t run = 1;
        bool seen_unl = false;
        for (size_t k = 0; k < ns; k++) {
            const AddrPoint& a = S[k];
            AK cur{first_row[k], hilb(a.lat, a.lng), ford(a.lat), ford(a.lng), a.hn, a.street};
            if (cur.row == NO) { unlisted++; seen_unl = true; } else if (seen_unl) unl_not_last++;
            if (k) {
                int r = ak_cmp(prev, cur);
                if (r > 0) desc++;
                if (r == 0) { run++; if (aslot[k] < aslot[k - 1]) tslot++; }
                else { if (run > 1) { tg++; tm += run; } run = 1; }
            }
            prev = cur;
        }
        if (run > 1) { tg++; tm += run; }
    }
    printf("addr key (first listing row, hilbert24, lat, lng, housenumber, street): descents %" PRIu64 ", equal-key runs %" PRIu64 " / %" PRIu64
           " records, equal-key runs not in slot order %" PRIu64 ", unlisted points %" PRIu64 " (not last: %" PRIu64 ")\n",
           desc, tg, tm, tslot, unlisted, unl_not_last);
    CHECK(desc == 0 && tslot == 0 && unl_not_last == 0, "addr not in key order");
    printf(g_fail ? "RESULT: %d FAILURES\n" : "RESULT: ALL CHECKS PASS (%d failures)\n", g_fail);
    return g_fail ? 1 : 0;
}
