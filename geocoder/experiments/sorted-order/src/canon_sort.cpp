// canon_sort: rewrite a published planet/full dir with street ways, interp
// ways and addr points in a canonical, content-derived order, in TODAY's file
// format (today's reader, differ and patcher read the result).
//
//   canon_sort SRC_FULL_DIR OUT_DIR META_DIR [--threads N]
//
// Order (slot = the record's index in SRC, the strategy-2 identity proxy):
//   street ways  : hilbert24(first node), name_id, first node lat, lng, slot
//   interp ways  : hilbert24(first node), street_id, first node lat, lng, slot
//                  (--way-key spec: hilbert24, name, node_count, slot); interps
//                  then keep slot order where two lines can tie in the
//                  reader (see constrained_interp_order)
//   addr points  : first geo_cells row listing the point, hilbert24(point),
//                  lat, lng, housenumber_id, street_id, slot
// Tombstones (tombstone record content and no reference) are dropped.
// Rewritten: street_ways, street_nodes (repacked), way_parents, way_postcodes,
// street_entries; interp_ways, interp_nodes, interp_postcodes,
// interp_entries; addr_points (parent_way_id remapped, vertex_offset
// repacked), addr_vertices, addr_postcodes, addr_entries. Each cell list
// keeps its length (so geo_cells.bin is unchanged) and is re-sorted by the
// new ids, as the builder writes lists.
// META_DIR gets <family>.slot.u32 (new id -> SRC slot) and stats.txt.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <omp.h>
#include <parallel/algorithm>
#include <queue>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

static const uint32_t NO = 0xFFFFFFFFu;

struct WayHeader { uint32_t off; uint16_t cnt; uint16_t pad; uint32_t name; };
struct InterpWay { uint32_t off; uint16_t cnt; uint16_t pad1; uint32_t street; uint32_t start, end; uint8_t interp, p4, p5, p6; };
struct AddrPoint { float lat, lng; uint32_t hn, street, parent, voff, vcnt; };
struct NodeCoord { float lat, lng; };
static_assert(sizeof(WayHeader) == 12 && sizeof(InterpWay) == 24 && sizeof(AddrPoint) == 28, "layout");

static FILE* g_stats = nullptr;
#define STAT(...) do { fprintf(stderr, __VA_ARGS__); if (g_stats) fprintf(g_stats, __VA_ARGS__); } while (0)

static double now_s() {
    return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count();
}
static double g_t0;
static void phase(const char* what) { fprintf(stderr, "[%8.2fs] %s\n", now_s() - g_t0, what); }

[[noreturn]] static void die(const std::string& m) { fprintf(stderr, "FATAL: %s\n", m.c_str()); exit(2); }

struct Mapped {
    const uint8_t* p = nullptr;
    size_t n = 0;
    template <class T> const T* as() const { return reinterpret_cast<const T*>(p); }
    template <class T> size_t count() const {
        if (n % sizeof(T)) die("file size not a multiple of the record size");
        return n / sizeof(T);
    }
};

static Mapped map_file(const std::string& path) {
    Mapped m;
    int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) die("open " + path);
    struct stat st;
    fstat(fd, &st);
    m.n = size_t(st.st_size);
    if (m.n) {
        void* p = mmap(nullptr, m.n, PROT_READ, MAP_SHARED, fd, 0);
        if (p == MAP_FAILED) die("mmap " + path);
        m.p = static_cast<const uint8_t*>(p);
    }
    close(fd);
    return m;
}

static void write_file(const std::string& path, const void* data, size_t n) {
    // A fresh inode: the output dir holds hardlinks of published files, which
    // truncating in place would rewrite.
    if (unlink(path.c_str()) && errno != ENOENT) die("unlink " + path);
    int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
    if (fd < 0) die("create " + path);
    const char* p = static_cast<const char*>(data);
    size_t left = n;
    while (left) {
        ssize_t w = ::write(fd, p, std::min(left, size_t(1) << 30));
        if (w < 0) { if (errno == EINTR) continue; die("write " + path); }
        p += w;
        left -= size_t(w);
    }
    if (close(fd)) die("close " + path);
}

// 2-D Hilbert index on a 2^24 x 2^24 lng/lat grid (same as max/streets/reorder.py).
static uint64_t hilbert24(float lat_f, float lng_f) {
    const int ORDER = 24;
    const int64_t n = int64_t(1) << ORDER;
    auto q = [&](double v, double lo, double span) {
        int64_t x = int64_t((v - lo) / span * double(n));  // truncation, v >= lo
        return std::min<int64_t>(std::max<int64_t>(x, 0), n - 1);
    };
    int64_t x = q(double(lng_f), -180.0, 360.0), y = q(double(lat_f), -90.0, 180.0);
    uint64_t d = 0;
    for (int64_t s = n >> 1; s > 0; s >>= 1) {
        int64_t rx = (x & s) > 0, ry = (y & s) > 0;
        d += uint64_t(s) * uint64_t(s) * uint64_t((3 * rx) ^ ry);
        if (ry == 0) {
            if (rx == 1) { x = n - 1 - x; y = n - 1 - y; }
            std::swap(x, y);
        }
    }
    return d;
}

// Float bits mapped so unsigned order == numeric order (total, -0 < +0).
static uint32_t fkey(float f) {
    uint32_t b;
    memcpy(&b, &f, 4);
    return (b & 0x80000000u) ? ~b : (b | 0x80000000u);
}

// Lowers *p to v; returns the value it saw last (NO when first).
static uint32_t atomic_min(uint32_t* p, uint32_t v) {
    uint32_t cur = __atomic_load_n(p, __ATOMIC_RELAXED);
    while (v < cur && !__atomic_compare_exchange_n(p, &cur, v, true, __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {}
    return cur;
}

// Uninitialised buffer of POD records (the planet outputs run to GBs; zero
// filling them first only costs time).
template <class T>
struct Buf {
    T* p = nullptr;
    size_t n = 0;
    explicit Buf(size_t count) : n(count) {
        p = static_cast<T*>(malloc(std::max<size_t>(count, 1) * sizeof(T)));
        if (!p) die("out of memory");
    }
    ~Buf() { free(p); }
    Buf(const Buf&) = delete;
    Buf& operator=(const Buf&) = delete;
    T& operator[](size_t i) { return p[i]; }
    T* data() { return p; }
    size_t size() const { return n; }
};

// ---------------------------------------------------------------- geo index
struct __attribute__((packed)) GeoRow { uint64_t cell; uint32_t off[3]; };
static_assert(sizeof(GeoRow) == 20, "GeoRow");
static const char* ENTRY_FILES[3] = {"street_entries.bin", "addr_entries.bin", "interp_entries.bin"};
static const char* TABLE_NAMES[3] = {"street", "addr", "interp"};

struct Refs {
    std::vector<uint8_t> ref[3];   // record listed by some cell
    std::vector<uint32_t> addr_row; // first geo_cells row listing the addr point
    uint64_t addr_multi = 0;        // addr points listed by more than one cell
};

static Refs scan_geo(const Mapped& geo, const Mapped* entries, const size_t n_rec[3]) {
    Refs r;
    const size_t rows = geo.count<GeoRow>();
    const GeoRow* g = geo.as<GeoRow>();
    for (int t = 0; t < 3; t++) r.ref[t].assign(n_rec[t], 0);
    r.addr_row.assign(n_rec[1], NO);
    std::vector<uint8_t> multi(n_rec[1], 0);
    uint64_t lists[3] = {0, 0, 0}, ids[3] = {0, 0, 0}, unsorted[3] = {0, 0, 0}, dups[3] = {0, 0, 0},
             bytes[3] = {0, 0, 0}, bad[3] = {0, 0, 0}, rows_unsorted = 0;
#pragma omp parallel for schedule(dynamic, 1 << 16) reduction(+ : lists[:3], ids[:3], unsorted[:3], dups[:3], bytes[:3], bad[:3], rows_unsorted)
    for (size_t i = 0; i < rows; i++) {
        if (i && g[i].cell <= g[i - 1].cell) rows_unsorted++;
        for (int t = 0; t < 3; t++) {
            const uint32_t off = g[i].off[t];
            if (off == NO) continue;
            if (size_t(off) + 2 > entries[t].n) { bad[t]++; continue; }
            uint16_t c;
            memcpy(&c, entries[t].p + off, 2);
            if (size_t(off) + 2 + size_t(c) * 4 > entries[t].n) { bad[t]++; continue; }
            lists[t]++;
            ids[t] += c;
            bytes[t] += 2 + size_t(c) * 4;
            const uint8_t* q = entries[t].p + off + 2;
            uint32_t prev = 0;
            bool sorted = true;
            for (uint32_t k = 0; k < c; k++) {
                uint32_t id;
                memcpy(&id, q + 4 * k, 4);
                if (id >= n_rec[t]) { bad[t]++; continue; }
                if (k) {
                    if (id < prev) sorted = false;
                    if (id == prev) dups[t]++;
                }
                prev = id;
                __atomic_store_n(&r.ref[t][id], uint8_t(1), __ATOMIC_RELAXED);
                if (t == 1) {
                    const uint32_t seen = atomic_min(&r.addr_row[id], uint32_t(i));
                    if (seen != NO && seen != uint32_t(i)) __atomic_store_n(&multi[id], uint8_t(1), __ATOMIC_RELAXED);
                }
            }
            if (!sorted) unsorted[t]++;
        }
    }
    STAT("geo_cells rows %zu (rows out of order: %" PRIu64 ")\n", rows, rows_unsorted);
    for (int t = 0; t < 3; t++) {
        STAT("  %-6s lists %" PRIu64 " ids %" PRIu64 " bytes %" PRIu64 " (file %zu) unsorted-lists %" PRIu64
             " dup-adjacent %" PRIu64 " bad %" PRIu64 "\n",
             TABLE_NAMES[t], lists[t], ids[t], bytes[t], entries[t].n, unsorted[t], dups[t], bad[t]);
        if (bad[t]) die(std::string("bad entries in ") + ENTRY_FILES[t]);
        if (bytes[t] != entries[t].n) die(std::string("entries not covered exactly: ") + ENTRY_FILES[t]);
    }
    if (rows_unsorted) die("geo_cells rows out of order");
    for (uint8_t m : multi) r.addr_multi += m;
    STAT("  addr points listed by more than one cell: %" PRIu64 "\n", r.addr_multi);
    return r;
}

// Rewrite every list of table t through remap (old -> new), re-sorted.
static void rewrite_entries(const Mapped& geo, const Mapped& entries, int t, const std::vector<uint32_t>& remap,
                            const std::string& path) {
    Buf<uint8_t> out(entries.n);
    memcpy(out.data(), entries.p, entries.n);
    const size_t rows = geo.count<GeoRow>();
    const GeoRow* g = geo.as<GeoRow>();
    uint64_t missing = 0;
#pragma omp parallel reduction(+ : missing)
    {
        std::vector<uint32_t> buf;
#pragma omp for schedule(dynamic, 1 << 16)
        for (size_t i = 0; i < rows; i++) {
            const uint32_t off = g[i].off[t];
            if (off == NO) continue;
            uint16_t c;
            memcpy(&c, entries.p + off, 2);
            buf.resize(c);
            memcpy(buf.data(), entries.p + off + 2, size_t(c) * 4);
            for (auto& id : buf) {
                const uint32_t m = remap[id];
                if (m == NO) missing++;
                id = m;
            }
            std::sort(buf.begin(), buf.end());
            memcpy(out.data() + off + 2, buf.data(), size_t(c) * 4);
        }
    }
    if (missing) die(std::string("cell list references a dropped record in ") + ENTRY_FILES[t]);
    write_file(path, out.data(), out.size());
}

// ---------------------------------------------------------------- helpers
template <class Key>
static std::vector<Key> sorted_keys(size_t n, const std::vector<uint8_t>& keep, Key (*make)(size_t, const void*),
                                    const void* ctx) {
    std::vector<size_t> live;
    live.reserve(n);
    for (size_t i = 0; i < n; i++)
        if (keep[i]) live.push_back(i);
    std::vector<Key> keys(live.size());
#pragma omp parallel for schedule(static)
    for (size_t k = 0; k < live.size(); k++) keys[k] = make(live[k], ctx);
    __gnu_parallel::sort(keys.begin(), keys.end());
    return keys;
}

static std::vector<uint32_t> invert(const std::vector<uint32_t>& perm, size_t n_old) {
    std::vector<uint32_t> remap(n_old, NO);
#pragma omp parallel for schedule(static)
    for (size_t k = 0; k < perm.size(); k++) remap[perm[k]] = uint32_t(k);
    return remap;
}

// Permute a u32-per-record sidecar file.
static void permute_u32(const std::string& src, const std::string& dst, const std::vector<uint32_t>& perm,
                        size_t n_old) {
    Mapped m = map_file(src);
    if (m.count<uint32_t>() != n_old) die("sidecar length mismatch: " + src);
    const uint32_t* v = m.as<uint32_t>();
    Buf<uint32_t> out(perm.size());
#pragma omp parallel for schedule(static)
    for (size_t k = 0; k < perm.size(); k++) out[k] = v[perm[k]];
    write_file(dst, out.data(), out.size() * 4);
    munmap(const_cast<uint8_t*>(m.p), m.n);
}

// Records of a node-array family (ways / interps): repack nodes in perm order.
template <class Rec>
static void write_noded(const std::string& out_dir, const char* rec_file, const char* node_file, const Rec* recs,
                        const NodeCoord* nodes, size_t n_nodes, const std::vector<uint32_t>& perm) {
    const size_t n = perm.size();
    Buf<Rec> out(n);
    std::vector<uint64_t> at(n + 1, 0);
    for (size_t k = 0; k < n; k++) at[k + 1] = at[k] + recs[perm[k]].cnt;
    if (at[n] > 0xFFFFFFFFull) die("node offsets overflow");
    Buf<NodeCoord> nout(at[n]);
    uint64_t oob = 0;
#pragma omp parallel for schedule(dynamic, 4096) reduction(+ : oob)
    for (size_t k = 0; k < n; k++) {
        Rec r = recs[perm[k]];
        if (r.cnt) {
            if (size_t(r.off) + r.cnt > n_nodes) { oob++; continue; }
            memcpy(&nout[at[k]], nodes + r.off, size_t(r.cnt) * sizeof(NodeCoord));
            r.off = uint32_t(at[k]);
        }
        out[k] = r;
    }
    if (oob) die(std::string("node range out of bounds in ") + rec_file);
    write_file(out_dir + "/" + rec_file, out.data(), out.size() * sizeof(Rec));
    write_file(out_dir + "/" + node_file, nout.data(), nout.size() * sizeof(NodeCoord));
    STAT("  wrote %s %zu records, %s %zu nodes (src %zu)\n", rec_file, n, node_file, nout.size(), n_nodes);
}

// ---------------------------------------------------------------- ways (street + interp)
// Way key modes:
//   tie  (default): hilbert24(first node), name, first node lat, lng, slot.
//        Ways with the same name and the same first node are exactly the
//        reader's street tie class (it compares dist, name, first lat, first
//        lng, then takes the earlier-listed way), so they keep slot order and
//        every street answer stays as published.
//   spec : hilbert24(first node), name, node_count, slot (node_count can
//        reorder a tie class against slot order and flip the reader's choice).
static bool g_spec_key = false;

struct WayKey {
    uint64_t h;
    uint32_t name;
    uint32_t lat, lng;  // first node (tie mode; 0 in spec mode)
    uint16_t cnt;       // node count (spec mode orders by it; tie mode only reports)
    uint32_t slot;
    bool operator<(const WayKey& o) const {
        if (h != o.h) return h < o.h;
        if (name != o.name) return name < o.name;
        if (g_spec_key) {
            if (cnt != o.cnt) return cnt < o.cnt;
        } else {
            if (lat != o.lat) return lat < o.lat;
            if (lng != o.lng) return lng < o.lng;
        }
        return slot < o.slot;
    }
    bool same_but_slot(const WayKey& o) const {
        return h == o.h && name == o.name && (g_spec_key ? cnt == o.cnt : (lat == o.lat && lng == o.lng));
    }
};

struct WayCtx { const void* recs; size_t stride; const NodeCoord* nodes; size_t n_nodes; };

template <class Rec>
static WayKey make_way_key(size_t i, const void* c) {
    const WayCtx* ctx = static_cast<const WayCtx*>(c);
    const Rec& r = static_cast<const Rec*>(ctx->recs)[i];
    uint32_t name;
    if constexpr (sizeof(Rec) == sizeof(WayHeader)) name = r.name; else name = r.street;
    uint64_t h = ~uint64_t(0);  // node-less records last
    uint32_t lat = 0, lng = 0;
    if (r.cnt && size_t(r.off) < ctx->n_nodes) {
        const NodeCoord nd = ctx->nodes[r.off];
        h = hilbert24(nd.lat, nd.lng);
        if (!g_spec_key) { lat = fkey(nd.lat); lng = fkey(nd.lng); }
    }
    return WayKey{h, name, lat, lng, r.cnt, uint32_t(i)};
}

struct TieStats { uint64_t groups = 0, members = 0; };

// Runs whose keys differ only in slot (the history-dependent tiebreak); of
// those, the runs whose members differ in node_count (which a content rule
// could have ordered, at the price of the reader's tie choice).
static void way_tiebreak_stats(const char* fam, const std::vector<WayKey>& k) {
    TieStats slot_t, cnt_t;
    size_t i = 0;
    while (i < k.size()) {
        size_t j = i + 1;
        while (j < k.size() && k[j].same_but_slot(k[i])) j++;
        if (j - i > 1) {
            slot_t.groups++;
            slot_t.members += j - i;
            bool cnt_differs = false;
            for (size_t a = i + 1; a < j; a++) cnt_differs |= k[a].cnt != k[i].cnt;
            if (cnt_differs) { cnt_t.groups++; cnt_t.members += j - i; }
        }
        i = j;
    }
    STAT("  %s tiebreak (%s key): slot needed in %" PRIu64 " groups / %" PRIu64 " records (of %zu); "
         "node_count differs inside %" PRIu64 " of those groups / %" PRIu64 " records\n",
         fam, g_spec_key ? "spec" : "tie", slot_t.groups, slot_t.members, k.size(), cnt_t.groups, cnt_t.members);
}

// Reader tie classes: records with equal name and identical first node tie
// in the reader's street comparison (dist, name, first lat, first lng); among
// them the earlier-listed one wins. Count classes whose new order differs
// from slot order.
template <class Rec>
static void reader_tie_stats(const char* fam, const Rec* recs, const NodeCoord* nodes, size_t n_nodes,
                             const std::vector<uint32_t>& perm, const std::vector<uint32_t>& remap) {
    struct T { uint32_t name, lat, lng, slot, nid; };
    std::vector<T> v;
    v.reserve(perm.size());
    for (size_t k = 0; k < perm.size(); k++) {
        const Rec& r = recs[perm[k]];
        if (!r.cnt || size_t(r.off) >= n_nodes) continue;
        uint32_t name;
        if constexpr (sizeof(Rec) == sizeof(WayHeader)) name = r.name; else name = r.street;
        v.push_back(T{name, fkey(nodes[r.off].lat), fkey(nodes[r.off].lng), perm[k], uint32_t(k)});
    }
    __gnu_parallel::sort(v.begin(), v.end(), [](const T& a, const T& b) {
        if (a.name != b.name) return a.name < b.name;
        if (a.lat != b.lat) return a.lat < b.lat;
        if (a.lng != b.lng) return a.lng < b.lng;
        return a.slot < b.slot;
    });
    uint64_t classes = 0, members = 0, flipped = 0, flipped_members = 0;
    size_t i = 0;
    while (i < v.size()) {
        size_t j = i + 1;
        while (j < v.size() && v[j].name == v[i].name && v[j].lat == v[i].lat && v[j].lng == v[i].lng) j++;
        if (j - i > 1) {
            classes++;
            members += j - i;
            bool ok = true;
            for (size_t a = i + 1; a < j; a++)
                if (v[a].nid < v[a - 1].nid) ok = false;
            if (!ok) { flipped++; flipped_members += j - i; }
        }
        i = j;
    }
    (void)remap;
    STAT("  %s reader tie classes (same name + same first node): %" PRIu64 " classes / %" PRIu64
         " records; new order != slot order in %" PRIu64 " classes / %" PRIu64 " records\n",
         fam, classes, members, flipped, flipped_members);
}

// ---------------------------------------------------------------- interp tie constraints
// query_geo keeps the first-listed of equally near interpolation lines (it
// compares distance only), and lines that share a vertex are equally near
// for every query whose nearest point on both is that vertex. Two such lines
// the reader can tell apart -- other street, other TIGER postcode, or the
// same start number and type (find_interp_on_way's tie) -- keep their
// published (slot) order; everything else follows the key. The order is
// Kahn's topological sort of those slot-order constraints, always taking the
// available line with the smallest key, so it is the key order wherever no
// constraint objects.
static bool g_interp_constrain = true;

static std::vector<uint32_t> constrained_interp_order(const InterpWay* recs, const NodeCoord* nodes, size_t n,
                                                      const std::vector<WayKey>& keys, const uint32_t* pc) {
    const size_t m = keys.size();
    std::vector<uint32_t> rank(n, NO);
    for (size_t k = 0; k < m; k++) rank[keys[k].slot] = uint32_t(k);
    struct __attribute__((packed)) Inc {
        uint64_t v;
        uint32_t s;
        bool operator<(const Inc& o) const { return v != o.v ? v < o.v : s < o.s; }
    };
    std::vector<uint64_t> at(m + 1, 0);
    for (size_t k = 0; k < m; k++) at[k + 1] = at[k] + recs[keys[k].slot].cnt;
    std::vector<Inc> inc(at[m]);
#pragma omp parallel for schedule(dynamic, 4096)
    for (size_t k = 0; k < m; k++) {
        const uint32_t s = keys[k].slot;
        const InterpWay& r = recs[s];
        for (uint32_t j = 0; j < r.cnt; j++) {
            uint32_t la, lo;
            memcpy(&la, &nodes[r.off + j].lat, 4);
            memcpy(&lo, &nodes[r.off + j].lng, 4);
            inc[at[k] + j] = Inc{(uint64_t(la) << 32) | lo, s};
        }
    }
    __gnu_parallel::sort(inc.begin(), inc.end());
    auto observable = [&](uint32_t a, uint32_t b) {
        const InterpWay &x = recs[a], &y = recs[b];
        return x.street != y.street || (pc && pc[a] != pc[b]) || (x.start == y.start && x.interp == y.interp);
    };
    // Runs of one vertex, split between threads at run starts.
    const int T = omp_get_max_threads();
    std::vector<size_t> cut(T + 1, inc.size());
    cut[0] = 0;
    for (int t = 1; t < T; t++) {
        size_t c = inc.size() * t / T;
        while (c > 0 && c < inc.size() && inc[c].v == inc[c - 1].v) c++;
        cut[t] = std::max(c, cut[t - 1]);
    }
    std::vector<std::vector<std::pair<uint32_t, uint32_t>>> local(T);
    uint64_t shared_vertices = 0, big = 0;
#pragma omp parallel num_threads(T) reduction(+ : shared_vertices, big)
    {
        const int t = omp_get_thread_num();
        std::vector<uint32_t> mem;
        for (size_t i = cut[t]; i < cut[t + 1];) {
            size_t j = i + 1;
            while (j < cut[t + 1] && inc[j].v == inc[i].v) j++;
            mem.clear();
            for (size_t a = i; a < j; a++)
                if (mem.empty() || mem.back() != inc[a].s) mem.push_back(inc[a].s);
            if (mem.size() > 1) {
                shared_vertices++;
                if (mem.size() > 64) {
                    big++;
                    for (size_t a = 1; a < mem.size(); a++) local[t].push_back({mem[a - 1], mem[a]});
                } else {
                    for (size_t a = 0; a < mem.size(); a++)
                        for (size_t b = a + 1; b < mem.size(); b++)
                            if (observable(mem[a], mem[b])) local[t].push_back({mem[a], mem[b]});
                }
            }
            i = j;
        }
    }
    inc = {};
    std::vector<std::pair<uint32_t, uint32_t>> edges;
    for (auto& l : local) { edges.insert(edges.end(), l.begin(), l.end()); l = {}; }
    for (auto& e : edges) e = {rank[e.first], rank[e.second]};
    __gnu_parallel::sort(edges.begin(), edges.end());
    edges.erase(std::unique(edges.begin(), edges.end()), edges.end());
    uint64_t against = 0;
    std::vector<uint32_t> indeg(m, 0);
    std::vector<uint64_t> first(m + 1, 0);
    for (auto& e : edges) {
        if (e.first > e.second) against++;
        indeg[e.second]++;
        first[e.first + 1]++;
    }
    for (size_t k = 0; k < m; k++) first[k + 1] += first[k];
    std::priority_queue<uint32_t, std::vector<uint32_t>, std::greater<uint32_t>> ready;
    for (size_t k = 0; k < m; k++)
        if (!indeg[k]) ready.push(uint32_t(k));
    std::vector<uint32_t> perm;
    perm.reserve(m);
    uint64_t moved = 0, moved_far = 0;
    while (!ready.empty()) {
        const uint32_t r = ready.top();
        ready.pop();
        if (r != perm.size()) {
            moved++;
            if (std::max<uint64_t>(r, perm.size()) - std::min<uint64_t>(r, perm.size()) > 1000) moved_far++;
        }
        perm.push_back(keys[r].slot);
        for (uint64_t e = first[r]; e < first[r + 1]; e++)
            if (--indeg[edges[e].second] == 0) ready.push(edges[e].second);
    }
    if (perm.size() != m) die("interp tie constraints form a cycle");
    // Lines out of key order: all but a longest run that stays in key order
    // (longest increasing subsequence of the emitted ranks).
    std::vector<uint32_t> tails;
    for (uint32_t s : perm) {
        const uint32_t r = rank[s];
        auto it = std::lower_bound(tails.begin(), tails.end(), r);
        if (it == tails.end()) tails.push_back(r); else *it = r;
    }
    STAT("  interp tie constraints: %" PRIu64 " shared vertices (%" PRIu64 " with > 64 lines), %zu slot-order "
         "constraints, %" PRIu64 " of them against the key order; %zu lines out of key order (n - LIS), %" PRIu64
         " lines more than 1000 places from their key position (%" PRIu64 " shifted at all)\n",
         shared_vertices, big, edges.size(), against, m - tails.size(), moved_far, moved);
    return perm;
}

template <class Rec>
struct WayFamily {
    std::vector<uint32_t> perm, remap;
};

template <class Rec>
static WayFamily<Rec> sort_way_family(const char* fam, const std::string& src, const std::string& out,
                                      const std::string& meta, const char* rec_file, const char* node_file,
                                      const std::vector<uint8_t>& ref_entries, const std::vector<uint8_t>* ref_other,
                                      const std::vector<const char*>& sidecars) {
    Mapped rm = map_file(src + "/" + rec_file), nm = map_file(src + "/" + node_file);
    const size_t n = rm.count<Rec>(), n_nodes = nm.count<NodeCoord>();
    const Rec* recs = rm.as<Rec>();
    const NodeCoord* nodes = nm.as<NodeCoord>();
    if (ref_entries.size() != n) die("ref size");
    // Tombstone content (strategy-2 fill): node_count 0, offset 0, name NO_DATA, rest 0.
    auto tomb = [&](const Rec& r) {
        if constexpr (sizeof(Rec) == sizeof(WayHeader)) return r.cnt == 0 && r.off == 0 && r.name == NO && r.pad == 0;
        else return r.cnt == 0 && r.off == 0 && r.street == NO && r.start == 0 && r.end == 0 && r.interp == 0;
    };
    std::vector<uint8_t> keep(n, 1);
    uint64_t n_tomb = 0, tomb_ref = 0, live_unref = 0, live_cnt0 = 0, packed_bad = 0;
#pragma omp parallel for schedule(static) reduction(+ : n_tomb, tomb_ref, live_unref, live_cnt0)
    for (size_t i = 0; i < n; i++) {
        const bool referenced = ref_entries[i] || (ref_other && (*ref_other)[i]);
        if (tomb(recs[i])) {
            n_tomb++;
            if (referenced) tomb_ref++;
            else keep[i] = 0;
        } else {
            if (!ref_entries[i]) live_unref++;
            if (recs[i].cnt == 0) live_cnt0++;
        }
    }
    // Are the source nodes packed in slot order (as the builder repacks)?
    {
        uint64_t at = 0;
        for (size_t i = 0; i < n; i++) {
            if (!recs[i].cnt) continue;
            if (recs[i].off != at) packed_bad++;
            at = uint64_t(recs[i].off) + recs[i].cnt;
        }
    }
    STAT("%s: %zu slots, %" PRIu64 " tombstones dropped, %" PRIu64 " tombstone-looking but referenced (kept), "
         "%" PRIu64 " live not listed by any cell, %" PRIu64 " live with node_count 0, src nodes packed in slot "
         "order: %s (%" PRIu64 " breaks)\n",
         fam, n, n_tomb - tomb_ref, tomb_ref, live_unref, live_cnt0, packed_bad ? "no" : "yes", packed_bad);
    WayCtx ctx{recs, sizeof(Rec), nodes, n_nodes};
    std::vector<WayKey> keys = sorted_keys<WayKey>(n, keep, &make_way_key<Rec>, &ctx);
    way_tiebreak_stats(fam, keys);
    WayFamily<Rec> f;
    if constexpr (sizeof(Rec) == sizeof(InterpWay)) {
        if (g_interp_constrain) {
            Mapped pcm = map_file(src + "/interp_postcodes.bin");
            if (pcm.count<uint32_t>() != n) die("interp_postcodes length");
            f.perm = constrained_interp_order(recs, nodes, n, keys, pcm.as<uint32_t>());
            munmap(const_cast<uint8_t*>(pcm.p), pcm.n);
        }
    }
    if (f.perm.empty()) {
        f.perm.resize(keys.size());
        for (size_t k = 0; k < keys.size(); k++) f.perm[k] = keys[k].slot;
    }
    keys = {};
    f.remap = invert(f.perm, n);
    reader_tie_stats<Rec>(fam, recs, nodes, n_nodes, f.perm, f.remap);
    write_noded<Rec>(out, rec_file, node_file, recs, nodes, n_nodes, f.perm);
    for (const char* sc : sidecars) permute_u32(src + "/" + sc, out + "/" + sc, f.perm, n);
    write_file(meta + "/" + std::string(fam) + ".slot.u32", f.perm.data(), f.perm.size() * 4);
    munmap(const_cast<uint8_t*>(rm.p), rm.n);
    munmap(const_cast<uint8_t*>(nm.p), nm.n);
    return f;
}

// ---------------------------------------------------------------- addr points
struct AddrKey {
    uint32_t row;
    uint64_t h;
    uint32_t lat, lng, hn, street;
    uint32_t slot;
    bool operator<(const AddrKey& o) const {
        if (row != o.row) return row < o.row;
        if (h != o.h) return h < o.h;
        if (lat != o.lat) return lat < o.lat;
        if (lng != o.lng) return lng < o.lng;
        if (hn != o.hn) return hn < o.hn;
        if (street != o.street) return street < o.street;
        return slot < o.slot;
    }
    bool same_but_slot(const AddrKey& o) const {
        return row == o.row && h == o.h && lat == o.lat && lng == o.lng && hn == o.hn && street == o.street;
    }
};
struct AddrCtx { const AddrPoint* p; const uint32_t* row; };
static AddrKey make_addr_key(size_t i, const void* c) {
    const AddrCtx* ctx = static_cast<const AddrCtx*>(c);
    const AddrPoint& a = ctx->p[i];
    return AddrKey{ctx->row[i], hilbert24(a.lat, a.lng), fkey(a.lat), fkey(a.lng), a.hn, a.street, uint32_t(i)};
}

static size_t poly_bytes(const uint8_t* v, size_t vn, uint32_t off, uint32_t cnt) {
    if (size_t(off) >= vn) die("vertex offset past addr_vertices");
    const uint8_t enc = v[off];
    const size_t stride = enc >= 3 ? 8 : 4;  // VERTEX_STRIDE {4,4,4,8}
    if (enc > 3) die("unknown vertex encoding");
    return 10 + size_t(cnt) * stride;
}

static std::vector<uint32_t> sort_addrs(const std::string& src, const std::string& out, const std::string& meta,
                                        const Refs& refs, const std::vector<uint32_t>& way_remap) {
    Mapped pm = map_file(src + "/addr_points.bin"), vm = map_file(src + "/addr_vertices.bin");
    const size_t n = pm.count<AddrPoint>();
    const AddrPoint* p = pm.as<AddrPoint>();
    std::vector<uint8_t> keep(n, 1);
    uint64_t n_tomb = 0, tomb_ref = 0, live_unref = 0;
    auto tomb = [&](const AddrPoint& a) {
        return a.lat == 0 && a.lng == 0 && a.hn == 0 && a.street == 0 && a.parent == NO && a.voff == NO &&
               a.vcnt == 0;
    };
#pragma omp parallel for schedule(static) reduction(+ : n_tomb, tomb_ref, live_unref)
    for (size_t i = 0; i < n; i++) {
        if (tomb(p[i])) {
            n_tomb++;
            if (refs.ref[1][i]) tomb_ref++;
            else keep[i] = 0;
        } else if (!refs.ref[1][i]) {
            live_unref++;
        }
    }
    // Source vertex blobs: packed in slot order without gaps?
    uint64_t vbreaks = 0, polys = 0;
    {
        uint64_t at = 0;
        for (size_t i = 0; i < n; i++) {
            if (!p[i].vcnt || p[i].voff == NO) continue;
            polys++;
            if (p[i].voff != at) vbreaks++;
            at = uint64_t(p[i].voff) + poly_bytes(vm.p, vm.n, p[i].voff, p[i].vcnt);
        }
        STAT("addr: src vertex blobs %" PRIu64 ", packed in slot order: %s (%" PRIu64 " breaks), end %" PRIu64
             " of %zu bytes\n", polys, vbreaks ? "no" : "yes", vbreaks, at, vm.n);
    }
    STAT("addr: %zu slots, %" PRIu64 " tombstones dropped, %" PRIu64 " tombstone-looking but listed (kept), %" PRIu64
         " live not listed by any cell (sorted last)\n", n, n_tomb - tomb_ref, tomb_ref, live_unref);
    AddrCtx ctx{p, refs.addr_row.data()};
    std::vector<AddrKey> keys = sorted_keys<AddrKey>(n, keep, &make_addr_key, &ctx);
    {
        uint64_t groups = 0, members = 0;
        size_t i = 0;
        while (i < keys.size()) {
            size_t j = i + 1;
            while (j < keys.size() && keys[j].same_but_slot(keys[i])) j++;
            if (j - i > 1) { groups++; members += j - i; }
            i = j;
        }
        STAT("  addr tiebreak: slot needed in %" PRIu64 " groups / %" PRIu64 " records (of %zu): identical cell, "
             "point, housenumber and street\n", groups, members, keys.size());
    }
    std::vector<uint32_t> perm(keys.size());
    for (size_t k = 0; k < keys.size(); k++) perm[k] = keys[k].slot;
    keys = {};
    std::vector<uint32_t> remap = invert(perm, n);

    // Records + vertex blobs in new order.
    const size_t m = perm.size();
    Buf<AddrPoint> outp(m);
    std::vector<uint64_t> vat(m + 1, 0);
    for (size_t k = 0; k < m; k++) {
        const AddrPoint& a = p[perm[k]];
        vat[k + 1] = vat[k] + ((a.vcnt && a.voff != NO) ? poly_bytes(vm.p, vm.n, a.voff, a.vcnt) : 0);
    }
    if (vat[m] > 0xFFFFFFFFull) die("addr vertex offsets overflow");
    Buf<uint8_t> outv(vat[m]);
    uint64_t bad_parent = 0, parents = 0;
#pragma omp parallel for schedule(dynamic, 8192) reduction(+ : bad_parent, parents)
    for (size_t k = 0; k < m; k++) {
        AddrPoint a = p[perm[k]];
        if (a.parent != NO) {
            parents++;
            if (a.parent >= way_remap.size() || way_remap[a.parent] == NO) bad_parent++;
            else a.parent = way_remap[a.parent];
        }
        if (a.vcnt && a.voff != NO) {
            const size_t len = vat[k + 1] - vat[k];
            if (size_t(a.voff) + len > vm.n) die("vertex blob past addr_vertices");
            memcpy(outv.data() + vat[k], vm.p + a.voff, len);
            a.voff = uint32_t(vat[k]);
        }
        outp[k] = a;
    }
    if (bad_parent) die("addr parent_way_id points to a dropped/out-of-range way");
    STAT("  addr parent_way_id remapped: %" PRIu64 "; addr_vertices %zu -> %zu bytes\n", parents, vm.n, outv.size());
    write_file(out + "/addr_points.bin", outp.data(), outp.size() * sizeof(AddrPoint));
    write_file(out + "/addr_vertices.bin", outv.data(), outv.size());
    permute_u32(src + "/addr_postcodes.bin", out + "/addr_postcodes.bin", perm, n);
    write_file(meta + "/addr.slot.u32", perm.data(), perm.size() * 4);
    munmap(const_cast<uint8_t*>(pm.p), pm.n);
    munmap(const_cast<uint8_t*>(vm.p), vm.n);
    return remap;
}

int main(int argc, char** argv) {
    if (argc < 4) {
        fprintf(stderr, "usage: canon_sort SRC_FULL_DIR OUT_DIR META_DIR [--threads N] [--way-key tie|spec] [--interp-order constrained|key]\n");
        return 1;
    }
    const std::string src = argv[1], out = argv[2], meta = argv[3];
    for (int i = 4; i < argc; i++) {
        if (!strcmp(argv[i], "--threads") && i + 1 < argc) omp_set_num_threads(atoi(argv[++i]));
        else if (!strcmp(argv[i], "--way-key") && i + 1 < argc) {
            const std::string k = argv[++i];
            if (k == "spec") g_spec_key = true;
            else if (k != "tie") die("--way-key spec|tie");
        } else if (!strcmp(argv[i], "--interp-order") && i + 1 < argc) {
            const std::string k = argv[++i];
            if (k == "key") g_interp_constrain = false;
            else if (k != "constrained") die("--interp-order constrained|key");
        } else die(std::string("unknown argument ") + argv[i]);
    }
    g_t0 = now_s();
    g_stats = fopen((meta + "/stats.txt").c_str(), "w");
    if (!g_stats) die("stats file");
    STAT("canon_sort %s -> %s (threads %d, way key %s, interp order %s)\n", src.c_str(), out.c_str(),
         omp_get_max_threads(), g_spec_key ? "spec" : "tie", g_interp_constrain ? "constrained" : "key");

    Mapped geo = map_file(src + "/geo_cells.bin");
    Mapped ent[3];
    for (int t = 0; t < 3; t++) ent[t] = map_file(src + "/" + ENTRY_FILES[t]);
    size_t n_rec[3];
    {
        Mapped w = map_file(src + "/street_ways.bin"), a = map_file(src + "/addr_points.bin"),
               iw = map_file(src + "/interp_ways.bin");
        n_rec[0] = w.count<WayHeader>();
        n_rec[1] = a.count<AddrPoint>();
        n_rec[2] = iw.count<InterpWay>();
        // addr points' parent ways count as references to street ways
        munmap(const_cast<uint8_t*>(w.p), w.n);
        munmap(const_cast<uint8_t*>(iw.p), iw.n);
        munmap(const_cast<uint8_t*>(a.p), a.n);
    }
    phase("scan geo index");
    Refs refs = scan_geo(geo, ent, n_rec);

    std::vector<uint8_t> parent_ref(n_rec[0], 0);
    {
        Mapped a = map_file(src + "/addr_points.bin");
        const AddrPoint* p = a.as<AddrPoint>();
        uint64_t oob = 0;
#pragma omp parallel for schedule(static) reduction(+ : oob)
        for (size_t i = 0; i < n_rec[1]; i++) {
            const uint32_t w = p[i].parent;
            if (w == NO) continue;
            if (w >= n_rec[0]) { oob++; continue; }
            __atomic_store_n(&parent_ref[w], uint8_t(1), __ATOMIC_RELAXED);
        }
        if (oob) die("addr parent_way_id out of range");
        munmap(const_cast<uint8_t*>(a.p), a.n);
    }

    phase("street ways");
    auto streets = sort_way_family<WayHeader>("street", src, out, meta, "street_ways.bin", "street_nodes.bin",
                                              refs.ref[0], &parent_ref, {"way_parents.bin", "way_postcodes.bin"});
    phase("interp ways");
    auto interps = sort_way_family<InterpWay>("interp", src, out, meta, "interp_ways.bin", "interp_nodes.bin",
                                              refs.ref[2], nullptr, {"interp_postcodes.bin"});
    phase("addr points");
    std::vector<uint32_t> addr_remap = sort_addrs(src, out, meta, refs, streets.remap);

    phase("entries");
    const std::vector<uint32_t>* remaps[3] = {&streets.remap, &addr_remap, &interps.remap};
    for (int t = 0; t < 3; t++) {
        rewrite_entries(geo, ent[t], t, *remaps[t], out + "/" + ENTRY_FILES[t]);
    }
    phase("done");
    STAT("elapsed %.2f s\n", now_s() - g_t0);
    fclose(g_stats);
    return 0;
}
