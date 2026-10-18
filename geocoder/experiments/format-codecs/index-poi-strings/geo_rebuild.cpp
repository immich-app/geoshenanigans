// Can geo_cells.bin + street/addr/interp_entries.bin be rebuilt at install/load from the
// shipped geometry? Recomputes every (level-17 cell, item) pair with the builder's own
// covering code (point_to_cell / cover_edge / cover_long_edge / emit_polyline_cells copied
// verbatim from builder/src/s2_helpers.cpp + build_index.cpp at main 5caed45), on ONE core,
// and compares the sorted pairs with the pairs decoded from the shipped files.
//
// usage: geo_rebuild <index dir> <table: street|interp|addr>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

#include <s2/s2cell.h>
#include <s2/s2coords.h>
#include <s2/s2latlng.h>
#include <s2/s2polyline.h>
#include <s2/s2region_coverer.h>

#include "segment_rect.h"  // builder/src/segment_rect.h, verbatim

static int kStreetCellLevel = 17;
static const uint32_t NO_DATA = 0xFFFFFFFFu;

struct NodeCoord { float lat, lng; };
struct CellItemPair { uint64_t cell_id; uint32_t item_id; };
static bool cell_item_less(const CellItemPair& a, const CellItemPair& b) {
    return a.cell_id != b.cell_id ? a.cell_id < b.cell_id : a.item_id < b.item_id;
}

// ---- verbatim from builder/src/s2_helpers.cpp ----
static void cover_long_edge(const S2Point& p1, const S2Point& p2, std::vector<S2CellId>& out) {
    thread_local S2RegionCoverer start_coverer = []() {
        S2RegionCoverer::Options options;
        options.set_max_cells(4);
        options.set_max_level(kStreetCellLevel);
        return S2RegionCoverer(options);
    }();
    static constexpr double kMinFaceCosine = 0.1;
    std::vector<S2Point> points = {p1, p2};
    S2Polyline polyline(points);
    thread_local std::vector<S2CellId> start;
    start_coverer.GetFastCovering(polyline, &start);
    struct Cell { int i, j, level; };
    thread_local std::vector<Cell> pending;
    for (S2CellId id : start) {
        int i, j;
        const int face = id.ToFaceIJOrientation(&i, &j, nullptr);
        const S2Point norm = S2::GetNorm(face);
        const bool planar = p1.DotProd(norm) > kMinFaceCosine && p2.DotProd(norm) > kMinFaceCosine;
        R2Point a, b;
        if (planar) {
            S2::ValidFaceXYZtoUV(face, p1, &a);
            S2::ValidFaceXYZtoUV(face, p2, &b);
        }
        const int size = S2CellId::GetSizeIJ(id.level());
        pending.assign(1, Cell{i & -size, j & -size, id.level()});
        while (!pending.empty()) {
            const Cell c = pending.back();
            pending.pop_back();
            const int cell_size = S2CellId::GetSizeIJ(c.level);
            SegmentRect meets = SegmentRect::Unsure;
            if (planar) {
                meets = segment_meets_rect(a[0], a[1], b[0], b[1],
                    S2::STtoUV(S2::IJtoSTMin(c.i)), S2::STtoUV(S2::IJtoSTMin(c.i + cell_size)),
                    S2::STtoUV(S2::IJtoSTMin(c.j)), S2::STtoUV(S2::IJtoSTMin(c.j + cell_size)));
            }
            if (meets == SegmentRect::Disjoint) continue;
            auto cell_id = [&] { return S2CellId::FromFaceIJ(face, c.i, c.j).parent(c.level); };
            if (meets == SegmentRect::Unsure && !polyline.MayIntersect(S2Cell(cell_id()))) continue;
            if (c.level >= kStreetCellLevel) {
                out.push_back(cell_id());
                continue;
            }
            const int half = cell_size >> 1;
            for (int k = 0; k < 4; k++)
                pending.push_back({c.i + (k & 1) * half, c.j + (k >> 1) * half, c.level + 1});
        }
    }
    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
}

static void cover_edge(S2CellId c1, S2CellId c2, double lat1, double lng1, double lat2, double lng2,
                       std::vector<S2CellId>& out) {
    out.clear();
    if (c1 == c2) { out.push_back(c1); return; }
    S2CellId cm = S2CellId(S2LatLng::FromDegrees((lat1+lat2)/2, (lng1+lng2)/2)).parent(kStreetCellLevel);
    if (cm == c1 || cm == c2) { out.push_back(c1); out.push_back(c2); return; }
    S2Point p1 = S2LatLng::FromDegrees(lat1, lng1).ToPoint();
    S2Point p2 = S2LatLng::FromDegrees(lat2, lng2).ToPoint();
    if (p1 == p2) { out.push_back(c1); return; }
    cover_long_edge(p1, p2, out);
}

static S2CellId point_to_cell(double lat, double lng) {
    return S2CellId(S2LatLng::FromDegrees(lat, lng)).parent(kStreetCellLevel);
}

// ---- verbatim from builder/src/build_index.cpp ----
struct EdgeCellScratch {
    std::vector<S2CellId> node_cells;
    std::vector<S2CellId> edge_cells;
    std::vector<uint64_t> cells;
};
static void emit_polyline_cells(const NodeCoord* nodes, uint16_t count, uint32_t item,
                EdgeCellScratch& s, std::vector<CellItemPair>& out) {
    s.cells.clear();
    s.node_cells.clear();
    if (count >= 2)
        for (uint16_t j = 0; j < count; j++) s.node_cells.push_back(point_to_cell(nodes[j].lat, nodes[j].lng));
    for (uint16_t j = 0; j + 1 < count; j++) {
        cover_edge(s.node_cells[j], s.node_cells[j + 1], nodes[j].lat, nodes[j].lng, nodes[j + 1].lat,
                   nodes[j + 1].lng, s.edge_cells);
        for (const auto& c : s.edge_cells) s.cells.push_back(c.id());
    }
    std::sort(s.cells.begin(), s.cells.end());
    s.cells.erase(std::unique(s.cells.begin(), s.cells.end()), s.cells.end());
    for (uint64_t cell_id : s.cells) out.push_back({cell_id, item});
}
// ---- end verbatim ----

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
static double now() { return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count(); }
static long peak_rss_mb() { struct rusage u; getrusage(RUSAGE_SELF, &u); return u.ru_maxrss / 1024; }

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "usage: geo_rebuild <dir> street|interp|addr\n"); return 2; }
    const std::string dir = argv[1], table = argv[2];
    const int col = table == "street" ? 0 : table == "addr" ? 1 : 2;
    const char* efile = col == 0 ? "street_entries.bin" : col == 1 ? "addr_entries.bin" : "interp_entries.bin";

    std::vector<CellItemPair> got;
    double t0 = now();
    size_t items = 0, covered_items = 0;
    if (col == 0 || col == 2) {
        // street_ways: u32 node_offset, u16 node_count, pad, u32 name_id (12 B)
        // interp_ways: u32 node_offset, u16 node_count, pad, u32 street_id, u32 start, u32 end, u8 interp, pad3 (24 B)
        Map ways = map_file(dir + (col == 0 ? "/street_ways.bin" : "/interp_ways.bin"));
        Map nodes = map_file(dir + (col == 0 ? "/street_nodes.bin" : "/interp_nodes.bin"));
        const size_t stride = col == 0 ? 12 : 24;
        items = ways.n / stride;
        EdgeCellScratch scratch;
        const NodeCoord* nd = reinterpret_cast<const NodeCoord*>(nodes.p);
        for (size_t w = 0; w < items; w++) {
            const uint8_t* h = ways.p + w * stride;
            uint32_t off = rd<uint32_t>(h);
            uint16_t cnt = rd<uint16_t>(h + 4);
            if (col == 2) {
                // reader skips start/end == 0; the builder covers deferred interps (all parsed ones)
            }
            size_t before = got.size();
            emit_polyline_cells(nd + off, cnt, static_cast<uint32_t>(w), scratch, got);
            if (got.size() > before) covered_items++;
        }
    } else {
        // addr_points: f32 lat, f32 lng, ... (28 B): one cell per point at the stored f32 coordinate
        Map pts = map_file(dir + "/addr_points.bin");
        items = pts.n / 28;
        got.reserve(items);
        for (size_t i = 0; i < items; i++) {
            float lat = rd<float>(pts.p + i * 28), lng = rd<float>(pts.p + i * 28 + 4);
            got.push_back({point_to_cell(lat, lng).id(), static_cast<uint32_t>(i)});
        }
        covered_items = items;
    }
    double t1 = now();
    std::sort(got.begin(), got.end(), cell_item_less);
    if (col == 2) got.erase(std::unique(got.begin(), got.end(), [](const CellItemPair& a, const CellItemPair& b) {
        return a.cell_id == b.cell_id && a.item_id == b.item_id; }), got.end());
    double t2 = now();
    printf("%s: %zu items, %zu produced cells, %zu pairs; cover %.1fs sort %.1fs (1 core); peak RSS %ld MiB\n",
           table.c_str(), items, covered_items, got.size(), t1 - t0, t2 - t1, peak_rss_mb());
    fflush(stdout);

    // shipped pairs, in file order (cell ascending, list order)
    Map geo = map_file(dir + "/geo_cells.bin");
    Map ent = map_file(dir + "/" + efile);
    size_t rows = geo.n / 20;
    std::vector<CellItemPair> want;
    want.reserve(got.size() + (got.size() >> 4));
    size_t unsorted_lists = 0, dup_in_list = 0;
    for (size_t r = 0; r < rows; r++) {
        uint64_t cell = rd<uint64_t>(geo.p + r * 20);
        uint32_t off = rd<uint32_t>(geo.p + r * 20 + 8 + 4 * col);
        if (off == NO_DATA) continue;
        uint16_t c = rd<uint16_t>(ent.p + off);
        bool sorted = true;
        for (uint16_t k = 0; k < c; k++) {
            uint32_t id = rd<uint32_t>(ent.p + off + 2 + 4 * k);
            if (k && id <= want.back().item_id) { sorted = false; if (id == want.back().item_id) dup_in_list++; }
            want.push_back({cell, id});
        }
        if (!sorted) unsorted_lists++;
    }
    std::vector<CellItemPair> want_sorted = want;
    std::sort(want_sorted.begin(), want_sorted.end(), cell_item_less);
    size_t want_unique = std::unique(want_sorted.begin(), want_sorted.end(), [](const CellItemPair& a, const CellItemPair& b) {
        return a.cell_id == b.cell_id && a.item_id == b.item_id; }) - want_sorted.begin();
    want_sorted.resize(want_unique);
    // set comparison
    size_t only_got = 0, only_want = 0, both = 0;
    size_t i = 0, j = 0;
    std::vector<CellItemPair> ex_got, ex_want;
    while (i < got.size() || j < want_sorted.size()) {
        if (j == want_sorted.size() || (i < got.size() && cell_item_less(got[i], want_sorted[j]))) {
            if (ex_got.size() < 5) ex_got.push_back(got[i]);
            only_got++; i++;
        } else if (i == got.size() || cell_item_less(want_sorted[j], got[i])) {
            if (ex_want.size() < 5) ex_want.push_back(want_sorted[j]);
            only_want++; j++;
        } else { both++; i++; j++; }
    }
    printf("shipped pairs %zu (distinct %zu; lists not strictly ascending %zu, duplicate ids inside a list %zu)\n",
           want.size(), want_unique, unsorted_lists, dup_in_list);
    printf("rebuilt vs shipped (as sets): both %zu, only rebuilt %zu, only shipped %zu -> %s\n",
           both, only_got, only_want, (only_got == 0 && only_want == 0) ? "IDENTICAL SET" : "DIFFERENT");
    bool same_seq = got.size() == want.size() &&
        std::equal(got.begin(), got.end(), want.begin(), [](const CellItemPair& a, const CellItemPair& b) {
            return a.cell_id == b.cell_id && a.item_id == b.item_id; });
    printf("rebuilt sequence == shipped sequence (byte-identical files would follow): %s\n", same_seq ? "yes" : "no");
    for (auto& p : ex_got) printf("  only rebuilt: cell %016llx item %u\n", (unsigned long long)p.cell_id, p.item_id);
    for (auto& p : ex_want) printf("  only shipped: cell %016llx item %u\n", (unsigned long long)p.cell_id, p.item_id);
    printf("peak RSS %ld MiB\n", peak_rss_mb());
    return 0;
}
