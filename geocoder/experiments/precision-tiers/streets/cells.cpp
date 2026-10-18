// cells: level-17 geo-cell effects of the coordinate precision (builder order: SRC = out-nocap/planet/full,
// DUMPS = the aligned exact dumps). Uses the builder's own cover_edge / point_to_cell (s2_helpers.cpp).
//
//   cells stats   SRC DUMPS                  per-record cell-set changes between coordinate variants
//   cells reindex SRC DUMPS OUT today|snapf32 geo_cells.bin + street/addr/interp entries in today's format
//   cells cmpidx  DIR_A DIR_B                 (cell, id) pair sets of two geo indexes, per table
//
// Coordinate variants of a node / point: f32 = shipped f32, e5 = k / 1e5 (1.1 m base, double),
// e7 = E / 1e7 (exact, double), sf = f32(k / 1e5) (what the f32-format snap test's reader sees).
// "today" = the builder's own rule: streets / interps from the shipped f32, address nodes and POI-relation
// centroids from the exact double, building centroids from the f32 centroid.
#include "common.h"
#include <omp.h>
#include <s2/s2cell_id.h>
#include <s2/s2latlng.h>

// From the builder's s2_helpers.cpp (linked in; its header clashes with common.h's record structs).
void cover_edge(S2CellId c1, S2CellId c2, double lat1, double lng1, double lat2, double lng2, std::vector<S2CellId>& out);
S2CellId point_to_cell(double lat, double lng);

enum V { F32 = 0, E5 = 1, E7V = 2, SF = 3, NV = 4 };
static const char* VN[NV] = {"f32", "e5", "e7", "f32(e5)"};

static inline void coord(V v, const NodeCoord& f, const E7& e, double& la, double& lg) {
    switch (v) {
        case F32: la = f.lat; lg = f.lng; break;
        case E5: la = e5_of(e.lat) / 1e5; lg = e5_of(e.lng) / 1e5; break;
        case E7V: la = e.lat / 1e7; lg = e.lng / 1e7; break;
        default: la = f32_of_e5(e5_of(e.lat)); lg = f32_of_e5(e5_of(e.lng)); break;
    }
}

struct Scratch {
    std::vector<S2CellId> nc, ec;
    std::vector<double> la, lg;
};
// The builder's emit_polyline_cells: the street-level cells the polyline's edges cross, sorted, unique.
static void poly_cells(Scratch& s, size_t count, std::vector<uint64_t>& cells) {
    cells.clear();
    s.nc.clear();
    if (count >= 2)
        for (size_t j = 0; j < count; j++) s.nc.push_back(point_to_cell(s.la[j], s.lg[j]));
    for (size_t j = 0; j + 1 < count; j++) {
        cover_edge(s.nc[j], s.nc[j + 1], s.la[j], s.lg[j], s.la[j + 1], s.lg[j + 1], s.ec);
        for (const auto& c : s.ec) cells.push_back(c.id());
    }
    std::sort(cells.begin(), cells.end());
    cells.erase(std::unique(cells.begin(), cells.end()), cells.end());
}

static void way_cells(const NodeCoord* f, const E7* e, size_t c, V v, Scratch& s, std::vector<uint64_t>& out) {
    s.la.resize(c);
    s.lg.resize(c);
    for (size_t k = 0; k < c; k++) coord(v, f[k], e[k], s.la[k], s.lg[k]);
    poly_cells(s, c, out);
}

static uint64_t only_in(const std::vector<uint64_t>& a, const std::vector<uint64_t>& b) {
    uint64_t n = 0;
    size_t j = 0;
    for (uint64_t x : a) {
        while (j < b.size() && b[j] < x) j++;
        if (j == b.size() || b[j] != x) n++;
    }
    return n;
}

static const int NP = 5;
static const int PA[NP] = {F32, F32, E5, F32, F32}, PB[NP] = {E5, E7V, E7V, SF, E7V};

static void stats(const std::string& src, const std::string& dumps) {
    for (int fam = 0; fam < 2; fam++) {
        const bool street = fam == 0;
        Ways W;
        W.open(src + (street ? "/street_ways.bin" : "/interp_ways.bin"), street);
        MF F = map_ro(src + (street ? "/street_nodes.bin" : "/interp_nodes.bin"));
        MF E = map_ro(dumps + (street ? "/street_nodes.e7" : "/interp_nodes.e7"));
        const NodeCoord* f = F.as<NodeCoord>();
        const E7* e = E.as<E7>();
        uint64_t total[NV] = {0}, wd[NP] = {0}, oa[NP] = {0}, ob[NP] = {0}, uni = 0;
#pragma omp parallel
        {
            Scratch s;
            std::vector<uint64_t> cs[NV], u;
            uint64_t lt[NV] = {0}, lwd[NP] = {0}, loa[NP] = {0}, lob[NP] = {0}, lu = 0;
#pragma omp for schedule(dynamic, 1024)
            for (size_t j = 0; j < W.n; j++) {
                const size_t c = W.cnt(j), o = W.off(j);
                for (int v = 0; v < NV; v++) {
                    way_cells(f + o, e + o, c, V(v), s, cs[v]);
                    lt[v] += cs[v].size();
                }
                for (int p = 0; p < NP; p++) {
                    const auto &a = cs[PA[p]], &b = cs[PB[p]];
                    if (a == b) continue;
                    lwd[p]++;
                    loa[p] += only_in(a, b);
                    lob[p] += only_in(b, a);
                }
                u.clear();
                std::set_union(cs[E5].begin(), cs[E5].end(), cs[E7V].begin(), cs[E7V].end(), std::back_inserter(u));
                lu += u.size();
            }
#pragma omp critical
            {
                for (int v = 0; v < NV; v++) total[v] += lt[v];
                for (int p = 0; p < NP; p++) { wd[p] += lwd[p]; oa[p] += loa[p]; ob[p] += lob[p]; }
                uni += lu;
            }
        }
        const char* nm = street ? "street" : "interp";
        printf("%s ways %zu; (cell, way) pairs:", nm, W.n);
        for (int v = 0; v < NV; v++) printf(" %s %llu", VN[v], (unsigned long long)total[v]);
        printf("; union(e5, e7) %llu (+%llu over e5)\n", (unsigned long long)uni, (unsigned long long)(uni - total[E5]));
        for (int p = 0; p < NP; p++)
            printf("  %s vs %s: ways with a different cell set %llu (%.4f%%), pairs only in %s %llu, only in %s %llu\n",
                   VN[PA[p]], VN[PB[p]], (unsigned long long)wd[p], 100.0 * double(wd[p]) / double(W.n), VN[PA[p]],
                   (unsigned long long)oa[p], VN[PB[p]], (unsigned long long)ob[p]);
    }
    MF P = map_ro(src + "/addr_points.bin"), E = map_ro(dumps + "/addr_points.e7"), K = map_ro(dumps + "/addr_points.kind");
    const size_t n = P.count<AddrPoint>();
    const AddrPoint* p = P.as<AddrPoint>();
    const E7* e = E.as<E7>();
    // index 4 = today's rule
    static const int NA = 5;
    const char* an[NA] = {"f32", "e5", "e7", "f32(e5)", "today(index)"};
    const int QA[] = {4, 4, 1, 0, 4, 4}, QB[] = {0, 2, 2, 3, 3, 1};
    const int NQ = 6;
    uint64_t diff[3][NQ] = {{0}}, kc[3] = {0};
#pragma omp parallel
    {
        uint64_t ld[3][NQ] = {{0}}, lk[3] = {0};
#pragma omp for schedule(static)
        for (size_t i = 0; i < n; i++) {
            const int k = K.p[i] > 2 ? 2 : K.p[i];
            lk[k]++;
            uint64_t c[NA];
            const NodeCoord f{p[i].lat, p[i].lng};
            for (int v = 0; v < NV; v++) {
                double la, lg;
                coord(V(v), f, e[i], la, lg);
                c[v] = point_to_cell(la, lg).id();
            }
            c[4] = k == 1 ? c[F32] : c[E7V];
            for (int q = 0; q < NQ; q++) ld[k][q] += c[QA[q]] != c[QB[q]];
        }
#pragma omp critical
        for (int k = 0; k < 3; k++) {
            kc[k] += lk[k];
            for (int q = 0; q < NQ; q++) diff[k][q] += ld[k][q];
        }
    }
    const char* kn[3] = {"OSM nodes", "building centroids", "POI-relation centroids"};
    for (int k = 0; k < 3; k++) {
        printf("addr %s (%llu): points whose cell differs:", kn[k], (unsigned long long)kc[k]);
        for (int q = 0; q < NQ; q++)
            printf(" [%s vs %s] %llu (%.4f%%)", an[QA[q]], an[QB[q]], (unsigned long long)diff[k][q],
                   100.0 * double(diff[k][q]) / double(kc[k]));
        printf("\n");
    }
}

// ---------------------------------------------------------------- reindex
struct Pair { uint64_t cell; uint32_t id; };
static bool pless(const Pair& a, const Pair& b) { return a.cell != b.cell ? a.cell < b.cell : a.id < b.id; }

static void write_index(const std::string& out, std::vector<Pair>* t[3]) {
    const char* names[3] = {"street_entries.bin", "addr_entries.bin", "interp_entries.bin"};
    std::vector<uint8_t> ent[3];
    std::vector<uint8_t> rows;
    size_t at[3] = {0, 0, 0};
    for (;;) {
        bool any = false;
        uint64_t cell = 0;
        for (int k = 0; k < 3; k++)
            if (at[k] < t[k]->size() && (!any || (*t[k])[at[k]].cell < cell)) { cell = (*t[k])[at[k]].cell; any = true; }
        if (!any) break;
        uint32_t off[3] = {0xFFFFFFFFu, 0xFFFFFFFFu, 0xFFFFFFFFu};
        for (int k = 0; k < 3; k++) {
            auto& v = *t[k];
            if (at[k] >= v.size() || v[at[k]].cell != cell) continue;
            if (ent[k].size() > 0xFFFFFFFFull) die("entries offset overflow");
            off[k] = uint32_t(ent[k].size());
            size_t r = at[k];
            while (r < v.size() && v[r].cell == cell) r++;
            if (r - at[k] > 65535) die("cell list too long");
            const uint16_t c = uint16_t(r - at[k]);
            ent[k].insert(ent[k].end(), (const uint8_t*)&c, (const uint8_t*)&c + 2);
            for (; at[k] < r; at[k]++) {
                const uint32_t id = v[at[k]].id;
                ent[k].insert(ent[k].end(), (const uint8_t*)&id, (const uint8_t*)&id + 4);
            }
        }
        rows.insert(rows.end(), (const uint8_t*)&cell, (const uint8_t*)&cell + 8);
        rows.insert(rows.end(), (const uint8_t*)off, (const uint8_t*)off + 12);
    }
    write_file(out + "/geo_cells.bin", rows.data(), rows.size());
    for (int k = 0; k < 3; k++) write_file(out + "/" + names[k], ent[k].data(), ent[k].size());
    printf("index: %zu rows; pairs street %zu addr %zu interp %zu\n", rows.size() / 20, t[0]->size(), t[1]->size(),
           t[2]->size());
}

static void reindex(const std::string& src, const std::string& dumps, const std::string& out, bool today) {
    std::vector<Pair> tabs[3];
    for (int fam = 0; fam < 2; fam++) {
        const bool street = fam == 0;
        Ways W;
        W.open(src + (street ? "/street_ways.bin" : "/interp_ways.bin"), street);
        MF F = map_ro(src + (street ? "/street_nodes.bin" : "/interp_nodes.bin"));
        MF E = map_ro(dumps + (street ? "/street_nodes.e7" : "/interp_nodes.e7"));
        const NodeCoord* f = F.as<NodeCoord>();
        const E7* e = E.as<E7>();
        std::vector<std::vector<Pair>> per(omp_get_max_threads());
#pragma omp parallel
        {
            Scratch s;
            std::vector<uint64_t> cs;
            auto& mine = per[omp_get_thread_num()];
#pragma omp for schedule(dynamic, 1024)
            for (size_t j = 0; j < W.n; j++) {
                const size_t c = W.cnt(j), o = W.off(j);
                way_cells(f + o, e + o, c, today ? F32 : SF, s, cs);
                for (uint64_t x : cs) mine.push_back({x, uint32_t(j)});
            }
        }
        auto& t = tabs[street ? 0 : 2];
        for (auto& v : per) { t.insert(t.end(), v.begin(), v.end()); std::vector<Pair>().swap(v); }
        std::sort(t.begin(), t.end(), pless);
    }
    MF P = map_ro(src + "/addr_points.bin"), E = map_ro(dumps + "/addr_points.e7"), K = map_ro(dumps + "/addr_points.kind");
    const size_t n = P.count<AddrPoint>();
    const AddrPoint* p = P.as<AddrPoint>();
    const E7* e = E.as<E7>();
    auto& t = tabs[1];
    t.resize(n);
#pragma omp parallel for schedule(static)
    for (size_t i = 0; i < n; i++) {
        double la, lg;
        if (!today) coord(SF, NodeCoord{p[i].lat, p[i].lng}, e[i], la, lg);
        else if (K.p[i] == 1) { la = p[i].lat; lg = p[i].lng; }
        else { la = e[i].lat / 1e7; lg = e[i].lng / 1e7; }
        t[i] = {point_to_cell(la, lg).id(), uint32_t(i)};
    }
    std::sort(t.begin(), t.end(), pless);
    std::vector<Pair>* tp[3] = {&tabs[0], &tabs[1], &tabs[2]};
    write_index(out, tp);
}

// ---------------------------------------------------------------- cmpidx
static std::vector<Pair> read_pairs(const std::string& dir, int k) {
    const char* names[3] = {"street_entries.bin", "addr_entries.bin", "interp_entries.bin"};
    MF G = map_ro(dir + "/geo_cells.bin"), En = map_ro(dir + "/" + names[k]);
    std::vector<Pair> v;
    for (size_t r = 0; r < G.n / 20; r++) {
        const uint64_t cell = rd64(G.p + 20 * r);
        const uint32_t off = rd32(G.p + 20 * r + 8 + 4 * k);
        if (off == 0xFFFFFFFFu) continue;
        const uint16_t c = rd16(En.p + off);
        for (uint16_t i = 0; i < c; i++) v.push_back({cell, rd32(En.p + off + 2 + 4 * size_t(i))});
    }
    std::sort(v.begin(), v.end(), pless);
    const size_t before = v.size();
    v.erase(std::unique(v.begin(), v.end(), [](const Pair& a, const Pair& b) { return a.cell == b.cell && a.id == b.id; }),
            v.end());
    printf("  %s table %d: %zu pairs (%zu duplicates dropped)\n", dir.c_str(), k, v.size(), before - v.size());
    return v;
}
static void cmpidx(const std::string& a, const std::string& b) {
    for (int k = 0; k < 3; k++) {
        const auto A = read_pairs(a, k), B = read_pairs(b, k);
        size_t i = 0, j = 0, oa = 0, ob = 0;
        while (i < A.size() || j < B.size()) {
            if (j == B.size() || (i < A.size() && pless(A[i], B[j]))) { oa++; i++; }
            else if (i == A.size() || pless(B[j], A[i])) { ob++; j++; }
            else { i++; j++; }
        }
        printf("table %d: unique pairs only in A %zu, only in B %zu\n", k, oa, ob);
    }
}

int main(int argc, char** argv) {
    if (argc < 2) die("usage: see source");
    const std::string cmd = argv[1];
    if (cmd == "stats" && argc == 4) stats(argv[2], argv[3]);
    else if (cmd == "reindex" && argc == 6) reindex(argv[2], argv[3], argv[4], std::string(argv[5]) == "today");
    else if (cmd == "cmpidx" && argc == 4) cmpidx(argv[2], argv[3]);
    else die("bad arguments");
    return 0;
}
