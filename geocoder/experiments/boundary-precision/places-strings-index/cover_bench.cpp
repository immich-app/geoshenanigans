// Rebuild-at-load estimate for admin_cells/admin_entries: decode every admin-minimal
// polygon from the SHIPPED q2.5 admin_vertices.bin (as the server would) and cover it
// with the builder's own algorithm (copy of cover_polygon, builder/src/s2_helpers.cpp:127,
// kAdminCellLevel = 10, MAX_S2_CELLS_PER_POLY = 200). Single thread. Reports time,
// peak pair count, and agreement with the shipped admin index.
//
// usage: cover_bench <admin-minimal dir> [max_polys]
#include <s2/s2cell_id.h>
#include <s2/s2latlng.h>
#include <s2/s2loop.h>
#include <s2/s2polygon.h>
#include <s2/s2region_coverer.h>
#include <s2/s2cell_union.h>
#include <s2/s2error.h>
#include <s2/s2latlng_rect.h>
#include <sys/resource.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

static const int kAdminCellLevel = 10;
static const int MAX_S2_CELLS_PER_POLY = 200;
static const uint32_t INTERIOR_FLAG = 0x80000000u, ID_MASK = 0x7FFFFFFFu, NO_DATA = 0xFFFFFFFFu;

#pragma pack(push, 1)
struct AdminPolygon { uint32_t vertex_offset, vertex_count, name_id; uint8_t admin_level, pto, p2, p3; float area; uint16_t cc, p4; };
#pragma pack(pop)

static std::vector<char> slurp(const std::string& p) {
    std::ifstream f(p, std::ios::binary);
    return std::vector<char>((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
}

static std::vector<std::pair<S2CellId, bool>> cover_polygon(const std::vector<std::pair<double, double>>& vertices) {
    std::vector<S2Point> points;
    points.reserve(vertices.size());
    for (const auto& [lat, lng] : vertices) {
        S2Point p = S2LatLng::FromDegrees(lat, lng).ToPoint();
        if (!points.empty() && points.back() == p) continue;
        points.push_back(p);
    }
    if (points.size() > 1 && points.front() == points.back()) points.pop_back();
    if (points.size() < 3) return {};
    S2Error error;
    auto loop = std::make_unique<S2Loop>(points, S2Debug::DISABLE);
    loop->Normalize();
    S2Polygon polygon(std::move(loop), S2Debug::DISABLE);
    S2RegionCoverer::Options options;
    if (polygon.FindValidationError(&error)) {
        if (!getenv("RECT_FALLBACK")) return {};
        // Fallback for loops q2.5 quantisation made invalid: every L10 cell of a 0.03 deg
        // lat/lng sample grid over the bounding box, plus its 8 neighbours (a superset;
        // never marked interior).
        double la0 = 1e9, la1 = -1e9, lo0 = 1e9, lo1 = -1e9;
        for (const auto& [lat, lng] : vertices) { la0 = std::min(la0, lat); la1 = std::max(la1, lat); lo0 = std::min(lo0, lng); lo1 = std::max(lo1, lng); }
        std::vector<uint64_t> ids;
        for (double la = la0; la <= la1 + 0.03; la += 0.03)
            for (double lo = lo0; lo <= lo1 + 0.03; lo += 0.03) {
                S2CellId c = S2CellId(S2LatLng::FromDegrees(std::min(la, 90.0), std::min(lo, 180.0)).Normalized()).parent(kAdminCellLevel);
                ids.push_back(c.id());
                std::vector<S2CellId> nb;
                c.AppendAllNeighbors(kAdminCellLevel, &nb);
                for (auto& x : nb) ids.push_back(x.id());
            }
        std::sort(ids.begin(), ids.end());
        ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
        std::vector<std::pair<S2CellId, bool>> res;
        for (auto id : ids) res.emplace_back(S2CellId(id), false);
        return res;
    }
    options.set_max_level(kAdminCellLevel);
    options.set_max_cells(MAX_S2_CELLS_PER_POLY);
    S2RegionCoverer coverer(options);
    S2CellUnion covering = coverer.GetCovering(polygon);
    S2CellUnion interior = coverer.GetInteriorCovering(polygon);
    std::unordered_set<uint64_t> interior_set;
    for (const auto& cell : interior.cell_ids()) {
        if (cell.level() <= kAdminCellLevel) {
            auto begin = cell.range_min().parent(kAdminCellLevel);
            auto end = cell.range_max().parent(kAdminCellLevel);
            for (auto c = begin; c != end; c = c.next()) interior_set.insert(c.id());
            interior_set.insert(end.id());
        } else {
            interior_set.insert(cell.parent(kAdminCellLevel).id());
        }
    }
    std::vector<std::pair<S2CellId, bool>> result;
    for (const auto& cell : covering.cell_ids()) {
        if (cell.level() <= kAdminCellLevel) {
            auto begin = cell.range_min().parent(kAdminCellLevel);
            auto end = cell.range_max().parent(kAdminCellLevel);
            for (auto c = begin; c != end; c = c.next()) result.emplace_back(c, interior_set.count(c.id()) > 0);
            result.emplace_back(end, interior_set.count(end.id()) > 0);
        } else {
            auto parent = cell.parent(kAdminCellLevel);
            result.emplace_back(parent, interior_set.count(parent.id()) > 0);
        }
    }
    std::sort(result.begin(), result.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
    auto it = result.begin();
    for (auto curr = result.begin(); curr != result.end();) {
        auto next = curr + 1;
        bool is_interior = curr->second;
        while (next != result.end() && next->first == curr->first) { is_interior = is_interior || next->second; ++next; }
        *it = {curr->first, is_interior};
        ++it;
        curr = next;
    }
    result.erase(it, result.end());
    return result;
}

int main(int argc, char** argv) {
    std::string dir = argv[1];
    size_t max_polys = (argc > 2 && std::stoull(argv[2])) ? std::stoull(argv[2]) : SIZE_MAX;
    auto polys_raw = slurp(dir + "/admin_polygons.bin");
    auto verts = slurp(dir + "/admin_vertices.bin");
    auto cells_raw = slurp(dir + "/admin_cells.bin");
    auto entries = slurp(dir + "/admin_entries.bin");
    size_t npoly = polys_raw.size() / sizeof(AdminPolygon);
    const AdminPolygon* polys = reinterpret_cast<const AdminPolygon*>(polys_raw.data());
    static const double scale[4] = {1e-5, 1e-4, 1e-6, 1e-7};

    std::vector<std::pair<uint64_t, uint32_t>> pairs;  // (cell, id|flag)
    auto t0 = std::chrono::steady_clock::now();
    size_t done = 0, nverts = 0, failed = 0;
    for (size_t i = 0; i < npoly && done < max_polys; i++) {
        const AdminPolygon& p = polys[i];
        if (p.name_id == NO_DATA || p.vertex_count == 0 || p.vertex_offset == NO_DATA) continue;
        const unsigned char* h = reinterpret_cast<const unsigned char*>(verts.data()) + p.vertex_offset;
        uint8_t enc = h[0];
        float bl, bg;
        std::memcpy(&bl, h + 2, 4);
        std::memcpy(&bg, h + 6, 4);
        const unsigned char* v = h + 10;
        std::vector<std::pair<double, double>> pts(p.vertex_count);
        for (uint32_t k = 0; k < p.vertex_count; k++) {
            double dl, dg;
            if (enc == 3) { uint32_t a, b; std::memcpy(&a, v + k * 8, 4); std::memcpy(&b, v + k * 8 + 4, 4); dl = a; dg = b; }
            else if (enc == 4) { float a, b; std::memcpy(&a, v + k * 8, 4); std::memcpy(&b, v + k * 8 + 4, 4); pts[k] = {a, b}; continue; }
            else { uint16_t a, b; std::memcpy(&a, v + k * 4, 2); std::memcpy(&b, v + k * 4 + 2, 2); dl = a; dg = b; }
            // match the server: f32(bmin + d*scale)
            pts[k] = {double(float(double(bl) + dl * scale[enc])), double(float(double(bg) + dg * scale[enc]))};
        }
        nverts += p.vertex_count;
        auto cov = cover_polygon(pts);
        if (cov.empty()) failed++;
        for (auto& [c, in] : cov) pairs.push_back({c.id(), uint32_t(i) | (in ? INTERIOR_FLAG : 0)});
        done++;
    }
    double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    std::sort(pairs.begin(), pairs.end());
    auto t1 = std::chrono::steady_clock::now();
    double sort_secs = std::chrono::duration<double>(t1 - t0).count() - secs;
    struct rusage ru; getrusage(RUSAGE_SELF, &ru);
    std::printf("polygons covered %zu (failed validation %zu), vertices %zu, cover time %.1fs, sort %.1fs, pairs %zu, peak RSS %ld MiB\n",
                done, failed, nverts, secs, sort_secs, pairs.size(), ru.ru_maxrss / 1024);

    if (max_polys != SIZE_MAX) return 0;
    // agreement with shipped index
    std::vector<std::pair<uint64_t, uint32_t>> shipped;
    size_t ncell = cells_raw.size() / 12;
    for (size_t c = 0; c < ncell; c++) {
        uint64_t id; uint32_t off;
        std::memcpy(&id, cells_raw.data() + c * 12, 8);
        std::memcpy(&off, cells_raw.data() + c * 12 + 8, 4);
        uint16_t n; std::memcpy(&n, entries.data() + off, 2);
        for (uint16_t k = 0; k < n; k++) { uint32_t e; std::memcpy(&e, entries.data() + off + 2 + 4 * k, 4); shipped.push_back({id, e}); }
    }
    std::sort(shipped.begin(), shipped.end());
    auto strip = [](std::vector<std::pair<uint64_t, uint32_t>> v) { for (auto& x : v) x.second &= ID_MASK; std::sort(v.begin(), v.end()); return v; };
    auto a = strip(shipped), b = strip(pairs);
    std::vector<std::pair<uint64_t, uint32_t>> only_a, only_b;
    std::set_difference(a.begin(), a.end(), b.begin(), b.end(), std::back_inserter(only_a));
    std::set_difference(b.begin(), b.end(), a.begin(), a.end(), std::back_inserter(only_b));
    std::printf("shipped pairs %zu, rebuilt pairs %zu, only-in-shipped %zu, only-in-rebuilt %zu\n", a.size(), b.size(), only_a.size(), only_b.size());
    // write rebuilt index in the shipped format for a grid_query check
    std::string out = argc > 3 ? argv[3] : "";
    if (!out.empty()) {
        std::ofstream fc(out + "/admin_cells.bin", std::ios::binary), fe(out + "/admin_entries.bin", std::ios::binary);
        uint32_t off = 0;
        for (size_t s = 0; s < pairs.size();) {
            size_t e = s;
            while (e < pairs.size() && pairs[e].first == pairs[s].first) e++;
            std::vector<uint32_t> ids;
            for (size_t k = s; k < e; k++) ids.push_back(pairs[k].second);
            std::sort(ids.begin(), ids.end());
            uint16_t n = uint16_t(ids.size());
            fc.write(reinterpret_cast<const char*>(&pairs[s].first), 8);
            fc.write(reinterpret_cast<const char*>(&off), 4);
            fe.write(reinterpret_cast<const char*>(&n), 2);
            fe.write(reinterpret_cast<const char*>(ids.data()), 4 * n);
            off += 2 + 4 * n;
            s = e;
        }
    }
    return 0;
}
