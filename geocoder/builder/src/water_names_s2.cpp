// The naming join (water_names.h) over the river and canal areas' exact
// rings: their L10 covering, the admin index's (admin_polygon_cells, with
// interior flags), then each candidate segment's midpoint in it.
#include "water_names_s2.h"

#include <algorithm>
#include <atomic>
#include <iostream>
#include <memory>

#include <s2/s2cell_id.h>
#include <s2/s2latlng.h>

#include "admin_cover.h"
#include "exact_pip.h"
#include "parallel.h"
#include "s2_helpers.h"

namespace water {

namespace {

uint64_t cell_of(PackedLocation p) {
    return S2CellId(S2LatLng::FromE7(p.lat_e7, p.lon_e7)).parent(kAdminCellLevel).id();
}

// A segment whose midpoint lies in a covered cell, and that cell.
#pragma pack(push, 4)
struct Midpoint {
    uint64_t cell;
    uint32_t line;
    uint32_t index;  // its first vertex in the line
};
#pragma pack(pop)

// A segment to test against a polygon of a boundary cell.
#pragma pack(push, 4)
struct Probe {
    uint32_t polygon;
    uint32_t line;
    uint32_t index;
};
#pragma pack(pop)

// Adds a credit, onto the last one where that is of the same object, key
// and display (a line's segments in a row).
void credit(std::vector<Credit>& out, uint32_t object, const NamingLine& line, uint64_t mm) {
    if (!out.empty() && out.back().object == object && out.back().key == line.key && out.back().display == line.display) {
        out.back().mm += mm;
        return;
    }
    out.push_back({object, line.key, line.display, mm});
}

}  // namespace

std::vector<Credit> credit_naming_lines(const AdminExactRings& rings, const std::vector<uint32_t>& polygons,
                                        const std::vector<uint32_t>& object_of, const NamingLines& lines,
                                        NamingCounts& counts, unsigned threads) {
    if (threads == 0) threads = parallel_threads();
    const PackedLocation* v = rings.vertices.data();

    // The polygons' covering: (cell, polygon | INTERIOR_FLAG), by cell.
    std::vector<std::vector<CellItemPair>> found(threads);
    const std::vector<uint32_t> largest_first = costliest_first(polygons.size(), [&](size_t k) {
        return static_cast<float>(rings.occurrences(polygons[k]));
    }, threads);
    parallel_for_dynamic(largest_first.size(), 1, [&](size_t begin, size_t end, unsigned worker) {
        std::vector<uint64_t> cells, boundary;
        std::vector<CellList> lists;
        std::vector<std::vector<uint64_t>> per_ring;
        for (size_t k = begin; k < end; k++) {
            const uint32_t p = polygons[largest_first[k]];
            const uint32_t n = rings.polygons[p].ring_count;
            const RingSpan* spans = rings.rings_of(p);
            per_ring.assign(n, {});
            lists.clear();
            for (uint32_t r = 0; r < n; r++) {
                admin_arc_cells(v + spans[r].begin, spans[r].count, kAdminCellLevel, ADMIN_COVER_REACH_RAD, per_ring[r]);
                lists.push_back({per_ring[r].data(), per_ring[r].data() + per_ring[r].size()});
            }
            merge_cell_lists(lists, boundary);
            for (const auto& [cell, interior] : admin_polygon_cells(boundary, v, spans, n, kAdminCellLevel))
                found[worker].push_back({cell, interior ? p | INTERIOR_FLAG : p});
        }
    }, threads);
    std::vector<CellItemPair> cover;
    for (auto& f : found) {
        cover.insert(cover.end(), f.begin(), f.end());
        std::vector<CellItemPair>().swap(f);
    }
    parallel_sort(cover.begin(), cover.end(), cell_item_less, threads);
    counts.cover_entries = cover.size();
    std::vector<uint64_t> covered(cover.size());
    for (size_t i = 0; i < cover.size(); i++) covered[i] = cover[i].cell_id;
    covered.erase(std::unique(covered.begin(), covered.end()), covered.end());

    // The segments whose midpoints lie in a covered cell, by cell.
    std::vector<Midpoint> mids = parallel_collect<Midpoint>(lines.lines.size(), [&](size_t l, std::vector<Midpoint>& out) {
        const NamingLine& line = lines.lines[l];
        const PackedLocation* lv = lines.vertices.data() + line.first;
        for (uint32_t i = 0; i + 1 < line.count; i++) {
            const uint64_t cell = cell_of(segment_midpoint(lv[i], lv[i + 1]));
            if (std::binary_search(covered.begin(), covered.end(), cell))
                out.push_back({cell, static_cast<uint32_t>(l), i});
        }
    }, threads);
    std::vector<uint64_t>().swap(covered);
    parallel_sort(mids.begin(), mids.end(), [](const Midpoint& a, const Midpoint& b) {
        if (a.cell != b.cell) return a.cell < b.cell;
        if (a.line != b.line) return a.line < b.line;
        return a.index < b.index;
    }, threads);
    counts.segments_in_cover = mids.size();

    // Each cell's segments against its polygons: an interior entry holds
    // them, the others are probed.
    std::vector<size_t> starts = parallel_filter<size_t>(mids.size(), [&](size_t i) {
        return i == 0 || mids[i].cell != mids[i - 1].cell;
    }, threads);
    std::vector<std::vector<Credit>> credits(threads);
    std::vector<std::vector<Probe>> probes(threads);
    parallel_for_dynamic(starts.size(), 64, [&](size_t begin, size_t end, unsigned worker) {
        for (size_t c = begin; c < end; c++) {
            const size_t from = starts[c], to = c + 1 < starts.size() ? starts[c + 1] : mids.size();
            const uint64_t cell = mids[from].cell;
            auto it = std::lower_bound(cover.begin(), cover.end(), cell,
                                       [](const CellItemPair& e, uint64_t id) { return e.cell_id < id; });
            for (; it != cover.end() && it->cell_id == cell; ++it) {
                const uint32_t p = it->item_id & ID_MASK;
                const bool interior = (it->item_id & INTERIOR_FLAG) != 0;
                for (size_t i = from; i < to; i++) {
                    const NamingLine& line = lines.lines[mids[i].line];
                    if (interior) {
                        const PackedLocation* lv = lines.vertices.data() + line.first + mids[i].index;
                        credit(credits[worker], object_of[p], line, segment_mm(lv[0], lv[1]));
                    } else {
                        probes[worker].push_back({p, mids[i].line, mids[i].index});
                    }
                }
            }
        }
    }, threads);
    std::vector<Midpoint>().swap(mids);
    std::vector<CellItemPair>().swap(cover);
    std::vector<Probe> all;
    for (auto& p : probes) {
        all.insert(all.end(), p.begin(), p.end());
        std::vector<Probe>().swap(p);
    }
    parallel_sort(all.begin(), all.end(), [](const Probe& a, const Probe& b) {
        if (a.polygon != b.polygon) return a.polygon < b.polygon;
        if (a.line != b.line) return a.line < b.line;
        return a.index < b.index;
    }, threads);
    counts.probes = all.size();

    // The probes polygon by polygon, the costliest first, each big ring's
    // edges indexed once (ExactRingEdgeIndex).
    std::vector<size_t> runs = parallel_filter<size_t>(all.size(), [&](size_t i) {
        return i == 0 || all[i].polygon != all[i - 1].polygon;
    }, threads);
    const std::vector<uint32_t> run_order = costliest_first(runs.size(), [&](size_t k) {
        const size_t to = k + 1 < runs.size() ? runs[k + 1] : all.size();
        return static_cast<float>(to - runs[k]) * 0.001f + static_cast<float>(rings.occurrences(all[runs[k]].polygon)) * 0.01f;
    }, threads);
    std::atomic<size_t> held{0};
    parallel_for_dynamic(run_order.size(), 1, [&](size_t begin, size_t end, unsigned worker) {
        for (size_t k = begin; k < end; k++) {
            const size_t r = run_order[k];
            const size_t from = runs[r], to = r + 1 < runs.size() ? runs[r + 1] : all.size();
            const uint32_t p = all[from].polygon;
            const uint32_t n = rings.polygons[p].ring_count;
            const RingSpan* spans = rings.rings_of(p);
            constexpr uint32_t kIndexedRing = 256;
            std::vector<std::unique_ptr<ExactRingEdgeIndex>> index(n);
            std::vector<ExactRingBox> boxes(n);
            for (uint32_t q = 0; q < n; q++) {
                boxes[q] = exact_ring_box(v + spans[q].begin, spans[q].count);
                if (spans[q].count > kIndexedRing && to - from > 8)
                    index[q] = std::make_unique<ExactRingEdgeIndex>(v + spans[q].begin, spans[q].count);
            }
            for (size_t i = from; i < to; i++) {
                const NamingLine& line = lines.lines[all[i].line];
                const PackedLocation* lv = lines.vertices.data() + line.first + all[i].index;
                const PackedLocation mid = segment_midpoint(lv[0], lv[1]);
                bool inside = false;
                for (uint32_t q = 0; q < n; q++) {
                    if (exact_ring_box_excludes(boxes[q], mid)) continue;
                    const bool in = index[q] ? index[q]->contains(mid)
                                             : exact_ring_contains(v + spans[q].begin, spans[q].count, mid);
                    inside = inside != in;
                }
                if (!inside) continue;
                credit(credits[worker], object_of[p], line, segment_mm(lv[0], lv[1]));
                held.fetch_add(1, std::memory_order_relaxed);
            }
        }
    }, threads);
    counts.probes_inside = held.load();
    std::vector<Credit> out;
    for (auto& c : credits) {
        out.insert(out.end(), c.begin(), c.end());
        std::vector<Credit>().swap(c);
    }
    counts.credits = out.size();
    return out;
}

}  // namespace water
