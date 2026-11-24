// The naming join (water_names.h) over the river and canal areas' exact
// rings: their covering at the admin cell level, the admin index's
// (admin_polygon_cells, with interior flags), then each candidate segment
// against the polygons of the cells it passes near.
#include "water_names_s2.h"

#include <algorithm>
#include <atomic>
#include <iostream>
#include <memory>

#include "admin_cover.h"
#include "exact_pip.h"
#include "parallel.h"
#include "ring_edge_buckets.h"
#include "s2_helpers.h"

namespace water {

namespace {

// A segment near a covered cell: that cell, the segment (its line and its
// first vertex in the line), and how many cells it passes near.
#pragma pack(push, 4)
struct Near {
    uint64_t cell;
    uint32_t line;
    uint32_t index;
    uint32_t cells;
};
#pragma pack(pop)

// A polygon a cell near a segment lists: the polygon, the segment, and the
// segment's cell count, kInterior set for a cell all inside the polygon.
struct Pair {
    uint32_t polygon;
    uint32_t line;
    uint32_t index;
    uint32_t cells;
};
constexpr uint32_t kInterior = 1u << 31;

bool pair_less(const Pair& a, const Pair& b) {
    if (a.polygon != b.polygon) return a.polygon < b.polygon;
    if (a.line != b.line) return a.line < b.line;
    return a.index < b.index;
}
bool same_segment(const Pair& a, const Pair& b) {
    return a.polygon == b.polygon && a.line == b.line && a.index == b.index;
}

// Whether the box of segment a-b misses `box`.
bool box_misses(const ExactRingBox& box, PackedLocation a, PackedLocation b) {
    return std::max(a.lat_e7, b.lat_e7) < box.lat_lo || std::min(a.lat_e7, b.lat_e7) > box.lat_hi ||
           std::max(a.lon_e7, b.lon_e7) < box.lng_lo || std::min(a.lon_e7, b.lon_e7) > box.lng_hi;
}

// Adds a credit, onto the last one where that is of the same object, key,
// display and wikidata item (a line's segments in a row); none of no
// length, which would lend its line's wikidata item for nothing.
void credit(std::vector<Credit>& out, uint32_t object, const NamingLine& line, uint64_t mm) {
    if (mm == 0) return;
    if (!out.empty() && out.back().object == object && out.back().key == line.key && out.back().display == line.display &&
        out.back().qid == line.qid) {
        out.back().mm += mm;
        return;
    }
    out.push_back({object, line.key, line.display, mm, line.qid});
}

}  // namespace

std::vector<Credit> credit_naming_lines(const AdminExactRings& rings, const std::vector<uint32_t>& polygons,
                                        const std::vector<uint32_t>& object_of, const NamingLines& lines,
                                        NamingCounts& counts, unsigned threads) {
    if (polygons.empty() || lines.lines.empty()) return {};
    if (threads == 0) threads = parallel_threads();
    const PackedLocation* v = rings.vertices.data();

    // The polygons' covering: (cell, polygon | INTERIOR_FLAG), by cell; and
    // the box of each one's rings, which a segment that misses it can't
    // cross or start in.
    std::vector<std::vector<CellItemPair>> found(threads);
    std::vector<ExactRingBox> boxes(rings.polygons.size());
    const std::vector<uint32_t> largest_first = costliest_first(polygons.size(), [&](size_t k) {
        return static_cast<float>(rings.occurrences(polygons[k]));
    }, threads);
    parallel_for_dynamic(largest_first.size(), 1, [&](size_t begin, size_t end, unsigned worker) {
        std::vector<uint64_t> boundary;
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
            ExactRingBox box = exact_ring_box(v + spans[0].begin, spans[0].count);
            for (uint32_t r = 1; r < n; r++) {
                const ExactRingBox hole = exact_ring_box(v + spans[r].begin, spans[r].count);
                box = {std::min(box.lat_lo, hole.lat_lo), std::max(box.lat_hi, hole.lat_hi),
                       std::min(box.lng_lo, hole.lng_lo), std::max(box.lng_hi, hole.lng_hi)};
            }
            boxes[p] = box;
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

    // The lines with a segment across the antimeridian or along it, each
    // stood in for by a copy of it cut there (cut_at_antimeridian), past the
    // others: a piece either side of it is a segment of its own, and a reach
    // along it runs at 180 east.
    const size_t own = lines.lines.size();
    const std::vector<uint32_t> wrapping = parallel_filter<uint32_t>(own, [&](size_t l) {
        const NamingLine& line = lines.lines[l];
        const PackedLocation* lv = lines.vertices.data() + line.first;
        for (uint32_t i = 0; i + 1 < line.count; i++)
            if (segment_wraps(lv[i], lv[i + 1]) || segment_on_antimeridian(lv[i], lv[i + 1])) return true;
        return false;
    }, threads);
    std::vector<NamingLine> cut;
    std::vector<PackedLocation> cut_vertices;
    for (uint32_t l : wrapping) {
        NamingLine line = lines.lines[l];
        const size_t first = cut_vertices.size();
        cut_at_antimeridian(lines.vertices.data() + line.first, line.count, cut_vertices);
        line.first = first;
        line.count = static_cast<uint32_t>(cut_vertices.size() - first);
        cut.push_back(line);
    }
    auto line_of = [&](uint32_t l) -> const NamingLine& { return l < own ? lines.lines[l] : cut[l - own]; };
    auto vertices_of = [&](uint32_t l) {
        return (l < own ? lines.vertices.data() : cut_vertices.data()) + line_of(l).first;
    };

    // Each segment's covered cells among those it passes near, as the
    // covering finds a ring's (admin_arc_cells), by cell. A polygon that
    // meets the segment is in one of them.
    std::vector<Near> near = parallel_collect<Near>(own + cut.size(), [&](size_t l, std::vector<Near>& out) {
        thread_local std::vector<uint64_t> cells;
        if (l < own && std::binary_search(wrapping.begin(), wrapping.end(), static_cast<uint32_t>(l))) return;
        const NamingLine& line = line_of(static_cast<uint32_t>(l));
        const PackedLocation* lv = vertices_of(static_cast<uint32_t>(l));
        for (uint32_t i = 0; i + 1 < line.count; i++) {
            if (lv[i] == lv[i + 1] || segment_wraps(lv[i], lv[i + 1])) continue;
            cells.clear();
            admin_arc_cells(lv + i, 2, kAdminCellLevel, ADMIN_COVER_REACH_RAD, cells);
            for (uint64_t cell : cells)
                if (std::binary_search(covered.begin(), covered.end(), cell))
                    out.push_back({cell, static_cast<uint32_t>(l), i, static_cast<uint32_t>(cells.size())});
        }
    }, threads);
    std::vector<uint64_t>().swap(covered);
    parallel_sort(near.begin(), near.end(), [](const Near& a, const Near& b) {
        if (a.cell != b.cell) return a.cell < b.cell;
        if (a.line != b.line) return a.line < b.line;
        return a.index < b.index;
    }, threads);
    counts.segments_in_cover = near.size();

    // Each cell's segments against its polygons whose box the segment's
    // meets, then each segment and polygon once: all inside where every cell
    // the segment passes near is all inside the polygon, else clipped to its
    // rings.
    std::vector<size_t> starts = parallel_filter<size_t>(near.size(), [&](size_t i) {
        return i == 0 || near[i].cell != near[i - 1].cell;
    }, threads);
    std::vector<std::vector<Pair>> per_worker(threads);
    parallel_for_dynamic(starts.size(), 64, [&](size_t begin, size_t end, unsigned worker) {
        for (size_t c = begin; c < end; c++) {
            const size_t from = starts[c], to = c + 1 < starts.size() ? starts[c + 1] : near.size();
            const uint64_t cell = near[from].cell;
            auto it = std::lower_bound(cover.begin(), cover.end(), cell,
                                       [](const CellItemPair& e, uint64_t id) { return e.cell_id < id; });
            for (; it != cover.end() && it->cell_id == cell; ++it) {
                const uint32_t p = it->item_id & ID_MASK;
                const uint32_t interior = (it->item_id & INTERIOR_FLAG) ? kInterior : 0;
                const ExactRingBox& box = boxes[p];
                for (size_t i = from; i < to; i++) {
                    const PackedLocation* lv = vertices_of(near[i].line) + near[i].index;
                    if (box_misses(box, lv[0], lv[1])) continue;
                    per_worker[worker].push_back({p, near[i].line, near[i].index, near[i].cells | interior});
                }
            }
        }
    }, threads);
    std::vector<Near>().swap(near);
    std::vector<CellItemPair>().swap(cover);
    std::vector<size_t>().swap(starts);
    std::vector<Pair> pairs;
    for (auto& w : per_worker) {
        pairs.insert(pairs.end(), w.begin(), w.end());
        std::vector<Pair>().swap(w);
    }
    parallel_sort(pairs.begin(), pairs.end(), pair_less, threads);
    std::vector<size_t> groups = parallel_filter<size_t>(pairs.size(), [&](size_t i) {
        return i == 0 || !same_segment(pairs[i], pairs[i - 1]);
    }, threads);
    // Whether group g's polygon holds its segment whole: every cell it
    // passes near an interior one of the polygon's.
    auto whole = [&](size_t g) {
        const size_t from = groups[g], to = g + 1 < groups.size() ? groups[g + 1] : pairs.size();
        size_t interior = 0;
        for (size_t i = from; i < to; i++) interior += (pairs[i].cells & kInterior) != 0;
        return interior == (pairs[from].cells & ~kInterior);
    };
    std::vector<std::vector<Credit>> credits(threads);
    parallel_for_dynamic(groups.size(), 256, [&](size_t begin, size_t end, unsigned worker) {
        for (size_t g = begin; g < end; g++) {
            if (!whole(g)) continue;
            const Pair& pair = pairs[groups[g]];
            const NamingLine& line = line_of(pair.line);
            const PackedLocation* lv = vertices_of(pair.line) + pair.index;
            credit(credits[worker], object_of[pair.polygon], line, segment_mm(lv[0], lv[1]));
        }
    }, threads);
    // The pairs left to clip, a segment and polygon each, by polygon.
    std::vector<Pair> clips = parallel_collect<Pair>(groups.size(), [&](size_t g, std::vector<Pair>& out) {
        if (!whole(g)) out.push_back(pairs[groups[g]]);
    }, threads);
    std::vector<Pair>().swap(pairs);
    std::vector<size_t>().swap(groups);
    counts.probes = clips.size();

    // The clips polygon by polygon, the costliest first: a segment's
    // crossings with its rings' edges near it, and whether its start is in
    // them, each big ring's edges bucketed by longitude once.
    std::vector<size_t> runs = parallel_filter<size_t>(clips.size(), [&](size_t i) {
        return i == 0 || clips[i].polygon != clips[i - 1].polygon;
    }, threads);
    const std::vector<uint32_t> run_order = costliest_first(runs.size(), [&](size_t k) {
        const size_t to = k + 1 < runs.size() ? runs[k + 1] : clips.size();
        return static_cast<float>(to - runs[k]) * 0.001f +
               static_cast<float>(rings.occurrences(clips[runs[k]].polygon)) * 0.01f;
    }, threads);
    std::atomic<size_t> held{0};
    parallel_for_dynamic(run_order.size(), 1, [&](size_t begin, size_t end, unsigned worker) {
        for (size_t k = begin; k < end; k++) {
            const size_t r = run_order[k];
            const size_t from = runs[r], to = r + 1 < runs.size() ? runs[r + 1] : clips.size();
            const uint32_t p = clips[from].polygon;
            const uint32_t n = rings.polygons[p].ring_count;
            const RingSpan* spans = rings.rings_of(p);
            constexpr uint32_t kIndexedRing = 256;
            std::vector<std::unique_ptr<RingEdgeBuckets>> index(n);
            std::vector<ExactRingBox> boxes(n);
            for (uint32_t q = 0; q < n; q++) {
                const PackedLocation* rv = v + spans[q].begin;
                boxes[q] = exact_ring_box(rv, spans[q].count);
                if (spans[q].count > kIndexedRing && to - from > 8)
                    index[q] = std::make_unique<RingEdgeBuckets>(spans[q].count,
                        std::min<uint32_t>(spans[q].count / 16, 1u << 20), [rv](uint32_t i) { return rv[i].lon_e7; });
            }
            for (size_t i = from; i < to; i++) {
                const NamingLine& line = line_of(clips[i].line);
                const PackedLocation* lv = vertices_of(clips[i].line) + clips[i].index;
                SegmentClip clip(lv[0], lv[1]);
                const int32_t start = clip.start().lon_e7;
                const int32_t lng_lo = std::min(lv[0].lon_e7, lv[1].lon_e7);
                const int32_t lng_hi = std::max(lv[0].lon_e7, lv[1].lon_e7);
                const int32_t lat_lo = std::min(lv[0].lat_e7, lv[1].lat_e7);
                const int32_t lat_hi = std::max(lv[0].lat_e7, lv[1].lat_e7);
                bool inside = false;
                for (uint32_t q = 0; q < n; q++) {
                    const ExactRingBox& box = boxes[q];
                    // A ring west, south or east of the segment neither
                    // crosses it nor its start's ray; one north of it holds
                    // the ray alone.
                    if (box.lng_hi < lng_lo || box.lng_lo > lng_hi || box.lat_hi < lat_lo) continue;
                    const PackedLocation* rv = v + spans[q].begin;
                    const uint32_t count = spans[q].count;
                    bool odd = false;
                    if (index[q]) {
                        odd = index[q]->odd_crossings(start, [&](uint32_t a, uint32_t b) {
                            return clip.start_crossed_by(rv[b], rv[a]);
                        });
                        if (box.lat_lo <= lat_hi)
                            index[q]->edges_meeting(lng_lo, lng_hi, [rv](uint32_t e) { return rv[e].lon_e7; },
                                                    [&](uint32_t a, uint32_t b) { clip.add_crossing(rv[b], rv[a]); });
                    } else {
                        for (uint32_t a = 1; a < count; a++) {
                            odd ^= clip.start_crossed_by(rv[a - 1], rv[a]);
                            clip.add_crossing(rv[a - 1], rv[a]);
                        }
                    }
                    inside ^= odd;
                }
                const uint64_t mm = share_mm(lv[0], lv[1], clip.share(inside));
                if (mm == 0) continue;
                credit(credits[worker], object_of[p], line, mm);
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
