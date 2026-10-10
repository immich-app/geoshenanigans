// Deterministic ordering steps: each sorts, dedups and renumbers one record
// family (with its parallel arrays, vertex/node buffers and cell pairs) so the
// same input yields the same on-disk byte layout whatever the thread
// scheduling (enables patching). reorder_deterministically (build_index.cpp)
// runs them in order once the string pool is partitioned into tiers.
#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <numeric>
#include <vector>

#include "parallel.h"
#include "parsed_data.h"
#include "types.h"

struct QidSitelinks { uint32_t qid; uint16_t count; };

// Reinterpret a float as a uint32 that sorts in the same order as the float
// (IEEE-754 total ordering): flip all bits for negatives, flip just the sign
// bit for non-negatives. Used as the byte-layout-defining lat/lng tiebreak in
// the deterministic-ordering pass.
static inline uint32_t float_bits(float v) {
    uint32_t bits;
    memcpy(&bits, &v, 4);
    return (bits & 0x80000000) ? ~bits : (bits ^ 0x80000000);
}

// Sorted positions that start a run of duplicates: i == 0 or !same(i - 1, i).
// The serial dedups compared each record with the last one they kept; for an
// equivalence `same` that is the same test as comparing with the predecessor,
// since every record in between duplicates the kept one.
template <class Same>
std::vector<uint32_t> run_starts(size_t n, Same same, unsigned threads) {
    return parallel_filter(n, [&](size_t i) { return i == 0 || !same(i - 1, i); }, threads);
}

inline size_t run_end(const std::vector<uint32_t>& starts, size_t k, size_t n) {
    return k + 1 < starts.size() ? starts[k + 1] : n;
}

// old_to_new for records deduped into runs: every record of run k (sorted
// positions [starts[k], starts[k + 1])) becomes record k.
inline std::vector<uint32_t> renumber_runs(const std::vector<uint32_t>& order,
                                           const std::vector<uint32_t>& starts, unsigned threads) {
    std::vector<uint32_t> old_to_new(order.size());
    parallel_for(starts.size(), [&](size_t b, size_t e, unsigned) {
        for (size_t k = b; k < e; k++)
            for (size_t i = starts[k], end = run_end(starts, k, order.size()); i < end; i++)
                old_to_new[order[i]] = static_cast<uint32_t>(k);
    }, threads);
    return old_to_new;
}

// Per run, the first value (in sorted order) that isn't `none`, else none:
// the serial dedups filled each kept slot from the first duplicate with one.
template <class T>
std::vector<T> first_set_per_run(const std::vector<T>& values, const std::vector<uint32_t>& order,
                                 const std::vector<uint32_t>& starts, T none, unsigned threads) {
    std::vector<T> out(starts.size(), none);
    parallel_for(starts.size(), [&](size_t b, size_t e, unsigned) {
        for (size_t k = b; k < e; k++)
            for (size_t i = starts[k], end = run_end(starts, k, order.size()); i < end && out[k] == none; i++)
                out[k] = values[order[i]];
    }, threads);
    return out;
}

// Renumbers the item ids of (cell_id, item_id) pairs and restores their
// (cell, item) order. Pairs sorted by cell stay grouped by cell, so only
// each cell's run needs sorting; pairs equal in both fields are
// interchangeable, so either way this matches a full std::sort.
template <class Renumber>
void renumber_cell_pairs(std::vector<CellItemPair>& pairs, Renumber renumber, unsigned threads) {
    parallel_for(pairs.size(), [&](size_t b, size_t e, unsigned) {
        for (size_t i = b; i < e; i++) pairs[i].item_id = renumber(pairs[i].item_id);
    }, threads);
    bool by_cell = !parallel_any(pairs.empty() ? 0 : pairs.size() - 1, [&](size_t i) {
        return pairs[i + 1].cell_id < pairs[i].cell_id;
    }, threads);
    if (by_cell)
        parallel_sort_runs(pairs.begin(), pairs.end(),
                           [](const CellItemPair& a, const CellItemPair& b) { return a.cell_id == b.cell_id; },
                           cell_item_less, threads);
    else
        parallel_sort(pairs.begin(), pairs.end(), cell_item_less, threads);
}

template <class T>
struct Repacked {
    std::vector<T> items;
    std::vector<size_t> at;  // where each source range landed; n + 1 entries
};

// Copies count(i) elements of src from from(i) onwards into a new buffer,
// for i in [0, n) in order.
template <class T, class Count, class From>
Repacked<T> repack(size_t n, const std::vector<T>& src, Count count, From from, unsigned threads) {
    Repacked<T> out{{}, parallel_offsets<size_t>(n, count, threads)};
    out.items.resize(out.at[n]);
    parallel_for(n, [&](size_t b, size_t e, unsigned) {
        for (size_t i = b; i < e; i++)
            if (size_t c = out.at[i + 1] - out.at[i]) std::copy_n(src.begin() + from(i), c, out.items.begin() + out.at[i]);
    }, threads);
    return out;
}

// Sorts addr_points by (street_id, housenumber_id, lat_bits, lng_bits, ...).
// String offsets are already remapped to the sorted pool, so they order
// deterministically; raw float bits break the remaining ties.
inline void reorder_addr_points(ParsedData& data, unsigned threads = 0) {
    if (data.sorted_addr_cells.empty()) return;

    const size_t n = data.addr_points.size();
    const auto& points = data.addr_points;
    const auto& vertices = data.addr_vertices;
    const bool have_osm = data.addr_osm_ids.size() == n;
    auto addr_less = [&](uint32_t a, uint32_t b) {
        const auto& pa = points[a];
        const auto& pb = points[b];
        if (pa.street_id != pb.street_id) return pa.street_id < pb.street_id;
        if (pa.housenumber_id != pb.housenumber_id) return pa.housenumber_id < pb.housenumber_id;
        uint32_t la = float_bits(pa.lat), lb = float_bits(pb.lat);
        if (la != lb) return la < lb;
        uint32_t ga = float_bits(pa.lng), gb = float_bits(pb.lng);
        if (ga != gb) return ga < gb;
        // Final tiebreaker: osm_id. Using the original index `a < b`
        // here resolved ties non-deterministically because the
        // index reflected build-encounter order from multi-threaded
        // PBF parsing. osm_id is invariant per record and gives a
        // truly canonical order, so dedup picks the same record
        // every run.
        if (have_osm && data.addr_osm_ids[a] != data.addr_osm_ids[b])
            return data.addr_osm_ids[a] < data.addr_osm_ids[b];
        // osm_id can still COLLIDE: synthetic TIGER ids are a 56-bit
        // FNV hash of (lat,lng,housenumber,street), so two distinct
        // records with the same addressing keys share an id. Without a
        // further tiebreak std::sort leaves their relative order to
        // build-encounter order (non-deterministic); when a polygon/node
        // pair swaps, the vertex_count sequence shifts addr_vertices
        // packing and cascades vertex_offset for every later record
        // (~98.9 MiB planet churn) plus a strategy-2 tombstone. Extend
        // to a total order: vertex_count, parent_way_id, then the
        // polygon vertex bytes (pre-sort offsets are still valid here —
        // addr_vertices is repacked only after this sort).
        if (pa.vertex_count != pb.vertex_count)
            return pa.vertex_count < pb.vertex_count;
        if (pa.parent_way_id != pb.parent_way_id)
            return pa.parent_way_id < pb.parent_way_id;
        if (pa.vertex_count > 0 &&
            pa.vertex_offset != NO_DATA && pb.vertex_offset != NO_DATA &&
            (size_t)pa.vertex_offset + pa.vertex_count <= vertices.size() &&
            (size_t)pb.vertex_offset + pb.vertex_count <= vertices.size()) {
            int c = std::memcmp(&vertices[pa.vertex_offset], &vertices[pb.vertex_offset],
                                (size_t)pa.vertex_count * sizeof(NodeCoord));
            if (c != 0) return c < 0;
        }
        return a < b;
    };
    // The comparator ends on the index, so it is a strict total order —
    // provided the vertex tiebreak applies to every polygon. A polygon
    // without its vertices skips it, which can make the order intransitive;
    // std::sort then decides alone.
    bool total_order = !parallel_any(n, [&](size_t i) {
        const auto& p = points[i];
        return p.vertex_count > 0 &&
               (p.vertex_offset == NO_DATA || (size_t)p.vertex_offset + p.vertex_count > vertices.size());
    }, threads);
    std::vector<uint32_t> order = total_order
        ? parallel_sort_indices(n, addr_less, [](uint32_t, uint32_t) { return true; }, threads)
        : std_sort_indices(n, addr_less);

    // Dedup consecutive identical records (planet has ~4M duplicates).
    // Compare everything except vertex_offset — same polygon content
    // stored at different offsets should still dedup.
    std::vector<uint32_t> starts = run_starts(n, [&](size_t i, size_t j) {
        const auto& a = points[order[i]];
        const auto& b = points[order[j]];
        return a.lat == b.lat && a.lng == b.lng &&
               a.housenumber_id == b.housenumber_id &&
               a.street_id == b.street_id &&
               a.parent_way_id == b.parent_way_id &&
               a.vertex_count == b.vertex_count;
    }, threads);
    const size_t kept = starts.size();

    // Reorder the vertex buffer alongside addr_points. Duplicates keep
    // their copy of the vertices too: the buffer is packed in sorted order
    // before the dedup.
    auto has_polygon = [](const AddrPoint& p) { return p.vertex_count > 0 && p.vertex_offset != NO_DATA; };
    Repacked<NodeCoord> repacked = repack(n, vertices,
        [&](size_t i) { const auto& p = points[order[i]]; return has_polygon(p) ? size_t(p.vertex_count) : 0; },
        [&](size_t i) { return points[order[i]].vertex_offset; }, threads);
    std::vector<AddrPoint> sorted(kept);
    parallel_for(kept, [&](size_t b, size_t e, unsigned) {
        for (size_t k = b; k < e; k++) {
            AddrPoint p = points[order[starts[k]]];
            if (has_polygon(p)) p.vertex_offset = static_cast<uint32_t>(repacked.at[starts[k]]);
            sorted[k] = p;
        }
    }, threads);
    data.addr_vertices = std::move(repacked.items);
    repacked.at = {};
    data.addr_points = std::move(sorted);
    // Reorder + dedup addr_osm_ids in lockstep so slot[i] keeps the
    // matching osm_id. Without this, strategy-2 reads garbage osm_ids
    // (whatever survived in build-encounter order at index i) and the
    // resulting addr_points.bin / sidecar are non-deterministic across
    // same-PBF rebuilds; left at the pre-dedup size, it fails the
    // size-equality check in apply_strategy2_addrs and strategy-2
    // silently early-returns.
    if (have_osm) data.addr_osm_ids = first_set_per_run(data.addr_osm_ids, order, starts, uint64_t(0), threads);
    if (data.addr_postcode_ids.size() == n)
        data.addr_postcode_ids = first_set_per_run(data.addr_postcode_ids, order, starts, NO_DATA, threads);
    if (kept < n)
        std::cerr << "  Deduped addr_points: " << n << " → " << kept
                  << " (-" << (n - kept) << ")" << std::endl;

    const std::vector<uint32_t> old_to_new = renumber_runs(order, starts, threads);
    order = {};
    starts = {};
    renumber_cell_pairs(data.sorted_addr_cells, [&](uint32_t id) { return old_to_new[id]; }, threads);
    data.cell_to_addrs.clear();
    std::cerr << "  Addr points sorted: " << n << std::endl;
}

// Sorts ways by (name, node_count, nodes) and reorders their nodes.
inline void reorder_ways(ParsedData& data, unsigned threads = 0) {
    if (data.ways.empty()) return;

    const size_t n = data.ways.size();
    const auto& ways = data.ways;
    const auto& nodes = data.street_nodes;
    const bool have_osm = data.way_osm_ids.size() == n;
    auto way_less = [&](uint32_t a, uint32_t b) {
        const auto& wa = ways[a];
        const auto& wb = ways[b];
        if (wa.name_id != wb.name_id) return wa.name_id < wb.name_id;
        if (wa.node_count != wb.node_count) return wa.node_count < wb.node_count;
        // Compare all nodes for total order
        uint16_t nc = std::min(wa.node_count, wb.node_count);
        for (uint16_t j = 0; j < nc; j++) {
            uint32_t la = float_bits(nodes[wa.node_offset + j].lat);
            uint32_t lb = float_bits(nodes[wb.node_offset + j].lat);
            if (la != lb) return la < lb;
            uint32_t ga = float_bits(nodes[wa.node_offset + j].lng);
            uint32_t gb = float_bits(nodes[wb.node_offset + j].lng);
            if (ga != gb) return ga < gb;
        }
        // osm_id tiebreaker for full determinism — without it,
        // duplicate-content ways resolve to whichever index was
        // first in the build-encounter order (non-deterministic).
        if (have_osm) return data.way_osm_ids[a] < data.way_osm_ids[b];
        return false;
    };
    // Ways that tie have the same name, nodes and osm id, so the dedup
    // below folds them into one way whose header, nodes and osm id are the
    // same whichever came first; only differing header padding could tell.
    auto tie_matters = [&](uint32_t a, uint32_t b) {
        constexpr size_t kAfterOffset = offsetof(WayHeader, node_count);
        return std::memcmp(reinterpret_cast<const char*>(&ways[a]) + kAfterOffset,
                           reinterpret_cast<const char*>(&ways[b]) + kAfterOffset,
                           sizeof(WayHeader) - kAfterOffset) != 0;
    };
    std::vector<uint32_t> order = parallel_sort_indices(n, way_less, tie_matters, threads);

    // Dedup identical consecutive ways (name, node_count, node bytes).
    std::vector<uint32_t> starts = run_starts(n, [&](size_t i, size_t j) {
        const auto& a = ways[order[i]];
        const auto& b = ways[order[j]];
        return a.name_id == b.name_id && a.node_count == b.node_count &&
               std::memcmp(nodes.data() + a.node_offset, nodes.data() + b.node_offset,
                           a.node_count * sizeof(NodeCoord)) == 0;
    }, threads);
    const size_t kept = starts.size();

    Repacked<NodeCoord> repacked = repack(kept, nodes,
        [&](size_t k) { return size_t(ways[order[starts[k]]].node_count); },
        [&](size_t k) { return ways[order[starts[k]]].node_offset; }, threads);
    std::vector<WayHeader> new_ways(kept);
    parallel_for(kept, [&](size_t b, size_t e, unsigned) {
        for (size_t k = b; k < e; k++) {
            new_ways[k] = ways[order[starts[k]]];
            new_ways[k].node_offset = static_cast<uint32_t>(repacked.at[k]);
        }
    }, threads);
    // Reorder + dedup way_osm_ids in lockstep. Each kept way takes the
    // first non-zero osm id of its duplicates in SORT order (not original
    // order), matching how data.ways picks its dedup survivor above.
    // Original order is build-encounter order, non-deterministic, and
    // the resulting way_osm_ids[k] wouldn't match the osm_id of
    // data.ways[k].
    if (have_osm) data.way_osm_ids = first_set_per_run(data.way_osm_ids, order, starts, int64_t(0), threads);
    // Remap way_parent_ids + way_postcode_ids: reorder + dedup. Each kept
    // way takes the set value of the duplicate with the highest original
    // index, as the serial pass did walking the original order with set
    // values overwriting.
    auto remap_way_vec = [&](std::vector<uint32_t>& vec) {
        if (vec.size() != n) return;
        std::vector<uint32_t> nv(kept, NO_DATA);
        parallel_for(kept, [&](size_t b, size_t e, unsigned) {
            for (size_t k = b; k < e; k++) {
                uint32_t last = 0;
                bool found = false;
                for (size_t i = starts[k], end = run_end(starts, k, n); i < end; i++) {
                    uint32_t oi = order[i];
                    if (vec[oi] != NO_DATA && (!found || oi > last)) {
                        last = oi;
                        found = true;
                    }
                }
                if (found) nv[k] = vec[last];
            }
        }, threads);
        vec = std::move(nv);
    };
    remap_way_vec(data.way_parent_ids);
    remap_way_vec(data.way_postcode_ids);
    const std::vector<uint32_t> old_to_new = renumber_runs(order, starts, threads);
    order = {};
    starts = {};
    data.ways = std::move(new_ways);
    data.street_nodes = std::move(repacked.items);
    renumber_cell_pairs(data.sorted_way_cells, [&](uint32_t id) { return old_to_new[id]; }, threads);
    data.cell_to_ways.clear();
    // Remap addr_point parent_way_id references through way old_to_new
    parallel_for(data.addr_points.size(), [&](size_t b, size_t e, unsigned) {
        for (size_t i = b; i < e; i++) {
            auto& a = data.addr_points[i];
            if (a.parent_way_id != NO_DATA && a.parent_way_id < n) a.parent_way_id = old_to_new[a.parent_way_id];
        }
    }, threads);
    std::cerr << "  Ways sorted: " << n << " (" << (n - kept) << " duplicates removed)" << std::endl;
}

// Sorts interps by (street, start, end, type, nodes) and reorders their nodes.
inline void reorder_interps(ParsedData& data) {
    if (!data.interp_ways.empty()) {
        size_t n = data.interp_ways.size();
        std::vector<uint32_t> order(n);
        std::iota(order.begin(), order.end(), 0);
        const auto& sp = data.string_pool.data();
        std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) {
            const auto& ia = data.interp_ways[a];
            const auto& ib = data.interp_ways[b];
            if (ia.street_id != ib.street_id) return ia.street_id < ib.street_id;
            if (ia.start_number != ib.start_number) return ia.start_number < ib.start_number;
            if (ia.end_number != ib.end_number) return ia.end_number < ib.end_number;
            if (ia.interpolation != ib.interpolation) return ia.interpolation < ib.interpolation;
            uint16_t nc = std::min(ia.node_count, ib.node_count);
            for (uint16_t j = 0; j < nc; j++) {
                uint32_t la = float_bits(data.interp_nodes[ia.node_offset + j].lat);
                uint32_t lb = float_bits(data.interp_nodes[ib.node_offset + j].lat);
                if (la != lb) return la < lb;
                uint32_t ga = float_bits(data.interp_nodes[ia.node_offset + j].lng);
                uint32_t gb = float_bits(data.interp_nodes[ib.node_offset + j].lng);
                if (ga != gb) return ga < gb;
            }
            if (ia.node_count != ib.node_count) return ia.node_count < ib.node_count;
            if (data.interp_osm_ids.size() == data.interp_ways.size())
                return data.interp_osm_ids[a] < data.interp_osm_ids[b];
            return false;
        });
        // Reorder + DEDUP. TIGER frequently emits the exact same
        // interpolation way twice (identical street/range/type/geometry —
        // e.g. overlapping county extracts). Those duplicates share a
        // synthetic osm_id, which the strategy-2 allocator can't
        // disambiguate: it tombstones + reslots one of each pair non-
        // deterministically (~97k tombstones on the planet), churning
        // interp_ways/nodes/entries between same-PBF builds. They are
        // genuine duplicates (a redundant interpolation segment), so drop
        // consecutive identical entries — mirroring the street-way dedup —
        // and map both old indices to the surviving one. (Compared on
        // street/range/type/node_count + node geometry; node_offset is
        // position-dependent and excluded.)
        std::vector<uint32_t> old_to_new(n);
        std::vector<InterpWay> new_interps;
        std::vector<NodeCoord> new_nodes;
        new_interps.reserve(n);
        new_nodes.reserve(data.interp_nodes.size());
        const bool have_interp_osm = (data.interp_osm_ids.size() == n);
        std::vector<uint64_t> new_osm;
        if (have_interp_osm) new_osm.reserve(n);
        const bool have_interp_pc = (data.interp_postcode_ids.size() == n);
        std::vector<uint32_t> new_pc;
        if (have_interp_pc) new_pc.reserve(n);
        for (uint32_t i = 0; i < n; i++) {
            uint32_t oi = order[i];
            const InterpWay& cur = data.interp_ways[oi];
            uint32_t old_off = cur.node_offset;
            bool is_dup = false;
            if (!new_interps.empty()) {
                const InterpWay& prev = new_interps.back();
                if (prev.street_id == cur.street_id &&
                    prev.start_number == cur.start_number &&
                    prev.end_number == cur.end_number &&
                    prev.interpolation == cur.interpolation &&
                    prev.node_count == cur.node_count) {
                    is_dup = true;
                    for (uint16_t j = 0; j < cur.node_count; j++) {
                        if (memcmp(&new_nodes[prev.node_offset + j],
                                   &data.interp_nodes[old_off + j],
                                   sizeof(NodeCoord)) != 0) { is_dup = false; break; }
                    }
                }
            }
            if (is_dup) {
                old_to_new[oi] = static_cast<uint32_t>(new_interps.size() - 1);
            } else {
                old_to_new[oi] = static_cast<uint32_t>(new_interps.size());
                InterpWay iw = cur;
                iw.node_offset = static_cast<uint32_t>(new_nodes.size());
                for (uint16_t j = 0; j < iw.node_count; j++)
                    new_nodes.push_back(data.interp_nodes[old_off + j]);
                if (have_interp_osm) new_osm.push_back(data.interp_osm_ids[oi]);
                if (have_interp_pc) new_pc.push_back(data.interp_postcode_ids[oi]);
                new_interps.push_back(iw);
            }
        }
        size_t interp_deduped = n - new_interps.size();
        if (have_interp_osm) data.interp_osm_ids = std::move(new_osm);
        if (have_interp_pc) data.interp_postcode_ids = std::move(new_pc);
        data.interp_ways = std::move(new_interps);
        data.interp_nodes = std::move(new_nodes);
        for (auto& p : data.sorted_interp_cells) p.item_id = old_to_new[p.item_id];
        auto cmp = cell_item_less;
        std::sort(data.sorted_interp_cells.begin(), data.sorted_interp_cells.end(), cmp);
        // Dedup now-identical (cell_id, item_id) pairs: when two duplicate
        // interps in the same cell collapse to one survivor their cell
        // entries become identical, which would otherwise list the survivor
        // twice per cell.
        data.sorted_interp_cells.erase(
            std::unique(data.sorted_interp_cells.begin(), data.sorted_interp_cells.end(),
                        [](const CellItemPair& a, const CellItemPair& b) {
                            return a.cell_id == b.cell_id && a.item_id == b.item_id;
                        }),
            data.sorted_interp_cells.end());
        data.cell_to_interps.clear();
        std::cerr << "  Interps sorted: " << data.interp_ways.size()
                  << " (" << interp_deduped << " duplicates removed)" << std::endl;
    }
}

// Sorts admin polygons by (name, level, country, vertex_count) and reorders
// their vertices.
inline void reorder_admin_polygons(ParsedData& data) {
    if (!data.admin_polygons.empty()) {
        size_t n = data.admin_polygons.size();
        std::vector<uint32_t> order(n);
        std::iota(order.begin(), order.end(), 0);
        const auto& sp = data.string_pool.data();
        std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) {
            const auto& pa = data.admin_polygons[a];
            const auto& pb = data.admin_polygons[b];
            if (pa.name_id != pb.name_id) return pa.name_id < pb.name_id;
            if (pa.admin_level != pb.admin_level) return pa.admin_level < pb.admin_level;
            if (pa.country_code != pb.country_code) return pa.country_code < pb.country_code;
            if (pa.vertex_count != pb.vertex_count) return pa.vertex_count < pb.vertex_count;
            // Compare first few vertices for tiebreaking
            uint32_t nc = std::min(pa.vertex_count, pb.vertex_count);
            nc = std::min(nc, 20u); // limit comparison depth
            for (uint32_t j = 0; j < nc; j++) {
                uint32_t la = float_bits(data.admin_vertices[pa.vertex_offset + j].lat);
                uint32_t lb = float_bits(data.admin_vertices[pb.vertex_offset + j].lat);
                if (la != lb) return la < lb;
                uint32_t ga = float_bits(data.admin_vertices[pa.vertex_offset + j].lng);
                uint32_t gb = float_bits(data.admin_vertices[pb.vertex_offset + j].lng);
                if (ga != gb) return ga < gb;
            }
            if (data.admin_osm_ids.size() == data.admin_polygons.size())
                return data.admin_osm_ids[a] < data.admin_osm_ids[b];
            return false;
        });
        std::vector<uint32_t> old_to_new(n);
        for (uint32_t i = 0; i < n; i++) old_to_new[order[i]] = i;
        std::vector<AdminPolygon> new_polys(n);
        std::vector<NodeCoord> new_verts;
        new_verts.reserve(data.admin_vertices.size());
        for (uint32_t i = 0; i < n; i++) {
            auto p = data.admin_polygons[order[i]];
            uint32_t old_off = p.vertex_offset;
            p.vertex_offset = static_cast<uint32_t>(new_verts.size());
            for (uint32_t j = 0; j < p.vertex_count; j++)
                new_verts.push_back(data.admin_vertices[old_off + j]);
            new_polys[i] = p;
        }
        // Reorder admin_osm_ids in lockstep (no dedup happens in
        // this sort — just a permutation — so the size stays the
        // same).
        if (data.admin_osm_ids.size() == n) {
            std::vector<uint64_t> new_osm(n);
            for (uint32_t i = 0; i < n; i++)
                new_osm[i] = data.admin_osm_ids[order[i]];
            data.admin_osm_ids = std::move(new_osm);
        }
        data.admin_polygons = std::move(new_polys);
        data.admin_vertices = std::move(new_verts);
        // Remap place-node parent_poly_id references
        for (auto& pn : data.place_nodes) {
            if (pn.parent_poly_id != 0xFFFFFFFFu &&
                pn.parent_poly_id < old_to_new.size()) {
                pn.parent_poly_id = old_to_new[pn.parent_poly_id];
            }
        }
        // Remap poi parent_poly_id references
        for (auto& pr : data.poi_records) {
            if (pr.parent_poly_id != 0xFFFFFFFFu &&
                pr.parent_poly_id < old_to_new.size()) {
                pr.parent_poly_id = old_to_new[pr.parent_poly_id];
            }
        }
        // Remap admin_parent_ids (both indices and values)
        if (data.admin_parent_ids.size() == n) {
            std::vector<uint32_t> new_ap(n);
            for (uint32_t i = 0; i < n; i++) {
                uint32_t old_parent = data.admin_parent_ids[order[i]];
                new_ap[i] = (old_parent != NO_DATA && old_parent < n)
                    ? old_to_new[old_parent] : NO_DATA;
            }
            data.admin_parent_ids = std::move(new_ap);
        }
        // Remap way_parent_ids (values only — way order hasn't changed yet)
        for (auto& pid : data.way_parent_ids) {
            if (pid != NO_DATA && pid < n) pid = old_to_new[pid];
        }
        // Remap admin cell entries
        for (auto& [cell_id, ids] : data.cell_to_admin) {
            for (auto& id : ids) {
                uint32_t flags = id & INTERIOR_FLAG;
                uint32_t masked = id & ID_MASK;
                if (masked < old_to_new.size())
                    id = old_to_new[masked] | flags;
            }
            std::sort(ids.begin(), ids.end());
        }
        std::cerr << "  Admin polygons sorted: " << n << std::endl;
    }
}

// Sorts POI records by (category, tier, name_id, lat_bits, lng_bits), reorders
// their vertices and computes their importance.
inline void reorder_pois(ParsedData& data, std::vector<float>& poi_elevations,
                         std::vector<uint32_t>& poi_qids,
                         const std::vector<QidSitelinks>& sitelinks_data) {
    auto lookup_sitelinks = [&sitelinks_data](uint32_t qid) -> uint16_t {
        auto it = std::lower_bound(sitelinks_data.begin(), sitelinks_data.end(), qid,
            [](const QidSitelinks& entry, uint32_t q) { return entry.qid < q; });
        if (it != sitelinks_data.end() && it->qid == qid) return it->count;
        return 0;
    };

    if (!data.poi_records.empty()) {
        size_t n = data.poi_records.size();
        std::vector<uint32_t> order(n);
        std::iota(order.begin(), order.end(), 0);
        std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) {
            const auto& pa = data.poi_records[a];
            const auto& pb = data.poi_records[b];
            if (pa.category != pb.category) return pa.category < pb.category;
            if (pa.tier != pb.tier) return pa.tier < pb.tier;
            if (pa.name_id != pb.name_id) return pa.name_id < pb.name_id;
            uint32_t la = float_bits(pa.lat), lb = float_bits(pb.lat);
            if (la != lb) return la < lb;
            uint32_t ga = float_bits(pa.lng), gb = float_bits(pb.lng);
            if (ga != gb) return ga < gb;
            if (data.poi_osm_ids.size() == data.poi_records.size())
                return data.poi_osm_ids[a] < data.poi_osm_ids[b];
            return a < b;
        });

        // Build old→new mapping
        std::vector<uint32_t> old_to_new(n);
        for (uint32_t i = 0; i < n; i++) old_to_new[order[i]] = i;

        // Reorder records + vertices + elevations + qids, dedup
        bool have_elevations = (poi_elevations.size() == n);
        bool have_qids = (poi_qids.size() == n);
        std::vector<PoiRecord> new_pois;
        std::vector<NodeCoord> new_poi_verts;
        std::vector<float> new_poi_elevations;
        std::vector<uint32_t> new_poi_qids;
        new_pois.reserve(n);
        new_poi_verts.reserve(data.poi_vertices.size());
        if (have_elevations) new_poi_elevations.reserve(n);
        if (have_qids) new_poi_qids.reserve(n);
        std::vector<uint32_t> dedup_remap(n);
        size_t write_pos = 0;

        for (uint32_t i = 0; i < n; i++) {
            auto p = data.poi_records[order[i]];
            uint32_t old_voff = p.vertex_offset;
            uint32_t vc = p.vertex_count;

            // Check for duplicate vs previous
            bool is_dup = false;
            if (write_pos > 0) {
                auto& prev = new_pois[write_pos - 1];
                if (prev.category == p.category && prev.tier == p.tier &&
                    prev.name_id == p.name_id &&
                    prev.lat == p.lat && prev.lng == p.lng &&
                    prev.vertex_count == p.vertex_count && prev.flags == p.flags) {
                    is_dup = true;
                }
            }

            if (is_dup) {
                dedup_remap[i] = static_cast<uint32_t>(write_pos - 1);
            } else {
                dedup_remap[i] = static_cast<uint32_t>(write_pos);
                p.vertex_offset = static_cast<uint32_t>(new_poi_verts.size());
                if (vc > 0 && old_voff != NO_DATA) {
                    for (uint32_t j = 0; j < vc; j++)
                        new_poi_verts.push_back(data.poi_vertices[old_voff + j]);
                }
                new_pois.push_back(p);
                if (have_elevations) new_poi_elevations.push_back(poi_elevations[order[i]]);
                if (have_qids) new_poi_qids.push_back(poi_qids[order[i]]);
                write_pos++;
            }
        }

        size_t deduped = n - new_pois.size();
        // Reorder + dedup poi_osm_ids in lockstep. The POI dedup loop
        // above iterates SORT order (for (uint32_t i = 0; i < n; i++)
        // using order[i]), so dedup_remap[i] is sort-indexed and the
        // FIRST sort-position to map to each new_idx wins. Match
        // that here too.
        if (data.poi_osm_ids.size() == n) {
            std::vector<uint64_t> new_osm(new_pois.size(), 0);
            for (uint32_t si = 0; si < n; si++) {
                uint32_t new_idx = dedup_remap[si];
                if (new_idx < new_osm.size() && new_osm[new_idx] == 0)
                    new_osm[new_idx] = data.poi_osm_ids[order[si]];
            }
            data.poi_osm_ids = std::move(new_osm);
        }
        data.poi_records = std::move(new_pois);
        data.poi_vertices = std::move(new_poi_verts);
        if (have_elevations) poi_elevations = std::move(new_poi_elevations);
        if (have_qids) poi_qids = std::move(new_poi_qids);

        // Remap sorted_poi_cells IDs (preserving INTERIOR_FLAG)
        for (auto& p : data.sorted_poi_cells) {
            uint32_t flags = p.item_id & INTERIOR_FLAG;
            uint32_t old_id = p.item_id & ID_MASK;
            p.item_id = dedup_remap[old_to_new[old_id]] | flags;
        }
        auto cmp = cell_item_less;
        std::sort(data.sorted_poi_cells.begin(), data.sorted_poi_cells.end(), cmp);
        data.cell_to_pois.clear();
        std::cerr << "  POI records sorted: " << n << " (" << deduped << " duplicates removed)" << std::endl;

        // Compute importance
        {
            for (size_t i = 0; i < data.poi_records.size(); i++) {
                auto& pr = data.poi_records[i];
                PoiCategory cat = static_cast<PoiCategory>(pr.category);
                double base = category_base_importance(cat);

                // Wiki multiplier
                bool has_wp = (pr.flags & POI_FLAG_WIKIPEDIA) != 0;
                bool has_wd = (pr.flags & POI_FLAG_WIKIDATA) != 0;
                double wiki_mult = 1.0;
                if (!sitelinks_data.empty() && i < poi_qids.size() && poi_qids[i] > 0) {
                    uint16_t sl = lookup_sitelinks(poi_qids[i]);
                    if (sl > 0) {
                        // Smooth curve: 1 sitelink=1.2x, 10=1.8x, 50=2.9x, 200=3.6x
                        wiki_mult = 1.0 + std::log2(1.0 + sl) / 3.0;
                    } else if (has_wp && has_wd) {
                        wiki_mult = 3.0;  // fallback to binary flags
                    } else if (has_wp) {
                        wiki_mult = 2.5;
                    } else if (has_wd) {
                        wiki_mult = 1.5;
                    }
                } else {
                    // No sitelinks data — use binary flags
                    if (has_wp && has_wd) wiki_mult = 3.0;
                    else if (has_wp) wiki_mult = 2.5;
                    else if (has_wd) wiki_mult = 1.5;
                }

                double raw = base * wiki_mult;

                // Peak/volcano elevation scaling
                if ((cat == PoiCategory::PEAK || cat == PoiCategory::VOLCANO) && i < poi_elevations.size()) {
                    float ele = poi_elevations[i];
                    if (ele > 0) raw *= std::min((double)ele / 2000.0, 3.0);
                    else raw *= 0.5;
                }

                // Polygon area scaling
                if (pr.vertex_count > 0 && pr.vertex_offset != NO_DATA) {
                    // Approximate area in km² using shoelace formula
                    double area_deg2 = 0;
                    for (uint32_t j = 0; j < pr.vertex_count; j++) {
                        uint32_t k = (j + 1) % pr.vertex_count;
                        const auto& a = data.poi_vertices[pr.vertex_offset + j];
                        const auto& b = data.poi_vertices[pr.vertex_offset + k];
                        area_deg2 += (double)a.lng * b.lat - (double)b.lng * a.lat;
                    }
                    area_deg2 = std::abs(area_deg2) / 2.0;
                    double lat_mid = std::abs((double)pr.lat);
                    double deg_to_km = 111.32 * std::cos(lat_mid * M_PI / 180.0);
                    double area_km2 = area_deg2 * 111.32 * deg_to_km;
                    if (area_km2 > 0) raw *= std::min(1.0 + std::log2(1.0 + area_km2) / 4.0, 2.0);
                }

                pr.importance = static_cast<uint8_t>(std::max(1.0, std::min(255.0, raw)));
            }
            std::cerr << "  POI importance computed" << std::endl;
        }
    }
}

// Sorts place nodes by (place_type, name_id, lat_bits, lng_bits).
inline void reorder_place_nodes(ParsedData& data) {
    if (!data.place_nodes.empty()) {
        size_t n = data.place_nodes.size();
        std::vector<uint32_t> order(n);
        std::iota(order.begin(), order.end(), 0);
        std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) {
            const auto& pa = data.place_nodes[a];
            const auto& pb = data.place_nodes[b];
            if (pa.place_type != pb.place_type) return pa.place_type < pb.place_type;
            if (pa.name_id != pb.name_id) return pa.name_id < pb.name_id;
            uint32_t la = float_bits(pa.lat), lb = float_bits(pb.lat);
            if (la != lb) return la < lb;
            uint32_t ga = float_bits(pa.lng), gb = float_bits(pb.lng);
            if (ga != gb) return ga < gb;
            if (data.place_osm_ids.size() == data.place_nodes.size())
                return data.place_osm_ids[a] < data.place_osm_ids[b];
            return false;
        });
        std::vector<uint32_t> old_to_new(n);
        for (uint32_t i = 0; i < n; i++) old_to_new[order[i]] = i;
        std::vector<PlaceNode> sorted(n);
        for (uint32_t i = 0; i < n; i++) sorted[i] = data.place_nodes[order[i]];
        // Reorder place_osm_ids in lockstep
        if (data.place_osm_ids.size() == n) {
            std::vector<uint64_t> new_osm(n);
            for (uint32_t i = 0; i < n; i++) new_osm[i] = data.place_osm_ids[order[i]];
            data.place_osm_ids = std::move(new_osm);
        }
        data.place_nodes = std::move(sorted);
        for (auto& p : data.sorted_place_cells) p.item_id = old_to_new[p.item_id];
        auto cmp = cell_item_less;
        std::sort(data.sorted_place_cells.begin(), data.sorted_place_cells.end(), cmp);
        std::cerr << "  Place nodes sorted: " << n << std::endl;
    }
}
