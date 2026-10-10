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

// A sorted order, saying so in the build log when ties sent the sort to
// the serial std::sort (minutes on the planet).
inline std::vector<uint32_t> take_order(const char* what, SortedIndices sorted) {
    if (sorted.serial)
        std::cerr << "  " << what << ": records tie in ways the output shows; sorted serially" << std::endl;
    return std::move(sorted.order);
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
// An `out` of one element per run (see vector_beside) is filled in place of
// a new one.
template <class T>
std::vector<T> first_set_per_run(const std::vector<T>& values, const std::vector<uint32_t>& order,
                                 const std::vector<uint32_t>& starts, T none, unsigned threads,
                                 std::vector<T> out = {}) {
    if (out.size() != starts.size()) out.assign(starts.size(), none);
    parallel_for(starts.size(), [&](size_t b, size_t e, unsigned) {
        for (size_t k = b; k < e; k++) {
            out[k] = none;
            for (size_t i = starts[k], end = run_end(starts, k, order.size()); i < end && out[k] == none; i++)
                out[k] = values[order[i]];
        }
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
// for i in [0, n) in order. Buffers made beside earlier work (see
// vector_beside) can come in: `at` of n + 1 elements, `items` of at least as
// many elements as get copied, shrunk to fit.
template <class T, class Count, class From>
Repacked<T> repack(size_t n, const std::vector<T>& src, Count count, From from, unsigned threads,
                   std::vector<size_t> at = {}, std::vector<T> items = {}) {
    Repacked<T> out{std::move(items), parallel_offsets<size_t>(n, count, threads, std::move(at))};
    if (out.items.size() < out.at[n]) out.items.assign(out.at[n], T{});
    out.items.resize(out.at[n]);
    parallel_for(n, [&](size_t b, size_t e, unsigned) {
        for (size_t i = b; i < e; i++)
            if (size_t c = out.at[i + 1] - out.at[i]) std::copy_n(src.begin() + from(i), c, out.items.begin() + out.at[i]);
    }, threads);
    return out;
}

// values[order[i]] for every i.
template <class T>
std::vector<T> gather(const std::vector<T>& values, const std::vector<uint32_t>& order, unsigned threads) {
    std::vector<T> out(order.size());
    parallel_for(order.size(), [&](size_t b, size_t e, unsigned) {
        for (size_t i = b; i < e; i++) out[i] = values[order[i]];
    }, threads);
    return out;
}

// old_to_new for a permutation: order[i] becomes i.
inline std::vector<uint32_t> invert(const std::vector<uint32_t>& order, unsigned threads) {
    std::vector<uint32_t> old_to_new(order.size());
    parallel_for(order.size(), [&](size_t b, size_t e, unsigned) {
        for (size_t i = b; i < e; i++) old_to_new[order[i]] = static_cast<uint32_t>(i);
    }, threads);
    return old_to_new;
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
    // The arrays the sorted points and vertices move into fault in beside
    // the sort, which leaves their sizes alone.
    auto has_polygon = [](const AddrPoint& p) { return p.vertex_count > 0 && p.vertex_offset != NO_DATA; };
    const size_t vertex_total = parallel_sum<size_t>(n, [&](size_t i) {
        return has_polygon(points[i]) ? size_t(points[i].vertex_count) : 0;
    }, threads);
    auto at_buffer = vector_beside<size_t>(n + 1);
    auto vertex_buffer = vector_beside<NodeCoord>(vertex_total);

    // The sort orders keys holding the fields that decide nearly every
    // comparison, rather than indices into the planet's addr_points.
    struct AddrKey {
        uint32_t street_id, housenumber_id, lat_bits, lng_bits, index;
    };
    auto key_of = [&](size_t i) {
        const auto& p = points[i];
        return AddrKey{p.street_id, p.housenumber_id, float_bits(p.lat), float_bits(p.lng), static_cast<uint32_t>(i)};
    };
    auto key_less = [&](const AddrKey& a, const AddrKey& b) {
        if (a.street_id != b.street_id) return a.street_id < b.street_id;
        if (a.housenumber_id != b.housenumber_id) return a.housenumber_id < b.housenumber_id;
        if (a.lat_bits != b.lat_bits) return a.lat_bits < b.lat_bits;
        if (a.lng_bits != b.lng_bits) return a.lng_bits < b.lng_bits;
        return addr_less(a.index, b.index);
    };
    auto any_tie = [](uint32_t, uint32_t) { return true; };
    std::vector<uint32_t> order = total_order
        ? take_order("addr_points", parallel_sort_keys(n, key_of, key_less, any_tie, threads))
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
    const bool have_postcodes = data.addr_postcode_ids.size() == n;
    auto sorted_buffer = vector_beside<AddrPoint>(kept);

    // Reorder the vertex buffer alongside addr_points. Duplicates keep
    // their copy of the vertices too: the buffer is packed in sorted order
    // before the dedup.
    Repacked<NodeCoord> repacked = repack(n, vertices,
        [&](size_t i) { const auto& p = points[order[i]]; return has_polygon(p) ? size_t(p.vertex_count) : 0; },
        [&](size_t i) { return points[order[i]].vertex_offset; }, threads, at_buffer.get(), vertex_buffer.get());
    std::vector<AddrPoint> sorted = sorted_buffer.get();
    auto osm_buffer = vector_beside<uint64_t>(have_osm ? kept : 0);
    auto postcode_buffer = vector_beside<uint32_t>(have_postcodes ? kept : 0);
    parallel_for(kept, [&](size_t b, size_t e, unsigned) {
        for (size_t k = b; k < e; k++) {
            AddrPoint p = points[order[starts[k]]];
            if (has_polygon(p)) p.vertex_offset = static_cast<uint32_t>(repacked.at[starts[k]]);
            sorted[k] = p;
        }
    }, threads);
    // The arrays these replace unmap beside the steps left.
    auto old_vertices_freed = free_beside(std::move(data.addr_vertices));
    auto old_points_freed = free_beside(std::move(data.addr_points));
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
    if (have_osm)
        data.addr_osm_ids = first_set_per_run(data.addr_osm_ids, order, starts, uint64_t(0), threads, osm_buffer.get());
    if (have_postcodes)
        data.addr_postcode_ids =
            first_set_per_run(data.addr_postcode_ids, order, starts, NO_DATA, threads, postcode_buffer.get());
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
    // The arrays the sorted ways and nodes move into fault in beside the
    // sort, sized for every way: the dedup drops a handful.
    auto node_buffer = vector_beside<NodeCoord>(parallel_sum<size_t>(n, [&](size_t i) {
        return size_t(ways[i].node_count);
    }, threads));
    auto way_buffer = vector_beside<WayHeader>(n);
    auto osm_buffer = vector_beside<int64_t>(have_osm ? n : 0);

    // The sort orders keys holding the fields that decide nearly every
    // comparison, rather than indices into the planet's ways and nodes.
    struct WayKey {
        uint32_t name_id;
        uint16_t node_count;
        uint32_t lat_bits, lng_bits, index;  // of the first node; 0 for none
    };
    auto key_of = [&](size_t i) {
        const auto& w = ways[i];
        WayKey k{w.name_id, w.node_count, 0, 0, static_cast<uint32_t>(i)};
        if (w.node_count > 0) {
            k.lat_bits = float_bits(nodes[w.node_offset].lat);
            k.lng_bits = float_bits(nodes[w.node_offset].lng);
        }
        return k;
    };
    auto key_less = [&](const WayKey& a, const WayKey& b) {
        if (a.name_id != b.name_id) return a.name_id < b.name_id;
        if (a.node_count != b.node_count) return a.node_count < b.node_count;
        if (a.lat_bits != b.lat_bits) return a.lat_bits < b.lat_bits;
        if (a.lng_bits != b.lng_bits) return a.lng_bits < b.lng_bits;
        return way_less(a.index, b.index);
    };
    std::vector<uint32_t> order =
        take_order("ways", parallel_sort_keys(n, key_of, key_less, tie_matters, threads));

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
        [&](size_t k) { return ways[order[starts[k]]].node_offset; }, threads, {}, node_buffer.get());
    std::vector<WayHeader> new_ways = way_buffer.get();
    new_ways.resize(kept);
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
    if (have_osm) {
        std::vector<int64_t> osm = osm_buffer.get();
        osm.resize(kept);
        data.way_osm_ids = first_set_per_run(data.way_osm_ids, order, starts, int64_t(0), threads, std::move(osm));
    }
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
    // The arrays these replace unmap beside the steps left.
    auto old_ways_freed = free_beside(std::move(data.ways));
    auto old_nodes_freed = free_beside(std::move(data.street_nodes));
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
inline void reorder_interps(ParsedData& data, unsigned threads = 0) {
    if (data.interp_ways.empty()) return;

    const size_t n = data.interp_ways.size();
    const auto& interps = data.interp_ways;
    const auto& nodes = data.interp_nodes;
    const bool have_interp_osm = (data.interp_osm_ids.size() == n);
    const bool have_interp_pc = (data.interp_postcode_ids.size() == n);
    // The sort orders keys: when tied TIGER duplicates force the serial
    // std::sort, its comparisons then read adjacent keys rather than chase
    // indices across the planet's interps.
    struct InterpKey {
        uint32_t street_id, start_number, end_number, node_offset;
        uint16_t node_count;
        uint8_t interpolation;
        uint32_t index;
    };
    auto key_of = [&](size_t i) {
        const auto& iw = interps[i];
        return InterpKey{iw.street_id, iw.start_number, iw.end_number, iw.node_offset,
                         iw.node_count, iw.interpolation, static_cast<uint32_t>(i)};
    };
    auto interp_less = [&](const InterpKey& ia, const InterpKey& ib) {
        if (ia.street_id != ib.street_id) return ia.street_id < ib.street_id;
        if (ia.start_number != ib.start_number) return ia.start_number < ib.start_number;
        if (ia.end_number != ib.end_number) return ia.end_number < ib.end_number;
        if (ia.interpolation != ib.interpolation) return ia.interpolation < ib.interpolation;
        uint16_t nc = std::min(ia.node_count, ib.node_count);
        for (uint16_t j = 0; j < nc; j++) {
            uint32_t la = float_bits(nodes[ia.node_offset + j].lat);
            uint32_t lb = float_bits(nodes[ib.node_offset + j].lat);
            if (la != lb) return la < lb;
            uint32_t ga = float_bits(nodes[ia.node_offset + j].lng);
            uint32_t gb = float_bits(nodes[ib.node_offset + j].lng);
            if (ga != gb) return ga < gb;
        }
        if (ia.node_count != ib.node_count) return ia.node_count < ib.node_count;
        if (have_interp_osm) return data.interp_osm_ids[ia.index] < data.interp_osm_ids[ib.index];
        return false;
    };
    // Interps that tie are duplicates the dedup below folds into the first,
    // which keeps its own header and postcode: their order shows only when
    // those differ. (Duplicate TIGER rows can carry different postcodes;
    // then the surviving one is whichever std::sort put first.)
    auto tie_matters = [&](uint32_t a, uint32_t b) {
        constexpr size_t kAfterOffset = offsetof(InterpWay, node_count);
        return std::memcmp(reinterpret_cast<const char*>(&interps[a]) + kAfterOffset,
                           reinterpret_cast<const char*>(&interps[b]) + kAfterOffset,
                           sizeof(InterpWay) - kAfterOffset) != 0 ||
               (have_interp_pc && data.interp_postcode_ids[a] != data.interp_postcode_ids[b]);
    };
    std::vector<uint32_t> order =
        take_order("interps", parallel_sort_keys(n, key_of, interp_less, tie_matters, threads));

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
    std::vector<uint32_t> starts = run_starts(n, [&](size_t i, size_t j) {
        const auto& a = interps[order[i]];
        const auto& b = interps[order[j]];
        return a.street_id == b.street_id && a.start_number == b.start_number &&
               a.end_number == b.end_number && a.interpolation == b.interpolation &&
               a.node_count == b.node_count &&
               std::memcmp(nodes.data() + a.node_offset, nodes.data() + b.node_offset,
                           a.node_count * sizeof(NodeCoord)) == 0;
    }, threads);
    const size_t kept = starts.size();

    Repacked<NodeCoord> repacked = repack(kept, nodes,
        [&](size_t k) { return size_t(interps[order[starts[k]]].node_count); },
        [&](size_t k) { return interps[order[starts[k]]].node_offset; }, threads);
    std::vector<InterpWay> new_interps(kept);
    std::vector<uint64_t> new_osm(have_interp_osm ? kept : 0);
    std::vector<uint32_t> new_pc(have_interp_pc ? kept : 0);
    parallel_for(kept, [&](size_t b, size_t e, unsigned) {
        for (size_t k = b; k < e; k++) {
            uint32_t oi = order[starts[k]];
            new_interps[k] = interps[oi];
            new_interps[k].node_offset = static_cast<uint32_t>(repacked.at[k]);
            if (have_interp_osm) new_osm[k] = data.interp_osm_ids[oi];
            if (have_interp_pc) new_pc[k] = data.interp_postcode_ids[oi];
        }
    }, threads);
    const std::vector<uint32_t> old_to_new = renumber_runs(order, starts, threads);
    order = {};
    starts = {};
    if (have_interp_osm) data.interp_osm_ids = std::move(new_osm);
    if (have_interp_pc) data.interp_postcode_ids = std::move(new_pc);
    data.interp_ways = std::move(new_interps);
    data.interp_nodes = std::move(repacked.items);
    renumber_cell_pairs(data.sorted_interp_cells, [&](uint32_t id) { return old_to_new[id]; }, threads);
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
              << " (" << (n - kept) << " duplicates removed)" << std::endl;
}

// Moves admin polygon order[i] to slot i, with its vertices repacked in the
// new order and its osm id in lockstep; returns old_to_new. References to
// polygons elsewhere are the caller's to remap.
inline std::vector<uint32_t> permute_admin_polygons(ParsedData& data, const std::vector<uint32_t>& order,
                                                    unsigned threads) {
    const size_t n = order.size();
    const auto& polys = data.admin_polygons;
    std::vector<uint32_t> old_to_new = invert(order, threads);
    Repacked<NodeCoord> repacked = repack(n, data.admin_vertices,
        [&](size_t i) { return size_t(polys[order[i]].vertex_count); },
        [&](size_t i) { return polys[order[i]].vertex_offset; }, threads);
    std::vector<AdminPolygon> new_polys = gather(polys, order, threads);
    parallel_for(n, [&](size_t b, size_t e, unsigned) {
        for (size_t i = b; i < e; i++) new_polys[i].vertex_offset = static_cast<uint32_t>(repacked.at[i]);
    }, threads);
    if (data.admin_osm_ids.size() == n) data.admin_osm_ids = gather(data.admin_osm_ids, order, threads);
    data.admin_polygons = std::move(new_polys);
    data.admin_vertices = std::move(repacked.items);
    return old_to_new;
}

// Sorts admin polygons by (name, level, country, vertex_count) and reorders
// their vertices.
inline void reorder_admin_polygons(ParsedData& data, unsigned threads = 0) {
    if (data.admin_polygons.empty()) return;

    const size_t n = data.admin_polygons.size();
    const auto& polys = data.admin_polygons;
    const auto& vertices = data.admin_vertices;
    const bool have_osm = data.admin_osm_ids.size() == n;
    auto admin_less = [&](uint32_t a, uint32_t b) {
        const auto& pa = polys[a];
        const auto& pb = polys[b];
        if (pa.name_id != pb.name_id) return pa.name_id < pb.name_id;
        if (pa.admin_level != pb.admin_level) return pa.admin_level < pb.admin_level;
        if (pa.country_code != pb.country_code) return pa.country_code < pb.country_code;
        if (pa.vertex_count != pb.vertex_count) return pa.vertex_count < pb.vertex_count;
        // Compare first few vertices for tiebreaking
        uint32_t nc = std::min(pa.vertex_count, pb.vertex_count);
        nc = std::min(nc, 20u); // limit comparison depth
        for (uint32_t j = 0; j < nc; j++) {
            uint32_t la = float_bits(vertices[pa.vertex_offset + j].lat);
            uint32_t lb = float_bits(vertices[pb.vertex_offset + j].lat);
            if (la != lb) return la < lb;
            uint32_t ga = float_bits(vertices[pa.vertex_offset + j].lng);
            uint32_t gb = float_bits(vertices[pb.vertex_offset + j].lng);
            if (ga != gb) return ga < gb;
        }
        if (have_osm) return data.admin_osm_ids[a] < data.admin_osm_ids[b];
        return false;
    };
    // Every polygon keeps its own slot and references point at it, so any
    // tie's order shows.
    const std::vector<uint32_t> order =
        take_order("admin polygons",
                   parallel_sort_indices(n, admin_less, [](uint32_t, uint32_t) { return true; }, threads));
    // No dedup happens in this sort — just a permutation — so
    // admin_osm_ids keeps its size.
    const std::vector<uint32_t> old_to_new = permute_admin_polygons(data, order, threads);

    auto remap_each = [&](size_t count, auto&& remap_one) {
        parallel_for(count, [&](size_t b, size_t e, unsigned) {
            for (size_t i = b; i < e; i++) remap_one(i);
        }, threads);
    };
    // Remap place-node and POI parent_poly_id references
    remap_each(data.place_nodes.size(), [&](size_t i) {
        auto& pn = data.place_nodes[i];
        if (pn.parent_poly_id != 0xFFFFFFFFu && pn.parent_poly_id < n) pn.parent_poly_id = old_to_new[pn.parent_poly_id];
    });
    remap_each(data.poi_records.size(), [&](size_t i) {
        auto& pr = data.poi_records[i];
        if (pr.parent_poly_id != 0xFFFFFFFFu && pr.parent_poly_id < n) pr.parent_poly_id = old_to_new[pr.parent_poly_id];
    });
    // Remap admin_parent_ids (both indices and values)
    if (data.admin_parent_ids.size() == n) {
        std::vector<uint32_t> new_ap = gather(data.admin_parent_ids, order, threads);
        remap_each(n, [&](size_t i) {
            new_ap[i] = (new_ap[i] != NO_DATA && new_ap[i] < n) ? old_to_new[new_ap[i]] : NO_DATA;
        });
        data.admin_parent_ids = std::move(new_ap);
    }
    // Remap way_parent_ids (values only)
    remap_each(data.way_parent_ids.size(), [&](size_t i) {
        uint32_t& pid = data.way_parent_ids[i];
        if (pid != NO_DATA && pid < n) pid = old_to_new[pid];
    });
    // Remap admin cell entries
    const auto cells = cell_lists(data.cell_to_admin, threads);
    remap_each(cells.size(), [&](size_t c) {
        auto& ids = *cells[c].second;
        for (auto& id : ids) {
            uint32_t flags = id & INTERIOR_FLAG;
            uint32_t masked = id & ID_MASK;
            if (masked < n) id = old_to_new[masked] | flags;
        }
        std::sort(ids.begin(), ids.end());
    });
    std::cerr << "  Admin polygons sorted: " << n << std::endl;
}

// Sorts POI records by (category, tier, name_id, lat_bits, lng_bits), reorders
// their vertices and computes their importance.
inline void reorder_pois(ParsedData& data, std::vector<float>& poi_elevations,
                         std::vector<uint32_t>& poi_qids,
                         const std::vector<QidSitelinks>& sitelinks_data, unsigned threads = 0) {
    auto lookup_sitelinks = [&sitelinks_data](uint32_t qid) -> uint16_t {
        auto it = std::lower_bound(sitelinks_data.begin(), sitelinks_data.end(), qid,
            [](const QidSitelinks& entry, uint32_t q) { return entry.qid < q; });
        if (it != sitelinks_data.end() && it->qid == qid) return it->count;
        return 0;
    };

    if (data.poi_records.empty()) return;

    const size_t n = data.poi_records.size();
    const auto& pois = data.poi_records;
    const auto& vertices = data.poi_vertices;
    const bool have_osm = data.poi_osm_ids.size() == n;
    const bool have_elevations = (poi_elevations.size() == n);
    const bool have_qids = (poi_qids.size() == n);
    auto poi_less = [&](uint32_t a, uint32_t b) {
        const auto& pa = pois[a];
        const auto& pb = pois[b];
        if (pa.category != pb.category) return pa.category < pb.category;
        if (pa.tier != pb.tier) return pa.tier < pb.tier;
        if (pa.name_id != pb.name_id) return pa.name_id < pb.name_id;
        uint32_t la = float_bits(pa.lat), lb = float_bits(pb.lat);
        if (la != lb) return la < lb;
        uint32_t ga = float_bits(pa.lng), gb = float_bits(pb.lng);
        if (ga != gb) return ga < gb;
        if (have_osm) return data.poi_osm_ids[a] < data.poi_osm_ids[b];
        return a < b;
    };
    // POIs that tie share an osm id. Identical in everything the dedup below
    // keeps of a record (its bytes bar the vertex offset, vertices,
    // elevation, qid), they fold into one POI whichever came first.
    auto tie_matters = [&](uint32_t a, uint32_t b) {
        PoiRecord pa = pois[a], pb = pois[b];
        bool va = pa.vertex_count > 0 && pa.vertex_offset != NO_DATA;
        bool vb = pb.vertex_count > 0 && pb.vertex_offset != NO_DATA;
        uint32_t from_a = pa.vertex_offset, from_b = pb.vertex_offset;
        pa.vertex_offset = pb.vertex_offset = 0;
        if (va != vb || std::memcmp(&pa, &pb, sizeof(PoiRecord)) != 0) return true;
        if (va && std::memcmp(&vertices[from_a], &vertices[from_b], pa.vertex_count * sizeof(NodeCoord)) != 0)
            return true;
        if (have_elevations && std::memcmp(&poi_elevations[a], &poi_elevations[b], sizeof(float)) != 0) return true;
        return have_qids && poi_qids[a] != poi_qids[b];
    };
    // The arrays the sorted POIs move into fault in beside the sort, sized
    // for every POI: the dedup drops few.
    auto has_polygon = [](const PoiRecord& p) { return p.vertex_count > 0 && p.vertex_offset != NO_DATA; };
    auto vertex_buffer = vector_beside<NodeCoord>(parallel_sum<size_t>(n, [&](size_t i) {
        return has_polygon(pois[i]) ? size_t(pois[i].vertex_count) : 0;
    }, threads));
    auto poi_buffer = vector_beside<PoiRecord>(n);
    auto elevation_buffer = vector_beside<float>(have_elevations ? n : 0);
    auto qid_buffer = vector_beside<uint32_t>(have_qids ? n : 0);
    auto osm_buffer = vector_beside<uint64_t>(have_osm ? n : 0);

    // The sort orders keys holding the fields that decide nearly every
    // comparison, rather than indices into the planet's poi_records.
    struct PoiKey {
        uint8_t category, tier;
        uint32_t name_id, lat_bits, lng_bits, index;
    };
    auto key_of = [&](size_t i) {
        const auto& p = pois[i];
        return PoiKey{p.category, p.tier, p.name_id, float_bits(p.lat), float_bits(p.lng), static_cast<uint32_t>(i)};
    };
    auto key_less = [&](const PoiKey& a, const PoiKey& b) {
        if (a.category != b.category) return a.category < b.category;
        if (a.tier != b.tier) return a.tier < b.tier;
        if (a.name_id != b.name_id) return a.name_id < b.name_id;
        if (a.lat_bits != b.lat_bits) return a.lat_bits < b.lat_bits;
        if (a.lng_bits != b.lng_bits) return a.lng_bits < b.lng_bits;
        return poi_less(a.index, b.index);
    };
    std::vector<uint32_t> order =
        take_order("POI records", parallel_sort_keys(n, key_of, key_less, tie_matters, threads));

    // Dedup consecutive POIs equal in everything but their vertices.
    std::vector<uint32_t> starts = run_starts(n, [&](size_t i, size_t j) {
        const auto& prev = pois[order[i]];
        const auto& p = pois[order[j]];
        return prev.category == p.category && prev.tier == p.tier &&
               prev.name_id == p.name_id &&
               prev.lat == p.lat && prev.lng == p.lng &&
               prev.vertex_count == p.vertex_count && prev.flags == p.flags;
    }, threads);
    const size_t kept = starts.size();

    // Reorder records + vertices + elevations + qids. Point POIs too take the
    // running vertex offset.
    Repacked<NodeCoord> repacked = repack(kept, vertices,
        [&](size_t k) { const auto& p = pois[order[starts[k]]]; return has_polygon(p) ? size_t(p.vertex_count) : 0; },
        [&](size_t k) { return pois[order[starts[k]]].vertex_offset; }, threads, {}, vertex_buffer.get());
    std::vector<PoiRecord> new_pois = poi_buffer.get();
    std::vector<float> new_poi_elevations = elevation_buffer.get();
    std::vector<uint32_t> new_poi_qids = qid_buffer.get();
    new_pois.resize(kept);
    new_poi_elevations.resize(have_elevations ? kept : 0);
    new_poi_qids.resize(have_qids ? kept : 0);
    parallel_for(kept, [&](size_t b, size_t e, unsigned) {
        for (size_t k = b; k < e; k++) {
            uint32_t oi = order[starts[k]];
            new_pois[k] = pois[oi];
            new_pois[k].vertex_offset = static_cast<uint32_t>(repacked.at[k]);
            if (have_elevations) new_poi_elevations[k] = poi_elevations[oi];
            if (have_qids) new_poi_qids[k] = poi_qids[oi];
        }
    }, threads);
    // Reorder + dedup poi_osm_ids in lockstep: each kept POI takes the
    // first non-zero osm id of its duplicates in SORT order.
    if (have_osm) {
        std::vector<uint64_t> osm = osm_buffer.get();
        osm.resize(kept);
        data.poi_osm_ids = first_set_per_run(data.poi_osm_ids, order, starts, uint64_t(0), threads, std::move(osm));
    }
    const std::vector<uint32_t> old_to_new = renumber_runs(order, starts, threads);
    order = {};
    starts = {};
    // The arrays these replace unmap beside the steps left.
    auto old_pois_freed = free_beside(std::move(data.poi_records));
    auto old_vertices_freed = free_beside(std::move(data.poi_vertices));
    data.poi_records = std::move(new_pois);
    data.poi_vertices = std::move(repacked.items);
    if (have_elevations) poi_elevations = std::move(new_poi_elevations);
    if (have_qids) poi_qids = std::move(new_poi_qids);

    // Remap sorted_poi_cells IDs (preserving INTERIOR_FLAG)
    renumber_cell_pairs(data.sorted_poi_cells, [&](uint32_t id) {
        return old_to_new[id & ID_MASK] | (id & INTERIOR_FLAG);
    }, threads);
    data.cell_to_pois.clear();
    std::cerr << "  POI records sorted: " << n << " (" << (n - kept) << " duplicates removed)" << std::endl;

    // Compute importance
    parallel_for(data.poi_records.size(), [&](size_t first, size_t last, unsigned) {
        for (size_t i = first; i < last; i++) {
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
    }, threads);
    std::cerr << "  POI importance computed" << std::endl;
}

// Sorts place nodes by (place_type, name_id, lat_bits, lng_bits).
inline void reorder_place_nodes(ParsedData& data, unsigned threads = 0) {
    if (data.place_nodes.empty()) return;

    const size_t n = data.place_nodes.size();
    const auto& places = data.place_nodes;
    const bool have_osm = data.place_osm_ids.size() == n;
    auto place_less = [&](uint32_t a, uint32_t b) {
        const auto& pa = places[a];
        const auto& pb = places[b];
        if (pa.place_type != pb.place_type) return pa.place_type < pb.place_type;
        if (pa.name_id != pb.name_id) return pa.name_id < pb.name_id;
        uint32_t la = float_bits(pa.lat), lb = float_bits(pb.lat);
        if (la != lb) return la < lb;
        uint32_t ga = float_bits(pa.lng), gb = float_bits(pb.lng);
        if (ga != gb) return ga < gb;
        if (have_osm) return data.place_osm_ids[a] < data.place_osm_ids[b];
        return false;
    };
    const std::vector<uint32_t> order =
        take_order("place nodes",
                   parallel_sort_indices(n, place_less, [](uint32_t, uint32_t) { return true; }, threads));
    // Reorder place_osm_ids in lockstep
    if (have_osm) data.place_osm_ids = gather(data.place_osm_ids, order, threads);
    data.place_nodes = gather(places, order, threads);
    const std::vector<uint32_t> old_to_new = invert(order, threads);
    renumber_cell_pairs(data.sorted_place_cells, [&](uint32_t id) { return old_to_new[id]; }, threads);
    std::cerr << "  Place nodes sorted: " << n << std::endl;
}
