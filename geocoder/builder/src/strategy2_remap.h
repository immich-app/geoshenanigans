#pragma once

#include <algorithm>
#include <cstdint>
#include <unordered_map>
#include <vector>

#include "parallel.h"
#include "types.h"

// Strategy-2 reference-site remap helpers. These were previously
// redefined as identical local lambdas in every apply_strategy2_*
// function; hoisted here verbatim so all call sites share one
// definition. `remap` maps old record index → new (post-reorder) index.
//
// remap_index: plain index field. Leaves NO_DATA and any value past the
// remap table untouched (same out-of-range guard the lambdas used).
inline void remap_index(uint32_t& v, const std::vector<uint32_t>& remap) {
    if (v != NO_DATA && v < remap.size()) v = remap[v];
}

// remap_index_flagged: index field that may carry the high-bit
// INTERIOR_FLAG (cell-to-record entries set during the S2-covering
// pass). Mask the flag off, remap the index, then re-OR the flag —
// mirroring the deterministic-sort pass's handling of the same arrays.
inline void remap_index_flagged(uint32_t& v, const std::vector<uint32_t>& remap) {
    uint32_t flag = v & INTERIOR_FLAG;
    uint32_t idx  = v & ~INTERIOR_FLAG;
    if (idx != (NO_DATA & ~INTERIOR_FLAG) && idx < remap.size())
        v = remap[idx] | flag;
}

// Moves values[i] to slot remap[i] of a new n_new-long array; the slots no
// value lands in hold `fill`. remap is injective (strategy-2 slots).
template <class T>
void reorder_by_remap(std::vector<T>& values, const std::vector<uint32_t>& remap, size_t n_new, const T& fill) {
    std::vector<T> out(n_new, fill);
    parallel_for(std::min(values.size(), remap.size()), [&](size_t begin, size_t end, unsigned) {
        for (size_t i = begin; i < end; i++) out[remap[i]] = values[i];
    });
    values = std::move(out);
}

// Copies each record's nodes into a new array in record order and points the
// record at them, so offsets ascend with record ids (the patch tool replays
// node merges sequentially). Records without nodes keep their offset; records
// whose nodes run past the array come out empty.
template <class Record>
void repack_nodes(std::vector<Record>& records, std::vector<NodeCoord>& nodes) {
    auto in_range = [&](const Record& r) {
        return static_cast<size_t>(r.node_offset) + r.node_count <= nodes.size();
    };
    std::vector<NodeCoord> packed;
    parallel_prefix_fill(records.size(),
        [&](size_t k) -> size_t { return in_range(records[k]) ? records[k].node_count : 0; },
        [&](size_t total) { packed.resize(total); },
        [&](size_t k, size_t offset) -> size_t {
            Record& r = records[k];
            if (r.node_count == 0) return 0;
            if (!in_range(r)) {
                r.node_offset = 0;
                r.node_count = 0;
                return 0;
            }
            std::copy(nodes.begin() + r.node_offset, nodes.begin() + r.node_offset + r.node_count,
                      packed.begin() + offset);
            r.node_offset = static_cast<uint32_t>(offset);
            return r.node_count;
        });
    nodes = std::move(packed);
}

// Remap every id of a cell → ids table and restore its canonical order (each
// list sorted by raw value, flag bit included; pair tables by cell_item_less),
// as the deterministic-ordering pass leaves them. The entry writers emit lists
// as stored and diff/patch rebuild them sorted, so a list left in pre-remap
// order costs a correction in every patch.
template <typename RemapOne>
void remap_cell_map(std::unordered_map<uint64_t, std::vector<uint32_t>>& cell_map, RemapOne remap_one) {
    for (auto& [cell, ids] : cell_map) {
        for (auto& id : ids) remap_one(id);
        std::sort(ids.begin(), ids.end());
    }
}

// A remap keeps every cell_id, so a table already grouped by cell only needs
// each cell's run re-sorted: whole runs per worker, no global sort. remap_one
// runs on several threads at once.
template <typename RemapOne>
void remap_cell_pairs(std::vector<CellItemPair>& pairs, RemapOne remap_one) {
    const unsigned threads = parallel_threads();
    std::vector<char> ungrouped(threads, 0);
    parallel_for(pairs.size(), [&](size_t begin, size_t end, unsigned w) {
        for (size_t i = std::max<size_t>(begin, 1); i < end; i++)
            if (pairs[i].cell_id < pairs[i - 1].cell_id) { ungrouped[w] = 1; break; }
    }, threads);
    if (std::find(ungrouped.begin(), ungrouped.end(), 1) != ungrouped.end()) {
        for (auto& p : pairs) remap_one(p.item_id);
        parallel_sort(pairs.begin(), pairs.end(), cell_item_less);
        return;
    }

    parallel_for_runs(pairs.size(), same_cell(pairs), [&](size_t begin, size_t end, unsigned) {
        for (size_t i = begin; i < end; i++) remap_one(pairs[i].item_id);
        for (size_t run = begin; run < end; ) {
            size_t run_end = run + 1;
            while (run_end < end && pairs[run_end].cell_id == pairs[run].cell_id) run_end++;
            if (run_end - run > 1)
                std::sort(pairs.begin() + run, pairs.begin() + run_end, cell_item_less);
            run = run_end;
        }
    }, threads);
}
