#pragma once

#include <algorithm>
#include <cstdint>
#include <unordered_map>
#include <vector>

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

template <typename RemapOne>
void remap_cell_pairs(std::vector<CellItemPair>& pairs, RemapOne remap_one) {
    for (auto& p : pairs) remap_one(p.item_id);
    std::sort(pairs.begin(), pairs.end(), cell_item_less);
}
