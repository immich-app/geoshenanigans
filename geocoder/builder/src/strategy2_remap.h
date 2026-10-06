#pragma once

#include <cstdint>
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
