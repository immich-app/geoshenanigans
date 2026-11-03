// Passes that look around each item's cell: the items grouped by cell, and
// a walk over them that gathers each cell's lookup once.
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>

#include <s2/s2cell_id.h>

#include "parallel.h"
#include "types.h"

// Items 0..n-1 as (cell_of(i), i) pairs sorted by cell then item, so a pass
// that looks around each item's cell can walk a cell's items together and
// share the lookup. cell_of returns 0 (never a valid cell id) to drop an item.
template <typename CellOf>
std::vector<CellItemPair> items_by_cell(size_t n, unsigned int threads, CellOf cell_of) {
    std::vector<CellItemPair> order(n);
    parallel_for(n, [&](size_t begin, size_t end, unsigned) {
        for (size_t i = begin; i < end; i++) order[i] = {cell_of(i), static_cast<uint32_t>(i)};
    }, threads);
    parallel_sort(order.begin(), order.end(), cell_item_less, threads);
    order.erase(order.begin(), std::find_if(order.begin(), order.end(),
                                            [](const CellItemPair& p) { return p.cell_id != 0; }));
    return order;
}

// Items per chunk of a cell walk: small enough to spread dense cells over the
// workers, large enough that chunk edges rarely repeat a cell's lookup.
inline constexpr size_t kCellWalkGrain = 1024;

// Items per chunk when a pass hands items to whichever worker is free: one
// shared counter bump per chunk instead of per item (the planet's 116M POIs
// are mostly a single cell lookup each), small enough that a chunk of costly
// items ends quickly.
inline constexpr size_t kItemGrain = 64;

// Walks `order` (from items_by_cell) on every core: on_cell(cell, state) runs
// on entering a cell, then on_item(item, state) for each of its items. Each
// worker owns one State. A cell split across chunks is entered once per chunk,
// so on_cell must rebuild the state from the cell alone.
template <typename State, typename OnCell, typename OnItem>
void walk_items_by_cell(const std::vector<CellItemPair>& order, unsigned int threads,
                OnCell on_cell, OnItem on_item) {
    std::vector<State> states(std::max(1u, threads));
    parallel_for_dynamic(order.size(), kCellWalkGrain, [&](size_t begin, size_t end, unsigned worker) {
        State& state = states[worker];
        for (size_t k = begin; k < end; k++) {
            if (k == begin || order[k].cell_id != order[k - 1].cell_id)
                on_cell(S2CellId(order[k].cell_id), state);
            on_item(order[k].item_id, state);
        }
    }, threads);
}
