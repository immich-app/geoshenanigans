// The cell ids a cell file gained and lost between two builds.
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <vector>

#include "parallel.h"

struct CellIdDiff {
    std::vector<uint64_t> added, removed;
};

// Whether the u64 ids leading each `stride`-byte record never decrease.
inline bool cell_ids_sorted(const char* data, size_t n, size_t stride, unsigned threads = 0) {
    auto id = [&](size_t i) { uint64_t v; memcpy(&v, data + i * stride, 8); return v; };
    std::vector<char> ok(std::max<size_t>(1, std::min<size_t>(threads ? threads : parallel_threads(), n)), 1);
    parallel_for(n > 0 ? n - 1 : 0, [&](size_t b, size_t e, unsigned w) {
        for (size_t i = b; i < e && ok[w]; i++) ok[w] = id(i) <= id(i + 1);
    }, threads);
    return std::all_of(ok.begin(), ok.end(), [](char c) { return c != 0; });
}

// The ids new holds and old doesn't (added) and the reverse (removed), as
// multisets in ascending order: what std::set_difference over sorted copies
// of both id lists gives. Cell files are written sorted, so the mapped files
// are merge-walked as they are (planet geo_cells: no 3.2 GB copy per side to
// sort); a list out of order still gets the copies.
inline CellIdDiff diff_cell_ids(const char* old_data, size_t old_n, const char* new_data, size_t new_n,
                                size_t stride, unsigned threads = 0) {
    CellIdDiff out;
    if (!cell_ids_sorted(old_data, old_n, stride, threads) || !cell_ids_sorted(new_data, new_n, stride, threads)) {
        std::vector<uint64_t> old_ids(old_n), new_ids(new_n);
        for (size_t i = 0; i < old_n; i++) memcpy(&old_ids[i], old_data + i * stride, 8);
        for (size_t i = 0; i < new_n; i++) memcpy(&new_ids[i], new_data + i * stride, 8);
        std::sort(old_ids.begin(), old_ids.end());
        std::sort(new_ids.begin(), new_ids.end());
        std::set_difference(new_ids.begin(), new_ids.end(), old_ids.begin(), old_ids.end(), std::back_inserter(out.added));
        std::set_difference(old_ids.begin(), old_ids.end(), new_ids.begin(), new_ids.end(), std::back_inserter(out.removed));
        return out;
    }
    auto id = [&](const char* data, size_t i) { uint64_t v; memcpy(&v, data + i * stride, 8); return v; };
    size_t o = 0, n = 0;
    while (o < old_n && n < new_n) {
        uint64_t a = id(old_data, o), b = id(new_data, n);
        if (a < b) { out.removed.push_back(a); o++; }
        else if (b < a) { out.added.push_back(b); n++; }
        else { o++; n++; }
    }
    for (; o < old_n; o++) out.removed.push_back(id(old_data, o));
    for (; n < new_n; n++) out.added.push_back(id(new_data, n));
    return out;
}
