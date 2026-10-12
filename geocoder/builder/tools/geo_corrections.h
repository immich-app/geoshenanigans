// The street / addr / interp entry corrections (GEO_ENTRY_DELTA_MARKER) of a
// diff: the patcher derives each geo cell's list from its old one through
// the file's id remap, and every cell where that differs from the new list
// travels as a delta.
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

#include "cell_id_diff.h"
#include "parallel.h"
#include "patch_format.h"

// A geo_cells.bin: 20-byte records, cell id first, then the street, addr
// and interp entry offsets.
struct GeoCells {
    const char* data;
    size_t n;
};

struct GeoEntryCorrections {
    std::vector<char> section;  // marker, file id, cell count, deltas
    uint32_t cells = 0;
};

// Merge-walks the effective old cells (old - removed + added, each added
// cell with an empty list) against the new ones, both sorted by cell id.
// geo_off_pos: where the file's entry offset sits in a geo cell record.
//
// With both cell files strictly ascending (as the builder writes them) and
// `added` sorted, the walk handles each cell id range on its own: the cells
// split into ranges at new cell ids, walked on every core, and the ranges'
// deltas concatenate to the one walk's bytes (planet: ~400M cells a file).
inline GeoEntryCorrections geo_entry_corrections(uint32_t file_id, GeoCells old_geo, GeoCells new_geo,
                                                 const std::vector<uint64_t>& added,
                                                 const std::vector<uint64_t>& removed,
                                                 ByteSpan old_entries, ByteSpan new_entries, size_t geo_off_pos,
                                                 const std::vector<uint32_t>& id_rm, unsigned threads = 0) {
    constexpr uint32_t NO_DATA = 0xFFFFFFFFu;
    // Remap IDs in-place using vector remap (O(1) per ID)
    auto remap_ids_vec = [](std::vector<uint32_t>& ids, const std::vector<uint32_t>& rm) {
        for (auto& id : ids)
            if (id < rm.size() && rm[id] != NO_DATA) id = rm[id];
        std::sort(ids.begin(), ids.end());
    };
    std::vector<uint64_t> removed_sorted(removed);
    std::sort(removed_sorted.begin(), removed_sorted.end());
    auto is_removed = [&](uint64_t id) {
        return std::binary_search(removed_sorted.begin(), removed_sorted.end(), id);
    };
    auto cell_id = [](const GeoCells& g, size_t i) { uint64_t v; memcpy(&v, g.data + i * 20, 8); return v; };

    // The walk over old cells [oi, o_end), new cells [ni, n_end) and added
    // cells [ai, a_end), appending to buf.
    auto walk = [&](size_t oi, size_t o_end, size_t ni, size_t n_end, size_t ai, size_t a_end,
                    std::vector<char>& buf, uint32_t& dc) {
        std::vector<uint32_t> d_ids, n_ids;
        while (oi < o_end || ni < n_end || ai < a_end) {
            // Determine next effective-old cell_id
            uint64_t eff_old = UINT64_MAX;
            bool is_added = false;
            // Skip removed old cells
            while (oi < o_end) {
                eff_old = cell_id(old_geo, oi);
                if (!is_removed(eff_old)) break;
                oi++;
                eff_old = UINT64_MAX;
            }
            // Check if an added cell comes before
            if (ai < a_end && added[ai] < eff_old) {
                eff_old = added[ai];
                is_added = true;
            }

            uint64_t n_cid = UINT64_MAX;
            if (ni < n_end) n_cid = cell_id(new_geo, ni);

            if (eff_old == UINT64_MAX && n_cid == UINT64_MAX) break;

            if (eff_old <= n_cid) {
                // The effective-old cell's derived list (an added cell's is empty)
                d_ids.clear();
                if (!is_added) {
                    uint32_t off; memcpy(&off, old_geo.data + oi * 20 + geo_off_pos, 4);
                    read_entry_list(old_entries, off, d_ids);
                    remap_ids_vec(d_ids, id_rm);
                    oi++;
                } else {
                    ai++;
                }
                if (eff_old == n_cid) {
                    // Cell in both — compare with the new list
                    uint32_t n_off; memcpy(&n_off, new_geo.data + ni * 20 + geo_off_pos, 4);
                    read_entry_list(new_entries, n_off, n_ids);
                    if (d_ids != n_ids) {
                        append_geo_list_delta(buf, eff_old, d_ids, n_ids);
                        dc++;
                    }
                    ni++;
                } else if (!d_ids.empty()) {
                    // Cell only in effective-old — correct to empty
                    append_geo_list_delta(buf, eff_old, d_ids, {});
                    dc++;
                }
            } else {
                // Cell only in new — emit new entries
                uint32_t n_off; memcpy(&n_off, new_geo.data + ni * 20 + geo_off_pos, 4);
                read_entry_list(new_entries, n_off, n_ids);
                if (!n_ids.empty()) {
                    append_geo_list_delta(buf, n_cid, {}, n_ids);
                    dc++;
                }
                ni++;
            }
        }
    };

    std::vector<char> buf(12, 0);
    uint32_t dc = 0;
    size_t chunks = std::min<size_t>(new_geo.n, 8 * size_t(threads ? threads : parallel_threads()));
    // UINT64_MAX is the walk's "no cell", so no range may hold one.
    bool ranges = chunks > 1 && cell_ids_ascending(old_geo.data, old_geo.n, 20, threads) &&
                  cell_ids_ascending(new_geo.data, new_geo.n, 20, threads) &&
                  std::is_sorted(added.begin(), added.end()) &&
                  (old_geo.n == 0 || cell_id(old_geo, old_geo.n - 1) != UINT64_MAX) &&
                  cell_id(new_geo, new_geo.n - 1) != UINT64_MAX &&
                  (added.empty() || added.back() != UINT64_MAX);
    if (!ranges) {
        walk(0, old_geo.n, 0, new_geo.n, 0, added.size(), buf, dc);
    } else {
        // Range k: new cells [new_at(k), new_at(k + 1)), and the old and
        // added cells from its first new cell id up to the next range's.
        auto new_at = [&](size_t k) { return new_geo.n * k / chunks; };
        auto old_bound = [&](size_t k) -> size_t {
            if (k == 0) return 0;
            if (k == chunks) return old_geo.n;
            uint64_t c = cell_id(new_geo, new_at(k));
            size_t lo = 0, hi = old_geo.n;
            while (lo < hi) {
                size_t mid = lo + (hi - lo) / 2;
                if (cell_id(old_geo, mid) < c) lo = mid + 1; else hi = mid;
            }
            return lo;
        };
        auto added_bound = [&](size_t k) -> size_t {
            if (k == 0) return 0;
            if (k == chunks) return added.size();
            uint64_t c = cell_id(new_geo, new_at(k));
            return static_cast<size_t>(std::lower_bound(added.begin(), added.end(), c) - added.begin());
        };
        std::vector<std::vector<char>> bufs(chunks);
        std::vector<uint32_t> counts(chunks, 0);
        parallel_for(chunks, [&](size_t b, size_t e, unsigned) {
            for (size_t k = b; k < e; k++)
                walk(old_bound(k), old_bound(k + 1), new_at(k), new_at(k + 1), added_bound(k), added_bound(k + 1),
                     bufs[k], counts[k]);
        }, threads);
        for (size_t k = 0; k < chunks; k++) {
            buf.insert(buf.end(), bufs[k].begin(), bufs[k].end());
            dc += counts[k];
        }
    }
    uint32_t marker = GEO_ENTRY_DELTA_MARKER;
    memcpy(buf.data(), &marker, 4); memcpy(buf.data() + 4, &file_id, 4); memcpy(buf.data() + 8, &dc, 4);
    return {std::move(buf), dc};
}
