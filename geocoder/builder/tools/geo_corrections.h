// The street / addr / interp entry corrections (GEO_ENTRY_DELTA_MARKER) of a
// diff: the patcher derives each geo cell's list from its old one through
// the file's id remap, and every cell where that differs from the new list
// travels as a delta.
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <unordered_set>
#include <vector>

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
inline GeoEntryCorrections geo_entry_corrections(uint32_t file_id, GeoCells old_geo, GeoCells new_geo,
                                                 const std::vector<uint64_t>& added,
                                                 const std::vector<uint64_t>& removed,
                                                 ByteSpan old_entries, ByteSpan new_entries, size_t geo_off_pos,
                                                 const std::vector<uint32_t>& id_rm) {
    constexpr uint32_t NO_DATA = 0xFFFFFFFFu;
    auto parse_ids = [](ByteSpan entries, uint32_t off) {
        std::vector<uint32_t> ids;
        read_entry_list(entries, off, ids);
        return ids;
    };
    // Remap IDs in-place using vector remap (O(1) per ID)
    auto remap_ids_vec = [](std::vector<uint32_t>& ids, const std::vector<uint32_t>& rm) {
        for (auto& id : ids)
            if (id < rm.size() && rm[id] != NO_DATA) id = rm[id];
        std::sort(ids.begin(), ids.end());
    };
    std::unordered_set<uint64_t> removed_set(removed.begin(), removed.end());
    const size_t old_nc = old_geo.n, new_nc = new_geo.n;

    std::vector<char> buf; buf.resize(12, 0); uint32_t dc = 0;
    size_t oi = 0, ni = 0, added_i = 0;
    while (oi < old_nc || ni < new_nc || added_i < added.size()) {
        // Determine next effective-old cell_id
        uint64_t eff_old = UINT64_MAX;
        bool is_added = false;
        // Skip removed old cells
        while (oi < old_nc) {
            memcpy(&eff_old, old_geo.data + oi * 20, 8);
            if (!removed_set.count(eff_old)) break;
            oi++;
            eff_old = UINT64_MAX;
        }
        // Check if an added cell comes before
        if (added_i < added.size() && added[added_i] < eff_old) {
            eff_old = added[added_i];
            is_added = true;
        }

        uint64_t n_cid = UINT64_MAX;
        if (ni < new_nc) memcpy(&n_cid, new_geo.data + ni * 20, 8);

        if (eff_old == UINT64_MAX && n_cid == UINT64_MAX) break;

        if (eff_old == n_cid) {
            // Cell in both — compute derived and compare
            std::vector<uint32_t> d_ids;
            if (!is_added) {
                uint32_t off; memcpy(&off, old_geo.data + oi * 20 + geo_off_pos, 4);
                d_ids = parse_ids(old_entries, off);
                remap_ids_vec(d_ids, id_rm);
                oi++;
            } else {
                added_i++;
            }
            uint32_t n_off; memcpy(&n_off, new_geo.data + ni * 20 + geo_off_pos, 4);
            auto n_ids = parse_ids(new_entries, n_off);
            bool differs = (d_ids.size() != n_ids.size()) ||
                (!d_ids.empty() && memcmp(d_ids.data(), n_ids.data(), d_ids.size() * 4) != 0);
            if (differs) {
                append_geo_list_delta(buf, eff_old, d_ids, n_ids);
                dc++;
            }
            ni++;
        } else if (eff_old < n_cid) {
            // Cell only in effective-old — correct to empty
            std::vector<uint32_t> d_ids;
            if (!is_added) {
                uint32_t off; memcpy(&off, old_geo.data + oi * 20 + geo_off_pos, 4);
                d_ids = parse_ids(old_entries, off);
                remap_ids_vec(d_ids, id_rm);
                oi++;
            } else {
                added_i++;
            }
            if (!d_ids.empty()) {
                append_geo_list_delta(buf, eff_old, d_ids, {});
                dc++;
            }
        } else {
            // Cell only in new — emit new entries
            uint32_t n_off; memcpy(&n_off, new_geo.data + ni * 20 + geo_off_pos, 4);
            auto n_ids = parse_ids(new_entries, n_off);
            if (!n_ids.empty()) {
                append_geo_list_delta(buf, n_cid, {}, n_ids);
                dc++;
            }
            ni++;
        }
    }
    uint32_t marker = GEO_ENTRY_DELTA_MARKER;
    memcpy(buf.data(), &marker, 4); memcpy(buf.data() + 4, &file_id, 4); memcpy(buf.data() + 8, &dc, 4);
    return {std::move(buf), dc};
}
