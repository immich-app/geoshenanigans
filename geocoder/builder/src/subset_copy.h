// Copying a subset of planet records (continent split): records in id order,
// their coordinate runs back to back, and the planet -> subset id map.
#pragma once

#include <algorithm>
#include <cstdint>
#include <vector>

#include "parallel.h"
#include "types.h"

// One record's coordinate run in the planet arrays.
struct CoordRun { uint32_t first; uint32_t count; };

// Copies the planet records at `ids` (ascending) that keep(id) accepts into
// `records`, in id order, and each one's coordinate run (coords_of(id)) into
// `coords`, back to back; rebase(record, offset, count) points the copy at its
// run. Returns planet id -> subset id, NO_DATA for records left out.
template <class Record, class Keep, class CoordsOf, class Rebase>
std::vector<uint32_t> copy_subset(const std::vector<uint32_t>& ids,
                                         const std::vector<Record>& full_records,
                                         const std::vector<NodeCoord>& full_coords,
                                         Keep keep, CoordsOf coords_of, Rebase rebase,
                                         std::vector<Record>& records, std::vector<NodeCoord>& coords) {
    std::vector<char> kept(ids.size());
    parallel_for(ids.size(), [&](size_t begin, size_t end, unsigned) {
        for (size_t k = begin; k < end; k++) kept[k] = keep(ids[k]) ? 1 : 0;
    });
    std::vector<uint32_t> remap(full_records.size(), NO_DATA);
    parallel_prefix_fill(ids.size(), [&](size_t k) -> size_t { return kept[k]; },
        [&](size_t total) { records.resize(total); },
        [&](size_t k, size_t slot) -> size_t {
            if (!kept[k]) return 0;
            records[slot] = full_records[ids[k]];
            remap[ids[k]] = static_cast<uint32_t>(slot);
            return 1;
        });
    parallel_prefix_fill(ids.size(), [&](size_t k) -> size_t { return kept[k] ? coords_of(ids[k]).count : 0; },
        [&](size_t total) { coords.resize(total); },
        [&](size_t k, size_t offset) -> size_t {
            if (!kept[k]) return 0;
            CoordRun run = coords_of(ids[k]);
            std::copy(full_coords.begin() + run.first, full_coords.begin() + run.first + run.count,
                      coords.begin() + offset);
            rebase(records[remap[ids[k]]], static_cast<uint32_t>(offset), run.count);
            return run.count;
        });
    return remap;
}

// osm_ids of a subset's records, parallel to them.
template <class Id>
std::vector<Id> subset_osm_ids(const std::vector<uint32_t>& ids, const std::vector<uint32_t>& remap,
                                      size_t subset_size, const std::vector<Id>& full_osm_ids) {
    std::vector<Id> out(subset_size);
    parallel_for(ids.size(), [&](size_t begin, size_t end, unsigned) {
        for (size_t k = begin; k < end; k++) {
            uint32_t slot = remap[ids[k]];
            if (slot != NO_DATA) out[slot] = ids[k] < full_osm_ids.size() ? full_osm_ids[ids[k]] : 0;
        }
    });
    return out;
}
