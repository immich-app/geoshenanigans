// Pure helpers (no S2) for the passes that link records to their parents:
// the admin polygons containing them and the streets nearest to them.
#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <utility>
#include <vector>

#include "types.h"

// Even-odd ray cast along the latitude axis: edge (a, b) toggles when it
// straddles lng (one end > lng, the other <= lng) and lat lies below the
// latitude the edge reaches at lng. Every linking pass must share exactly this
// float arithmetic: another rounding moves boundary points between polygons
// and changes parent ids.
inline bool ring_contains(const NodeCoord* verts, uint32_t n, float lat, float lng) {
    bool inside = false;
    for (uint32_t a = 0, b = n - 1; a < n; b = a++) {
        if (((verts[a].lng > lng) != (verts[b].lng > lng)) &&
            (lat < (verts[b].lat - verts[a].lat) * (lng - verts[a].lng) / (verts[b].lng - verts[a].lng) + verts[a].lat))
            inside = !inside;
    }
    return inside;
}

// Latitude slack of RingBox. The latitude ring_contains interpolates for an
// edge overshoots the edge's end latitudes only by the rounding of its four
// float ops, under 7 * 2^-24 * |lat| (< 4e-5 degrees; vertex coordinates sit
// on OSM's 1e-7 degree grid, so nothing underflows).
static constexpr float kRingBoxLatPad = 1e-3f;

// Bounding box of a ring, for a cheap reject in front of ring_contains.
struct RingBox {
    float lat_lo, lat_hi, lng_lo, lng_hi;
};

inline RingBox ring_box(const NodeCoord* verts, uint32_t n) {
    float min_lat = INFINITY, max_lat = -INFINITY, min_lng = INFINITY, max_lng = -INFINITY;
    for (uint32_t i = 0; i < n; i++) {
        min_lat = std::min(min_lat, verts[i].lat);
        max_lat = std::max(max_lat, verts[i].lat);
        min_lng = std::min(min_lng, verts[i].lng);
        max_lng = std::max(max_lng, verts[i].lng);
    }
    return {min_lat - kRingBoxLatPad, max_lat + kRingBoxLatPad, min_lng, max_lng};
}

// True only where ring_contains is false, so skipping the ray cast never
// changes an answer. Longitude is exact: no edge straddles a lng at or above
// every vertex or below every vertex. At or above the padded top no straddling
// edge passes above the point; below the padded bottom every one does, and
// straddling edges come in pairs going round the ring.
inline bool ring_box_excludes(const RingBox& b, float lat, float lng) {
    return lng < b.lng_lo || lng >= b.lng_hi || lat < b.lat_lo || lat >= b.lat_hi;
}

// Reorders the polygon ids a neighbourhood scan meets (scan order, repeats
// allowed) into unique ids sorted by `better`, ties to the earlier first
// occurrence. The first ranked id containing a point is then the one a scan
// over the original sequence ends on when it keeps a containing id only if it
// is strictly better than the current pick: containment depends on the id
// alone, so that scan keeps the earliest id of the best containing class.
// `better` must be a strict weak order.
template <class Better>
void rank_candidates(std::vector<uint32_t>& ids, Better better,
                     std::vector<std::pair<uint32_t, uint32_t>>& scratch) {
    scratch.clear();
    for (uint32_t pos = 0; pos < ids.size(); pos++) scratch.push_back({ids[pos], pos});
    std::sort(scratch.begin(), scratch.end());
    scratch.erase(std::unique(scratch.begin(), scratch.end(),
                              [](const auto& a, const auto& b) { return a.first == b.first; }),
                  scratch.end());
    std::sort(scratch.begin(), scratch.end(), [&](const auto& a, const auto& b) {
        if (better(a.first, b.first)) return true;
        if (better(b.first, a.first)) return false;
        return a.second < b.second;
    });
    ids.clear();
    for (const auto& entry : scratch) ids.push_back(entry.first);
}

// Drops repeated ids, keeping each first occurrence in order: rank_candidates
// with no order, in linear time (open addressing over `table`). No id may be
// NO_DATA, the empty-slot marker.
inline void keep_first_occurrences(std::vector<uint32_t>& ids, std::vector<uint32_t>& table) {
    int bits = 4;
    while ((size_t(1) << bits) < 2 * ids.size()) bits++;
    size_t mask = (size_t(1) << bits) - 1;
    table.assign(mask + 1, NO_DATA);
    size_t kept = 0;
    for (uint32_t id : ids) {
        size_t h = static_cast<size_t>((id * 0x9E3779B97F4A7C15ull) >> (64 - bits));
        while (table[h] != NO_DATA && table[h] != id) h = (h + 1) & mask;
        if (table[h] == id) continue;
        table[h] = id;
        ids[kept++] = id;
    }
    ids.resize(kept);
}

// std::lower_bound(first, last, value, comp), found by galloping from first:
// probes first+0, 1, 3, 7, ... then binary-searches the last gap, so a lookup
// landing near first costs a few nearby probes, not a search of the range.
template <class It, class T, class Comp>
It gallop_lower_bound(It first, It last, const T& value, Comp comp) {
    auto n = last - first;
    decltype(n) lo = 0, hi = 0, step = 1;
    while (hi < n && comp(first[hi], value)) {
        lo = hi + 1;
        hi += step;
        step *= 2;
    }
    return std::lower_bound(first + lo, first + std::min(hi, n), value, comp);
}

// The first of `candidates` in `before` order (a strict total order) that
// passes `qualifies`, or nullptr. Heap-ordered, so a costly `qualifies` runs
// only on the candidates ranked ahead of the winner. Reorders `candidates`.
template <class T, class Before, class Qualifies>
const T* first_qualifying(std::vector<T>& candidates, Before before, Qualifies qualifies) {
    auto after = [&](const T& a, const T& b) { return before(b, a); };
    std::make_heap(candidates.begin(), candidates.end(), after);
    for (auto end = candidates.end(); end != candidates.begin(); --end) {
        std::pop_heap(candidates.begin(), end, after);
        if (qualifies(*(end - 1))) return &*(end - 1);
    }
    return nullptr;
}
