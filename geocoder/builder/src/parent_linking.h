// Pure helpers (no S2) for the passes that link records to their parents:
// the admin polygons containing them and the streets nearest to them.
#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <functional>
#include <utility>
#include <vector>

#include "ring_contains.h"
#include "types.h"

// ring_contains (ring_contains.h) is the even-odd ray cast every linking pass
// shares: edge (a, b) toggles when it straddles lng (one end > lng, the other
// <= lng) and lat lies below the latitude the edge reaches at lng. Another
// rounding would move boundary points between polygons and change parent ids.

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

// The smallest distance-squared from (plat, plng) to the polyline's segments,
// in local-flat approximation (good enough for nearest-street ranking):
// longitudes scale by cos_lat, the cosine of plat.
inline double street_d2(const NodeCoord* nodes, uint16_t count, float plat, float plng, double cos_lat) {
    double min_d2 = INFINITY;
    for (uint16_t k = 0; k + 1 < count; k++) {
        const auto& a = nodes[k];
        const auto& b = nodes[k + 1];
        double ax = (a.lng - plng) * cos_lat;
        double ay = (a.lat - plat);
        double bx = (b.lng - plng) * cos_lat;
        double by = (b.lat - plat);
        double dx = bx - ax;
        double dy = by - ay;
        double seg_len2 = dx * dx + dy * dy;
        double d2;
        if (seg_len2 < 1e-18) {
            d2 = ax * ax + ay * ay;
        } else {
            double tt = -(ax * dx + ay * dy) / seg_len2;
            if (tt < 0.0) tt = 0.0;
            else if (tt > 1.0) tt = 1.0;
            double qx = ax + tt * dx;
            double qy = ay + tt * dy;
            d2 = qx * qx + qy * qy;
        }
        if (d2 < min_d2) min_d2 = d2;
    }
    return min_d2;
}

// A street's lat/lng bounds.
struct StreetBox {
    float lat_lo, lat_hi, lng_lo, lng_hi;
};

inline StreetBox street_box(const NodeCoord* nodes, uint16_t count) {
    StreetBox box{INFINITY, -INFINITY, INFINITY, -INFINITY};
    for (uint16_t k = 0; k < count; k++)
        box = {std::min(box.lat_lo, nodes[k].lat), std::max(box.lat_hi, nodes[k].lat),
               std::min(box.lng_lo, nodes[k].lng), std::max(box.lng_hi, nodes[k].lng)};
    return box;
}

// Distance-squared from (plat, plng) to the box, in street_d2's frame. The
// frame rounds a node's coordinates as it rounds the box corners, so every
// segment lies in the rounded box, and no street_d2 of the street's nodes
// is below this but by rounding: their square roots differ by under 1e-13
// degrees (differences of up to 360 degrees, a few roundings each), far
// under kStreetBoxSlack.
inline double street_box_d2(const StreetBox& box, float plat, float plng, double cos_lat) {
    double x_lo = (box.lng_lo - plng) * cos_lat, x_hi = (box.lng_hi - plng) * cos_lat;
    double y_lo = (box.lat_lo - plat), y_hi = (box.lat_hi - plat);
    double dx = x_lo > 0 ? x_lo : (x_hi < 0 ? -x_hi : 0.0);
    double dy = y_lo > 0 ? y_lo : (y_hi < 0 ? -y_hi : 0.0);
    return dx * dx + dy * dy;
}

// Degrees: a street whose sqrt(street_box_d2) exceeds a sqrt(street_d2) by
// this has a street_d2 above it.
static constexpr double kStreetBoxSlack = 1e-11;

// Hands out items [0, n) in ascending key order (a strict total order under
// <), computing an item's key only once it could come next: key_of(i) is
// costly, bound(i) a cheap lower bound on it, and beyond(b, key) true only
// when every item bounded by b or more has a key above `key`. A caller that
// stops early never pays for the keys of the items it did not reach. One
// instance serves many orders, keeping its storage.
template <class Key>
class LazyAscending {
public:
    template <class Bound>
    void reset(uint32_t n, Bound bound) {
        unvisited_.clear();
        visited_.clear();
        for (uint32_t i = 0; i < n; i++) unvisited_.push_back({bound(i), i});
        std::make_heap(unvisited_.begin(), unvisited_.end(), std::greater<>());
    }

    // The next item and its key; false once every item has been handed out.
    template <class KeyOf, class Beyond>
    bool next(uint32_t& item, Key& key, KeyOf key_of, Beyond beyond) {
        while (!unvisited_.empty() &&
               (visited_.empty() || !beyond(unvisited_.front().first, visited_.front().first))) {
            std::pop_heap(unvisited_.begin(), unvisited_.end(), std::greater<>());
            const uint32_t i = unvisited_.back().second;
            unvisited_.pop_back();
            visited_.push_back({key_of(i), i});
            std::push_heap(visited_.begin(), visited_.end(), std::greater<>());
        }
        if (visited_.empty()) return false;
        std::pop_heap(visited_.begin(), visited_.end(), std::greater<>());
        item = visited_.back().second;
        key = visited_.back().first;
        visited_.pop_back();
        return true;
    }

private:
    std::vector<std::pair<double, uint32_t>> unvisited_;  // (bound, item), a min-heap
    std::vector<std::pair<Key, uint32_t>> visited_;       // (key, item), a min-heap
};
