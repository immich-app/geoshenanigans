// Crossing-number point-in-ring test over packed admin vertices, in the
// float arithmetic the country lookup has always used (its answers are
// build output, so the arithmetic must not change).
#pragma once

#include <algorithm>
#include <cstdint>
#include <utility>
#include <vector>

#include "types.h"

// Whether the edge between va and vb crosses the ray from (plat, plng)
// towards +lat.
inline bool ring_edge_crosses(const NodeCoord& va, const NodeCoord& vb, float plat, float plng) {
    return ((va.lng > plng) != (vb.lng > plng)) &&
           (plat < (vb.lat - va.lat) * (plng - va.lng) / (vb.lng - va.lng) + va.lat);
}

inline bool ring_contains(const NodeCoord* verts, uint32_t cnt, float plat, float plng) {
    bool inside = false;
    for (uint32_t a = 0, b = cnt - 1; a < cnt; b = a++)
        if (ring_edge_crosses(verts[a], verts[b], plat, plng)) inside = !inside;
    return inside;
}

// A ring's edges bucketed along one axis, for testing many points against
// one ring by crossing number with the ray along the other axis. An edge can
// only cross the ray of a point whose coordinate on this axis lies within the
// edge's own span on it, and the bucket of a coordinate only grows with it,
// so the point's bucket holds every edge that can cross; the parity of the
// crossings is the same in any order: the answer is the full scan's.
class RingEdgeBuckets {
public:
    // axis(k): vertex k's coordinate on the bucket axis, k in [0, cnt); at
    // most `buckets` buckets.
    template <class Axis>
    RingEdgeBuckets(uint32_t cnt, uint32_t buckets, Axis axis) : cnt_(cnt) {
        double lo = cnt ? static_cast<double>(axis(0)) : 0, hi = lo;
        for (uint32_t k = 1; k < cnt; k++) {
            lo = std::min(lo, static_cast<double>(axis(k)));
            hi = std::max(hi, static_cast<double>(axis(k)));
        }
        origin_ = lo;
        if (hi > lo) {
            buckets_ = std::max<uint32_t>(1, buckets);
            scale_ = buckets_ / (hi - lo);
        }
        start_.assign(buckets_ + 1, 0);
        for_each_edge_bucket(axis, [&](uint32_t, uint32_t bucket) { start_[bucket + 1]++; });
        for (uint32_t b = 0; b < buckets_; b++) start_[b + 1] += start_[b];
        edges_.resize(start_[buckets_]);
        std::vector<uint32_t> cursor(start_.begin(), start_.end() - 1);
        for_each_edge_bucket(axis, [&](uint32_t a, uint32_t bucket) { edges_[cursor[bucket]++] = a; });
    }

    // Whether an odd number of the edges that can cross at coordinate x do:
    // crosses(a, b) tests the edge from vertex b = a - 1 (cnt - 1 for a = 0)
    // to vertex a.
    template <class Crosses>
    bool odd_crossings(double x, Crosses crosses) const {
        const uint32_t bucket = bucket_of(x);
        bool odd = false;
        for (uint32_t k = start_[bucket]; k < start_[bucket + 1]; k++) {
            uint32_t a = edges_[k];
            if (crosses(a, a == 0 ? cnt_ - 1 : a - 1)) odd = !odd;
        }
        return odd;
    }

private:
    // Monotone in x; NaN goes to bucket 0, where nothing crosses it.
    uint32_t bucket_of(double x) const {
        x = (x - origin_) * scale_;
        if (!(x > 0)) return 0;
        if (x >= buckets_) return buckets_ - 1;
        return static_cast<uint32_t>(x);
    }

    // fn(a, bucket) for edge (a - 1, a) and every bucket its span touches.
    template <class Axis, class Fn>
    void for_each_edge_bucket(Axis& axis, Fn fn) const {
        for (uint32_t a = 0, b = cnt_ - 1; a < cnt_; b = a++) {
            double lo = axis(a), hi = axis(b);
            if (hi < lo) std::swap(lo, hi);
            for (uint32_t bucket = bucket_of(lo), last = bucket_of(hi); bucket <= last; bucket++) fn(a, bucket);
        }
    }

    uint32_t cnt_;
    double origin_ = 0;
    double scale_ = 0;
    uint32_t buckets_ = 1;
    std::vector<uint32_t> start_;  // bucket b's edges: edges_[start_[b], start_[b + 1])
    std::vector<uint32_t> edges_;  // edge (a - 1, a) stored as a
};

// ring_contains for many points against one ring (its ray runs along
// latitude, so the edges are bucketed by longitude).
class RingEdgeIndex {
public:
    // verts must outlive the index.
    RingEdgeIndex(const NodeCoord* verts, uint32_t cnt)
        : verts_(verts),
          buckets_(cnt, std::min<uint32_t>(cnt / 16, 1u << 20), [verts](uint32_t k) { return verts[k].lng; }) {}

    bool contains(float plat, float plng) const {
        return buckets_.odd_crossings(plng, [&](uint32_t a, uint32_t b) {
            return ring_edge_crosses(verts_[a], verts_[b], plat, plng);
        });
    }

private:
    const NodeCoord* verts_;
    RingEdgeBuckets buckets_;
};
