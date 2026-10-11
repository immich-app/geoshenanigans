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

// A ring's edges bucketed by longitude, for testing many points against one
// ring. An edge can only cross the ray of a point whose longitude lies within
// the edge's own longitude span, and the bucket of a longitude only grows with
// it, so the point's bucket holds every edge that can cross; contains() tests
// just those, with ring_edge_crosses, and the parity of the crossings is the
// same in any order: the answer is ring_contains's.
class RingEdgeIndex {
public:
    // verts must outlive the index.
    RingEdgeIndex(const NodeCoord* verts, uint32_t cnt) : verts_(verts), cnt_(cnt) {
        float lo = verts[0].lng, hi = verts[0].lng;
        for (uint32_t k = 1; k < cnt; k++) {
            if (verts[k].lng < lo) lo = verts[k].lng;
            if (verts[k].lng > hi) hi = verts[k].lng;
        }
        origin_ = lo;
        if (hi > lo) {
            buckets_ = std::max<uint32_t>(1, std::min<uint32_t>(cnt / 16, 1u << 20));
            scale_ = buckets_ / (static_cast<double>(hi) - static_cast<double>(lo));
        }
        start_.assign(buckets_ + 1, 0);
        for_each_edge_bucket([&](uint32_t, uint32_t bucket) { start_[bucket + 1]++; });
        for (uint32_t b = 0; b < buckets_; b++) start_[b + 1] += start_[b];
        edges_.resize(start_[buckets_]);
        std::vector<uint32_t> cursor(start_.begin(), start_.end() - 1);
        for_each_edge_bucket([&](uint32_t a, uint32_t bucket) { edges_[cursor[bucket]++] = a; });
    }

    bool contains(float plat, float plng) const {
        const uint32_t bucket = bucket_of(plng);
        bool inside = false;
        for (uint32_t k = start_[bucket]; k < start_[bucket + 1]; k++) {
            uint32_t a = edges_[k];
            if (ring_edge_crosses(verts_[a], verts_[a == 0 ? cnt_ - 1 : a - 1], plat, plng)) inside = !inside;
        }
        return inside;
    }

private:
    // Monotone in lng; NaN goes to bucket 0, where nothing crosses it.
    uint32_t bucket_of(float lng) const {
        double x = (static_cast<double>(lng) - origin_) * scale_;
        if (!(x > 0)) return 0;
        if (x >= buckets_) return buckets_ - 1;
        return static_cast<uint32_t>(x);
    }

    // fn(a, bucket) for edge (a - 1, a) and every bucket its span touches.
    template <class Fn>
    void for_each_edge_bucket(Fn fn) const {
        for (uint32_t a = 0, b = cnt_ - 1; a < cnt_; b = a++) {
            float lo = verts_[a].lng, hi = verts_[b].lng;
            if (hi < lo) std::swap(lo, hi);
            for (uint32_t bucket = bucket_of(lo), last = bucket_of(hi); bucket <= last; bucket++) fn(a, bucket);
        }
    }

    const NodeCoord* verts_;
    uint32_t cnt_;
    double origin_ = 0;
    double scale_ = 0;
    uint32_t buckets_ = 1;
    std::vector<uint32_t> start_;  // bucket b's edges: edges_[start_[b], start_[b + 1])
    std::vector<uint32_t> edges_;  // edge (a - 1, a) stored as a
};
