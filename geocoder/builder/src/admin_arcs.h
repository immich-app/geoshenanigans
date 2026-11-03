// Admin boundaries as arcs: each stretch of border between two junctions held
// once, however many polygon rings run along it. The topology works on the
// exact ring store (ParsedData::admin_exact_rings), OSM's own e7 points, where
// a vertex is its coordinate K (coord_key): nodes at one coordinate are one
// vertex, and a vertex with other than two distinct neighbours over all the
// rings is a junction. Rings are cut at their junctions into pieces, and equal
// pieces are one arc, read in its canonical order. A ring is the chain of its
// refs to arcs. Arcs are presented in Hilbert order of their first vertex and
// keyed on their ends and their smallest interior vertex.
#pragma once

#include <climits>
#include <cstdint>
#include <functional>
#include <utility>
#include <vector>

#include "geometry.h"
#include "packed_location.h"
#include "polygon_rings.h"
#include "types.h"

struct AdminExactRings;  // parsed_data.h

// splitmix64's finalizer: a bijection of 64-bit values that spreads every bit.
inline uint64_t mix64(uint64_t x) {
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ull;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebull;
    x ^= x >> 31;
    return x;
}

// K, the order the canonical forms compare vertices in: coord_key's bits, unsigned.
inline uint64_t vertex_key(PackedLocation p) { return static_cast<uint64_t>(coord_key(p)); }

inline uint32_t arc_partition(uint64_t vertex) { return static_cast<uint32_t>((mix64(vertex) >> 32) % kArcPartitions); }

// An arc's identity from one build to the next, IdAllocator's 56-bit stable_id:
// its first and last vertex, its smallest interior vertex (UINT64_MAX when it
// has none) and whether it is closed. An edit inside an arc keeps it unless it
// moves that smallest vertex; a moved junction changes it.
inline uint64_t arc_key(uint64_t first, uint64_t last, uint64_t min_interior, bool closed) {
    uint64_t h = mix64(first + 0x9e3779b97f4a7c15ull);
    h = mix64(h ^ last);
    h = mix64(h ^ min_interior);
    h = mix64(h ^ (closed ? 1u : 2u));
    return h & 0x00FFFFFFFFFFFFFFull;
}

// The index of cell (x, y) along the Hilbert curve over 2^32 x 2^32 cells.
inline uint64_t hilbert_index(uint32_t x, uint32_t y) {
    uint64_t d = 0;
    for (uint32_t s = 1u << 31; s > 0; s >>= 1) {
        const uint32_t rx = (x & s) ? 1 : 0, ry = (y & s) ? 1 : 0;
        d += uint64_t(s) * s * ((3 * rx) ^ ry);
        if (ry == 0) {
            // The quadrant's bits are spent, so flipping all of x and y
            // flips what is left of them.
            if (rx == 1) {
                x = ~x;
                y = ~y;
            }
            std::swap(x, y);
        }
    }
    return d;
}

// An e7 point's Hilbert index, longitude + 1.8e9 by latitude + 9e8: the curve
// is 2^32 wide because the longitude needs 32 bits.
inline uint64_t hilbert_index(PackedLocation p) {
    return hilbert_index(static_cast<uint32_t>(int64_t(p.lon_e7) + 1800000000),
                         static_cast<uint32_t>(int64_t(p.lat_e7) + 900000000));
}

// A ring's use of an arc: along the arc's own order, or against it.
struct ArcRef {
    uint32_t arc = 0;
    bool reversed = false;
};
inline bool operator==(const ArcRef& a, const ArcRef& b) { return a.arc == b.arc && a.reversed == b.reversed; }

// Where an arc's vertices sit in the exact ring store, in the arc's order:
// `count` places of ring `ring`'s cycle (its vertices but the closing one)
// from place `start`, backwards when `reversed`, wrapping round. A closed arc
// (a ring with no junction) comes back to its first vertex at the end.
struct ArcView {
    uint32_t ring = 0;
    uint32_t start = 0;
    uint32_t count = 0;
    bool reversed = false;
    bool closed = false;
};

// An arc's raw vertices' extent in e7 units, the three sides a north ray's
// skip reads.
struct ArcBox {
    int32_t min_lng = INT32_MAX;
    int32_t max_lng = INT32_MIN;
    int32_t max_lat = INT32_MIN;
};
inline bool operator==(const ArcBox& a, const ArcBox& b) {
    return a.min_lng == b.min_lng && a.max_lng == b.max_lng && a.max_lat == b.max_lat;
}

struct Arc {
    ArcView view;
    ArcBox box;
    uint64_t key = 0;          // arc_key
};

// An arc's vertices in its order, read through its view of a ring.
class ArcVertices {
public:
    ArcVertices(const PackedLocation* ring, uint32_t ring_count, const ArcView& view)
        : ring_(ring), cycle_(ring_count - 1), view_(view) {}

    uint32_t size() const { return view_.count; }
    PackedLocation operator[](uint32_t i) const {
        const uint64_t step = i % cycle_;
        return ring_[(view_.reversed ? view_.start + cycle_ - step : view_.start + step) % cycle_];
    }

private:
    const PackedLocation* ring_;
    uint64_t cycle_;
    ArcView view_;
};

// The arc a view shows, in a store with `vertices` and `rings` (AdminExactRings).
template <class Store>
ArcVertices arc_vertices(const Store& store, const ArcView& view) {
    const RingSpan& ring = store.rings[view.ring];
    return {store.vertices.data() + ring.begin, ring.count, view};
}

struct AdminArcCounts {
    size_t junctions = 0;
    size_t arcs = 0;
    size_t closed_arcs = 0;
    size_t refs = 0;
    size_t arc_vertices = 0;       // over every arc, a closed arc's first vertex twice
    size_t largest_partition = 0;  // vertex occurrences
    size_t hash_collisions = 0;    // pieces whose hash another arc had
    size_t duplicate_keys = 0;     // arcs whose key an earlier one has
};

// The arcs of every admin polygon's rings, one set (the full set).
struct AdminArcs {
    // In presentation order; their views read the exact ring store, so only
    // while it lives.
    std::vector<Arc> arcs;
    // Ring r's refs, in the ring's order, are refs[ring_refs[r], ring_refs[r + 1]),
    // the rings numbered as the exact ring store numbers them.
    std::vector<uint32_t> ring_refs;
    std::vector<ArcRef> refs;
    AdminArcCounts counts;
};

// The topology of the store's rings: their arcs, in presentation order (the
// Hilbert index of the first vertex, the key, then the vertices), each with
// its view, box and key, and every ring's refs. On at most `threads` threads,
// with the same result on any number. `phase_done` is told as each step ends.
AdminArcs build_arc_topology(const AdminExactRings& store, unsigned threads,
                             const std::function<void(const char*)>& phase_done = {});

// Ring `ring` of the store as its refs chain their arcs: each arc walked as
// its ref says, a junction two arcs share once, ending where it starts.
std::vector<PackedLocation> rebuild_ring(const AdminArcs& arcs, const AdminExactRings& store, uint32_t ring);

// Whether two closed rings (each ending on its first vertex) are one cycle,
// from any vertex and in either direction.
bool same_closed_ring(const PackedLocation* a, uint32_t a_count, const PackedLocation* b, uint32_t b_count);
