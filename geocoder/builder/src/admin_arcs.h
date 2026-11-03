// Admin boundaries as arcs: each stretch of border between two junctions held
// once, however many polygon rings run along it. The topology works on the
// exact ring store (ParsedData::admin_exact_rings), OSM's own e7 points, where
// a vertex is its coordinate K (coord_key): nodes at one coordinate are one
// vertex, and a vertex with other than two distinct neighbours over all the
// rings is a junction. Rings are cut at their junctions into pieces, and equal
// pieces are one arc, read in its canonical order. A ring is the chain of its
// refs to arcs. Arcs are presented in Hilbert order of their first vertex and
// take their slots from IdAllocator (ObjectType::ARC), keyed on their ends and
// their smallest interior vertex.
//
// That is the full set, every admin polygon's rings. The minimal set holds
// admin-minimal's rings alone: its arcs are chains of full arcs, joined where
// a full junction is one only for other polygons' rings. Each set has its
// tiers (kArcTiers): the exact tier stores every raw vertex, a simplified tier
// the vertices Douglas-Peucker keeps of each arc, snapped to its grid, with
// vertices given back where a ring would degenerate. build-index runs the
// arc stage (build_admin_arcs) only with --arcs-out.
#pragma once

#include <algorithm>
#include <climits>
#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "geometry.h"
#include "id_allocator.h"
#include "packed_location.h"
#include "polygon_rings.h"
#include "types.h"

struct AdminExactRings;  // parsed_data.h

// The stage's thread cap: the topology's transients grow with the threads.
constexpr unsigned kArcTopologyMaxThreads = 32;

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
    uint32_t planet_slot = 0;  // in the planet's full set (build_admin_arcs)
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

// A polygon's rings, outer first, as AdminArcs numbers them.
struct AdminArcPolygon {
    uint32_t first_ring = 0;
    uint32_t ring_count = 0;
};
inline bool operator==(const AdminArcPolygon& a, const AdminArcPolygon& b) {
    return a.first_ring == b.first_ring && a.ring_count == b.ring_count;
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

struct AdminArcSlotCounts {
    size_t kept = 0;      // the previous build's slot of the arc's key
    size_t recycled = 0;  // a slot that was a tombstone in the previous build
    size_t appended = 0;
    size_t tombstones = 0;
};

// One arc of the minimal set: full arcs chained end to end, each walked as
// its part says, so its vertices are theirs with each join once. A closed
// one (a loop of parts) starts at place `start` of its first part.
struct MinimalArc {
    uint32_t first_part = 0;  // MinimalArcs::parts[first_part, first_part + part_count)
    uint32_t part_count = 0;
    uint32_t count = 0;  // vertices, a closed arc's first twice
    uint32_t start = 0;
    bool closed = false;
    ArcBox box;
    uint64_t key = 0;          // arc_key
    uint32_t planet_slot = 0;  // in the planet's minimal set
};

struct MinimalArcCounts {
    size_t rings = 0;     // of the minimal polygons
    size_t polygons = 0;  // admin_minimal_keeps' with rings
    size_t core_arcs = 0;           // full arcs a minimal ring uses
    size_t joined_junctions = 0;    // full junctions inside a minimal arc
    size_t arcs = 0;
    size_t closed_arcs = 0;
    size_t refs = 0;
    size_t arc_vertices = 0;  // over every arc, a closed arc's first twice
    size_t duplicate_keys = 0;
};

// The minimal set: the arcs of the rings of admin_minimal_keeps' polygons.
struct MinimalArcs {
    std::vector<MinimalArc> arcs;  // in presentation order
    // Each arc's parts in its order, and where each part starts among its
    // arc's places (a part's last vertex is the next one's first).
    std::vector<ArcRef> parts;
    std::vector<uint32_t> part_at;
    // As AdminArcs' refs, over the same ring numbers: a ring of no minimal
    // polygon has none.
    std::vector<uint32_t> ring_refs;
    std::vector<ArcRef> refs;
    std::vector<gc::id_alloc::SidecarSlot> planet_sidecar;
    MinimalArcCounts counts;
    AdminArcSlotCounts slot_counts;
};

// The raw vertices a simplified tier keeps of each arc of its set: bits from
// word word_at_[a] on, one per raw vertex. Arcs never share a word, so
// threads can mark different arcs at once.
class ArcKeep {
public:
    ArcKeep() = default;
    template <class Count>
    ArcKeep(size_t arcs, Count count) : word_at_(arcs + 1, 0) {
        for (size_t a = 0; a < arcs; a++) word_at_[a + 1] = word_at_[a] + (size_t(count(a)) + 63) / 64;
        words_.assign(word_at_[arcs], 0);
    }
    bool empty() const { return word_at_.empty(); }
    bool test(uint32_t arc, uint32_t i) const { return (words_[word_at_[arc] + i / 64] >> (i % 64)) & 1; }
    void set(uint32_t arc, uint32_t i) { words_[word_at_[arc] + i / 64] |= uint64_t(1) << (i % 64); }
    friend bool operator==(const ArcKeep& a, const ArcKeep& b) {
        return a.word_at_ == b.word_at_ && a.words_ == b.words_;
    }

private:
    std::vector<size_t> word_at_;
    std::vector<uint64_t> words_;
};

struct ArcTierCounts {
    size_t kept_vertices = 0;    // raw vertices kept
    size_t stored_vertices = 0;  // after the snap drops repeats
    size_t single_point_arcs = 0;  // arcs whose stored vertices are one point
    int validity_rounds = 0;
    size_t validity_restored = 0;
    size_t degenerate_rings = 0;  // under 3 vertices or of no area, after every round
};

// What a tier holds: the keep marks of a simplified tier (empty for one that
// stores every raw vertex) and its counts.
struct ArcTierState {
    ArcKeep kept;
    ArcTierCounts counts;
};

// The arcs of every admin polygon's rings (the full set), and the minimal
// set over them.
struct AdminArcs {
    // In presentation order; their views read the exact ring store, so only
    // while it lives.
    std::vector<Arc> arcs;
    // Ring r's refs, in the ring's order, are refs[ring_refs[r], ring_refs[r + 1]),
    // the rings numbered as the exact ring store numbers them.
    std::vector<uint32_t> ring_refs;
    std::vector<ArcRef> refs;
    // Parallel to ParsedData::admin_polygons; follows each admin permute.
    std::vector<AdminArcPolygon> polygons;
    // The planet slots' IdAllocator table, its arc_index.osm_ids sidecar.
    std::vector<gc::id_alloc::SidecarSlot> planet_sidecar;
    AdminArcCounts counts;
    AdminArcSlotCounts slot_counts;
    MinimalArcs minimal;
    // Parallel to kArcTiers.
    std::vector<ArcTierState> tiers;
};

// A minimal arc's vertices in its order, read through its parts' views.
class MinimalArcVertices {
public:
    template <class Store>
    MinimalArcVertices(const Store& store, const AdminArcs& arcs, const MinimalArcs& minimal, uint32_t arc)
        : rings_(store.rings.data()),
          vertices_(store.vertices.data()),
          full_(arcs.arcs.data()),
          parts_(minimal.parts.data()),
          part_at_(minimal.part_at.data()),
          arc_(minimal.arcs[arc]) {}
    template <class Store>
    MinimalArcVertices(const Store& store, const AdminArcs& arcs, uint32_t arc)
        : MinimalArcVertices(store, arcs, arcs.minimal, arc) {}

    uint32_t size() const { return arc_.count; }
    PackedLocation operator[](uint32_t i) const {
        const uint32_t places = arc_.count - 1;
        const uint32_t place = arc_.closed ? (arc_.start + i) % places : i;
        const uint32_t* first = part_at_ + arc_.first_part;
        const uint32_t j = static_cast<uint32_t>(std::upper_bound(first, first + arc_.part_count, place) - first) - 1;
        const ArcRef part = parts_[arc_.first_part + j];
        const ArcView& view = full_[part.arc].view;
        const ArcVertices av(vertices_ + rings_[view.ring].begin, rings_[view.ring].count, view);
        const uint32_t k = place - first[j];
        return av[part.reversed ? av.size() - 1 - k : k];
    }

private:
    const RingSpan* rings_;
    const PackedLocation* vertices_;
    const Arc* full_;
    const ArcRef* parts_;
    const uint32_t* part_at_;
    MinimalArc arc_;
};

// The topology of the store's rings: their arcs, in presentation order (the
// Hilbert index of the first vertex, the key, then the vertices), each with
// its view, box and key, and every ring's refs. On at most `threads` threads,
// with the same result on any number. `phase_done` is told as each step ends.
AdminArcs build_arc_topology(const AdminExactRings& store, unsigned threads,
                             const std::function<void(const char*)>& phase_done = {});

struct ArcSlots {
    std::vector<uint32_t> slots;  // by presentation order
    std::vector<gc::id_alloc::SidecarSlot> sidecar;
    AdminArcSlotCounts counts;
};

// Slots for arcs keyed `keys`, in presentation order, against the previous
// build's sidecar at `prev_sidecar` (none when empty): a key that held a slot
// keeps it (the lowest presentation index of a repeated key), the others take
// a slot that was a tombstone in the previous build, then new ones. A slot the
// previous build's arc held and no arc holds now becomes a tombstone, so it is
// reused only the day after.
ArcSlots allocate_arc_slots(const std::vector<uint64_t>& keys, const std::string& prev_sidecar);

// Where a region's arc sidecar sits under its dir, as PR 3 ships them: the
// full set's with the exact tier, the minimal set's with admin-minimal.
std::string arc_sidecar_path(const std::string& region_dir, ArcSet set);

// The minimal set over the full set `arcs` of the store: the rings of the
// polygons `in_minimal` marks (parallel to the store's polygons), cut only
// where they meet one another. A full junction that two of the arcs those
// rings use end at, and no third, joins those two, and so on along the
// chain; a chain that closes on itself is one closed arc. The arcs come in
// the full set's canonical form and presentation order, so they are those
// build_arc_topology finds for the minimal polygons alone.
MinimalArcs build_minimal_arcs(const AdminArcs& arcs, const AdminExactRings& store,
                               const std::vector<bool>& in_minimal, unsigned threads);

// Tier `tier` of its set of `arcs`. A simplified tier keeps each arc's ends
// and the vertices Douglas-Peucker keeps at the tier's tolerance in degrees at
// the arc's first latitude (meters_to_degrees), capped at cap_factor times
// the square root of the smallest area of a ring of the set that uses the
// arc; an arc whose ends meet keeps at least 4. It then gives vertices back
// to a ring of under 3 vertices or of no area once snapped: the farthest
// dropped one of its arc that strays most, for up to kArcValidityRounds
// rounds. A tier with no tolerance keeps everything and counts nothing.
// `phase_done` is told as each step ends.
ArcTierState build_arc_tier(const AdminArcs& arcs, const AdminExactRings& store, const ArcTier& tier,
                            unsigned threads, const std::function<void(const char*)>& phase_done = {});

// A vertex a tier stores: at `raw` among its arc's raw vertices, snapped.
struct StoredVertex {
    uint32_t raw;
    PackedLocation at;
};

// The vertices `tier` (as `state` holds it) stores of arc `arc` of its set:
// every raw vertex as it is when the tier simplifies nothing, else the kept
// ones snapped to the tier's grid, a vertex that snaps onto the one before it
// left out (the last raw vertex taking that one's place, so the arc still
// ends there) unless the arc is all one point.
void tier_vertices(const AdminArcs& arcs, const AdminExactRings& store, const ArcTier& tier,
                   const ArcTierState& state, uint32_t arc, std::vector<StoredVertex>& out);

// The arc stage over the planet's polygons: the topology, then the full set's
// planet slots against the sidecar under `prev_planet_dir` (a cold start when
// empty), every polygon's rings, the minimal set of the polygons `in_minimal`
// marks and its planet slots, and every tier. Logs its steps and counts.
AdminArcs build_admin_arcs(const AdminExactRings& store, const std::vector<bool>& in_minimal,
                           const std::string& prev_planet_dir, unsigned threads);

// Ring `ring` of the store as set `set`'s refs chain its arcs: each arc
// walked as its ref says, a junction two arcs share once, ending where it
// starts. A ring of no minimal polygon has no minimal refs and no vertices.
std::vector<PackedLocation> rebuild_ring(const AdminArcs& arcs, const AdminExactRings& store, ArcSet set,
                                         uint32_t ring);

// Whether two closed rings (each ending on its first vertex) are one cycle,
// from any vertex and in either direction.
bool same_closed_ring(const PackedLocation* a, uint32_t a_count, const PackedLocation* b, uint32_t b_count);

// build-index's arc dirs, refused before any work: --arcs-out new or empty and
// apart from the output dir (which publishes whatever it holds), --prev-arcs
// only with it, and holding the planet's sidecar of each set.
void require_arc_dirs(const std::string& output_dir, const std::string& arcs_out_dir,
                      const std::string& prev_arcs_dir);
