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
// vertices given back where a ring would degenerate, two simplified segments
// cross or a polygon's rings stop nesting. build-index runs the arc stage
// (build_admin_arcs) on every build with admin polygons: every dir that holds
// admin geometry holds it as arcs.
#pragma once

#include <algorithm>
#include <climits>
#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "arc_blocks.h"
#include "geometry.h"
#include "id_allocator.h"
#include "packed_location.h"
#include "polygon_rings.h"
#include "types.h"

struct AdminExactRings;  // parsed_data.h

// The stage's thread cap: the topology's transients grow with the threads.
constexpr unsigned kArcTopologyMaxThreads = 32;

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
    size_t previous_sidecar_missing = 0;  // 1: a previous build was given without this sidecar
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
    size_t turnback_junctions = 0;  // of two core arc ends, kept: a minimal ring turns back there
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
    size_t crossings_simplified = 0;  // segment pairs simplification made meet, before the repair
    int repair_rounds = 0;
    size_t repair_restored = 0;
    size_t nesting_restored = 0;  // vertices given back to polygons whose rings stopped nesting
    int e7_grid_rounds = 0;
    size_t e7_grid_arcs = 0;  // arcs stored on OSM's own grid (arc_grid), some vertices only
    size_t crossings_left = 0;
    size_t unrepairable_segments = 0;  // in a crossing left, with no dropped vertex
    size_t raw_crossings = 0;  // segment pairs meeting whose raw spans already meet
    size_t rings_nest_introduced = 0;  // polygons whose rings nest in exact, not here
};

// Which vertices of a simplified tier's arc stay on OSM's own e7 grid
// instead of the tier's: its first and last (each a junction other arcs end
// at too) and the ones between. A polygon whose rings stop nesting only once
// snapped, with no dropped vertex to give back, keeps its arcs and their
// junctions on the e7 grid, and so does every arc at one of those junctions
// at that end: a junction is one point in every arc that ends there.
namespace arc_grid {
constexpr uint8_t kE7First = 1;
constexpr uint8_t kE7Last = 2;
constexpr uint8_t kE7Between = 4;
constexpr uint8_t kE7Whole = kE7First | kE7Last | kE7Between;
}  // namespace arc_grid

// What a tier holds: the keep marks of a simplified tier (empty for one that
// stores every raw vertex), each arc's arc_grid marks (empty, or one per arc
// of a simplified tier) and its counts.
struct ArcTierState {
    ArcKeep kept;
    std::vector<uint8_t> e7_grid;
    ArcTierCounts counts;

    uint8_t grid_of(uint32_t arc) const { return e7_grid.empty() ? 0 : e7_grid[arc]; }
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
    // The planet slots' IdAllocator table, its arc_index.osm_ids sidecar.
    std::vector<gc::id_alloc::SidecarSlot> planet_sidecar;
    AdminArcCounts counts;
    AdminArcSlotCounts slot_counts;
    MinimalArcs minimal;
    // Parallel to kArcTiers.
    std::vector<ArcTierState> tiers;
    size_t rings_nest_raw = 0;  // polygons whose exact rings don't nest (rings_nest)
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
// previous build's arcs held and no arc holds now, a repeated key's other
// slots included, becomes a tombstone, so it is reused only the day after.
// A previous build without the sidecar (none at `prev_sidecar`) leaves every
// slot cold, as one without the admin sidecar does, and the counts say so; a
// sidecar there that can't be read is refused.
ArcSlots allocate_arc_slots(const std::vector<uint64_t>& keys, const std::string& prev_sidecar);

// The log lines of a region's slot allocation for `set` against
// `prev_sidecar`, when a previous build was given: the cold start's, and the
// ARC_CHECK count o11y reports.
std::string arc_slot_start_lines(const std::string& region, ArcSet set, const std::string& prev_sidecar,
                                 const AdminArcSlotCounts& counts);

// Where a region's arc sidecar sits under its dir: the full set's with the
// exact tier, the minimal set's with admin-minimal.
std::string arc_sidecar_path(const std::string& region_dir, ArcSet set);

// The minimal set over the full set `arcs` of the store: the rings of the
// polygons `in_minimal` marks (parallel to the store's polygons), cut only
// where they meet one another. A full junction that two of the arcs those
// rings use end at, and no third, joins those two, and so on along the
// chain; a chain that closes on itself is one closed arc. The arcs come in
// the full set's canonical form and presentation order, so they are those
// build_arc_topology finds for the minimal polygons alone, but for one
// case: where a minimal ring turns back along its arc at such a junction
// (a spur's tip that only another polygon makes a junction), the junction
// stays, since a chain through it would lose that ring.
MinimalArcs build_minimal_arcs(const AdminArcs& arcs, const AdminExactRings& store,
                               const std::vector<bool>& in_minimal, unsigned threads);

// The polygons of the store whose exact rings nest (rings_nest): true for
// one with no holes.
std::vector<bool> exact_rings_nest(const AdminExactRings& store, unsigned threads);

// Tier `tier` of its set of `arcs`. A simplified tier keeps each arc's ends
// and the vertices Douglas-Peucker keeps at the tier's tolerance in degrees at
// the arc's first latitude (meters_to_degrees), capped at cap_factor times
// the square root of the smallest area of a ring of the set that uses the
// arc; an arc whose ends meet keeps at least 4. It then gives vertices back:
// to a ring of under 3 vertices or of no area once snapped, the farthest
// dropped one of its arc that strays most, for up to kArcValidityRounds
// rounds; and for up to kArcRepairRounds rounds, where two snapped segments
// meet away from a shared end but their raw spans don't, the farthest dropped
// one of each span, and to a polygon whose rings nest exactly
// (`nests_exact`, exact_rings_nest) but not once stored, the farthest dropped
// one of each span of a ring whose box holds a vertex of another ring. A
// polygon whose rings still don't nest then keeps its arcs on the e7 grid
// (arc_grid), round after round while that changes anything. A tier with no
// tolerance keeps everything and counts nothing.
// `phase_done` is told as each step ends.
ArcTierState build_arc_tier(const AdminArcs& arcs, const AdminExactRings& store, const ArcTier& tier,
                            const std::vector<bool>& nests_exact, unsigned threads,
                            const std::function<void(const char*)>& phase_done = {});

// A coordinate on a grid of `step` e7 units, half up: floordiv(c + step / 2, step).
inline int32_t snap(int32_t c, int32_t step) {
    const int64_t x = int64_t(c) + step / 2;
    int64_t q = x / step;
    if (x % step != 0 && x < 0) q--;
    return static_cast<int32_t>(q * step);
}

inline PackedLocation snapped(PackedLocation p, int32_t step) { return {snap(p.lat_e7, step), snap(p.lon_e7, step)}; }

// A vertex a tier stores: at `raw` among its arc's raw vertices, snapped.
struct StoredVertex {
    uint32_t raw;
    PackedLocation at;
};

// The grid step, in e7 units, a tier stores raw vertex i of an arc of n
// raw vertices on, given the arc's arc_grid marks.
inline int32_t vertex_grid_e7(const ArcTier& tier, uint8_t e7_grid, uint32_t i, uint32_t n) {
    const uint8_t mark = i == 0 ? arc_grid::kE7First : i + 1 == n ? arc_grid::kE7Last : arc_grid::kE7Between;
    return (e7_grid & mark) ? 1 : tier.grid_e7;
}

// The grid exponent of a tier's block of an arc: OSM's own once any of its
// vertices stays there.
inline uint8_t arc_grid_exponent(const ArcTier& tier, uint8_t e7_grid) {
    return arc_blocks::grid_exponent(e7_grid ? 1 : tier.grid_e7);
}

// The vertices `tier` (as `state` holds it) stores of arc `arc` of its set:
// every raw vertex as it is when the tier simplifies nothing, else the kept
// ones snapped (each to vertex_grid_e7's step), a vertex that snaps onto the
// one before it left out (the last raw vertex taking that one's place, so
// the arc still ends there) unless the arc is all one point.
void tier_vertices(const AdminArcs& arcs, const AdminExactRings& store, const ArcTier& tier,
                   const ArcTierState& state, uint32_t arc, std::vector<StoredVertex>& out);
// The same, the arc's raw vertices left in `raw`.
void tier_vertices(const AdminArcs& arcs, const AdminExactRings& store, const ArcTier& tier,
                   const ArcTierState& state, uint32_t arc, std::vector<PackedLocation>& raw,
                   std::vector<StoredVertex>& out);

// Ring `ring` of the store as `tier` (as `state` holds it) stores it, into
// `out`: its refs' stored vertices chained, each junction once, its first
// vertex again at its end; empty for a ring the tier's set doesn't hold.
// `raw` and `stored` are scratch.
void tier_ring(const AdminArcs& arcs, const AdminExactRings& store, const ArcTier& tier, const ArcTierState& state,
               uint32_t ring, std::vector<PackedLocation>& out, std::vector<PackedLocation>& raw,
               std::vector<StoredVertex>& stored);

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

// --- The encoded arcs: what the stage leaves the build ---

// Bytes [offset, offset + size) of a buffer.
struct BlockSpan {
    uint32_t offset = 0;
    uint32_t size = 0;
};
inline bool operator==(const BlockSpan& a, const BlockSpan& b) { return a.offset == b.offset && a.size == b.size; }

// A polygon's rings as the arc sets hold them: its rings' numbers among the
// sets' ring refs, outer first, and its ring block in each set (empty in a
// set none of its rings is in). Follows each admin permute; a tombstone's
// is empty.
struct AdminArcPolygon {
    uint32_t first_ring = 0;
    uint32_t ring_count = 0;
    BlockSpan blocks[2];  // by ArcSet, in ArcSetBlocks::ring_blocks
};
inline bool operator==(const AdminArcPolygon& a, const AdminArcPolygon& b) {
    return a.first_ring == b.first_ring && a.ring_count == b.ring_count && a.blocks[0] == b.blocks[0] &&
           a.blocks[1] == b.blocks[1];
}

// Blocks back to back: block i is bytes[at[i], at[i + 1]).
struct ArcBlocks {
    std::vector<uint8_t> bytes;
    std::vector<uint32_t> at;
    size_t size() const { return at.empty() ? 0 : at.size() - 1; }
    const uint8_t* data(size_t i) const { return bytes.data() + at[i]; }
    size_t bytes_of(size_t i) const { return at[i + 1] - at[i]; }
};

// One arc set of a region: its arcs, in the planet's presentation order, by
// their raw boxes, keys and planet slots and the region's slots (the
// planet's on the planet) and slot table; each ring's refs to them, the
// rings numbered as AdminArcPolygon numbers them; and the polygons' ring
// blocks.
struct ArcSetBlocks {
    std::vector<ArcBox> boxes;
    std::vector<uint64_t> keys;
    std::vector<uint32_t> planet_slots;
    std::vector<uint32_t> slots;
    std::vector<gc::id_alloc::SidecarSlot> sidecar;
    AdminArcSlotCounts slot_counts;
    std::vector<uint32_t> ring_refs;  // ring r's refs: refs[ring_refs[r], ring_refs[r + 1])
    std::vector<ArcRef> refs;
    std::vector<uint8_t> ring_blocks;

    size_t arcs() const { return keys.size(); }
    bool has_ring(uint32_t r) const { return ring_refs[r + 1] > ring_refs[r]; }
};

// A region's encoded arcs: the sets, each tier's arc blocks (parallel to
// kArcTiers, over its set's arcs), and every polygon's place in them
// (parallel to ParsedData::admin_polygons). Empty unless the arc stage ran.
struct AdminArcBlocks {
    std::vector<AdminArcPolygon> polygons;
    ArcSetBlocks sets[2];  // by ArcSet
    ArcBlocks tiers[kArcTierCount];

    const ArcSetBlocks& set(ArcSet s) const { return sets[static_cast<size_t>(s)]; }
    ArcSetBlocks& set(ArcSet s) { return sets[static_cast<size_t>(s)]; }
    // The bytes it holds.
    uint64_t bytes() const;
};

// The arc stage's encoding of `arcs` (build_admin_arcs) of the store: every
// tier's arc blocks and every polygon's ring blocks in both sets, with the
// planet's slots. Aborts (std::runtime_error) unless every ring's refs chain
// closes, in every tier's arc blocks too, every arc block decodes to the
// vertices its tier stores, those are raw vertices of the arc each snapped
// to its own grid (vertex_grid_e7, within kChunkBoxSlackE7 of them), every
// ref names a live slot of its arc, and
// every ring of every polygon in each set comes back exactly from its ring
// block's refs to the exact tier's arc blocks (check_lossless). Logs its
// counts as ARC_CHECK lines.
AdminArcBlocks encode_admin_arcs(const AdminArcs& arcs, const AdminExactRings& store, unsigned threads);

// Encodes every polygon's ring block in set `set` with the set's slots
// (ArcSetBlocks::slots), cut where its refs' planet slots say. Aborts unless
// every ref names a live slot holding its arc's key.
void encode_ring_blocks(AdminArcBlocks& blocks, ArcSet set, unsigned threads);

// Losslessness: aborts unless, for every polygon of the store and each set
// that holds its rings, its ring block decodes to as many rings, each of
// whose refs, read through the slots to the exact tier's arc blocks (a
// minimal arc's through its full arcs), chain into that exact ring of the
// store, from some vertex in either direction.
void check_lossless(const AdminArcs& arcs, const AdminArcBlocks& blocks, const AdminExactRings& store, unsigned threads);

// The arc of each of the set's slots, UINT32_MAX for a tombstone.
std::vector<uint32_t> arc_at_slot(const ArcSetBlocks& set);

// A continent's share of the planet's encoded arcs: those its polygons
// (`polygons`, planet ids ascending) use, in the planet's order, their
// blocks, boxes, keys and planet slots copied, and its polygons' rings'
// refs. It has no slots or ring blocks until assign_region_arc_slots.
AdminArcBlocks subset_admin_arcs(const AdminArcBlocks& planet, const std::vector<uint32_t>& polygons, unsigned threads);

// A continent's own slots for each set, against its previous build's
// sidecars under `prev_region_dir` (none when empty), its arcs presented in
// the planet's order; then its ring blocks with them. Logs the slots.
void assign_region_arc_slots(AdminArcBlocks& blocks, const std::string& prev_region_dir, const std::string& region,
                             unsigned threads);

// A tier dir's arc files: arc_index.bin (each slot's block offset in
// arc_vertices.bin, a u32) and arc_vertices.bin (the blocks in slot order, a
// tombstone's empty).
void write_arc_files(const std::string& dir, const ArcSetBlocks& set, const ArcBlocks& tier);

// The admin_polygons.bin and admin_rings.bin of `dirs`, the same bytes in
// each, in set `set`: records[slot] with its ring block (that of polygon
// polygon_of[slot]) laid out in slot order, a tombstone record (NO_DATA)
// holding none. Returns admin_rings.bin's size.
size_t write_ring_files(const std::vector<std::string>& dirs, const AdminArcBlocks& blocks, ArcSet set,
                        std::vector<AdminPolygon> records, const std::vector<uint32_t>& polygon_of);
