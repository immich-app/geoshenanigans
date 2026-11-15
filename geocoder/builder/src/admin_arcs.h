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
#include <stdexcept>
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

// The identities an arc whose arc_key changed can still keep its slot by,
// one fallback pass each (IdAllocator::allocate_all), in this order: its way
// set, the OSM ways along its edges in every ring that runs along it, which
// stays when vertices move, its ends included; and its ends, which stay when
// its interior changes, ways and all. 56-bit hashes, or kNoFallbackKey (an
// arc of no ring with way runs; a tombstone's slot).
struct ArcFallbackKeys {
    uint64_t way_set = gc::id_alloc::kNoFallbackKey;
    uint64_t ends = gc::id_alloc::kNoFallbackKey;
};
static_assert(sizeof(ArcFallbackKeys) == 16, "an arc fallback sidecar slot is 16 bytes");
inline bool operator==(const ArcFallbackKeys& a, const ArcFallbackKeys& b) {
    return a.way_set == b.way_set && a.ends == b.ends;
}

// The way-set key of the ways `ways`, ascending, each once; none for none.
inline uint64_t arc_way_set_key(const int64_t* ways, size_t n) {
    if (n == 0) return gc::id_alloc::kNoFallbackKey;
    uint64_t h = mix64(uint64_t(n) + 0x632be59bd9b4e019ull);
    for (size_t i = 0; i < n; i++) h = mix64(h ^ static_cast<uint64_t>(ways[i]));
    return h & 0x00FFFFFFFFFFFFFFull;
}

// The ends key of an arc from vertex `first` to `last` (its smallest vertex
// twice when closed).
inline uint64_t arc_ends_key(uint64_t first, uint64_t last, bool closed) {
    uint64_t h = mix64(first + 0x8cb92ba72f3d8dd7ull);
    h = mix64(h ^ last);
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
    ArcFallbackKeys fallback;
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
    size_t arcs_without_ways = 0;  // of rings with no way runs: no way-set key
    size_t shared_way_sets = 0;    // arcs whose way-set key an earlier one has
    size_t shared_ends = 0;        // arcs whose ends key an earlier one has
};

struct AdminArcSlotCounts {
    size_t kept = 0;        // the previous build's slot of the arc's key
    size_t by_way_set = 0;  // the previous slot of an arc of its way set (a fallback pass)
    size_t by_ends = 0;     // the previous slot of an arc of its ends (the next)
    size_t recycled = 0;    // a slot that was a tombstone in the previous build
    size_t appended = 0;
    size_t tombstones = 0;
    size_t previous_sidecar_missing = 0;  // 1: a previous build was given without this sidecar
    // 1: its arc slots came without their fallback keys (arc_index.fallback.osm_ids)
    size_t previous_fallback_sidecar_missing = 0;
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
    ArcFallbackKeys fallback;  // its way set its parts' ways
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
    size_t shared_way_sets = 0;
    size_t shared_ends = 0;
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
    // Arc `arc`'s marks as `from`, the marks of the same arcs, holds them.
    void copy_arc(const ArcKeep& from, uint32_t arc) {
        check_same_arcs(from);
        copy_words(from, arc);
    }
    // copy_arc for each of `arcs`, `from`'s arcs compared with these once,
    // for all of them, before any is copied.
    void copy_arcs(const ArcKeep& from, const std::vector<uint32_t>& arcs) {
        check_same_arcs(from);
        for (uint32_t a : arcs) copy_words(from, a);
    }
    friend bool operator==(const ArcKeep& a, const ArcKeep& b) {
        return a.word_at_ == b.word_at_ && a.words_ == b.words_;
    }

private:
    // Whether `from` marks the same arcs, as many vertices each: a compare of
    // every arc's offset.
    void check_same_arcs(const ArcKeep& from) const {
        if (from.word_at_ != word_at_) throw std::logic_error("arc keep marks of other arcs");
    }
    // Arc `arc`'s words from `from`, which check_same_arcs passed.
    void copy_words(const ArcKeep& from, uint32_t arc) {
        std::copy(from.words_.begin() + static_cast<std::ptrdiff_t>(word_at_[arc]),
                  from.words_.begin() + static_cast<std::ptrdiff_t>(word_at_[arc + 1]),
                  words_.begin() + static_cast<std::ptrdiff_t>(word_at_[arc]));
    }

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
    // A tier that coarsens (ArcTier::coarsens), built on its fine grid first:
    size_t fine_degenerate_rings = 0;  // rings degenerate there, which the coarsening leaves as they are
    size_t fine_crossings_left = 0;    // crossings left there
    size_t crossings_coarse = 0;       // segment pairs meeting once coarsened, before the repair on its own grid
    int coarse_rounds = 0;             // rounds that keep collapsing rings fine
    size_t fine_rings = 0;             // rings kept fine: valid on the fine grid, collapsing on the tier's
    size_t fine_junctions = 0;         // junctions kept fine in every arc that ends there
    size_t fine_arcs = 0;              // arcs stored on the fine grid (arc_grid), none on OSM's
    int put_back_rounds = 0;           // rounds that put arcs back as the fine build left them
    size_t put_back_arcs = 0;          // arcs of crossings and of polygons not nesting put back so
    int collapsed_put_back_rounds = 0;     // last rounds that put back rings collapsing or folding still, all fine
    size_t collapsed_put_back_rings = 0;   // the rings they put back
    size_t folded_put_back_rings = 0;      // of them, rings with an area that fold: turned over where the fine one isn't
    size_t folds_left = 0;                 // rings valid on the fine grid that fold still
};

// Which vertices of a simplified tier's arc are stored on another grid than
// the tier's: its first and last (each a junction other arcs end at too) and
// the ones between, each on OSM's own e7 grid, or else on the tier's fine
// grid (ArcTier::fine_grid_e7).
//
// A polygon whose rings stop nesting only once snapped, with no dropped
// vertex to give back, keeps its arcs and their junctions on the e7 grid,
// and so does every arc at one of those junctions at that end: a junction is
// one point in every arc that ends there.
//
// On a tier that coarsens, a junction is on the tier's grid in every arc
// that ends there, and an arc's other vertices on the grid of the arc: the
// tier's, or the fine one for an arc kept fine. A ring valid on the fine grid
// that the tier's would collapse keeps its arcs fine and its junctions fine,
// and every arc that ends at a junction kept fine is kept fine too, so that
// it stores the junction as the others do; its other end stays on the tier's
// grid, which the fine one holds exactly, unless that is a junction kept fine
// too.
namespace arc_grid {
constexpr uint8_t kE7First = 1;
constexpr uint8_t kE7Last = 2;
constexpr uint8_t kE7Between = 4;
constexpr uint8_t kE7Whole = kE7First | kE7Last | kE7Between;
constexpr uint8_t kFineFirst = 8;
constexpr uint8_t kFineLast = 16;
constexpr uint8_t kFineBetween = 32;  // the arc is kept fine
constexpr uint8_t kFineWhole = kFineFirst | kFineLast | kFineBetween;
}  // namespace arc_grid

// What a tier holds: the keep marks of a simplified tier (empty for one that
// stores every raw vertex), each arc's arc_grid marks (empty, or one per arc
// of a simplified tier) and its counts. A tier that coarsens holds too the
// junctions it keeps fine (their vertex_key, ascending) and, by ring of the
// store, which rings of its set it found valid on its fine grid: those its
// grid must not collapse.
struct ArcTierState {
    ArcKeep kept;
    std::vector<uint8_t> grid;
    std::vector<uint64_t> fine_junctions;
    std::vector<uint8_t> fine_valid;
    ArcTierCounts counts;

    uint8_t grid_of(uint32_t arc) const { return grid.empty() ? 0 : grid[arc]; }
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
    // Arc a's member ways (its way set), ascending, each once:
    // ways[way_at[a], way_at[a + 1]). The minimal set's way sets are its
    // parts'; build_admin_arcs frees them once it has those.
    std::vector<uint32_t> way_at;
    std::vector<int64_t> ways;
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
// its view, box, key and fallback keys, its way set the ways of the store's
// way runs along it in every ring, and every ring's refs. On at most
// `threads` threads, with the same result on any number and in any ring
// order. Aborts (std::logic_error) unless each ring with way runs has them
// over all of its edges. `phase_done` is told as each step ends.
AdminArcs build_arc_topology(const AdminExactRings& store, unsigned threads,
                             const std::function<void(const char*)>& phase_done = {});

struct ArcSlots {
    std::vector<uint32_t> slots;  // by presentation order
    std::vector<gc::id_alloc::SidecarSlot> sidecar;
    AdminArcSlotCounts counts;
};

// Slots for arcs keyed `keys`, in presentation order, against the previous
// build's sidecar at `prev_sidecar` (none when empty): a key that held a slot
// keeps it (the lowest presentation index of a repeated key). The others,
// given their fallback keys (`fallback`, parallel to `keys`) and the
// previous build's (its fallback sidecar at `prev_fallback_sidecar`), take
// the previous slot no key held whose arc had their way set, then their
// ends, where exactly one such arc and one such slot have that key; then a
// slot that was a tombstone in the previous build, then new ones. A slot the
// previous build's arcs held and no arc holds now, a repeated key's other
// slots included, becomes a tombstone, so it is reused only the day after.
// A previous build without the sidecar (none at `prev_sidecar`) leaves every
// slot cold, as one without the admin sidecar does, and one with it but
// without the fallback sidecar (none at `prev_fallback_sidecar`, the first
// night with fallback keys) matches by keys alone; the counts say so. A
// sidecar there that can't be read (a fallback sidecar of another slot
// count too), or a path that can't be looked up, is refused.
ArcSlots allocate_arc_slots(const std::vector<uint64_t>& keys, const std::string& prev_sidecar,
                            const std::vector<ArcFallbackKeys>& fallback = {},
                            const std::string& prev_fallback_sidecar = "");

// The log lines of a region's slot allocation for `set` against
// `prev_sidecar`, when a previous build was given: the cold start's, and the
// ARC_CHECK counts o11y reports; with the sidecar there, the fallback
// passes' too.
std::string arc_slot_start_lines(const std::string& region, ArcSet set, const std::string& prev_sidecar,
                                 const AdminArcSlotCounts& counts);

// Where a region's arc sidecars sit under its dir: the full set's with the
// exact tier, the minimal set's with admin-minimal. arc_index.osm_ids holds
// the slots (IdAllocator's), arc_index.fallback.osm_ids their arcs' fallback
// keys.
std::string arc_sidecar_path(const std::string& region_dir, ArcSet set);
std::string arc_fallback_sidecar_path(const std::string& region_dir, ArcSet set);

// arc_index.fallback.osm_ids: a header of u32 magic, u32 version and u32
// slot count, then each slot's ArcFallbackKeys (16 B) in slot order,
// kNoFallbackKey twice for a tombstone. Its suffix keeps it out of the
// client files, as every strategy-2 sidecar is.
constexpr uint32_t kArcFallbackSidecarMagic = 0xFA11BAC5;
constexpr uint32_t kArcFallbackSidecarVersion = 1;

struct ArcSetBlocks;

// Writes set's fallback sidecar at `path`: its arcs' fallback keys at their
// slots, among all of its slot table's.
void write_arc_fallback_sidecar(const std::string& path, const ArcSetBlocks& set);

// The fallback keys of a fallback sidecar of `slots` slots (those of the
// arc_index.osm_ids beside it). Refused (std::runtime_error) unless it reads
// whole, as one of that many slots.
std::vector<ArcFallbackKeys> read_arc_fallback_sidecar(const std::string& path, size_t slots);

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
//
// A tier that coarsens does all that on its fine grid, notes the rings valid
// there, then snaps every arc and junction to its own grid and keeps
// collapsing rings fine: each of those rings that its grid leaves with under
// 3 vertices or no area gets its arcs and its junctions fine, and every arc
// that ends at one of those junctions is kept fine, round after round over
// the rings of the arcs a round changed, until none collapses. Each round
// only moves arcs and junctions to the fine grid, where every such ring is
// valid, so it ends, and no such ring is lost. The repair then runs again on
// the tier's own grid, every vertex it gives back on its arc's grid, keeping
// the rings of the arcs each of its rounds changes fine where they collapse.
// The arcs of each crossing it leaves, and of each polygon whose rings then
// stop nesting, are put back as the fine build left them (its vertices, on
// the fine grid, their junctions fine), round after round: two arcs put back
// cross only where the fine build's do. Last, a ring valid on the fine grid
// that collapses still with every arc and junction of it fine (the repair
// gave it a vertex back that takes its area away) has its arcs put back too,
// and so has one that folds where the fine one doesn't: it winds the other
// way, or a loop of it split off where it comes back to a vertex winds
// against the rest and lies outside it (a strip narrower than the grid
// turned over, which the ring and its neighbours on both sides would all
// cover; no crossing shows it). What that makes is put back in turn.
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
inline int32_t vertex_grid_e7(const ArcTier& tier, uint8_t grid, uint32_t i, uint32_t n) {
    const bool first = i == 0, last = i + 1 == n;
    const uint8_t e7 = first ? arc_grid::kE7First : last ? arc_grid::kE7Last : arc_grid::kE7Between;
    const uint8_t fine = first ? arc_grid::kFineFirst : last ? arc_grid::kFineLast : arc_grid::kFineBetween;
    return (grid & e7) ? 1 : (grid & fine) ? tier.fine_grid_e7 : tier.grid_e7;
}

// The grid exponent of a tier's block of an arc: the finest any of its
// vertices is stored on.
inline uint8_t arc_grid_exponent(const ArcTier& tier, uint8_t grid) {
    return arc_blocks::grid_exponent((grid & arc_grid::kE7Whole)     ? 1
                                     : (grid & arc_grid::kFineWhole) ? tier.fine_grid_e7
                                                                     : tier.grid_e7);
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
// their raw boxes, keys, fallback keys and planet slots and the region's slots (the
// planet's on the planet) and slot table; each ring's refs to them, the
// rings numbered as AdminArcPolygon numbers them; and the polygons' ring
// blocks.
struct ArcSetBlocks {
    std::vector<ArcBox> boxes;
    std::vector<uint64_t> keys;
    std::vector<ArcFallbackKeys> fallback_keys;
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
// block's refs to the exact tier's arc blocks (check_lossless); and, in a
// tier that coarsens, unless every ring valid on its fine grid is valid as
// its arc blocks store it and every arc ending at a junction kept fine is
// stored on the fine grid or finer. Logs its counts as ARC_CHECK lines.
AdminArcBlocks encode_admin_arcs(const AdminArcs& arcs, const AdminExactRings& store, unsigned threads);

// ring_folds_over's scratch, kept from one call to the next.
struct FoldScratch {
    // A point beside the midpoint (lat, lon, in doubled units) of an edge
    // that isn't on one meridian, and the edges a ray from it towards +lat
    // crosses, each +1 running east and -1 west: north of the midpoint
    // (crossed) and through it (through).
    struct Probe {
        int64_t lat, lon;
        int crossed, through;
    };
    std::vector<uint64_t> keys;
    std::vector<PackedLocation> ring, rest;  // the ring without its spikes; what is left of it split
    std::vector<Probe> probes;
};

// Whether `ring` (each vertex once where its neighbour repeats it, without
// the closing one) folds over itself: it winds round some point one way and
// round another the other way. So it does where a strip narrower than the
// grid is turned over between the two vertices its ends pinch to, a loop
// winding against the ring outside it, which the polygon covers and so do
// the neighbours whose borders bound it. A spur, a pinched neck, a lobe
// inside a lobe and a hole the ring pinches off (which it winds round not at
// all) fold nothing, wherever the ring starts. A ring that comes back to no
// vertex folds nothing here. Its spikes taken off first (a vertex it goes
// out to and comes back from along one line, which changes nothing it winds
// round), split at each vertex it comes back to, the ring is loops: when
// those with an area all wind one way, it winds round no point the other
// way. Otherwise it is stretches from one vertex it comes back to to the
// next, and every part of the plane it bounds lies beside one of them. Where
// the ring meets itself only at those vertices or along whole edges, so that
// what lies beside a stretch is the same all along it, it folds exactly
// when it winds round a point beside one stretch's first edge off one
// meridian one way and round a point beside another's (or the other side of
// the same one) the other way; where it meets itself elsewhere too, an end
// on another edge or a crossing (the repair puts those the tier's grid
// makes back as the fine build has them), a fold it finds is still one, but
// it may miss some. Its windings round all the probes take one pass over its
// edges, each going only to the probes within its longitudes.
bool ring_folds_over(const std::vector<PackedLocation>& ring, FoldScratch& scratch);

// The checks tier t, one that coarsens, aborts on (std::runtime_error) over
// its arc blocks `blocks`: every ring of its set that was valid on its fine
// grid (ArcTierState::fine_valid) is valid as the blocks store it, of 3
// vertices or more and an area; and every arc that ends at a junction kept
// fine (ArcTierState::fine_junctions) is stored on the fine grid or finer, so
// that it holds the junction where the arcs kept fine do.
void check_coarsened_tier(const AdminArcs& arcs, const AdminExactRings& store, size_t t, const ArcBlocks& blocks,
                          unsigned threads);

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
// blocks, boxes, keys, fallback keys and planet slots copied, and its polygons' rings'
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
// each, in set `set`: records[slot] as an AdminPolygonRecord with its ring
// block (that of polygon polygon_of[slot]) laid out in slot order, a
// tombstone (polygon_of NO_DATA) holding an empty one. Returns
// admin_rings.bin's size.
size_t write_ring_files(const std::vector<std::string>& dirs, const AdminArcBlocks& blocks, ArcSet set,
                        const std::vector<AdminPolygon>& records, const std::vector<uint32_t>& polygon_of);
