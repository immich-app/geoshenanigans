#include "admin_arcs.h"

#include <algorithm>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <stdexcept>

#include "output_dir.h"
#include "parallel.h"
#include "parsed_data.h"

namespace {

// The vertex occurrences the scatter counts per partition at a time: fixed,
// so where an occurrence lands doesn't depend on the threads.
constexpr size_t kScatterChunk = size_t(1) << 20;
// A scattered occurrence is its store index, this bit set where it starts its
// ring (its neighbour before it is the ring's last).
constexpr uint32_t kRingStart = 0x80000000u;

// The store's rings as the stage reads them: closed, back to back in store
// order, every index fitting the scatter's 31 bits.
void require_closed_rings_in_order(const AdminExactRings& store) {
    if (store.vertices.size() >= kRingStart)
        throw std::length_error("admin exact rings past 2^31 vertices: the arc scatter holds an index in 31 bits");
    size_t next = 0;
    for (const RingSpan& ring : store.rings) {
        if (ring.begin < next || ring.count < 2 || size_t(ring.begin) + ring.count > store.vertices.size())
            throw std::logic_error("admin exact rings not in store order");
        if (store.vertices[ring.begin] != store.vertices[ring.begin + ring.count - 1])
            throw std::logic_error("an admin exact ring doesn't close");
        next = size_t(ring.begin) + ring.count;
    }
}

// A ring's cycle: its vertices but the closing one, walked from any place in
// either direction.
struct Cycle {
    const PackedLocation* v;
    uint32_t size;
};

struct Walk {
    uint32_t start;
    bool reversed;
};

inline uint64_t walk_key(const Cycle& c, Walk w, uint32_t i) {
    const uint64_t step = i % c.size;
    return vertex_key(c.v[(w.reversed ? uint64_t(w.start) + c.size - step : uint64_t(w.start) + step) % c.size]);
}

// The first `count` vertices of two walks against each other: < 0, 0, > 0.
int compare_walks(const Cycle& a, Walk wa, const Cycle& b, Walk wb, uint32_t count) {
    for (uint32_t i = 0; i < count; i++) {
        const uint64_t ka = walk_key(a, wa, i), kb = walk_key(b, wb, i);
        if (ka != kb) return ka < kb ? -1 : 1;
    }
    return 0;
}

uint64_t hash_walk(const Cycle& c, Walk w, uint32_t count, bool closed) {
    uint64_t h = mix64((uint64_t(count) << 1) | (closed ? 1u : 0u));
    for (uint32_t i = 0; i < count; i++) h = mix64(h ^ walk_key(c, w, i));
    return h;
}

// A vertex occurrence and its two neighbours along its ring.
struct Neighbours {
    uint64_t vertex, lo, hi;
};

// Whether the occurrences of one vertex give it other than two distinct
// neighbours. A neighbour equal to the vertex (a coordinate repeated) counts
// as one, so the repeat ends an arc of its own.
bool is_junction(const Neighbours* run, size_t n) {
    uint64_t seen[3];
    unsigned distinct = 0;
    auto add = [&](uint64_t k) {
        for (unsigned i = 0; i < distinct; i++)
            if (seen[i] == k) return;
        seen[distinct++] = k;
    };
    for (size_t i = 0; i < n && distinct < 3; i++) {
        add(run[i].lo);
        if (distinct < 3) add(run[i].hi);
    }
    return distinct != 2;
}

// The junctions: every vertex occurrence scattered once into the partition of
// its vertex, then in each partition each vertex's neighbours sorted together
// and counted.
std::vector<uint64_t> find_junctions(const AdminExactRings& store, unsigned threads, size_t& largest_partition) {
    const std::vector<RingSpan>& rings = store.rings;
    const PackedLocation* v = store.vertices.data();
    const std::vector<size_t> occurrence_at =
        parallel_offsets<size_t>(rings.size(), [&](size_t r) { return size_t(rings[r].count - 1); }, threads);
    const size_t occurrences = occurrence_at.back();
    const size_t chunks = (occurrences + kScatterChunk - 1) / kScatterChunk;

    // Per chunk and partition: its occurrences, then where they go.
    std::vector<uint32_t> at(chunks * kArcPartitions, 0);
    auto each_occurrence = [&](auto&& visit) {
        parallel_for_parts(occurrence_at, kScatterChunk, [&](size_t r, size_t begin, size_t end, size_t global) {
            uint32_t* chunk = at.data() + (global / kScatterChunk) * kArcPartitions;
            for (size_t k = begin; k < end; k++) visit(chunk, static_cast<uint32_t>(rings[r].begin + k), k == 0);
        }, threads);
    };
    each_occurrence([&](uint32_t* chunk, uint32_t i, bool) { chunk[arc_partition(vertex_key(v[i]))]++; });
    std::vector<size_t> partition_at(kArcPartitions + 1, 0);
    for (size_t c = 0; c < chunks; c++)
        for (uint32_t p = 0; p < kArcPartitions; p++) partition_at[p + 1] += at[c * kArcPartitions + p];
    for (uint32_t p = 0; p < kArcPartitions; p++) partition_at[p + 1] += partition_at[p];
    {
        std::vector<size_t> next(partition_at.begin(), partition_at.end() - 1);
        for (size_t c = 0; c < chunks; c++)
            for (uint32_t p = 0; p < kArcPartitions; p++) {
                const uint32_t n = at[c * kArcPartitions + p];
                at[c * kArcPartitions + p] = static_cast<uint32_t>(next[p]);
                next[p] += n;
            }
    }
    std::vector<uint32_t> scattered;
    parallel_resize(scattered, occurrences);
    each_occurrence([&](uint32_t* chunk, uint32_t i, bool ring_start) {
        scattered[chunk[arc_partition(vertex_key(v[i]))]++] = i | (ring_start ? kRingStart : 0);
    });
    at = {};

    // The vertex before a ring's first occurrence is the ring's last.
    auto last_of_ring_from = [&](uint32_t first) {
        const RingSpan& ring = *std::lower_bound(rings.begin(), rings.end(), first,
                                                 [](const RingSpan& s, uint32_t i) { return s.begin < i; });
        return ring.begin + ring.count - 2;
    };
    // Each partition sorted and counted on one thread, in a buffer of that
    // thread's.
    std::vector<std::vector<uint64_t>> found(kArcPartitions);
    std::vector<std::vector<Neighbours>> buffers(threads);
    parallel_for_dynamic(kArcPartitions, 1, [&](size_t begin, size_t end, unsigned worker) {
        std::vector<Neighbours>& neighbours = buffers[worker];
        for (size_t p = begin; p < end; p++) {
            const size_t first = partition_at[p], n = partition_at[p + 1] - first;
            neighbours.resize(n);
            for (size_t k = 0; k < n; k++) {
                const uint32_t s = scattered[first + k], i = s & ~kRingStart;
                const uint64_t before = vertex_key(v[(s & kRingStart) ? last_of_ring_from(i) : i - 1]);
                const uint64_t after = vertex_key(v[i + 1]);
                neighbours[k] = {vertex_key(v[i]), std::min(before, after), std::max(before, after)};
            }
            std::sort(neighbours.begin(), neighbours.end(), [](const Neighbours& a, const Neighbours& b) {
                return a.vertex != b.vertex ? a.vertex < b.vertex : a.lo != b.lo ? a.lo < b.lo : a.hi < b.hi;
            });
            for (size_t k = 0; k < n;) {
                size_t run_end = k + 1;
                while (run_end < n && neighbours[run_end].vertex == neighbours[k].vertex) run_end++;
                if (is_junction(&neighbours[k], run_end - k)) found[p].push_back(neighbours[k].vertex);
                k = run_end;
            }
        }
    }, threads);
    buffers = {};
    scattered = {};
    largest_partition = 0;
    for (uint32_t p = 0; p < kArcPartitions; p++)
        largest_partition = std::max(largest_partition, partition_at[p + 1] - partition_at[p]);
    std::vector<uint64_t> junctions;
    for (const auto& part : found) junctions.insert(junctions.end(), part.begin(), part.end());
    return junctions;
}

// Membership in a set of vertex keys, by open addressing.
class VertexSet {
public:
    explicit VertexSet(const std::vector<uint64_t>& keys) {
        size_t n = 16;
        while (n < 2 * keys.size()) n <<= 1;
        slots_.assign(n, kEmpty);
        mask_ = n - 1;
        for (uint64_t k : keys) {
            if (k == kEmpty) {
                holds_empty_ = true;
                continue;
            }
            size_t i = mix64(k) & mask_;
            while (slots_[i] != kEmpty && slots_[i] != k) i = (i + 1) & mask_;
            slots_[i] = k;
        }
    }

    bool contains(uint64_t k) const {
        if (k == kEmpty) return holds_empty_;
        for (size_t i = mix64(k) & mask_;; i = (i + 1) & mask_) {
            if (slots_[i] == k) return true;
            if (slots_[i] == kEmpty) return false;
        }
    }

private:
    static constexpr uint64_t kEmpty = UINT64_MAX;
    std::vector<uint64_t> slots_;
    size_t mask_ = 0;
    bool holds_empty_ = false;
};

// A ring between two of its junctions (or the whole ring, closed, when it has
// none), in its canonical walk: from the smaller end of the two walks, a
// closed one from its smallest vertex towards the smaller neighbour.
// `reversed` says the ring walks it backwards; `ref` is its place in the refs.
struct Piece {
    uint64_t hash;
    uint32_t ring;
    uint32_t start;
    uint32_t count;
    uint32_t ref;
    bool reversed;
    bool closed;
};

Piece closed_piece(const Cycle& c, uint32_t ring, uint32_t ref) {
    uint64_t smallest = UINT64_MAX;
    for (uint32_t place = 0; place < c.size; place++) smallest = std::min(smallest, vertex_key(c.v[place]));
    const uint32_t count = c.size + 1;
    Walk best{0, false};
    bool found = false;
    for (uint32_t place = 0; place < c.size; place++) {
        if (vertex_key(c.v[place]) != smallest) continue;

        for (bool reversed : {false, true}) {
            const Walk w{place, reversed};
            if (!found || compare_walks(c, w, c, best, count) < 0) best = w;
            found = true;
        }
    }
    return {hash_walk(c, best, count, true), ring, best.start, count, ref, best.reversed, true};
}

Piece open_piece(const Cycle& c, uint32_t ring, uint32_t ref, uint32_t from, uint64_t to) {
    const uint32_t count = static_cast<uint32_t>(to - from + 1);
    const Walk forwards{from, false}, backwards{static_cast<uint32_t>(to % c.size), true};
    const bool reversed = compare_walks(c, backwards, c, forwards, count) < 0;
    const Walk w = reversed ? backwards : forwards;
    return {hash_walk(c, w, count, false), ring, w.start, count, ref, reversed, false};
}

Cycle cycle_of(const AdminExactRings& store, uint32_t ring) {
    const RingSpan& span = store.rings[ring];
    return {store.vertices.data() + span.begin, span.count - 1};
}

}  // namespace

AdminArcs build_arc_topology(const AdminExactRings& store, unsigned threads,
                             const std::function<void(const char*)>& phase_done) {
    auto done = [&](const char* step) {
        if (phase_done) phase_done(step);
    };
    if (threads == 0) threads = parallel_threads();
    require_closed_rings_in_order(store);
    const size_t ring_count = store.rings.size();
    AdminArcs out;

    const std::vector<uint64_t> junctions = find_junctions(store, threads, out.counts.largest_partition);
    out.counts.junctions = junctions.size();
    done("      Admin arcs: junctions");

    // Each ring's junction places, the costliest rings first.
    const VertexSet is_junction_vertex(junctions);
    const std::vector<uint32_t> by_cost =
        costliest_first(ring_count, [&](size_t r) { return float(store.rings[r].count); }, threads);
    std::vector<std::vector<uint32_t>> cuts(ring_count);
    parallel_for_dynamic(ring_count, 64, [&](size_t begin, size_t end, unsigned) {
        for (size_t k = begin; k < end; k++) {
            const uint32_t r = by_cost[k];
            const Cycle c = cycle_of(store, r);
            for (uint32_t place = 0; place < c.size; place++)
                if (is_junction_vertex.contains(vertex_key(c.v[place]))) cuts[r].push_back(place);
        }
    }, threads);
    const std::vector<size_t> ref_at = parallel_offsets<size_t>(
        ring_count, [&](size_t r) { return std::max<size_t>(1, cuts[r].size()); }, threads);
    const size_t ref_count = ref_at.back();
    if (ref_count >= UINT32_MAX) throw std::length_error("admin arc refs past 2^32");

    std::vector<Piece> pieces;
    parallel_resize(pieces, ref_count);
    parallel_for_dynamic(ring_count, 64, [&](size_t begin, size_t end, unsigned) {
        for (size_t k = begin; k < end; k++) {
            const uint32_t r = by_cost[k];
            const Cycle c = cycle_of(store, r);
            const std::vector<uint32_t>& cut = cuts[r];
            const uint32_t ref = static_cast<uint32_t>(ref_at[r]);
            if (cut.empty()) {
                pieces[ref] = closed_piece(c, r, ref);
                continue;
            }
            for (size_t j = 0; j < cut.size(); j++) {
                const uint64_t to = j + 1 < cut.size() ? cut[j + 1] : uint64_t(cut[0]) + c.size;
                pieces[ref + j] = open_piece(c, r, static_cast<uint32_t>(ref + j), cut[j], to);
            }
            std::vector<uint32_t>().swap(cuts[r]);
        }
    }, threads);
    cuts = {};
    done("      Admin arcs: pieces");

    // Equal pieces are one arc: alike in hash and shape, then vertex for vertex.
    parallel_sort(pieces.begin(), pieces.end(), [](const Piece& a, const Piece& b) {
        if (a.hash != b.hash) return a.hash < b.hash;
        if (a.closed != b.closed) return a.closed < b.closed;
        if (a.count != b.count) return a.count < b.count;
        return a.ref < b.ref;
    }, threads);
    auto same_shape = [&](size_t k) {
        const Piece &a = pieces[k - 1], &b = pieces[k];
        return a.hash == b.hash && a.closed == b.closed && a.count == b.count;
    };
    auto same_vertices = [&](const Piece& a, const Piece& b) {
        return compare_walks(cycle_of(store, a.ring), {a.start, a.reversed}, cycle_of(store, b.ring),
                             {b.start, b.reversed}, a.count) == 0;
    };
    std::vector<uint32_t> first_alike;  // the first piece of each piece's arc
    parallel_resize(first_alike, ref_count);
    std::vector<size_t> collisions(threads, 0);
    parallel_for_runs(ref_count, same_shape, [&](size_t begin, size_t end, unsigned range) {
        std::vector<uint32_t> firsts;
        for (size_t k = begin; k < end; k++) {
            if (k == begin || !same_shape(k)) firsts.clear();
            uint32_t first = static_cast<uint32_t>(k);
            for (uint32_t f : firsts)
                if (same_vertices(pieces[f], pieces[k])) {
                    first = f;
                    break;
                }
            if (first == k) {
                if (!firsts.empty()) collisions[range]++;
                firsts.push_back(first);
            }
            first_alike[k] = first;
        }
    }, threads);
    for (size_t c : collisions) out.counts.hash_collisions += c;
    const std::vector<uint32_t> arc_at = parallel_offsets<uint32_t>(
        ref_count, [&](size_t k) { return first_alike[k] == k ? 1u : 0u; }, threads);
    const size_t arc_count = arc_at.back();

    // The arcs, then the presentation order: Hilbert index of the first
    // vertex, key, then the vertices themselves (two arcs are never equal).
    struct Presented {
        uint64_t hilbert;
        uint64_t key;
        uint32_t arc;
    };
    std::vector<Arc> found;
    std::vector<Presented> order;
    parallel_resize(found, arc_count);
    parallel_resize(order, arc_count);
    parallel_for(ref_count, [&](size_t begin, size_t end, unsigned) {
        for (size_t k = begin; k < end; k++) {
            if (first_alike[k] != k) continue;

            const Piece& p = pieces[k];
            Arc& arc = found[arc_at[k]];
            arc.view = {p.ring, p.start, p.count, p.reversed, p.closed};
            const ArcVertices av = arc_vertices(store, arc.view);
            uint64_t min_interior = UINT64_MAX;
            for (uint32_t i = 0; i < av.size(); i++) {
                const PackedLocation q = av[i];
                arc.box.min_lng = std::min(arc.box.min_lng, q.lon_e7);
                arc.box.max_lng = std::max(arc.box.max_lng, q.lon_e7);
                arc.box.max_lat = std::max(arc.box.max_lat, q.lat_e7);
                if (i > 0 && i + 1 < av.size()) min_interior = std::min(min_interior, vertex_key(q));
            }
            arc.key = arc_key(vertex_key(av[0]), vertex_key(av[av.size() - 1]), min_interior, p.closed);
            order[arc_at[k]] = {hilbert_index(av[0]), arc.key, arc_at[k]};
        }
    }, threads);
    auto vertices_less = [&](const ArcView& a, const ArcView& b) {
        const ArcVertices va = arc_vertices(store, a), vb = arc_vertices(store, b);
        const uint32_t n = std::min(va.size(), vb.size());
        for (uint32_t i = 0; i < n; i++) {
            const uint64_t ka = vertex_key(va[i]), kb = vertex_key(vb[i]);
            if (ka != kb) return ka < kb;
        }
        return va.size() != vb.size() ? va.size() < vb.size() : a.closed < b.closed;
    };
    parallel_sort(order.begin(), order.end(), [&](const Presented& a, const Presented& b) {
        if (a.hilbert != b.hilbert) return a.hilbert < b.hilbert;
        if (a.key != b.key) return a.key < b.key;
        return vertices_less(found[a.arc].view, found[b.arc].view);
    }, threads);
    std::vector<uint32_t> rank_of;
    parallel_resize(rank_of, arc_count);
    parallel_resize(out.arcs, arc_count);
    parallel_for(arc_count, [&](size_t begin, size_t end, unsigned) {
        for (size_t rank = begin; rank < end; rank++) {
            out.arcs[rank] = found[order[rank].arc];
            rank_of[order[rank].arc] = static_cast<uint32_t>(rank);
        }
    }, threads);
    found = {};
    done("      Admin arcs: arcs");

    parallel_resize(out.refs, ref_count);
    parallel_for(ref_count, [&](size_t begin, size_t end, unsigned) {
        for (size_t k = begin; k < end; k++)
            out.refs[pieces[k].ref] = {rank_of[arc_at[first_alike[k]]], pieces[k].reversed};
    }, threads);
    parallel_resize(out.ring_refs, ref_at.size());
    parallel_for(ref_at.size(), [&](size_t begin, size_t end, unsigned) {
        for (size_t r = begin; r < end; r++) out.ring_refs[r] = static_cast<uint32_t>(ref_at[r]);
    }, threads);

    out.counts.arcs = arc_count;
    out.counts.refs = ref_count;
    out.counts.closed_arcs = parallel_count(arc_count, [&](size_t a) { return out.arcs[a].view.closed; }, threads);
    out.counts.arc_vertices = parallel_sum<size_t>(arc_count, [&](size_t a) { return out.arcs[a].view.count; }, threads);
    {
        std::vector<uint64_t> keys(arc_count);
        for (size_t a = 0; a < arc_count; a++) keys[a] = out.arcs[a].key;
        parallel_sort(keys.begin(), keys.end(), std::less<uint64_t>(), threads);
        out.counts.duplicate_keys = arc_count - static_cast<size_t>(std::unique(keys.begin(), keys.end()) - keys.begin());
    }
    done("      Admin arcs: refs");
    return out;
}

ArcSlots allocate_arc_slots(const std::vector<uint64_t>& keys, const std::string& prev_sidecar) {
    using namespace gc::id_alloc;
    IdAllocator alloc;
    if (!prev_sidecar.empty() && !alloc.load_previous(prev_sidecar))
        throw std::runtime_error("unreadable arc sidecar " + prev_sidecar);
    // The key each previous slot can be claimed by, none for its tombstones.
    const size_t prev_slots = alloc.total_slots();
    std::vector<uint64_t> prev_key(prev_slots, UINT64_MAX);
    for (size_t s = 0; s < prev_slots; s++) {
        const SidecarSlot& slot = alloc.slots()[s];
        if (is_reclaimable(slot)) prev_key[s] = make_key(static_cast<ObjectType>(slot.object_type), slot.stable_id);
    }

    ArcSlots out;
    out.slots = alloc.allocate_all(keys.size(), [&](size_t i) { return SlotIdentity{ObjectType::ARC, keys[i]}; });
    alloc.finalize();
    out.sidecar = alloc.take_slots();
    for (size_t i = 0; i < keys.size(); i++) {
        const uint32_t s = out.slots[i];
        if (s >= prev_slots) out.counts.appended++;
        else if (prev_key[s] == make_key(ObjectType::ARC, keys[i])) out.counts.kept++;
        else out.counts.recycled++;
    }
    out.counts.tombstones = out.sidecar.size() - keys.size();
    return out;
}

std::string arc_sidecar_path(const std::string& region_dir, ArcSet set) {
    return region_dir + (set == ArcSet::Full ? "/quality/exact" : "/admin-minimal") + "/arc_index.osm_ids";
}

AdminArcs build_admin_arcs(const AdminExactRings& store, const std::string& prev_planet_dir, unsigned threads) {
    threads = std::min(threads ? threads : parallel_threads(), kArcTopologyMaxThreads);
    auto t = std::chrono::steady_clock::now();
    auto cpu = CpuTicks::now();
    AdminArcs arcs = build_arc_topology(store, threads, [&](const char* step) { log_phase(step, t, cpu); });

    std::vector<uint64_t> keys(arcs.arcs.size());
    for (size_t a = 0; a < keys.size(); a++) keys[a] = arcs.arcs[a].key;
    ArcSlots slots =
        allocate_arc_slots(keys, prev_planet_dir.empty() ? "" : arc_sidecar_path(prev_planet_dir, ArcSet::Full));
    for (size_t a = 0; a < keys.size(); a++) arcs.arcs[a].planet_slot = slots.slots[a];
    arcs.planet_sidecar = std::move(slots.sidecar);
    arcs.slot_counts = slots.counts;
    arcs.polygons.resize(store.polygons.size());
    for (size_t p = 0; p < store.polygons.size(); p++)
        arcs.polygons[p] = {store.polygons[p].first_ring, store.polygons[p].ring_count};
    log_phase("      Admin arcs: planet slots", t, cpu);

    const AdminArcCounts& c = arcs.counts;
    const AdminArcSlotCounts& s = arcs.slot_counts;
    std::ostringstream line;
    line << "  Admin arcs: " << c.junctions << " junctions, " << c.arcs << " arcs (" << c.closed_arcs << " closed, "
         << c.arc_vertices << " vertices), " << c.refs << " refs over " << store.rings.size() << " rings; largest partition "
         << c.largest_partition << " occurrences, " << c.hash_collisions << " hash collisions, " << c.duplicate_keys
         << " duplicate keys; planet slots: " << s.kept << " kept, " << s.recycled << " recycled, " << s.appended
         << " appended, " << s.tombstones << " tombstones (" << arcs.planet_sidecar.size() << " slots) on " << threads
         << " threads\n";
    std::cerr << line.str();
    return arcs;
}

std::vector<PackedLocation> rebuild_ring(const AdminArcs& arcs, const AdminExactRings& store, uint32_t ring) {
    std::vector<PackedLocation> out;
    for (uint32_t k = arcs.ring_refs[ring]; k < arcs.ring_refs[ring + 1]; k++) {
        const ArcRef ref = arcs.refs[k];
        const ArcVertices av = arc_vertices(store, arcs.arcs[ref.arc].view);
        const uint32_t n = av.size();
        for (uint32_t i = out.empty() ? 0 : 1; i < n; i++) out.push_back(av[ref.reversed ? n - 1 - i : i]);
    }
    return out;
}

bool same_closed_ring(const PackedLocation* a, uint32_t a_count, const PackedLocation* b, uint32_t b_count) {
    if (a_count != b_count) return false;
    if (a_count == 0) return true;
    if (a[0] != a[a_count - 1] || b[0] != b[b_count - 1]) return false;
    const uint32_t n = a_count - 1;
    if (n == 0) return a[0] == b[0];
    for (uint32_t s = 0; s < n; s++) {
        if (b[s] != a[0]) continue;

        bool forwards = true, backwards = true;
        for (uint32_t i = 1; i < n && (forwards || backwards); i++) {
            forwards = forwards && a[i] == b[(s + i) % n];
            backwards = backwards && a[i] == b[(s + n - i) % n];
        }
        if (forwards || backwards) return true;
    }
    return false;
}

void require_arc_dirs(const std::string& output_dir, const std::string& arcs_out_dir,
                      const std::string& prev_arcs_dir) {
    namespace fs = std::filesystem;
    if (arcs_out_dir.empty()) {
        if (!prev_arcs_dir.empty())
            throw std::runtime_error("--prev-arcs " + prev_arcs_dir + " without --arcs-out: only the arc stage reads it");
        return;
    }
    require_new_or_empty_output_dir(arcs_out_dir);
    auto normal = [](const std::string& dir) {
        fs::path p = fs::weakly_canonical(fs::absolute(dir));
        if (p.filename().empty()) p = p.parent_path();
        return p;
    };
    auto within = [](const fs::path& dir, const fs::path& p) {
        return std::mismatch(dir.begin(), dir.end(), p.begin(), p.end()).first == dir.end();
    };
    const fs::path output = normal(output_dir), arcs = normal(arcs_out_dir);
    if (within(output, arcs) || within(arcs, output))
        throw std::runtime_error("--arcs-out " + arcs_out_dir + " overlaps the output dir " + output_dir +
                                 ", which publishes every file under it: keep them apart");
    if (!prev_arcs_dir.empty()) {
        const std::string sidecar = arc_sidecar_path(prev_arcs_dir + "/planet", ArcSet::Full);
        std::error_code ec;
        if (!fs::is_regular_file(sidecar, ec))
            throw std::runtime_error("--prev-arcs " + prev_arcs_dir + " holds no planet arc sidecar " + sidecar);
    }
}
