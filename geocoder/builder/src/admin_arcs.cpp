#include "admin_arcs.h"

#include <algorithm>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <stdexcept>

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
    free_storage(at);

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
    free_storage(buffers);
    free_storage(scattered);
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

// A closed arc's canonical walk round the cycle: the smallest of the walks
// from a smallest vertex, either way.
Walk smallest_closed_walk(const Cycle& c) {
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
    return best;
}

// The walk an open arc reads in from place `from` to place `to` of the cycle
// (past its end when it wraps): from its smaller end, and from the end whose
// walk is smaller when they are one vertex.
Walk smaller_open_walk(const Cycle& c, uint32_t from, uint64_t to) {
    const uint32_t count = static_cast<uint32_t>(to - from + 1);
    const Walk forwards{from, false}, backwards{static_cast<uint32_t>(to % c.size), true};
    return compare_walks(c, backwards, c, forwards, count) < 0 ? backwards : forwards;
}

Piece closed_piece(const Cycle& c, uint32_t ring, uint32_t ref) {
    const Walk best = smallest_closed_walk(c);
    const uint32_t count = c.size + 1;
    return {hash_walk(c, best, count, true), ring, best.start, count, ref, best.reversed, true};
}

Piece open_piece(const Cycle& c, uint32_t ring, uint32_t ref, uint32_t from, uint64_t to) {
    const uint32_t count = static_cast<uint32_t>(to - from + 1);
    const Walk w = smaller_open_walk(c, from, to);
    return {hash_walk(c, w, count, false), ring, w.start, count, ref, w.reversed, false};
}

Cycle cycle_of(const AdminExactRings& store, uint32_t ring) {
    const RingSpan& span = store.rings[ring];
    return {store.vertices.data() + span.begin, span.count - 1};
}

// An arc's box and key from its vertices in its order (ArcVertices,
// MinimalArcVertices).
template <class Vertices>
ArcBox box_of(const Vertices& v) {
    ArcBox box;
    for (uint32_t i = 0; i < v.size(); i++) box.add(v[i]);
    return box;
}

template <class Vertices>
uint64_t key_of(const Vertices& v, bool closed) {
    uint64_t min_interior = UINT64_MAX;
    for (uint32_t i = 1; i + 1 < v.size(); i++) min_interior = std::min(min_interior, vertex_key(v[i]));
    return arc_key(vertex_key(v[0]), vertex_key(v[v.size() - 1]), min_interior, closed);
}

template <class Vertices>
uint64_t ends_key_of(const Vertices& v, bool closed) {
    return arc_ends_key(vertex_key(v[0]), vertex_key(v[v.size() - 1]), closed);
}

// A member way of a piece (its ref) and of an arc.
#pragma pack(push, 4)
struct RefWay {
    uint32_t ref;
    int64_t way;
};
struct ArcWay {
    uint32_t arc;
    int64_t way;
};
#pragma pack(pop)

// The member ways of ring r's pieces, each piece's once, appended to `out`:
// the pieces from ref `ref` on, cut at the places `cut` (none: the ring
// whole, one closed piece), read off the ring's way runs, which must cover
// its `cycle` edges. `scratch` is scratch.
void add_piece_ways(const AdminExactRings& store, uint32_t r, uint32_t cycle, const std::vector<uint32_t>& cut,
                    uint32_t ref, std::vector<int64_t>& scratch, std::vector<RefWay>& out) {
    const auto [first, last] = store.runs_of_ring(r);
    const size_t n = static_cast<size_t>(last - first);
    if (n == 0) return;
    uint64_t edges = 0;
    for (size_t k = 0; k < n; k++) {
        if (first[k].edges == 0) throw std::logic_error("an admin ring's way run of no edges");
        edges += first[k].edges;
    }
    if (edges != cycle) throw std::logic_error("an admin ring's way runs don't cover its edges");
    // The run holding the first piece's first edge, and its edges from there.
    size_t k = 0;
    uint64_t skip = cut.empty() ? 0 : cut[0];
    while (skip >= first[k].edges) skip -= first[k++].edges;
    uint64_t left = first[k].edges - skip;
    const size_t pieces = std::max<size_t>(1, cut.size());
    for (size_t j = 0; j < pieces; j++) {
        uint64_t span = cycle;
        if (!cut.empty()) span = (j + 1 < cut.size() ? uint64_t(cut[j + 1]) : uint64_t(cut[0]) + cycle) - cut[j];
        scratch.clear();
        while (span > 0) {
            const int64_t way = first[k].way;  // by value: WayRun packs it to 4 bytes
            scratch.push_back(way);
            const uint64_t step = std::min(span, left);
            span -= step;
            left -= step;
            if (left == 0) {
                k = (k + 1) % n;
                left = first[k].edges;
            }
        }
        std::sort(scratch.begin(), scratch.end());
        scratch.erase(std::unique(scratch.begin(), scratch.end()), scratch.end());
        for (int64_t way : scratch) out.push_back({static_cast<uint32_t>(ref + j), way});
    }
}

// Each arc's member ways from its pieces' (`piece_ways`, each piece known by
// its ref): ascending and each once per arc, into `way_at` and `ways` (as
// AdminArcs holds them). The order the threads gathered them in is gone.
void gather_arc_ways(std::vector<std::vector<RefWay>>& piece_ways, const std::vector<ArcRef>& refs, size_t arcs,
                     std::vector<uint32_t>& way_at, std::vector<int64_t>& ways, unsigned threads) {
    std::vector<size_t> at(piece_ways.size() + 1, 0);
    for (size_t w = 0; w < piece_ways.size(); w++) at[w + 1] = at[w] + piece_ways[w].size();
    // Each as its arc, through its ref, and the way.
    std::vector<ArcWay> arc_ways;
    parallel_resize(arc_ways, at.back());
    parallel_for_dynamic(piece_ways.size(), 1, [&](size_t begin, size_t end, unsigned) {
        for (size_t w = begin; w < end; w++) {
            for (size_t i = 0; i < piece_ways[w].size(); i++)
                arc_ways[at[w] + i] = {refs[piece_ways[w][i].ref].arc, piece_ways[w][i].way};
            std::vector<RefWay>().swap(piece_ways[w]);
        }
    }, threads);
    parallel_sort(arc_ways.begin(), arc_ways.end(), [](const ArcWay& a, const ArcWay& b) {
        return a.arc != b.arc ? a.arc < b.arc : a.way < b.way;
    }, threads);
    arc_ways.erase(std::unique(arc_ways.begin(), arc_ways.end(),
                               [](const ArcWay& a, const ArcWay& b) { return a.arc == b.arc && a.way == b.way; }),
                   arc_ways.end());
    if (arc_ways.size() > UINT32_MAX) throw std::length_error("admin arc member ways past 2^32");
    way_at.assign(arcs + 1, 0);
    for (const ArcWay& aw : arc_ways) way_at[aw.arc + 1]++;
    for (size_t a = 0; a < arcs; a++) way_at[a + 1] += way_at[a];
    parallel_resize(ways, arc_ways.size());
    parallel_for(arc_ways.size(), [&](size_t begin, size_t end, unsigned) {
        for (size_t i = begin; i < end; i++) ways[i] = arc_ways[i].way;
    }, threads);
}

// How many of the keys, kNoFallbackKey left out, an earlier one has.
template <class Key>
size_t shared_among(size_t n, Key key, unsigned threads) {
    std::vector<uint64_t> keys;
    for (size_t a = 0; a < n; a++)
        if (key(a) != gc::id_alloc::kNoFallbackKey) keys.push_back(key(a));
    parallel_sort(keys.begin(), keys.end(), std::less<uint64_t>(), threads);
    return keys.size() - static_cast<size_t>(std::unique(keys.begin(), keys.end()) - keys.begin());
}

// The presentation order's last word between two arcs of one Hilbert index
// and key: their vertices, then their length, then closed after open.
template <class Vertices>
bool vertices_less(const Vertices& a, bool a_closed, const Vertices& b, bool b_closed) {
    const uint32_t n = std::min(a.size(), b.size());
    for (uint32_t i = 0; i < n; i++) {
        const uint64_t ka = vertex_key(a[i]), kb = vertex_key(b[i]);
        if (ka != kb) return ka < kb;
    }
    return a.size() != b.size() ? a.size() < b.size() : a_closed < b_closed;
}

// Where each arc goes in presentation order: by the Hilbert index of its
// first vertex, then its key, then `less` (vertices_less).
struct Presented {
    uint64_t hilbert;
    uint64_t key;
    uint32_t arc;
};

template <class Less>
void sort_presented(std::vector<Presented>& order, Less less, unsigned threads) {
    parallel_sort(order.begin(), order.end(), [&](const Presented& a, const Presented& b) {
        if (a.hilbert != b.hilbert) return a.hilbert < b.hilbert;
        if (a.key != b.key) return a.key < b.key;
        return less(a.arc, b.arc);
    }, threads);
}

template <class Key>
size_t duplicates_among(size_t n, Key key, unsigned threads) {
    std::vector<uint64_t> keys(n);
    for (size_t a = 0; a < n; a++) keys[a] = key(a);
    parallel_sort(keys.begin(), keys.end(), std::less<uint64_t>(), threads);
    return n - static_cast<size_t>(std::unique(keys.begin(), keys.end()) - keys.begin());
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

    // The pieces, and their member ways off the rings' way runs.
    const bool with_runs = !store.first_run.empty();
    if (with_runs && store.first_run.size() != ring_count)
        throw std::logic_error("admin exact rings' way runs out of step with the rings");
    std::vector<std::vector<RefWay>> piece_ways(threads);
    std::vector<Piece> pieces;
    parallel_resize(pieces, ref_count);
    parallel_for_dynamic(ring_count, 64, [&](size_t begin, size_t end, unsigned worker) {
        std::vector<int64_t> scratch;
        for (size_t k = begin; k < end; k++) {
            const uint32_t r = by_cost[k];
            const Cycle c = cycle_of(store, r);
            const std::vector<uint32_t>& cut = cuts[r];
            const uint32_t ref = static_cast<uint32_t>(ref_at[r]);
            if (with_runs) add_piece_ways(store, r, c.size, cut, ref, scratch, piece_ways[worker]);
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
    free_storage(cuts);
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
            arc.box = box_of(av);
            arc.key = key_of(av, p.closed);
            arc.fallback.ends = ends_key_of(av, p.closed);
            order[arc_at[k]] = {hilbert_index(av[0]), arc.key, arc_at[k]};
        }
    }, threads);
    sort_presented(order, [&](uint32_t a, uint32_t b) {
        const ArcView &va = found[a].view, &vb = found[b].view;
        return vertices_less(arc_vertices(store, va), va.closed, arc_vertices(store, vb), vb.closed);
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
    free_storage(found);
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
    free_storage(pieces);

    out.counts.arcs = arc_count;
    out.counts.refs = ref_count;
    out.counts.closed_arcs = parallel_count(arc_count, [&](size_t a) { return out.arcs[a].view.closed; }, threads);
    out.counts.arc_vertices = parallel_sum<size_t>(arc_count, [&](size_t a) { return out.arcs[a].view.count; }, threads);
    out.counts.duplicate_keys = duplicates_among(arc_count, [&](size_t a) { return out.arcs[a].key; }, threads);
    done("      Admin arcs: refs");

    // Each arc's way set, its pieces' ways in every ring, and its key.
    gather_arc_ways(piece_ways, out.refs, arc_count, out.way_at, out.ways, threads);
    parallel_for(arc_count, [&](size_t begin, size_t end, unsigned) {
        for (size_t a = begin; a < end; a++)
            out.arcs[a].fallback.way_set = arc_way_set_key(out.ways.data() + out.way_at[a], out.way_at[a + 1] - out.way_at[a]);
    }, threads);
    out.counts.arcs_without_ways = parallel_count(arc_count, [&](size_t a) { return out.way_at[a + 1] == out.way_at[a]; }, threads);
    out.counts.shared_way_sets = shared_among(arc_count, [&](size_t a) { return out.arcs[a].fallback.way_set; }, threads);
    out.counts.shared_ends = shared_among(arc_count, [&](size_t a) { return out.arcs[a].fallback.ends; }, threads);
    done("      Admin arcs: way sets");
    return out;
}

namespace {

// A chain of full arcs, each part walked from one end to the other, the next
// part starting where it ends.
struct Chain {
    uint32_t first_part;
    uint32_t part_count;
    bool closed;
};

// A full arc's ends are numbered 2 * arc (its first vertex) and 2 * arc + 1
// (its last); an end joined to none is joined to kNoEnd.
constexpr uint32_t kNoEnd = UINT32_MAX;

// The vertices of a chain's parts (`parts`) end to end, each join once: a
// closed chain's cycle without its closing vertex.
void chain_vertices(const AdminArcs& arcs, const AdminExactRings& store, const ArcRef* parts, uint32_t part_count,
                    bool closed, std::vector<PackedLocation>& out) {
    out.clear();
    for (uint32_t j = 0; j < part_count; j++) {
        const ArcVertices av = arc_vertices(store, arcs.arcs[parts[j].arc].view);
        const uint32_t n = av.size();
        for (uint32_t i = j == 0 ? 0 : 1; i < n; i++) out.push_back(av[parts[j].reversed ? n - 1 - i : i]);
    }
    if (closed) out.pop_back();
}

// The parts walked the other way: in the opposite order, each reversed.
void reverse_parts(ArcRef* parts, uint32_t part_count) {
    std::reverse(parts, parts + part_count);
    for (uint32_t j = 0; j < part_count; j++) parts[j].reversed = !parts[j].reversed;
}

// The way-set key of a chain of full arcs: every way of its parts' way sets.
uint64_t way_set_of_parts(const AdminArcs& arcs, const ArcRef* parts, uint32_t part_count, std::vector<int64_t>& ways) {
    ways.clear();
    for (uint32_t j = 0; j < part_count; j++) {
        const uint32_t a = parts[j].arc;
        ways.insert(ways.end(), arcs.ways.begin() + arcs.way_at[a], arcs.ways.begin() + arcs.way_at[a + 1]);
    }
    std::sort(ways.begin(), ways.end());
    ways.erase(std::unique(ways.begin(), ways.end()), ways.end());
    return arc_way_set_key(ways.data(), ways.size());
}

// Where each part of a chain starts among the chain's places.
void place_parts(const AdminArcs& arcs, const ArcRef* parts, uint32_t part_count, uint32_t* part_at) {
    uint32_t at = 0;
    for (uint32_t j = 0; j < part_count; j++) {
        part_at[j] = at;
        at += arcs.arcs[parts[j].arc].view.count - 1;
    }
}

}  // namespace

MinimalArcs build_minimal_arcs(const AdminArcs& arcs, const AdminExactRings& store,
                               const std::vector<bool>& in_minimal, unsigned threads) {
    if (threads == 0) threads = parallel_threads();
    if (in_minimal.size() != store.polygons.size())
        throw std::logic_error("minimal polygon marks out of step with the polygons");
    const size_t ring_count = store.rings.size(), full_count = arcs.arcs.size();
    if (arcs.way_at.size() != full_count + 1) throw std::logic_error("full arcs without their way sets");
    MinimalArcs out;

    // The minimal polygons' rings, and the full arcs they use (the core).
    std::vector<uint8_t> minimal_ring(ring_count, 0);
    for (size_t p = 0; p < in_minimal.size(); p++) {
        if (!in_minimal[p] || store.polygons[p].ring_count == 0) continue;

        out.counts.polygons++;
        for (uint32_t r = 0; r < store.polygons[p].ring_count; r++) minimal_ring[store.polygons[p].first_ring + r] = 1;
    }
    std::vector<uint8_t> core(full_count, 0);
    for (size_t r = 0; r < ring_count; r++) {
        if (!minimal_ring[r]) continue;

        out.counts.rings++;
        for (uint32_t k = arcs.ring_refs[r]; k < arcs.ring_refs[r + 1]; k++) core[arcs.refs[k].arc] = 1;
    }
    out.counts.core_arcs = static_cast<size_t>(std::count(core.begin(), core.end(), 1));

    // A full junction where exactly two core arc ends meet is none for the
    // minimal rings: those two ends join. Every minimal ring through it then
    // goes from one of the ends to the other, unless it turns back there,
    // leaving its arc by an end and coming straight back along it (a spur's
    // tip another polygon touches): that ring would leave the chain, so such
    // a junction stays one. The ends minimal rings turn back at:
    std::vector<uint8_t> turned_at(2 * full_count, 0);
    for (size_t r = 0; r < ring_count; r++) {
        if (!minimal_ring[r]) continue;

        const uint32_t first = arcs.ring_refs[r], n = arcs.ring_refs[r + 1] - first;
        for (uint32_t t = 0; t < n; t++) {
            const ArcRef here = arcs.refs[first + t], next = arcs.refs[first + (t + 1) % n];
            const uint32_t leave = 2 * here.arc + (here.reversed ? 0 : 1);
            const uint32_t enter = 2 * next.arc + (next.reversed ? 1 : 0);
            if (leave == enter) turned_at[leave] = 1;
        }
    }
    struct End {
        uint64_t vertex;
        uint32_t end;
    };
    std::vector<End> ends;
    for (uint32_t a = 0; a < full_count; a++) {
        if (!core[a] || arcs.arcs[a].view.closed) continue;

        const ArcVertices av = arc_vertices(store, arcs.arcs[a].view);
        ends.push_back({vertex_key(av[0]), 2 * a});
        ends.push_back({vertex_key(av[av.size() - 1]), 2 * a + 1});
    }
    parallel_sort(ends.begin(), ends.end(), [](const End& x, const End& y) {
        return x.vertex != y.vertex ? x.vertex < y.vertex : x.end < y.end;
    }, threads);
    std::vector<uint32_t> joined(2 * full_count, kNoEnd);
    for (size_t k = 0; k < ends.size();) {
        size_t run_end = k + 1;
        while (run_end < ends.size() && ends[run_end].vertex == ends[k].vertex) run_end++;
        if (run_end - k == 2) {
            if (turned_at[ends[k].end] || turned_at[ends[k + 1].end]) {
                out.counts.turnback_junctions++;
            } else {
                joined[ends[k].end] = ends[k + 1].end;
                joined[ends[k + 1].end] = ends[k].end;
                out.counts.joined_junctions++;
            }
        }
        k = run_end;
    }
    free_storage(ends);
    free_storage(turned_at);

    // The chains, each from the first of its arcs in full order: forwards
    // from that arc's last vertex, then (unless it came back round) backwards
    // from its first.
    std::vector<ArcRef> parts;
    std::vector<Chain> chains;
    {
        std::vector<uint8_t> chained(full_count, 0);
        std::vector<ArcRef> before;
        for (uint32_t a = 0; a < full_count; a++) {
            if (!core[a] || chained[a]) continue;

            const uint32_t first = static_cast<uint32_t>(parts.size());
            chained[a] = 1;
            if (arcs.arcs[a].view.closed) {
                parts.push_back({a, false});
                chains.push_back({first, 1, true});
                continue;
            }
            parts.push_back({a, false});
            bool closed = false;
            for (uint32_t leave = 2 * a + 1; joined[leave] != kNoEnd;) {
                const uint32_t enter = joined[leave], b = enter / 2;
                if (b == a) {
                    closed = true;
                    break;
                }
                parts.push_back({b, (enter & 1) != 0});
                chained[b] = 1;
                leave = enter ^ 1;
            }
            if (!closed) {
                before.clear();
                for (uint32_t leave = 2 * a; joined[leave] != kNoEnd;) {
                    const uint32_t enter = joined[leave], b = enter / 2;
                    before.push_back({b, (enter & 1) == 0});
                    chained[b] = 1;
                    leave = enter ^ 1;
                }
                parts.insert(parts.begin() + first, before.rbegin(), before.rend());
            }
            chains.push_back({first, static_cast<uint32_t>(parts.size()) - first, closed});
        }
    }
    free_storage(joined);

    // Each chain in its canonical walk, as build_arc_topology walks a piece:
    // an open one from its smaller end, a closed one round from a smallest
    // vertex, rotated so that it starts in its first part.
    const size_t arc_count = chains.size();
    std::vector<uint32_t> part_at(parts.size());
    std::vector<MinimalArc> found(arc_count);
    std::vector<Presented> order(arc_count);
    parallel_for_dynamic(arc_count, 256, [&](size_t begin, size_t end, unsigned) {
        std::vector<PackedLocation> v;
        std::vector<ArcRef> rotated;
        for (size_t c = begin; c < end; c++) {
            const Chain& chain = chains[c];
            ArcRef* p = parts.data() + chain.first_part;
            uint32_t* at = part_at.data() + chain.first_part;
            chain_vertices(arcs, store, p, chain.part_count, chain.closed, v);
            const Cycle cycle{v.data(), static_cast<uint32_t>(v.size())};
            MinimalArc& arc = found[c];
            arc.first_part = chain.first_part;
            arc.part_count = chain.part_count;
            arc.closed = chain.closed;
            if (!chain.closed) {
                arc.count = cycle.size;
                if (smaller_open_walk(cycle, 0, cycle.size - 1).reversed) reverse_parts(p, chain.part_count);
            } else {
                arc.count = cycle.size + 1;
                const Walk w = smallest_closed_walk(cycle);
                uint32_t start = w.start;
                if (w.reversed) {
                    reverse_parts(p, chain.part_count);
                    start = (cycle.size - start) % cycle.size;
                }
                place_parts(arcs, p, chain.part_count, at);
                const uint32_t j =
                    static_cast<uint32_t>(std::upper_bound(at, at + chain.part_count, start) - at) - 1;
                rotated.assign(p + j, p + chain.part_count);
                rotated.insert(rotated.end(), p, p + j);
                std::copy(rotated.begin(), rotated.end(), p);
                arc.start = start - at[j];
            }
            place_parts(arcs, p, chain.part_count, at);
            arc.box = {};
            for (uint32_t j = 0; j < chain.part_count; j++) arc.box.add(arcs.arcs[p[j].arc].box);
        }
    }, threads);
    free_storage(chains);

    // The arcs' keys and fallback keys, then the arcs in presentation order,
    // their parts with them.
    out.arcs = std::move(found);
    out.parts = std::move(parts);
    out.part_at = std::move(part_at);
    parallel_for_dynamic(arc_count, 256, [&](size_t begin, size_t end, unsigned) {
        std::vector<int64_t> ways;
        for (size_t a = begin; a < end; a++) {
            const MinimalArcVertices v(store, arcs, out, static_cast<uint32_t>(a));
            MinimalArc& arc = out.arcs[a];
            arc.key = key_of(v, arc.closed);
            arc.fallback = {way_set_of_parts(arcs, out.parts.data() + arc.first_part, arc.part_count, ways),
                            ends_key_of(v, arc.closed)};
            order[a] = {hilbert_index(v[0]), arc.key, static_cast<uint32_t>(a)};
        }
    }, threads);
    sort_presented(order, [&](uint32_t a, uint32_t b) {
        return vertices_less(MinimalArcVertices(store, arcs, out, a), out.arcs[a].closed,
                             MinimalArcVertices(store, arcs, out, b), out.arcs[b].closed);
    }, threads);
    {
        std::vector<MinimalArc> presented(arc_count);
        std::vector<ArcRef> presented_parts;
        std::vector<uint32_t> presented_at;
        presented_parts.reserve(out.parts.size());
        presented_at.reserve(out.parts.size());
        for (size_t rank = 0; rank < arc_count; rank++) {
            MinimalArc arc = out.arcs[order[rank].arc];
            const uint32_t first = static_cast<uint32_t>(presented_parts.size());
            presented_parts.insert(presented_parts.end(), out.parts.begin() + arc.first_part,
                                   out.parts.begin() + arc.first_part + arc.part_count);
            presented_at.insert(presented_at.end(), out.part_at.begin() + arc.first_part,
                                out.part_at.begin() + arc.first_part + arc.part_count);
            arc.first_part = first;
            presented[rank] = arc;
        }
        out.arcs = std::move(presented);
        out.parts = std::move(presented_parts);
        out.part_at = std::move(presented_at);
    }
    free_storage(order);

    // Each minimal ring's refs: its full refs a chain at a time, from the
    // first that starts one (its first part walked forwards, or its last
    // backwards).
    struct InChain {
        uint32_t arc = UINT32_MAX;
        uint32_t part = 0;
    };
    std::vector<InChain> chain_of(full_count);
    for (uint32_t m = 0; m < arc_count; m++)
        for (uint32_t j = 0; j < out.arcs[m].part_count; j++) chain_of[out.parts[out.arcs[m].first_part + j].arc] = {m, j};
    auto forwards = [&](const ArcRef& ref) {
        const InChain in = chain_of[ref.arc];
        return ref.reversed == out.parts[out.arcs[in.arc].first_part + in.part].reversed;
    };
    auto starts_chain = [&](const ArcRef& ref) {
        const InChain in = chain_of[ref.arc];
        return forwards(ref) ? in.part == 0 : in.part + 1 == out.arcs[in.arc].part_count;
    };
    const std::vector<uint32_t> ref_at = parallel_offsets<uint32_t>(ring_count, [&](size_t r) {
        if (!minimal_ring[r]) return 0u;
        uint32_t n = 0;
        for (uint32_t k = arcs.ring_refs[r]; k < arcs.ring_refs[r + 1]; k++) n += starts_chain(arcs.refs[k]);
        return n;
    }, threads);
    out.ring_refs.assign(ref_at.begin(), ref_at.end());
    parallel_resize(out.refs, ref_at.back());
    parallel_for_dynamic(ring_count, 1024, [&](size_t begin, size_t end, unsigned) {
        for (size_t r = begin; r < end; r++) {
            if (!minimal_ring[r]) continue;

            const uint32_t first = arcs.ring_refs[r], n = arcs.ring_refs[r + 1] - first;
            uint32_t from = 0;
            while (from < n && !starts_chain(arcs.refs[first + from])) from++;
            uint32_t next = ref_at[r];
            for (uint32_t t = 0; t < n;) {
                const ArcRef ref = arcs.refs[first + (from + t) % n];
                const InChain in = chain_of[ref.arc];
                const bool along = forwards(ref);
                const MinimalArc& m = out.arcs[in.arc];
                for (uint32_t j = 0; j < m.part_count; j++) {
                    const ArcRef part = arcs.refs[first + (from + t + j) % n];
                    const InChain at = chain_of[part.arc];
                    if (t + j >= n || at.arc != in.arc || at.part != (along ? j : m.part_count - 1 - j) ||
                        forwards(part) != along)
                        throw std::logic_error("a minimal ring leaves its arc's chain of full arcs");
                }
                out.refs[next++] = {in.arc, !along};
                t += m.part_count;
            }
        }
    }, threads);

    out.counts.arcs = arc_count;
    out.counts.closed_arcs = parallel_count(arc_count, [&](size_t a) { return out.arcs[a].closed; }, threads);
    out.counts.refs = out.refs.size();
    out.counts.arc_vertices = parallel_sum<size_t>(arc_count, [&](size_t a) { return out.arcs[a].count; }, threads);
    out.counts.duplicate_keys = duplicates_among(arc_count, [&](size_t a) { return out.arcs[a].key; }, threads);
    out.counts.shared_way_sets = shared_among(arc_count, [&](size_t a) { return out.arcs[a].fallback.way_set; }, threads);
    out.counts.shared_ends = shared_among(arc_count, [&](size_t a) { return out.arcs[a].fallback.ends; }, threads);
    return out;
}

ArcSlots allocate_arc_slots(const std::vector<uint64_t>& keys, const std::string& prev_sidecar) {
    using namespace gc::id_alloc;
    IdAllocator alloc;
    // Only a sidecar the lookup finds absent starts cold; one it can't look up (ec set) is read, and refused.
    std::error_code ec;
    const bool missing = !prev_sidecar.empty() && !std::filesystem::exists(prev_sidecar, ec) && !ec;
    if (!prev_sidecar.empty() && !missing && !alloc.load_previous(prev_sidecar))
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
    // Of a repeated key the allocator reclaims only the lowest previous slot
    // and leaves the others live, though no arc holds them.
    std::vector<char> held(out.sidecar.size(), 0);
    for (uint32_t s : out.slots) held[s] = 1;
    for (size_t s = 0; s < out.sidecar.size(); s++)
        if (!held[s] && !is_tombstone(out.sidecar[s])) out.sidecar[s] = tombstone_of(out.sidecar[s]);
    for (size_t i = 0; i < keys.size(); i++) {
        const uint32_t s = out.slots[i];
        if (s >= prev_slots) out.counts.appended++;
        else if (prev_key[s] == make_key(ObjectType::ARC, keys[i])) out.counts.kept++;
        else out.counts.recycled++;
    }
    out.counts.tombstones = out.sidecar.size() - keys.size();
    out.counts.previous_sidecar_missing = missing;
    return out;
}

std::string arc_slot_start_lines(const std::string& region, ArcSet set, const std::string& prev_sidecar,
                                 const AdminArcSlotCounts& counts) {
    if (prev_sidecar.empty()) return "";
    const char* set_name = set == ArcSet::Full ? "full" : "minimal";
    std::ostringstream line;
    if (counts.previous_sidecar_missing)
        line << "  " << region << " arcs: no previous " << set_name << " arc slots at " << prev_sidecar
             << ", every slot cold\n";
    line << "ARC_CHECK slots=arcs set=" << set_name << " region=" << region
         << " previous_sidecar_missing=" << counts.previous_sidecar_missing << "\n";
    return line.str();
}

std::string arc_sidecar_path(const std::string& region_dir, ArcSet set) {
    return region_dir + (set == ArcSet::Full ? "/quality/exact" : "/admin-minimal") + "/arc_index.osm_ids";
}

namespace {

// Slots for a set's arcs in presentation order (key(a)) against the
// previous planet build's sidecar of that set.
template <class Key>
ArcSlots planet_slots(size_t n, Key key, const std::string& prev_planet_dir, ArcSet set) {
    std::vector<uint64_t> keys(n);
    for (size_t a = 0; a < n; a++) keys[a] = key(a);
    const std::string prev = prev_planet_dir.empty() ? "" : arc_sidecar_path(prev_planet_dir, set);
    ArcSlots slots = allocate_arc_slots(keys, prev);
    std::cerr << arc_slot_start_lines("planet", set, prev, slots.counts);
    return slots;
}

std::string slot_summary(const AdminArcSlotCounts& s, size_t slots) {
    std::ostringstream line;
    line << "planet slots: " << s.kept << " kept, " << s.recycled << " recycled, " << s.appended << " appended, "
         << s.tombstones << " tombstones (" << slots << " slots)";
    return line.str();
}

std::string tier_summary(const ArcTier& tier, const ArcTierCounts& c) {
    std::ostringstream line;
    line << "  Admin arc tier " << tier.dir << " (" << (tier.set == ArcSet::Full ? "full" : "minimal") << " set): ";
    if (!tier.simplifies()) {
        line << c.stored_vertices << " vertices, every raw one; " << c.single_point_arcs << " arcs of one point\n";
        return line.str();
    }
    line << c.kept_vertices << " raw vertices kept, " << c.stored_vertices << " stored once snapped, "
         << c.single_point_arcs << " arcs of one point; validity: " << c.validity_rounds << " rounds, "
         << c.validity_restored << " vertices back, " << c.degenerate_rings << " degenerate rings; repair: "
         << c.crossings_simplified << " crossings from simplifying, " << c.repair_rounds << " rounds, "
         << c.repair_restored << " vertices back to crossings, " << c.nesting_restored
         << " to polygons not nesting, " << c.crossings_left << " crossings left, "
         << c.unrepairable_segments << " unrepairable segments, " << c.raw_crossings
         << " raw crossings; rings_nest: " << c.rings_nest_introduced << " polygons introduced\n";
    return line.str();
}

}  // namespace

AdminArcs build_admin_arcs(const AdminExactRings& store, const std::vector<bool>& in_minimal,
                           const std::string& prev_planet_dir, unsigned threads) {
    threads = std::min(threads ? threads : parallel_threads(), kArcTopologyMaxThreads);
    auto t = std::chrono::steady_clock::now();
    auto cpu = CpuTicks::now();
    AdminArcs arcs = build_arc_topology(store, threads, [&](const char* step) { log_phase(step, t, cpu); });

    ArcSlots slots = planet_slots(arcs.arcs.size(), [&](size_t a) { return arcs.arcs[a].key; }, prev_planet_dir,
                                  ArcSet::Full);
    for (size_t a = 0; a < arcs.arcs.size(); a++) arcs.arcs[a].planet_slot = slots.slots[a];
    arcs.planet_sidecar = std::move(slots.sidecar);
    arcs.slot_counts = slots.counts;
    log_phase("      Admin arcs: planet slots", t, cpu);

    arcs.minimal = build_minimal_arcs(arcs, store, in_minimal, threads);
    free_storage(arcs.way_at);
    free_storage(arcs.ways);
    MinimalArcs& minimal = arcs.minimal;
    slots = planet_slots(minimal.arcs.size(), [&](size_t a) { return minimal.arcs[a].key; }, prev_planet_dir,
                         ArcSet::Minimal);
    for (size_t a = 0; a < minimal.arcs.size(); a++) minimal.arcs[a].planet_slot = slots.slots[a];
    minimal.planet_sidecar = std::move(slots.sidecar);
    minimal.slot_counts = slots.counts;
    log_phase("      Admin arcs: minimal set", t, cpu);

    const std::vector<bool> nests_exact = exact_rings_nest(store, threads);
    arcs.rings_nest_raw = static_cast<size_t>(std::count(nests_exact.begin(), nests_exact.end(), false));
    log_phase("      Admin arcs: exact nesting", t, cpu);
    for (const ArcTier& tier : kArcTiers) {
        arcs.tiers.push_back(build_arc_tier(arcs, store, tier, nests_exact, threads, [&](const char* step) {
            log_phase((std::string("      Admin arcs: ") + tier.dir + " " + step).c_str(), t, cpu);
        }));
        log_phase((std::string("      Admin arcs: ") + tier.dir).c_str(), t, cpu);
    }

    const AdminArcCounts& c = arcs.counts;
    const MinimalArcCounts& m = minimal.counts;
    std::ostringstream line;
    line << "  Admin arcs: " << c.junctions << " junctions, " << c.arcs << " arcs (" << c.closed_arcs << " closed, "
         << c.arc_vertices << " vertices), " << c.refs << " refs over " << store.rings.size() << " rings; largest partition "
         << c.largest_partition << " occurrences, " << c.hash_collisions << " hash collisions, " << c.duplicate_keys
         << " duplicate keys, " << c.arcs_without_ways << " arcs without way runs, " << c.shared_way_sets
         << " shared way sets, " << c.shared_ends << " shared ends; "
         << slot_summary(arcs.slot_counts, arcs.planet_sidecar.size()) << " on " << threads << " threads\n";
    line << "  Admin arcs minimal set: " << m.arcs << " arcs (" << m.closed_arcs << " closed, " << m.arc_vertices
         << " vertices), " << m.refs << " refs over " << m.rings << " rings of " << m.polygons << " polygons; "
         << m.core_arcs << " full arcs in " << m.joined_junctions << " full junctions joined ("
         << m.turnback_junctions << " kept where a ring turns back), " << m.duplicate_keys << " duplicate keys, "
         << m.shared_way_sets << " shared way sets, " << m.shared_ends << " shared ends; "
         << slot_summary(minimal.slot_counts, minimal.planet_sidecar.size()) << "\n";
    line << "  Admin arcs rings_nest raw: " << arcs.rings_nest_raw << " polygons whose exact rings don't nest\n";
    for (size_t k = 0; k < kArcTierCount; k++) line << tier_summary(kArcTiers[k], arcs.tiers[k].counts);
    std::cerr << line.str();
    return arcs;
}

std::vector<PackedLocation> rebuild_ring(const AdminArcs& arcs, const AdminExactRings& store, ArcSet set,
                                         uint32_t ring) {
    std::vector<PackedLocation> out;
    auto chain = [&](const auto& v, bool reversed) {
        const uint32_t n = v.size();
        for (uint32_t i = out.empty() ? 0 : 1; i < n; i++) out.push_back(v[reversed ? n - 1 - i : i]);
    };
    if (set == ArcSet::Full) {
        for (uint32_t k = arcs.ring_refs[ring]; k < arcs.ring_refs[ring + 1]; k++)
            chain(arc_vertices(store, arcs.arcs[arcs.refs[k].arc].view), arcs.refs[k].reversed);
    } else {
        const MinimalArcs& m = arcs.minimal;
        for (uint32_t k = m.ring_refs[ring]; k < m.ring_refs[ring + 1]; k++)
            chain(MinimalArcVertices(store, arcs, m.refs[k].arc), m.refs[k].reversed);
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
