// The arcs' fallback identities (admin_arcs.h, ArcFallbackKeys): each arc's
// way set, the OSM ways along it in every ring the exact ring store's way runs
// give, and its ends key; the minimal set's from its parts. In the sanitized
// suite: the way gathering indexes its arrays directly.
#include "admin_arcs.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <map>
#include <random>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "parsed_data.h"
#include "scratch_dir.h"
#include "test_framework.h"

using gctest::refusal_of;

namespace {

using Ring = std::vector<PackedLocation>;

PackedLocation pt(int32_t lat, int32_t lng) { return {lat, lng}; }

// A polygon of one ring and the ring's way runs.
struct Shape {
    Ring outer;
    std::vector<WayRun> runs;
};

AdminExactRings store_of(const std::vector<Shape>& shapes) {
    AdminExactRings store;
    for (const Shape& s : shapes) store.append(s.outer, {}, {s.runs});
    return store;
}

// A lattice of rows x cols square cells of side `side`, each edge a way of
// its own with `inner` vertices between its corners, jittered off the
// straight line: cells side by side share the edge's coordinates and way.
// The corners, inner vertices and ways can be edited before the blocks are
// taken.
struct WayLattice {
    WayLattice(int rows, int cols, int inner, uint64_t seed, int32_t side) {
        std::mt19937_64 rng(seed);
        std::uniform_int_distribution<int32_t> jitter(-side / 8, side / 8);
        corners.assign(rows + 1, std::vector<PackedLocation>(cols + 1));
        for (int r = 0; r <= rows; r++)
            for (int c = 0; c <= cols; c++) corners[r][c] = pt(r * side, c * side);
        auto make = [&](int edge_rows, int edge_cols, bool horizontal, int64_t first_way,
                        std::vector<std::vector<int64_t>>& ways) {
            std::vector<std::vector<Ring>> edges(edge_rows, std::vector<Ring>(edge_cols));
            ways.assign(edge_rows, std::vector<int64_t>(edge_cols));
            for (int r = 0; r < edge_rows; r++)
                for (int c = 0; c < edge_cols; c++) {
                    ways[r][c] = first_way + 1000 * r + c;
                    for (int k = 1; k <= inner; k++) {
                        const int32_t along = k * side / (inner + 1), off = jitter(rng);
                        edges[r][c].push_back(horizontal ? pt(r * side + off, c * side + along)
                                                         : pt(r * side + along, c * side + off));
                    }
                }
            return edges;
        };
        across = make(rows + 1, cols, true, 1000000, across_way);
        upward = make(rows, cols + 1, false, 2000000, upward_way);
    }

    // The boundary of the cells [r, r + h) x [c, c + w), counterclockwise
    // from its lower left corner, closed, and its way runs.
    Shape block(int r, int c, int h, int w) const {
        Shape s;
        auto add = [&](PackedLocation from, const Ring& inner, bool backwards, int64_t way) {
            s.outer.push_back(from);
            if (backwards) s.outer.insert(s.outer.end(), inner.rbegin(), inner.rend());
            else s.outer.insert(s.outer.end(), inner.begin(), inner.end());
            const uint32_t edges = static_cast<uint32_t>(inner.size() + 1);
            if (!s.runs.empty() && s.runs.back().way == way) s.runs.back().edges += edges;
            else s.runs.push_back({way, edges});
        };
        for (int j = c; j < c + w; j++) add(corners[r][j], across[r][j], false, across_way[r][j]);
        for (int i = r; i < r + h; i++) add(corners[i][c + w], upward[i][c + w], false, upward_way[i][c + w]);
        for (int j = c + w; j > c; j--) add(corners[r + h][j], across[r + h][j - 1], true, across_way[r + h][j - 1]);
        for (int i = r + h; i > r; i--) add(corners[i][c], upward[i - 1][c], true, upward_way[i - 1][c]);
        s.outer.push_back(s.outer.front());
        return s;
    }

    // Every cell, a polygon each.
    std::vector<Shape> cells() const {
        std::vector<Shape> out;
        for (size_t r = 0; r + 1 < corners.size(); r++)
            for (size_t c = 0; c + 1 < corners[r].size(); c++) out.push_back(block(int(r), int(c), 1, 1));
        return out;
    }

    std::vector<std::vector<PackedLocation>> corners;
    std::vector<std::vector<Ring>> across;  // the edge from corner (r, c) to (r, c + 1)
    std::vector<std::vector<Ring>> upward;  // the edge from corner (r, c) to (r + 1, c)
    std::vector<std::vector<int64_t>> across_way, upward_way;
};

// An island of `n` vertices round (lat, lng), radius about `r`: one way all round.
Shape island(int32_t lat, int32_t lng, int32_t r, int n, int64_t way, uint64_t seed) {
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<int32_t> jitter(-r / 10, r / 10);
    Shape s;
    for (int k = 0; k < n; k++) {
        const double a = 2 * 3.14159265358979 * k / n;
        s.outer.push_back(pt(lat + int32_t(std::lround(r * std::sin(a))) + jitter(rng),
                             lng + int32_t(std::lround(r * std::cos(a))) + jitter(rng)));
    }
    s.outer.push_back(s.outer.front());
    s.runs = {{way, static_cast<uint32_t>(n)}};
    return s;
}

// The ways along each edge of the store's rings, either way round, by the
// edge's two vertex keys (smaller first): the store's way runs read edge by
// edge, an oracle the topology's piece walk isn't.
using EdgeWays = std::map<std::pair<uint64_t, uint64_t>, std::set<int64_t>>;
EdgeWays edge_ways(const AdminExactRings& store) {
    EdgeWays out;
    for (uint32_t r = 0; r < store.rings.size(); r++) {
        const PackedLocation* v = store.vertices.data() + store.rings[r].begin;
        const auto [first, last] = store.runs_of_ring(r);
        uint32_t e = 0;
        for (const WayRun* run = first; run != last; ++run)
            for (uint32_t i = 0; i < run->edges; i++, e++) {
                const uint64_t a = vertex_key(v[e]), b = vertex_key(v[e + 1]);
                const int64_t way = run->way;  // by value: WayRun packs it to 4 bytes
                out[{std::min(a, b), std::max(a, b)}].insert(way);
            }
    }
    return out;
}

// The ways along vertices `v` (an arc's, in its order) by the oracle.
template <class Vertices>
std::vector<int64_t> ways_along(const Vertices& v, const EdgeWays& edges) {
    std::set<int64_t> ways;
    for (uint32_t i = 0; i + 1 < v.size(); i++) {
        const uint64_t a = vertex_key(v[i]), b = vertex_key(v[i + 1]);
        const auto it = edges.find({std::min(a, b), std::max(a, b)});
        if (it != edges.end()) ways.insert(it->second.begin(), it->second.end());
    }
    return std::vector<int64_t>(ways.begin(), ways.end());
}

std::vector<int64_t> way_set_of(const AdminArcs& arcs, uint32_t a) {
    return std::vector<int64_t>(arcs.ways.begin() + arcs.way_at[a], arcs.ways.begin() + arcs.way_at[a + 1]);
}

AdminArcs full_and_minimal(const AdminExactRings& store, const std::vector<bool>& in_minimal, unsigned threads = 1) {
    AdminArcs arcs = build_arc_topology(store, threads);
    arcs.minimal = build_minimal_arcs(arcs, store, in_minimal, threads);
    return arcs;
}

// The shapes restarted `shift(k)` vertices on and walked backwards where
// `backwards(k)`, k counting the shapes: their way runs with them.
template <class Shift, class Backwards>
std::vector<Shape> restarted(std::vector<Shape> shapes, Shift shift, Backwards backwards) {
    for (size_t k = 0; k < shapes.size(); k++) {
        Shape& s = shapes[k];
        Ring cycle(s.outer.begin(), s.outer.end() - 1);
        const size_t start = shift(k) % cycle.size();
        std::rotate(cycle.begin(), cycle.begin() + static_cast<std::ptrdiff_t>(start), cycle.end());
        restart_way_runs(s.runs, start);
        if (backwards(k)) {
            std::reverse(cycle.begin() + 1, cycle.end());
            std::reverse(s.runs.begin(), s.runs.end());
        }
        cycle.push_back(cycle.front());
        s.outer = cycle;
    }
    return shapes;
}

}  // namespace

// --- Way sets ---

TEST(an_arcs_way_set_is_every_way_along_it_in_every_ring) {
    const WayLattice lattice(3, 3, 4, 17, 1000);
    std::vector<Shape> shapes = lattice.cells();
    // A region over the lower two cells of the middle column, whose ring maps
    // a way of its own (77) on the nodes of the cells' way along its right side.
    Shape region = lattice.block(0, 1, 2, 1);
    for (WayRun& run : region.runs)
        if (run.way == lattice.upward_way[0][2]) run.way = 77;
    shapes.push_back(region);
    const AdminExactRings store = store_of(shapes);
    const AdminArcs arcs = build_arc_topology(store, 2);
    const EdgeWays edges = edge_ways(store);
    REQUIRE(arcs.way_at.size() == arcs.arcs.size() + 1);
    size_t two_ways = 0;
    for (uint32_t a = 0; a < arcs.arcs.size(); a++) {
        const std::vector<int64_t> ways = way_set_of(arcs, a);
        CHECK(ways == ways_along(arc_vertices(store, arcs.arcs[a].view), edges));
        CHECK(!ways.empty());
        CHECK_EQ(arcs.arcs[a].fallback.way_set, arc_way_set_key(ways.data(), ways.size()));
        if (ways == std::vector<int64_t>({77, lattice.upward_way[0][2]})) two_ways++;
    }
    // The cells' and the region's way along that side: one arc, both ways.
    CHECK_EQ(two_ways, size_t(1));
    CHECK_EQ(arcs.counts.arcs_without_ways, size_t(0));
    // Rim arcs run round the lattice's corners, along two ways each.
    CHECK(std::any_of(arcs.arcs.begin(), arcs.arcs.end(), [&](const Arc& arc) {
        const uint32_t a = static_cast<uint32_t>(&arc - arcs.arcs.data());
        return way_set_of(arcs, a).size() == 2 && way_set_of(arcs, a)[0] != 77;
    }));
}

TEST(way_sets_and_ends_keys_are_the_same_in_any_ring_order_and_on_any_threads) {
    const WayLattice lattice(4, 4, 3, 29, 1000);
    std::vector<Shape> shapes = lattice.cells();
    shapes.push_back(lattice.block(0, 0, 4, 4));
    shapes.push_back(island(-3000, -3000, 600, 9, 99, 5));
    auto fallbacks = [](const AdminExactRings& store, unsigned threads) {
        const AdminArcs arcs = build_arc_topology(store, threads);
        std::map<uint64_t, ArcFallbackKeys> out;
        for (const Arc& a : arcs.arcs) out[a.key] = a.fallback;
        return out;
    };
    const auto base = fallbacks(store_of(shapes), 1);
    REQUIRE(base.size() > 30u);
    std::vector<Shape> backwards(shapes.rbegin(), shapes.rend());
    backwards = restarted(backwards, [](size_t k) { return 3 * k + 1; }, [](size_t k) { return k % 2 == 1; });
    for (unsigned threads : {1u, 3u, 8u}) {
        CHECK(fallbacks(store_of(shapes), threads) == base);
        CHECK(fallbacks(store_of(backwards), threads) == base);
    }
}

TEST(an_arcs_ends_key_is_its_first_and_last_vertex_and_a_closed_arcs_its_smallest_twice) {
    const WayLattice lattice(2, 2, 3, 41, 1000);
    std::vector<Shape> shapes = lattice.cells();
    shapes.push_back(island(-5000, -5000, 800, 11, 501, 3));
    // A lens: an island cut by a chord into halves, whose three arcs (two
    // round, one across) all run between the chord's ends.
    const Shape whole = island(9000, 9000, 1000, 12, 502, 8);
    const PackedLocation middle = pt(9000, 9010);
    Shape a{Ring(whole.outer.begin(), whole.outer.begin() + 7), {{502, 6}, {503, 2}}};
    a.outer.push_back(middle);
    a.outer.push_back(a.outer.front());
    Shape b{Ring(whole.outer.begin() + 6, whole.outer.end() - 1), {{502, 6}, {503, 2}}};
    b.outer.push_back(whole.outer.front());
    b.outer.push_back(middle);
    b.outer.push_back(b.outer.front());
    shapes.push_back(a);
    shapes.push_back(b);
    const AdminExactRings store = store_of(shapes);
    const AdminArcs arcs = build_arc_topology(store, 1);
    size_t closed = 0, lens = 0;
    const uint64_t lens_ends = arc_ends_key(std::min(vertex_key(whole.outer[0]), vertex_key(whole.outer[6])),
                                            std::max(vertex_key(whole.outer[0]), vertex_key(whole.outer[6])), false);
    for (const Arc& arc : arcs.arcs) {
        const ArcVertices v = arc_vertices(store, arc.view);
        CHECK_EQ(arc.fallback.ends, arc_ends_key(vertex_key(v[0]), vertex_key(v[v.size() - 1]), arc.view.closed));
        if (arc.view.closed) {
            closed++;
            uint64_t smallest = UINT64_MAX;
            for (uint32_t i = 0; i < v.size(); i++) smallest = std::min(smallest, vertex_key(v[i]));
            CHECK_EQ(arc.fallback.ends, arc_ends_key(smallest, smallest, true));
        }
        lens += arc.fallback.ends == lens_ends;
    }
    CHECK_EQ(closed, size_t(1));
    // The lens's three arcs share their ends key: two of them after the first.
    CHECK_EQ(lens, size_t(3));
    CHECK(arcs.counts.shared_ends >= size_t(2));
}

TEST(a_minimal_arcs_way_set_is_its_parts_ways_and_its_ends_its_own) {
    // Cells of another level inside a minimal region: the region's ring is
    // one closed minimal arc chained across the cells' junctions on its rim,
    // and a minimal cell in a corner cuts it into open ones.
    const WayLattice lattice(3, 3, 3, 53, 1000);
    std::vector<Shape> shapes = lattice.cells();
    std::vector<bool> in_minimal(shapes.size(), false);
    shapes.push_back(lattice.block(0, 0, 3, 3));
    in_minimal.push_back(true);
    for (bool corner_cell : {false, true}) {
        in_minimal[0] = corner_cell;
        const AdminExactRings store = store_of(shapes);
        const AdminArcs arcs = full_and_minimal(store, in_minimal);
        const MinimalArcs& m = arcs.minimal;
        const EdgeWays edges = edge_ways(store);
        size_t joined = 0;
        for (uint32_t a = 0; a < m.arcs.size(); a++) {
            const MinimalArc& arc = m.arcs[a];
            std::set<int64_t> parts_ways;
            for (uint32_t j = 0; j < arc.part_count; j++) {
                const std::vector<int64_t> ways = way_set_of(arcs, m.parts[arc.first_part + j].arc);
                parts_ways.insert(ways.begin(), ways.end());
            }
            const std::vector<int64_t> ways(parts_ways.begin(), parts_ways.end());
            const MinimalArcVertices v(store, arcs, a);
            CHECK(ways == ways_along(v, edges));
            CHECK_EQ(arc.fallback.way_set, arc_way_set_key(ways.data(), ways.size()));
            CHECK_EQ(arc.fallback.ends, arc_ends_key(vertex_key(v[0]), vertex_key(v[v.size() - 1]), arc.closed));
            joined += arc.part_count > 1;
        }
        CHECK(joined > 0);
        // With the corner cell: the rim round the lattice's corner, the rest
        // of the rim, and the cell's two inner sides joined.
        CHECK_EQ(m.arcs.size(), corner_cell ? size_t(3) : size_t(1));
    }
}

TEST(way_runs_that_dont_cover_their_ring_are_refused) {
    const WayLattice lattice(1, 1, 3, 61, 1000);
    Shape cell = lattice.block(0, 0, 1, 1);
    auto refusal = [](const std::vector<Shape>& shapes, bool drop_a_first_run = false) {
        AdminExactRings store = store_of(shapes);
        if (drop_a_first_run) store.first_run.pop_back();
        return refusal_of<std::logic_error>([&] { build_arc_topology(store, 2); });
    };
    CHECK(refusal({cell}).empty());
    Shape short_runs = cell;
    short_runs.runs.back().edges--;
    CHECK(refusal({short_runs}).find("don't cover its edges") != std::string::npos);
    Shape empty_run = cell;
    empty_run.runs.insert(empty_run.runs.begin() + 1, WayRun{5, 0});
    CHECK(refusal({empty_run}).find("way run of no edges") != std::string::npos);
    CHECK(refusal({cell, cell}, true).find("out of step") != std::string::npos);
}

TEST(rings_without_way_runs_leave_their_arcs_without_a_way_set_but_with_ends) {
    const WayLattice lattice(2, 2, 3, 67, 1000);
    const std::vector<Shape> shapes = lattice.cells();
    // No runs at all; then only the first cell's.
    AdminExactRings none;
    for (const Shape& s : shapes) none.append(s.outer, {});
    const AdminArcs arcs = build_arc_topology(none, 2);
    CHECK_EQ(arcs.counts.arcs_without_ways, arcs.arcs.size());
    for (const Arc& a : arcs.arcs) {
        CHECK_EQ(a.fallback.way_set, gc::id_alloc::kNoFallbackKey);
        CHECK(a.fallback.ends != gc::id_alloc::kNoFallbackKey);
    }
    CHECK_EQ(arcs.counts.shared_way_sets, size_t(0));
    AdminExactRings some;
    some.append(shapes[0].outer, {}, {shapes[0].runs});
    for (size_t k = 1; k < shapes.size(); k++) some.append(shapes[k].outer, {});
    const AdminArcs partly = build_arc_topology(some, 2);
    const EdgeWays edges = edge_ways(some);
    size_t with_ways = 0;
    for (uint32_t a = 0; a < partly.arcs.size(); a++) {
        const std::vector<int64_t> ways = way_set_of(partly, a);
        CHECK(ways == ways_along(arc_vertices(some, partly.arcs[a].view), edges));
        with_ways += !ways.empty();
    }
    // The first cell's two inner sides and its rim arc.
    CHECK_EQ(with_ways, size_t(3));
    CHECK_EQ(partly.counts.arcs_without_ways, partly.arcs.size() - 3);
}

TEST(the_arc_stage_carries_the_fallback_keys_to_its_sets_and_continents_and_frees_the_way_sets) {
    const WayLattice lattice(4, 4, 3, 71, 1000);
    std::vector<Shape> shapes = lattice.cells();
    std::vector<bool> in_minimal(shapes.size(), false);
    for (size_t k = 0; k < shapes.size(); k += 3) in_minimal[k] = true;
    shapes.push_back(lattice.block(0, 0, 4, 4));
    in_minimal.push_back(true);
    const AdminExactRings store = store_of(shapes);
    const AdminArcs arcs = build_admin_arcs(store, in_minimal, "", 2);
    CHECK(arcs.ways.empty() && arcs.way_at.empty());
    // As the topology and the minimal set give them.
    const AdminArcs again = full_and_minimal(store, in_minimal, 2);
    REQUIRE(again.arcs.size() == arcs.arcs.size() && again.minimal.arcs.size() == arcs.minimal.arcs.size());
    for (size_t a = 0; a < arcs.arcs.size(); a++) CHECK(arcs.arcs[a].fallback == again.arcs[a].fallback);
    for (size_t a = 0; a < arcs.minimal.arcs.size(); a++)
        CHECK(arcs.minimal.arcs[a].fallback == again.minimal.arcs[a].fallback);

    const AdminArcBlocks planet = encode_admin_arcs(arcs, store, 2);
    const ArcSetBlocks &full = planet.set(ArcSet::Full), &minimal = planet.set(ArcSet::Minimal);
    REQUIRE(full.fallback_keys.size() == arcs.arcs.size() && minimal.fallback_keys.size() == arcs.minimal.arcs.size());
    for (size_t a = 0; a < arcs.arcs.size(); a++) CHECK(full.fallback_keys[a] == arcs.arcs[a].fallback);
    for (size_t a = 0; a < arcs.minimal.arcs.size(); a++) CHECK(minimal.fallback_keys[a] == arcs.minimal.arcs[a].fallback);

    // A continent of the lower half's cells: each of its arcs keeps the planet's keys.
    std::vector<uint32_t> polygons;
    for (uint32_t k = 0; k < 8; k++) polygons.push_back(k);
    const AdminArcBlocks continent = subset_admin_arcs(planet, polygons, 2);
    for (ArcSet set : {ArcSet::Full, ArcSet::Minimal}) {
        const ArcSetBlocks &from = planet.set(set), &to = continent.set(set);
        std::map<uint64_t, ArcFallbackKeys> by_key;
        for (size_t a = 0; a < from.arcs(); a++) by_key[from.keys[a]] = from.fallback_keys[a];
        REQUIRE(to.fallback_keys.size() == to.arcs() && to.arcs() > 0);
        for (size_t a = 0; a < to.arcs(); a++) CHECK(to.fallback_keys[a] == by_key.at(to.keys[a]));
    }
}

// --- Slots by fallback keys ---

namespace {

namespace fs = std::filesystem;

// A set as the allocator left it: its keys, fallback keys, slots and table.
ArcSetBlocks set_of(const std::vector<uint64_t>& keys, const std::vector<ArcFallbackKeys>& fallback, const ArcSlots& slots) {
    ArcSetBlocks set;
    set.keys = keys;
    set.fallback_keys = fallback;
    set.slots = slots.slots;
    set.sidecar = slots.sidecar;
    return set;
}

// Both of a region's sets written as a previous build's sidecars under region_dir.
void write_region_sidecars(const std::string& region_dir, const AdminArcBlocks& blocks, bool with_fallback = true) {
    for (ArcSet set : {ArcSet::Full, ArcSet::Minimal}) {
        const std::string sidecar = arc_sidecar_path(region_dir, set);
        fs::create_directories(fs::path(sidecar).parent_path());
        gc::id_alloc::IdAllocator::write_sidecar(sidecar, blocks.set(set).sidecar);
        if (with_fallback) write_arc_fallback_sidecar(arc_fallback_sidecar_path(region_dir, set), blocks.set(set));
    }
}

// A planet day of the lattice's cells: its store, arc stage (chained on the
// previous build at prev_dir, none when empty) and encoded sets.
struct Day {
    AdminExactRings store;
    AdminArcs arcs;
    AdminArcBlocks blocks;
};
Day planet_day(const WayLattice& lattice, const std::vector<bool>& in_minimal, const std::string& prev_dir) {
    Day d;
    d.store = store_of(lattice.cells());
    d.arcs = build_admin_arcs(d.store, in_minimal, prev_dir.empty() ? "" : prev_dir + "/planet", 2);
    d.blocks = encode_admin_arcs(d.arcs, d.store, 2);
    return d;
}

// The smallest inner vertex of an edge, moved `dlat` (keeping its place
// among the edge's vertices: the edges here are 1000 long, jittered 125).
void move_smallest(Ring& edge, int32_t dlat) {
    auto smallest = std::min_element(edge.begin(), edge.end(), [](const PackedLocation& a, const PackedLocation& b) {
        return vertex_key(a) < vertex_key(b);
    });
    smallest->lat_e7 += dlat;
}

enum class PairBy { WaySet, Ends };

// Whether day two's set holds every slot day one's did and no other: an
// arc of a key day one had at that key's slot, each of the `changed` others
// at the slot of day one's arc of its way set or its ends (unique here), the
// counts saying how.
void check_slots_kept(const ArcSetBlocks& one, const ArcSetBlocks& two, PairBy by, size_t changed, size_t by_way_set,
                      size_t by_ends) {
    auto pair_key = [&](const ArcFallbackKeys& k) { return by == PairBy::WaySet ? k.way_set : k.ends; };
    std::map<uint64_t, uint32_t> slot_of_key, slot_of_pair;
    for (size_t a = 0; a < one.arcs(); a++) {
        slot_of_key[one.keys[a]] = one.slots[a];
        slot_of_pair[pair_key(one.fallback_keys[a])] = one.slots[a];
    }
    size_t others = 0;
    for (size_t a = 0; a < two.arcs(); a++) {
        const auto it = slot_of_key.find(two.keys[a]);
        if (it != slot_of_key.end()) {
            CHECK_EQ(two.slots[a], it->second);
            continue;
        }
        others++;
        const auto paired = slot_of_pair.find(pair_key(two.fallback_keys[a]));
        REQUIRE(paired != slot_of_pair.end());
        CHECK_EQ(two.slots[a], paired->second);
    }
    CHECK_EQ(others, changed);
    const AdminArcSlotCounts& c = two.slot_counts;
    CHECK_EQ(c.kept, two.arcs() - changed);
    CHECK_EQ(c.by_way_set, by_way_set);
    CHECK_EQ(c.by_ends, by_ends);
    CHECK_EQ(c.recycled + c.appended + c.tombstones, size_t(0));
    CHECK_EQ(c.previous_fallback_sidecar_missing, size_t(0));
    CHECK_EQ(two.sidecar.size(), one.sidecar.size());
}

}  // namespace

TEST(lost_arc_keys_take_their_previous_slots_by_way_set_then_by_ends) {
    using namespace gc::id_alloc;
    const ScratchDir dir("arc-fallback-passes");
    auto day = [&](int n, const std::vector<uint64_t>& keys, const std::vector<ArcFallbackKeys>& fallback, int prev) {
        const std::string at = dir.path() + "/" + std::to_string(prev);
        ArcSlots slots = allocate_arc_slots(keys, prev ? at : "", fallback, prev ? at + ".fallback" : "");
        const std::string next = dir.path() + "/" + std::to_string(n);
        IdAllocator::write_sidecar(next, slots.sidecar);
        write_arc_fallback_sidecar(next + ".fallback", set_of(keys, fallback, slots));
        return slots;
    };
    const ArcSlots one = day(1, {10, 20, 30, 40}, {{100, 200}, {101, 201}, {102, 202}, {103, 203}}, 0);
    CHECK(one.slots == std::vector<uint32_t>({0, 1, 2, 3}));
    // 10's arc keeps its way set, 20's its ends; 40 goes and 41 is new.
    const std::vector<ArcFallbackKeys> fallback = {{100, 900}, {555, 201}, {102, 202}, {777, 888}};
    const ArcSlots two = day(2, {11, 21, 30, 41}, fallback, 1);
    CHECK(two.slots == std::vector<uint32_t>({0, 1, 2, 4}));
    CHECK_EQ(two.counts.kept, size_t(1));
    CHECK_EQ(two.counts.by_way_set, size_t(1));
    CHECK_EQ(two.counts.by_ends, size_t(1));
    CHECK_EQ(two.counts.recycled, size_t(0));
    CHECK_EQ(two.counts.appended, size_t(1));
    CHECK_EQ(two.counts.tombstones, size_t(1));
    REQUIRE(two.sidecar.size() == 5u);
    CHECK(is_tombstone(two.sidecar[3]));
    CHECK(two.sidecar[0].stable_id == 11 && two.sidecar[1].stable_id == 21);
    CHECK(read_arc_fallback_sidecar(dir.path() + "/2.fallback", 5) ==
          std::vector<ArcFallbackKeys>({{100, 900}, {555, 201}, {102, 202}, {}, {777, 888}}));
    // The day after, the slots are their new keys'; 50 takes 40's tombstone.
    std::vector<ArcFallbackKeys> more = fallback;
    more.push_back({600, 601});
    const ArcSlots three = day(3, {11, 21, 30, 41, 50}, more, 2);
    CHECK(three.slots == std::vector<uint32_t>({0, 1, 2, 4, 3}));
    CHECK_EQ(three.counts.kept, size_t(4));
    CHECK_EQ(three.counts.by_way_set + three.counts.by_ends, size_t(0));
    CHECK_EQ(three.counts.recycled, size_t(1));

    // Two lost arcs of one way set: it matches neither, and their ends each its own.
    const ArcSlots a = day(4, {10, 20}, {{100, 200}, {100, 201}}, 0);
    const ArcSlots b = day(5, {11, 21}, {{100, 201}, {100, 200}}, 4);
    CHECK(b.slots == std::vector<uint32_t>({1, 0}));
    CHECK_EQ(b.counts.by_way_set, size_t(0));
    CHECK_EQ(b.counts.by_ends, size_t(2));
    // Without fallback keys, today's or the previous build's, they are new arcs.
    const ArcSlots c = allocate_arc_slots({11, 21}, dir.path() + "/4");
    CHECK(c.slots == std::vector<uint32_t>({2, 3}));
    CHECK_EQ(c.counts.previous_fallback_sidecar_missing, size_t(0));
    CHECK(refusal_of<std::logic_error>([&] { allocate_arc_slots({11, 21}, dir.path() + "/4", {{1, 2}}, dir.path() + "/4.fallback"); })
              .find("out of step") != std::string::npos);
}

TEST(the_first_night_without_fallback_keys_takes_slots_by_arc_keys_alone_and_says_so) {
    using namespace gc::id_alloc;
    const ScratchDir dir("arc-fallback-first-night");
    const std::vector<ArcFallbackKeys> fallback = {{100, 200}, {101, 201}};
    const ArcSlots one = allocate_arc_slots({10, 20}, "", fallback, "");
    IdAllocator::write_sidecar(dir.path() + "/1", one.sidecar);
    // No fallback sidecar beside it: 11 is a new arc, 10's slot a tombstone.
    const ArcSlots two = allocate_arc_slots({11, 20}, dir.path() + "/1", fallback, dir.path() + "/1.fallback");
    CHECK(two.slots == std::vector<uint32_t>({2, 1}));
    CHECK_EQ(two.counts.previous_fallback_sidecar_missing, size_t(1));
    CHECK_EQ(two.counts.appended, size_t(1));
    CHECK_EQ(two.counts.tombstones, size_t(1));
    const std::string lines = arc_slot_start_lines("planet", ArcSet::Full, dir.path() + "/1", two.counts);
    CHECK(lines.find("planet arcs: no previous full arc fallback keys beside " + dir.path() + "/1, slots by arc keys alone\n") !=
          std::string::npos);
    CHECK(lines.find("ARC_CHECK fallbacks=arcs set=full region=planet previous_fallback_sidecar_missing=1 by_way_set=0 "
                     "by_ends=0\n") != std::string::npos);
    // With it there, the passes' counts.
    write_arc_fallback_sidecar(dir.path() + "/1.fallback", set_of({10, 20}, fallback, one));
    const ArcSlots warm = allocate_arc_slots({11, 20}, dir.path() + "/1", fallback, dir.path() + "/1.fallback");
    CHECK(warm.slots == std::vector<uint32_t>({0, 1}));
    CHECK(arc_slot_start_lines("europe", ArcSet::Minimal, dir.path() + "/1", warm.counts) ==
          "ARC_CHECK slots=arcs set=minimal region=europe previous_sidecar_missing=0\n"
          "ARC_CHECK fallbacks=arcs set=minimal region=europe previous_fallback_sidecar_missing=0 by_way_set=1 by_ends=0\n");
    // A previous build without arc slots says that alone.
    const ArcSlots cold = allocate_arc_slots({11, 20}, dir.path() + "/none", fallback, dir.path() + "/none.fallback");
    CHECK_EQ(cold.counts.previous_fallback_sidecar_missing, size_t(0));
    CHECK(arc_slot_start_lines("planet", ArcSet::Full, dir.path() + "/none", cold.counts).find("fallbacks=") ==
          std::string::npos);
}

TEST(the_fallback_sidecar_holds_each_slots_keys_and_is_refused_unless_whole) {
    const ScratchDir dir("arc-fallback-sidecar");
    ArcSetBlocks set;
    set.keys = {7, 8};
    set.fallback_keys = {{70, 71}, {80, 81}};
    set.slots = {2, 0};
    set.sidecar.resize(4);  // slots 1 and 3 tombstones
    const std::string path = dir.path() + "/arc_index.fallback.osm_ids";
    write_arc_fallback_sidecar(path, set);
    CHECK_EQ(fs::file_size(path), uintmax_t(12 + 4 * 16));
    const ArcFallbackKeys none;
    CHECK(read_arc_fallback_sidecar(path, 4) == std::vector<ArcFallbackKeys>({{80, 81}, none, {70, 71}, none}));
    auto refusal = [&](const std::string& at, size_t slots) {
        return refusal_of([&] { read_arc_fallback_sidecar(at, slots); });
    };
    CHECK(refusal(path, 3).find("4 slots beside 3 in arc_index.osm_ids") != std::string::npos);
    CHECK(refusal(dir.path() + "/none", 4).find("no header") != std::string::npos);
    std::ofstream(dir.path() + "/garbage") << "not a fallback sidecar at all";
    CHECK(refusal(dir.path() + "/garbage", 4).find("not one") != std::string::npos);
    fs::copy_file(path, dir.path() + "/short");
    fs::resize_file(dir.path() + "/short", 12 + 3 * 16);
    CHECK(refusal(dir.path() + "/short", 4).find("cut short") != std::string::npos);
    fs::copy_file(path, dir.path() + "/long");
    std::ofstream(dir.path() + "/long", std::ios::binary | std::ios::app) << 'x';
    CHECK(refusal(dir.path() + "/long", 4).find("bytes past its slots") != std::string::npos);
    // The allocator refuses one it can't read rather than go without.
    gc::id_alloc::IdAllocator::write_sidecar(dir.path() + "/slots", std::vector<gc::id_alloc::SidecarSlot>(
        4, gc::id_alloc::SidecarSlot{uint8_t(gc::id_alloc::ObjectType::ARC), 0, 0, 0, 5}));
    CHECK(refusal_of([&] { allocate_arc_slots({5}, dir.path() + "/slots", {{1, 2}}, dir.path() + "/garbage"); })
              .find("unreadable arc fallback sidecar") != std::string::npos);
    set.slots = {2, 4};
    CHECK(refusal_of<std::logic_error>([&] { write_arc_fallback_sidecar(path, set); }).find("past its set's slot table") !=
          std::string::npos);
}

TEST(a_moved_smallest_vertex_keeps_its_arcs_slot_by_way_set) {
    const ScratchDir prev("arc-fallback-smallest");
    WayLattice lattice(3, 3, 4, 83, 1000);
    const std::vector<bool> in_minimal(9, true);
    const Day one = planet_day(lattice, in_minimal, "");
    write_region_sidecars(prev.path() + "/planet", one.blocks);
    // The edge from corner (1, 0) to (1, 1): its arc's key changes, not its ends or way.
    move_smallest(lattice.across[1][0], -40);
    const Day two = planet_day(lattice, in_minimal, prev.path());
    for (ArcSet set : {ArcSet::Full, ArcSet::Minimal})
        check_slots_kept(one.blocks.set(set), two.blocks.set(set), PairBy::WaySet, 1, 1, 0);
}

TEST(a_moved_junction_keeps_its_arcs_slots_by_way_set) {
    const ScratchDir prev("arc-fallback-junction");
    WayLattice lattice(3, 3, 4, 89, 1000);
    const std::vector<bool> in_minimal(9, true);
    const Day one = planet_day(lattice, in_minimal, "");
    write_region_sidecars(prev.path() + "/planet", one.blocks);
    // A corner four arcs end at: their keys and ends change, not their ways.
    lattice.corners[1][1] = pt(1030, 980);
    const Day two = planet_day(lattice, in_minimal, prev.path());
    for (ArcSet set : {ArcSet::Full, ArcSet::Minimal})
        check_slots_kept(one.blocks.set(set), two.blocks.set(set), PairBy::WaySet, 4, 4, 0);
}

TEST(a_moved_smallest_vertex_keeps_its_arcs_slot_by_ends_where_its_way_set_isnt_its_own) {
    const std::vector<bool> in_minimal(9, true);
    {
        // One way along two edges in a row: both arcs' way sets are it.
        const ScratchDir prev("arc-fallback-shared-way");
        WayLattice lattice(3, 3, 4, 97, 1000);
        lattice.across_way[1][1] = lattice.across_way[1][0];
        const Day one = planet_day(lattice, in_minimal, "");
        CHECK(one.arcs.counts.shared_way_sets >= size_t(1));
        write_region_sidecars(prev.path() + "/planet", one.blocks);
        move_smallest(lattice.across[1][0], -40);
        move_smallest(lattice.across[1][1], -40);
        const Day two = planet_day(lattice, in_minimal, prev.path());
        for (ArcSet set : {ArcSet::Full, ArcSet::Minimal})
            check_slots_kept(one.blocks.set(set), two.blocks.set(set), PairBy::Ends, 2, 0, 2);
    }
    {
        // The edge's way mapped again as a new one, its smallest vertex moved.
        const ScratchDir prev("arc-fallback-new-way");
        WayLattice lattice(3, 3, 4, 101, 1000);
        const Day one = planet_day(lattice, in_minimal, "");
        write_region_sidecars(prev.path() + "/planet", one.blocks);
        lattice.across_way[1][0] = 4242;
        move_smallest(lattice.across[1][0], -40);
        const Day two = planet_day(lattice, in_minimal, prev.path());
        for (ArcSet set : {ArcSet::Full, ArcSet::Minimal})
            check_slots_kept(one.blocks.set(set), two.blocks.set(set), PairBy::Ends, 1, 0, 1);
    }
}

TEST(the_first_night_without_fallback_sidecars_gives_a_moved_arc_a_new_slot) {
    const ScratchDir prev("arc-fallback-first-planet-night");
    WayLattice lattice(3, 3, 4, 83, 1000);
    const std::vector<bool> in_minimal(9, true);
    const Day one = planet_day(lattice, in_minimal, "");
    write_region_sidecars(prev.path() + "/planet", one.blocks, false);
    move_smallest(lattice.across[1][0], -40);
    const Day two = planet_day(lattice, in_minimal, prev.path());
    for (ArcSet set : {ArcSet::Full, ArcSet::Minimal}) {
        const AdminArcSlotCounts& c = two.blocks.set(set).slot_counts;
        CHECK_EQ(c.previous_fallback_sidecar_missing, size_t(1));
        CHECK_EQ(c.by_way_set + c.by_ends, size_t(0));
        CHECK_EQ(c.appended, size_t(1));
        CHECK_EQ(c.tombstones, size_t(1));
    }
}

TEST(a_continent_keeps_a_moved_arcs_slot_by_its_own_fallback_sidecars) {
    const ScratchDir prev("arc-fallback-continent");
    WayLattice lattice(3, 3, 4, 107, 1000);
    const std::vector<bool> in_minimal(9, true);
    // The continent: the lower two rows' cells.
    const std::vector<uint32_t> polygons = {0, 1, 2, 3, 4, 5};
    auto continent = [&](const Day& d, const std::string& prev_dir) {
        AdminArcBlocks c = subset_admin_arcs(d.blocks, polygons, 2);
        assign_region_arc_slots(c, prev_dir, "test", 2);
        return c;
    };
    const Day one = planet_day(lattice, in_minimal, "");
    write_region_sidecars(prev.path() + "/planet", one.blocks);
    const AdminArcBlocks c1 = continent(one, "");
    write_region_sidecars(prev.path() + "/test", c1);
    move_smallest(lattice.across[1][0], -40);
    const Day two = planet_day(lattice, in_minimal, prev.path());
    const AdminArcBlocks c2 = continent(two, prev.path() + "/test");
    for (ArcSet set : {ArcSet::Full, ArcSet::Minimal})
        check_slots_kept(c1.set(set), c2.set(set), PairBy::WaySet, 1, 1, 0);
}
