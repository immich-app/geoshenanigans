// admin_arcs.h: the arc topology (junctions, cuts, canonical arcs, refs), the
// arcs' keys and Hilbert presentation order, their slots, the minimal set, the
// tiers (simplifying, the snap, validity), the arc stage's dirs and its
// per-polygon rings following each admin permute. In the sanitized suite: the
// topology's scatter and partition passes and the minimal chains index their
// arrays directly.
#include "admin_arcs.h"

#include <algorithm>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <map>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "parsed_data.h"
#include "reorder.h"
#include "scratch_dir.h"
#include "test_framework.h"

using gctest::refusal_of;

namespace {

using Ring = std::vector<PackedLocation>;

struct Polygon {
    Ring outer;
    std::vector<Ring> holes;
};

PackedLocation pt(int32_t lat, int32_t lng) { return {lat, lng}; }

Ring closed(Ring ring) {
    ring.push_back(ring.front());
    return ring;
}

AdminExactRings store_of(const std::vector<Polygon>& polygons) {
    AdminExactRings store;
    for (const Polygon& p : polygons) store.append(p.outer, p.holes);
    return store;
}

std::vector<PackedLocation> vertices_of(const AdminExactRings& store, const Arc& arc) {
    const ArcVertices av = arc_vertices(store, arc.view);
    std::vector<PackedLocation> out;
    for (uint32_t i = 0; i < av.size(); i++) out.push_back(av[i]);
    return out;
}

// Whether every ring of the store in set `set` (a minimal ring for the
// minimal set) comes back from its refs: the same cycle, in the ring's own
// direction.
bool every_ring_rebuilds(const AdminArcs& arcs, const AdminExactRings& store, ArcSet set = ArcSet::Full) {
    for (uint32_t r = 0; r < store.rings.size(); r++) {
        const RingSpan& span = store.rings[r];
        const std::vector<PackedLocation> rebuilt = rebuild_ring(arcs, store, set, r);
        if (set == ArcSet::Minimal && rebuilt.empty()) continue;
        if (!same_closed_ring(rebuilt.data(), static_cast<uint32_t>(rebuilt.size()), store.vertices.data() + span.begin,
                              span.count))
            return false;
        // Not merely the reverse: the chain runs the way the ring does.
        Ring forwards(store.vertices.begin() + span.begin, store.vertices.begin() + span.begin + span.count);
        Ring doubled(forwards.begin(), forwards.end() - 1);
        doubled.insert(doubled.end(), forwards.begin(), forwards.end() - 1);
        bool found = false;
        for (size_t s = 0; s + 1 < forwards.size() && !found; s++)
            found = std::equal(rebuilt.begin(), rebuilt.end() - 1, doubled.begin() + s);
        if (!found) return false;
    }
    return true;
}

// The refs that use each arc.
std::vector<std::vector<ArcRef>> users_of(const AdminArcs& arcs) {
    std::vector<std::vector<ArcRef>> users(arcs.arcs.size());
    for (const ArcRef& ref : arcs.refs) users[ref.arc].push_back(ref);
    return users;
}

// A lattice of rows x cols square cells of side `side` e7 units whose edges
// each carry `inner` vertices between the corners, jittered off the straight
// line, so that cells side by side share their edge (the same coordinates,
// walked the other way).
struct Lattice {
    Lattice(int rows, int cols, int inner, uint64_t seed, int32_t side) : side(side) {
        std::mt19937_64 rng(seed);
        std::uniform_int_distribution<int32_t> jitter(-side / 8, side / 8);
        auto make = [&](int edge_rows, int edge_cols, bool horizontal) {
            std::vector<std::vector<Ring>> edges(edge_rows, std::vector<Ring>(edge_cols));
            for (int r = 0; r < edge_rows; r++)
                for (int c = 0; c < edge_cols; c++)
                    for (int k = 1; k <= inner; k++) {
                        const int32_t along = k * side / (inner + 1), off = jitter(rng);
                        edges[r][c].push_back(horizontal ? pt(r * side + off, c * side + along)
                                                         : pt(r * side + along, c * side + off));
                    }
            return edges;
        };
        across = make(rows + 1, cols, true);
        upward = make(rows, cols + 1, false);
    }

    PackedLocation corner(int r, int c) const { return pt(r * side, c * side); }

    // The boundary of the cells [r, r + h) x [c, c + w), counterclockwise
    // from its lower left corner, closed.
    Ring block(int r, int c, int h, int w) const {
        Ring ring;
        for (int j = c; j < c + w; j++) {
            ring.push_back(corner(r, j));
            ring.insert(ring.end(), across[r][j].begin(), across[r][j].end());
        }
        for (int i = r; i < r + h; i++) {
            ring.push_back(corner(i, c + w));
            ring.insert(ring.end(), upward[i][c + w].begin(), upward[i][c + w].end());
        }
        for (int j = c + w; j > c; j--) {
            ring.push_back(corner(r + h, j));
            ring.insert(ring.end(), across[r + h][j - 1].rbegin(), across[r + h][j - 1].rend());
        }
        for (int i = r + h; i > r; i--) {
            ring.push_back(corner(i, c));
            ring.insert(ring.end(), upward[i - 1][c].rbegin(), upward[i - 1][c].rend());
        }
        return closed(ring);
    }

    int32_t side;
    std::vector<std::vector<Ring>> across;  // the edge from corner (r, c) to (r, c + 1)
    std::vector<std::vector<Ring>> upward;  // the edge from corner (r, c) to (r + 1, c)
};

// An n x n grid of the lattice's cells, every cell a polygon.
std::vector<Polygon> grid(int n, int inner, uint64_t seed, int32_t side = 1000) {
    const Lattice lattice(n, n, inner, seed, side);
    std::vector<Polygon> cells;
    for (int r = 0; r < n; r++)
        for (int c = 0; c < n; c++) cells.push_back({lattice.block(r, c, 1, 1), {}});
    return cells;
}

// The same rings, each starting `shift(ring)` vertices on, and walked
// backwards where `backwards(ring)`.
template <class Shift, class Backwards>
std::vector<Polygon> restarted(std::vector<Polygon> polygons, Shift shift, Backwards backwards) {
    size_t k = 0;
    auto redo = [&](Ring& ring) {
        Ring cycle(ring.begin(), ring.end() - 1);
        std::rotate(cycle.begin(), cycle.begin() + shift(k) % cycle.size(), cycle.end());
        if (backwards(k)) std::reverse(cycle.begin(), cycle.end());
        ring = closed(cycle);
        k++;
    };
    for (Polygon& p : polygons) {
        redo(p.outer);
        for (Ring& h : p.holes) redo(h);
    }
    return polygons;
}

std::vector<uint64_t> keys_of(const AdminArcs& arcs) {
    std::vector<uint64_t> keys;
    for (const Arc& a : arcs.arcs) keys.push_back(a.key);
    return keys;
}

// A key's arc, by key (the tests' arcs have unique keys).
std::map<uint64_t, std::vector<PackedLocation>> arcs_by_key(const AdminArcs& arcs, const AdminExactRings& store) {
    std::map<uint64_t, std::vector<PackedLocation>> out;
    for (const Arc& a : arcs.arcs) out[a.key] = vertices_of(store, a);
    return out;
}

}  // namespace

// --- Topology ---

TEST(two_neighbours_share_their_border_as_one_arc) {
    const AdminExactRings store = store_of({
        {closed({pt(0, 0), pt(0, 10), pt(5, 10), pt(10, 10), pt(10, 0)}), {}},
        {closed({pt(0, 10), pt(0, 20), pt(10, 20), pt(10, 10), pt(5, 10)}), {}},
    });
    const AdminArcs arcs = build_arc_topology(store, 1);
    CHECK_EQ(arcs.counts.junctions, size_t(2));
    CHECK_EQ(arcs.counts.arcs, size_t(3));
    CHECK_EQ(arcs.counts.refs, size_t(4));
    CHECK(every_ring_rebuilds(arcs, store));
    // The border from its smaller end, once, walked each way.
    const Ring border{pt(0, 10), pt(5, 10), pt(10, 10)};
    const auto users = users_of(arcs);
    bool found = false;
    for (size_t a = 0; a < arcs.arcs.size(); a++) {
        if (vertices_of(store, arcs.arcs[a]) != border) continue;

        found = true;
        REQUIRE(users[a].size() == 2u);
        CHECK(users[a][0].reversed != users[a][1].reversed);
    }
    CHECK(found);
}

TEST(a_ring_with_no_junction_is_one_closed_arc_from_its_smallest_vertex) {
    // Smallest K: the smallest latitude, then longitude; towards (2, 8),
    // the smaller of its neighbours.
    const Ring cycle{pt(5, 0), pt(0, 3), pt(2, 8), pt(8, 8), pt(10, 3)};
    const Ring canonical{pt(0, 3), pt(2, 8), pt(8, 8), pt(10, 3), pt(5, 0), pt(0, 3)};
    for (size_t shift = 0; shift < cycle.size(); shift++)
        for (bool backwards : {false, true}) {
            Ring given = cycle;
            std::rotate(given.begin(), given.begin() + shift, given.end());
            if (backwards) std::reverse(given.begin(), given.end());
            const AdminExactRings store = store_of({{closed(given), {}}});
            const AdminArcs arcs = build_arc_topology(store, 1);
            REQUIRE(arcs.arcs.size() == 1u);
            CHECK(arcs.arcs[0].view.closed);
            CHECK(vertices_of(store, arcs.arcs[0]) == canonical);
            CHECK_EQ(arcs.counts.junctions, size_t(0));
            REQUIRE(arcs.refs.size() == 1u);
            CHECK_EQ(arcs.refs[0].reversed, backwards);
            CHECK(every_ring_rebuilds(arcs, store));
        }
    // K is unsigned: a negative latitude sorts after every positive one.
    const AdminExactRings south = store_of({{closed({pt(-5, 0), pt(3, 4), pt(3, -4)}), {}}});
    const AdminArcs arcs = build_arc_topology(south, 1);
    REQUIRE(arcs.arcs.size() == 1u);
    CHECK(vertices_of(south, arcs.arcs[0]).front() == pt(3, 4));
}

TEST(a_spike_is_cut_at_its_tip_and_its_base_and_shared_by_both_sides) {
    const AdminExactRings store =
        store_of({{closed({pt(0, 0), pt(0, 10), pt(5, 10), pt(5, 15), pt(5, 10), pt(10, 10), pt(10, 0)}), {}}});
    const AdminArcs arcs = build_arc_topology(store, 1);
    CHECK_EQ(arcs.counts.junctions, size_t(2));
    CHECK_EQ(arcs.counts.arcs, size_t(2));
    CHECK_EQ(arcs.counts.refs, size_t(3));
    CHECK(every_ring_rebuilds(arcs, store));
    const auto users = users_of(arcs);
    for (size_t a = 0; a < arcs.arcs.size(); a++) {
        const auto v = vertices_of(store, arcs.arcs[a]);
        if (v.size() == 2) {
            CHECK(v == Ring({pt(5, 10), pt(5, 15)}));
            REQUIRE(users[a].size() == 2u);
            CHECK(users[a][0].reversed != users[a][1].reversed);
        } else {
            // Both ends at the spike's base: the smaller second vertex first.
            CHECK(v == Ring({pt(5, 10), pt(0, 10), pt(0, 0), pt(10, 0), pt(10, 10), pt(5, 10)}));
        }
    }
}

TEST(rings_meeting_at_one_coordinate_meet_at_one_vertex) {
    // Two triangles touching at (5, 5): different nodes in OSM, one vertex here.
    const AdminExactRings touching = store_of({
        {closed({pt(0, 0), pt(0, 10), pt(5, 5)}), {}},
        {closed({pt(5, 5), pt(10, 10), pt(10, 0)}), {}},
    });
    const AdminArcs arcs = build_arc_topology(touching, 1);
    CHECK_EQ(arcs.counts.junctions, size_t(1));
    CHECK_EQ(arcs.counts.arcs, size_t(2));
    CHECK(every_ring_rebuilds(arcs, touching));
    for (const Arc& a : arcs.arcs) CHECK(vertices_of(touching, a).front() == pt(5, 5));

    // A coordinate twice in a row stays twice: a junction, and an arc of its own.
    const AdminExactRings repeated = store_of({{closed({pt(0, 0), pt(0, 10), pt(0, 10), pt(10, 10), pt(10, 0)}), {}}});
    const AdminArcs arcs2 = build_arc_topology(repeated, 1);
    CHECK_EQ(arcs2.counts.junctions, size_t(1));
    CHECK_EQ(arcs2.counts.arcs, size_t(2));
    CHECK_EQ(arcs2.counts.arc_vertices, size_t(2 + 5));
    CHECK(every_ring_rebuilds(arcs2, repeated));
}

TEST(a_grid_of_cells_has_a_junction_at_every_corner_three_borders_meet) {
    for (int n : {2, 3, 7}) {
        const int inner = 3;
        std::vector<Polygon> cells = grid(n, inner, 17 + n);
        // An island in the middle cell, which has it as a hole: one closed arc.
        const int32_t mid = (n / 2) * 1000;
        const Ring island = closed({pt(mid + 400, mid + 400), pt(mid + 400, mid + 600), pt(mid + 600, mid + 500)});
        cells[(n / 2) * n + n / 2].holes.push_back(island);
        cells.push_back({island, {}});
        const AdminExactRings store = store_of(cells);
        const AdminArcs arcs = build_arc_topology(store, 1);
        const size_t corners = size_t(n + 1) * (n + 1), edges = size_t(2) * n * (n + 1);
        CHECK_EQ(arcs.counts.junctions, corners - 4);
        CHECK_EQ(arcs.counts.arcs, edges - 4 + 1);
        CHECK_EQ(arcs.counts.closed_arcs, size_t(1));
        CHECK_EQ(arcs.counts.refs, size_t(4) * n * n - 4 + 2);
        // A border inside the grid is one arc of two refs, walked each way.
        const auto users = users_of(arcs);
        size_t shared = 0;
        for (size_t a = 0; a < users.size(); a++) {
            REQUIRE(!users[a].empty());
            if (users[a].size() == 2) {
                shared++;
                if (!arcs.arcs[a].view.closed) CHECK(users[a][0].reversed != users[a][1].reversed);
            }
        }
        CHECK_EQ(shared, edges - 4 * size_t(n) + 1);
        CHECK(every_ring_rebuilds(arcs, store));
        // Every vertex once per arc, a closed arc's first twice.
        CHECK_EQ(arcs.counts.arc_vertices,
                 (edges - 4 * size_t(n)) * (inner + 2) + (4 * size_t(n) - 8) * (inner + 2) + 4 * size_t(2 * inner + 3) + 4);
    }
}

TEST(arc_boxes_bound_their_raw_vertices) {
    const AdminExactRings store = store_of(grid(4, 5, 99));
    const AdminArcs arcs = build_arc_topology(store, 1);
    for (const Arc& a : arcs.arcs) {
        ArcBox want;
        for (const PackedLocation& p : vertices_of(store, a)) {
            want.min_lng = std::min(want.min_lng, p.lon_e7);
            want.max_lng = std::max(want.max_lng, p.lon_e7);
            want.max_lat = std::max(want.max_lat, p.lat_e7);
        }
        CHECK(a.box == want);
    }
}

// As a split way can leave its relation's ring: the same coordinates, from
// another vertex or the other way round.
TEST(arcs_and_their_keys_survive_a_ring_starting_elsewhere_or_running_backwards) {
    const std::vector<Polygon> cells = grid(6, 4, 5);
    const AdminExactRings store = store_of(cells);
    const AdminArcs arcs = build_arc_topology(store, 1);
    std::mt19937_64 rng(11);
    std::vector<size_t> shifts(cells.size());
    std::vector<bool> flips(cells.size());
    for (size_t k = 0; k < cells.size(); k++) {
        shifts[k] = rng() % 20;
        flips[k] = rng() % 2;
    }
    const AdminExactRings moved = store_of(restarted(cells, [&](size_t k) { return shifts[k]; },
                                                     [&](size_t k) { return bool(flips[k]); }));
    const AdminArcs moved_arcs = build_arc_topology(moved, 1);
    REQUIRE(moved_arcs.arcs.size() == arcs.arcs.size());
    for (size_t a = 0; a < arcs.arcs.size(); a++) {
        CHECK(vertices_of(moved, moved_arcs.arcs[a]) == vertices_of(store, arcs.arcs[a]));
        CHECK_EQ(moved_arcs.arcs[a].key, arcs.arcs[a].key);
        CHECK(moved_arcs.arcs[a].box == arcs.arcs[a].box);
    }
    CHECK(every_ring_rebuilds(moved_arcs, moved));
}

// --- Keys ---

namespace {

// The vertex of the border between cells (1, 0) and (1, 1) of a 2 x 2 grid
// (side 1000, from the middle up to (2000, 1000)) at `lat`.
PackedLocation middle_border_vertex(const std::vector<Polygon>& cells, int32_t lat) {
    for (const PackedLocation& p : cells[3].outer)
        if (p.lat_e7 == lat && std::abs(p.lon_e7 - 1000) <= 125) return p;
    return pt(0, 0);
}

std::vector<Polygon> moved(std::vector<Polygon> polygons, PackedLocation from, PackedLocation to) {
    for (Polygon& p : polygons)
        for (PackedLocation& v : p.outer)
            if (v == from) v = to;
    return polygons;
}

size_t keys_not_in(const AdminArcs& arcs, const AdminArcs& before) {
    const std::vector<uint64_t> was = keys_of(before);
    const std::set<uint64_t> had(was.begin(), was.end());
    size_t changed = 0;
    for (const Arc& a : arcs.arcs) changed += had.count(a.key) == 0;
    return changed;
}

}  // namespace

TEST(an_arc_keeps_its_key_when_an_interior_vertex_other_than_its_smallest_moves) {
    const std::vector<Polygon> cells = grid(2, 3, 23);
    const AdminExactRings store = store_of(cells);
    const AdminArcs arcs = build_arc_topology(store, 1);
    // The middle border's inner vertices sit at latitudes 1250, 1500 and
    // 1750: the smallest is the first. Move the second, in both cells.
    const PackedLocation second = middle_border_vertex(cells, 1500);
    REQUIRE(second.lat_e7 == 1500);
    const AdminExactRings edited = store_of(moved(cells, second, pt(1530, second.lon_e7 + 70)));
    const AdminArcs edited_arcs = build_arc_topology(edited, 1);
    CHECK(keys_of(edited_arcs) == keys_of(arcs));
    CHECK(arcs_by_key(edited_arcs, edited) != arcs_by_key(arcs, store));

    // Moving the smallest one changes the key of that arc alone.
    const PackedLocation first = middle_border_vertex(cells, 1250);
    REQUIRE(first.lat_e7 == 1250);
    const AdminArcs lowered = build_arc_topology(store_of(moved(cells, first, pt(1100, first.lon_e7))), 1);
    CHECK_EQ(keys_not_in(lowered, arcs), size_t(1));
}

TEST(an_arc_key_changes_when_its_junction_moves) {
    const std::vector<Polygon> cells = grid(2, 3, 31);
    const AdminExactRings store = store_of(cells);
    const AdminArcs arcs = build_arc_topology(store, 1);
    // The grid's middle, where four arcs end.
    const AdminExactRings moved_store = store_of(moved(cells, pt(1000, 1000), pt(1010, 990)));
    const AdminArcs moved_arcs = build_arc_topology(moved_store, 1);
    const std::vector<uint64_t> was = keys_of(arcs);
    const std::set<uint64_t> before(was.begin(), was.end());
    size_t at_middle = 0;
    for (const Arc& a : moved_arcs.arcs) {
        const auto v = vertices_of(moved_store, a);
        const bool ends_there = v.front() == pt(1010, 990) || v.back() == pt(1010, 990);
        CHECK_EQ(before.count(a.key) == 0, ends_there);
        at_middle += ends_there;
    }
    CHECK_EQ(at_middle, size_t(4));
    CHECK_EQ(keys_not_in(moved_arcs, arcs), size_t(4));
}

// --- Order ---

TEST(the_hilbert_index_walks_its_curve_one_cell_at_a_time) {
    // The curve's first 4^4 cells fill the 16 x 16 corner, each next to the last.
    std::vector<std::pair<uint32_t, uint32_t>> at(256, {UINT32_MAX, UINT32_MAX});
    for (uint32_t x = 0; x < 16; x++)
        for (uint32_t y = 0; y < 16; y++) {
            const uint64_t d = hilbert_index(x, y);
            REQUIRE(d < 256);
            CHECK(at[d].first == UINT32_MAX);
            at[d] = {x, y};
        }
    for (size_t d = 1; d < 256; d++) {
        const int64_t dx = int64_t(at[d].first) - at[d - 1].first, dy = int64_t(at[d].second) - at[d - 1].second;
        CHECK_EQ(std::abs(dx) + std::abs(dy), int64_t(1));
    }
    // An e7 point's corner of the globe is the curve's start; the far one fits.
    CHECK_EQ(hilbert_index(pt(-900000000, -1800000000)), uint64_t(0));
    CHECK(hilbert_index(pt(900000000, 1800000000)) == hilbert_index(uint32_t(3600000000u), uint32_t(1800000000u)));
}

TEST(arcs_are_presented_in_one_total_order_whatever_the_threads) {
    std::vector<Polygon> cells = grid(200, 2, 41, 2000);
    // Shifted so that some cells straddle the zero meridian and equator.
    for (Polygon& p : cells)
        for (PackedLocation& v : p.outer) v = pt(v.lat_e7 - 150000, v.lon_e7 - 170000);
    const AdminExactRings store = store_of(cells);
    const AdminArcs arcs = build_arc_topology(store, 1);
    for (size_t a = 1; a < arcs.arcs.size(); a++) {
        const Arc &prev = arcs.arcs[a - 1], &next = arcs.arcs[a];
        const auto pv = vertices_of(store, prev), nv = vertices_of(store, next);
        const uint64_t ph = hilbert_index(pv[0]), nh = hilbert_index(nv[0]);
        auto keys = [](const std::vector<PackedLocation>& v) {
            std::vector<uint64_t> k;
            for (const PackedLocation& p : v) k.push_back(vertex_key(p));
            return k;
        };
        const bool ordered = ph != nh ? ph < nh : prev.key != next.key ? prev.key < next.key : keys(pv) < keys(nv);
        CHECK(ordered);
    }
    for (unsigned threads : {2u, 7u, 32u}) {
        const AdminArcs again = build_arc_topology(store, threads);
        REQUIRE(again.arcs.size() == arcs.arcs.size());
        bool same = again.refs == arcs.refs && again.ring_refs == arcs.ring_refs;
        for (size_t a = 0; a < arcs.arcs.size(); a++)
            same = same && vertices_of(store, again.arcs[a]) == vertices_of(store, arcs.arcs[a]) &&
                   again.arcs[a].key == arcs.arcs[a].key;
        CHECK(same);
        CHECK_EQ(again.counts.junctions, arcs.counts.junctions);
    }
}

// --- Slots ---

TEST(no_arc_slot_is_reused_on_the_day_it_is_freed) {
    using namespace gc::id_alloc;
    const ScratchDir dir("admin-arc-slots");
    auto day = [&](const std::vector<uint64_t>& keys, const std::string& prev, const std::string& next) {
        ArcSlots slots = allocate_arc_slots(keys, prev);
        IdAllocator::write_sidecar(next, slots.sidecar);
        return slots;
    };
    const ArcSlots one = day({10, 20, 30}, "", dir.path() + "/1");
    CHECK(one.slots == std::vector<uint32_t>({0, 1, 2}));
    // 20 goes: its slot is a tombstone today, and 40 is appended.
    const ArcSlots two = day({10, 30, 40}, dir.path() + "/1", dir.path() + "/2");
    CHECK(two.slots == std::vector<uint32_t>({0, 2, 3}));
    REQUIRE(two.sidecar.size() == 4u);
    CHECK(is_tombstone(two.sidecar[1]));
    CHECK_EQ(two.counts.kept, size_t(2));
    CHECK_EQ(two.counts.appended, size_t(1));
    CHECK_EQ(two.counts.tombstones, size_t(1));
    CHECK_EQ(two.sidecar[3].object_type, uint8_t(ObjectType::ARC));
    // The day after, 50 takes it.
    const ArcSlots three = day({10, 30, 40, 50}, dir.path() + "/2", dir.path() + "/3");
    CHECK(three.slots == std::vector<uint32_t>({0, 2, 3, 1}));
    CHECK_EQ(three.counts.recycled, size_t(1));
    CHECK_EQ(three.counts.tombstones, size_t(0));
    // A key twice: the first in presentation order keeps the slot.
    const ArcSlots twice = allocate_arc_slots({30, 10, 30}, dir.path() + "/3");
    CHECK(twice.slots == std::vector<uint32_t>({2, 0, 4}));
    CHECK(refusal_of([&] { allocate_arc_slots({1}, dir.path() + "/none"); }).find("unreadable") != std::string::npos);
}

TEST(a_chained_arc_stage_keeps_each_unchanged_arcs_slot) {
    using gc::id_alloc::IdAllocator;
    const ScratchDir prev("admin-arc-chain");
    const std::vector<Polygon> cells = grid(5, 3, 77);
    const AdminExactRings store = store_of(cells);
    // Every cell in the minimal set too, whose arcs are then the full set's.
    const std::vector<bool> in_minimal(cells.size(), true);
    const AdminArcs day_one = build_admin_arcs(store, in_minimal, "", 4);
    for (const Arc& a : day_one.arcs) CHECK(a.planet_slot < day_one.arcs.size());
    for (ArcSet set : {ArcSet::Full, ArcSet::Minimal}) {
        const std::string sidecar = arc_sidecar_path(prev.path() + "/planet", set);
        std::filesystem::create_directories(std::filesystem::path(sidecar).parent_path());
        IdAllocator::write_sidecar(sidecar, set == ArcSet::Full ? day_one.planet_sidecar : day_one.minimal.planet_sidecar);
    }

    // A junction moves: its arcs get new slots, not the ones they leave.
    const AdminExactRings edited = store_of(moved(cells, pt(2000, 2000), pt(2020, 1990)));
    const AdminArcs day_two = build_admin_arcs(edited, in_minimal, prev.path() + "/planet", 4);
    auto slots_follow_keys = [&](auto key_slot_one, auto key_slot_two, size_t one, size_t two) {
        std::map<uint64_t, uint32_t> slot_of;
        for (size_t a = 0; a < one; a++) slot_of[key_slot_one(a).first] = key_slot_one(a).second;
        size_t kept = 0;
        for (size_t a = 0; a < two; a++) {
            const auto [key, slot] = key_slot_two(a);
            auto it = slot_of.find(key);
            if (it != slot_of.end()) {
                CHECK_EQ(slot, it->second);
                kept++;
            } else {
                CHECK(slot >= one);
            }
        }
        return kept;
    };
    const size_t kept = slots_follow_keys(
        [&](size_t a) { return std::make_pair(day_one.arcs[a].key, day_one.arcs[a].planet_slot); },
        [&](size_t a) { return std::make_pair(day_two.arcs[a].key, day_two.arcs[a].planet_slot); },
        day_one.arcs.size(), day_two.arcs.size());
    CHECK_EQ(kept, day_one.arcs.size() - 4);
    CHECK_EQ(day_two.slot_counts.kept, kept);
    CHECK_EQ(day_two.slot_counts.appended, size_t(4));
    CHECK_EQ(day_two.slot_counts.tombstones, size_t(4));
    CHECK_EQ(day_two.planet_sidecar.size(), day_one.arcs.size() + 4);
    const MinimalArcs &m1 = day_one.minimal, &m2 = day_two.minimal;
    CHECK_EQ(m1.arcs.size(), day_one.arcs.size());
    const size_t minimal_kept = slots_follow_keys(
        [&](size_t a) { return std::make_pair(m1.arcs[a].key, m1.arcs[a].planet_slot); },
        [&](size_t a) { return std::make_pair(m2.arcs[a].key, m2.arcs[a].planet_slot); }, m1.arcs.size(),
        m2.arcs.size());
    CHECK_EQ(minimal_kept, m1.arcs.size() - 4);
    CHECK_EQ(m2.slot_counts.kept, minimal_kept);
    CHECK_EQ(m2.slot_counts.appended, size_t(4));
}

// --- The minimal set ---

namespace {

std::vector<PackedLocation> minimal_vertices(const AdminExactRings& store, const AdminArcs& arcs, uint32_t a) {
    const MinimalArcVertices v(store, arcs, a);
    std::vector<PackedLocation> out;
    for (uint32_t i = 0; i < v.size(); i++) out.push_back(v[i]);
    return out;
}

AdminArcs full_and_minimal(const AdminExactRings& store, const std::vector<bool>& in_minimal, unsigned threads = 1) {
    AdminArcs arcs = build_arc_topology(store, threads);
    arcs.minimal = build_minimal_arcs(arcs, store, in_minimal, threads);
    return arcs;
}

// An island of `n` vertices round (lat, lng), radius about `r`, jittered.
Ring island(int32_t lat, int32_t lng, int32_t r, int n, uint64_t seed) {
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<int32_t> jitter(-r / 10, r / 10);
    Ring ring;
    for (int k = 0; k < n; k++) {
        const double a = 2 * 3.14159265358979 * k / n;
        ring.push_back(pt(lat + int32_t(std::lround(r * std::sin(a))) + jitter(rng),
                          lng + int32_t(std::lround(r * std::cos(a))) + jitter(rng)));
    }
    return closed(ring);
}

// The halves of a closed ring either side of the chord from its vertex 0 to
// its vertex k, through `middle`.
std::pair<Ring, Ring> halves(const Ring& ring, size_t k, PackedLocation middle) {
    Ring a(ring.begin(), ring.begin() + k + 1), b(ring.begin() + k, ring.end());
    a.push_back(middle);
    b.push_back(middle);
    return {closed(a), closed(b)};
}

}  // namespace

TEST(a_minimal_border_is_one_arc_across_a_junction_only_other_polygons_make) {
    // Two minimal squares and a triangle of another level (not minimal)
    // inside the left one, touching their border at (5, 0).
    const Ring left = closed({pt(0, -10), pt(0, 0), pt(5, 0), pt(10, 0), pt(10, -10)});
    const Ring right = closed({pt(0, 0), pt(0, 10), pt(10, 10), pt(10, 0), pt(5, 0)});
    const Ring triangle = closed({pt(5, 0), pt(4, -3), pt(6, -3)});
    const AdminExactRings store = store_of({{left, {}}, {right, {}}, {triangle, {}}});
    const AdminArcs arcs = full_and_minimal(store, {true, true, false});
    CHECK_EQ(arcs.counts.junctions, size_t(3));
    CHECK_EQ(arcs.counts.arcs, size_t(5));
    const MinimalArcs& m = arcs.minimal;
    CHECK_EQ(m.counts.joined_junctions, size_t(1));
    CHECK_EQ(m.counts.arcs, size_t(3));
    CHECK_EQ(m.counts.rings, size_t(2));
    CHECK_EQ(m.counts.polygons, size_t(2));
    bool found = false;
    for (uint32_t a = 0; a < m.arcs.size(); a++) {
        if (minimal_vertices(store, arcs, a) != Ring({pt(0, 0), pt(5, 0), pt(10, 0)})) continue;
        found = true;
        CHECK_EQ(m.arcs[a].part_count, 2u);
        CHECK(!m.arcs[a].closed);
    }
    CHECK(found);
    CHECK(every_ring_rebuilds(arcs, store, ArcSet::Minimal));
    // The triangle's ring has no minimal refs.
    CHECK_EQ(m.ring_refs[3], m.ring_refs[2]);
}

TEST(a_minimal_loop_joined_from_full_arcs_starts_at_its_smallest_vertex) {
    // A minimal island cut in two by polygons of another level, whose chord
    // meets it at its vertices 0 and 5: two full arcs, one minimal loop.
    const Ring ring = island(5000, 5000, 400, 10, 9);
    const auto [a, b] = halves(ring, 5, pt(5000, 5000));
    const AdminExactRings store = store_of({{ring, {}}, {a, {}}, {b, {}}});
    const AdminArcs arcs = full_and_minimal(store, {true, false, false});
    CHECK_EQ(arcs.counts.junctions, size_t(2));
    const MinimalArcs& m = arcs.minimal;
    REQUIRE(m.arcs.size() == 1u);
    CHECK(m.arcs[0].closed);
    CHECK_EQ(m.arcs[0].part_count, 2u);
    CHECK_EQ(m.counts.joined_junctions, size_t(2));
    // As the island alone has it.
    const AdminExactRings alone = store_of({{ring, {}}});
    const AdminArcs alone_arcs = build_arc_topology(alone, 1);
    REQUIRE(alone_arcs.arcs.size() == 1u);
    CHECK(minimal_vertices(store, arcs, 0) == vertices_of(alone, alone_arcs.arcs[0]));
    CHECK_EQ(m.arcs[0].key, alone_arcs.arcs[0].key);
    uint64_t smallest = UINT64_MAX;
    for (const PackedLocation& p : ring) smallest = std::min(smallest, vertex_key(p));
    CHECK_EQ(vertex_key(minimal_vertices(store, arcs, 0).front()), smallest);
    // Neither junction is the smallest: the loop starts inside a full arc.
    CHECK(m.arcs[0].start > 0 || vertex_key(ring[0]) == smallest || vertex_key(ring[5]) == smallest);
    CHECK(every_ring_rebuilds(arcs, store, ArcSet::Minimal));
}

TEST(the_minimal_set_is_the_topology_of_the_minimal_polygons_alone) {
    for (uint64_t seed : {3u, 4u, 5u}) {
        // Minimal 2 x 2 blocks over every cell of another level, one block
        // and its cell with a minimal island as a hole, the island cut in
        // two by polygons of another level.
        const Lattice lattice(6, 6, 3, seed, 1000);
        const Ring isle = island(500, 500, 200, 10, seed);
        const auto [half_a, half_b] = halves(isle, 5, pt(500, 500));
        std::vector<Polygon> polygons, minimal_only;
        std::vector<bool> in_minimal;
        auto add = [&](Polygon p, bool minimal) {
            polygons.push_back(p);
            in_minimal.push_back(minimal);
            if (minimal) minimal_only.push_back(p);
        };
        for (int r = 0; r < 6; r += 2)
            for (int c = 0; c < 6; c += 2) {
                Polygon block{lattice.block(r, c, 2, 2), {}};
                if (r == 0 && c == 0) block.holes.push_back(isle);
                add(block, true);
            }
        for (int r = 0; r < 6; r++)
            for (int c = 0; c < 6; c++) {
                Polygon cell{lattice.block(r, c, 1, 1), {}};
                if (r == 0 && c == 0) cell.holes.push_back(isle);
                add(cell, false);
            }
        add({isle, {}}, true);
        add({half_a, {}}, false);
        add({half_b, {}}, false);

        const AdminExactRings store = store_of(polygons), alone = store_of(minimal_only);
        const AdminArcs arcs = full_and_minimal(store, in_minimal, 3);
        const AdminArcs expected = build_arc_topology(alone, 1);
        const MinimalArcs& m = arcs.minimal;
        CHECK(m.counts.joined_junctions > 0);
        REQUIRE(m.arcs.size() == expected.arcs.size());
        for (uint32_t a = 0; a < m.arcs.size(); a++) {
            CHECK(minimal_vertices(store, arcs, a) == vertices_of(alone, expected.arcs[a]));
            CHECK_EQ(m.arcs[a].key, expected.arcs[a].key);
            CHECK_EQ(m.arcs[a].closed, expected.arcs[a].view.closed);
            CHECK(m.arcs[a].box == expected.arcs[a].box);
        }
        // The minimal rings' refs are the lone topology's, ring for ring.
        uint32_t alone_ring = 0;
        for (size_t p = 0; p < polygons.size(); p++) {
            if (!in_minimal[p]) continue;
            for (uint32_t k = 0; k < store.polygons[p].ring_count; k++, alone_ring++) {
                const uint32_t r = store.polygons[p].first_ring + k;
                const std::vector<ArcRef> got(m.refs.begin() + m.ring_refs[r], m.refs.begin() + m.ring_refs[r + 1]);
                const std::vector<ArcRef> want(expected.refs.begin() + expected.ring_refs[alone_ring],
                                               expected.refs.begin() + expected.ring_refs[alone_ring + 1]);
                CHECK(got == want);
            }
        }
        CHECK(every_ring_rebuilds(arcs, store, ArcSet::Minimal));
        // The same on one thread.
        const AdminArcs one = full_and_minimal(store, in_minimal, 1);
        CHECK(one.minimal.refs == m.refs && one.minimal.parts == m.parts && one.minimal.part_at == m.part_at);
    }
}

// --- Tiers ---

namespace {

std::vector<StoredVertex> stored_of(const AdminArcs& arcs, const AdminExactRings& store, const ArcTier& tier,
                                    const ArcTierState& state, uint32_t a) {
    std::vector<StoredVertex> out;
    tier_vertices(arcs, store, tier, state, a, out);
    return out;
}

Ring points_of(const std::vector<StoredVertex>& stored) {
    Ring out;
    for (const StoredVertex& v : stored) out.push_back(v.at);
    return out;
}

ArcTierState tier_of(const AdminArcs& arcs, const AdminExactRings& store, const ArcTier& tier, unsigned threads = 1) {
    return build_arc_tier(arcs, store, tier, threads);
}

// The full set's arc holding vertex p, or none.
uint32_t arc_through(const AdminArcs& arcs, const AdminExactRings& store, PackedLocation p) {
    for (uint32_t a = 0; a < arcs.arcs.size(); a++) {
        const Ring v = vertices_of(store, arcs.arcs[a]);
        if (std::find(v.begin(), v.end(), p) != v.end()) return a;
    }
    return UINT32_MAX;
}

int32_t floordiv_snap(int32_t c) {
    int64_t x = int64_t(c) + 50, q = x / 100;
    if (x % 100 != 0 && x < 0) q--;
    return int32_t(q * 100);
}

const ArcTier& simplified_tier() { return kArcTiers[0]; }

// Two squares of side `side` e7 units at latitude `lat`, either side of the
// border along longitude 0, which bows out to `bow` at its middle; the right
// one only `width` wide.
AdminExactRings squares_either_side(int32_t lat, int32_t side, int32_t bow, int32_t width) {
    const PackedLocation low = pt(lat, 0), mid = pt(lat + side / 2, bow), high = pt(lat + side, 0);
    return store_of({
        {closed({pt(lat, -side), low, mid, high, pt(lat + side, -side)}), {}},
        {closed({low, pt(lat, width), pt(lat + side, width), high, mid}), {}},
    });
}

}  // namespace

TEST(the_simplified_tolerance_is_37_5_m_at_the_first_vertex_capped_by_the_smallest_ring) {
    const int32_t lat = 450000000, side = 100000;
    const double epsilon_e7 = meters_to_degrees(kArcSimplifyToleranceM, lat / 1e7) * 1e7;
    for (int32_t bow : {int32_t(epsilon_e7) - 5, int32_t(epsilon_e7) + 6}) {
        const AdminExactRings store = squares_either_side(lat, side, bow, side);
        const AdminArcs arcs = build_arc_topology(store, 1);
        const ArcTierState state = tier_of(arcs, store, simplified_tier());
        const uint32_t border = arc_through(arcs, store, pt(lat + side / 2, bow));
        REQUIRE(border != UINT32_MAX);
        CHECK_EQ(stored_of(arcs, store, simplified_tier(), state, border).size(), size_t(bow > epsilon_e7 ? 3 : 2));
    }
    // Beside a strip 2000 wide the cap is 0.1 * sqrt of the strip's area,
    // under the tolerance: a bow of 1600 stays that a square's border loses.
    for (int32_t width : {side, 2000}) {
        const AdminExactRings store = squares_either_side(lat, side, 1600, width);
        const AdminArcs arcs = build_arc_topology(store, 1);
        const ArcTierState state = tier_of(arcs, store, simplified_tier());
        const RingSpan strip = store.rings[1];
        const __int128 a2 = exact_ring_area2(store.vertices.data() + strip.begin, strip.count);
        const double cap_e7 = kArcCapFactor * std::sqrt(double(a2 < 0 ? -a2 : a2) * 0.5e-14) * 1e7;
        const bool capped = cap_e7 < 1600;
        CHECK_EQ(capped, width == 2000);
        const uint32_t border = arc_through(arcs, store, pt(lat + side / 2, 1600));
        CHECK_EQ(stored_of(arcs, store, simplified_tier(), state, border).size(), size_t(capped ? 3 : 2));
    }
}

TEST(an_arc_whose_ends_meet_keeps_four_vertices) {
    // A sliver island: its middle vertices lie 300 and 250 off the line
    // between its tips, inside the tolerance and the cap, so Douglas-Peucker
    // alone keeps the tips; the bisection keeps the farther middle one.
    const Ring sliver = closed({pt(0, 0), pt(300, 50000), pt(0, 100000), pt(-250, 50000)});
    const AdminExactRings store = store_of({{sliver, {}}});
    const AdminArcs arcs = build_arc_topology(store, 1);
    const ArcTierState state = tier_of(arcs, store, simplified_tier());
    REQUIRE(arcs.arcs.size() == 1u);
    const std::vector<StoredVertex> v = stored_of(arcs, store, simplified_tier(), state, 0);
    CHECK(points_of(v) == Ring({pt(0, 0), pt(300, 50000), pt(0, 100000), pt(0, 0)}));
    CHECK_EQ(state.counts.degenerate_rings, size_t(0));
}

TEST(the_simplified_tier_snaps_to_the_1_1_m_grid_and_drops_repeats) {
    // A zigzag far out of the tolerance, so every vertex stays, at
    // latitudes about the grid's half steps, and two that snap together.
    const Ring zigzag = {pt(-151, 0), pt(100049, 10000), pt(-149, 20000), pt(100050, 30000), pt(100070, 30010),
                         pt(-150, 40000), pt(100051, 50000)};
    Ring above = zigzag, below = zigzag;
    above.insert(above.end(), {pt(200000, 50000), pt(200000, 0)});
    below.insert(below.end(), {pt(-300000, 50000), pt(-300000, 0)});
    const AdminExactRings store = store_of({{closed(above), {}}, {closed(below), {}}});
    const AdminArcs arcs = build_arc_topology(store, 1);
    const ArcTierState state = tier_of(arcs, store, simplified_tier());
    const uint32_t a = arc_through(arcs, store, pt(100049, 10000));
    REQUIRE(a != UINT32_MAX);
    Ring raw = vertices_of(store, arcs.arcs[a]);
    if (raw.front() != zigzag.front()) std::reverse(raw.begin(), raw.end());
    REQUIRE(raw == zigzag);
    Ring got = points_of(stored_of(arcs, store, simplified_tier(), state, a));
    if (got.front() != pt(-200, 0)) std::reverse(got.begin(), got.end());
    CHECK(got == Ring({pt(-200, 0), pt(100000, 10000), pt(-100, 20000), pt(100100, 30000), pt(-100, 40000),
                       pt(100100, 50000)}));
    for (const PackedLocation& p : got) CHECK(p.lat_e7 == floordiv_snap(p.lat_e7) && p.lon_e7 == floordiv_snap(p.lon_e7));
}

TEST(simplified_vertices_are_raw_vertices_of_their_arc_snapped) {
    std::vector<Polygon> cells = grid(6, 12, 51, 4000);
    for (Polygon& p : cells)
        for (PackedLocation& v : p.outer) v = pt(v.lat_e7 - 12345, v.lon_e7 - 6789);
    const AdminExactRings store = store_of(cells);
    const AdminArcs arcs = build_arc_topology(store, 1);
    const ArcTierState state = tier_of(arcs, store, simplified_tier());
    size_t dropped = 0;
    for (uint32_t a = 0; a < arcs.arcs.size(); a++) {
        const Ring raw = vertices_of(store, arcs.arcs[a]);
        const std::vector<StoredVertex> v = stored_of(arcs, store, simplified_tier(), state, a);
        REQUIRE(!v.empty());
        CHECK_EQ(v.front().raw, 0u);
        if (v.size() > 1) CHECK_EQ(v.back().raw, uint32_t(raw.size() - 1));
        for (size_t k = 0; k < v.size(); k++) {
            if (k > 0) CHECK(v[k].raw > v[k - 1].raw && v[k].at != v[k - 1].at);
            CHECK(v[k].at == pt(floordiv_snap(raw[v[k].raw].lat_e7), floordiv_snap(raw[v[k].raw].lon_e7)));
        }
        dropped += raw.size() - v.size();
    }
    CHECK(dropped > 0);
}

TEST(the_exact_tier_stores_every_raw_vertex_as_it_is) {
    std::vector<Polygon> cells = grid(5, 6, 61, 1000);
    for (Polygon& p : cells)
        for (PackedLocation& v : p.outer) v = pt(v.lat_e7 - 2501, v.lon_e7 - 2499);
    // A coordinate twice in a row, and a ring the simplified tier snaps to a point.
    cells.push_back({closed({pt(-90000, -90000), pt(-90000, -80000), pt(-90000, -80000), pt(-80000, -80000)}), {}});
    cells.push_back({closed({pt(70000, 70000), pt(70010, 70000), pt(70000, 70010)}), {}});
    const AdminExactRings store = store_of(cells);
    const AdminArcs arcs = build_admin_arcs(store, std::vector<bool>(cells.size(), true), "", 2);
    const ArcTier& exact = kArcTiers[1];
    REQUIRE(!exact.simplifies());
    const ArcTierState& state = arcs.tiers[1];
    CHECK(state.kept.empty());
    size_t vertices = 0;
    for (uint32_t a = 0; a < arcs.arcs.size(); a++) {
        const std::vector<StoredVertex> v = stored_of(arcs, store, exact, state, a);
        CHECK(points_of(v) == vertices_of(store, arcs.arcs[a]));
        for (uint32_t k = 0; k < v.size(); k++) CHECK_EQ(v[k].raw, k);
        vertices += v.size();
    }
    CHECK_EQ(state.counts.stored_vertices, vertices);
    CHECK_EQ(state.counts.stored_vertices, arcs.counts.arc_vertices);
    CHECK_EQ(state.counts.single_point_arcs, size_t(1));
    CHECK_EQ(state.counts.degenerate_rings + state.counts.validity_restored, size_t(0));
    // The simplified tier has the one-point ring as degenerate.
    CHECK_EQ(arcs.tiers[0].counts.degenerate_rings, size_t(1));
}

TEST(the_minimal_tier_simplifies_across_a_junction_it_joins) {
    // The border of two minimal squares runs straight through (5, 0), where
    // a triangle of another level touches it: the full set keeps it as an
    // arc end, the minimal set drops it.
    const int32_t s = 100000;
    const Ring left = closed({pt(0, -s), pt(0, 0), pt(s / 2, 0), pt(s, 0), pt(s, -s)});
    const Ring right = closed({pt(0, 0), pt(0, s), pt(s, s), pt(s, 0), pt(s / 2, 0)});
    const Ring triangle = closed({pt(s / 2, 0), pt(s / 2 - 20000, -30000), pt(s / 2 + 20000, -30000)});
    const AdminExactRings store = store_of({{left, {}}, {right, {}}, {triangle, {}}});
    const AdminArcs arcs = build_admin_arcs(store, {true, true, false}, "", 1);
    std::vector<PackedLocation> full_border, minimal_border;
    for (uint32_t a = 0; a < arcs.arcs.size(); a++) {
        const Ring v = points_of(stored_of(arcs, store, kArcTiers[0], arcs.tiers[0], a));
        if (std::find(v.begin(), v.end(), pt(s / 2, 0)) != v.end() && v.size() == 2) full_border.push_back(v.front());
    }
    CHECK_EQ(full_border.size(), size_t(2));
    bool found = false;
    for (uint32_t a = 0; a < arcs.minimal.arcs.size(); a++) {
        const Ring raw = minimal_vertices(store, arcs, a);
        if (std::find(raw.begin(), raw.end(), pt(s / 2, 0)) == raw.end()) continue;
        found = true;
        CHECK(points_of(stored_of(arcs, store, kArcTiers[2], arcs.tiers[2], a)) == Ring({pt(0, 0), pt(s, 0)}));
    }
    CHECK(found);
}

// --- Validity ---

TEST(a_ring_simplified_to_no_area_gets_its_most_straying_vertex_back) {
    // A sliver between two squares, its sides bowing 80 off the line between
    // its ends, where three polygons meet: both sides simplify to that line.
    const int32_t s = 100000;
    const PackedLocation j1 = pt(0, 0), j2 = pt(0, s);
    const Ring upper = {j1, pt(50, 25000), pt(80, 50000), pt(50, 75000), j2};
    const Ring lower = {j1, pt(-50, 25000), pt(-80, 50000), pt(-50, 75000), j2};
    Ring above = upper, below = lower, sliver = upper;
    above.insert(above.end(), {pt(s, s), pt(s, 0)});
    below.insert(below.end(), {pt(-s, s), pt(-s, 0)});
    std::reverse(below.begin(), below.end());
    sliver.insert(sliver.end(), lower.rbegin() + 1, lower.rend() - 1);
    const AdminExactRings store = store_of({{closed(above), {}}, {closed(below), {}}, {closed(sliver), {}}});
    const AdminArcs arcs = build_arc_topology(store, 1);
    CHECK_EQ(arcs.counts.junctions, size_t(2));
    const ArcTierState state = tier_of(arcs, store, simplified_tier());
    CHECK_EQ(state.counts.validity_rounds, 1);
    CHECK_EQ(state.counts.validity_restored, size_t(1));
    CHECK_EQ(state.counts.degenerate_rings, size_t(0));
    // The upper side's middle, first of the two equally far.
    size_t middles = 0;
    for (uint32_t a = 0; a < arcs.arcs.size(); a++) {
        const Ring v = points_of(stored_of(arcs, store, simplified_tier(), state, a));
        middles += std::count(v.begin(), v.end(), pt(100, 50000)) + std::count(v.begin(), v.end(), pt(-100, 50000));
    }
    CHECK_EQ(middles, size_t(1));
}

// --- The stage's dirs ---

TEST(the_arc_dirs_are_refused_before_the_build_starts) {
    const ScratchDir root("admin-arc-dirs");
    const std::string out = root.path() + "/out", arcs = root.path() + "/arcs", prev = root.path() + "/prev";
    auto refusal = [&](const std::string& o, const std::string& a, const std::string& p) {
        return refusal_of([&] { require_arc_dirs(o, a, p); });
    };
    CHECK_EQ(refusal(out, "", ""), std::string());
    CHECK_EQ(refusal(out, arcs, ""), std::string());
    CHECK(refusal(out, "", prev).find("without --arcs-out") != std::string::npos);
    CHECK(refusal(out, out, "").find("overlaps") != std::string::npos);
    CHECK(refusal(out, out + "/arcs", "").find("overlaps") != std::string::npos);
    CHECK(refusal(arcs + "/out", arcs, "").find("overlaps") != std::string::npos);
    CHECK(refusal(out, root.path() + "/./out/", "").find("overlaps") != std::string::npos);
    CHECK(refusal(out, root.path() + "/outside", "").empty());
    std::filesystem::create_directories(arcs);
    std::ofstream(arcs + "/left") << "x";
    CHECK(refusal(out, arcs, "").find("not empty") != std::string::npos);
    CHECK(refusal(out, root.path() + "/arcs2", prev).find("no planet arc sidecar") != std::string::npos);
    const std::string sidecar = arc_sidecar_path(prev + "/planet", ArcSet::Full);
    CHECK_EQ(sidecar, prev + "/planet/quality/exact/arc_index.osm_ids");
    CHECK_EQ(arc_sidecar_path(prev + "/planet", ArcSet::Minimal), prev + "/planet/admin-minimal/arc_index.osm_ids");
    std::filesystem::create_directories(prev + "/planet/quality/exact");
    gc::id_alloc::IdAllocator::write_sidecar(sidecar, {});
    // The minimal set's too.
    CHECK(refusal(out, root.path() + "/arcs2", prev).find("admin-minimal/arc_index.osm_ids") != std::string::npos);
    std::filesystem::create_directories(prev + "/planet/admin-minimal");
    gc::id_alloc::IdAllocator::write_sidecar(arc_sidecar_path(prev + "/planet", ArcSet::Minimal), {});
    CHECK_EQ(refusal(out, root.path() + "/arcs2", prev), std::string());
}

// --- Polygons in lockstep ---

namespace {

// Polygons named 0..n-1, each of a ring of its own (and a hole every third),
// their arcs built: the names run backwards in polygon order, so sorting
// moves every one.
ParsedData named_polygons_with_arcs(int n) {
    ParsedData d;
    for (int i = 0; i < n; i++) {
        AdminPolygon p{};
        p.name_id = static_cast<uint32_t>(n - 1 - i);
        d.admin_polygons.push_back(p);
        d.admin_osm_ids.push_back(1000 + p.name_id);
        const int32_t at = 100 * static_cast<int32_t>(p.name_id);
        std::vector<Ring> holes;
        if (i % 3 == 0) holes.push_back(closed({pt(at + 2, 2), pt(at + 2, 4), pt(at + 4, 3)}));
        d.admin_exact_rings.append(closed({pt(at, 0), pt(at, 10), pt(at + 10, 10), pt(at + 10, 0)}), holes);
    }
    d.admin_arcs = build_admin_arcs(d.admin_exact_rings, std::vector<bool>(n, false), "", 1);
    return d;
}

// Whether each polygon's arc rings rebuild to its own rings (its name sets
// its latitude).
bool arc_rings_follow_the_polygons(const ParsedData& d) {
    if (d.admin_arcs.polygons.size() != d.admin_polygons.size()) return false;
    for (size_t k = 0; k < d.admin_polygons.size(); k++) {
        const AdminArcPolygon& p = d.admin_arcs.polygons[k];
        if (p.ring_count == 0) return false;
        const std::vector<PackedLocation> outer = rebuild_ring(d.admin_arcs, d.admin_exact_rings, ArcSet::Full, p.first_ring);
        int32_t lowest = INT32_MAX;
        for (const PackedLocation& v : outer) lowest = std::min(lowest, v.lat_e7);
        if (lowest != 100 * static_cast<int32_t>(d.admin_polygons[k].name_id)) return false;
        if (!(p == AdminArcPolygon{d.admin_exact_rings.polygons[k].first_ring, d.admin_exact_rings.polygons[k].ring_count}))
            return false;
    }
    return true;
}

}  // namespace

TEST(each_polygons_arc_rings_follow_it_through_the_admin_permutes) {
    for (unsigned threads : {1u, 3u}) {
        ParsedData d = named_polygons_with_arcs(9);
        CHECK(arc_rings_follow_the_polygons(d));
        permute_admin_polygons(d, {4, 0, 8, 1, 7, 2, 6, 3, 5}, threads);
        CHECK(arc_rings_follow_the_polygons(d));
        reorder_admin_polygons(d, threads);
        CHECK_EQ(d.admin_polygons[0].name_id, 0u);
        CHECK(arc_rings_follow_the_polygons(d));
    }
    // Out of step is refused before anything moves.
    ParsedData d = named_polygons_with_arcs(3);
    d.admin_arcs.polygons.pop_back();
    const std::vector<uint64_t> osm_ids = d.admin_osm_ids;
    CHECK(refusal_of<std::logic_error>([&] { permute_admin_polygons(d, {2, 1, 0}, 1); }).find("out of step") !=
          std::string::npos);
    CHECK(d.admin_osm_ids == osm_ids);
}
