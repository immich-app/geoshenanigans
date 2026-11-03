// admin_arcs.h: the arc topology (junctions, cuts, canonical arcs, refs), the
// arcs' keys and Hilbert presentation order, their slots, the arc stage's dirs
// and its per-polygon rings following each admin permute. In the sanitized
// suite: the topology's scatter and partition passes index the store directly.
#include "admin_arcs.h"

#include <algorithm>
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

// Whether every ring of the store comes back from its refs: the same cycle,
// in the ring's own direction.
bool every_ring_rebuilds(const AdminArcs& arcs, const AdminExactRings& store) {
    for (uint32_t r = 0; r < store.rings.size(); r++) {
        const RingSpan& span = store.rings[r];
        const std::vector<PackedLocation> rebuilt = rebuild_ring(arcs, store, r);
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

// An n x n grid of square cells of side `side` e7 units, every cell a polygon
// whose sides each carry `inner` vertices between the corners, jittered off
// the straight line and shared with the cell across (the same coordinates,
// walked the other way). Rings run counterclockwise from each cell's lower
// left corner.
std::vector<Polygon> grid(int n, int inner, uint64_t seed, int32_t side = 1000) {
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<int32_t> jitter(-side / 8, side / 8);
    // The inner vertices of each edge, from its lower-left end: horizontal
    // edges by (row, column), vertical ones likewise.
    auto make = [&](int rows, int cols, bool horizontal) {
        std::vector<std::vector<Ring>> edges(rows, std::vector<Ring>(cols));
        for (int r = 0; r < rows; r++)
            for (int c = 0; c < cols; c++)
                for (int k = 1; k <= inner; k++) {
                    const int32_t along = k * side / (inner + 1), off = jitter(rng);
                    edges[r][c].push_back(horizontal ? pt(r * side + off, c * side + along)
                                                     : pt(r * side + along, c * side + off));
                }
        return edges;
    };
    const auto across = make(n + 1, n, true);   // the edge from (r, c) to (r, c + 1)
    const auto upward = make(n, n + 1, false);  // the edge from (r, c) to (r + 1, c)
    std::vector<Polygon> cells;
    for (int r = 0; r < n; r++)
        for (int c = 0; c < n; c++) {
            Ring ring{pt(r * side, c * side)};
            for (const auto& p : across[r][c]) ring.push_back(p);
            ring.push_back(pt(r * side, (c + 1) * side));
            for (const auto& p : upward[r][c + 1]) ring.push_back(p);
            ring.push_back(pt((r + 1) * side, (c + 1) * side));
            for (auto it = across[r + 1][c].rbegin(); it != across[r + 1][c].rend(); ++it) ring.push_back(*it);
            ring.push_back(pt((r + 1) * side, c * side));
            for (auto it = upward[r][c].rbegin(); it != upward[r][c].rend(); ++it) ring.push_back(*it);
            cells.push_back({closed(ring), {}});
        }
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
    const AdminArcs day_one = build_admin_arcs(store, "", 4);
    for (const Arc& a : day_one.arcs) CHECK(a.planet_slot < day_one.arcs.size());
    const std::string sidecar = arc_sidecar_path(prev.path() + "/planet", ArcSet::Full);
    std::filesystem::create_directories(std::filesystem::path(sidecar).parent_path());
    IdAllocator::write_sidecar(sidecar, day_one.planet_sidecar);

    // A junction moves: its arcs get new slots, not the ones they leave.
    const AdminExactRings edited = store_of(moved(cells, pt(2000, 2000), pt(2020, 1990)));
    const AdminArcs day_two = build_admin_arcs(edited, prev.path() + "/planet", 4);
    std::map<uint64_t, uint32_t> slot_of;
    for (const Arc& a : day_one.arcs) slot_of[a.key] = a.planet_slot;
    size_t kept = 0;
    for (const Arc& a : day_two.arcs) {
        auto it = slot_of.find(a.key);
        if (it != slot_of.end()) {
            CHECK_EQ(a.planet_slot, it->second);
            kept++;
        } else {
            CHECK(a.planet_slot >= day_one.arcs.size());
        }
    }
    CHECK_EQ(kept, day_one.arcs.size() - 4);
    CHECK_EQ(day_two.slot_counts.kept, kept);
    CHECK_EQ(day_two.slot_counts.appended, size_t(4));
    CHECK_EQ(day_two.slot_counts.tombstones, size_t(4));
    CHECK_EQ(day_two.planet_sidecar.size(), day_one.arcs.size() + 4);
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
    d.admin_arcs = build_admin_arcs(d.admin_exact_rings, "", 1);
    return d;
}

// Whether each polygon's arc rings rebuild to its own rings (its name sets
// its latitude).
bool arc_rings_follow_the_polygons(const ParsedData& d) {
    if (d.admin_arcs.polygons.size() != d.admin_polygons.size()) return false;
    for (size_t k = 0; k < d.admin_polygons.size(); k++) {
        const AdminArcPolygon& p = d.admin_arcs.polygons[k];
        if (p.ring_count == 0) return false;
        const std::vector<PackedLocation> outer = rebuild_ring(d.admin_arcs, d.admin_exact_rings, p.first_ring);
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
