// admin-minimal on the 11 m grid (kMinimalCoarseGridE7, admin_arcs.h's
// arc_grid): its arcs stored at k = 3, every junction on that grid in every
// arc that ends there; a ring valid on the 1.1 m grid that the 11 m grid
// would collapse keeping its arcs and its junctions fine, every arc that ends
// at one of those junctions fine too, round after round; the repair on the
// 11 m grid, and the arcs of the crossings and polygons not nesting it leaves,
// and of the rings it collapses again or folds, put back as the 1.1 m tier
// has them; and the two checks the arc stage aborts on. S2-free, in the
// sanitized suite.
#include <algorithm>
#include <cmath>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "admin_arcs.h"
#include "exact_pip.h"
#include "parsed_data.h"
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

// floordiv(c + step / 2, step) * step, worked out here rather than taken
// from admin_arcs.h's snap.
int32_t on_grid(int32_t c, int32_t step) {
    int64_t q = (int64_t(c) + step / 2) / step;
    if ((int64_t(c) + step / 2) % step != 0 && int64_t(c) + step / 2 < 0) q--;
    return int32_t(q * step);
}

PackedLocation on_grid(PackedLocation p, int32_t step) { return {on_grid(p.lat_e7, step), on_grid(p.lon_e7, step)}; }

constexpr int32_t kFine = 100, kCoarse = 1000;

size_t minimal_tier() {
    for (size_t t = 0; t < kArcTierCount; t++)
        if (kArcTiers[t].set == ArcSet::Minimal) return t;
    return kArcTierCount;
}

size_t full_simplified_tier() {
    for (size_t t = 0; t < kArcTierCount; t++)
        if (kArcTiers[t].set == ArcSet::Full && kArcTiers[t].simplifies()) return t;
    return kArcTierCount;
}

struct Built {
    AdminExactRings store;
    AdminArcs arcs;
    AdminArcBlocks blocks;
};

// The arc stage over every polygon of `polygons`, all of them minimal unless
// `in_minimal` says otherwise.
Built built(const std::vector<Polygon>& polygons, std::vector<bool> in_minimal = {}, unsigned threads = 1) {
    if (in_minimal.empty()) in_minimal.assign(polygons.size(), true);
    Built b;
    b.store = store_of(polygons);
    b.arcs = build_admin_arcs(b.store, in_minimal, "", threads);
    b.blocks = encode_admin_arcs(b.arcs, b.store, threads);
    return b;
}

Ring minimal_raw(const Built& b, uint32_t a) {
    const MinimalArcVertices v(b.store, b.arcs, a);
    Ring out;
    for (uint32_t i = 0; i < v.size(); i++) out.push_back(v[i]);
    return out;
}

// The minimal arc whose raw vertices run from `from` to `to`, either way;
// UINT32_MAX for none.
uint32_t minimal_arc(const Built& b, PackedLocation from, PackedLocation to) {
    for (uint32_t a = 0; a < b.arcs.minimal.arcs.size(); a++) {
        const Ring v = minimal_raw(b, a);
        if ((v.front() == from && v.back() == to) || (v.front() == to && v.back() == from)) return a;
    }
    return UINT32_MAX;
}

// The minimal arc holding raw vertex p; UINT32_MAX for none.
uint32_t minimal_arc_through(const Built& b, PackedLocation p) {
    for (uint32_t a = 0; a < b.arcs.minimal.arcs.size(); a++) {
        const Ring v = minimal_raw(b, a);
        if (std::find(v.begin(), v.end(), p) != v.end()) return a;
    }
    return UINT32_MAX;
}

arc_blocks::DecodedArc block_of(const AdminArcBlocks& blocks, size_t t, uint32_t a) {
    arc_blocks::DecodedArc d;
    REQUIRE(arc_blocks::decode_arc(blocks.tiers[t].data(a), blocks.tiers[t].bytes_of(a), d));
    return d;
}

// Polygon p's rings in `set` as tier t's blocks hold them: each ref's arc
// decoded and walked as the ref says. `chained` turns false when an arc
// doesn't start where the one before it ends, or a ring doesn't end where it
// starts.
std::vector<Ring> decoded_rings(const AdminArcBlocks& blocks, ArcSet set, size_t t, uint32_t p, bool& chained) {
    const ArcSetBlocks& s = blocks.set(set);
    const BlockSpan span = blocks.polygons[p].blocks[static_cast<size_t>(set)];
    std::vector<std::vector<arc_blocks::SlotRef>> refs;
    REQUIRE(arc_blocks::decode_rings(s.ring_blocks.data() + span.offset, span.size, refs));
    const std::vector<uint32_t> arc_of = arc_at_slot(s);
    std::vector<Ring> rings;
    for (const auto& ring : refs) {
        Ring out;
        for (const arc_blocks::SlotRef& ref : ring) {
            arc_blocks::DecodedArc arc = block_of(blocks, t, arc_of.at(ref.slot));
            if (ref.reversed) std::reverse(arc.vertices.begin(), arc.vertices.end());
            if (!out.empty() && out.back() != arc.vertices.front()) chained = false;
            out.insert(out.end(), arc.vertices.begin() + (out.empty() ? 0 : 1), arc.vertices.end());
        }
        if (out.empty() || out.front() != out.back()) chained = false;
        rings.push_back(out);
    }
    return rings;
}

// Whether a closed ring (its first vertex again at its end) is valid: 3
// vertices or more, repeats in a row once, and an area.
bool valid(Ring ring) {
    ring.erase(std::unique(ring.begin(), ring.end()), ring.end());
    if (ring.size() >= 2 && ring.front() == ring.back()) ring.pop_back();
    return ring.size() >= 3 && exact_ring_area2(ring.data(), static_cast<uint32_t>(ring.size())) != 0;
}

// Which way a closed ring winds: the sign of its exact area.
int winding_of(const Ring& ring) {
    const __int128 a2 = exact_ring_area2(ring.data(), static_cast<uint32_t>(ring.size()));
    return (a2 > 0) - (a2 < 0);
}

Ring snapped_ring(const Ring& raw, int32_t step) {
    Ring out;
    for (const PackedLocation& p : raw) out.push_back(on_grid(p, step));
    return out;
}

bool same_ring(const Ring& a, const Ring& b) {
    return same_closed_ring(a.data(), static_cast<uint32_t>(a.size()), b.data(), static_cast<uint32_t>(b.size()));
}

// The blocks with tier t's block of arc `a` holding `vertices` at grid
// exponent k instead.
AdminArcBlocks with_block(const AdminArcBlocks& from, size_t t, uint32_t a, const Ring& vertices, uint8_t k) {
    AdminArcBlocks out = from;
    const ArcBlocks& old = from.tiers[t];
    ArcBlocks& blocks = out.tiers[t];
    blocks = {};
    blocks.at.push_back(0);
    for (size_t i = 0; i < old.size(); i++) {
        if (i == a) {
            auto at = [&](size_t j) { return vertices[j]; };
            const size_t n = arc_blocks::encode_arc(vertices.size(), at, k, nullptr);
            blocks.bytes.resize(blocks.bytes.size() + n);
            arc_blocks::encode_arc(vertices.size(), at, k, blocks.bytes.data() + blocks.bytes.size() - n);
        } else {
            blocks.bytes.insert(blocks.bytes.end(), old.data(i), old.data(i) + old.bytes_of(i));
        }
        blocks.at.push_back(static_cast<uint32_t>(blocks.bytes.size()));
    }
    return out;
}

using Pairs = std::vector<std::pair<PackedLocation, PackedLocation>>;

// The full arc holding raw vertex p; UINT32_MAX for none.
uint32_t full_arc_through(const Built& b, PackedLocation p) {
    for (uint32_t a = 0; a < b.arcs.arcs.size(); a++) {
        const ArcVertices v = arc_vertices(b.store, b.arcs.arcs[a].view);
        for (uint32_t i = 0; i < v.size(); i++)
            if (v[i] == p) return a;
    }
    return UINT32_MAX;
}

// The e7 triangle (100, 100), (400, 100), (100, 400), the cell of a square
// three spokes from its corners cut into three more (west, north-east and
// south), so that another border meets it at each corner: all three corners
// snap to (0, 0) on the 11 m grid, and no vertex between them is left to
// give back. The square's corners sit off both grids.
struct Triangle {
    PackedLocation c1 = pt(100, 100), c2 = pt(400, 100), c3 = pt(100, 400);
    PackedLocation sw = pt(-99637, -100449), nw = pt(100363, -100449), ne = pt(100363, 99551), se = pt(-99637, 99551);
    std::vector<Polygon> polygons() const {
        return {
            {closed({c1, c2, c3}), {}},
            {closed({c1, sw, nw, c2}), {}},
            {closed({c2, nw, ne, se, c3}), {}},
            {closed({c3, se, sw, c1}), {}},
        };
    }
};

}  // namespace

TEST(minimal_arcs_are_stored_at_k_3_on_the_11_m_grid) {
    // A 3 x 3 lattice of cells 0.02 degrees wide, off both grids, each edge
    // zigzagging 2 km either side of the line, so every vertex stays.
    const int n = 3, side = 200000, bow = 20000;
    auto corner = [&](int r, int c) { return pt(r * side + 37 + 113 * c, c * side - 263 + 71 * r); };
    // An edge from a to b, its middle moved off the line across it: along
    // latitude for an edge along longitude (`across`), else along longitude.
    auto edge = [&](PackedLocation a, PackedLocation b, int k, bool across) {
        const int32_t off = k % 2 ? bow : -bow;
        const PackedLocation mid = pt((a.lat_e7 + b.lat_e7) / 2 + (across ? off : 0) + 11,
                                      (a.lon_e7 + b.lon_e7) / 2 + (across ? 0 : off) - 29);
        return Ring{a, mid, b};
    };
    std::vector<Polygon> cells;
    for (int r = 0; r < n; r++)
        for (int c = 0; c < n; c++) {
            const Ring south = edge(corner(r, c), corner(r, c + 1), r + 2 * c, true);
            const Ring east = edge(corner(r, c + 1), corner(r + 1, c + 1), 3 * r + c + 1, false);
            const Ring north = edge(corner(r + 1, c), corner(r + 1, c + 1), r + 1 + 2 * c, true);
            const Ring west = edge(corner(r, c), corner(r + 1, c), 3 * r + c, false);
            Ring ring = {south[0], south[1], east[0], east[1], north[2], north[1], west[2], west[1]};
            cells.push_back({closed(ring), {}});
        }
    const Built b = built(cells);
    const size_t m = minimal_tier(), f = full_simplified_tier();
    const ArcTierState& state = b.arcs.tiers[m];
    REQUIRE(b.arcs.minimal.arcs.size() > 0);
    for (uint32_t a = 0; a < b.arcs.minimal.arcs.size(); a++) {
        const arc_blocks::DecodedArc d = block_of(b.blocks, m, a);
        CHECK_EQ(int(d.k), 3);
        // Every raw vertex of the arc stays, in order, on the 11 m grid.
        CHECK(d.vertices == snapped_ring(minimal_raw(b, a), kCoarse));
    }
    for (uint32_t a = 0; a < b.arcs.arcs.size(); a++) CHECK_EQ(int(block_of(b.blocks, f, a).k), 2);
    CHECK_EQ(state.counts.fine_rings, size_t(0));
    CHECK(state.fine_junctions.empty());
    CHECK_EQ(state.counts.fine_arcs, size_t(0));
    CHECK_EQ(state.counts.degenerate_rings, size_t(0));
}

TEST(a_tiny_islet_keeps_k_2_and_still_holds_its_interior_point) {
    // The islet's three corners snap to one point on the 11 m grid, not on
    // the 1.1 m one; a speck beside it is degenerate on both, so it stays as
    // the 1.1 m tier has it: on the 11 m grid, degenerate.
    const Ring big = closed({pt(37, 41), pt(37, 200041), pt(200037, 200041), pt(200037, 41)});
    const Ring islet = closed({pt(500100, 500100), pt(500100, 500400), pt(500400, 500100)});
    const Ring speck = closed({pt(700010, 700010), pt(700020, 700030), pt(700030, 700010)});
    const Built b = built({{big, {}}, {islet, {}}, {speck, {}}});
    const size_t m = minimal_tier();
    const ArcTierState& state = b.arcs.tiers[m];
    const uint32_t big_arc = minimal_arc_through(b, big[1]), islet_arc = minimal_arc_through(b, islet[1]),
                   speck_arc = minimal_arc_through(b, speck[1]);
    REQUIRE(big_arc != UINT32_MAX && islet_arc != UINT32_MAX && speck_arc != UINT32_MAX);
    CHECK_EQ(int(block_of(b.blocks, m, big_arc).k), 3);
    CHECK_EQ(int(block_of(b.blocks, m, speck_arc).k), 3);
    const arc_blocks::DecodedArc d = block_of(b.blocks, m, islet_arc);
    CHECK_EQ(int(d.k), 2);
    CHECK(same_ring(d.vertices, islet));
    bool chained = true;
    const std::vector<Ring> rings = decoded_rings(b.blocks, ArcSet::Minimal, m, 1, chained);
    REQUIRE(rings.size() == 1);
    CHECK(chained);
    CHECK(exact_ring_contains(rings[0].data(), static_cast<uint32_t>(rings[0].size()), pt(500200, 500200)));
    CHECK_EQ(state.counts.fine_rings, size_t(1));
    CHECK_EQ(state.counts.fine_degenerate_rings, size_t(1));
    CHECK_EQ(state.counts.degenerate_rings, size_t(1));
    CHECK_EQ(state.counts.fine_arcs, size_t(1));
    CHECK(state.fine_junctions.empty());  // a closed arc has none
}

TEST(an_all_junction_ring_keeps_its_junctions_fine_and_every_arc_that_ends_there_at_k_2) {
    const Triangle tri;
    const Built b = built(tri.polygons());
    const size_t m = minimal_tier(), f = full_simplified_tier();
    const ArcTierState& state = b.arcs.tiers[m];
    std::vector<uint64_t> corners = {vertex_key(tri.c1), vertex_key(tri.c2), vertex_key(tri.c3)};
    std::sort(corners.begin(), corners.end());
    CHECK(state.fine_junctions == corners);
    CHECK_EQ(state.counts.fine_rings, size_t(1));
    CHECK_EQ(state.counts.fine_junctions, size_t(3));
    // Its three arcs, at k = 2 as they are; the spokes at k = 2, the corner
    // they end at fine, their square's corner on the 11 m grid; the rest at
    // k = 3.
    for (auto [from, to] : Pairs{{tri.c1, tri.c2}, {tri.c2, tri.c3}, {tri.c3, tri.c1}}) {
        const uint32_t a = minimal_arc(b, from, to);
        REQUIRE(a != UINT32_MAX);
        const arc_blocks::DecodedArc d = block_of(b.blocks, m, a);
        CHECK_EQ(int(d.k), 2);
        CHECK(d.vertices == minimal_raw(b, a));
    }
    for (auto [corner, far] : Pairs{{tri.c1, tri.sw}, {tri.c2, tri.nw}, {tri.c3, tri.se}}) {
        const uint32_t a = minimal_arc(b, corner, far);
        REQUIRE(a != UINT32_MAX);
        const arc_blocks::DecodedArc d = block_of(b.blocks, m, a);
        CHECK_EQ(int(d.k), 2);
        Ring want = {corner, on_grid(far, kCoarse)};
        if (minimal_raw(b, a).front() != corner) std::reverse(want.begin(), want.end());
        CHECK(d.vertices == want);
    }
    for (auto [from, to] : Pairs{{tri.sw, tri.nw}, {tri.nw, tri.se}, {tri.se, tri.sw}}) {
        const uint32_t a = minimal_arc(b, from, to);
        REQUIRE(a != UINT32_MAX);
        CHECK_EQ(int(block_of(b.blocks, m, a).k), 3);
    }
    CHECK_EQ(state.counts.fine_arcs, size_t(6));
    // Every ring's chain closes, and the triangle comes back as the 1.1 m
    // tier has it.
    for (uint32_t p = 0; p < 4; p++) {
        bool chained = true;
        const std::vector<Ring> rings = decoded_rings(b.blocks, ArcSet::Minimal, m, p, chained);
        REQUIRE(rings.size() == 1);
        CHECK(chained);
        CHECK(valid(rings[0]));
    }
    bool chained = true;
    const Ring got = decoded_rings(b.blocks, ArcSet::Minimal, m, 0, chained)[0];
    const Ring fine = decoded_rings(b.blocks, ArcSet::Full, f, 0, chained)[0];
    CHECK(chained);
    CHECK(same_ring(got, fine));
    CHECK(same_ring(got, closed({tri.c1, tri.c2, tri.c3})));
}

TEST(a_kept_fine_arc_and_a_coarse_arc_meet_at_a_bit_identical_junction) {
    const Triangle tri;
    const Built b = built(tri.polygons());
    const size_t m = minimal_tier();
    // At each outer corner a spoke kept fine meets two square sides at
    // k = 3: all three store it at the same point, the 11 m one, though the
    // 1.1 m grid puts it elsewhere.
    for (auto [kept_fine, corner] : Pairs{{tri.c1, tri.sw}, {tri.c2, tri.nw}, {tri.c3, tri.se}}) {
        CHECK(on_grid(corner, kFine) != on_grid(corner, kCoarse));
        const uint32_t spoke = minimal_arc(b, kept_fine, corner);
        REQUIRE(spoke != UINT32_MAX);
        const arc_blocks::DecodedArc fine = block_of(b.blocks, m, spoke);
        CHECK_EQ(int(fine.k), 2);
        const PackedLocation at = minimal_raw(b, spoke).front() == corner ? fine.vertices.front() : fine.vertices.back();
        CHECK(at == on_grid(corner, kCoarse));
        size_t sides = 0;
        for (uint32_t a = 0; a < b.arcs.minimal.arcs.size(); a++) {
            const Ring raw = minimal_raw(b, a);
            if (a == spoke || (raw.front() != corner && raw.back() != corner)) continue;
            const arc_blocks::DecodedArc d = block_of(b.blocks, m, a);
            CHECK_EQ(int(d.k), 3);
            sides++;
            const PackedLocation there = raw.front() == corner ? d.vertices.front() : d.vertices.back();
            CHECK(there.lat_e7 == at.lat_e7 && there.lon_e7 == at.lon_e7);
        }
        CHECK_EQ(sides, size_t(2));
    }
    // And at a corner kept fine, the triangle's arcs and the spoke store it
    // as it is.
    for (uint32_t a = 0; a < b.arcs.minimal.arcs.size(); a++) {
        const Ring raw = minimal_raw(b, a);
        const arc_blocks::DecodedArc d = block_of(b.blocks, m, a);
        if (raw.front() == tri.c1) CHECK(d.vertices.front() == tri.c1);
        if (raw.back() == tri.c1) CHECK(d.vertices.back() == tri.c1);
    }
}

TEST(a_chain_of_two_all_junction_rings_that_share_a_junction_converges) {
    // T1 = j, a, b snaps to one point on the 11 m grid. T2 = j, g1, g2 is
    // valid there, but once j is kept fine for T1 it lies on the line
    // through g1 and g2 as the 11 m grid has them, so it collapses then and
    // is kept fine in a second round. A square touches each other corner,
    // so that every corner is a junction.
    const PackedLocation j = pt(100, 100), a = pt(400, 100), bb = pt(100, -200), g1 = pt(1080, 1960), g2 = pt(10000, 21000);
    const std::vector<Polygon> polygons = {
        {closed({j, a, bb}), {}},
        {closed({j, g1, g2}), {}},
        {closed({a, pt(5400, 100), pt(5400, -4900), pt(400, -4900)}), {}},
        {closed({bb, pt(-4900, -200), pt(-4900, -5200), pt(100, -5200)}), {}},
        {closed({g1, pt(4080, 1960), pt(4080, 460), pt(1080, 460)}), {}},
        {closed({g2, pt(15000, 21000), pt(15000, 16000), pt(10000, 16000)}), {}},
    };
    // As the plan of the fixture says.
    CHECK(!valid(snapped_ring(polygons[0].outer, kCoarse)) && valid(snapped_ring(polygons[0].outer, kFine)));
    CHECK(valid(snapped_ring(polygons[1].outer, kCoarse)) && valid(snapped_ring(polygons[1].outer, kFine)));
    CHECK(!valid(closed({j, on_grid(g1, kCoarse), on_grid(g2, kCoarse)})));
    for (unsigned threads : {1u, 3u}) {
        const Built b = built(polygons, {}, threads);
        const size_t m = minimal_tier();
        const ArcTierState& state = b.arcs.tiers[m];
        CHECK_EQ(state.counts.fine_rings, size_t(2));
        CHECK_EQ(state.counts.coarse_rounds, 2);
        std::vector<uint64_t> corners = {vertex_key(j), vertex_key(a), vertex_key(bb), vertex_key(g1), vertex_key(g2)};
        std::sort(corners.begin(), corners.end());
        CHECK(state.fine_junctions == corners);
        for (uint32_t p = 0; p < polygons.size(); p++) {
            bool chained = true;
            const std::vector<Ring> rings = decoded_rings(b.blocks, ArcSet::Minimal, m, p, chained);
            REQUIRE(rings.size() == 1);
            CHECK(chained);
            CHECK(valid(rings[0]));
            if (p < 2) CHECK(same_ring(rings[0], snapped_ring(polygons[p].outer, kFine)));
        }
    }
}

TEST(the_loop_ends_on_random_fixtures_and_no_ring_valid_on_the_1_1_m_grid_comes_out_degenerate) {
    const size_t m = minimal_tier();
    // The minimal tier as it was before the 11 m grid: the same set and
    // tolerance on the 1.1 m grid alone.
    const ArcTier fine_tier = {ArcSet::Minimal, kArcTiers[m].dir, kArcTiers[m].tolerance_m, kArcTiers[m].cap_factor,
                               kFine, kFine};
    size_t fine_rings = 0, fine_arcs = 0, coarse_arcs = 0, rings_checked = 0;
    for (uint64_t seed = 1; seed <= 12; seed++) {
        std::mt19937_64 rng(seed);
        auto uniform = [&](int32_t lo, int32_t hi) { return std::uniform_int_distribution<int32_t>(lo, hi)(rng); };
        // A lattice of small cells, edges jittered, and islands, small ones
        // among them, some across the cells.
        const int n = uniform(3, 6);
        const int32_t side = uniform(1200, 6000);
        std::vector<std::vector<PackedLocation>> corners(n + 1, std::vector<PackedLocation>(n + 1));
        for (int r = 0; r <= n; r++)
            for (int c = 0; c <= n; c++)
                corners[r][c] = pt(r * side + uniform(-side / 5, side / 5), c * side + uniform(-side / 5, side / 5));
        auto between = [&](PackedLocation x, PackedLocation y) {
            return pt((x.lat_e7 + y.lat_e7) / 2 + uniform(-side / 8, side / 8), (x.lon_e7 + y.lon_e7) / 2 + uniform(-side / 8, side / 8));
        };
        std::vector<std::vector<PackedLocation>> across(n + 1, std::vector<PackedLocation>(n)), up(n, std::vector<PackedLocation>(n + 1));
        for (int r = 0; r <= n; r++)
            for (int c = 0; c < n; c++) across[r][c] = between(corners[r][c], corners[r][c + 1]);
        for (int r = 0; r < n; r++)
            for (int c = 0; c <= n; c++) up[r][c] = between(corners[r][c], corners[r + 1][c]);
        std::vector<Polygon> polygons;
        std::vector<bool> in_minimal;
        for (int r = 0; r < n; r++)
            for (int c = 0; c < n; c++) {
                polygons.push_back({closed({corners[r][c], across[r][c], corners[r][c + 1], up[r][c + 1], corners[r + 1][c + 1],
                                            across[r + 1][c], corners[r + 1][c], up[r][c]}),
                                    {}});
                in_minimal.push_back(rng() % 4 != 0);
            }
        for (int k = 0; k < 24; k++) {
            const int32_t radius = k % 2 ? uniform(80, 700) : uniform(120, 2500);
            const int32_t lat = uniform(-5000, n * side + 5000), lng = uniform(-5000, n * side + 5000);
            const int vertices = uniform(3, 9);
            Ring ring;
            for (int v = 0; v < vertices; v++) {
                const double angle = 2 * 3.14159265358979 * v / vertices;
                ring.push_back(pt(lat + int32_t(radius * std::sin(angle)) + uniform(-radius / 6, radius / 6),
                                  lng + int32_t(radius * std::cos(angle)) + uniform(-radius / 6, radius / 6)));
            }
            polygons.push_back({closed(ring), {}});
            in_minimal.push_back(rng() % 5 != 0);
        }
        const Built b = built(polygons, in_minimal, seed % 2 ? 1 : 3);
        const ArcTierState& state = b.arcs.tiers[m];
        const ArcTierState before = build_arc_tier(b.arcs, b.store, fine_tier, exact_rings_nest(b.store, 1), 1);
        REQUIRE(state.fine_valid.size() == b.store.rings.size());
        std::vector<PackedLocation> ring, raw;
        std::vector<StoredVertex> stored;
        for (uint32_t p = 0; p < polygons.size(); p++) {
            if (!in_minimal[p]) continue;
            bool chained = true;
            const std::vector<Ring> rings = decoded_rings(b.blocks, ArcSet::Minimal, m, p, chained);
            CHECK(chained);
            REQUIRE(rings.size() == b.store.polygons[p].ring_count);
            for (uint32_t k = 0; k < rings.size(); k++) {
                const uint32_t r = b.store.polygons[p].first_ring + k;
                tier_ring(b.arcs, b.store, fine_tier, before, r, ring, raw, stored);
                const bool fine_valid = valid(ring);
                CHECK_EQ(bool(state.fine_valid[r]), fine_valid);
                if (fine_valid) CHECK(valid(rings[k]));
                rings_checked++;
            }
        }
        fine_rings += state.counts.fine_rings;
        for (uint32_t a = 0; a < b.arcs.minimal.arcs.size(); a++) {
            const int k = block_of(b.blocks, m, a).k;
            fine_arcs += k == 2;
            coarse_arcs += k == 3;
        }
    }
    // The fixtures keep rings fine, and leave most arcs coarse.
    CHECK(fine_rings > 10);
    CHECK(fine_arcs > 10);
    CHECK(coarse_arcs > fine_arcs);
    CHECK(rings_checked > 250);
}

TEST(the_repair_on_the_11_m_grid_gives_back_a_vertex_on_its_arcs_grid) {
    // A border bowing 1400 south (under the 37.5 m tolerance), straight once
    // simplified; an island 400 north of the straight line, 1660 north of
    // the bow, which the 11 m grid puts on that line: the repair there gives
    // the bow back, on the 11 m grid.
    const Ring border = {pt(0, 0), pt(0, 40000), pt(-1400, 50000), pt(0, 60000), pt(0, 100000)};
    Ring above = border, below = border;
    above.insert(above.end(), {pt(100000, 100000), pt(100000, 0)});
    below.insert(below.end(), {pt(-100000, 100000), pt(-100000, 0)});
    std::reverse(below.begin(), below.end());
    const Ring island = closed({pt(400, 49000), pt(400, 51000), pt(3000, 51000), pt(3000, 49000)});
    const Built b = built({{closed(above), {}}, {closed(below), {}}, {island, {}}});
    const size_t m = minimal_tier(), f = full_simplified_tier();
    const ArcTierCounts& fine = b.arcs.tiers[f].counts;
    CHECK_EQ(fine.crossings_simplified, size_t(0));
    CHECK_EQ(fine.crossings_left, size_t(0));
    const ArcTierCounts& c = b.arcs.tiers[m].counts;
    CHECK_EQ(c.fine_crossings_left, size_t(0));
    CHECK(c.crossings_coarse >= 1);
    CHECK(c.repair_restored >= 1);
    CHECK_EQ(c.crossings_left, size_t(0));
    CHECK_EQ(c.fine_rings, size_t(0));
    const uint32_t a = minimal_arc_through(b, pt(-1400, 50000));
    REQUIRE(a != UINT32_MAX);
    const arc_blocks::DecodedArc d = block_of(b.blocks, m, a);
    CHECK_EQ(int(d.k), 3);
    CHECK(std::find(d.vertices.begin(), d.vertices.end(), pt(-1000, 50000)) != d.vertices.end());
    // The 1.1 m tier keeps the border straight.
    const uint32_t full = full_arc_through(b, pt(-1400, 50000));
    REQUIRE(full != UINT32_MAX);
    CHECK_EQ(block_of(b.blocks, f, full).vertices.size(), size_t(2));
}

TEST(the_stage_aborts_on_a_ring_valid_on_the_1_1_m_grid_that_collapses_or_a_coarse_arc_at_a_junction_kept_fine) {
    const Triangle tri;
    const Built b = built(tri.polygons());
    const size_t m = minimal_tier();
    CHECK_EQ(refusal_of([&] { check_coarsened_tier(b.arcs, b.store, m, b.blocks.tiers[m], 2); }), std::string());
    // The triangle's arcs at k = 2 but on the 11 m grid: it collapses.
    AdminArcBlocks collapsed = b.blocks;
    for (auto [from, to] : Pairs{{tri.c1, tri.c2}, {tri.c2, tri.c3}, {tri.c3, tri.c1}}) {
        const uint32_t a = minimal_arc(b, from, to);
        REQUIRE(a != UINT32_MAX);
        collapsed = with_block(collapsed, m, a, snapped_ring(minimal_raw(b, a), kCoarse), 2);
    }
    CHECK(refusal_of([&] { check_coarsened_tier(b.arcs, b.store, m, collapsed.tiers[m], 2); })
              .find("valid on the fine grid, collapses in admin-minimal") != std::string::npos);
    // A spoke at k = 3, its corner of the triangle moved onto the 11 m grid.
    const uint32_t spoke = minimal_arc(b, tri.c1, tri.sw);
    REQUIRE(spoke != UINT32_MAX);
    const AdminArcBlocks coarse = with_block(b.blocks, m, spoke, snapped_ring(minimal_raw(b, spoke), kCoarse), 3);
    CHECK(refusal_of([&] { check_coarsened_tier(b.arcs, b.store, m, coarse.tiers[m], 2); })
              .find("ends at a junction kept fine but is stored at k = 3") != std::string::npos);
}

TEST(a_crossing_the_11_m_grid_makes_with_nothing_to_give_back_puts_both_arcs_back_as_the_1_1_m_tier_has_them) {
    // An island 400 north of a square's straight edge, every vertex of both
    // kept: the 11 m grid puts the island's edge on the square's, and the
    // repair there has no vertex to give back.
    const Ring square = closed({pt(0, 0), pt(0, 100000), pt(-100000, 100000), pt(-100000, 0)});
    const Ring island = closed({pt(400, 49000), pt(400, 51000), pt(3000, 51000), pt(3000, 49000)});
    const Built b = built({{square, {}}, {island, {}}});
    const size_t m = minimal_tier();
    const ArcTierCounts& c = b.arcs.tiers[m].counts;
    CHECK_EQ(c.fine_crossings_left, size_t(0));
    CHECK(c.crossings_coarse >= 1);
    CHECK_EQ(c.crossings_left, size_t(0));
    CHECK_EQ(c.put_back_arcs, size_t(2));
    CHECK_EQ(c.e7_grid_arcs, size_t(0));
    for (const Ring* ring : {&square, &island}) {
        const uint32_t a = minimal_arc_through(b, (*ring)[1]);
        REQUIRE(a != UINT32_MAX);
        const arc_blocks::DecodedArc d = block_of(b.blocks, m, a);
        CHECK_EQ(int(d.k), 2);
        CHECK(same_ring(d.vertices, *ring));
    }
}

TEST(a_hole_the_11_m_grid_puts_on_its_outer_ring_goes_back_to_the_1_1_m_tier_not_osms_grid) {
    // A hole 400 inside its outer ring's straight edge: on the 11 m grid it
    // touches it, so it stops nesting, with nothing to give back.
    const Ring outer = closed({pt(0, 0), pt(0, 100000), pt(100000, 100000), pt(100000, 0)});
    const Ring hole = closed({pt(400, 40000), pt(400, 60000), pt(3000, 60000), pt(3000, 40000)});
    const Built b = built({{outer, {hole}}});
    const size_t m = minimal_tier();
    const ArcTierCounts& c = b.arcs.tiers[m].counts;
    CHECK(c.crossings_coarse >= 1);
    CHECK_EQ(c.crossings_left, size_t(0));
    CHECK_EQ(c.rings_nest_introduced, size_t(0));
    CHECK_EQ(c.e7_grid_arcs, size_t(0));
    CHECK_EQ(c.put_back_arcs, size_t(2));
    bool chained = true;
    const std::vector<Ring> rings = decoded_rings(b.blocks, ArcSet::Minimal, m, 0, chained);
    REQUIRE(rings.size() == 2);
    CHECK(chained);
    CHECK(same_ring(rings[0], outer) && same_ring(rings[1], hole));
    for (const Ring* ring : {&outer, &hole}) {
        const uint32_t a = minimal_arc_through(b, (*ring)[1]);
        REQUIRE(a != UINT32_MAX);
        CHECK_EQ(int(block_of(b.blocks, m, a).k), 2);
    }
}

TEST(a_polygon_the_11_m_grid_unnests_without_a_crossing_goes_back_to_the_1_1_m_tier) {
    // A hole just inside its outer ring's straight north edge (at 300), small
    // enough to collapse on the 11 m grid, so kept fine: the 11 m grid moves
    // the edge to 0, which leaves the hole outside, touching nothing, with
    // nothing dropped to give back. The polygon's arcs go back as the 1.1 m
    // tier has them, not to OSM's grid.
    const Ring outer = closed({pt(300, 0), pt(300, 100000), pt(-100000, 100000), pt(-100000, 0)});
    const Ring hole = closed({pt(137, 50037), pt(237, 49737), pt(237, 50337)});
    const Built b = built({{outer, {hole}}});
    const size_t m = minimal_tier();
    const ArcTierCounts& c = b.arcs.tiers[m].counts;
    CHECK_EQ(c.crossings_coarse, size_t(0));
    CHECK_EQ(c.put_back_arcs, size_t(2));
    CHECK_EQ(c.e7_grid_arcs, size_t(0));
    CHECK_EQ(c.rings_nest_introduced, size_t(0));
    bool chained = true;
    const std::vector<Ring> rings = decoded_rings(b.blocks, ArcSet::Minimal, m, 0, chained);
    REQUIRE(rings.size() == 2);
    CHECK(chained);
    CHECK(same_ring(rings[0], outer));
    CHECK(same_ring(rings[1], snapped_ring(hole, kFine)));
    const uint32_t a = minimal_arc_through(b, outer[1]);
    REQUIRE(a != UINT32_MAX);
    CHECK_EQ(int(block_of(b.blocks, m, a).k), 2);
}

TEST(an_arc_put_back_drops_the_vertex_the_11_m_repair_gave_it) {
    // A square whose north edge bows 300 south at 30000, dropped on both
    // grids, and an island 400 north of that edge, which the 11 m grid lays
    // on it. The repair there gives the bow back, on the 11 m grid, so on the
    // edge still, and has nothing more to give: both arcs go back as the
    // 1.1 m tier has them, the bow dropped again.
    const Ring square = closed({pt(0, 0), pt(-300, 30000), pt(0, 100000), pt(-100000, 100000), pt(-100000, 0)});
    const Ring island = closed({pt(400, 49000), pt(400, 51000), pt(3000, 51000), pt(3000, 49000)});
    const Built b = built({{square, {}}, {island, {}}});
    const size_t m = minimal_tier();
    const ArcTierCounts& c = b.arcs.tiers[m].counts;
    CHECK(c.repair_restored >= 1);
    CHECK_EQ(c.put_back_arcs, size_t(2));
    CHECK_EQ(c.crossings_left, size_t(0));
    const uint32_t a = minimal_arc_through(b, square[1]);
    REQUIRE(a != UINT32_MAX);
    const arc_blocks::DecodedArc d = block_of(b.blocks, m, a);
    CHECK_EQ(int(d.k), 2);
    CHECK(same_ring(d.vertices, closed({pt(0, 0), pt(0, 100000), pt(-100000, 100000), pt(-100000, 0)})));
}

TEST(an_arc_put_back_that_meets_a_coarse_one_puts_that_one_back_in_another_round) {
    // The 11 m grid lays the triangle T's south edge on the square Q's north
    // edge, 400 below: both go back. T's apex then sits, on the 1.1 m grid,
    // at 500 on P's south edge as the 11 m grid has it (600 raw), so a second
    // round puts P back too.
    const int32_t L = 500000;
    const Ring T = closed({pt(-5000, L - 3000), pt(450, L), pt(-5000, L + 3000)});
    const Ring Q = closed({pt(-5400, L - 2000), pt(-5400, L + 2000), pt(-9000, L + 2000), pt(-9000, L - 2000)});
    const Ring P = closed({pt(1450, L - 10000), pt(-250, L + 10000), pt(20000, L + 10000), pt(20000, L - 10000)});
    const Built b = built({{T, {}}, {Q, {}}, {P, {}}});
    const size_t m = minimal_tier();
    const ArcTierCounts& c = b.arcs.tiers[m].counts;
    CHECK_EQ(c.put_back_arcs, size_t(3));
    CHECK_EQ(c.put_back_rounds, 2);
    CHECK_EQ(c.crossings_left, size_t(0));
    for (const Ring* ring : {&T, &Q, &P}) {
        const uint32_t a = minimal_arc_through(b, (*ring)[1]);
        REQUIRE(a != UINT32_MAX);
        const arc_blocks::DecodedArc d = block_of(b.blocks, m, a);
        CHECK_EQ(int(d.k), 2);
        CHECK(same_ring(d.vertices, snapped_ring(*ring, kFine)));
    }
}

TEST(a_ring_kept_fine_that_the_11_m_repair_collapses_again_goes_back_to_the_1_1_m_tier) {
    // R = A B C is valid on the 1.1 m grid and a line on the 11 m one, so
    // the loop keeps it fine. Its arc B..C drops D, at A's latitude; the
    // square Q, 300 below B..C, lands on it on the 11 m grid, and the repair
    // there gives D back. A B D C has no area: the bowtie the raw ring is
    // already (A-B crosses D-C; E, dropped, gives the raw ring its area).
    // Every arc and junction of R is fine by then, so the loop has nothing to
    // move: R's arcs go back as the 1.1 m tier has them, and Q's, which cross
    // B..C again then, too. Triangles touching R at A, B and C alone make
    // each a junction.
    const PackedLocation A = pt(400, 500000), B = pt(0, 0), C = pt(0, 1000000), D = pt(400, 100000), E = pt(500, 750000);
    const Ring R = closed({A, B, D, C, E});
    const Ring Q = closed({pt(-300, 200000), pt(-300, 300000), pt(-5300, 300000), pt(-5300, 200000)});
    const std::vector<Polygon> polygons = {{R, {}},
                                           {closed({A, pt(5000, 499000), pt(5000, 501000)}), {}},
                                           {closed({B, pt(-5000, 1000), pt(-5000, -1000)}), {}},
                                           {closed({C, pt(-5000, 1001000), pt(-5000, 999000)}), {}},
                                           {Q, {}}};
    // As the plan of the fixture says.
    CHECK(valid(snapped_ring(closed({A, B, C}), kFine)) && !valid(snapped_ring(closed({A, B, C}), kCoarse)));
    CHECK(!valid(closed({A, B, D, C})));
    Built b;
    const std::string why = refusal_of([&] { b = built(polygons); });
    CHECK_EQ(why, std::string());
    if (!why.empty()) return;
    const size_t m = minimal_tier();
    const ArcTierCounts& c = b.arcs.tiers[m].counts;
    CHECK(c.repair_restored >= 1);
    CHECK_EQ(c.crossings_left, size_t(0));
    CHECK_EQ(c.degenerate_rings, size_t(0));
    CHECK_EQ(c.e7_grid_arcs, size_t(0));
    bool chained = true;
    const std::vector<Ring> rings = decoded_rings(b.blocks, ArcSet::Minimal, m, 0, chained);
    REQUIRE(rings.size() == 1);
    CHECK(chained);
    CHECK(same_ring(rings[0], closed({A, B, C})));
    const uint32_t bc = minimal_arc(b, B, C), q = minimal_arc_through(b, Q[1]);
    REQUIRE(bc != UINT32_MAX && q != UINT32_MAX);
    const arc_blocks::DecodedArc arc = block_of(b.blocks, m, bc);
    CHECK_EQ(int(arc.k), 2);
    CHECK(std::find(arc.vertices.begin(), arc.vertices.end(), D) == arc.vertices.end());
    const arc_blocks::DecodedArc square = block_of(b.blocks, m, q);
    CHECK_EQ(int(square.k), 2);
    CHECK(same_ring(square.vertices, Q));
}

// How many of the first `polygons` polygons hold p, each by its minimal rings
// as tier t's blocks hold them (even-odd over all of them).
size_t holders(const Built& b, size_t t, uint32_t polygons, PackedLocation p) {
    size_t n = 0;
    for (uint32_t i = 0; i < polygons; i++) {
        bool chained = true, in = false;
        for (const Ring& r : decoded_rings(b.blocks, ArcSet::Minimal, t, i, chained))
            if (exact_ring_contains(r.data(), static_cast<uint32_t>(r.size()), p)) in = !in;
        n += in;
    }
    return n;
}

TEST(a_strip_the_11_m_grid_turns_over_goes_back_to_the_1_1_m_tier) {
    // The strip S N V, 0.3 m wide, between A, east of its edge S..N, and B,
    // west of its edge N..V..S. On the 11 m grid V lands east of S..N: the
    // strip winds the other way, a triangle that it, A and B would all hold
    // (P), and nothing crosses. Its arcs go back as the 1.1 m tier has them.
    const PackedLocation S = pt(0, -499), N = pt(20000, 1499), V = pt(10499, 520), P = pt(12000, 900);
    const Ring strip = closed({S, N, V});
    const Ring A = closed({S, pt(0, 20000), pt(20000, 20000), N});
    const Ring B = closed({S, V, N, pt(20000, -20000), pt(0, -20000)});
    // As the plan of the fixture says.
    CHECK_EQ(winding_of(snapped_ring(strip, kFine)), winding_of(strip));
    CHECK_EQ(winding_of(snapped_ring(strip, kCoarse)), -winding_of(strip));
    CHECK(exact_ring_contains(snapped_ring(strip, kCoarse).data(), 4, P));
    CHECK(!exact_ring_contains(snapped_ring(strip, kFine).data(), 4, P));
    Built b;
    const std::string why = refusal_of([&] { b = built({{strip, {}}, {A, {}}, {B, {}}}); });
    CHECK_EQ(why, std::string());
    if (!why.empty()) return;
    const size_t m = minimal_tier();
    const ArcTierCounts& c = b.arcs.tiers[m].counts;
    CHECK_EQ(c.folded_put_back_rings, size_t(1));
    CHECK_EQ(c.folds_left, size_t(0));
    CHECK_EQ(c.crossings_left, size_t(0));
    bool chained = true;
    const std::vector<Ring> rings = decoded_rings(b.blocks, ArcSet::Minimal, m, 0, chained);
    REQUIRE(rings.size() == 1);
    CHECK(chained);
    CHECK(same_ring(rings[0], snapped_ring(strip, kFine)));
    CHECK_EQ(winding_of(rings[0]), winding_of(strip));
    CHECK_EQ(holders(b, m, 3, P), size_t(1));
}

bool folds_over(const Ring& ring) {
    static FoldScratch scratch;  // as the tier's, kept from one ring to the next
    return ring_folds_over(ring, scratch);
}

// Every walk of `ring`: from each of its vertices, either way.
std::vector<Ring> walks(Ring ring) {
    std::vector<Ring> out;
    for (int way = 0; way < 2; way++) {
        for (size_t k = 0; k < ring.size(); k++) {
            out.push_back(ring);
            std::rotate(ring.begin(), ring.begin() + 1, ring.end());
        }
        std::reverse(ring.begin(), ring.end());
    }
    return out;
}

// How many walks of `ring` fold over (folds_over).
size_t folding_walks(const Ring& ring) {
    size_t n = 0;
    for (const Ring& w : walks(ring)) n += folds_over(w);
    return n;
}

TEST(a_ring_folds_over_where_a_loop_it_pinches_off_winds_against_it_outside_it) {
    // The square A B C D, and at C a loop: outside the square winding
    // against it (a fold), outside it winding as it does (a pinched neck),
    // inside it winding against it (a hole pinched off), or of no area (a
    // spur). Only the first folds.
    const PackedLocation A = pt(0, 0), B = pt(0, 100), C = pt(100, 100), D = pt(100, 0);
    const PackedLocation E = pt(150, 150), F = pt(150, 100), G = pt(50, 50), H = pt(50, 90), K = pt(150, 120);
    const Ring square = {A, B, C, D};
    // As the plan of the fixture says.
    CHECK_EQ(winding_of({C, F, E}), -winding_of(square));
    CHECK_EQ(winding_of({C, E, F}), winding_of(square));
    CHECK_EQ(winding_of({C, H, G}), -winding_of(square));
    CHECK(exact_ring_contains(square.data(), 4, G) && exact_ring_contains(square.data(), 4, H));
    CHECK(folds_over({A, B, C, F, E, C, D}));
    CHECK(!folds_over({A, B, C, E, F, C, D}));
    CHECK(!folds_over({A, B, C, H, G, C, D}));
    CHECK(!folds_over({A, B, C, K, C, D}));
    CHECK(!folds_over(square));
    // Where the ring starts doesn't matter.
    CHECK(folds_over({E, C, D, A, B, C, F}));
    CHECK(!folds_over({G, C, D, A, B, C, H}));
}

TEST(a_ring_left_a_spur_once_its_lobes_are_split_off_folds_where_one_winds_against_another_outside_it) {
    // The square and the fold at C above, the ring walked from P, on the
    // square's edge C..D, to C and back at its end: split, it is the two
    // lobes and, left, the spur P C, of no area. However it is walked, it
    // folds; with a pinched neck or a hole at C instead it doesn't.
    const PackedLocation A = pt(0, 0), B = pt(0, 100), C = pt(100, 100), D = pt(100, 0), P = pt(100, 50);
    const PackedLocation E = pt(150, 150), F = pt(150, 100), G = pt(50, 50), H = pt(50, 90);
    const Ring fold = {P, C, F, E, C, D, A, B, C};
    CHECK(folds_over(fold));
    CHECK_EQ(folding_walks(fold), size_t(18));
    CHECK_EQ(folding_walks({P, C, E, F, C, D, A, B, C}), size_t(0));
    CHECK_EQ(folding_walks({P, C, H, G, C, D, A, B, C}), size_t(0));
    // More lobes: a pinched neck at A besides the fold (A M N, outside the
    // square, winding as it does); or the fold moved to the spur's far end
    // (P X Y, outside the square, winding against it), which it touches the
    // rest of the ring at alone. With the fold turned round, neither folds.
    const PackedLocation M = pt(-40, -10), N = pt(-40, 30), X = pt(130, 40), Y = pt(130, 60);
    const Ring square = {A, B, C, D};
    CHECK_EQ(winding_of({A, M, N}), winding_of(square));
    CHECK_EQ(winding_of({P, X, Y}), -winding_of(square));
    CHECK_EQ(folding_walks({P, C, F, E, C, D, A, M, N, A, B, C}), size_t(24));
    CHECK_EQ(folding_walks({P, C, E, F, C, D, A, M, N, A, B, C}), size_t(0));
    CHECK_EQ(folding_walks({P, X, Y, P, C, D, A, B, C}), size_t(18));
    CHECK_EQ(folding_walks({P, Y, X, P, C, D, A, B, C}), size_t(0));
}

TEST(a_hole_a_lobe_of_the_ring_pinches_off_folds_nothing_wherever_the_ring_starts) {
    // The square, and at C the pinched neck C Q3 Q2 Q1 (outside the square,
    // winding as it does), which pinches off at Q2 the hole Q2 R1 R2. The
    // hole lies outside the square and winds against it, but the ring winds
    // round it not at all: nothing folds, however the ring is walked. Turned
    // round, the hole is a lobe inside a lobe, which folds nothing either.
    const PackedLocation A = pt(0, 0), B = pt(0, 100), C = pt(100, 100), D = pt(100, 0);
    const PackedLocation Q1 = pt(200, 100), Q2 = pt(200, 200), Q3 = pt(100, 200), R1 = pt(150, 180), R2 = pt(180, 150);
    const Ring square = {A, B, C, D}, neck = {C, Q3, Q2, Q1}, hole = {Q2, R1, R2};
    CHECK_EQ(winding_of(neck), winding_of(square));
    CHECK_EQ(winding_of(hole), -winding_of(square));
    CHECK(exact_ring_contains(neck.data(), 4, R1) && exact_ring_contains(neck.data(), 4, R2));
    const Ring ring = {A, B, C, Q3, Q2, R1, R2, Q2, Q1, C, D};
    CHECK(!folds_over(ring));
    CHECK_EQ(folding_walks(ring), size_t(0));
    CHECK_EQ(folding_walks({A, B, C, Q3, Q2, R2, R1, Q2, Q1, C, D}), size_t(0));
    // The lobe turned round instead, a fold, with the hole turned round too:
    // a hole in the fold, which folds still.
    CHECK_EQ(folding_walks({A, B, C, Q1, Q2, R2, R1, Q2, Q3, C, D}), size_t(22));
}

TEST(a_loop_inside_a_hole_the_ring_pinches_off_folds_where_it_winds_as_the_hole_does) {
    // The square pinches off at C the hole C H1 H2, and the hole pinches off
    // at H1 the loop H1 T2 T1 inside it, winding as the hole does: the ring
    // winds round that loop's inside against the square's. It folds,
    // however the ring is walked; with the loop turned round, an island in
    // the hole, it doesn't.
    const PackedLocation A = pt(0, 0), B = pt(0, 100), C = pt(100, 100), D = pt(100, 0);
    const PackedLocation H1 = pt(10, 90), H2 = pt(90, 10), T1 = pt(40, 80), T2 = pt(70, 50);
    const Ring square = {A, B, C, D}, hole = {C, H1, H2}, loop = {H1, T2, T1};
    CHECK_EQ(winding_of(hole), -winding_of(square));
    CHECK_EQ(winding_of(loop), winding_of(hole));
    CHECK(exact_ring_contains(square.data(), 4, H1) && exact_ring_contains(square.data(), 4, H2));
    CHECK(exact_ring_contains(hole.data(), 3, T1) && exact_ring_contains(hole.data(), 3, T2));
    const Ring ring = {A, B, C, H1, T2, T1, H1, H2, C, D};
    CHECK(folds_over(ring));
    CHECK_EQ(folding_walks(ring), size_t(20));
    CHECK_EQ(folding_walks({A, B, C, H1, T1, T2, H1, H2, C, D}), size_t(0));
}

TEST(an_arc_keeps_copy_takes_one_arcs_marks_from_another_of_the_same_arcs) {
    auto sizes = [](size_t a) { return a == 1 ? 130u : 7u; };
    ArcKeep from(3, sizes), to(3, sizes);
    for (uint32_t i : {0u, 63u, 64u, 129u}) from.set(1, i);
    from.set(0, 3);
    to.set(1, 5);
    to.set(2, 6);
    to.copy_arc(from, 1);
    for (uint32_t i = 0; i < 130; i++) CHECK_EQ(to.test(1, i), from.test(1, i));
    CHECK(!to.test(0, 3));
    CHECK(to.test(2, 6));
    const ArcKeep other(3, [](size_t) { return 7u; });
    CHECK(refusal_of<std::logic_error>([&] { to.copy_arc(other, 0); }).find("other arcs") != std::string::npos);
}

TEST(an_arc_keeps_copy_of_a_list_takes_each_listed_arcs_marks_and_refuses_other_arcs_before_any) {
    auto sizes = [](size_t a) { return a % 2 ? 130u : 7u; };
    ArcKeep from(5, sizes), to(5, sizes);
    for (uint32_t a = 0; a < 5; a++)
        for (uint32_t i : {0u, 6u}) from.set(a, i);
    for (uint32_t i : {63u, 64u, 129u}) from.set(3, i);
    to.set(1, 64);
    to.set(2, 5);
    to.copy_arcs(from, {1, 3, 4});
    for (uint32_t a : {1u, 3u})
        for (uint32_t i = 0; i < 130; i++) CHECK_EQ(to.test(a, i), from.test(a, i));
    for (uint32_t i = 0; i < 7; i++) CHECK_EQ(to.test(4, i), from.test(4, i));
    // The arcs not listed keep their own marks.
    for (uint32_t i = 0; i < 7; i++) CHECK_EQ(to.test(0, i), false);
    for (uint32_t i = 0; i < 7; i++) CHECK_EQ(to.test(2, i), i == 5);
    // Marks of other arcs: refused before any arc is copied, and for an
    // empty list too.
    const ArcKeep other(5, [](size_t) { return 130u; });
    const ArcKeep was = to;
    CHECK(refusal_of<std::logic_error>([&] { to.copy_arcs(other, {0, 2}); }).find("other arcs") != std::string::npos);
    CHECK(to == was);
    CHECK(refusal_of<std::logic_error>([&] { to.copy_arcs(other, {}); }).find("other arcs") != std::string::npos);
}
