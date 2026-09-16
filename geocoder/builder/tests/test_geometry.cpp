// Unit tests for the pure geometry helpers in geometry.h.
// These lock in CURRENT behaviour (a regression net for refactors), not a
// spec: coord_key determinism + collision-resolution at the nanodegree grid,
// polygon_area shoelace magnitude/winding-insensitivity, and the
// Douglas-Peucker simplify_polygon / simplify_polygon_epsilon variants.
//
// NOTE: point_in_polygon / point_in_polygon_nc are NOT defined in geometry.h
// (they live in continent_boundaries.h), so they are intentionally not tested
// here — this file covers the geometry.h module exactly.
#include "geometry.h"

#include <utility>
#include <vector>

#include "test_framework.h"

using Pt = std::pair<double, double>;

// ---------------------------------------------------------------------------
// coord_key
// ---------------------------------------------------------------------------

TEST(geometry_coord_key_deterministic) {
    // Same lat/lng -> identical key, every time.
    int64_t a = coord_key(51.4769, -0.0005);
    int64_t b = coord_key(51.4769, -0.0005);
    CHECK_EQ(a, b);

    // A literally-recomputed call also matches (no hidden state).
    CHECK_EQ(coord_key(0.0, 0.0), coord_key(0.0, 0.0));
    CHECK_EQ(coord_key(0.0, 0.0), int64_t(0));
}

TEST(geometry_coord_key_distinct_coords_differ) {
    // Points separated by more than one nanodegree grid step get distinct keys.
    int64_t a = coord_key(10.0, 20.0);
    int64_t b = coord_key(10.0, 20.0001);  // ~1e-4 deg apart in lng
    int64_t c = coord_key(10.0001, 20.0);  // ~1e-4 deg apart in lat
    CHECK(a != b);
    CHECK(a != c);
    CHECK(b != c);

    // Sign of lat vs lng must not collide: (lat,lng) packing keeps lat high.
    CHECK(coord_key(1.0, 2.0) != coord_key(2.0, 1.0));
}

TEST(geometry_coord_key_grid_resolution) {
    // The grid is 1e7 nanodegrees per degree (1e-7 deg). Two coords that round
    // to the SAME nanodegree integer must produce the same key; differences far
    // below the grid step collapse together.
    int64_t base = coord_key(45.1234567, -73.7654321);
    // +1e-9 deg: well below the 1e-7 grid step, rounds to same integer.
    int64_t sub = coord_key(45.12345671, -73.76543211);
    CHECK_EQ(base, sub);

    // +1e-7 deg (one full grid step) yields a different key.
    int64_t step = coord_key(45.1234577, -73.7654321);
    CHECK(base != step);
}

TEST(geometry_coord_key_packs_lat_high_lng_low) {
    // Reproduce the exact packing the header documents so a refactor that
    // changes shift/rounding is caught.
    double lat = 12.3456789, lng = -98.7654321;
    int32_t ilat = static_cast<int32_t>(lat * 1e7 + (lat >= 0 ? 0.5 : -0.5));
    int32_t ilng = static_cast<int32_t>(lng * 1e7 + (lng >= 0 ? 0.5 : -0.5));
    int64_t expected =
        (static_cast<int64_t>(ilat) << 32) | static_cast<uint32_t>(ilng);
    CHECK_EQ(coord_key(lat, lng), expected);

    // Negative lng must be preserved in the low 32 bits (via uint32_t cast),
    // not sign-extended into lat's region.
    int64_t k = coord_key(0.0, -1.0);
    CHECK_EQ(static_cast<uint32_t>(k & 0xFFFFFFFFull),
             static_cast<uint32_t>(-10000000));
}

// ---------------------------------------------------------------------------
// polygon_area
// ---------------------------------------------------------------------------

TEST(geometry_polygon_area_unit_square) {
    // Unit square (CCW). Area should be ~1.0 sq-degree. The ring is given
    // OPEN (no repeated closing vertex); the shoelace uses modular wrap.
    std::vector<Pt> sq = {{0, 0}, {0, 1}, {1, 1}, {1, 0}};
    CHECK_NEAR(polygon_area(sq), 1.0f, 1e-6);
}

TEST(geometry_polygon_area_winding_insensitive) {
    // fabs() makes the result independent of CW vs CCW orientation.
    std::vector<Pt> ccw = {{0, 0}, {0, 1}, {1, 1}, {1, 0}};
    std::vector<Pt> cw = {{0, 0}, {1, 0}, {1, 1}, {0, 1}};
    CHECK_NEAR(polygon_area(ccw), polygon_area(cw), 1e-6);
    CHECK_NEAR(polygon_area(cw), 1.0f, 1e-6);
}

TEST(geometry_polygon_area_closed_ring_same) {
    // Passing the ring CLOSED (first == last) adds a degenerate zero-area
    // segment, so the area is unchanged.
    std::vector<Pt> open = {{0, 0}, {0, 2}, {3, 2}, {3, 0}};
    std::vector<Pt> closed = {{0, 0}, {0, 2}, {3, 2}, {3, 0}, {0, 0}};
    CHECK_NEAR(polygon_area(open), 6.0f, 1e-5);
    CHECK_NEAR(polygon_area(closed), polygon_area(open), 1e-5);
}

TEST(geometry_polygon_area_degenerate) {
    // Collinear / empty inputs have zero area, no crash.
    std::vector<Pt> line = {{0, 0}, {1, 1}, {2, 2}};
    CHECK_NEAR(polygon_area(line), 0.0f, 1e-6);
    std::vector<Pt> point = {{5, 5}};
    CHECK_NEAR(polygon_area(point), 0.0f, 1e-6);
    std::vector<Pt> empty;
    CHECK_NEAR(polygon_area(empty), 0.0f, 1e-6);
}

TEST(geometry_polygon_area_triangle) {
    // Right triangle legs 4 x 3 -> area 6.
    std::vector<Pt> tri = {{0, 0}, {4, 0}, {0, 3}};
    CHECK_NEAR(polygon_area(tri), 6.0f, 1e-5);
}

// ---------------------------------------------------------------------------
// simplify_polygon (binary-search-to-max-vertices variant)
// ---------------------------------------------------------------------------

TEST(geometry_simplify_polygon_passthrough_small) {
    // Inputs at or below max_vertices are returned unchanged (same data).
    std::vector<Pt> pts = {{0, 0}, {1, 1}, {2, 0}};
    auto out = simplify_polygon(pts, 500);
    CHECK_EQ(out.size(), pts.size());
    CHECK_EQ(out, pts);

    // Tiny inputs do not crash even when below default cap.
    std::vector<Pt> two = {{0, 0}, {1, 1}};
    CHECK_EQ(simplify_polygon(two).size(), size_t(2));
    std::vector<Pt> one = {{0, 0}};
    CHECK_EQ(simplify_polygon(one).size(), size_t(1));
    std::vector<Pt> none;
    CHECK_EQ(simplify_polygon(none).size(), size_t(0));
}

TEST(geometry_simplify_polygon_removes_collinear) {
    // A dense straight line with both endpoints far apart: forcing the result
    // below the cap should drop interior collinear points but keep endpoints.
    std::vector<Pt> pts;
    for (int i = 0; i <= 20; i++) pts.push_back({double(i), 0.0});
    // Add one off-line bump in the middle so it isn't perfectly degenerate.
    pts[10] = {10.0, 1.0};

    auto out = simplify_polygon(pts, 4);  // force aggressive simplification
    CHECK(out.size() <= pts.size());
    CHECK(out.size() >= 2);
    // Endpoints are always preserved.
    CHECK_EQ(out.front(), pts.front());
    CHECK_EQ(out.back(), pts.back());
    // The salient bump (max-deviation point) survives binary-search DP.
    bool has_bump = false;
    for (const auto& p : out)
        if (p.first == 10.0 && p.second == 1.0) has_bump = true;
    CHECK(has_bump);
}

TEST(geometry_simplify_polygon_respects_max_vertices) {
    // Many distinct points, force down to a small cap. Result count must not
    // exceed the cap (binary search targets <= max_vertices).
    std::vector<Pt> pts;
    for (int i = 0; i < 100; i++)
        pts.push_back({double(i), (i % 2) ? 0.3 : -0.3});  // zig-zag
    auto out = simplify_polygon(pts, 10);
    CHECK(out.size() <= size_t(10));
    CHECK(out.size() >= 2);
    CHECK_EQ(out.front(), pts.front());
    CHECK_EQ(out.back(), pts.back());
}

// ---------------------------------------------------------------------------
// simplify_polygon_epsilon (error-bounded variant)
// ---------------------------------------------------------------------------

TEST(geometry_simplify_epsilon_passthrough_min) {
    // At or below min_vertices the input is returned unchanged.
    std::vector<Pt> pts = {{0, 0}, {1, 0}, {1, 1}, {0, 1}};
    auto out = simplify_polygon_epsilon(pts, 0.5, 4);
    CHECK_EQ(out, pts);
}

TEST(geometry_simplify_epsilon_drops_within_tolerance) {
    // Straight run sampled densely; a large epsilon should collapse the
    // interior collinear points, keeping endpoints. min_vertices floor still
    // applies, so we expect at least min_vertices kept where possible.
    std::vector<Pt> pts;
    for (int i = 0; i <= 30; i++) pts.push_back({double(i) * 0.1, 0.0});
    auto out = simplify_polygon_epsilon(pts, 1.0, 2);
    CHECK(out.size() <= pts.size());
    CHECK(out.size() >= size_t(2));
    CHECK_EQ(out.front(), pts.front());
    CHECK_EQ(out.back(), pts.back());
    // A perfectly collinear line under a generous epsilon collapses to just
    // the two endpoints.
    CHECK_EQ(out.size(), size_t(2));
}

TEST(geometry_simplify_epsilon_min_vertices_floor) {
    // Even with a huge epsilon that would otherwise reduce to 2 points, the
    // min_vertices floor forces the result back up to at least min_vertices
    // (when the input has enough distinct points to supply them).
    std::vector<Pt> pts;
    for (int i = 0; i < 12; i++)
        pts.push_back({double(i), (i % 2) ? 0.01 : -0.01});
    size_t min_v = 6;
    auto out = simplify_polygon_epsilon(pts, 100.0, min_v);
    CHECK(out.size() >= min_v);
    CHECK(out.size() <= pts.size());
    CHECK_EQ(out.front(), pts.front());
    CHECK_EQ(out.back(), pts.back());
}

TEST(geometry_simplify_epsilon_tiny_inputs_no_crash) {
    std::vector<Pt> one = {{0, 0}};
    CHECK_EQ(simplify_polygon_epsilon(one, 0.5).size(), size_t(1));
    std::vector<Pt> none;
    CHECK_EQ(simplify_polygon_epsilon(none, 0.5).size(), size_t(0));
    std::vector<Pt> four = {{0, 0}, {1, 0}, {1, 1}, {0, 1}};
    // default min_vertices is 4, so this passes through untouched.
    CHECK_EQ(simplify_polygon_epsilon(four, 0.5).size(), size_t(4));
}

// ---------------------------------------------------------------------------
// meters_to_degrees / admin_epsilon_meters (small numeric helpers)
// ---------------------------------------------------------------------------

TEST(geometry_meters_to_degrees_equator) {
    // At the equator cos(0)=1, so 111320 m == 1 degree.
    CHECK_NEAR(meters_to_degrees(111320.0, 0.0), 1.0, 1e-9);
    // Higher latitude shrinks the degree-per-meter span (cos factor).
    double eq = meters_to_degrees(1000.0, 0.0);
    double hi = meters_to_degrees(1000.0, 60.0);
    CHECK(hi > eq);  // same meters span MORE degrees as cos shrinks
    CHECK_NEAR(meters_to_degrees(1000.0, 60.0),
               1000.0 / (111320.0 * std::cos(60.0 * M_PI / 180.0)), 1e-9);
}

TEST(geometry_admin_epsilon_meters_table) {
    // Default table values with scale 1.0 (the inline default).
    double saved_scale = kEpsilonScale;
    kEpsilonScale = 1.0;
    CHECK_NEAR(admin_epsilon_meters(2), 500.0, 1e-9);   // country
    CHECK_NEAR(admin_epsilon_meters(8), 15.0, 1e-9);    // city
    // Levels above 11 clamp to index 11.
    CHECK_NEAR(admin_epsilon_meters(11), 15.0, 1e-9);
    CHECK_NEAR(admin_epsilon_meters(200), admin_epsilon_meters(11), 1e-9);
    // Scale multiplies uniformly.
    kEpsilonScale = 2.0;
    CHECK_NEAR(admin_epsilon_meters(2), 1000.0, 1e-9);
    kEpsilonScale = saved_scale;
}

// --- is_included_highway allow-list ----------------------------------------
// Parity-critical: the accept set mirrors Nominatim's presets.lua
// MAIN_TAGS_STREETS.default. A missing class silently drops whole road
// classes from the index (a live Dresden footway bug motivated the sync).

TEST(highway_allowlist_accepts_full_set) {
    const char* accepted[] = {
        "bridleway", "cycleway", "footway", "living_street",
        "motorway", "motorway_link", "primary", "primary_link",
        "pedestrian", "path", "road", "residential",
        "secondary", "secondary_link", "service", "steps",
        "tertiary", "tertiary_link", "trunk", "trunk_link", "track",
        "unclassified",
    };
    for (const char* v : accepted) CHECK(is_included_highway(v));
}

TEST(highway_allowlist_rejects_non_streets) {
    const char* rejected[] = {
        "bus_stop", "construction", "proposed", "raceway", "corridor",
        "elevator", "platform", "rest_area", "services", "traffic_signals",
        "crossing", "give_way", "stop", "street_lamp", "emergency_bay",
        "motorway_junction", "passing_place", "turning_circle", "busway",
        "", "residentia", "residentiall",
    };
    for (const char* v : rejected) CHECK(!is_included_highway(v));
    CHECK(!is_included_highway(nullptr));
}

TEST(highway_full_filter_footway_subtypes) {
    // footway=sidewalk / crossing are road adjuncts and excluded; any other
    // footway subtype (or none) passes.
    CHECK(!is_included_highway_full("footway", "sidewalk"));
    CHECK(!is_included_highway_full("footway", "crossing"));
    CHECK(is_included_highway_full("footway", "alley"));
    CHECK(is_included_highway_full("footway", nullptr));
    // The subtype filter applies to footway only.
    CHECK(is_included_highway_full("residential", "sidewalk"));
}

TEST(highway_full_filter_ignores_access_and_tunnel) {
    // Nominatim indexes highways regardless of access/tunnel — lock in that
    // is_included_highway_full does NOT filter on them (the Treasury Annex
    // Tunnel case is handled by the addr preference rule instead).
    CHECK(is_included_highway_full("service", nullptr, "private", "yes"));
    CHECK(is_included_highway_full("residential", nullptr, "no", "yes"));
}
