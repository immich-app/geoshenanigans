// Unit tests for resolve_interpolation_endpoints (interpolation.cpp).
//
// This function backfills each interpolation way's start_number/end_number by
// matching the way's first and last node coordinates against the address-point
// set, keyed on a quantized (lat*1e5, lng*1e5) int32 coordinate bucket. These
// tests lock in the CURRENT read-path behaviour as a regression net for a
// refactor — not a spec. They assert exactly what the code does today:
//   * ways with node_count < 2 are skipped untouched,
//   * matching is purely by quantized coordinate (way street vs addr street is
//     irrelevant),
//   * the matched housenumber is run through parse_house_number (leading
//     digits only),
//   * an unmatched endpoint leaves the pre-existing number untouched,
//   * coordinate-bucket collisions keep the deterministically "smallest"
//     address (housenumber strcmp, then street strcmp; NO_DATA street by raw
//     id).
#include "interpolation.h"

#include <string>

#include "parsed_data.h"
#include "types.h"

#include "test_framework.h"

namespace {

// Build helper: intern a string into the pool and return its offset.
uint32_t S(ParsedData& d, const std::string& s) {
    return d.string_pool.intern(s);
}

// Append an addr_point at (lat,lng) with the given housenumber + street.
// Returns its index.
uint32_t add_addr(ParsedData& d, float lat, float lng,
                  uint32_t hn_id, uint32_t street_id) {
    AddrPoint p{};
    p.lat = lat;
    p.lng = lng;
    p.housenumber_id = hn_id;
    p.street_id = street_id;
    p.parent_way_id = NO_DATA;
    p.vertex_offset = NO_DATA;
    p.vertex_count = 0;
    d.addr_points.push_back(p);
    return static_cast<uint32_t>(d.addr_points.size() - 1);
}

// Append an interp way whose nodes are `coords` (lat,lng pairs). Returns its
// index. start_number/end_number/street_id seeded by the caller's args so we
// can observe whether the function overwrites them.
uint32_t add_interp(ParsedData& d,
                    const std::vector<std::pair<float, float>>& coords,
                    uint32_t street_id, uint32_t start_seed,
                    uint32_t end_seed) {
    InterpWay iw{};
    iw.node_offset = static_cast<uint32_t>(d.interp_nodes.size());
    iw.node_count = static_cast<uint8_t>(coords.size());
    iw.street_id = street_id;
    iw.start_number = start_seed;
    iw.end_number = end_seed;
    iw.interpolation = 0;
    for (auto& c : coords) {
        NodeCoord nc{};
        nc.lat = c.first;
        nc.lng = c.second;
        d.interp_nodes.push_back(nc);
    }
    d.interp_ways.push_back(iw);
    return static_cast<uint32_t>(d.interp_ways.size() - 1);
}

}  // namespace

// Basic two-node way whose endpoints sit exactly on two addr_points: the
// start/end numbers are filled from the matched housenumbers.
TEST(interpolation_basic_endpoint_resolution) {
    ParsedData d;
    uint32_t st = S(d, "Main Street");
    uint32_t hn10 = S(d, "10");
    uint32_t hn20 = S(d, "20");
    add_addr(d, 51.5f, -0.1f, hn10, st);
    add_addr(d, 51.6f, -0.2f, hn20, st);

    add_interp(d, {{51.5f, -0.1f}, {51.6f, -0.2f}}, st, 0u, 0u);
    resolve_interpolation_endpoints(d);

    CHECK_EQ(d.interp_ways[0].start_number, 10u);
    CHECK_EQ(d.interp_ways[0].end_number, 20u);
}

// node_count < 2 is skipped entirely: a 1-node way keeps its seeded numbers
// even though that single node coincides with an addr_point.
TEST(interpolation_single_node_way_skipped) {
    ParsedData d;
    uint32_t st = S(d, "Skip Lane");
    uint32_t hn5 = S(d, "5");
    add_addr(d, 10.0f, 20.0f, hn5, st);

    add_interp(d, {{10.0f, 20.0f}}, st, 777u, 888u);
    resolve_interpolation_endpoints(d);

    // Untouched — the loop `continue`s before any lookup.
    CHECK_EQ(d.interp_ways[0].start_number, 777u);
    CHECK_EQ(d.interp_ways[0].end_number, 888u);
}

// Matching is purely by coordinate; the way's street_id and the addr's
// street_id need not agree. A way with a totally different street still gets
// resolved from coincident addr coords.
TEST(interpolation_match_is_coordinate_only) {
    ParsedData d;
    uint32_t way_st = S(d, "Way Street");
    uint32_t addr_st = S(d, "Addr Street");
    uint32_t hn1 = S(d, "1");
    uint32_t hn99 = S(d, "99");
    add_addr(d, 1.0f, 1.0f, hn1, addr_st);
    add_addr(d, 2.0f, 2.0f, hn99, addr_st);

    add_interp(d, {{1.0f, 1.0f}, {2.0f, 2.0f}}, way_st, 0u, 0u);
    resolve_interpolation_endpoints(d);

    CHECK_EQ(d.interp_ways[0].start_number, 1u);
    CHECK_EQ(d.interp_ways[0].end_number, 99u);
}

// An endpoint with no matching addr coordinate leaves the corresponding
// number untouched (retains the seeded value); the matched end still updates.
TEST(interpolation_unmatched_endpoint_keeps_seed) {
    ParsedData d;
    uint32_t st = S(d, "Half Road");
    uint32_t hn30 = S(d, "30");
    // Only the END node has an addr point.
    add_addr(d, 5.5f, 6.6f, hn30, st);

    add_interp(d, {{9.9f, 9.9f}, {5.5f, 6.6f}}, st, 1234u, 5678u);
    resolve_interpolation_endpoints(d);

    // start unmatched -> keeps seed; end matched -> overwritten.
    CHECK_EQ(d.interp_ways[0].start_number, 1234u);
    CHECK_EQ(d.interp_ways[0].end_number, 30u);
}

// parse_house_number takes only the leading digit run. "12a" -> 12, a
// non-digit-leading housenumber -> 0.
TEST(interpolation_parses_leading_digits_only) {
    ParsedData d;
    uint32_t st = S(d, "Mixed Way");
    uint32_t hn12a = S(d, "12a");
    uint32_t hnletter = S(d, "B-7");  // leading non-digit -> 0
    add_addr(d, 0.10f, 0.10f, hn12a, st);
    add_addr(d, 0.20f, 0.20f, hnletter, st);

    add_interp(d, {{0.10f, 0.10f}, {0.20f, 0.20f}}, st, 0u, 0u);
    resolve_interpolation_endpoints(d);

    CHECK_EQ(d.interp_ways[0].start_number, 12u);
    CHECK_EQ(d.interp_ways[0].end_number, 0u);
}

// Quantization: coordinates that differ only below the 1e-5 grid land in the
// same bucket and so resolve to the same (first-inserted/deterministic) addr.
// 0.100000 and 0.100001 both quantize to int32(10000) (and 10000.1 trunc).
TEST(interpolation_subgrid_coords_same_bucket) {
    ParsedData d;
    uint32_t st = S(d, "Grid Ave");
    uint32_t hn8 = S(d, "8");
    // addr at 0.100000,0.200000 -> bucket (10000, 20000)
    add_addr(d, 0.100000f, 0.200000f, hn8, st);

    // way endpoint at 0.1000005,0.2000005 -> same quantized bucket.
    add_interp(d, {{0.1000005f, 0.2000005f}, {0.1000005f, 0.2000005f}}, st,
               0u, 0u);
    // node_count==2 so it is processed; both endpoints share the bucket.
    resolve_interpolation_endpoints(d);

    CHECK_EQ(d.interp_ways[0].start_number, 8u);
    CHECK_EQ(d.interp_ways[0].end_number, 8u);
}

// Collision resolution: two addr_points in the same quantized bucket. The
// function keeps the deterministically smallest by housenumber strcmp. "10"
// < "9" lexicographically (string compare, not numeric), so the way endpoint
// resolves to "10" -> 10, NOT "9" -> 9.
TEST(interpolation_collision_keeps_lexicographically_smallest_housenumber) {
    ParsedData d;
    uint32_t st = S(d, "Collide Rd");
    uint32_t hn9 = S(d, "9");
    uint32_t hn10 = S(d, "10");
    // Both at the exact same coordinate (same bucket). Insert "9" first so
    // we are sure the winner comes from the addr_less comparison, not order.
    add_addr(d, 3.0f, 3.0f, hn9, st);
    add_addr(d, 3.0f, 3.0f, hn10, st);

    add_interp(d, {{3.0f, 3.0f}, {3.0f, 3.0f}}, st, 0u, 0u);
    resolve_interpolation_endpoints(d);

    // strcmp("10","9") < 0  -> "10" wins -> 10.
    CHECK_EQ(d.interp_ways[0].start_number, 10u);
    CHECK_EQ(d.interp_ways[0].end_number, 10u);
}

// Collision tie-break on equal housenumber falls through to street strcmp.
// Two addrs share coord AND housenumber "5"; the one with the lexicographically
// smaller street wins. Both map to housenumber 5 so the resolved NUMBER is the
// same — this test mainly guards that equal-housenumber collisions don't crash
// and still resolve (exercising the street strcmp branch).
TEST(interpolation_collision_equal_housenumber_street_tiebreak) {
    ParsedData d;
    uint32_t st_a = S(d, "Aardvark St");
    uint32_t st_b = S(d, "Zebra St");
    uint32_t hn5 = S(d, "5");
    add_addr(d, 7.0f, 8.0f, hn5, st_b);   // inserted first
    add_addr(d, 7.0f, 8.0f, hn5, st_a);   // smaller street, should win compare

    uint32_t wst = S(d, "Way St");
    add_interp(d, {{7.0f, 8.0f}, {7.0f, 8.0f}}, wst, 0u, 0u);
    resolve_interpolation_endpoints(d);

    CHECK_EQ(d.interp_ways[0].start_number, 5u);
    CHECK_EQ(d.interp_ways[0].end_number, 5u);
}

// Collision where one colliding addr has street_id == NO_DATA: the addr_less
// guard compares raw street ids instead of dereferencing. This must not crash
// and must still resolve a number. (NO_DATA == 0xFFFFFFFF is the max uint32,
// so when comparing raw ids the NO_DATA side is "larger".)
TEST(interpolation_collision_no_data_street_no_crash) {
    ParsedData d;
    uint32_t hn3 = S(d, "3");
    uint32_t hn3b = S(d, "3");  // same string interns to same offset
    uint32_t st = S(d, "Real St");
    // Two addrs, same coord, same housenumber "3"; one has NO_DATA street.
    add_addr(d, 4.0f, 4.0f, hn3, NO_DATA);
    add_addr(d, 4.0f, 4.0f, hn3b, st);

    add_interp(d, {{4.0f, 4.0f}, {4.0f, 4.0f}}, st, 0u, 0u);
    resolve_interpolation_endpoints(d);

    // Whichever wins, housenumber is "3" -> 3. The point is: no OOB deref.
    CHECK_EQ(d.interp_ways[0].start_number, 3u);
    CHECK_EQ(d.interp_ways[0].end_number, 3u);
}

// A way with no addr matches at all (empty addr set) leaves both seeds intact.
TEST(interpolation_no_addr_points_leaves_seeds) {
    ParsedData d;
    uint32_t st = S(d, "Lonely Way");
    add_interp(d, {{40.0f, 40.0f}, {41.0f, 41.0f}}, st, 111u, 222u);
    resolve_interpolation_endpoints(d);

    CHECK_EQ(d.interp_ways[0].start_number, 111u);
    CHECK_EQ(d.interp_ways[0].end_number, 222u);
}

// Multi-node (>2) way: only the FIRST and LAST nodes are consulted; an
// intermediate node coinciding with an addr_point is ignored.
TEST(interpolation_uses_first_and_last_node_only) {
    ParsedData d;
    uint32_t st = S(d, "Long Way");
    uint32_t hnFirst = S(d, "100");
    uint32_t hnMid = S(d, "555");
    uint32_t hnLast = S(d, "200");
    add_addr(d, 1.0f, 1.0f, hnFirst, st);   // first node
    add_addr(d, 1.5f, 1.5f, hnMid, st);     // middle node — should be ignored
    add_addr(d, 2.0f, 2.0f, hnLast, st);    // last node

    add_interp(d, {{1.0f, 1.0f}, {1.5f, 1.5f}, {2.0f, 2.0f}}, st, 0u, 0u);
    resolve_interpolation_endpoints(d);

    CHECK_EQ(d.interp_ways[0].start_number, 100u);
    CHECK_EQ(d.interp_ways[0].end_number, 200u);
}
