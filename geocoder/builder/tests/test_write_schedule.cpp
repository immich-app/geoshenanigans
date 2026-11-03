// Write schedule estimates (write_schedule.h): the budget past what the build
// holds, a continent's share of the planet, and when its steps run at once.
#include "write_schedule.h"

#include "test_framework.h"

namespace {

constexpr uint64_t kGiB = uint64_t(1) << 30;

KindValues kinds(uint64_t ways, uint64_t addrs, uint64_t interps, uint64_t pois, uint64_t places, uint64_t admins) {
    return {ways, addrs, interps, pois, places, admins};
}

}  // namespace

TEST(write_budget_leaves_the_reserve_and_what_the_build_holds) {
    CHECK_EQ(write_budget(128 * kGiB, 80 * kGiB), 40 * kGiB);  // reserve 128/16 = 8 GiB
    CHECK_EQ(write_budget(16 * kGiB, 5 * kGiB), 7 * kGiB);     // reserve at least 4 GiB
    CHECK_EQ(write_budget(16 * kGiB, 14 * kGiB), uint64_t(0));
    CHECK_EQ(write_budget(0, 14 * kGiB), UINT64_MAX);           // no known limit
}

TEST(continent_share_scales_each_kind_by_its_pairs) {
    const KindValues planet = kinds(1000, 2000, 300, 400, 100, 600);
    const KindValues planet_pairs = kinds(100, 200, 30, 40, 10, 0);
    const KindValues share = continent_share(planet, planet_pairs, kinds(50, 20, 0, 40, 1, 0));
    // With the margin; admins take the share the others add up to:
    // (500 + 200 + 400 + 10) / 3800.
    CHECK((share == kinds(550, 220, 0, 440, 11, 600 * 1110 / 3800 * 110 / 100)));
    CHECK((continent_share(planet, kinds(0, 0, 0, 0, 0, 0), kinds(0, 0, 0, 0, 0, 0)) == kinds(0, 0, 0, 0, 0, 0)));
}

TEST(strategy2_cost_holds_every_kind_or_the_largest) {
    const KindValues counts = kinds(10, 30, 5, 20, 1, 2);
    CHECK_EQ(strategy2_cost(counts, RunOrder::Concurrent), 68 * kStrategy2BytesPerRecord);
    CHECK_EQ(strategy2_cost(counts, RunOrder::Serial), 30 * kStrategy2BytesPerRecord);
}

TEST(strategy2_pass_costs_add_the_repacked_nodes_to_streets_and_interps) {
    ParsedData d;
    d.ways.resize(2);
    d.street_nodes.resize(7);
    d.addr_points.resize(3);
    d.addr_vertices.resize(11);  // addrs keep their vertices where they are
    d.interp_ways.resize(1);
    d.interp_nodes.resize(5);
    d.poi_records.resize(4);
    const KindValues costs = strategy2_pass_costs(d);
    CHECK_EQ(at(costs, RecordKind::Ways), 2 * kStrategy2BytesPerRecord + 7 * sizeof(NodeCoord));
    CHECK_EQ(at(costs, RecordKind::Addrs), 3 * kStrategy2BytesPerRecord);
    CHECK_EQ(at(costs, RecordKind::Interps), 1 * kStrategy2BytesPerRecord + 5 * sizeof(NodeCoord));
    CHECK_EQ(at(costs, RecordKind::Pois), 4 * kStrategy2BytesPerRecord);
    CHECK_EQ(at(costs, RecordKind::Places), uint64_t(0));
}

TEST(ordering_cost_copies_the_records_but_not_their_cell_pairs) {
    ParsedData d;
    d.ways.resize(2);
    d.street_nodes.resize(7);
    d.addr_points.resize(3);
    d.addr_vertices.resize(11);
    d.poi_records.resize(4);
    d.admin_polygons.resize(5);         // ordered on their own
    d.sorted_addr_cells.resize(100);    // renumbered in place
    const uint64_t copies = 2 * sizeof(WayHeader) + 7 * sizeof(NodeCoord) + 3 * sizeof(AddrPoint) +
                            11 * sizeof(NodeCoord) + 4 * sizeof(PoiRecord);
    CHECK_EQ(ordering_cost(d), copies + (2 + 3 + 4) * kReorderBytesPerRecord);
}

TEST(filter_cost_holds_the_planet_maps_and_half_the_subset_pairs) {
    const KindValues planet_counts = kinds(80, 160, 0, 0, 0, 0);
    const KindValues pairs = kinds(10, 0, 0, 0, 0, 0);
    // 240 records: 4 B id maps and 3 bits of marks, whatever the workers.
    CHECK_EQ(filter_cost(planet_counts, pairs), uint64_t(240 * 4 + 240 * 3 / 8 + 10 * 16 / 2));
}

TEST(continent_cost_runs_steps_one_at_a_time_only_past_the_budget) {
    const KindValues bytes = kinds(4 * kGiB, 8 * kGiB, kGiB, 3 * kGiB, kGiB / 8, 2 * kGiB);
    const KindValues counts = kinds(20'000'000, 100'000'000, 1'000'000, 50'000'000, 1'000'000, 400'000);
    const uint64_t at_once = region_peak(bytes, counts, 0, RunOrder::Concurrent);
    const uint64_t serial = region_peak(bytes, counts, 0, RunOrder::Serial);
    CHECK(serial < at_once);

    RegionCost roomy = continent_cost(bytes, counts, 0, at_once);
    CHECK(roomy.order == RunOrder::Concurrent);
    CHECK_EQ(roomy.bytes, at_once);
    RegionCost tight = continent_cost(bytes, counts, 0, at_once - 1);
    CHECK(tight.order == RunOrder::Serial);
    CHECK_EQ(tight.bytes, serial);
}

TEST(region_peak_holds_the_arc_geometry_stages_beside_the_steps_at_once_or_as_one_of_them_one_at_a_time) {
    // Admins well past the POIs: admin-minimal is the largest stage.
    const KindValues bytes = kinds(4 * kGiB, 8 * kGiB, kGiB, 3 * kGiB, kGiB / 8, 24 * kGiB);
    const KindValues counts = kinds(0, 0, 0, 0, 0, 0);  // no strategy-2 passes past the steps
    const StageCosts stages = stage_costs(bytes);
    // The boundary tiers and both admin-minimal dirs, which write the arcs.
    CHECK_EQ(stages.admin_tiers, 24 * kGiB / kAdminTierAdminDivisor);
    CHECK_EQ(stages.admin_minimal, 24 * kGiB / kAdminMinimalAdminDivisor);
    REQUIRE(stages.admin_minimal > std::max({stages.modes, stages.admin_tiers, stages.places, stages.poi_tiers}));
    // At once: beside every other stage.
    CHECK_EQ(region_peak(bytes, counts, 0, RunOrder::Concurrent),
             sum(bytes) + stages.modes + stages.places + stages.poi_tiers + stages.admin_tiers + stages.admin_minimal);
    // One at a time: the largest stage, here admin-minimal.
    CHECK_EQ(region_peak(bytes, counts, 0, RunOrder::Serial), sum(bytes) + stages.admin_minimal);
}

TEST(a_continents_reservation_holds_its_share_of_the_arc_geometry_stages_in_either_order) {
    const KindValues planet = kinds(4 * kGiB, 8 * kGiB, kGiB, 3 * kGiB, kGiB / 8, 24 * kGiB);
    const KindValues planet_pairs = kinds(100, 200, 30, 40, 10, 0);
    // Half of every kind's pairs: half the planet's admins, with the margin.
    const KindValues bytes = continent_share(planet, planet_pairs, kinds(50, 100, 15, 20, 5, 0));
    REQUIRE(at(bytes, RecordKind::Admins) == uint64_t(14173392076));  // 24 GiB / 2 * 110 %
    const KindValues counts = kinds(0, 0, 0, 0, 0, 0);
    const StageCosts stages = stage_costs(bytes);
    CHECK_EQ(stages.admin_tiers, at(bytes, RecordKind::Admins) / kAdminTierAdminDivisor);
    CHECK_EQ(stages.admin_minimal, at(bytes, RecordKind::Admins) / kAdminMinimalAdminDivisor);
    // Its write takes no stage leases: with room for every stage at once,
    // all are reserved, the arc geometry stages among them.
    const uint64_t at_once =
        sum(bytes) + stages.modes + stages.places + stages.poi_tiers + stages.admin_tiers + stages.admin_minimal;
    const RegionCost roomy = continent_cost(bytes, counts, 0, at_once);
    CHECK(roomy.order == RunOrder::Concurrent);
    CHECK_EQ(roomy.bytes, at_once);
    // A byte short: one at a time, admin-minimal the largest.
    const RegionCost tight = continent_cost(bytes, counts, 0, at_once - 1);
    CHECK(tight.order == RunOrder::Serial);
    CHECK_EQ(tight.bytes, sum(bytes) + stages.admin_minimal);
}

TEST(region_peak_counts_the_records_and_the_largest_step) {
    const KindValues bytes = kinds(kGiB, 0, 0, 0, 0, 0);
    const KindValues counts = kinds(0, 0, 0, 0, 0, 0);
    // The split's scratch past every other step.
    CHECK_EQ(region_peak(bytes, counts, 10 * kGiB, RunOrder::Serial), 11 * kGiB);
    CHECK_EQ(region_peak(bytes, counts, 0, RunOrder::Serial),
             kGiB + stage_costs(bytes).largest());
}
