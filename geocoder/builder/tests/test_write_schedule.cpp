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

TEST(continent_share_gives_the_waters_what_their_share_holds_and_leaves_them_out_of_the_admins) {
    KindValues planet = kinds(1000, 2000, 300, 400, 100, 600), planet_pairs = kinds(100, 200, 30, 40, 10, 0),
               continent = kinds(50, 20, 0, 40, 1, 0);
    at(planet, RecordKind::Waters) = 5000;
    // The waters take the bytes given them, whatever share of the water
    // cells the continent holds (a polygon of one of its cells comes whole);
    // none given, none.
    at(planet_pairs, RecordKind::Waters) = 1000;
    at(continent, RecordKind::Waters) = 10;
    const KindValues share = continent_share(planet, planet_pairs, continent, 3000);
    CHECK_EQ(at(share, RecordKind::Waters), uint64_t(3000));
    CHECK_EQ(at(continent_share(planet, planet_pairs, continent), RecordKind::Waters), uint64_t(0));
    // The admins' share is the other kinds', the waters left out.
    CHECK_EQ(at(share, RecordKind::Admins), uint64_t(600 * 1110 / 3800 * 110 / 100));
    CHECK((continent_share(planet, planet_pairs, continent, 3000) == [&] {
        KindValues without = continent_share(planet, kinds(100, 200, 30, 40, 10, 0), kinds(50, 20, 0, 40, 1, 0));
        at(without, RecordKind::Waters) = 3000;
        return without;
    }()));
}

TEST(the_water_stage_holds_three_times_its_cells_beside_its_share_of_the_rest) {
    // A layer of 1 GiB, a quarter of it its 16 Mi cell entries of 16 bytes.
    KindValues bytes = kinds(0, 0, 0, 0, 0, 0);
    at(bytes, RecordKind::Waters) = kGiB;
    const uint64_t pairs = kGiB / 4 / sizeof(CellItemPair);
    // The cells renumbered, collected in parts beside the copy and sorted
    // through a buffer: three times them, on top of the share of the rest.
    CHECK_EQ(stage_costs(bytes, pairs).water, 3 * kGiB / 4 * kWaterStageWaterTenths / 10 + 3 * (kGiB / 4));
    CHECK_EQ(stage_costs(bytes).water, kGiB * kWaterStageWaterTenths / 10);
    // A layer of cells alone needs three times itself, past the share alone.
    const uint64_t all = kGiB / sizeof(CellItemPair);
    CHECK_EQ(stage_costs(bytes, all).water, 3 * kGiB);
    CHECK(stage_costs(bytes, all).water > stage_costs(bytes).water);
    // The reservations carry it.
    const KindValues counts = kinds(0, 0, 0, 0, 0, 0);
    CHECK_EQ(region_peak(bytes, counts, 0, RunOrder::Serial, all), kGiB + 3 * kGiB);
    // At once, the water stage beside the three modes; a byte short of that,
    // one after another, the water stage the largest.
    const uint64_t at_once = kGiB + 3 * kGiB + 3 * kModeBytes;
    const RegionCost fits = continent_cost(bytes, counts, 0, at_once, all);
    CHECK_EQ(fits.bytes, at_once);
    CHECK(fits.order == RunOrder::Concurrent);
    const RegionCost short_of_it = continent_cost(bytes, counts, 0, at_once - 1, all);
    CHECK_EQ(short_of_it.bytes, kGiB + 3 * kGiB);
    CHECK(short_of_it.order == RunOrder::Serial);
}

TEST(the_water_stage_holds_its_slot_arrays_for_every_slot_the_previous_build_left_tombstones_among_them) {
    // A layer of 1 GiB over a previous build of 3 Mi slots: the write's
    // records at their slots, each slot's polygon and the records it
    // writes, 40 B a slot, or the slot allocation's working set of it where
    // that is more, past the share of the layer today's polygons take.
    KindValues bytes = kinds(0, 0, 0, 0, 0, 0);
    at(bytes, RecordKind::Waters) = kGiB;
    const uint64_t pairs = kGiB / 4 / sizeof(CellItemPair), slots = uint64_t(3) << 20;
    CHECK(kWaterSlotBytes >= sizeof(AdminPolygon) + sizeof(uint32_t) + sizeof(AdminPolygonRecord));
    CHECK_EQ(kWaterPreviousSlotBytes, std::max(kWaterSlotBytes, kWaterSlotAllocationBytes));
    CHECK_EQ(stage_costs(bytes, pairs, slots).water, stage_costs(bytes, pairs).water + slots * kWaterPreviousSlotBytes);
    // The reservations carry it.
    const KindValues counts = kinds(0, 0, 0, 0, 0, 0);
    CHECK_EQ(region_peak(bytes, counts, 0, RunOrder::Serial, pairs, slots),
             region_peak(bytes, counts, 0, RunOrder::Serial, pairs) + slots * kWaterPreviousSlotBytes);
    const RegionCost at_once = continent_cost(bytes, counts, 0, UINT64_MAX, pairs, slots);
    CHECK(at_once.order == RunOrder::Concurrent);
    CHECK_EQ(at_once.bytes,
             region_peak(bytes, counts, 0, RunOrder::Concurrent, pairs) + slots * kWaterPreviousSlotBytes);
}

TEST(the_water_stage_holds_the_polygon_slot_allocation_of_each_slot_the_previous_build_left_past_a_small_layer) {
    // A layer of 1 MiB over a previous build of 3 Mi slots, few polygons of
    // it left today: the polygon slot allocation holds, for each slot, the
    // slot as loaded, its key among the previous keys, a fallback pass's key
    // of it and the sort's copy of them, its fallback key and its marks,
    // more than the write's arrays of it, which come once it is done.
    KindValues bytes = kinds(0, 0, 0, 0, 0, 0);
    at(bytes, RecordKind::Waters) = uint64_t(1) << 20;
    const uint64_t slots = uint64_t(3) << 20;
    const uint64_t allocation =
        sizeof(gc::id_alloc::SidecarSlot) + 3 * sizeof(gc::id_alloc::KeyIdx) + sizeof(uint64_t) + 2;
    CHECK(allocation > sizeof(AdminPolygon) + sizeof(uint32_t) + sizeof(AdminPolygonRecord));
    CHECK(stage_costs(bytes, 0, slots).water >= stage_costs(bytes).water + slots * allocation);
    // The reservations carry it beside the other stages.
    const KindValues counts = kinds(0, 0, 0, 0, 0, 0);
    CHECK(region_peak(bytes, counts, 0, RunOrder::Concurrent, 0, slots) >=
          region_peak(bytes, counts, 0, RunOrder::Concurrent) + slots * allocation);
}

TEST(a_build_without_multi_output_holds_the_previous_arc_slots_of_the_all_set_past_todays_and_none_of_the_major_tiers) {
    // Its continents' arc slots load the all set's sidecar of a previous
    // build that wrote the water dirs; it writes no major tier, so loads
    // none of that tier's.
    using water::ArcCounts;
    const uint64_t Mi = uint64_t(1) << 20;
    CHECK_EQ(water_arc_slots_past(ArcCounts{5 * Mi, 3 * Mi}, ArcCounts{Mi, Mi / 10}, false), 4 * Mi);
    CHECK_EQ(water_arc_slots_past(ArcCounts{Mi, 3 * Mi}, ArcCounts{Mi, Mi / 10}, false), uint64_t(0));
    CHECK_EQ(water_arc_slots_past(ArcCounts{Mi, 3 * Mi}, ArcCounts{Mi, Mi / 10}, true), 3 * Mi - Mi / 10);
    CHECK_EQ(water_arc_slots_past(ArcCounts{Mi, Mi / 10}, ArcCounts{Mi + 5, Mi / 10}, false), uint64_t(0));
}

TEST(the_water_stage_holds_the_slot_allocation_of_each_previous_arc_slot_past_todays_arcs) {
    // A previous build of 5 Mi all-set and 3 Mi major arc slots over a
    // layer of 1 Mi arcs, 0.1 Mi of them the major tier's: the all set's
    // 4 Mi past today's, the larger, as the two allocations run one after
    // the other.
    using water::ArcCounts;
    const uint64_t Mi = uint64_t(1) << 20;
    CHECK_EQ(water_arc_slots_past(ArcCounts{5 * Mi, 3 * Mi}, ArcCounts{Mi, Mi / 10}), 4 * Mi);
    CHECK_EQ(water_arc_slots_past(ArcCounts{Mi, 3 * Mi}, ArcCounts{Mi, Mi / 10}), 3 * Mi - Mi / 10);
    // As many slots as arcs, or fewer (water gained since): none past them.
    CHECK_EQ(water_arc_slots_past(ArcCounts{Mi, Mi / 10}, ArcCounts{Mi, Mi / 10}), uint64_t(0));
    CHECK_EQ(water_arc_slots_past(ArcCounts{Mi, Mi / 10}, ArcCounts{Mi + 5, Mi / 10 + 3}), uint64_t(0));
    CHECK_EQ(water_arc_slots_past(ArcCounts{}, ArcCounts{Mi, Mi / 10}), uint64_t(0));
    // Each slot past them: the slot as loaded and its key, the previous keys
    // and their sort's copy, its fallback keys, a fallback pass's keys and
    // their sort's copy, and its marks.
    CHECK(kWaterArcSlotBytes >= sizeof(gc::id_alloc::SidecarSlot) + sizeof(uint64_t) + 4 * sizeof(gc::id_alloc::KeyIdx) +
                                    sizeof(ArcFallbackKeys) + 2);
    KindValues bytes = kinds(0, 0, 0, 0, 0, 0);
    at(bytes, RecordKind::Waters) = kGiB;
    const uint64_t pairs = kGiB / 4 / sizeof(CellItemPair), slots = uint64_t(3) << 20, past = 4 * Mi;
    CHECK_EQ(stage_costs(bytes, pairs, slots, past).water, stage_costs(bytes, pairs, slots).water + past * kWaterArcSlotBytes);
    CHECK_EQ(stage_costs(bytes, pairs, slots, 0).water, stage_costs(bytes, pairs, slots).water);
    // The reservations carry it.
    const KindValues counts = kinds(0, 0, 0, 0, 0, 0);
    CHECK_EQ(region_peak(bytes, counts, 0, RunOrder::Serial, pairs, slots, past),
             region_peak(bytes, counts, 0, RunOrder::Serial, pairs, slots) + past * kWaterArcSlotBytes);
    const RegionCost at_once = continent_cost(bytes, counts, 0, UINT64_MAX, pairs, slots, past);
    CHECK(at_once.order == RunOrder::Concurrent);
    CHECK_EQ(at_once.bytes,
             region_peak(bytes, counts, 0, RunOrder::Concurrent, pairs, slots) + past * kWaterArcSlotBytes);
    // A byte short of that, the steps run one after another, and the water
    // stage still holds the slots past today's arcs.
    const RegionCost one_by_one = continent_cost(bytes, counts, 0, at_once.bytes - 1, pairs, slots, past);
    CHECK(one_by_one.order == RunOrder::Serial);
    CHECK_EQ(one_by_one.bytes,
             region_peak(bytes, counts, 0, RunOrder::Serial, pairs, slots) + past * kWaterArcSlotBytes);
}

TEST(pair_counts_takes_the_water_cell_index_entries_for_the_waters_pairs) {
    ParsedData d;
    d.sorted_way_cells.resize(3);
    d.water.cells.resize(7);
    const KindValues n = pair_counts(d);
    CHECK_EQ(at(n, RecordKind::Ways), uint64_t(3));
    CHECK_EQ(at(n, RecordKind::Waters), uint64_t(7));
    CHECK_EQ(at(n, RecordKind::Admins), uint64_t(0));
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

TEST(filter_cost_holds_the_continents_water_pairs_among_the_pairs_the_workers_copy_and_marks_of_the_planets_water) {
    // The water split reads the planet's water masks, which the build holds
    // with the other kinds' while the continents run, and marks each of the
    // planet's water polygons and arcs, ranked, as the other kinds' records
    // are marked: 3 bits each, and no id map. The planet's counts as the
    // build takes them, of a planet of 40 water records over 60 arcs.
    ParsedData d;
    d.ways.resize(80);
    d.addr_points.resize(160);
    d.water.records.resize(40);
    const KindValues planet_counts = record_counts(d);
    KindValues pairs = kinds(10, 0, 0, 0, 0, 0);
    at(pairs, RecordKind::Waters) = 40;
    CHECK_EQ(filter_cost(planet_counts, pairs), uint64_t(240 * 4 + 240 * 3 / 8 + 50 * 16 / 2));
    CHECK_EQ(filter_cost(planet_counts, pairs, 40, 60), uint64_t(240 * 4 + (240 + 100) * 3 / 8 + 50 * 16 / 2));
    // A planet of water alone: its marks, no map.
    CHECK_EQ(filter_cost(kinds(0, 0, 0, 0, 0, 0), kinds(0, 0, 0, 0, 0, 0), 1000, 3000), uint64_t(4000 * 3 / 8));
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
    // The boundary tiers and both admin-minimal dirs, which write the arcs;
    // an admin-minimal dir also holds its place nodes and their writer's.
    CHECK_EQ(stages.admin_tiers, 24 * kGiB / kAdminTierAdminDivisor);
    CHECK_EQ(stages.admin_minimal, 24 * kGiB / kAdminMinimalAdminDivisor + kGiB / 8 * (1 + kPlaceWriterTimes));
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
    CHECK_EQ(stages.admin_minimal, at(bytes, RecordKind::Admins) / kAdminMinimalAdminDivisor +
                                       at(bytes, RecordKind::Places) * (1 + kPlaceWriterTimes));
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

TEST(the_place_writers_working_set_is_in_the_place_stage_and_in_admin_minimal) {
    const KindValues counts = kinds(0, 0, 0, 0, 0, 0);
    const KindValues without = kinds(kGiB, 0, 0, 0, 0, 3 * kGiB), with = kinds(kGiB, 0, 0, 0, kGiB / 4, 3 * kGiB);
    const StageCosts bare = stage_costs(without), stages = stage_costs(with);
    CHECK_EQ(bare.places, uint64_t(0));
    // The keys, the order and the three encoded files beside the nodes.
    CHECK_EQ(stages.places, kGiB / 4 * kPlaceWriterTimes);
    // An admin-minimal dir's own copy of the nodes, and the writer's.
    CHECK_EQ(stages.admin_minimal - bare.admin_minimal, kGiB / 4 * (1 + kPlaceWriterTimes));
    CHECK_EQ(stages.total() - bare.total(), kGiB / 4 * (1 + 2 * kPlaceWriterTimes));
    // Both reach a region's peak: at once as the sum, one at a time as the
    // largest stage, which the place nodes make admin-minimal here.
    CHECK_EQ(region_peak(with, counts, 0, RunOrder::Concurrent), sum(with) + stages.total());
    CHECK_EQ(region_peak(with, counts, 0, RunOrder::Concurrent) - region_peak(without, counts, 0, RunOrder::Concurrent),
             kGiB / 4 + kGiB / 4 * (1 + 2 * kPlaceWriterTimes));
    REQUIRE(stages.largest() == stages.admin_minimal && bare.largest() == bare.admin_minimal);
    CHECK_EQ(region_peak(with, counts, 0, RunOrder::Serial), sum(with) + stages.admin_minimal);
    // Place nodes alone: both stages grow with them, admin-minimal's the larger by its copy.
    const KindValues only = kinds(0, 0, 0, 0, 4 * kGiB, 0);
    CHECK_EQ(stage_costs(only).places, 4 * kGiB * kPlaceWriterTimes);
    CHECK_EQ(stage_costs(only).largest(), 4 * kGiB * (1 + kPlaceWriterTimes));
}

TEST(region_peak_counts_the_records_and_the_largest_step) {
    const KindValues bytes = kinds(kGiB, 0, 0, 0, 0, 0);
    const KindValues counts = kinds(0, 0, 0, 0, 0, 0);
    // The split's scratch past every other step.
    CHECK_EQ(region_peak(bytes, counts, 10 * kGiB, RunOrder::Serial), 11 * kGiB);
    CHECK_EQ(region_peak(bytes, counts, 0, RunOrder::Serial),
             kGiB + stage_costs(bytes).largest());
}
