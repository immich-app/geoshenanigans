// Unit tests for the POI tier selection and poi_meta_categories (parsed_data.h).
#include "parsed_data.h"

#include <algorithm>
#include <stdexcept>

#include "test_framework.h"

using gc::id_alloc::ObjectType;
using gc::id_alloc::SidecarSlot;
using gc::id_alloc::SLOT_FLAG_TOMBSTONE;
using gctest::refusal_of;

static PoiRecord poi(PoiCategory category, uint8_t tier) {
    PoiRecord pr{};
    pr.category = static_cast<uint8_t>(category);
    pr.tier = tier;
    return pr;
}

static SidecarSlot live_slot(uint8_t tier) {
    SidecarSlot s{};
    s.object_type = static_cast<uint8_t>(ObjectType::OSM_NODE);
    s.tier = tier;
    return s;
}

// A tombstone record is memset 0, so it reads as a tier-0 MUSEUM; its slot
// remembers the tier of the record that last held it.
static SidecarSlot tomb_slot(uint8_t tier) {
    SidecarSlot s{};
    s.object_type = static_cast<uint8_t>(ObjectType::NONE);
    s.flags = SLOT_FLAG_TOMBSTONE;
    s.tier = tier;
    return s;
}

constexpr uint8_t MAJOR = 1;
constexpr uint8_t NOTABLE = 2;
constexpr uint8_t ALL = POI_MAX_SHIPPED_TIER;

// Mixed set: live records of tiers 1..4, tombstones of tiers 0, 1, 3 and 4.
static const std::vector<PoiRecord> mixed_records = {
    poi(PoiCategory::HOTEL, 1),    // 0 live major
    PoiRecord{},                   // 1 tombstone, tier 3
    poi(PoiCategory::ZOO, 3),      // 2 live all
    PoiRecord{},                   // 3 tombstone, tier 0 (unknown)
    poi(PoiCategory::CINEMA, 4),   // 4 live, unshipped
    PoiRecord{},                   // 5 tombstone, tier 4
    poi(PoiCategory::THEATRE, 2),  // 6 live notable
    PoiRecord{},                   // 7 tombstone, tier 1
};
static const std::vector<SidecarSlot> mixed_slots = {
    live_slot(1), tomb_slot(3), live_slot(3), tomb_slot(0),
    live_slot(4), tomb_slot(4), live_slot(2), tomb_slot(1),
};

static bool selects(const PoiTierSelection& s, uint32_t idx) {
    return std::find(s.indices.begin(), s.indices.end(), idx) != s.indices.end();
}

// --- select_poi_tier ---

TEST(select_poi_tier_ships_a_tombstone_only_in_tiers_that_held_its_record) {
    const PoiTierSelection major = select_poi_tier(mixed_records, mixed_slots, MAJOR);
    const PoiTierSelection notable = select_poi_tier(mixed_records, mixed_slots, NOTABLE);
    const PoiTierSelection all = select_poi_tier(mixed_records, mixed_slots, ALL);
    CHECK(major.indices == std::vector<uint32_t>({0, 3, 7}));
    CHECK(notable.indices == std::vector<uint32_t>({0, 3, 6, 7}));
    CHECK(all.indices == std::vector<uint32_t>({0, 1, 2, 3, 6, 7}));
    CHECK_EQ(major.tombstones, size_t(2));
    CHECK_EQ(notable.tombstones, size_t(2));
    CHECK_EQ(all.tombstones, size_t(3));
}

TEST(select_poi_tier_tombstone_placement_by_remembered_tier) {
    struct Case { const char* name; uint32_t idx; bool major; bool notable; bool all; };
    const Case cases[] = {
        {"tier-3 tombstone", 1, false, false, true},
        {"tier-0 tombstone", 3, true, true, true},
        {"tier-4 tombstone", 5, false, false, false},
        {"tier-1 tombstone", 7, true, true, true},
    };
    const PoiTierSelection major = select_poi_tier(mixed_records, mixed_slots, MAJOR);
    const PoiTierSelection notable = select_poi_tier(mixed_records, mixed_slots, NOTABLE);
    const PoiTierSelection all = select_poi_tier(mixed_records, mixed_slots, ALL);
    for (const auto& c : cases) {
        CHECK_EQ(selects(major, c.idx), c.major);
        CHECK_EQ(selects(notable, c.idx), c.notable);
        CHECK_EQ(selects(all, c.idx), c.all);
    }
}

TEST(select_poi_tier_without_slot_table_filters_by_record_tier) {
    const std::vector<PoiRecord> records = {poi(PoiCategory::HOTEL, 1), poi(PoiCategory::ZOO, 3),
                                            poi(PoiCategory::CINEMA, 4)};
    const PoiTierSelection major = select_poi_tier(records, {}, MAJOR);
    const PoiTierSelection all = select_poi_tier(records, {}, ALL);
    CHECK(major.indices == std::vector<uint32_t>({0}));
    CHECK(all.indices == std::vector<uint32_t>({0, 1}));
    CHECK_EQ(all.tombstones, size_t(0));
}

TEST(select_poi_tier_throws_when_slot_table_is_not_parallel) {
    const std::vector<PoiRecord> records = {poi(PoiCategory::HOTEL, 1), poi(PoiCategory::ZOO, 3)};
    const std::vector<SidecarSlot> slots = {live_slot(1)};
    CHECK_EQ(refusal_of([&] { select_poi_tier(records, slots, ALL); }),
             std::string("POI slot table not parallel to poi_records"));
}

// --- poi_meta_categories ---

TEST(poi_meta_categories_skips_tombstones) {
    const std::vector<PoiRecord> records = {poi(PoiCategory::HOTEL, 1), PoiRecord{}};
    const std::vector<SidecarSlot> slots = {live_slot(1), tomb_slot(1)};
    CHECK(poi_meta_categories(records, slots, select_poi_tier(records, slots, MAJOR)) ==
          std::set<uint8_t>({uint8_t(PoiCategory::HOTEL)}));
}

TEST(poi_meta_categories_lists_live_records_the_tier_ships) {
    CHECK(poi_meta_categories(mixed_records, mixed_slots,
                              select_poi_tier(mixed_records, mixed_slots, MAJOR)) ==
          std::set<uint8_t>({uint8_t(PoiCategory::HOTEL)}));
    CHECK(poi_meta_categories(mixed_records, mixed_slots,
                              select_poi_tier(mixed_records, mixed_slots, NOTABLE)) ==
          std::set<uint8_t>({uint8_t(PoiCategory::HOTEL), uint8_t(PoiCategory::THEATRE)}));
    CHECK(poi_meta_categories(mixed_records, mixed_slots,
                              select_poi_tier(mixed_records, mixed_slots, ALL)) ==
          std::set<uint8_t>({uint8_t(PoiCategory::HOTEL), uint8_t(PoiCategory::ZOO),
                             uint8_t(PoiCategory::THEATRE)}));
}

TEST(poi_meta_categories_match_the_live_records_the_tier_writes) {
    // poi_meta.json and poi_records.bin come from the same selection, so
    // every listed category has a live record in the tier and vice versa.
    for (uint8_t max_tier : {MAJOR, NOTABLE, ALL}) {
        const PoiTierSelection selection = select_poi_tier(mixed_records, mixed_slots, max_tier);
        std::set<uint8_t> written;
        for (uint32_t i : selection.indices) {
            if (gc::id_alloc::is_tombstone(mixed_slots[i])) continue;

            CHECK(mixed_records[i].tier <= max_tier);
            written.insert(mixed_records[i].category);
        }
        CHECK(poi_meta_categories(mixed_records, mixed_slots, selection) == written);
        CHECK(!written.count(uint8_t(PoiCategory::MUSEUM)));
    }
}

TEST(poi_meta_categories_without_slot_table_counts_every_record) {
    const std::vector<PoiRecord> records = {poi(PoiCategory::HOTEL, 1), poi(PoiCategory::ZOO, 3)};
    CHECK(poi_meta_categories(records, {}, select_poi_tier(records, {}, ALL)) ==
          std::set<uint8_t>({uint8_t(PoiCategory::HOTEL), uint8_t(PoiCategory::ZOO)}));
}

TEST(a_water_object_of_a_less_important_category_ships_as_its_water_kind_in_the_tiers_between) {
    // A bay (tier 2) that is a pier (tier 3), and a glacier (tier 1) that is
    // an attraction (tier 2), of the highway flag: each ships from its water
    // kind's tier on, as that kind where its category doesn't ship, of no
    // water bits anywhere.
    PoiRecord pier = poi(PoiCategory::PIER, 3);
    pier.flags = poi_flags_with_water(0, PoiCategory::BAY);
    PoiRecord attraction = poi(PoiCategory::ATTRACTION, 2);
    attraction.flags = poi_flags_with_water(POI_FLAG_HIGHWAY, PoiCategory::GLACIER);
    const std::vector<PoiRecord> records = {pier, attraction, poi(PoiCategory::ZOO, 3)};
    CHECK_EQ(int(poi_lowest_tier(pier)), 2);
    CHECK_EQ(int(poi_lowest_tier(attraction)), 1);
    CHECK_EQ(int(poi_lowest_tier(records[2])), 3);

    const PoiTierSelection major = select_poi_tier(records, {}, MAJOR);
    const PoiTierSelection notable = select_poi_tier(records, {}, NOTABLE);
    const PoiTierSelection all = select_poi_tier(records, {}, ALL);
    CHECK(major.indices == std::vector<uint32_t>({1}));
    CHECK(notable.indices == std::vector<uint32_t>({0, 1}));
    CHECK(all.indices == std::vector<uint32_t>({0, 1, 2}));
    CHECK(poi_meta_categories(records, {}, major) == std::set<uint8_t>({uint8_t(PoiCategory::GLACIER)}));
    CHECK(poi_meta_categories(records, {}, notable) ==
          std::set<uint8_t>({uint8_t(PoiCategory::BAY), uint8_t(PoiCategory::ATTRACTION)}));
    CHECK(poi_meta_categories(records, {}, all) ==
          std::set<uint8_t>({uint8_t(PoiCategory::PIER), uint8_t(PoiCategory::ATTRACTION), uint8_t(PoiCategory::ZOO)}));

    struct Want {
        uint8_t max_tier;
        PoiCategory category;
        uint8_t tier;
    };
    for (const Want& w : {Want{NOTABLE, PoiCategory::BAY, 2}, Want{ALL, PoiCategory::PIER, 3}}) {
        const PoiRecord written = poi_record_in_tier(pier, w.max_tier);
        CHECK_EQ(int(written.category), int(w.category));
        CHECK_EQ(int(written.tier), int(w.tier));
        CHECK_EQ(int(written.flags), 0);
    }
    const PoiRecord glacier = poi_record_in_tier(attraction, MAJOR);
    CHECK_EQ(int(glacier.category), int(PoiCategory::GLACIER));
    CHECK_EQ(int(glacier.tier), 1);
    CHECK_EQ(int(glacier.flags), int(POI_FLAG_HIGHWAY));
    CHECK_EQ(int(poi_record_in_tier(attraction, NOTABLE).category), int(PoiCategory::ATTRACTION));
    CHECK_EQ(int(poi_record_in_tier(attraction, NOTABLE).tier), 2);
    CHECK_EQ(int(poi_record_in_tier(attraction, NOTABLE).flags), int(POI_FLAG_HIGHWAY));

    // A tombstone of the pier reaches the tier its slot remembers, notable.
    const std::vector<PoiRecord> dead = {PoiRecord{}};
    const std::vector<SidecarSlot> slot = {tomb_slot(poi_lowest_tier(pier))};
    CHECK(select_poi_tier(dead, slot, NOTABLE).indices == std::vector<uint32_t>({0}));
    CHECK(select_poi_tier(dead, slot, MAJOR).indices.empty());
}

TEST(a_reservoir_point_a_tier_ships_in_place_of_its_poi_links_no_parent_street) {
    // A generic rank-30 point of a wiki tag (tier 3) that is a reservoir
    // (tier 2), linked to its parent street: poi/notable ships the
    // RESERVOIR in its place, which never takes the primary slot, of no
    // parent street, and poi/all the POI, of its parent street. A reservoir
    // area, which the server keeps out of the lanes, keeps it, as a bay
    // point does.
    PoiRecord point = poi(PoiCategory::GENERIC_RANK30, 3);
    point.flags = poi_flags_with_water(POI_FLAG_WIKIDATA, PoiCategory::RESERVOIR);
    point.parent_street_id = 7;
    const PoiRecord reservoir = poi_record_in_tier(point, NOTABLE);
    CHECK_EQ(int(reservoir.category), int(PoiCategory::RESERVOIR));
    CHECK_EQ(int(reservoir.tier), 2);
    CHECK_EQ(reservoir.parent_street_id, NO_DATA);
    const PoiRecord generic = poi_record_in_tier(point, ALL);
    CHECK_EQ(int(generic.category), int(PoiCategory::GENERIC_RANK30));
    CHECK_EQ(generic.parent_street_id, uint32_t(7));

    PoiRecord area = point;
    area.vertex_count = 5;
    CHECK_EQ(poi_record_in_tier(area, NOTABLE).parent_street_id, uint32_t(7));
    PoiRecord bay = point;
    bay.flags = poi_flags_with_water(POI_FLAG_WIKIDATA, PoiCategory::BAY);
    CHECK_EQ(int(poi_record_in_tier(bay, NOTABLE).category), int(PoiCategory::BAY));
    CHECK_EQ(poi_record_in_tier(bay, NOTABLE).parent_street_id, uint32_t(7));
}

TEST(poi_flags_carry_each_water_kind_and_none) {
    CHECK(poi_water_of(0) == PoiCategory::UNKNOWN);
    CHECK(poi_water_of(POI_FLAG_WIKIPEDIA | POI_FLAG_WIKIDATA | POI_FLAG_HIGHWAY) == PoiCategory::UNKNOWN);
    for (const PoiCategory kind : kPoiWaterKinds) {
        const uint8_t flags = poi_flags_with_water(POI_FLAG_HIGHWAY, kind);
        CHECK(poi_water_of(flags) == kind);
        CHECK_EQ(int(flags & ~POI_FLAG_WATER_MASK), int(POI_FLAG_HIGHWAY));
    }
    CHECK_EQ(refusal_of<std::logic_error>([] { poi_flags_with_water(0, PoiCategory::PIER); }),
             std::string("poi_flags_with_water: no water kind"));
}

TEST(select_poi_tier_and_categories_match_a_serial_walk_of_a_large_set) {
    // Past the blocks the selection splits its work into.
    std::vector<PoiRecord> records(300000);
    std::vector<SidecarSlot> slots(records.size());
    for (size_t i = 0; i < records.size(); i++) {
        const uint8_t tier = static_cast<uint8_t>(1 + (i * 7919) % 4);
        if (i % 13 == 0) {
            slots[i] = tomb_slot(static_cast<uint8_t>((i / 13) % 5));
        } else {
            records[i] = poi(static_cast<PoiCategory>((i * 31) % 40), tier);
            slots[i] = live_slot(tier);
        }
    }
    for (uint8_t max_tier : {MAJOR, NOTABLE, ALL}) {
        std::vector<uint32_t> indices;
        size_t tombstones = 0;
        std::set<uint8_t> cats;
        for (size_t i = 0; i < records.size(); i++) {
            if (gc::id_alloc::poi_shipped_tier(records[i].tier, &slots[i]) > max_tier) continue;

            indices.push_back(static_cast<uint32_t>(i));
            if (gc::id_alloc::is_tombstone(slots[i])) tombstones++;
            else cats.insert(records[i].category);
        }
        const PoiTierSelection selection = select_poi_tier(records, slots, max_tier);
        CHECK(selection.indices == indices);
        CHECK_EQ(selection.tombstones, tombstones);
        CHECK(poi_meta_categories(records, slots, selection) == cats);
    }
}
