// Unit tests for the POI tier selection and poi_meta_categories (parsed_data.h).
#include "parsed_data.h"

#include <algorithm>
#include <stdexcept>

#include "test_framework.h"

using gc::id_alloc::ObjectType;
using gc::id_alloc::SidecarSlot;
using gc::id_alloc::SLOT_FLAG_TOMBSTONE;

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
    bool threw = false;
    try {
        select_poi_tier(records, slots, ALL);
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);
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
