// Unit tests for poi_meta_categories (parsed_data.h).
#include "parsed_data.h"

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

static SidecarSlot slot(ObjectType type, uint8_t flags) {
    SidecarSlot s{};
    s.object_type = static_cast<uint8_t>(type);
    s.flags = flags;
    return s;
}

static const SidecarSlot live_slot = slot(ObjectType::OSM_NODE, 0);
static const SidecarSlot tomb_slot = slot(ObjectType::NONE, SLOT_FLAG_TOMBSTONE);

// --- poi_meta_categories ---

TEST(poi_meta_categories_skips_tombstones) {
    // A tombstone record is memset 0, so it reads as a tier-0 MUSEUM.
    const std::vector<PoiRecord> records = {poi(PoiCategory::HOTEL, 1), PoiRecord{}};
    const std::vector<SidecarSlot> slots = {live_slot, tomb_slot};
    CHECK(poi_meta_categories(records, slots, 1) == std::set<uint8_t>({uint8_t(PoiCategory::HOTEL)}));
}

TEST(poi_meta_categories_lists_live_records_the_tier_ships) {
    const std::vector<PoiRecord> records = {poi(PoiCategory::MUSEUM, 1), poi(PoiCategory::HOTEL, 2)};
    const std::vector<SidecarSlot> slots = {live_slot, live_slot};
    CHECK(poi_meta_categories(records, slots, 1) == std::set<uint8_t>({uint8_t(PoiCategory::MUSEUM)}));
    CHECK(poi_meta_categories(records, slots, 2) ==
          std::set<uint8_t>({uint8_t(PoiCategory::MUSEUM), uint8_t(PoiCategory::HOTEL)}));
}

TEST(poi_meta_categories_without_slot_table_counts_every_record) {
    const std::vector<PoiRecord> records = {poi(PoiCategory::HOTEL, 1), poi(PoiCategory::ZOO, 3)};
    CHECK(poi_meta_categories(records, {}, 3) ==
          std::set<uint8_t>({uint8_t(PoiCategory::HOTEL), uint8_t(PoiCategory::ZOO)}));
}
