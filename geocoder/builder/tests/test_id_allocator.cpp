// Unit tests for the Strategy-2 persistent id allocator (id_allocator.h).
// This is determinism-critical: it decides which dense index each stable
// identity gets across builds, which defines record byte offsets. These tests
// lock in the current behaviour (key packing, reuse, free-list recycle,
// tombstoning, sidecar round-trip) so future refactors can't silently shift it.
#include "id_allocator.h"

#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>
#include <unistd.h>

#include "scratch_dir.h"
#include "test_framework.h"

using namespace gc::id_alloc;

TEST(make_key_packs_type_in_high_byte) {
    CHECK_EQ(make_key(ObjectType::OSM_NODE, 1), (uint64_t(1) << 56) | 1);
    CHECK_EQ(make_key(ObjectType::OSM_RELATION, 42),
             (uint64_t(3) << 56) | 42);
    // id is masked to the low 56 bits; the type byte never collides with it.
    uint64_t big = 0x00FFFFFFFFFFFFFFull;
    CHECK_EQ(make_key(ObjectType::OSM_WAY, big), (uint64_t(2) << 56) | big);
    // distinct types with the same numeric id never collide.
    CHECK(make_key(ObjectType::OSM_NODE, 5) != make_key(ObjectType::OSM_WAY, 5));
}

TEST(fresh_allocate_is_sequential) {
    IdAllocator a;
    CHECK_EQ(a.allocate(ObjectType::OSM_NODE, 100), 0u);
    CHECK_EQ(a.allocate(ObjectType::OSM_NODE, 200), 1u);
    CHECK_EQ(a.allocate(ObjectType::OSM_WAY, 100), 2u);  // diff type, new slot
    CHECK_EQ(a.total_slots(), 3u);
    CHECK_EQ(a.live_count(), size_t(3));
    CHECK_EQ(a.tombstone_count(), size_t(0));
}

static std::string tmp_sidecar(const char* tag) {
    return std::string("/tmp/gctest_sidecar_") + tag + "_" +
           std::to_string(::getpid()) + ".osm_ids";
}

TEST(sidecar_roundtrip_reuses_indices) {
    const std::string path = tmp_sidecar("reuse");
    // Build 1: three records.
    std::vector<SidecarSlot> slots;
    {
        IdAllocator a;
        a.allocate(ObjectType::OSM_NODE, 10);  // idx 0
        a.allocate(ObjectType::OSM_NODE, 20);  // idx 1
        a.allocate(ObjectType::OSM_WAY, 30);   // idx 2
        a.finalize();
        slots = a.take_slots();
        IdAllocator::write_sidecar(path, slots);
    }
    CHECK_EQ(slots.size(), size_t(3));

    // Build 2 (same identities, different encounter order): each must get
    // the SAME index it had in build 1.
    IdAllocator b;
    CHECK(b.load_previous(path));
    CHECK_EQ(b.allocate(ObjectType::OSM_WAY, 30), 2u);
    CHECK_EQ(b.allocate(ObjectType::OSM_NODE, 10), 0u);
    CHECK_EQ(b.allocate(ObjectType::OSM_NODE, 20), 1u);
    b.finalize();
    CHECK_EQ(b.tombstone_count(), size_t(0));
    std::remove(path.c_str());
}

TEST(deleted_record_tombstones_then_slot_recycled) {
    const std::string path = tmp_sidecar("tomb");
    {
        IdAllocator a;
        a.allocate(ObjectType::OSM_NODE, 10);  // idx 0
        a.allocate(ObjectType::OSM_NODE, 20);  // idx 1
        a.finalize();
        IdAllocator::write_sidecar(path, a.take_slots());
    }
    // Build 2: node 20 disappears, node 99 appears. node 10 keeps idx 0;
    // node 99 recycles the freed slot left by the (eventually) tombstoned 20.
    IdAllocator b;
    CHECK(b.load_previous(path));
    CHECK_EQ(b.allocate(ObjectType::OSM_NODE, 10), 0u);
    // node 20 not consumed → its slot 1 becomes a tombstone in finalize();
    // but a NEW identity recycles a free-list slot. In build 2 the free-list
    // only contains prior tombstones (none here), so node 99 appends at idx 2.
    CHECK_EQ(b.allocate(ObjectType::OSM_NODE, 99), 2u);
    b.finalize();
    // slot 1 (node 20) is now a tombstone.
    CHECK_EQ(b.total_slots(), 3u);
    CHECK_EQ(b.live_count(), size_t(2));
    CHECK_EQ(b.tombstone_count(), size_t(1));

    // Build 3: a brand-new identity should recycle the tombstoned slot 1.
    std::vector<SidecarSlot> s2 = b.take_slots();
    const std::string path3 = tmp_sidecar("tomb3");
    IdAllocator::write_sidecar(path3, s2);
    IdAllocator c;
    CHECK(c.load_previous(path3));
    CHECK_EQ(c.allocate(ObjectType::OSM_NODE, 10), 0u);   // reuse
    CHECK_EQ(c.allocate(ObjectType::OSM_NODE, 99), 2u);   // reuse
    CHECK_EQ(c.allocate(ObjectType::OSM_NODE, 77), 1u);   // recycle tombstone
    std::remove(path.c_str());
    std::remove(path3.c_str());
}

TEST(load_previous_missing_file_returns_false) {
    IdAllocator a;
    CHECK(!a.load_previous("/tmp/gctest_does_not_exist_zzz.osm_ids"));
    // Falls back to fresh allocation.
    CHECK_EQ(a.allocate(ObjectType::OSM_NODE, 1), 0u);
}

TEST(unclaimed_live_none_slot_becomes_tombstone) {
    // A continent POI whose planet osm id was missing allocates {NONE, 0}:
    // a live slot that load_previous puts on the free list. If nothing
    // claims it the next day it is dead, and only the flag says so.
    ScratchDir dir("gctest-none");
    const std::string path = dir.path() + "/prev.osm_ids";
    {
        IdAllocator a;
        a.allocate(ObjectType::NONE, 0);      // idx 0
        a.allocate(ObjectType::NONE, 0);      // idx 1
        a.allocate(ObjectType::OSM_NODE, 10); // idx 2
        a.finalize();
        IdAllocator::write_sidecar(path, a.take_slots());
    }
    IdAllocator b;
    REQUIRE(b.load_previous(path));
    CHECK_EQ(b.allocate(ObjectType::NONE, 0), 1u);       // recycles the free-list back
    CHECK_EQ(b.allocate(ObjectType::OSM_NODE, 10), 2u);
    b.finalize();
    const std::vector<SidecarSlot> slots = b.take_slots();
    REQUIRE(slots.size() == 3u);
    CHECK(is_tombstone(slots[0]));
    CHECK(!is_tombstone(slots[1]));
    CHECK(!is_tombstone(slots[2]));
}

// --- POI tier on slots ---

// One build against `prev` (empty = fresh): allocate each (osm node id, tier)
// in order, finalize, and return the slot table the next build loads.
struct TierBuild {
    std::vector<uint32_t> idx;
    std::vector<SidecarSlot> slots;
};
static TierBuild tier_build(const ScratchDir& dir, const std::vector<SidecarSlot>& prev,
                            const std::vector<std::pair<uint64_t, uint8_t>>& records) {
    IdAllocator a;
    if (!prev.empty()) {
        const std::string path = dir.path() + "/prev.osm_ids";
        IdAllocator::write_sidecar(path, prev);
        REQUIRE(a.load_previous(path));
    }
    TierBuild out;
    for (const auto& [id, tier] : records) out.idx.push_back(a.allocate(ObjectType::OSM_NODE, id, tier));
    a.finalize();
    out.slots = a.take_slots();
    return out;
}

TEST(allocate_stamps_tier_on_append_reuse_and_recycle) {
    ScratchDir dir("gctest-tier");
    // Build 1 appends: node 10 tier 2, node 20 tier 3.
    const TierBuild b1 = tier_build(dir, {}, {{10, 2}, {20, 3}});
    REQUIRE(b1.slots.size() == 2u);
    CHECK_EQ(b1.slots[0].tier, 2);
    CHECK_EQ(b1.slots[1].tier, 3);
    // Build 2: node 10 is reused at tier 1; node 20 dies.
    const TierBuild b2 = tier_build(dir, b1.slots, {{10, 1}});
    CHECK_EQ(b2.idx[0], 0u);
    CHECK_EQ(b2.slots[0].tier, 1);
    // Build 3: node 30 recycles node 20's tombstone and takes its own tier.
    const TierBuild b3 = tier_build(dir, b2.slots, {{10, 1}, {30, 2}});
    CHECK_EQ(b3.idx[1], 1u);
    CHECK(!is_tombstone(b3.slots[1]));
    CHECK_EQ(b3.slots[1].tier, 2);
    CHECK_EQ(b3.slots[1].stable_id, 30u);
}

TEST(dead_slot_keeps_tier_of_last_record) {
    ScratchDir dir("gctest-tier");
    const TierBuild b1 = tier_build(dir, {}, {{10, 1}, {20, 2}});
    const TierBuild b2 = tier_build(dir, b1.slots, {{10, 1}});
    REQUIRE(b2.slots.size() == 2u);
    CHECK(is_tombstone(b2.slots[1]));
    CHECK_EQ(b2.slots[1].object_type, uint8_t(ObjectType::NONE));
    CHECK_EQ(b2.slots[1].stable_id, 0u);
    CHECK_EQ(b2.slots[1].tier, 2);
}

TEST(still_dead_slot_keeps_its_bytes_across_builds) {
    ScratchDir dir("gctest-tier");
    const TierBuild b1 = tier_build(dir, {}, {{10, 1}, {20, 4}});
    const TierBuild b2 = tier_build(dir, b1.slots, {{10, 1}});
    const TierBuild b3 = tier_build(dir, b2.slots, {{10, 1}});
    REQUIRE(b3.slots.size() == 2u);
    CHECK(std::memcmp(&b3.slots[1], &b2.slots[1], sizeof(SidecarSlot)) == 0);
    CHECK_EQ(b3.slots[1].tier, 4);
}

TEST(sidecar_written_before_tiers_reads_as_unknown_tier) {
    // A version-1 sidecar from before the tier byte: 12-byte slots, bytes
    // 2..3 zero. The slot that dies keeps tier 0 (ships in every tier).
    ScratchDir dir("gctest-tier");
    const std::string path = dir.path() + "/old.osm_ids";
    {
        std::ofstream f(path, std::ios::binary);
        const uint32_t header[3] = {SIDECAR_MAGIC, 1, 2};
        f.write(reinterpret_cast<const char*>(header), sizeof(header));
        for (uint64_t id : {uint64_t(10), uint64_t(20)}) {
            const uint8_t head[4] = {uint8_t(ObjectType::OSM_NODE), 0, 0, 0};
            f.write(reinterpret_cast<const char*>(head), 4);
            f.write(reinterpret_cast<const char*>(&id), 8);
        }
    }
    IdAllocator a;
    REQUIRE(a.load_previous(path));
    CHECK_EQ(a.allocate(ObjectType::OSM_NODE, 10, 3), 0u);
    a.finalize();
    const std::vector<SidecarSlot> slots = a.take_slots();
    REQUIRE(slots.size() == 2u);
    CHECK_EQ(slots[0].tier, 3);
    CHECK(is_tombstone(slots[1]));
    CHECK_EQ(slots[1].tier, 0);
}

TEST(allocate_without_tier_leaves_slot_bytes_as_before) {
    // Every other record kind allocates without a tier; its sidecar bytes
    // must not change.
    IdAllocator a;
    a.allocate(ObjectType::OSM_WAY, 42);
    const SidecarSlot want{uint8_t(ObjectType::OSM_WAY), 0, 0, 0, 42};
    CHECK(std::memcmp(&a.slots()[0], &want, sizeof(SidecarSlot)) == 0);
}

TEST(is_tombstone_reads_the_flag_only) {
    // A live slot can carry ObjectType::NONE (continent POI without an osm id).
    const SidecarSlot live_none{uint8_t(ObjectType::NONE), 0, 0, 0, 0};
    const SidecarSlot tomb{uint8_t(ObjectType::NONE), SLOT_FLAG_TOMBSTONE, 2, 0, 0};
    CHECK(!is_tombstone(live_none));
    CHECK(is_tombstone(tomb));
}

TEST(poi_shipped_tier_is_record_tier_or_remembered_tombstone_tier) {
    struct Case { uint8_t record_tier; bool has_slot; uint8_t flags; uint8_t slot_tier; uint8_t want; };
    const Case cases[] = {
        {2, true, 0, 2, 2},                    // live record
        {2, true, 0, 0, 2},                    // live record, slot tier not yet stamped
        {0, true, SLOT_FLAG_TOMBSTONE, 1, 1},  // tombstone of a major POI
        {0, true, SLOT_FLAG_TOMBSTONE, 4, 4},  // tombstone of an unshipped POI: no tier
        {0, true, SLOT_FLAG_TOMBSTONE, 0, 0},  // tombstone of unknown tier: every tier
        {3, false, 0, 0, 3},                   // no slot table
    };
    for (const auto& c : cases) {
        const SidecarSlot slot{uint8_t(ObjectType::NONE), c.flags, c.slot_tier, 0, 0};
        CHECK_EQ(poi_shipped_tier(c.record_tier, c.has_slot ? &slot : nullptr), c.want);
    }
}
