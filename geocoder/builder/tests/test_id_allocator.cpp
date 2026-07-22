// Unit tests for the Strategy-2 persistent id allocator (id_allocator.h).
// This is determinism-critical: it decides which dense index each stable
// identity gets across builds, which defines record byte offsets. These tests
// lock in the current behaviour (key packing, reuse, free-list recycle,
// tombstoning, sidecar round-trip) so future refactors can't silently shift it.
#include "id_allocator.h"

#include <cstdio>
#include <string>
#include <unistd.h>

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
