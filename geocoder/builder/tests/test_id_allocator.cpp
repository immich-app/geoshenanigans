// Unit tests for the Strategy-2 persistent id allocator (id_allocator.h).
// This is determinism-critical: it decides which dense index each stable
// identity gets across builds, which defines record byte offsets. These tests
// lock in the current behaviour (key packing, reuse, free-list recycle,
// tombstoning, sidecar round-trip) so future refactors can't silently shift it.
#include "id_allocator.h"

#include <random>
#include <string>
#include <unordered_map>

#include "scratch_dir.h"
#include "test_framework.h"

using namespace gc::id_alloc;

// allocate_all over a list of identities, in list order.
static std::vector<uint32_t> allocate_list(IdAllocator& a, const std::vector<SlotIdentity>& ids,
                                           size_t records_per_pass = kRecordsPerMatchPass) {
    return a.allocate_all(ids.size(), [&](size_t i) { return ids[i]; }, records_per_pass);
}

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
    const auto idx = allocate_list(a, {{ObjectType::OSM_NODE, 100},
                                       {ObjectType::OSM_NODE, 200},
                                       {ObjectType::OSM_WAY, 100}});  // diff type, new slot
    CHECK(idx == std::vector<uint32_t>({0, 1, 2}));
    CHECK_EQ(a.total_slots(), 3u);
    CHECK_EQ(a.live_count(), size_t(3));
    CHECK_EQ(a.tombstone_count(), size_t(0));
}

TEST(sidecar_roundtrip_reuses_indices) {
    ScratchDir dir("gctest-reuse");
    const std::string path = dir.path() + "/prev.osm_ids";
    // Build 1: three records.
    std::vector<SidecarSlot> slots;
    {
        IdAllocator a;
        allocate_list(a, {{ObjectType::OSM_NODE, 10},    // idx 0
                          {ObjectType::OSM_NODE, 20},    // idx 1
                          {ObjectType::OSM_WAY, 30}});   // idx 2
        a.finalize();
        slots = a.take_slots();
        IdAllocator::write_sidecar(path, slots);
    }
    CHECK_EQ(slots.size(), size_t(3));

    // Build 2 (same identities, different encounter order): each must get
    // the SAME index it had in build 1.
    IdAllocator b;
    CHECK(b.load_previous(path));
    const auto idx = allocate_list(b, {{ObjectType::OSM_WAY, 30},
                                       {ObjectType::OSM_NODE, 10},
                                       {ObjectType::OSM_NODE, 20}});
    CHECK(idx == std::vector<uint32_t>({2, 0, 1}));
    b.finalize();
    CHECK_EQ(b.tombstone_count(), size_t(0));
}

TEST(deleted_record_tombstones_then_slot_recycled) {
    ScratchDir dir("gctest-tomb");
    const std::string path = dir.path() + "/build1.osm_ids";
    {
        IdAllocator a;
        allocate_list(a, {{ObjectType::OSM_NODE, 10},    // idx 0
                          {ObjectType::OSM_NODE, 20}});  // idx 1
        a.finalize();
        IdAllocator::write_sidecar(path, a.take_slots());
    }
    // Build 2: node 20 disappears, node 99 appears. node 10 keeps idx 0;
    // node 20 not consumed → its slot 1 becomes a tombstone in finalize();
    // but a NEW identity recycles a free-list slot. In build 2 the free-list
    // only contains prior tombstones (none here), so node 99 appends at idx 2.
    IdAllocator b;
    CHECK(b.load_previous(path));
    const auto idx2 = allocate_list(b, {{ObjectType::OSM_NODE, 10}, {ObjectType::OSM_NODE, 99}});
    CHECK(idx2 == std::vector<uint32_t>({0, 2}));
    b.finalize();
    // slot 1 (node 20) is now a tombstone.
    CHECK_EQ(b.total_slots(), 3u);
    CHECK_EQ(b.live_count(), size_t(2));
    CHECK_EQ(b.tombstone_count(), size_t(1));

    // Build 3: a brand-new identity should recycle the tombstoned slot 1.
    std::vector<SidecarSlot> s2 = b.take_slots();
    const std::string path3 = dir.path() + "/build2.osm_ids";
    IdAllocator::write_sidecar(path3, s2);
    IdAllocator c;
    CHECK(c.load_previous(path3));
    const auto idx3 = allocate_list(c, {{ObjectType::OSM_NODE, 10},    // reuse
                                        {ObjectType::OSM_NODE, 99},    // reuse
                                        {ObjectType::OSM_NODE, 77}});  // recycle tombstone
    CHECK(idx3 == std::vector<uint32_t>({0, 2, 1}));
}

TEST(load_previous_missing_file_returns_false) {
    IdAllocator a;
    ScratchDir dir("gctest-missing");
    CHECK(!a.load_previous(dir.path() + "/does_not_exist.osm_ids"));
    // Falls back to fresh allocation.
    CHECK(allocate_list(a, {{ObjectType::OSM_NODE, 1}}) == std::vector<uint32_t>({0}));
}

TEST(unclaimed_live_none_slot_becomes_tombstone) {
    // A continent POI whose planet osm id was missing allocates {NONE, 0}:
    // a live slot that load_previous puts on the free list. If nothing
    // claims it the next day it is dead, and only the flag says so.
    ScratchDir dir("gctest-none");
    const std::string path = dir.path() + "/prev.osm_ids";
    {
        IdAllocator a;
        allocate_list(a, {{ObjectType::NONE, 0},         // idx 0
                          {ObjectType::NONE, 0},         // idx 1
                          {ObjectType::OSM_NODE, 10}});  // idx 2
        a.finalize();
        IdAllocator::write_sidecar(path, a.take_slots());
    }
    IdAllocator b;
    REQUIRE(b.load_previous(path));
    const auto idx = allocate_list(b, {{ObjectType::NONE, 0},         // recycles the free-list back
                                       {ObjectType::OSM_NODE, 10}});
    CHECK(idx == std::vector<uint32_t>({1, 2}));
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
    out.idx = a.allocate_all(records.size(), [&](size_t i) {
        return SlotIdentity{ObjectType::OSM_NODE, records[i].first, records[i].second};
    });
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
    CHECK(allocate_list(a, {{ObjectType::OSM_NODE, 10, 3}}) == std::vector<uint32_t>({0}));
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
    allocate_list(a, {{ObjectType::OSM_WAY, 42}});
    const SidecarSlot want{uint8_t(ObjectType::OSM_WAY), 0, 0, 0, 42};
    CHECK(std::memcmp(&a.slots()[0], &want, sizeof(SidecarSlot)) == 0);
}

TEST(allocate_all_runs_once_per_allocator) {
    IdAllocator a;
    allocate_list(a, {{ObjectType::OSM_WAY, 42}});
    bool thrown = false;
    try {
        allocate_list(a, {{ObjectType::OSM_WAY, 43}});
    } catch (const std::logic_error&) {
        thrown = true;
    }
    CHECK(thrown);
}

TEST(repeated_identity_reclaims_the_lowest_slot_for_the_first_record) {
    // Slots 0 and 2 both claim node 5: slot 0 goes to the first record with
    // node 5, slot 2 stays untouched (live, not tombstoned); the second
    // record with node 5 recycles the free slot 1.
    ScratchDir dir("gctest-dup");
    const std::string path = dir.path() + "/prev.osm_ids";
    IdAllocator::write_sidecar(path, {{uint8_t(ObjectType::OSM_NODE), 0, 0, 0, 5},
                                      {uint8_t(ObjectType::NONE), SLOT_FLAG_TOMBSTONE, 0, 0, 0},
                                      {uint8_t(ObjectType::OSM_NODE), 0, 0, 0, 5}});
    IdAllocator a;
    REQUIRE(a.load_previous(path));
    const auto idx = allocate_list(a, {{ObjectType::OSM_NODE, 5, 2}, {ObjectType::OSM_NODE, 5, 3}});
    CHECK(idx == std::vector<uint32_t>({0, 1}));
    a.finalize();
    const auto slots = a.take_slots();
    REQUIRE(slots.size() == 3u);
    CHECK_EQ(slots[0].tier, 2);
    CHECK_EQ(slots[1].stable_id, 5u);
    CHECK_EQ(slots[1].tier, 3);
    CHECK(!is_tombstone(slots[2]));
    CHECK_EQ(slots[2].stable_id, 5u);
}

namespace {

// The one-at-a-time allocator allocate_all replaced: the oracle it must match.
class ReferenceAllocator {
public:
    explicit ReferenceAllocator(const std::vector<SidecarSlot>& prev) : slots_(prev) {
        for (uint32_t i = 0; i < slots_.size(); i++) {
            const SidecarSlot& s = slots_[i];
            if (is_tombstone(s) || s.object_type == uint8_t(ObjectType::NONE))
                free_list_.push_back(i);
            else
                prev_to_idx_.emplace(make_key(ObjectType(s.object_type), s.stable_id), i);
        }
    }

    uint32_t allocate(const SlotIdentity& id) {
        auto it = prev_to_idx_.find(make_key(id.type, id.stable_id));
        if (it != prev_to_idx_.end()) {
            uint32_t idx = it->second;
            prev_to_idx_.erase(it);
            slots_[idx].tier = id.tier;
            return idx;
        }
        const SidecarSlot fresh{uint8_t(id.type), 0, id.tier, 0, id.stable_id};
        if (!free_list_.empty()) {
            uint32_t idx = free_list_.back();
            free_list_.pop_back();
            slots_[idx] = fresh;
            return idx;
        }
        slots_.push_back(fresh);
        return static_cast<uint32_t>(slots_.size() - 1);
    }

    std::vector<SidecarSlot> finalize() {
        for (auto& kv : prev_to_idx_) {
            SidecarSlot& s = slots_[kv.second];
            s = SidecarSlot{uint8_t(ObjectType::NONE), SLOT_FLAG_TOMBSTONE, s.tier, 0, 0};
        }
        for (uint32_t i : free_list_) slots_[i].flags |= SLOT_FLAG_TOMBSTONE;
        return slots_;
    }

private:
    std::vector<SidecarSlot> slots_;
    std::unordered_map<uint64_t, uint32_t> prev_to_idx_;
    std::vector<uint32_t> free_list_;
};

std::vector<SlotIdentity> new_identities(std::mt19937_64& rng, size_t n, uint64_t id_range) {
    static const ObjectType kTypes[] = {ObjectType::NONE, ObjectType::OSM_NODE, ObjectType::OSM_WAY,
                                        ObjectType::SYNTHETIC};
    std::vector<SlotIdentity> ids(n);
    for (auto& id : ids) {
        id.type = rng() % 8 == 0 ? kTypes[rng() % 4] : ObjectType::OSM_NODE;
        id.stable_id = rng() % id_range;
        if (rng() % 50 == 0) id.stable_id |= uint64_t(0x7F) << 56;  // bits the key masks off
        id.tier = static_cast<uint8_t>(rng() % 5);
    }
    return ids;
}

bool same_slots(const std::vector<SidecarSlot>& a, const std::vector<SidecarSlot>& b) {
    return a.size() == b.size() &&
           (a.empty() || std::memcmp(a.data(), b.data(), a.size() * sizeof(SidecarSlot)) == 0);
}

}  // namespace

TEST(allocate_all_matches_one_at_a_time_allocation) {
    // Chained builds with repeated identities on both sides, NONE slots,
    // tombstones and masked-off id bits; large rounds take the parallel sort.
    ScratchDir dir("gctest-oracle");
    const std::string path = dir.path() + "/prev.osm_ids";
    std::mt19937_64 rng(17);
    for (int round = 0; round < 40; round++) {
        const bool large = round % 10 == 9;
        const size_t n = large ? 300000 : rng() % 400;
        const uint64_t id_range = large ? 250000 : 1 + rng() % 120;
        std::vector<SidecarSlot> prev;
        for (int build = 0; build < 3; build++) {
            const auto ids = new_identities(rng, build == 0 ? n : n - n / 4 + rng() % (n / 2 + 1), id_range);
            if (build == 1 && !prev.empty()) prev[rng() % prev.size()].object_type = uint8_t(ObjectType::NONE);
            ReferenceAllocator ref(prev);
            std::vector<uint32_t> want;
            for (const auto& id : ids) want.push_back(ref.allocate(id));
            const std::vector<SidecarSlot> want_slots = ref.finalize();

            IdAllocator a;
            if (build > 0) {
                IdAllocator::write_sidecar(path, prev);
                REQUIRE(a.load_previous(path));
            }
            // Matching in passes too small to hold a key's every record.
            const size_t per_pass = round % 3 == 0 ? kRecordsPerMatchPass : 1 + rng() % (large ? 50000 : 40);
            const auto got = allocate_list(a, ids, per_pass);
            a.finalize();
            const std::vector<SidecarSlot> got_slots = a.take_slots();
            CHECK(got == want);
            CHECK(same_slots(got_slots, want_slots));
            prev = got_slots;
        }
    }
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
