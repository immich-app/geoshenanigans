// Unit tests for IdAllocator's fallback key passes (id_allocator.h): a record
// whose identity found no slot takes the previous slot no identity claimed
// that shares a fallback key with it alone. tests/test_id_allocator.cpp pins
// everything else the allocator does, without passes.
#include "id_allocator.h"

#include <cstring>
#include <map>
#include <random>
#include <string>

#include "scratch_dir.h"
#include "test_framework.h"

using namespace gc::id_alloc;

namespace {

constexpr uint64_t NO = kNoFallbackKey;

// One pass's keys as vectors: record_keys[i] for record i, slot_keys[s] for
// previous slot s (indexed past the end of the table only by a bug).
struct PassKeys {
    std::vector<uint64_t> record_keys;
    std::vector<uint64_t> slot_keys;
};

std::vector<FallbackPass> passes_of(const std::vector<PassKeys>& keys) {
    std::vector<FallbackPass> passes;
    for (const PassKeys& k : keys)
        passes.push_back({[&k](size_t i) { return k.record_keys.at(i); },
                          [&k](uint32_t s) { return k.slot_keys.at(s); }});
    return passes;
}

struct Build {
    std::vector<uint32_t> idx;
    std::vector<SidecarSlot> slots;
    std::vector<size_t> matches;
};

// One build against `prev` (empty = fresh) with the given fallback keys,
// finalized; its slot table is what the next build loads.
Build build(const ScratchDir& dir, const std::vector<SidecarSlot>& prev, const std::vector<SlotIdentity>& ids,
            const std::vector<PassKeys>& keys, size_t records_per_pass = kRecordsPerMatchPass) {
    IdAllocator a;
    if (!prev.empty()) {
        const std::string path = dir.path() + "/prev.osm_ids";
        IdAllocator::write_sidecar(path, prev);
        REQUIRE(a.load_previous(path));
    }
    Build out;
    out.idx = a.allocate_all(ids.size(), [&](size_t i) { return ids[i]; }, records_per_pass, passes_of(keys));
    out.matches = a.fallback_matches();
    a.finalize();
    out.slots = a.take_slots();
    return out;
}

SidecarSlot live(ObjectType type, uint64_t id, uint8_t tier = 0) {
    return SidecarSlot{static_cast<uint8_t>(type), 0, tier, 0, id};
}

const SidecarSlot kTombstone{static_cast<uint8_t>(ObjectType::NONE), SLOT_FLAG_TOMBSTONE, 0, 0, 0};

bool same_slot(const SidecarSlot& a, const SidecarSlot& b) { return std::memcmp(&a, &b, sizeof(SidecarSlot)) == 0; }

bool same_slots(const std::vector<SidecarSlot>& a, const std::vector<SidecarSlot>& b) {
    return a.size() == b.size() &&
           (a.empty() || std::memcmp(a.data(), b.data(), a.size() * sizeof(SidecarSlot)) == 0);
}

}  // namespace

TEST(fallback_key_gives_a_record_the_slot_its_identity_lost) {
    // Arc 200 became arc 250 (its key moved) but kept its fallback key 8:
    // it takes arc 200's slot 1, which now holds 250. Arc 300 died (slot 2
    // a tombstone), and arc 400 is new: its key 5 is no slot's, so it
    // appends. The next build finds arc 250 at slot 1 by its identity.
    ScratchDir dir("gctest-fallback");
    const std::vector<SidecarSlot> prev = {live(ObjectType::ARC, 100), live(ObjectType::ARC, 200),
                                           live(ObjectType::ARC, 300)};
    const Build b = build(dir, prev, {{ObjectType::ARC, 100}, {ObjectType::ARC, 250}, {ObjectType::ARC, 400}},
                          {{{7, 8, 5}, {7, 8, 9}}});
    CHECK(b.idx == std::vector<uint32_t>({0, 1, 3}));
    CHECK(b.matches == std::vector<size_t>({1}));
    REQUIRE(b.slots.size() == 4u);
    CHECK(same_slot(b.slots[0], live(ObjectType::ARC, 100)));
    CHECK(same_slot(b.slots[1], live(ObjectType::ARC, 250)));
    CHECK(same_slot(b.slots[2], kTombstone));
    CHECK(same_slot(b.slots[3], live(ObjectType::ARC, 400)));

    const Build next = build(dir, b.slots, {{ObjectType::ARC, 400}, {ObjectType::ARC, 250}}, {});
    CHECK(next.idx == std::vector<uint32_t>({3, 1}));
    CHECK(next.matches.empty());
}

TEST(fallback_keys_without_a_pass_allocate_as_before) {
    // The same build with no pass: arc 250 appends like arc 400, and arc
    // 200's slot is a tombstone.
    ScratchDir dir("gctest-fallback");
    const std::vector<SidecarSlot> prev = {live(ObjectType::ARC, 100), live(ObjectType::ARC, 200),
                                           live(ObjectType::ARC, 300)};
    const Build b = build(dir, prev, {{ObjectType::ARC, 100}, {ObjectType::ARC, 250}, {ObjectType::ARC, 400}}, {});
    CHECK(b.idx == std::vector<uint32_t>({0, 3, 4}));
    CHECK(b.matches.empty());
    REQUIRE(b.slots.size() == 5u);
    CHECK(same_slot(b.slots[1], kTombstone));
    CHECK(same_slot(b.slots[2], kTombstone));
}

TEST(ambiguous_fallback_keys_stay_unmatched) {
    // Two records without a slot share key 8, which one unclaimed slot
    // holds; one record holds key 9, which two unclaimed slots hold. None
    // of them matches: the records append, the slots become tombstones.
    ScratchDir dir("gctest-fallback");
    const std::vector<SidecarSlot> prev = {live(ObjectType::ARC, 100), live(ObjectType::ARC, 200),
                                           live(ObjectType::ARC, 300), live(ObjectType::ARC, 400)};
    const Build b = build(dir, prev,
                          {{ObjectType::ARC, 100}, {ObjectType::ARC, 210}, {ObjectType::ARC, 220},
                           {ObjectType::ARC, 330}},
                          {{{1, 8, 8, 9}, {1, 8, 9, 9}}});
    CHECK(b.idx == std::vector<uint32_t>({0, 4, 5, 6}));
    CHECK(b.matches == std::vector<size_t>({0}));
    REQUIRE(b.slots.size() == 7u);
    for (size_t s = 1; s < 4; s++) CHECK(same_slot(b.slots[s], kTombstone));
    CHECK(same_slot(b.slots[4], live(ObjectType::ARC, 210)));
    CHECK(same_slot(b.slots[6], live(ObjectType::ARC, 330)));
}

TEST(fallback_passes_match_in_order_over_what_earlier_passes_left) {
    // Pass 1: records 1 and 2 share way-set key 8 (ambiguous); record 3
    // alone holds 9 and takes slot 3. Pass 2: records 1 and 2 take slots 1
    // and 2 by their endpoint keys. Record 3's pass-2 key 40, which slot 4
    // holds too, no longer counts, so record 4 alone holds 40 and takes slot
    // 4.
    ScratchDir dir("gctest-fallback");
    const std::vector<SidecarSlot> prev = {live(ObjectType::ARC, 100), live(ObjectType::ARC, 200),
                                           live(ObjectType::ARC, 300), live(ObjectType::ARC, 400),
                                           live(ObjectType::ARC, 500)};
    const Build b = build(dir, prev,
                          {{ObjectType::ARC, 100}, {ObjectType::ARC, 201}, {ObjectType::ARC, 301},
                           {ObjectType::ARC, 401}, {ObjectType::ARC, 501}},
                          {{{NO, 8, 8, 9, NO}, {NO, 8, 8, 9, NO}},
                           {{NO, 20, 30, 40, 40}, {NO, 20, 30, 50, 40}}});
    CHECK(b.idx == std::vector<uint32_t>({0, 1, 2, 3, 4}));
    CHECK(b.matches == std::vector<size_t>({1, 3}));
    REQUIRE(b.slots.size() == 5u);
    CHECK(same_slot(b.slots[1], live(ObjectType::ARC, 201)));
    CHECK(same_slot(b.slots[2], live(ObjectType::ARC, 301)));
    CHECK(same_slot(b.slots[3], live(ObjectType::ARC, 401)));
    CHECK(same_slot(b.slots[4], live(ObjectType::ARC, 501)));
}

TEST(fallback_offers_only_records_and_slots_no_identity_matched) {
    // Record 0 keeps slot 0 by its identity: its key 9, which unclaimed slot
    // 1 holds, takes no part, so slot 1 dies. Record 1's key 7 is slot 0's,
    // which is claimed: it appends. Record 2 and slot 2 both have no key.
    ScratchDir dir("gctest-fallback");
    const std::vector<SidecarSlot> prev = {live(ObjectType::ARC, 100), live(ObjectType::ARC, 200),
                                           live(ObjectType::ARC, 300)};
    const Build b = build(dir, prev, {{ObjectType::ARC, 100}, {ObjectType::ARC, 110}, {ObjectType::ARC, 120}},
                          {{{9, 7, NO}, {7, 9, NO}}});
    CHECK(b.idx == std::vector<uint32_t>({0, 3, 4}));
    CHECK(b.matches == std::vector<size_t>({0}));
    CHECK(same_slot(b.slots[1], kTombstone));
    CHECK(same_slot(b.slots[2], kTombstone));
}

TEST(fallback_never_offers_a_free_slot) {
    // Slots 1 and 2 are tombstones, on the free list; slot 1's key is the
    // record's. The record takes the free list's back, slot 2, as without
    // a pass.
    ScratchDir dir("gctest-fallback");
    const std::vector<SidecarSlot> prev = {live(ObjectType::ARC, 100), kTombstone, kTombstone};
    const Build b = build(dir, prev, {{ObjectType::ARC, 100}, {ObjectType::ARC, 200}}, {{{NO, 7}, {NO, 7, NO}}});
    CHECK(b.idx == std::vector<uint32_t>({0, 2}));
    CHECK(b.matches == std::vector<size_t>({0}));
}

TEST(fallback_match_takes_the_record_identity_and_tier) {
    // The slot held way 200 at tier 3; relation 9 takes it by key 8 and its
    // slot holds relation 9 at tier 1.
    ScratchDir dir("gctest-fallback");
    const std::vector<SidecarSlot> prev = {live(ObjectType::OSM_NODE, 100, 2), live(ObjectType::OSM_WAY, 200, 3)};
    const Build b = build(dir, prev, {{ObjectType::OSM_NODE, 100, 2}, {ObjectType::OSM_RELATION, 9, 1}},
                          {{{NO, 8}, {NO, 8}}});
    CHECK(b.idx == std::vector<uint32_t>({0, 1}));
    REQUIRE(b.slots.size() == 2u);
    CHECK(same_slot(b.slots[1], live(ObjectType::OSM_RELATION, 9, 1)));
}

TEST(fallback_passes_on_a_fresh_build_hand_out_slots_in_order) {
    // The first night: no previous table, so no slot to offer.
    ScratchDir dir("gctest-fallback");
    const Build b = build(dir, {}, {{ObjectType::ARC, 100}, {ObjectType::ARC, 200}}, {{{7, 8}, {}}});
    CHECK(b.idx == std::vector<uint32_t>({0, 1}));
    CHECK(b.matches == std::vector<size_t>({0}));
}

namespace {

// allocate_all with fallback passes as its definition says, one step at a
// time: identities first (the first record of a key takes its lowest slot),
// then each pass over the records and slots left, then the free list from
// the back and appends in record order.
Build reference_build(const std::vector<SidecarSlot>& prev, const std::vector<SlotIdentity>& ids,
                      const std::vector<PassKeys>& keys) {
    Build out;
    out.slots = prev;
    std::map<uint64_t, uint32_t> unclaimed;  // identity key -> its lowest previous slot
    std::vector<uint32_t> free_list;
    for (uint32_t s = 0; s < prev.size(); s++) {
        if (is_reclaimable(prev[s])) unclaimed.emplace(make_key(ObjectType(prev[s].object_type), prev[s].stable_id), s);
        else free_list.push_back(s);
    }
    out.idx.assign(ids.size(), TOMBSTONE_IDX);
    for (size_t i = 0; i < ids.size(); i++) {
        auto it = unclaimed.find(make_key(ids[i].type, ids[i].stable_id));
        if (it == unclaimed.end()) continue;
        out.idx[i] = it->second;
        out.slots[it->second].tier = ids[i].tier;
        unclaimed.erase(it);
    }
    for (const PassKeys& pass : keys) {
        std::map<uint64_t, std::vector<size_t>> records;
        std::map<uint64_t, std::vector<uint64_t>> slots;  // fallback key -> identity keys of unclaimed slots
        for (size_t i = 0; i < ids.size(); i++)
            if (out.idx[i] == TOMBSTONE_IDX && pass.record_keys[i] != NO) records[pass.record_keys[i]].push_back(i);
        for (const auto& [identity, s] : unclaimed)
            if (pass.slot_keys[s] != NO) slots[pass.slot_keys[s]].push_back(identity);
        size_t matched = 0;
        for (const auto& [key, rs] : records) {
            auto it = slots.find(key);
            if (rs.size() != 1 || it == slots.end() || it->second.size() != 1) continue;
            const uint32_t s = unclaimed.at(it->second[0]);
            const SlotIdentity& id = ids[rs[0]];
            out.idx[rs[0]] = s;
            out.slots[s] = live(id.type, id.stable_id, id.tier);
            unclaimed.erase(it->second[0]);
            matched++;
        }
        out.matches.push_back(matched);
    }
    for (size_t i = 0; i < ids.size(); i++) {
        if (out.idx[i] != TOMBSTONE_IDX) continue;
        const SidecarSlot fresh = live(ids[i].type, ids[i].stable_id, ids[i].tier);
        if (!free_list.empty()) {
            out.idx[i] = free_list.back();
            free_list.pop_back();
            out.slots[out.idx[i]] = fresh;
        } else {
            out.idx[i] = static_cast<uint32_t>(out.slots.size());
            out.slots.push_back(fresh);
        }
    }
    for (const auto& [identity, s] : unclaimed) out.slots[s] = tombstone_of(out.slots[s]);
    for (uint32_t s : free_list) out.slots[s].flags |= SLOT_FLAG_TOMBSTONE;
    return out;
}

// n identities over a range small enough to repeat, a few of other kinds.
std::vector<SlotIdentity> random_identities(std::mt19937_64& rng, size_t n, uint64_t id_range, bool tiers) {
    std::vector<SlotIdentity> ids(n);
    for (auto& id : ids) {
        id.type = rng() % 10 == 0 ? ObjectType::NONE : rng() % 10 == 0 ? ObjectType::OSM_WAY : ObjectType::ARC;
        id.stable_id = rng() % id_range;
        id.tier = tiers ? static_cast<uint8_t>(rng() % 5) : 0;
    }
    return ids;
}

// Keys from a range that repeats some, none for about one in five.
std::vector<uint64_t> random_keys(std::mt19937_64& rng, size_t n, uint64_t key_range) {
    std::vector<uint64_t> keys(n);
    for (auto& k : keys) k = rng() % 5 == 0 ? NO : rng() % key_range;
    return keys;
}

}  // namespace

TEST(fallback_passes_match_the_reference_allocation) {
    // Chained builds with up to three passes, repeated identities and keys
    // on both sides, tombstones, NONE slots and POI-like tiers, matched in
    // passes too small to hold a key's every record; large rounds take the
    // parallel sorts.
    ScratchDir dir("gctest-fallback-oracle");
    std::mt19937_64 rng(29);
    size_t matched = 0;
    for (int round = 0; round < 40; round++) {
        const bool large = round % 10 == 9;
        const size_t n = large ? 300000 : 1 + rng() % 300;
        const uint64_t id_range = large ? 400000 : 1 + rng() % (2 * n);
        const bool tiers = round % 4 == 1;
        std::vector<SidecarSlot> prev;
        for (int day = 0; day < 3; day++) {
            const size_t count = day == 0 ? n : n - n / 4 + rng() % (n / 2 + 1);
            const auto ids = random_identities(rng, count, id_range, tiers);
            std::vector<PassKeys> keys(rng() % 4);
            for (PassKeys& k : keys) {
                const uint64_t key_range = 1 + rng() % (large ? 2 * count : 2 * count + 4);
                k.record_keys = random_keys(rng, count, key_range);
                k.slot_keys = random_keys(rng, prev.size(), key_range);
            }
            const Build want = reference_build(prev, ids, keys);
            const size_t per_pass = round % 3 == 0 ? kRecordsPerMatchPass : 1 + rng() % (large ? 50000 : 40);
            const Build got = build(dir, prev, ids, keys, per_pass);
            CHECK(got.idx == want.idx);
            CHECK(same_slots(got.slots, want.slots));
            CHECK(got.matches == want.matches);
            for (size_t m : want.matches) matched += m;
            prev = got.slots;
        }
    }
    CHECK(matched > 0);  // the fixtures do reach the passes' matches
}

TEST(fallback_passes_that_match_nothing_change_nothing) {
    // Passes whose keys are all none, or never shared, give the slots and
    // the table allocate_all gives without a pass.
    ScratchDir dir("gctest-fallback-none");
    std::mt19937_64 rng(31);
    std::vector<SidecarSlot> prev;
    for (int day = 0; day < 4; day++) {
        const auto ids = random_identities(rng, 2000, 3000, day % 2 == 1);
        const Build plain = build(dir, prev, ids, {});
        PassKeys none{std::vector<uint64_t>(ids.size(), NO), std::vector<uint64_t>(prev.size(), NO)};
        PassKeys apart{std::vector<uint64_t>(ids.size()), std::vector<uint64_t>(prev.size())};
        for (size_t i = 0; i < ids.size(); i++) apart.record_keys[i] = 2 * i;
        for (size_t s = 0; s < prev.size(); s++) apart.slot_keys[s] = 2 * s + 1;
        const Build passes = build(dir, prev, ids, {none, apart});
        CHECK(passes.idx == plain.idx);
        CHECK(same_slots(passes.slots, plain.slots));
        CHECK(passes.matches == std::vector<size_t>({0, 0}));
        prev = plain.slots;
    }
}
