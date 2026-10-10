#pragma once

// Persistent ID allocator (Strategy 2).
//
// Reads the previous build's <file>.osm_ids sidecar to learn which
// dense `idx` was assigned to each stable identity (osm_id-derived) last
// build, and hands out the SAME idx to the same identity on the new
// build. New identities go to a free-list slot from a previously-deleted
// record, falling back to appending past the previous max.
//
// Net effect: unchanged records keep the same byte offset day-over-day,
// so the diff tool gets clean MATCH ops with no parent-id cascade
// shifts. PARENT_REMAP_MARKER becomes vestigial; all id_remap noise
// disappears from patches.
//
// Sidecar wire format:
//   uint32_t magic = 0xD0510EAD
//   uint32_t version = 1
//   uint32_t count   // number of slots, including tombstones
//   // count × 12 bytes:
//   //   uint8_t  object_type   (kind discriminator; see ObjectType)
//   //   uint8_t  flags         (bit 0 = tombstone)
//   //   uint8_t  tier          (POI tier of the record holding the slot, or
//   //                           that last held a tombstone; 0 = unknown)
//   //   uint8_t  reserved
//   //   uint64_t stable_id     (osm_id, or hash of stable identity for postcodes)
//
// Memory model: slots_ is the single source of truth — populated from
// the previous sidecar at load_previous(), then mutated in-place by
// allocate_all(). Reused slots keep their existing payload (already correct)
// apart from the tier; recycled tombstones get rewritten; fresh allocations append. After
// allocate_all(), finalize() marks any unconsumed-prev slots as
// tombstones and drops the bookkeeping immediately so only the slot
// table itself remains. take_slots() then moves the table out without
// copying.

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <vector>
#include <stdexcept>

#include "parallel.h"

namespace gc::id_alloc {

// Stable-identity discriminator. Determines what `stable_id` means.
// Values are persisted in the sidecar header byte; do NOT renumber.
enum class ObjectType : uint8_t {
    NONE         = 0,   // tombstone slot (no living record)
    OSM_NODE     = 1,
    OSM_WAY      = 2,
    OSM_RELATION = 3,
    POSTCODE     = 4,   // hash of (country_code, postcode_string)
    SYNTHETIC    = 5,   // builder-derived (e.g. multi-ring relation: hash of relation_id+ring_index)
};

constexpr uint32_t SIDECAR_MAGIC   = 0xD0510EAD;
constexpr uint32_t SIDECAR_VERSION = 1;
constexpr uint32_t TOMBSTONE_IDX   = 0xFFFFFFFFu;

// 12-byte on-disk slot record. Packed for stable layout.
#pragma pack(push, 1)
struct SidecarSlot {
    uint8_t  object_type;   // ObjectType
    uint8_t  flags;         // bit 0 = tombstone
    // POI tier of the record holding the slot, or for a tombstone of the
    // record that last held it. 0 = unknown: other kinds, and slots written
    // before tiers were recorded (always 0 there, so version 1 still fits).
    uint8_t  tier;
    uint8_t  reserved;
    uint64_t stable_id;
};
#pragma pack(pop)
static_assert(sizeof(SidecarSlot) == 12, "SidecarSlot must be 12 bytes");

constexpr uint8_t SLOT_FLAG_TOMBSTONE = 0x01;

// A slot finalize() turned into a tombstone. Only the flag counts: a live
// slot can carry ObjectType::NONE (a continent POI whose planet osm id was
// missing allocates {NONE, 0}).
inline bool is_tombstone(const SidecarSlot& s) {
    return (s.flags & SLOT_FLAG_TOMBSTONE) != 0;
}

// POI tier whose files carry a slot: a live record's own tier, or for a
// tombstone the tier of the record that last held it, so a death only
// reaches the files that held the record. A tombstone of unknown tier (0)
// ships in every tier. `slot` is null when strategy 2 did not run.
inline uint8_t poi_shipped_tier(uint8_t record_tier, const SidecarSlot* slot) {
    if (!slot || !is_tombstone(*slot)) return record_tier;

    return slot->tier;
}

// Internal: combine (object_type, stable_id) into a single uint64_t key
// so identities match and sort as one integer.
// Relies on ObjectType fitting in 8 bits and stable_id in 56 bits, which
// is true for all OSM ids today (highest osm_node_id ~13B = 34 bits).
inline uint64_t make_key(ObjectType t, uint64_t id) {
    return (static_cast<uint64_t>(t) << 56) | (id & 0x00FFFFFFFFFFFFFFull);
}

// One record's stable identity, as allocate_all asks for it. `tier` is
// stamped on the slot whichever way the record gets it (a live POI can
// change tier between days); other kinds leave it 0.
struct SlotIdentity {
    ObjectType type;
    uint64_t stable_id;
    uint8_t tier = 0;
};

class IdAllocator {
public:
    IdAllocator() = default;

    // Load the previous build's slot table from its sidecar. Tombstone
    // slots go onto the free-list so new records can reuse them.
    bool load_previous(const std::string& sidecar_path) {
        std::ifstream f(sidecar_path, std::ios::binary);
        if (!f) return false;
        uint32_t magic = 0, version = 0, count = 0;
        f.read(reinterpret_cast<char*>(&magic), 4);
        f.read(reinterpret_cast<char*>(&version), 4);
        f.read(reinterpret_cast<char*>(&count), 4);
        if (!f || magic != SIDECAR_MAGIC || version != SIDECAR_VERSION) return false;
        slots_.resize(count);
        if (count > 0) {
            f.read(reinterpret_cast<char*>(slots_.data()),
                   static_cast<std::streamsize>(count) * sizeof(SidecarSlot));
            if (!f) { slots_.clear(); return false; }
        }
        for (uint32_t i = 0; i < count; i++) {
            const SidecarSlot& s = slots_[i];
            if (is_tombstone(s) || s.object_type == static_cast<uint8_t>(ObjectType::NONE))
                free_list_.push_back(i);
        }
        return true;
    }

    // Assign every record of this build its dense idx, as handing them out
    // one at a time in record order would: a record whose identity held a
    // live slot last build gets that slot back (the first record when several
    // share an identity, the lowest slot when several slots do), every other
    // record recycles the free-list from the back, then appends.
    // identity(i) returns record i's SlotIdentity. Matching identities to
    // slots is two sorts and a merge on every core; only the hand-out of
    // recycled and new slots walks the records in order. Once per allocator.
    template <class Identity>
    std::vector<uint32_t> allocate_all(size_t n, Identity identity) {
        if (allocated_) throw std::logic_error("IdAllocator::allocate_all called twice");
        allocated_ = true;

        struct KeyIdx { uint64_t key; uint32_t idx; };
        auto by_key = [](const KeyIdx& a, const KeyIdx& b) {
            return a.key != b.key ? a.key < b.key : a.idx < b.idx;
        };

        // The previous build's live identities; of a repeated key only the
        // lowest slot can be reclaimed, the others stay as they are.
        std::vector<KeyIdx> prev;
        for (uint32_t i = 0; i < slots_.size(); i++) {
            const SidecarSlot& s = slots_[i];
            if (is_tombstone(s) || s.object_type == static_cast<uint8_t>(ObjectType::NONE)) continue;
            prev.push_back({make_key(static_cast<ObjectType>(s.object_type), s.stable_id), i});
        }
        parallel_sort(prev.begin(), prev.end(), by_key);
        prev.erase(std::unique(prev.begin(), prev.end(),
                               [](const KeyIdx& a, const KeyIdx& b) { return a.key == b.key; }),
                   prev.end());

        constexpr uint32_t kUnmatched = TOMBSTONE_IDX;
        std::vector<uint32_t> out(n, kUnmatched);
        std::vector<char> claimed(prev.size(), 0);
        {
            std::vector<KeyIdx> records(n);
            parallel_for(n, [&](size_t begin, size_t end, unsigned) {
                for (size_t i = begin; i < end; i++) {
                    SlotIdentity id = identity(i);
                    records[i] = {make_key(id.type, id.stable_id), static_cast<uint32_t>(i)};
                }
            });
            parallel_sort(records.begin(), records.end(), by_key);
            // The first record of each key (lowest index) takes the slot.
            parallel_for_runs(n, [&](size_t i) { return records[i].key == records[i - 1].key; },
                              [&](size_t begin, size_t end, unsigned) {
                auto p = std::lower_bound(prev.begin(), prev.end(), records[begin].key,
                                          [](const KeyIdx& e, uint64_t key) { return e.key < key; });
                for (size_t r = begin; r < end && p != prev.end(); r++) {
                    if (r > begin && records[r].key == records[r - 1].key) continue;
                    while (p != prev.end() && p->key < records[r].key) ++p;
                    if (p == prev.end() || p->key != records[r].key) continue;
                    out[records[r].idx] = p->idx;
                    claimed[p - prev.begin()] = 1;
                }
            });
        }

        for (size_t i = 0; i < n; i++) {
            SlotIdentity id = identity(i);
            if (out[i] != kUnmatched) {
                // slot already has correct {type, stable_id} from the load
                slots_[out[i]].tier = id.tier;
                continue;
            }
            const SidecarSlot fresh{static_cast<uint8_t>(id.type), 0, id.tier, 0, id.stable_id};
            if (!free_list_.empty()) {
                out[i] = free_list_.back();
                free_list_.pop_back();
                slots_[out[i]] = fresh;
            } else {
                out[i] = static_cast<uint32_t>(slots_.size());
                slots_.push_back(fresh);
            }
        }
        live_count_ += n;
        for (size_t p = 0; p < prev.size(); p++)
            if (!claimed[p]) unclaimed_.push_back(prev[p].idx);
        return out;
    }

    // Mark the previous build's slots no record reclaimed (= deleted
    // records) as tombstones, keeping the tier of the record that held each
    // one, and flag every free-list slot nothing claimed: a live NONE-type
    // slot sits there unflagged and is dead now (slots already flagged keep
    // their bytes). Then drop the bookkeeping before slots_ moves out.
    void finalize() {
        for (uint32_t i : unclaimed_) {
            SidecarSlot& s = slots_[i];
            s = SidecarSlot{static_cast<uint8_t>(ObjectType::NONE), SLOT_FLAG_TOMBSTONE, s.tier, 0, 0};
        }
        for (uint32_t i : free_list_) slots_[i].flags |= SLOT_FLAG_TOMBSTONE;
        std::vector<uint32_t>().swap(unclaimed_);
        std::vector<uint32_t>().swap(free_list_);
    }

    // Move-out the finalized slot table. After this returns the
    // allocator is empty and must not be allocated against again.
    std::vector<SidecarSlot> take_slots() {
        std::vector<SidecarSlot> out;
        out.swap(slots_);
        return out;
    }

    const std::vector<SidecarSlot>& slots() const { return slots_; }

    // Total slots including tombstones — i.e., the size the record
    // file should be written at. All claimed indices are < this value.
    uint32_t total_slots() const { return static_cast<uint32_t>(slots_.size()); }

    // Number of records actually claimed this build (live records).
    size_t live_count() const { return live_count_; }

    // Number of tombstone slots (deleted from previous build, not yet
    // recycled or recycled-to-tombstone at end-of-build).
    size_t tombstone_count() const {
        return slots_.size() - live_count_;
    }

    static void write_sidecar(const std::string& path,
                              const std::vector<SidecarSlot>& slots) {
        std::ofstream f(path, std::ios::binary);
        uint32_t magic = SIDECAR_MAGIC, version = SIDECAR_VERSION;
        uint32_t count = static_cast<uint32_t>(slots.size());
        f.write(reinterpret_cast<const char*>(&magic), 4);
        f.write(reinterpret_cast<const char*>(&version), 4);
        f.write(reinterpret_cast<const char*>(&count), 4);
        f.write(reinterpret_cast<const char*>(slots.data()),
                static_cast<std::streamsize>(slots.size()) * sizeof(SidecarSlot));
        f.flush();
        if (!f) throw std::runtime_error("failed to write " + path);
    }

private:
    std::vector<SidecarSlot> slots_;
    std::vector<uint32_t> free_list_;
    // Previous-build slots whose identity no record of this build had.
    std::vector<uint32_t> unclaimed_;
    size_t live_count_ = 0;
    bool allocated_ = false;
};

} // namespace gc::id_alloc
