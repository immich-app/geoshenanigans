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
//   //   uint16_t reserved
//   //   uint64_t stable_id     (osm_id, or hash of stable identity for postcodes)
//
// Memory model: slots_ is the single source of truth — populated from
// the previous sidecar at load_previous(), then mutated in-place by
// allocate(). Reused slots get their existing payload (already correct);
// recycled tombstones get rewritten; fresh allocations append. After
// the allocate() loop, finalize() marks any unconsumed-prev slots as
// tombstones and drops the prev_to_idx_ map + free-list immediately so
// only the slot table itself remains. take_slots() then moves the table
// out without copying — for 250M addr_points the savings are ~15 GiB
// of peak RSS over the previous build_sidecar()+claimed_ design.

#include <cstdint>
#include <cstring>
#include <fstream>
#include <unordered_map>
#include <vector>

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
    uint16_t reserved;
    uint64_t stable_id;
};
#pragma pack(pop)
static_assert(sizeof(SidecarSlot) == 12, "SidecarSlot must be 12 bytes");

// Internal: combine (object_type, stable_id) into a single uint64_t key
// so unordered_map lookups are O(1) without struct-equality boilerplate.
// Relies on ObjectType fitting in 8 bits and stable_id in 56 bits, which
// is true for all OSM ids today (highest osm_node_id ~13B = 34 bits).
inline uint64_t make_key(ObjectType t, uint64_t id) {
    return (static_cast<uint64_t>(t) << 56) | (id & 0x00FFFFFFFFFFFFFFull);
}

class IdAllocator {
public:
    IdAllocator() = default;

    // Load (osm_id → previous_idx) from a previous build's sidecar.
    // Tombstone slots in the previous build go onto the free-list so
    // they can be reused by new records this build.
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
        prev_to_idx_.reserve(count);
        for (uint32_t i = 0; i < count; i++) {
            const SidecarSlot& s = slots_[i];
            bool is_tomb = (s.flags & 0x01) != 0
                || s.object_type == static_cast<uint8_t>(ObjectType::NONE);
            if (is_tomb) {
                free_list_.push_back(i);
            } else {
                prev_to_idx_.emplace(
                    make_key(static_cast<ObjectType>(s.object_type), s.stable_id), i);
            }
        }
        return true;
    }

    // Assign a dense idx to a stable identity. Returns the same idx
    // it had in the previous build if present, else recycles a free
    // slot, else appends. Each call should be made exactly once per
    // distinct stable identity per build.
    uint32_t allocate(ObjectType type, uint64_t stable_id) {
        uint64_t key = make_key(type, stable_id);
        auto it = prev_to_idx_.find(key);
        if (it != prev_to_idx_.end()) {
            uint32_t idx = it->second;
            prev_to_idx_.erase(it);
            // slot already has correct {type, stable_id} from the load
            live_count_++;
            return idx;
        }
        if (!free_list_.empty()) {
            uint32_t idx = free_list_.back();
            free_list_.pop_back();
            slots_[idx] = SidecarSlot{
                static_cast<uint8_t>(type), 0, 0, stable_id};
            live_count_++;
            return idx;
        }
        uint32_t idx = static_cast<uint32_t>(slots_.size());
        slots_.push_back(SidecarSlot{
            static_cast<uint8_t>(type), 0, 0, stable_id});
        live_count_++;
        return idx;
    }

    // Mark surviving prev_to_idx_ entries (= deleted records) as
    // tombstones in slots_, then drop the lookup map and free-list to
    // release the bulk of the working memory before slots_ moves out.
    // For 250M addr_points the prev_to_idx_ unordered_map alone is
    // ~8 GiB of resident memory; releasing it here is what keeps the
    // peak inside the runner's RAM budget.
    void finalize() {
        for (auto& kv : prev_to_idx_) {
            slots_[kv.second] = SidecarSlot{
                static_cast<uint8_t>(ObjectType::NONE), 0x01, 0, 0};
        }
        std::unordered_map<uint64_t, uint32_t>().swap(prev_to_idx_);
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
    }

private:
    std::vector<SidecarSlot> slots_;
    std::unordered_map<uint64_t, uint32_t> prev_to_idx_;
    std::vector<uint32_t> free_list_;
    size_t live_count_ = 0;
};

} // namespace gc::id_alloc
