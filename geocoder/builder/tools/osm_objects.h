// OSM objects as pbf-apply moves them between PBF blocks and change files:
// fixed-size records whose strings and lists live in storage the block or
// change file chunk they came from owns.
#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include <string_view>
#include <vector>

enum class OsmType : uint8_t { Node = 0, Way = 1, Relation = 2 };
constexpr int kOsmTypes = 3;

// osmium's Location::undefined_coordinate: a node without a location.
constexpr int32_t kUndefinedCoordinate = 2147483647;

struct OsmTag {
    std::string_view key, value;
};

struct OsmMember {
    int64_t ref = 0;
    std::string_view role;
    OsmType type = OsmType::Node;
};

struct OsmObject {
    int64_t id = 0;
    uint32_t version = 0;
    uint32_t timestamp = 0;  // seconds since the epoch, 0 = none
    uint32_t changeset = 0;
    uint32_t uid = 0;
    std::string_view user;
    int32_t lon = kUndefinedCoordinate;  // 1e-7 degrees, nodes only
    int32_t lat = kUndefinedCoordinate;
    uint32_t first_tag = 0, tag_count = 0;
    uint32_t first_ref = 0, ref_count = 0;        // way node refs
    uint32_t first_member = 0, member_count = 0;  // relation members
    OsmType type = OsmType::Node;
    bool visible = true;
};

// The objects of a block or chunk and the lists they index into.
struct ObjectStore {
    std::vector<OsmObject> objects;
    std::vector<OsmTag> tags;
    std::vector<int64_t> refs;
    std::vector<OsmMember> members;

    void clear() {
        objects.clear();
        tags.clear();
        refs.clear();
        members.clear();
    }
    const OsmTag* tags_of(const OsmObject& o) const { return tags.data() + o.first_tag; }
    const int64_t* refs_of(const OsmObject& o) const { return refs.data() + o.first_ref; }
    const OsmMember* members_of(const OsmObject& o) const { return members.data() + o.first_member; }
};

// An object and the store holding its lists.
struct ObjectRef {
    const OsmObject* object;
    const ObjectStore* store;
};

// osmium's order of the objects of one type (object_order_type_id_reverse_version
// compares id() > 0, then positive_id()): ids <= 0 first by absolute value,
// then positive ids ascending, as one unsigned key.
constexpr uint64_t kPositiveIdBit = uint64_t(1) << 63;

inline uint64_t id_order_key(int64_t id) {
    return id > 0 ? kPositiveIdBit | uint64_t(id) : uint64_t(0) - uint64_t(id);
}

inline int64_t id_from_order_key(uint64_t key) {
    return key > kPositiveIdBit ? int64_t(key & ~kPositiveIdBit) : -int64_t(key);
}

// Append-only string storage whose views stay valid while it grows.
class StringArena {
public:
    std::string_view add(std::string_view s) {
        if (s.empty()) return {};
        if (s.size() > kPageBytes / 4) {
            large_.emplace_back(new char[s.size()]);
            std::memcpy(large_.back().get(), s.data(), s.size());
            return {large_.back().get(), s.size()};
        }
        if (pages_.empty() || used_ + s.size() > kPageBytes) {
            pages_.emplace_back(new char[kPageBytes]);
            used_ = 0;
        }
        char* p = pages_.back().get() + used_;
        std::memcpy(p, s.data(), s.size());
        used_ += s.size();
        return {p, s.size()};
    }

private:
    static constexpr size_t kPageBytes = size_t(1) << 20;
    std::vector<std::unique_ptr<char[]>> pages_;
    std::vector<std::unique_ptr<char[]>> large_;  // strings over a quarter page, one each
    size_t used_ = 0;
};
