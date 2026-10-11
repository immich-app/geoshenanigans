// OSM PBF blocks to and from OsmObjects for pbf-apply: decode a
// PrimitiveBlock the way osmium's PBF reader does, encode one the way
// osmium's PBF writer does (dense nodes, full metadata), wrap it in a zlib
// blob, find a blob's first object without inflating all of it, and read
// and write the header block.
#pragma once

#include <cstdint>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unistd.h>
#include <unordered_map>
#include <vector>
#include <zlib.h>

#include <protozero/pbf_reader.hpp>
#include <protozero/pbf_writer.hpp>

#include "osm_objects.h"
#include "pbf_format.h"
#include "pbf_reader.h"

// Most objects osmium's PBF writer puts in one block.
constexpr size_t kMaxBlockObjects = 8000;
// The PBF spec's limit on a blob's uncompressed size, and the size a block
// is kept under when it can be split (the spec's recommended maximum).
constexpr size_t kMaxBlockBytes = size_t(32) << 20;
constexpr size_t kTargetBlockBytes = size_t(16) << 20;
// zlib level of re-encoded blobs: osmium's default.
constexpr int kBlobZlibLevel = Z_DEFAULT_COMPRESSION;
// What a blob peek reads first: enough for 61% of planet blobs; all of
// them take 8 KiB on average (12.6 GiB read for 1.52M blobs).
constexpr size_t kPeekFirstReadBytes = 4 * 1024;

namespace pbf_codec_detail {

struct BlockScale {
    int64_t granularity = 100;
    int64_t date_granularity = 1000;
    int64_t lat_offset = 0;
    int64_t lon_offset = 0;
};

[[noreturn]] inline void format_error(const char* what) {
    throw std::runtime_error(std::string("PBF format error: ") + what);
}

inline std::string_view string_at(const std::vector<std::string_view>& table, uint64_t index) {
    if (index >= table.size()) format_error("string index out of range");
    return table[index];
}

// osmium's PBFPrimitiveBlockDecoder::convert_pbf_lat/lon: to 1e-7 degrees,
// truncating.
inline int32_t to_coordinate(int64_t value, int64_t offset, const BlockScale& scale) {
    return static_cast<int32_t>((value * scale.granularity + offset) / 100);
}

inline uint32_t to_timestamp(int64_t value, const BlockScale& scale) {
    return static_cast<uint32_t>(value * scale.date_granularity / 1000);
}

inline uint32_t to_version(int64_t value) {
    if (value < -1) format_error("negative version");
    return value == -1 ? 0 : static_cast<uint32_t>(value);
}

inline uint32_t to_changeset(int64_t value) {
    if (value < -1 || value >= int64_t(UINT32_MAX)) format_error("changeset out of range");
    return value == -1 ? 0 : static_cast<uint32_t>(value);
}

inline uint32_t to_uid(int32_t value) { return value < 0 ? 0 : static_cast<uint32_t>(value); }

// Decodes an Info message into o; returns the user's string index.
inline uint32_t decode_info(protozero::pbf_reader info, const std::vector<std::string_view>& table,
                            const BlockScale& scale, OsmObject& o) {
    uint32_t user_sid = kNoSid;
    while (info.next()) {
        switch (info.tag()) {
            case InfoTag::VERSION: o.version = to_version(info.get_int32()); break;
            case InfoTag::TIMESTAMP: o.timestamp = to_timestamp(info.get_int64(), scale); break;
            case InfoTag::CHANGESET: o.changeset = to_changeset(info.get_int64()); break;
            case InfoTag::UID: o.uid = to_uid(info.get_int32()); break;
            case InfoTag::USER_SID:
                user_sid = info.get_uint32();
                o.user = string_at(table, user_sid);
                break;
            case InfoTag::VISIBLE: o.visible = info.get_bool(); break;
            default: info.skip();
        }
    }
    return user_sid;
}

// The varints of a packed field, one at a time.
struct Varints {
    const char* p;
    const char* end;
    explicit Varints(protozero::data_view v) : p(v.data()), end(v.data() + v.size()) {}
    bool empty() const { return p >= end; }
    uint64_t next() { return protozero::decode_varint(&p, end); }
    int64_t next_sint() { return protozero::decode_zigzag64(next()); }
    int32_t next_sint32() { return protozero::decode_zigzag32(static_cast<uint32_t>(next())); }
};

inline void add_tag(uint64_t key, uint64_t value, const std::vector<std::string_view>& table, ObjectStore& out) {
    out.tags.push_back({string_at(table, key), string_at(table, value)});
    out.source.tag_sids.push_back(static_cast<uint32_t>(key));
    out.source.tag_sids.push_back(static_cast<uint32_t>(value));
}

// osmium's build_tag_list: pairs while both lists last.
inline void add_tags(protozero::data_view keys, protozero::data_view vals,
                     const std::vector<std::string_view>& table, ObjectStore& out, OsmObject& o) {
    o.first_tag = static_cast<uint32_t>(out.tags.size());
    for (Varints k(keys), v(vals); !k.empty() && !v.empty();) add_tag(k.next(), v.next(), table, out);
    o.tag_count = static_cast<uint32_t>(out.tags.size()) - o.first_tag;
}

// Appends a decoded object with its user's string index and, for a way or
// relation whose lists were skipped, its message.
inline void add_object(const OsmObject& o, uint32_t user_sid, std::string_view message, ObjectStore& out) {
    out.objects.push_back(o);
    out.source.user_sids.push_back(user_sid);
    out.source.messages.push_back(message);
}

inline int group_type(uint32_t tag) {
    switch (tag) {
        case PrimitiveGroupTag::NODES:
        case PrimitiveGroupTag::DENSE: return int(OsmType::Node);
        case PrimitiveGroupTag::WAYS: return int(OsmType::Way);
        case PrimitiveGroupTag::RELATIONS: return int(OsmType::Relation);
        default: return -1;
    }
}

inline void decode_dense(protozero::pbf_reader dense, const std::vector<std::string_view>& table,
                         const BlockScale& scale, ObjectStore& out) {
    protozero::data_view ids, lats, lons, kvs, versions, timestamps, changesets, uids, user_sids, visibles;
    bool has_info = false;
    while (dense.next()) {
        switch (dense.tag()) {
            case DenseNodesTag::ID: ids = dense.get_view(); break;
            case DenseNodesTag::LAT: lats = dense.get_view(); break;
            case DenseNodesTag::LON: lons = dense.get_view(); break;
            case DenseNodesTag::KEYS_VALS: kvs = dense.get_view(); break;
            case DenseNodesTag::DENSEINFO: {
                has_info = true;
                protozero::pbf_reader info = dense.get_message();
                while (info.next()) {
                    switch (info.tag()) {
                        case DenseInfoTag::VERSION: versions = info.get_view(); break;
                        case DenseInfoTag::TIMESTAMP: timestamps = info.get_view(); break;
                        case DenseInfoTag::CHANGESET: changesets = info.get_view(); break;
                        case DenseInfoTag::UID: uids = info.get_view(); break;
                        case DenseInfoTag::USER_SID: user_sids = info.get_view(); break;
                        case DenseInfoTag::VISIBLE: visibles = info.get_view(); break;
                        default: info.skip();
                    }
                }
                break;
            }
            default: dense.skip();
        }
    }
    Varints id_it(ids), lat_it(lats), lon_it(lons), kv_it(kvs), ver_it(versions), ts_it(timestamps),
        cs_it(changesets), uid_it(uids), user_it(user_sids), vis_it(visibles);
    int64_t id = 0, lat = 0, lon = 0, ts = 0, cs = 0, uid = 0, user = 0;
    while (!id_it.empty()) {
        if (lat_it.empty() || lon_it.empty()) format_error("dense node without coordinates");
        OsmObject o;
        o.type = OsmType::Node;
        uint32_t user_sid = kNoSid;
        id += id_it.next_sint();
        o.id = id;
        if (has_info) {
            if (!ver_it.empty()) o.version = to_version(static_cast<int32_t>(ver_it.next()));
            if (!cs_it.empty()) o.changeset = to_changeset(cs += cs_it.next_sint());
            if (!ts_it.empty()) o.timestamp = to_timestamp(ts += ts_it.next_sint(), scale);
            if (!uid_it.empty()) o.uid = to_uid(static_cast<int32_t>(uid += uid_it.next_sint32()));
            if (!vis_it.empty()) o.visible = vis_it.next() != 0;
            if (!user_it.empty()) {
                user += user_it.next_sint32();
                o.user = string_at(table, static_cast<uint64_t>(user));
                user_sid = static_cast<uint32_t>(user);
            }
        }
        lat += lat_it.next_sint();
        lon += lon_it.next_sint();
        if (o.visible) {
            o.lat = to_coordinate(lat, scale.lat_offset, scale);
            o.lon = to_coordinate(lon, scale.lon_offset, scale);
        }
        o.first_tag = static_cast<uint32_t>(out.tags.size());
        while (!kv_it.empty()) {
            uint64_t k = kv_it.next();
            if (k == 0) break;
            if (kv_it.empty()) format_error("dense node key without value");
            add_tag(k, kv_it.next(), table, out);
        }
        o.tag_count = static_cast<uint32_t>(out.tags.size()) - o.first_tag;
        add_object(o, user_sid, {}, out);
    }
}

inline void decode_node(protozero::pbf_reader msg, const std::vector<std::string_view>& table,
                        const BlockScale& scale, ObjectStore& out) {
    OsmObject o;
    o.type = OsmType::Node;
    protozero::data_view keys, vals;
    constexpr int64_t kMissing = INT64_MAX;
    int64_t lat = kMissing, lon = kMissing;
    uint32_t user_sid = kNoSid;
    while (msg.next()) {
        switch (msg.tag()) {
            case NodeTag::ID: o.id = msg.get_sint64(); break;
            case NodeTag::KEYS: keys = msg.get_view(); break;
            case NodeTag::VALS: vals = msg.get_view(); break;
            case NodeTag::INFO: user_sid = decode_info(msg.get_message(), table, scale, o); break;
            case NodeTag::LAT: lat = msg.get_sint64(); break;
            case NodeTag::LON: lon = msg.get_sint64(); break;
            default: msg.skip();
        }
    }
    if (o.visible) {
        if (lat == kMissing || lon == kMissing) format_error("node without coordinates");
        o.lat = to_coordinate(lat, scale.lat_offset, scale);
        o.lon = to_coordinate(lon, scale.lon_offset, scale);
    }
    add_tags(keys, vals, table, out, o);
    add_object(o, user_sid, {}, out);
}

// A way; with skip_lists only its id and Info, keeping its message.
inline void decode_way(protozero::data_view message, const std::vector<std::string_view>& table,
                       const BlockScale& scale, bool skip_lists, ObjectStore& out) {
    OsmObject o;
    o.type = OsmType::Way;
    protozero::data_view keys, vals;
    uint32_t user_sid = kNoSid;
    o.first_ref = static_cast<uint32_t>(out.refs.size());
    for (protozero::pbf_reader msg(message); msg.next();) {
        switch (msg.tag()) {
            case WayTag::ID: o.id = msg.get_int64(); break;
            case WayTag::INFO: user_sid = decode_info(msg.get_message(), table, scale, o); break;
            case WayTag::KEYS: keys = msg.get_view(); break;
            case WayTag::VALS: vals = msg.get_view(); break;
            case WayTag::REFS: {
                if (skip_lists) {
                    msg.skip();
                    break;
                }
                int64_t ref = 0;
                for (int64_t delta : msg.get_packed_sint64()) out.refs.push_back(ref += delta);
                break;
            }
            default: msg.skip();
        }
    }
    o.ref_count = static_cast<uint32_t>(out.refs.size()) - o.first_ref;
    if (skip_lists) {
        add_object(o, user_sid, std::string_view(message.data(), message.size()), out);
        return;
    }
    add_tags(keys, vals, table, out, o);
    add_object(o, user_sid, {}, out);
}

// A relation; with skip_lists only its id and Info, keeping its message.
inline void decode_relation(protozero::data_view message, const std::vector<std::string_view>& table,
                            const BlockScale& scale, bool skip_lists, ObjectStore& out) {
    OsmObject o;
    o.type = OsmType::Relation;
    protozero::data_view keys, vals, roles, memids, types;
    uint32_t user_sid = kNoSid;
    for (protozero::pbf_reader msg(message); msg.next();) {
        switch (msg.tag()) {
            case RelationTag::ID: o.id = msg.get_int64(); break;
            case RelationTag::KEYS: keys = msg.get_view(); break;
            case RelationTag::VALS: vals = msg.get_view(); break;
            case RelationTag::INFO: user_sid = decode_info(msg.get_message(), table, scale, o); break;
            case RelationTag::ROLES_SID: roles = msg.get_view(); break;
            case RelationTag::MEMIDS: memids = msg.get_view(); break;
            case RelationTag::TYPES: types = msg.get_view(); break;
            default: msg.skip();
        }
    }
    o.first_member = static_cast<uint32_t>(out.members.size());
    if (skip_lists) {
        add_object(o, user_sid, std::string_view(message.data(), message.size()), out);
        return;
    }
    // osmium's decode_relation: members while all three lists last.
    int64_t ref = 0;
    for (Varints r(roles), m(memids), t(types); !r.empty() && !m.empty() && !t.empty();) {
        int32_t role = static_cast<int32_t>(r.next());
        int32_t type = static_cast<int32_t>(t.next());
        if (type < 0 || type > 2) format_error("unknown relation member type");
        if (role < 0) format_error("negative role index");
        out.members.push_back({ref += m.next_sint(), string_at(table, uint64_t(role)), static_cast<OsmType>(type)});
        out.source.role_sids.push_back(static_cast<uint32_t>(role));
    }
    o.member_count = static_cast<uint32_t>(out.members.size()) - o.first_member;
    add_tags(keys, vals, table, out, o);
    add_object(o, user_sid, {}, out);
}

}  // namespace pbf_codec_detail

// What decode_block keeps of a way or relation.
enum class DecodeLists : uint8_t {
    All,
    // Only its id and metadata when its message can be copied as is into a
    // block that keeps the string table (encode_block with this store as
    // the seed): what merging it needs.
    UnlessCopyable,
};

// Decodes the PrimitiveBlock `raw` into `out` (cleared first) as osmium's
// PBF reader does: coordinates truncated to 1e-7 degrees, timestamps scaled
// by date_granularity, a negative uid as 0, version or changeset -1 as 0.
// Strings are views into raw; out.source keeps the string table and each
// string's index. Returns the type the objects share, or -1 for a block
// without objects; throws on a block mixing types.
inline int decode_block(std::string_view raw, ObjectStore& out, DecodeLists lists = DecodeLists::All) {
    using namespace pbf_codec_detail;
    out.clear();
    std::vector<std::string_view>& table = out.source.strings;
    BlockScale scale;
    protozero::pbf_reader block(raw.data(), raw.size());
    while (block.next()) {
        switch (block.tag()) {
            case PrimitiveBlockTag::STRINGTABLE: {
                protozero::pbf_reader st = block.get_message();
                while (st.next()) {
                    if (st.tag() == StringTableTag::S) {
                        auto v = st.get_view();
                        table.emplace_back(v.data(), v.size());
                    } else {
                        st.skip();
                    }
                }
                break;
            }
            case PrimitiveBlockTag::GRANULARITY: scale.granularity = block.get_int32(); break;
            case PrimitiveBlockTag::DATE_GRANULARITY: scale.date_granularity = block.get_int32(); break;
            case PrimitiveBlockTag::LAT_OFFSET: scale.lat_offset = block.get_int64(); break;
            case PrimitiveBlockTag::LON_OFFSET: scale.lon_offset = block.get_int64(); break;
            default: block.skip();
        }
    }
    // An Info timestamp counts in its block's date granularity; encode_block
    // writes the default one.
    const bool skip_lists = lists == DecodeLists::UnlessCopyable && scale.date_granularity == 1000;
    int type = -1;
    protozero::pbf_reader groups(raw.data(), raw.size());
    while (groups.next(PrimitiveBlockTag::PRIMITIVEGROUP)) {
        protozero::pbf_reader group = groups.get_message();
        while (group.next()) {
            int t = group_type(group.tag());
            if (t < 0) {
                group.skip();  // changesets: not objects, osmium skips them too
                continue;
            }
            if (type >= 0 && t != type) format_error("block mixes object types");
            type = t;
            switch (group.tag()) {
                case PrimitiveGroupTag::NODES: decode_node(group.get_message(), table, scale, out); break;
                case PrimitiveGroupTag::DENSE: decode_dense(group.get_message(), table, scale, out); break;
                case PrimitiveGroupTag::WAYS: decode_way(group.get_view(), table, scale, skip_lists, out); break;
                case PrimitiveGroupTag::RELATIONS:
                    decode_relation(group.get_view(), table, scale, skip_lists, out);
                    break;
            }
        }
    }
    return type;
}

namespace pbf_codec_detail {

// A block's string table: index 0 stays the empty placeholder dense nodes
// use as their tag terminator, so every string, "" too, gets an index >= 1
// (osmium's StringTable does the same). Seeded with a decoded block's
// table, it keeps that table's indices and adds strings after it.
class StringTableBuilder {
public:
    StringTableBuilder(const std::vector<std::string_view>* seed, size_t expected) {
        if (seed && !seed->empty()) strings_ = *seed;
        else strings_.emplace_back();
        index_.reserve(expected);
    }
    uint32_t add(std::string_view s) {
        auto [it, inserted] = index_.try_emplace(s, static_cast<uint32_t>(strings_.size()));
        if (inserted) strings_.push_back(s);
        return it->second;
    }
    const std::vector<std::string_view>& strings() const { return strings_; }

private:
    std::unordered_map<std::string_view, uint32_t> index_;
    std::vector<std::string_view> strings_;
};

// The string indices of the objects one block encodes: those of the seed
// store's own objects as decoded, the rest's added to the table.
class StringIndexer {
public:
    StringIndexer(const ObjectStore* seed, size_t objects)
        : seed_(seed && !seed->source.strings.empty() ? seed : nullptr),
          table_(seed_ ? &seed_->source.strings : nullptr, objects) {}

    uint32_t user(const ObjectRef& r) {
        if (from_seed(r)) {
            uint32_t sid = seed_->source.user_sids[index(r)];
            if (sid != kNoSid) return sid;
        }
        return table_.add(r.object->user);
    }
    uint32_t key(const ObjectRef& r, uint32_t t) {
        return from_seed(r) ? seed_->source.tag_sids[2 * (size_t(r.object->first_tag) + t)]
                            : table_.add(r.store->tags_of(*r.object)[t].key);
    }
    uint32_t value(const ObjectRef& r, uint32_t t) {
        return from_seed(r) ? seed_->source.tag_sids[2 * (size_t(r.object->first_tag) + t) + 1]
                            : table_.add(r.store->tags_of(*r.object)[t].value);
    }
    uint32_t role(const ObjectRef& r, uint32_t m) {
        return from_seed(r) ? seed_->source.role_sids[size_t(r.object->first_member) + m]
                            : table_.add(r.store->members_of(*r.object)[m].role);
    }
    // The way's or relation's message as decoded, empty if not kept: its
    // string indices hold in this table.
    std::string_view message(const ObjectRef& r) const {
        return from_seed(r) ? seed_->source.messages[index(r)] : std::string_view();
    }
    const std::vector<std::string_view>& strings() const { return table_.strings(); }

private:
    bool from_seed(const ObjectRef& r) const { return seed_ && r.store == seed_; }
    size_t index(const ObjectRef& r) const { return size_t(r.object - seed_->objects.data()); }

    const ObjectStore* seed_;
    StringTableBuilder table_;
};

inline void add_info(protozero::pbf_writer& parent, int tag, const ObjectRef& r, StringIndexer& ix) {
    const OsmObject& o = *r.object;
    protozero::pbf_writer info(parent, tag);
    info.add_int32(InfoTag::VERSION, static_cast<int32_t>(o.version));
    info.add_int64(InfoTag::TIMESTAMP, o.timestamp);
    info.add_int64(InfoTag::CHANGESET, o.changeset);
    info.add_int32(InfoTag::UID, static_cast<int32_t>(o.uid));
    info.add_uint32(InfoTag::USER_SID, ix.user(r));
}

inline void add_keys_vals(protozero::pbf_writer& msg, int keys_tag, int vals_tag, const ObjectRef& r,
                          StringIndexer& ix) {
    {
        protozero::packed_field_uint32 keys(msg, keys_tag);
        for (uint32_t t = 0; t < r.object->tag_count; t++) keys.add_element(ix.key(r, t));
    }
    protozero::packed_field_uint32 vals(msg, vals_tag);
    for (uint32_t t = 0; t < r.object->tag_count; t++) vals.add_element(ix.value(r, t));
}

inline void encode_dense(const ObjectRef* objs, size_t n, StringIndexer& ix, std::string& group) {
    std::vector<int64_t> ids, timestamps, changesets, lats, lons;
    std::vector<int32_t> versions, uids, user_sids, kvs;
    int64_t id = 0, ts = 0, cs = 0, lat = 0, lon = 0;
    int32_t uid = 0, user = 0;
    for (size_t i = 0; i < n; i++) {
        const OsmObject& o = *objs[i].object;
        ids.push_back(o.id - id);
        id = o.id;
        versions.push_back(static_cast<int32_t>(o.version));
        timestamps.push_back(int64_t(o.timestamp) - ts);
        ts = o.timestamp;
        changesets.push_back(int64_t(o.changeset) - cs);
        cs = o.changeset;
        uids.push_back(static_cast<int32_t>(o.uid - static_cast<uint32_t>(uid)));
        uid = static_cast<int32_t>(o.uid);
        int32_t sid = static_cast<int32_t>(ix.user(objs[i]));
        user_sids.push_back(sid - user);
        user = sid;
        lats.push_back(int64_t(o.lat) - lat);
        lat = o.lat;
        lons.push_back(int64_t(o.lon) - lon);
        lon = o.lon;
        for (uint32_t t = 0; t < o.tag_count; t++) {
            kvs.push_back(static_cast<int32_t>(ix.key(objs[i], t)));
            kvs.push_back(static_cast<int32_t>(ix.value(objs[i], t)));
        }
        kvs.push_back(0);
    }
    protozero::pbf_writer pg(group);
    protozero::pbf_writer dense(pg, PrimitiveGroupTag::DENSE);
    dense.add_packed_sint64(DenseNodesTag::ID, ids.begin(), ids.end());
    {
        protozero::pbf_writer info(dense, DenseNodesTag::DENSEINFO);
        info.add_packed_int32(DenseInfoTag::VERSION, versions.begin(), versions.end());
        info.add_packed_sint64(DenseInfoTag::TIMESTAMP, timestamps.begin(), timestamps.end());
        info.add_packed_sint64(DenseInfoTag::CHANGESET, changesets.begin(), changesets.end());
        info.add_packed_sint32(DenseInfoTag::UID, uids.begin(), uids.end());
        info.add_packed_sint32(DenseInfoTag::USER_SID, user_sids.begin(), user_sids.end());
    }
    dense.add_packed_sint64(DenseNodesTag::LAT, lats.begin(), lats.end());
    dense.add_packed_sint64(DenseNodesTag::LON, lons.begin(), lons.end());
    dense.add_packed_int32(DenseNodesTag::KEYS_VALS, kvs.begin(), kvs.end());
}

inline void encode_ways(const ObjectRef* objs, size_t n, StringIndexer& ix, std::string& group) {
    protozero::pbf_writer pg(group);
    for (size_t i = 0; i < n; i++) {
        std::string_view kept = ix.message(objs[i]);
        if (!kept.empty()) {
            pg.add_message(PrimitiveGroupTag::WAYS, kept.data(), kept.size());
            continue;
        }
        const OsmObject& o = *objs[i].object;
        protozero::pbf_writer way(pg, PrimitiveGroupTag::WAYS);
        way.add_int64(WayTag::ID, o.id);
        add_keys_vals(way, WayTag::KEYS, WayTag::VALS, objs[i], ix);
        add_info(way, WayTag::INFO, objs[i], ix);
        protozero::packed_field_sint64 refs(way, WayTag::REFS);
        const int64_t* r = objs[i].store->refs_of(o);
        int64_t prev = 0;
        for (uint32_t k = 0; k < o.ref_count; k++) {
            refs.add_element(r[k] - prev);
            prev = r[k];
        }
    }
}

inline void encode_relations(const ObjectRef* objs, size_t n, StringIndexer& ix, std::string& group) {
    protozero::pbf_writer pg(group);
    for (size_t i = 0; i < n; i++) {
        std::string_view kept = ix.message(objs[i]);
        if (!kept.empty()) {
            pg.add_message(PrimitiveGroupTag::RELATIONS, kept.data(), kept.size());
            continue;
        }
        const OsmObject& o = *objs[i].object;
        protozero::pbf_writer rel(pg, PrimitiveGroupTag::RELATIONS);
        rel.add_int64(RelationTag::ID, o.id);
        add_keys_vals(rel, RelationTag::KEYS, RelationTag::VALS, objs[i], ix);
        add_info(rel, RelationTag::INFO, objs[i], ix);
        const OsmMember* m = objs[i].store->members_of(o);
        {
            protozero::packed_field_int32 roles(rel, RelationTag::ROLES_SID);
            for (uint32_t k = 0; k < o.member_count; k++) roles.add_element(static_cast<int32_t>(ix.role(objs[i], k)));
        }
        {
            protozero::packed_field_sint64 memids(rel, RelationTag::MEMIDS);
            int64_t prev = 0;
            for (uint32_t k = 0; k < o.member_count; k++) {
                memids.add_element(m[k].ref - prev);
                prev = m[k].ref;
            }
        }
        protozero::packed_field_int32 types(rel, RelationTag::TYPES);
        for (uint32_t k = 0; k < o.member_count; k++) types.add_element(static_cast<int32_t>(m[k].type));
    }
}

}  // namespace pbf_codec_detail

// Encodes objs[0, n), all of `type` and in order, as one PrimitiveBlock
// laid out like osmium's PBF writer's (string table first, one group, dense
// nodes, granularity 100, full metadata). With a seed, a store decode_block
// filled, the block starts from the seed's string table, so the seed's own
// objects keep their string indices and its kept way and relation messages
// are copied as they are.
inline void encode_block(OsmType type, const ObjectRef* objs, size_t n, std::string& raw,
                         const ObjectStore* seed = nullptr) {
    using namespace pbf_codec_detail;
    StringIndexer ix(seed, n);
    std::string group;
    switch (type) {
        case OsmType::Node: encode_dense(objs, n, ix, group); break;
        case OsmType::Way: encode_ways(objs, n, ix, group); break;
        case OsmType::Relation: encode_relations(objs, n, ix, group); break;
    }
    raw.clear();
    protozero::pbf_writer block(raw);
    {
        protozero::pbf_writer table(block, PrimitiveBlockTag::STRINGTABLE);
        for (std::string_view s : ix.strings()) table.add_bytes(StringTableTag::S, s.data(), s.size());
    }
    block.add_message(PrimitiveBlockTag::PRIMITIVEGROUP, group);
}

// Appends `raw` to `out` as a PBF blob of `type` ("OSMHeader" or "OSMData"):
// length prefix, BlobHeader, zlib Blob.
inline void append_blob(std::string& out, std::string_view type, std::string_view raw,
                        int level = kBlobZlibLevel) {
    if (raw.size() > kMaxBlockBytes) throw std::runtime_error("PBF block over 32 MiB");
    uLongf zsize = compressBound(static_cast<uLong>(raw.size()));
    std::string z(zsize, '\0');
    if (compress2(reinterpret_cast<Bytef*>(z.data()), &zsize, reinterpret_cast<const Bytef*>(raw.data()),
                  static_cast<uLong>(raw.size()), level) != Z_OK)
        throw std::runtime_error("zlib compress failed");
    std::string blob;
    {
        protozero::pbf_writer b(blob);
        b.add_int32(BlobTag::RAW_SIZE, static_cast<int32_t>(raw.size()));
        b.add_bytes(BlobTag::ZLIB, z.data(), zsize);
    }
    std::string header;
    {
        protozero::pbf_writer h(header);
        h.add_string(BlobHeaderTag::TYPE, type.data(), type.size());
        h.add_int32(BlobHeaderTag::DATASIZE, static_cast<int32_t>(blob.size()));
    }
    uint32_t n = static_cast<uint32_t>(header.size());
    char prefix[4] = {char(n >> 24), char(n >> 16), char(n >> 8), char(n)};
    out.append(prefix, 4);
    out += header;
    out += blob;
}

// --- First object of a blob ---

struct BlobPeek {
    int type = -1;          // OsmType of the first object, -1 for none
    int64_t first_id = 0;
    size_t bytes_read = 0;  // of the blob's data, for the scan's statistics
};

namespace pbf_peek_detail {

enum class Parse { Found, NeedMore, Empty };

// A varint from [p, end); false if the buffer ends inside it.
inline bool read_varint(const uint8_t*& p, const uint8_t* end, uint64_t& v) {
    v = 0;
    for (int shift = 0; shift < 64; shift += 7) {
        if (p >= end) return false;
        uint8_t b = *p++;
        v |= uint64_t(b & 0x7f) << shift;
        if (!(b & 0x80)) return true;
    }
    throw std::runtime_error("PBF format error: varint too long");
}

// Steps p over a field of wire type wt; p may land past end (the field
// continues beyond the bytes at hand). False if its length is cut off.
inline bool skip_field(const uint8_t*& p, const uint8_t* end, uint32_t wt) {
    uint64_t v;
    switch (wt) {
        case 0: return read_varint(p, end, v);
        case 1: p += 8; return true;
        case 2:
            if (!read_varint(p, end, v)) return false;
            p += v;
            return true;
        case 5: p += 4; return true;
        default: throw std::runtime_error("PBF format error: unsupported wire type");
    }
}

// The first object's type and id in a PrimitiveBlock of which only
// [p, end) is at hand; `complete` says it is the whole block. A block
// prefix suffices because the id is an object's first field in practice
// and the string table, which comes first, is only stepped over.
inline Parse first_object(const uint8_t* p, const uint8_t* end, bool complete, BlobPeek& out) {
    const Parse need = complete ? Parse::Empty : Parse::NeedMore;
    while (p < end) {
        uint64_t key, len;
        if (!read_varint(p, end, key)) return need;
        if ((key >> 3) != PrimitiveBlockTag::PRIMITIVEGROUP || (key & 7) != 2) {
            if (!skip_field(p, end, key & 7)) return need;
            continue;
        }
        if (!read_varint(p, end, len)) return need;
        const uint8_t* group_end = p + len;
        while (p < group_end) {
            if (p >= end) return need;
            uint64_t gkey;
            if (!read_varint(p, end, gkey)) return need;
            uint32_t gtag = uint32_t(gkey >> 3);
            int type = pbf_codec_detail::group_type(gtag);
            if (type < 0 || (gkey & 7) != 2) {
                if (!skip_field(p, end, gkey & 7)) return need;
                continue;
            }
            uint64_t mlen;
            if (!read_varint(p, end, mlen)) return need;
            const uint8_t* msg_end = p + mlen;
            const uint8_t* q = p;
            while (q < msg_end) {
                if (q >= end) return need;
                uint64_t fkey, v;
                if (!read_varint(q, end, fkey)) return need;
                if ((fkey >> 3) != 1) {
                    if (!skip_field(q, end, fkey & 7)) return need;
                    continue;
                }
                if (gtag == PrimitiveGroupTag::DENSE) {
                    uint64_t ids_len;
                    if (!read_varint(q, end, ids_len)) return need;
                    if (ids_len == 0) break;  // no nodes in this group
                    if (!read_varint(q, end, v)) return need;
                    out.first_id = protozero::decode_zigzag64(v);
                } else {
                    if (!read_varint(q, end, v)) return need;
                    out.first_id = gtag == PrimitiveGroupTag::NODES ? protozero::decode_zigzag64(v) : int64_t(v);
                }
                out.type = type;
                return Parse::Found;
            }
            if (q >= msg_end && gtag != PrimitiveGroupTag::DENSE) {
                out.first_id = 0;  // no id field: the proto default
                out.type = type;
                return Parse::Found;
            }
            p = msg_end;
        }
        p = group_end;
    }
    return need;
}

}  // namespace pbf_peek_detail

// The type and id of the first object in the data blob `info` of fd,
// inflating only as much of it as that takes: reads `first_read` bytes,
// then twice as many each time more are needed.
inline BlobPeek peek_first_object(int fd, const BlobInfo& info, size_t first_read = kPeekFirstReadBytes) {
    using namespace pbf_peek_detail;
    BlobPeek out;
    const size_t data_offset = info.offset + 4 + info.header_size;
    std::string buf;
    size_t have = 0;  // bytes of the blob read into buf
    auto read_to = [&](size_t want) {
        want = std::min(want, info.data_size);
        if (want <= have) return;
        buf.resize(want);
        ssize_t n = pread(fd, buf.data() + have, want - have, static_cast<off_t>(data_offset + have));
        if (n != ssize_t(want - have)) throw std::runtime_error("pread failed in blob peek");
        have = want;
    };
    read_to(first_read);

    // Find the payload field in the Blob message.
    const uint8_t* b = reinterpret_cast<const uint8_t*>(buf.data());
    const uint8_t* p = b;
    uint64_t key = 0, len = 0;
    for (;;) {
        if (!read_varint(p, b + have, key)) throw std::runtime_error("PBF format error: truncated blob");
        uint32_t tag = uint32_t(key >> 3), wt = uint32_t(key & 7);
        if ((tag == BlobTag::RAW || tag == BlobTag::ZLIB) && wt == 2) {
            if (!read_varint(p, b + have, len)) throw std::runtime_error("PBF format error: truncated blob");
            break;
        }
        if (tag > BlobTag::ZLIB) throw std::runtime_error("unsupported PBF blob compression (only raw and zlib)");
        if (!skip_field(p, b + have, wt)) throw std::runtime_error("PBF format error: truncated blob");
    }
    const size_t payload_start = size_t(p - b);
    if (payload_start + len > info.data_size) throw std::runtime_error("PBF format error: blob payload overruns blob");

    if ((key >> 3) == BlobTag::RAW) {
        for (size_t want = first_read;; want *= 2) {
            read_to(want);
            const uint8_t* d = reinterpret_cast<const uint8_t*>(buf.data()) + payload_start;
            size_t avail = std::min<size_t>(len, have - payload_start);
            Parse r = first_object(d, d + avail, avail == len, out);
            if (r != Parse::NeedMore) break;
        }
        out.bytes_read = have;
        return out;
    }

    z_stream z{};
    if (inflateInit(&z) != Z_OK) throw std::runtime_error("inflateInit failed");
    struct End {
        z_stream& z;
        ~End() { inflateEnd(&z); }
    } end{z};
    std::string raw;
    size_t consumed = payload_start;  // blob bytes fed to zlib
    size_t want = first_read;
    bool done = false;
    for (;;) {
        if (consumed == have && consumed < payload_start + len) read_to(want *= 2);
        size_t avail_in = std::min(have, payload_start + len) - consumed;
        z.next_in = reinterpret_cast<Bytef*>(buf.data() + consumed);
        z.avail_in = static_cast<uInt>(avail_in);
        size_t produced = raw.size();
        raw.resize(produced + std::max<size_t>(2 * avail_in, 32 * 1024));
        z.next_out = reinterpret_cast<Bytef*>(raw.data() + produced);
        z.avail_out = static_cast<uInt>(raw.size() - produced);
        int ret = inflate(&z, Z_SYNC_FLUSH);
        if (ret != Z_OK && ret != Z_STREAM_END && ret != Z_BUF_ERROR)
            throw std::runtime_error("zlib inflate failed in blob peek: " + std::to_string(ret));
        consumed += avail_in - z.avail_in;
        raw.resize(raw.size() - z.avail_out);
        done = ret == Z_STREAM_END;
        if (!done && ret == Z_BUF_ERROR && consumed == payload_start + len)
            throw std::runtime_error("PBF format error: truncated zlib blob");
        const uint8_t* d = reinterpret_cast<const uint8_t*>(raw.data());
        if (first_object(d, d + raw.size(), done, out) != Parse::NeedMore) break;
    }
    out.bytes_read = have;
    return out;
}

// --- Header block ---

struct PbfHeader {
    struct Box {
        int64_t left = 0, right = 0, top = 0, bottom = 0;  // nanodegrees
    };
    std::optional<Box> bbox;
    std::vector<std::string> required_features, optional_features;
    std::string writingprogram, source;
    std::optional<int64_t> replication_timestamp, replication_sequence;
    std::string replication_base_url;
};

inline PbfHeader decode_header_block(std::string_view raw) {
    PbfHeader h;
    protozero::pbf_reader pb(raw.data(), raw.size());
    while (pb.next()) {
        switch (pb.tag()) {
            case HeaderBlockTag::BBOX: {
                PbfHeader::Box box;
                protozero::pbf_reader b = pb.get_message();
                while (b.next()) {
                    switch (b.tag()) {
                        case HeaderBBoxTag::LEFT: box.left = b.get_sint64(); break;
                        case HeaderBBoxTag::RIGHT: box.right = b.get_sint64(); break;
                        case HeaderBBoxTag::TOP: box.top = b.get_sint64(); break;
                        case HeaderBBoxTag::BOTTOM: box.bottom = b.get_sint64(); break;
                        default: b.skip();
                    }
                }
                h.bbox = box;
                break;
            }
            case HeaderBlockTag::REQUIRED_FEATURES: h.required_features.push_back(pb.get_string()); break;
            case HeaderBlockTag::OPTIONAL_FEATURES: h.optional_features.push_back(pb.get_string()); break;
            case HeaderBlockTag::WRITINGPROGRAM: h.writingprogram = pb.get_string(); break;
            case HeaderBlockTag::SOURCE: h.source = pb.get_string(); break;
            case HeaderBlockTag::REPLICATION_TIMESTAMP: h.replication_timestamp = pb.get_int64(); break;
            case HeaderBlockTag::REPLICATION_SEQUENCE_NUMBER: h.replication_sequence = pb.get_int64(); break;
            case HeaderBlockTag::REPLICATION_BASE_URL: h.replication_base_url = pb.get_string(); break;
            default: pb.skip();
        }
    }
    return h;
}

inline std::string encode_header_block(const PbfHeader& h) {
    std::string raw;
    protozero::pbf_writer pb(raw);
    if (h.bbox) {
        protozero::pbf_writer b(pb, HeaderBlockTag::BBOX);
        b.add_sint64(HeaderBBoxTag::LEFT, h.bbox->left);
        b.add_sint64(HeaderBBoxTag::RIGHT, h.bbox->right);
        b.add_sint64(HeaderBBoxTag::TOP, h.bbox->top);
        b.add_sint64(HeaderBBoxTag::BOTTOM, h.bbox->bottom);
    }
    for (const auto& f : h.required_features) pb.add_string(HeaderBlockTag::REQUIRED_FEATURES, f);
    for (const auto& f : h.optional_features) pb.add_string(HeaderBlockTag::OPTIONAL_FEATURES, f);
    if (!h.writingprogram.empty()) pb.add_string(HeaderBlockTag::WRITINGPROGRAM, h.writingprogram);
    if (!h.source.empty()) pb.add_string(HeaderBlockTag::SOURCE, h.source);
    if (h.replication_timestamp) pb.add_int64(HeaderBlockTag::REPLICATION_TIMESTAMP, *h.replication_timestamp);
    if (h.replication_sequence) pb.add_int64(HeaderBlockTag::REPLICATION_SEQUENCE_NUMBER, *h.replication_sequence);
    if (!h.replication_base_url.empty()) pb.add_string(HeaderBlockTag::REPLICATION_BASE_URL, h.replication_base_url);
    return raw;
}
