// OSM objects that own their strings, for the pbf-apply tests: build
// ObjectStores from them, write them as PBF and change files, read PBFs
// back, and print them one line each for comparisons.
#pragma once

#include <cstdio>
#include <fcntl.h>
#include <string>
#include <tuple>
#include <unistd.h>
#include <utility>
#include <vector>

#include "osm_objects.h"
#include "pbf_codec.h"
#include "pbf_reader.h"

struct TestObject {
    OsmType type = OsmType::Node;
    int64_t id = 0;
    uint32_t version = 1, timestamp = 0, changeset = 0, uid = 0;
    std::string user;
    int32_t lon = kUndefinedCoordinate, lat = kUndefinedCoordinate;
    bool visible = true;
    std::vector<std::pair<std::string, std::string>> tags;
    std::vector<int64_t> refs;
    std::vector<std::tuple<OsmType, int64_t, std::string>> members;
};

inline std::string describe(const TestObject& o) {
    std::string s = "nwr"[int(o.type)] + std::to_string(o.id) + " v" + std::to_string(o.version) + " t" +
                    std::to_string(o.timestamp) + " c" + std::to_string(o.changeset) + " i" + std::to_string(o.uid) +
                    " u" + o.user + (o.visible ? " V" : " D");
    if (o.type == OsmType::Node) s += " x" + std::to_string(o.lon) + " y" + std::to_string(o.lat);
    s += " T";
    for (const auto& [k, v] : o.tags) s += k + "=" + v + ",";
    s += " N";
    for (int64_t r : o.refs) s += std::to_string(r) + ",";
    s += " M";
    for (const auto& [t, r, role] : o.members) s += "nwr"[int(t)] + std::to_string(r) + "@" + role + ",";
    return s;
}

inline std::vector<std::string> describe(const std::vector<TestObject>& objs) {
    std::vector<std::string> out;
    for (const auto& o : objs) out.push_back(describe(o));
    return out;
}

inline TestObject test_object(const OsmObject& o, const ObjectStore& s) {
    TestObject t;
    t.type = o.type;
    t.id = o.id;
    t.version = o.version;
    t.timestamp = o.timestamp;
    t.changeset = o.changeset;
    t.uid = o.uid;
    t.user = std::string(o.user);
    t.lon = o.lon;
    t.lat = o.lat;
    t.visible = o.visible;
    for (uint32_t i = 0; i < o.tag_count; i++)
        t.tags.emplace_back(std::string(s.tags_of(o)[i].key), std::string(s.tags_of(o)[i].value));
    t.refs.assign(s.refs_of(o), s.refs_of(o) + o.ref_count);
    for (uint32_t i = 0; i < o.member_count; i++) {
        const OsmMember& m = s.members_of(o)[i];
        t.members.emplace_back(m.type, m.ref, std::string(m.role));
    }
    return t;
}

// An ObjectStore viewing `objs`, which must outlive it.
inline ObjectStore make_store(const std::vector<TestObject>& objs) {
    ObjectStore s;
    for (const TestObject& t : objs) {
        OsmObject o;
        o.type = t.type;
        o.id = t.id;
        o.version = t.version;
        o.timestamp = t.timestamp;
        o.changeset = t.changeset;
        o.uid = t.uid;
        o.user = t.user;
        o.lon = t.lon;
        o.lat = t.lat;
        o.visible = t.visible;
        o.first_tag = uint32_t(s.tags.size());
        for (const auto& [k, v] : t.tags) s.tags.push_back({k, v});
        o.tag_count = uint32_t(t.tags.size());
        o.first_ref = uint32_t(s.refs.size());
        s.refs.insert(s.refs.end(), t.refs.begin(), t.refs.end());
        o.ref_count = uint32_t(t.refs.size());
        o.first_member = uint32_t(s.members.size());
        for (const auto& [type, ref, role] : t.members) s.members.push_back({ref, role, type});
        o.member_count = uint32_t(t.members.size());
        s.objects.push_back(o);
    }
    return s;
}

// Writes objs (sorted by type then id) as a PBF of at most per_block
// objects a blob. Each block also states the default granularity, which
// pbf-apply's encoder leaves out, so a re-encoded blob never matches an
// input blob byte for byte.
inline void write_test_pbf(const std::string& path, const std::vector<TestObject>& objs, size_t per_block,
                           const PbfHeader& header = {}) {
    std::string out;
    append_blob(out, "OSMHeader", encode_header_block(header));
    ObjectStore store = make_store(objs);
    std::string raw;
    for (size_t b = 0; b < objs.size();) {
        size_t e = b;
        while (e < objs.size() && e - b < per_block && objs[e].type == objs[b].type) e++;
        std::vector<ObjectRef> refs;
        for (size_t i = b; i < e; i++) refs.push_back({&store.objects[i], &store});
        encode_block(objs[b].type, refs.data(), refs.size(), raw);
        protozero::pbf_writer(raw).add_int32(PrimitiveBlockTag::GRANULARITY, 100);
        append_blob(out, "OSMData", raw);
        b = e;
    }
    std::FILE* f = std::fopen(path.c_str(), "wb");
    std::fwrite(out.data(), 1, out.size(), f);
    std::fclose(f);
}

// The objects of a PBF's data blobs, in file order.
inline std::vector<TestObject> read_test_pbf(const std::string& path, std::vector<size_t>* blob_sizes = nullptr) {
    std::vector<TestObject> out;
    std::vector<BlobInfo> blobs = scan_pbf_blobs(path, 1);
    int fd = open(path.c_str(), O_RDONLY);
    for (size_t i = 1; i < blobs.size(); i++) {
        std::string payload = read_and_decompress_blob(fd, blobs[i]);
        ObjectStore s;
        decode_block(payload, s);
        if (blob_sizes) blob_sizes->push_back(s.objects.size());
        for (const OsmObject& o : s.objects) out.push_back(test_object(o, s));
    }
    close(fd);
    return out;
}
