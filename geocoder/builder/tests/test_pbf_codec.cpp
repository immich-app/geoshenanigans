// Unit tests for the PBF block codec (tools/pbf_codec.h): blocks round
// trip with every field, a blob's first object is found from a prefix,
// and the header block round trips.
#include "pbf_codec.h"

#include <string>
#include <vector>

#include "pbf_test_objects.h"
#include "scratch_dir.h"
#include "test_framework.h"

namespace {

std::vector<TestObject> sample_nodes() {
    std::vector<TestObject> v(4);
    v[0].id = -7;
    v[0].version = 3;
    v[0].timestamp = 1783502088;
    v[0].changeset = 185327442;
    v[0].uid = 15478351;
    v[0].user = "SpaghettiMapper";
    v[0].lon = -10815173;
    v[0].lat = 539593574;
    v[0].tags = {{"name", "York"}, {"", "empty key"}, {"empty value", ""}};
    v[1].id = 0;
    v[1].lon = 1800000000;
    v[1].lat = -900000000;
    v[2].id = 129;
    v[2].user = "SpaghettiMapper";
    v[2].tags = {{"name", "York"}};
    v[3].id = 13000000000;  // no location: undefined coordinates
    v[3].version = 0;
    return v;
}

std::vector<TestObject> sample_ways() {
    std::vector<TestObject> v(2);
    for (auto& w : v) w.type = OsmType::Way;
    v[0].id = 5;
    v[0].refs = {13000000000, 1, 13000000000, -2};
    v[0].tags = {{"highway", "residential"}};
    v[0].uid = 1;
    v[0].user = "u";
    v[1].id = 1300000000;
    v[1].version = 2;
    return v;
}

std::vector<TestObject> sample_relations() {
    std::vector<TestObject> v(2);
    for (auto& r : v) r.type = OsmType::Relation;
    v[0].id = 3;
    v[0].members = {{OsmType::Way, 5, "outer"}, {OsmType::Node, -7, ""}, {OsmType::Relation, 3, "subarea"}};
    v[0].tags = {{"type", "multipolygon"}};
    v[1].id = 4;
    v[1].timestamp = 1;
    return v;
}

std::vector<TestObject> round_trip(OsmType type, const std::vector<TestObject>& objs) {
    ObjectStore store = make_store(objs);
    std::vector<ObjectRef> refs;
    for (const auto& o : store.objects) refs.push_back({&o, &store});
    std::string raw;
    encode_block(type, refs.data(), refs.size(), raw);
    ObjectStore back;
    int got = decode_block(raw, back);
    std::vector<TestObject> out;
    if (got != int(type)) return out;
    for (const auto& o : back.objects) out.push_back(test_object(o, back));
    return out;
}

}  // namespace

TEST(pbf_codec_round_trips_nodes_with_metadata_and_empty_strings) {
    auto nodes = sample_nodes();
    CHECK(describe(round_trip(OsmType::Node, nodes)) == describe(nodes));
}

TEST(pbf_codec_round_trips_ways_and_relations) {
    auto ways = sample_ways();
    auto relations = sample_relations();
    CHECK(describe(round_trip(OsmType::Way, ways)) == describe(ways));
    CHECK(describe(round_trip(OsmType::Relation, relations)) == describe(relations));
}

TEST(pbf_codec_rejects_a_block_mixing_object_types) {
    auto nodes = sample_nodes();
    auto ways = sample_ways();
    ObjectStore ns = make_store(nodes), ws = make_store(ways);
    std::vector<ObjectRef> n{{&ns.objects[0], &ns}}, w{{&ws.objects[0], &ws}};
    std::string a, b;
    encode_block(OsmType::Node, n.data(), 1, a);
    encode_block(OsmType::Way, w.data(), 1, b);
    // Two blocks' fields concatenated are one block with both groups; the
    // way's string indices are off, so only the type check is reached.
    ObjectStore out;
    bool threw = false;
    try {
        decode_block(a + b, out);
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);
}

TEST(pbf_codec_decodes_granularity_and_offsets_like_osmium) {
    // lat = (stored * granularity + offset) / 100, truncated, in 1e-7 degrees.
    std::string raw;
    {
        protozero::pbf_writer block(raw);
        {
            protozero::pbf_writer st(block, PrimitiveBlockTag::STRINGTABLE);
            st.add_bytes(StringTableTag::S, "", 0);
        }
        std::string group;
        {
            protozero::pbf_writer pg(group);
            protozero::pbf_writer dense(pg, PrimitiveGroupTag::DENSE);
            std::vector<int64_t> ids{10}, lats{7}, lons{-7};
            dense.add_packed_sint64(DenseNodesTag::ID, ids.begin(), ids.end());
            dense.add_packed_sint64(DenseNodesTag::LAT, lats.begin(), lats.end());
            dense.add_packed_sint64(DenseNodesTag::LON, lons.begin(), lons.end());
        }
        block.add_message(PrimitiveBlockTag::PRIMITIVEGROUP, group);
        block.add_int32(PrimitiveBlockTag::GRANULARITY, 1000);
        block.add_int64(PrimitiveBlockTag::LAT_OFFSET, 55);
        block.add_int64(PrimitiveBlockTag::LON_OFFSET, -55);
    }
    ObjectStore out;
    REQUIRE(decode_block(raw, out) == int(OsmType::Node));
    REQUIRE(out.objects.size() == 1);
    CHECK_EQ(out.objects[0].lat, (7 * 1000 + 55) / 100);
    CHECK_EQ(out.objects[0].lon, (-7 * 1000 - 55) / 100);
    CHECK_EQ(out.objects[0].version, uint32_t(0));
}

TEST(pbf_peek_finds_the_first_object_from_a_prefix) {
    ScratchDir dir("pbf-codec-test");
    std::string path = dir.path() + "/t.osm.pbf";
    // Nodes with long distinct tags make a string table bigger than the
    // first read, so the peek has to read on.
    std::vector<TestObject> objs;
    for (int i = 0; i < 3000; i++) {
        TestObject o;
        o.id = 1000 + i;
        o.lon = i;
        o.lat = -i;
        o.tags = {{"k" + std::to_string(i * 7919 % 3001), "value-" + std::to_string(i * 104729)}};
        objs.push_back(o);
    }
    auto ways = sample_ways();
    auto relations = sample_relations();
    objs.insert(objs.end(), ways.begin(), ways.end());
    objs.insert(objs.end(), relations.begin(), relations.end());
    write_test_pbf(path, objs, 3000);
    std::vector<BlobInfo> blobs = scan_pbf_blobs(path, 1);
    REQUIRE(blobs.size() == 4);
    int fd = open(path.c_str(), O_RDONLY);
    for (size_t first_read : {size_t(64), size_t(16 * 1024)}) {
        BlobPeek n = peek_first_object(fd, blobs[1], first_read);
        CHECK_EQ(n.type, int(OsmType::Node));
        CHECK_EQ(n.first_id, int64_t(1000));
        BlobPeek w = peek_first_object(fd, blobs[2], first_read);
        CHECK_EQ(w.type, int(OsmType::Way));
        CHECK_EQ(w.first_id, int64_t(5));
        BlobPeek r = peek_first_object(fd, blobs[3], first_read);
        CHECK_EQ(r.type, int(OsmType::Relation));
        CHECK_EQ(r.first_id, int64_t(3));
    }
    close(fd);

    // Untagged nodes: a small string table, so a prefix suffices.
    std::string plain = dir.path() + "/plain.osm.pbf";
    std::vector<TestObject> untagged(8000);
    for (int i = 0; i < 8000; i++) {
        untagged[i].id = 77 + 3 * i;
        // Scattered coordinates, so the blob doesn't shrink to nothing.
        untagged[i].lon = int32_t(int64_t(uint32_t(i) * 2654435761u % 3600000000u) - 1800000000);
        untagged[i].lat = int32_t(uint32_t(i) * 40503u * 40503u % 1800000000u) - 900000000;
    }
    write_test_pbf(plain, untagged, 8000);
    std::vector<BlobInfo> plain_blobs = scan_pbf_blobs(plain, 1);
    REQUIRE(plain_blobs.size() == 2);
    fd = open(plain.c_str(), O_RDONLY);
    BlobPeek p = peek_first_object(fd, plain_blobs[1], 64);
    CHECK_EQ(p.first_id, int64_t(77));
    CHECK(p.bytes_read < plain_blobs[1].data_size / 4);
    close(fd);
}

TEST(pbf_peek_reports_a_block_without_objects) {
    ScratchDir dir("pbf-codec-test");
    std::string path = dir.path() + "/empty.osm.pbf";
    write_test_pbf(path, {}, 10);
    std::string bytes;
    append_blob(bytes, "OSMData", std::string());
    std::FILE* f = std::fopen(path.c_str(), "ab");
    std::fwrite(bytes.data(), 1, bytes.size(), f);
    std::fclose(f);
    std::vector<BlobInfo> blobs = scan_pbf_blobs(path, 1);
    REQUIRE(blobs.size() == 2);
    int fd = open(path.c_str(), O_RDONLY);
    CHECK_EQ(peek_first_object(fd, blobs[1]).type, -1);
    close(fd);
}

TEST(pbf_header_block_round_trips) {
    PbfHeader h;
    h.bbox = PbfHeader::Box{-1800000000, 1800000000, 262778100, -571648200};
    h.required_features = {"OsmSchema-V0.6", "DenseNodes"};
    h.optional_features = {"Sort.Type_then_ID"};
    h.writingprogram = "pbf-apply";
    h.source = "src";
    h.replication_timestamp = 1783502088;
    h.replication_sequence = 4811;
    h.replication_base_url = "https://planet.openstreetmap.org/replication/day";
    PbfHeader b = decode_header_block(encode_header_block(h));
    REQUIRE(b.bbox.has_value());
    CHECK_EQ(b.bbox->left, h.bbox->left);
    CHECK_EQ(b.bbox->bottom, h.bbox->bottom);
    CHECK(b.required_features == h.required_features);
    CHECK(b.optional_features == h.optional_features);
    CHECK_EQ(b.source, h.source);
    CHECK(b.replication_timestamp == h.replication_timestamp);
    CHECK(b.replication_sequence == h.replication_sequence);
    CHECK_EQ(b.replication_base_url, h.replication_base_url);
    CHECK(!decode_header_block(encode_header_block(PbfHeader{})).bbox.has_value());
}
