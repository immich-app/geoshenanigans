// Unit tests for PbfFile's streaming readers (src/pbf_reader.cpp): every node
// and way of a multi-blob file reaches the callback once, with its tags and
// refs, whatever the thread count and the concurrent-read limit.
#include "pbf_reader.h"

#include <algorithm>
#include <cstdlib>
#include <mutex>
#include <string>
#include <vector>

#include "pbf_test_objects.h"
#include "scratch_dir.h"
#include "test_framework.h"

namespace {

std::vector<TestObject> sample_file() {
    std::vector<TestObject> objs;
    for (int64_t id = 1; id <= 50; id++) {
        TestObject n;
        n.id = id * 3;
        n.lat = int32_t(id * 1000);
        n.lon = int32_t(-id * 700);
        if (id % 7 == 0) n.tags = {{"name", "n" + std::to_string(id)}, {"place", "town"}};
        objs.push_back(n);
    }
    for (int64_t id = 1; id <= 20; id++) {
        TestObject w;
        w.type = OsmType::Way;
        w.id = id;
        w.refs = {id * 3, id * 3 + 3, id * 3 + 6};
        if (id % 2) w.tags = {{"highway", "residential"}, {"name", "w" + std::to_string(id)}};
        objs.push_back(w);
    }
    TestObject r;
    r.type = OsmType::Relation;
    r.id = 1;
    r.members = {{OsmType::Way, 1, "outer"}};
    objs.push_back(r);
    return objs;
}

std::string tags_of(const uint32_t* keys, const uint32_t* vals, size_t ntags,
                    const std::vector<std::string>& st) {
    std::string s;
    for (size_t i = 0; i < ntags; i++) s += st[keys[i]] + "=" + st[vals[i]] + ",";
    return s;
}

std::string expected_tags(const TestObject& o) {
    std::string s;
    for (const auto& [k, v] : o.tags) s += k + "=" + v + ",";
    return s;
}

// Nodes and ways as the streaming readers deliver them, sorted.
struct Streamed {
    std::vector<std::string> nodes, ways;
};

Streamed stream(const std::string& path, unsigned threads) {
    Streamed out;
    std::mutex mutex;
    PbfFile pbf(path, threads);
    pbf.read_nodes_streaming([&](int64_t id, double lat, double lng, const uint32_t* keys,
                                 const uint32_t* vals, size_t ntags, const std::vector<std::string>& st) {
        std::string s = std::to_string(id) + " " + std::to_string(lround(lat * 1e7)) + " " +
                        std::to_string(lround(lng * 1e7)) + " " + tags_of(keys, vals, ntags, st);
        std::lock_guard<std::mutex> lock(mutex);
        out.nodes.push_back(s);
    });
    pbf.read_ways_streaming([&](int64_t id, const WayRefs& refs, const uint32_t* keys,
                                const uint32_t* vals, size_t ntags, const std::vector<std::string>& st) {
        std::string s = std::to_string(id) + " ";
        for (int64_t ref : refs.get()) s += std::to_string(ref) + ",";
        s += " " + tags_of(keys, vals, ntags, st);
        std::lock_guard<std::mutex> lock(mutex);
        out.ways.push_back(s);
    });
    std::sort(out.nodes.begin(), out.nodes.end());
    std::sort(out.ways.begin(), out.ways.end());
    return out;
}

Streamed expected(const std::vector<TestObject>& objs) {
    Streamed out;
    for (const auto& o : objs) {
        if (o.type == OsmType::Node) {
            out.nodes.push_back(std::to_string(o.id) + " " + std::to_string(o.lat) + " " + std::to_string(o.lon) +
                                " " + expected_tags(o));
        } else if (o.type == OsmType::Way) {
            std::string s = std::to_string(o.id) + " ";
            for (int64_t r : o.refs) s += std::to_string(r) + ",";
            out.ways.push_back(s + " " + expected_tags(o));
        }
    }
    std::sort(out.nodes.begin(), out.nodes.end());
    std::sort(out.ways.begin(), out.ways.end());
    return out;
}

}  // namespace

TEST(pbf_file_streams_every_node_and_way_once) {
    ScratchDir dir("pbf-file-test");
    const std::string path = dir.path() + "/sample.osm.pbf";
    const auto objs = sample_file();
    write_test_pbf(path, objs, 6);
    const Streamed want = expected(objs);
    for (unsigned threads : {1u, 3u, 8u}) {
        const Streamed got = stream(path, threads);
        CHECK(got.nodes == want.nodes);
        CHECK(got.ways == want.ways);
    }
}

TEST(pbf_file_streams_the_same_with_one_concurrent_read) {
    ScratchDir dir("pbf-file-test");
    const std::string path = dir.path() + "/sample.osm.pbf";
    const auto objs = sample_file();
    write_test_pbf(path, objs, 4);
    const char* old = getenv("BUILD_MAX_IO");
    const std::string saved = old ? old : "";
    setenv("BUILD_MAX_IO", "1", 1);
    const Streamed got = stream(path, 6);
    if (old) setenv("BUILD_MAX_IO", saved.c_str(), 1);
    else unsetenv("BUILD_MAX_IO");
    const Streamed want = expected(objs);
    CHECK(got.nodes == want.nodes);
    CHECK(got.ways == want.ways);
}
