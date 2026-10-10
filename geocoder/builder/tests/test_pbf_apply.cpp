// Unit tests for pbf-apply (tools/pbf_apply.h): each merge rule of osmium
// apply-changes, and whole runs on synthetic files checked against a
// literal re-implementation of osmium-tool's CommandApplyChanges::run().
#include "pbf_apply.h"

#include <algorithm>
#include <cstdio>
#include <iterator>
#include <set>
#include <string>
#include <tuple>
#include <vector>

#include "pbf_test_objects.h"
#include "scratch_dir.h"
#include "test_framework.h"

namespace {

constexpr uint32_t T0 = 1783502088;

struct Change {
    char action;  // 'c', 'm' or 'd'
    TestObject object;
};
using ChangeFile = std::vector<Change>;

uint64_t positive_id(int64_t id) { return id < 0 ? uint64_t(0) - uint64_t(id) : uint64_t(id); }

// osmium-tool's CommandApplyChanges::run() without history: reverse the
// change list, stable_sort it by object_order_type_id_reverse_version,
// set_union it with the input, copy_first_with_id.
std::vector<TestObject> osmium_apply(const std::vector<TestObject>& input, const std::vector<ChangeFile>& files) {
    std::vector<TestObject> changes;
    for (const auto& f : files)
        for (const auto& c : f) {
            changes.push_back(c.object);
            changes.back().visible = c.action != 'd';
        }
    auto order = [](const TestObject* l, const TestObject* r) {
        bool both = l->timestamp != 0 && r->timestamp != 0;
        return std::make_tuple(int(l->type), l->id > 0, positive_id(l->id), r->version, both ? r->timestamp : 0u) <
               std::make_tuple(int(r->type), r->id > 0, positive_id(r->id), l->version, both ? l->timestamp : 0u);
    };
    std::vector<const TestObject*> objects, in, merged;
    for (const auto& c : changes) objects.push_back(&c);
    std::reverse(objects.begin(), objects.end());
    std::stable_sort(objects.begin(), objects.end(), order);
    for (const auto& o : input) in.push_back(&o);
    std::set_union(objects.begin(), objects.end(), in.begin(), in.end(), std::back_inserter(merged), order);
    std::vector<TestObject> out;
    int64_t id = 0;
    for (const TestObject* o : merged) {
        if (o->id != id) {
            if (o->visible) out.push_back(*o);
            id = o->id;
        }
    }
    return out;
}

std::string xml_escape(const std::string& s) {
    std::string out;
    for (char c : s) {
        switch (c) {
            case '&': out += "&amp;"; break;
            case '<': out += "&lt;"; break;
            case '>': out += "&gt;"; break;
            case '"': out += "&quot;"; break;
            case '\n': out += "&#10;"; break;
            default: out += c;
        }
    }
    return out;
}

std::string coordinate(int32_t v) {
    char buf[32];
    std::snprintf(buf, sizeof buf, "%s%d.%07d", v < 0 ? "-" : "", std::abs(v / 10000000), std::abs(v % 10000000));
    return buf;
}

std::string to_osc(const ChangeFile& file) {
    static const char* sections[] = {"create", "modify", "delete"};
    std::string out = "<?xml version='1.0' encoding='UTF-8'?>\n<osmChange version=\"0.6\" generator=\"test\">\n";
    const char* open = nullptr;
    for (const Change& c : file) {
        const char* section = sections[c.action == 'c' ? 0 : c.action == 'm' ? 1 : 2];
        if (open != section) {
            if (open) out += std::string("  </") + open + ">\n";
            out += std::string("  <") + section + ">\n";
            open = section;
        }
        const TestObject& o = c.object;
        static const char* names[] = {"node", "way", "relation"};
        out += std::string("    <") + names[int(o.type)] + " id=\"" + std::to_string(o.id) + "\" version=\"" +
               std::to_string(o.version) + "\"";
        if (o.timestamp) {
            std::time_t t = o.timestamp;
            char ts[32];
            std::strftime(ts, sizeof ts, "%Y-%m-%dT%H:%M:%SZ", std::gmtime(&t));
            out += std::string(" timestamp=\"") + ts + "\"";
        }
        out += " uid=\"" + std::to_string(o.uid) + "\" user=\"" + xml_escape(o.user) + "\" changeset=\"" +
               std::to_string(o.changeset) + "\"";
        if (o.type == OsmType::Node && o.lat != kUndefinedCoordinate)
            out += " lat=\"" + coordinate(o.lat) + "\" lon=\"" + coordinate(o.lon) + "\"";
        out += ">\n";
        for (int64_t r : o.refs) out += "      <nd ref=\"" + std::to_string(r) + "\"/>\n";
        for (const auto& [t, r, role] : o.members)
            out += std::string("      <member type=\"") + names[int(t)] + "\" ref=\"" + std::to_string(r) +
                   "\" role=\"" + xml_escape(role) + "\"/>\n";
        for (const auto& [k, v] : o.tags)
            out += "      <tag k=\"" + xml_escape(k) + "\" v=\"" + xml_escape(v) + "\"/>\n";
        out += std::string("    </") + names[int(o.type)] + ">\n";
    }
    if (open) out += std::string("  </") + open + ">\n";
    return out + "</osmChange>\n";
}

struct Run {
    std::vector<TestObject> output;
    std::vector<TestObject> expected;
    std::vector<size_t> blob_objects;
    size_t verbatim_blobs = 0;  // output blobs byte-identical to an input blob
    ApplyStats stats;
};

std::set<std::string> raw_blobs(const std::string& path) {
    std::set<std::string> out;
    std::vector<BlobInfo> blobs = scan_pbf_blobs(path, 1);
    std::FILE* f = std::fopen(path.c_str(), "rb");
    for (size_t i = 1; i < blobs.size(); i++) {
        std::string b(4 + blobs[i].header_size + blobs[i].data_size, '\0');
        std::fseek(f, long(blobs[i].offset), SEEK_SET);
        if (std::fread(b.data(), 1, b.size(), f) != b.size()) break;
        out.insert(b);
    }
    std::fclose(f);
    return out;
}

Run run_apply(const std::vector<TestObject>& input, const std::vector<ChangeFile>& files, size_t input_per_block = 5,
              size_t max_block_objects = 4) {
    ScratchDir dir("pbf-apply-test");
    ApplyOptions opt;
    opt.input = dir.path() + "/in.osm.pbf";
    opt.output = dir.path() + "/out.osm.pbf";
    write_test_pbf(opt.input, input, input_per_block);
    for (size_t f = 0; f < files.size(); f++) {
        std::string path = dir.path() + "/c" + std::to_string(f) + ".osc";
        std::string text = to_osc(files[f]);
        std::FILE* fp = std::fopen(path.c_str(), "wb");
        std::fwrite(text.data(), 1, text.size(), fp);
        std::fclose(fp);
        opt.changes.push_back(path);
    }
    opt.max_block_objects = max_block_objects;
    opt.change_chunk_bytes = 300;
    opt.threads = 3;
    opt.verbose = false;
    Run run;
    run.stats = apply_changes(opt);
    run.output = read_test_pbf(opt.output, &run.blob_objects);
    run.expected = osmium_apply(input, files);
    std::set<std::string> in = raw_blobs(opt.input), out = raw_blobs(opt.output);
    for (const auto& b : out) run.verbatim_blobs += in.count(b);
    return run;
}

TestObject node(int64_t id, uint32_t version, uint32_t timestamp = T0, const std::string& tag = "") {
    TestObject o;
    o.id = id;
    o.version = version;
    o.timestamp = timestamp;
    o.lon = int32_t(id * 1000);
    o.lat = int32_t(-id * 1000);
    if (!tag.empty()) o.tags = {{"t", tag}};
    return o;
}

TestObject of_type(OsmType type, TestObject o) {
    o.type = type;
    if (type != OsmType::Node) o.lon = o.lat = kUndefinedCoordinate;
    return o;
}

// A small deterministic generator.
struct Lcg {
    uint64_t s;
    uint32_t next(uint32_t n) {
        s = s * 6364136223846793005ull + 1442695040888963407ull;
        return uint32_t((s >> 33) % n);
    }
};

}  // namespace

TEST(apply_merge_rules_match_osmium) {
    struct Case {
        const char* name;
        std::vector<TestObject> input;
        std::vector<ChangeFile> files;
        std::vector<std::string> expected;
    };
    TestObject n1 = node(1, 1, T0, "in");
    auto line = [](const TestObject& o) { return describe(o); };
    const Case cases[] = {
        {"the newest change wins whatever its position",
         {n1},
         {{{'m', node(1, 3, T0, "a")}, {'m', node(1, 2, T0, "b")}}},
         {line(node(1, 3, T0, "a"))}},
        {"the last-read change wins a tie, across files",
         {n1},
         {{{'m', node(1, 2, T0, "a")}}, {{'m', node(1, 2, T0, "b")}}},
         {line(node(1, 2, T0, "b"))}},
        {"a later timestamp breaks a version tie",
         {n1},
         {{{'m', node(1, 2, T0 + 1, "a")}, {'m', node(1, 2, T0, "b")}}},
         {line(node(1, 2, T0 + 1, "a"))}},
        {"a missing timestamp ties",
         {n1},
         {{{'m', node(1, 2, T0 + 1, "a")}, {'m', node(1, 2, 0, "b")}}},
         {line(node(1, 2, 0, "b"))}},
        {"a newer input object survives a stale change",
         {node(1, 5, T0, "in")},
         {{{'m', node(1, 4, T0, "a")}}},
         {line(node(1, 5, T0, "in"))}},
        {"a change equal to the input object wins",
         {node(1, 2, T0, "in")},
         {{{'m', node(1, 2, T0, "a")}}},
         {line(node(1, 2, T0, "a"))}},
        {"the newest change being a delete removes the object", {n1}, {{{'d', node(1, 2)}}}, {}},
        {"a delete older than a modify keeps the object",
         {n1},
         {{{'m', node(1, 3, T0, "a")}, {'d', node(1, 2)}}},
         {line(node(1, 3, T0, "a"))}},
        {"a delete of a missing object writes nothing", {n1}, {{{'d', node(2, 2)}}}, {line(n1)}},
        {"a create for an existing id is a newer version like any other",
         {n1},
         {{{'c', node(1, 2, T0, "a")}}},
         {line(node(1, 2, T0, "a"))}},
        {"objects before the first and after the last id land in order",
         {node(5, 1), node(6, 1)},
         {{{'c', node(9, 1)}, {'c', node(1, 1)}, {'c', node(-3, 1)}}},
         {line(node(-3, 1)), line(node(1, 1)), line(node(5, 1)), line(node(6, 1)), line(node(9, 1))}},
    };
    for (const Case& c : cases) {
        Run run = run_apply(c.input, c.files);
        bool ok = describe(run.output) == c.expected && describe(run.expected) == c.expected;
        if (!ok) std::printf("    case: %s\n", c.name);
        CHECK(ok);
    }
}

TEST(apply_drops_a_first_id_equal_to_the_previous_types_last_like_osmium) {
    // copy_first_with_id remembers only the id: way 7 follows node 7, and
    // the first object (id 0) follows the initial id 0.
    std::vector<TestObject> input = {node(0, 1), node(3, 1), node(7, 1), of_type(OsmType::Way, node(7, 1)),
                                     of_type(OsmType::Way, node(9, 1)), of_type(OsmType::Relation, node(1, 1))};
    Run run = run_apply(input, {{{'c', of_type(OsmType::Relation, node(2, 1))}}});
    CHECK(describe(run.output) == describe(run.expected));
    REQUIRE(run.output.size() == 5);
    CHECK_EQ(run.output[0].id, int64_t(3));
    CHECK(run.output[2].type == OsmType::Way && run.output[2].id == 9);
}

TEST(apply_matches_osmium_on_random_files_and_copies_untouched_blobs) {
    for (uint64_t seed = 1; seed <= 12; seed++) {
        Lcg rng{seed};
        std::vector<TestObject> input;
        const int64_t max_id[3] = {240, 90, 30};
        const char* values[] = {"a", "b&c", "x<y>", "q\"r", "line\nbreak", ""};
        for (int t = 0; t < 3; t++) {
            for (int64_t id = 1; id <= max_id[t]; id++) {
                if (rng.next(4) == 0) continue;
                TestObject o = of_type(OsmType(t), node(id, 1 + rng.next(3), T0 - rng.next(1000)));
                o.uid = rng.next(5);
                o.user = o.uid ? "user" + std::to_string(o.uid) : "";
                o.changeset = rng.next(100000);
                if (rng.next(3) == 0) o.tags = {{"k" + std::to_string(rng.next(4)), values[rng.next(6)]}};
                if (t == 1)
                    for (uint32_t k = rng.next(5); k-- > 0;) o.refs.push_back(1 + rng.next(240));
                if (t == 2)
                    for (uint32_t k = rng.next(4); k-- > 0;)
                        o.members.emplace_back(OsmType(rng.next(3)), 1 + rng.next(90), values[rng.next(6)]);
                input.push_back(o);
            }
        }
        std::vector<ChangeFile> files(2);
        for (auto& file : files) {
            for (uint32_t n = 20 + rng.next(40); n-- > 0;) {
                int t = int(rng.next(3));
                int64_t id = int64_t(rng.next(uint32_t(max_id[t] + 20))) - 3;
                TestObject o = of_type(OsmType(t), node(id, 1 + rng.next(4), rng.next(5) ? T0 + rng.next(3) : 0));
                o.user = "editor";
                o.uid = 99;
                if (rng.next(2)) o.tags = {{"name", values[rng.next(6)]}};
                if (t == 1) o.refs = {int64_t(rng.next(250)), 13000000000};
                if (t == 2) o.members = {{OsmType::Way, 5, "outer"}};
                char action = "cmmd"[rng.next(4)];
                file.push_back({action, o});
            }
        }
        // Creates past every existing id, enough for tail blobs.
        for (int t = 0; t < 3; t++)
            for (int64_t id = max_id[t] + 30; id < max_id[t] + 40; id++)
                files[1].push_back({'c', of_type(OsmType(t), node(id, 1))});
        Run run = run_apply(input, files);
        bool same = describe(run.output) == describe(run.expected);
        if (!same) std::printf("    seed %llu differs\n", (unsigned long long)seed);
        CHECK(same);
        CHECK(run.stats.copied_blobs > 0);
        CHECK(run.stats.merged_blobs > 0);
        CHECK_EQ(run.verbatim_blobs, run.stats.copied_blobs);
        CHECK(std::all_of(run.blob_objects.begin(), run.blob_objects.end(), [](size_t n) { return n >= 1 && n <= 5; }));
    }
}

TEST(apply_writes_the_header_the_options_ask_for) {
    ScratchDir dir("pbf-apply-test");
    ApplyOptions opt;
    opt.input = dir.path() + "/in.osm.pbf";
    opt.output = dir.path() + "/out.osm.pbf";
    PbfHeader in;
    in.bbox = PbfHeader::Box{-1, 2, 3, -4};
    in.required_features = {"OsmSchema-V0.6", "DenseNodes"};
    in.source = "test source";
    in.replication_timestamp = 5;
    in.replication_sequence = 6;
    in.replication_base_url = "https://example.org/replication";
    write_test_pbf(opt.input, {node(1, 1)}, 5, in);
    std::string change = dir.path() + "/c.osc";
    std::string text = to_osc({{'m', node(1, 2)}});
    std::FILE* fp = std::fopen(change.c_str(), "wb");
    std::fwrite(text.data(), 1, text.size(), fp);
    std::fclose(fp);
    opt.changes = {change};
    opt.verbose = false;
    opt.replication_timestamp = T0;
    apply_changes(opt);
    std::vector<BlobInfo> blobs = scan_pbf_blobs(opt.output, 1);
    int fd = open(opt.output.c_str(), O_RDONLY);
    PbfHeader out = decode_header_block(read_and_decompress_blob(fd, blobs[0]));
    close(fd);
    REQUIRE(out.bbox.has_value());
    CHECK_EQ(out.bbox->bottom, int64_t(-4));
    CHECK(out.required_features == std::vector<std::string>({"OsmSchema-V0.6", "DenseNodes"}));
    CHECK(out.optional_features == std::vector<std::string>({"Sort.Type_then_ID"}));
    CHECK_EQ(out.source, std::string("test source"));
    CHECK(out.replication_timestamp == std::optional<int64_t>(T0));
    CHECK(!out.replication_sequence.has_value());
    CHECK_EQ(out.replication_base_url, in.replication_base_url);

    in.required_features.push_back("HistoricalInformation");
    write_test_pbf(opt.input, {node(1, 1)}, 5, in);
    bool threw = false;
    try {
        apply_changes(opt);
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);
}
