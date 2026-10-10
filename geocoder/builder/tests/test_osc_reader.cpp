// Unit tests for the change file reader (tools/osc_reader.h): attribute
// values parsed as osmium parses them, and objects the same however the
// file is cut into chunks.
#include "osc_reader.h"

#include <cstdio>
#include <functional>
#include <string>
#include <vector>
#include <zlib.h>

#include "scratch_dir.h"
#include "test_framework.h"

namespace {

bool throws(const std::function<void()>& fn) {
    try {
        fn();
    } catch (const std::exception&) {
        return true;
    }
    return false;
}

std::string write_file(const ScratchDir& dir, const std::string& name, const std::string& text, bool gzip) {
    std::string path = dir.path() + "/" + name;
    if (gzip) {
        gzFile gz = gzopen(path.c_str(), "wb");
        gzwrite(gz, text.data(), static_cast<unsigned>(text.size()));
        gzclose(gz);
    } else {
        std::FILE* f = std::fopen(path.c_str(), "wb");
        std::fwrite(text.data(), 1, text.size(), f);
        std::fclose(f);
    }
    return path;
}

// One line per object with every field the reader fills.
std::vector<std::string> describe(const std::vector<std::unique_ptr<ChangeChunk>>& chunks) {
    std::vector<std::string> out;
    for (const auto& c : chunks) {
        const ObjectStore& s = c->store;
        for (const OsmObject& o : s.objects) {
            std::string line = "nwr"[int(o.type)] + std::to_string(o.id) + " v" + std::to_string(o.version) + " t" +
                               std::to_string(o.timestamp) + " c" + std::to_string(o.changeset) + " i" +
                               std::to_string(o.uid) + " u" + std::string(o.user) + (o.visible ? " V" : " D") + " x" +
                               std::to_string(o.lon) + " y" + std::to_string(o.lat) + " T";
            for (uint32_t i = 0; i < o.tag_count; i++)
                line += std::string(s.tags_of(o)[i].key) + "=" + std::string(s.tags_of(o)[i].value) + ",";
            line += " N";
            for (uint32_t i = 0; i < o.ref_count; i++) line += std::to_string(s.refs_of(o)[i]) + ",";
            line += " M";
            for (uint32_t i = 0; i < o.member_count; i++) {
                const OsmMember& m = s.members_of(o)[i];
                line += "nwr"[int(m.type)] + std::to_string(m.ref) + "@" + std::string(m.role) + ",";
            }
            out.push_back(line);
        }
    }
    return out;
}

const char* kChange =
    "<?xml version='1.0' encoding='UTF-8'?>\n"
    "<osmChange version=\"0.6\" generator=\"test\">\n"
    "  <modify>\n"
    "    <node id=\"1\" version=\"2\" timestamp=\"2026-07-08T09:14:48Z\" uid=\"7\" user=\"a&amp;b\" changeset=\"9\" "
    "lat=\"1.5\" lon=\"-2.25\">\n"
    "      <tag k=\"name\" v=\"x&#10;y &lt;z&gt;\"/>\n"
    "      <tag k=\"note\" v=\"a>b\"/>\n"
    "    </node>\n"
    "  </modify>\n"
    "  <delete>\n"
    "    <node id=\"2\" version=\"3\" timestamp=\"2026-07-08T09:14:49Z\" lat=\"1\" lon=\"1\"/>\n"
    "    <way id=\"5\" version=\"2\" visible=\"true\"/>\n"
    "  </delete>\n"
    "  <create>\n"
    "    <way id=\"10\" version=\"1\">\n"
    "      <nd ref=\"1\"/>\n"
    "      <nd ref=\"2\"/>\n"
    "      <tag k=\"highway\" v=\"road\"/>\n"
    "      <nd ref=\"3\"/>\n"
    "    </way>\n"
    "    <relation id=\"20\" version=\"1\">\n"
    "      <member type=\"node\" ref=\"1\" role=\"stop\"/>\n"
    "      <member type=\"way\" ref=\"10\" role=\"\"/>\n"
    "      <tag k=\"type\" v=\"route\"/>\n"
    "    </relation>\n"
    "    <node id=\"-4\" version=\"1\" lat=\"2\"/>\n"
    "  </create>\n"
    "  <create/>\n"
    "  <node id=\"30\" version=\"1\" lat=\"0\" lon=\"0\" visible=\"false\"/>\n"
    "</osmChange>\n";

const std::vector<std::string> kChangeObjects = {
    "n1 v2 t1783502088 c9 i7 ua&b V x-22500000 y15000000 Tname=x\ny <z>,note=a>b, N M",
    "n2 v3 t1783502089 c0 i0 u D x10000000 y10000000 T N M",
    "w5 v2 t0 c0 i0 u V x2147483647 y2147483647 T N M",
    "w10 v1 t0 c0 i0 u V x2147483647 y2147483647 Thighway=road, N1,2, M",
    "r20 v1 t0 c0 i0 u V x2147483647 y2147483647 Ttype=route, N Mn1@stop,w10@,",
    "n-4 v1 t0 c0 i0 u V x2147483647 y2147483647 T N M",
    "n30 v1 t0 c0 i0 u D x0 y0 T N M",
};

std::string wrap(const std::string& body) {
    return "<osmChange version=\"0.6\">\n" + body + "</osmChange>\n";
}

}  // namespace

TEST(osc_coordinates_round_like_osmium) {
    struct Case {
        const char* text;
        int32_t value;
    };
    const Case cases[] = {
        {"53.9593574", 539593574}, {"-1.0815173", -10815173}, {"1.23456785", 12345679}, {"-1.23456785", -12345679},
        {"1.23456784", 12345678},  {"0.00000005", 1},         {"-0.00000004", 0},       {"180", 1800000000},
        {"1e2", 1000000000},       {".5", 5000000},           {"12.3456789999", 123456790},
    };
    for (const Case& c : cases) CHECK_EQ(osc::parse_coordinate(c.text), c.value);
    CHECK(throws([] { osc::parse_coordinate("abc"); }));
    CHECK(throws([] { osc::parse_coordinate("1.0x"); }));
    CHECK(throws([] { osc::parse_coordinate(""); }));
    CHECK(throws([] { osc::parse_coordinate("300"); }));
}

TEST(osc_timestamps_parse_like_osmium) {
    CHECK_EQ(osc::parse_timestamp("2026-07-08T09:14:48Z"), uint32_t(1783502088));
    CHECK_EQ(osc::parse_timestamp("2026-07-08T09:14:48.75Z"), uint32_t(1783502088));
    CHECK(throws([] { osc::parse_timestamp("2026-07-08T09:14:48"); }));
    CHECK(throws([] { osc::parse_timestamp("2026-13-08T09:14:48Z"); }));
    CHECK(throws([] { osc::parse_timestamp("2026-07-08T09:14:48Zx"); }));
    CHECK(throws([] { osc::parse_timestamp("2026-07-08 09:14:48Z"); }));
}

TEST(osc_ids_and_counters_parse_like_osmium) {
    CHECK_EQ(osc::parse_id("-5"), int64_t(-5));
    CHECK_EQ(osc::parse_id("13000000000"), int64_t(13000000000));
    CHECK(throws([] { osc::parse_id(" 5"); }));
    CHECK(throws([] { osc::parse_id("5x"); }));
    CHECK_EQ(osc::parse_u32("-1", "version"), uint32_t(0));
    CHECK_EQ(osc::parse_u32("4294967294", "version"), uint32_t(4294967294u));
    CHECK(throws([] { osc::parse_u32("4294967295", "version"); }));
    CHECK(throws([] { osc::parse_u32("-2", "version"); }));
}

TEST(osc_reader_reads_sections_visibility_and_first_lists) {
    ScratchDir dir("osc-test");
    std::string path = write_file(dir, "c.osc", kChange, false);
    CHECK(describe(read_change_files({path}, kOscChunkBytes, 2)) == kChangeObjects);
}

TEST(osc_reader_gives_the_same_objects_however_the_file_is_chunked) {
    ScratchDir dir("osc-test");
    std::string path = write_file(dir, "c.osc.gz", kChange, true);
    for (size_t chunk : {size_t(1), size_t(40), size_t(97), size_t(300)}) {
        auto chunks = read_change_files({path}, chunk, 3);
        CHECK(describe(chunks) == kChangeObjects);
    }
    CHECK(read_change_files({path}, 1, 3).size() > 3);
}

TEST(osc_reader_keeps_file_order_across_files) {
    ScratchDir dir("osc-test");
    std::string a = write_file(dir, "a.osc", wrap("<create><node id=\"1\" version=\"1\" lat=\"0\" lon=\"0\"/></create>\n"), false);
    std::string b = write_file(dir, "b.osc", wrap("<delete><node id=\"1\" version=\"2\"/></delete>\n"), false);
    auto chunks = read_change_files({a, b}, kOscChunkBytes, 2);
    std::vector<std::pair<size_t, bool>> seen;  // (file, visible) in order
    for (const auto& c : chunks)
        for (const auto& o : c->store.objects) seen.emplace_back(c->file, o.visible);
    CHECK(seen == (std::vector<std::pair<size_t, bool>>{{0, true}, {1, false}}));
}

TEST(osc_reader_rejects_malformed_structure) {
    ScratchDir dir("osc-test");
    const char* bad[] = {
        "<osmChange version=\"0.6\"><create><node id=\"1\"/></osmChange>\n",
        "<osmChange version=\"0.6\"><create><create><node id=\"1\"/></create></create></osmChange>\n",
        "<osmChange version=\"0.5\"><create><node id=\"1\"/></create></osmChange>\n",
        "<osmChange version=\"0.6\"><create><node id=\"1\"/></create>\n",
        "<osm version=\"0.6\"><create><node id=\"1\"/></create></osm>\n",
        "<osmChange version=\"0.6\"><!-- c --><create><node id=\"1\"/></create></osmChange>\n",
        "<osmChange version=\"0.6\"><create><node id=\"1\"><nd ref=\"1\"/></node></create></osmChange>\n",
        "<osmChange version=\"0.6\"><create><relation id=\"1\"><member ref=\"1\"/></relation></create></osmChange>\n",
        "<?xml version='1.0' encoding='ISO-8859-1'?><osmChange version=\"0.6\"></osmChange>\n",
        "<osmChange version=\"0.6\"><?pi x?></osmChange>\n",
    };
    for (const char* text : bad) {
        std::string path = write_file(dir, "bad.osc", text, false);
        CHECK(throws([&] { read_change_files({path}, kOscChunkBytes, 2); }));
    }
}

TEST(osc_reader_skips_a_byte_order_mark) {
    ScratchDir dir("osc-test");
    std::string path = write_file(dir, "bom.osc", std::string("\xEF\xBB\xBF") + kChange, false);
    CHECK(describe(read_change_files({path}, kOscChunkBytes, 2)) == kChangeObjects);
}

TEST(osc_reader_reads_plain_osm_files_as_visible_objects) {
    ScratchDir dir("osc-test");
    std::string path =
        write_file(dir, "a.osm", "<osm version=\"0.6\"><node id=\"3\" version=\"1\" lat=\"1\" lon=\"2\"/></osm>", false);
    std::vector<std::string> objects = describe(read_change_files({path}, kOscChunkBytes, 2));
    CHECK(objects == std::vector<std::string>{"n3 v1 t0 c0 i0 u V x20000000 y10000000 T N M"});
}
