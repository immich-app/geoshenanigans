// A PBF of OSM objects laid out in boxes for the build tests, and the
// server's answers from the index build-index writes of it (through the
// query-server crate's grid_query example).
#pragma once

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "pbf_test_objects.h"
#include "scratch_dir.h"
#include "test_framework.h"
#include "tool_runs.h"

namespace pbf_fixture {

using Tags = std::vector<std::pair<std::string, std::string>>;

// The objects of a fixture, each kind numbered from its own base.
struct Fixture {
    std::vector<TestObject> nodes, ways, relations;
    int64_t next_node = 1, next_way = 1, next_relation = 1;

    int64_t node(double lat, double lng, Tags tags = {}) {
        return node_e7(static_cast<int32_t>(lat * 1e7), static_cast<int32_t>(lng * 1e7), std::move(tags));
    }

    // A node at (lat, lng) in whole e7 units, as OSM holds it.
    int64_t node_e7(int32_t lat, int32_t lng, Tags tags = {}) {
        TestObject o;
        o.type = OsmType::Node;
        o.id = next_node++;
        o.lat = lat;
        o.lon = lng;
        o.tags = std::move(tags);
        nodes.push_back(o);
        return o.id;
    }

    // A closed way round the box (south, west, north, east).
    int64_t square(double s, double w, double n, double e, Tags tags = {}) {
        const int64_t a = node(s, w), b = node(s, e), c = node(n, e), d = node(n, w);
        TestObject o;
        o.type = OsmType::Way;
        o.id = next_way++;
        o.refs = {a, b, c, d, a};
        o.tags = std::move(tags);
        ways.push_back(o);
        return o.id;
    }

    // An open way through new nodes at the points.
    int64_t line(const std::vector<std::pair<double, double>>& points, Tags tags = {}) {
        TestObject o;
        o.type = OsmType::Way;
        o.id = next_way++;
        for (const auto& [lat, lng] : points) o.refs.push_back(node(lat, lng));
        o.tags = std::move(tags);
        ways.push_back(o);
        return o.id;
    }

    // A relation of the box's way as its outer ring, and its other members.
    int64_t area(double s, double w, double n, double e, Tags tags,
                 std::vector<std::tuple<OsmType, int64_t, std::string>> members = {}) {
        TestObject o;
        o.type = OsmType::Relation;
        o.id = next_relation++;
        o.tags = std::move(tags);
        o.members.emplace_back(OsmType::Way, square(s, w, n, e), "outer");
        for (auto& m : members) o.members.push_back(std::move(m));
        relations.push_back(o);
        return o.id;
    }

    int64_t boundary(double s, double w, double n, double e, int level, const std::string& name, Tags more = {},
                     std::vector<std::tuple<OsmType, int64_t, std::string>> members = {}) {
        Tags tags = {{"type", "boundary"}, {"boundary", "administrative"}, {"admin_level", std::to_string(level)},
                     {"name", name}};
        tags.insert(tags.end(), more.begin(), more.end());
        return area(s, w, n, e, tags, std::move(members));
    }

    int64_t country(double s, double w, double n, double e, const std::string& name, const std::string& code) {
        return boundary(s, w, n, e, 2, name, {{"ISO3166-1", code}, {"ISO3166-1:alpha2", code}});
    }

    std::vector<TestObject> objects() const {
        std::vector<TestObject> all = nodes;
        all.insert(all.end(), ways.begin(), ways.end());
        all.insert(all.end(), relations.begin(), relations.end());
        return all;
    }
};

// The server's answers (its query_json, through the grid_query example) at
// each point from the index under dir, one JSON line each; none when it
// can't load the index. Its stderr goes to log.
inline std::vector<std::string> server_answers(const std::string& dir, const std::vector<std::pair<double, double>>& points,
                                               const std::string& scratch, const std::string& log) {
#ifdef GEOCODER_GRID_QUERY_BIN
    const std::string coords = scratch + ".coords", answers = scratch + ".answers";
    {
        std::ofstream f(coords);
        f.precision(10);
        for (const auto& [lat, lng] : points) f << lat << " " << lng << "\n";
    }
    const std::string cmd = gctest::quoted(GEOCODER_GRID_QUERY_BIN) + " " + gctest::quoted(dir) + " < " +
                            gctest::quoted(coords) + " > " + gctest::quoted(answers) + " 2>> " + gctest::quoted(log);
    if (std::system(cmd.c_str()) != 0) return {};
    std::vector<std::string> lines;
    std::ifstream f(answers);
    for (std::string line; std::getline(f, line);) lines.push_back(line);
    return lines;
#else
    (void)dir, (void)points, (void)scratch, (void)log;
    std::printf("    no grid_query: configure with cargo on PATH to build the server's read path\n");
    return {};
#endif
}

// Whether the answer names `value` as address field `field`.
inline bool names(const std::string& answer, const std::string& field, const std::string& value) {
    return answer.find("\"" + field + "\":\"" + value + "\"") != std::string::npos;
}

// The answers at the points from a build of the fixture, one each.
inline std::vector<std::string> answers_of(const Fixture& fixture, const std::string& name,
                                           const std::vector<std::pair<double, double>>& points) {
    const ScratchDir root(name);
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log";
    write_test_pbf(pbf, fixture.objects(), 1000);
    const std::string out = root.path() + "/out";
    if (!gctest::run({GEOCODER_BUILD_INDEX_BIN, out, pbf}, log)) return {};
    return server_answers(out + "/planet", points, root.path() + "/answers", log);
}

}  // namespace pbf_fixture

using namespace pbf_fixture;
