// build-index's admin dirs: every region's boundary tiers (with
// --multi-quality) and both admin-minimal dirs (with --multi-output) hold
// their admin geometry as arcs, and nothing else does; a chained build of the
// same data (--prev-output) writes them again unchanged. A chained build of
// edited data keeps the slot of every arc the previous day had, which a fresh
// build of it doesn't, and an arc whose key changes keeps its slot by its way
// set or its ends. A previous build without its arc sidecars leaves every
// arc slot cold and the admin slots as they were; one without their fallback
// sidecars, the slots of arcs whose keys changed. Without --multi-output,
// each region's dir is a whole index the server answers from, its side files
// a value for each street and each address, its interpolation files the two
// footers of no ways, its address files the ones their records have, the
// three footers without a record; with it, a build without --multi-quality is
// refused. A building's address is written with its footprint, which the
// server takes its distance from. A fresh build writes its streets and
// addresses in cell order, whatever their names, and its postcode centroids
// in cell order in each mode dir, which a chained build writes as a fresh one
// does, and its postcode grid of a grid country's addresses in every mode dir
// alike, which the server answers from before the nearest centroid. An
// address keeps its addr:city beside the full dir's address points where a
// place or a boundary has that name, which writes no string. Every mode dir
// credits the sources of the data it holds in attribution.json:
// OpenStreetMap, GeoNames and Code-Point Open, which its GB data is derived
// from, where a postcode centroid there came from it, TIGER where an
// interpolation, a centroid or the postcode grid's votes did.
#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include <s2/s2cell_id.h>
#include <s2/s2latlng.h>

#include "addr_files.h"
#include "country_code.h"
#include "id_allocator.h"
#include "pbf_test_objects.h"
#include "poi_files.h"
#include "postcode_files.h"
#include "postcode_grid.h"
#include "scratch_dir.h"
#include "side_blocks.h"
#include "string_files.h"
#include "test_framework.h"
#include "tool_runs.h"
#include "types.h"
#include "vertex_pack.h"
#include "way_files.h"

namespace fs = std::filesystem;

// The builder's street cells (s2_helpers.h, whose own includes clash with
// the tool headers here).
void cover_edge(S2CellId c1, S2CellId c2, double lat1, double lng1, double lat2, double lng2, std::vector<S2CellId>& out);
S2CellId point_to_cell(double lat, double lng);
extern int kAdminCellLevel;

namespace {

TestObject node(int64_t id, int32_t lat, int32_t lon, std::vector<std::pair<std::string, std::string>> tags = {}) {
    TestObject o;
    o.type = OsmType::Node;
    o.id = id;
    o.lat = lat;
    o.lon = lon;
    o.tags = std::move(tags);
    return o;
}

TestObject way(int64_t id, std::vector<int64_t> refs, std::vector<std::pair<std::string, std::string>> tags = {}) {
    TestObject o;
    o.type = OsmType::Way;
    o.id = id;
    o.refs = std::move(refs);
    o.tags = std::move(tags);
    return o;
}

TestObject boundary(int64_t id, int level, const std::string& name, const std::vector<int64_t>& outer,
                    const std::vector<int64_t>& inner = {}) {
    TestObject o;
    o.type = OsmType::Relation;
    o.id = id;
    o.tags = {{"admin_level", std::to_string(level)}, {"boundary", "administrative"}, {"name", name},
              {"type", "boundary"}};
    if (level == 2) o.tags.push_back({"ISO3166-1", "CH"});
    for (int64_t w : outer) o.members.emplace_back(OsmType::Way, w, "outer");
    for (int64_t w : inner) o.members.emplace_back(OsmType::Way, w, "inner");
    return o;
}

// A country of 2 x 2 municipalities near Zurich, one with a hole a village
// of another level fills, a region over them all, a city, a street and an
// address: every arc dir has polygons, shared borders and a hole. With
// `hole_moved` (the next day's edit), the hole and its village are in the
// opposite municipality, mapped again as a new way of new nodes: that day
// has one arc fewer and one more in each set (no fallback key of the old
// one's the new one's), the rest as they were.
std::vector<TestObject> fixture(bool hole_moved = false) {
    const int32_t lat0 = 470000000, lng0 = 80000000, side = 1000000;
    std::vector<TestObject> nodes, ways, relations;
    auto corner = [](int r, int c) { return int64_t(1 + 3 * r + c); };
    for (int r = 0; r <= 2; r++)
        for (int c = 0; c <= 2; c++) nodes.push_back(node(corner(r, c), lat0 + r * side, lng0 + c * side));
    int64_t next_node = 100;
    // An edge's way, its 5 inner nodes off the straight line by up to 200 m.
    auto edge = [&](int64_t id, int64_t from, int64_t to, int32_t lat_a, int32_t lng_a, int32_t lat_b, int32_t lng_b) {
        std::vector<int64_t> refs = {from};
        for (int k = 1; k <= 5; k++) {
            const int32_t off = ((k * 7 + int(id)) % 5 - 2) * 1000;
            const int32_t lat = lat_a + (lat_b - lat_a) / 6 * k + (lat_a == lat_b ? off : 0);
            const int32_t lng = lng_a + (lng_b - lng_a) / 6 * k + (lng_a == lng_b ? off : 0);
            nodes.push_back(node(next_node, lat, lng));
            refs.push_back(next_node++);
        }
        refs.push_back(to);
        ways.push_back(way(id, refs));
    };
    auto across = [](int r, int c) { return int64_t(1000 + 2 * r + c); };
    auto upward = [](int r, int c) { return int64_t(2000 + 3 * r + c); };
    for (int r = 0; r <= 2; r++)
        for (int c = 0; c < 2; c++)
            edge(across(r, c), corner(r, c), corner(r, c + 1), lat0 + r * side, lng0 + c * side, lat0 + r * side,
                 lng0 + (c + 1) * side);
    for (int r = 0; r < 2; r++)
        for (int c = 0; c <= 2; c++)
            edge(upward(r, c), corner(r, c), corner(r + 1, c), lat0 + r * side, lng0 + c * side, lat0 + (r + 1) * side,
                 lng0 + c * side);
    const int64_t hole = hole_moved ? 3001 : 3000, hole_node = hole_moved ? 504 : 500;
    // In the first municipality, or moved to the last, clear of the city,
    // the street and the address.
    const int32_t near = hole_moved ? 1650000 : 200000, far = hole_moved ? 1850000 : 500000;
    for (int k = 0; k < 4; k++)
        nodes.push_back(node(hole_node + k, lat0 + (k == 1 || k == 2 ? far : near), lng0 + (k >= 2 ? far : near)));
    ways.push_back(way(hole, {hole_node, hole_node + 1, hole_node + 2, hole_node + 3, hole_node}));
    nodes.push_back(node(900, lat0 + 1500000, lng0 + 1500000, {{"name", "Testville"}, {"place", "city"}}));
    nodes.push_back(node(950, lat0 + 1400000, lng0 + 1400000));
    nodes.push_back(node(951, lat0 + 1400000, lng0 + 1600000));
    nodes.push_back(node(952, lat0 + 1410000, lng0 + 1500000, {{"addr:housenumber", "1"}, {"addr:street", "Teststrasse"}}));
    ways.push_back(way(4000, {950, 951}, {{"highway", "residential"}, {"name", "Teststrasse"}}));
    for (int r = 0; r < 2; r++)
        for (int c = 0; c < 2; c++)
            relations.push_back(boundary(10 + 2 * r + c, 8, "Gemeinde " + std::to_string(2 * r + c),
                                         {across(r, c), upward(r, c + 1), across(r + 1, c), upward(r, c)},
                                         r == c && r == (hole_moved ? 1 : 0) ? std::vector<int64_t>{hole}
                                                                             : std::vector<int64_t>{}));
    relations.push_back(boundary(20, 10, "Weiler", {hole}));
    const std::vector<int64_t> rim = {across(0, 0), across(0, 1), upward(0, 2), upward(1, 2),
                                      across(2, 1), across(2, 0), upward(1, 0), upward(0, 0)};
    relations.push_back(boundary(30, 4, "Kanton", rim));
    relations.push_back(boundary(40, 2, "Testland", rim));
    std::sort(nodes.begin(), nodes.end(), [](const TestObject& a, const TestObject& b) { return a.id < b.id; });
    std::vector<TestObject> all = nodes;
    all.insert(all.end(), ways.begin(), ways.end());
    all.insert(all.end(), relations.begin(), relations.end());
    return all;
}

std::vector<char> bytes_of(const fs::path& p) {
    std::ifstream f(p, std::ios::binary);
    return std::vector<char>(std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>());
}

// Every file under dir by its path there.
std::map<std::string, std::vector<char>> tree(const std::string& dir) {
    std::map<std::string, std::vector<char>> files;
    for (const auto& e : fs::recursive_directory_iterator(dir))
        if (e.is_regular_file()) files[fs::relative(e.path(), dir).string()] = bytes_of(e.path());
    return files;
}

template <class Value>
std::set<std::string> names_of(const std::map<std::string, Value>& files) {
    std::set<std::string> out;
    for (const auto& [name, value] : files) out.insert(name);
    return out;
}

const std::vector<std::string> kRegions = {"planet",        "africa",        "asia",
                                           "europe",        "north-america", "south-america",
                                           "oceania",       "central-america", "antarctica"};

// The regions with polygons and place nodes: only these have admin-minimal.
const std::set<std::string> kRegionsHeld = {"planet", "europe"};

// The admin geometry files of every region's boundary tiers and, where it
// has place nodes, its admin-minimal dirs, the own names of the polygons
// beside them.
std::set<std::string> admin_dir_layout() {
    const std::vector<std::string> client = {"admin_polygons.bin", "admin_rings.bin", "arc_index.bin", "arc_vertices.bin",
                                             kAdminOwnNamesFile};
    std::set<std::string> out;
    for (const std::string& region : kRegions)
        for (const char* dir : {"quality/simplified", "quality/exact", "admin-minimal", "admin-minimal-poi"})
            if (kRegionsHeld.count(region) || std::string(dir).rfind("quality/", 0) == 0)
                for (const std::string& f : client) out.insert(region + "/" + dir + "/" + f);
    return out;
}

// An admin dir's geometry file: not the water layer's, which has files of
// the same names (water_layer.h).
bool admin_geometry(const std::string& name) {
    const std::string file = fs::path(name).filename().string();
    return name.find("/water/") == std::string::npos &&
           (file == "admin_polygons.bin" || file == "admin_rings.bin" || file == "arc_index.bin" ||
            file == "arc_vertices.bin" || file == kAdminOwnNamesFile);
}

// An arc sidecar's slots: each live one's arc key, UINT64_MAX for a
// tombstone; none when it can't be read.
std::vector<uint64_t> slot_keys(const std::string& path) {
    gc::id_alloc::IdAllocator alloc;
    if (!alloc.load_previous(path)) return {};
    std::vector<uint64_t> keys;
    for (const gc::id_alloc::SidecarSlot& s : alloc.slots())
        keys.push_back(gc::id_alloc::is_tombstone(s) ? UINT64_MAX : s.stable_id);
    return keys;
}

std::map<uint64_t, size_t> slot_of_key(const std::vector<uint64_t>& keys) {
    std::map<uint64_t, size_t> out;
    for (size_t s = 0; s < keys.size(); s++)
        if (keys[s] != UINT64_MAX) out[keys[s]] = s;
    return out;
}

// The next day's edits of the fixture's way 1002, the border from the
// corner (1, 0) between the first and third municipality to the middle
// (1, 1), where four borders meet: its inner nodes moved 3 cm north
// (its arc's smallest vertex with them), the middle moved, or the way
// mapped again as a new one, 1009, on its inner nodes moved.
enum class Edit { InnerNodesMoved, MiddleMoved, WayRemapped };

std::vector<TestObject> edited(Edit edit) {
    std::vector<TestObject> objects = fixture();
    std::set<int64_t> inner;
    for (const TestObject& o : objects)
        if (o.type == OsmType::Way && o.id == 1002) inner.insert(o.refs.begin() + 1, o.refs.end() - 1);
    for (TestObject& o : objects) {
        if (o.type == OsmType::Node && edit != Edit::MiddleMoved && inner.count(o.id)) o.lat += 300;
        if (o.type == OsmType::Node && edit == Edit::MiddleMoved && o.id == 5) {
            o.lat += 2000;
            o.lon -= 1500;
        }
        if (edit != Edit::WayRemapped) continue;
        if (o.type == OsmType::Way && o.id == 1002) o.id = 1009;
        for (auto& [type, id, role] : o.members)
            if (type == OsmType::Way && id == 1002) id = 1009;
    }
    std::stable_sort(objects.begin(), objects.end(), [](const TestObject& a, const TestObject& b) {
        return a.type != b.type ? a.type < b.type : a.id < b.id;
    });
    return objects;
}

// The admin geometry files and arc sidecars of a build's tree.
std::map<std::string, std::vector<char>> arc_files(const std::string& out) {
    std::map<std::string, std::vector<char>> files = tree(out);
    for (auto it = files.begin(); it != files.end();) {
        const std::string file = fs::path(it->first).filename().string();
        const bool admin_sidecar = file == "arc_index.osm_ids" && it->first.find("/water/") == std::string::npos;
        it = admin_geometry(it->first) || admin_sidecar ? std::next(it) : files.erase(it);
    }
    return files;
}

// The server's answers (its query_json, through the grid_query example) at
// each point from the index under dir, one JSON line each; none when it
// can't load the index. Its stderr goes to log.
std::vector<std::string> server_answers(const std::string& dir, const std::vector<std::pair<double, double>>& points,
                                        const std::string& scratch, const std::string& log) {
#ifdef GEOCODER_GRID_QUERY_BIN
    const std::string coords = scratch + ".coords", answers = scratch + ".answers";
    {
        // Each to the 1e-7 degree of the fixtures' nodes: a stream's own 6
        // digits would round 47.14033 to 47.1403.
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

bool says(const std::string& answer, const std::string& name) {
    return answer.find("\"" + name + "\"") != std::string::npos;
}

// A JSON value's text from i on, the JSON of an answer: its strings with
// their escapes, its objects by key.
struct JsonAt {
    const std::string& s;
    size_t i;
    void space() {
        while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i]))) i++;
    }
    // The string at i, unescaped (a \u escape as written), and i past it.
    std::string string() {
        std::string out;
        for (i++; i < s.size() && s[i] != '"'; i++) {
            if (s[i] != '\\') {
                out += s[i];
                continue;
            }
            const char e = s[++i];
            out += e == 'n' ? '\n' : e == 't' ? '\t' : e == 'r' ? '\r' : e == 'b' ? '\b' : e == 'f' ? '\f' : e;
            if (e == 'u') out.insert(out.size() - 1, "\\");
        }
        i++;
        return out;
    }
    // i past the value at i.
    void skip() {
        space();
        if (i >= s.size()) return;
        if (s[i] == '"') {
            string();
        } else if (s[i] == '{' || s[i] == '[') {
            int depth = 0;
            do {
                if (s[i] == '"') {
                    string();
                    continue;
                }
                depth += s[i] == '{' || s[i] == '[';
                depth -= s[i] == '}' || s[i] == ']';
                i++;
            } while (i < s.size() && depth > 0);
        } else {
            while (i < s.size() && s[i] != ',' && s[i] != '}' && s[i] != ']') i++;
        }
    }
    // With an object at i: whether it has `key`, and i at its value.
    bool member(const std::string& key) {
        space();
        if (i >= s.size() || s[i] != '{') return false;
        i++;
        for (;;) {
            space();
            if (i >= s.size() || s[i] != '"') return false;
            const std::string k = string();
            space();
            i++;  // ':'
            space();
            if (k == key) return true;
            skip();
            space();
            if (i < s.size() && s[i] == ',') i++;
        }
    }
};

// The string at `path` (keys from the top object down) in an answer, the
// JSON after grid_query's point; none where a key of the path is missing or
// the value is no string.
std::optional<std::string> answer_string(const std::string& answer, const std::vector<std::string>& path) {
    const size_t start = answer.find('{');
    if (start == std::string::npos) return std::nullopt;
    JsonAt j{answer, start};
    for (const std::string& key : path)
        if (!j.member(key)) return std::nullopt;
    j.space();
    if (j.i >= answer.size() || answer[j.i] != '"') return std::nullopt;
    return j.string();
}

// The ways of a kind `dir` of a tree holds, as its two way files decode: each
// way's logical record, a WayHeader or an InterpWay, and the ways' nodes back
// to back. A dir without either file holds none.
template <class Record>
struct Ways {
    std::vector<Record> ways;
    std::vector<NodeCoord> nodes;
};

template <class Record>
Ways<Record> ways_of(const way_files::Kind& kind, const std::map<std::string, std::vector<char>>& files,
                     const std::string& dir) {
    REQUIRE(kind.stride == sizeof(Record));
    auto bytes = [&](const char* name) {
        const auto file = files.find(dir + name);
        return file == files.end() ? nullptr : reinterpret_cast<const uint8_t*>(file->second.data());
    };
    auto size = [&](const char* name) {
        const auto file = files.find(dir + name);
        return file == files.end() ? way_files::ABSENT : uint64_t(file->second.size());
    };
    Ways<Record> out;
    way_files::decode(
        kind, bytes(kind.ways_file), size(kind.ways_file), bytes(kind.nodes_file), size(kind.nodes_file),
        [&](const uint8_t* record) {
            out.ways.emplace_back();
            std::memcpy(&out.ways.back(), record, sizeof(Record));
        },
        [&](const uint8_t* nodes, size_t n) {
            const size_t at = out.nodes.size();
            out.nodes.resize(at + n / sizeof(NodeCoord));
            std::memcpy(out.nodes.data() + at, nodes, n);
        });
    return out;
}

// The addresses `dir` of a tree holds, as its three address files decode:
// each one's logical record, an AddrPoint, and the footprints' logical blobs
// back to back. A dir without any of the files holds none. The files are the
// ones the serial encoder writes for the addresses: addr_index.bin is what
// the other two give.
struct Addresses {
    std::vector<AddrPoint> points;
    std::vector<uint8_t> vertices;
};

Addresses addresses_of(const std::map<std::string, std::vector<char>>& files, const std::string& dir) {
    auto bytes = [&](const char* name) {
        const auto file = files.find(dir + name);
        return file == files.end() ? nullptr : reinterpret_cast<const uint8_t*>(file->second.data());
    };
    auto size = [&](const char* name) {
        const auto file = files.find(dir + name);
        return file == files.end() ? addr_files::ABSENT : uint64_t(file->second.size());
    };
    const char* const points = addr_files::POINTS_FILE;
    const char* const vertices = addr_files::VERTICES_FILE;
    const char* const index = addr_files::INDEX_FILE;
    Addresses out;
    addr_files::decode(
        bytes(points), size(points), bytes(vertices), size(vertices), bytes(index), size(index),
        [&](const uint8_t* record) {
            out.points.emplace_back();
            std::memcpy(&out.points.back(), record, sizeof(AddrPoint));
        },
        [&](const uint8_t* blob, size_t n) { out.vertices.insert(out.vertices.end(), blob, blob + n); });
    if (size(points) == addr_files::ABSENT) return out;
    const addr_files::Files encoded = addr_files::encode(reinterpret_cast<const uint8_t*>(out.points.data()), out.points.size(),
                                                         out.vertices.data(), out.vertices.size());
    auto same = [&](const char* name, const std::vector<uint8_t>& want) {
        return size(name) == want.size() && std::memcmp(bytes(name), want.data(), want.size()) == 0;
    };
    CHECK(same(points, encoded.points) && same(vertices, encoded.vertices) && same(index, encoded.index));
    return out;
}

// The side files `dir` of a tree holds, by name, each one's values: one for
// each record of the file it sits beside.
std::map<std::string, std::vector<uint32_t>> side_files(const std::map<std::string, std::vector<char>>& files,
                                                        const std::string& dir) {
    struct SideFile {
        const char* name;
        const char* parent;
    };
    std::map<std::string, std::vector<uint32_t>> out;
    for (const SideFile& side : {SideFile{"way_parents.bin", "street_ways.bin"}, SideFile{"way_postcodes.bin", "street_ways.bin"},
                                 SideFile{"addr_postcodes.bin", "addr_points.bin"}, SideFile{"addr_cities.bin", "addr_points.bin"},
                                 SideFile{"interp_postcodes.bin", "interp_ways.bin"}}) {
        const auto file = files.find(dir + side.name);
        if (file == files.end()) continue;
        std::vector<uint32_t>& values = out[side.name];
        const std::vector<char>& bytes = file->second;
        side_blocks::decode(side.name, reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size(),
                            [&](const uint32_t* v, size_t n) { values.insert(values.end(), v, v + n); });
        const auto parent = files.find(dir + side.parent);
        REQUIRE(parent != files.end());
        const std::string of = side.parent;
        const size_t records = of == "street_ways.bin"   ? ways_of<WayHeader>(way_files::STREETS, files, dir).ways.size()
                               : of == "interp_ways.bin" ? ways_of<InterpWay>(way_files::INTERPS, files, dir).ways.size()
                                                         : addresses_of(files, dir).points.size();
        CHECK_EQ(values.size(), records);
    }
    return out;
}

// The least street-level cell that lists a way of these nodes: the builder
// covers each edge between the nodes as it stores them.
uint64_t least_way_cell(const NodeCoord* nodes, size_t count) {
    uint64_t least = UINT64_MAX;
    std::vector<S2CellId> cells;
    for (size_t j = 0; j + 1 < count; j++) {
        cover_edge(point_to_cell(nodes[j].lat, nodes[j].lng), point_to_cell(nodes[j + 1].lat, nodes[j + 1].lng),
                   nodes[j].lat, nodes[j].lng, nodes[j + 1].lat, nodes[j + 1].lng, cells);
        for (const S2CellId cell : cells) least = std::min<uint64_t>(least, cell.id());
    }
    return least;
}

bool close_to(double a, double b) { return std::fabs(a - b) < 1e-4; }

// The postcode centroids `dir` of a tree holds, as its two postcode files
// decode (strictly: the files are the encoder's for these rows); none in a
// dir without them, which has neither.
std::vector<postcode_files::LogicalCentroid> centroids_of(const std::map<std::string, std::vector<char>>& files,
                                                          const std::string& dir) {
    const auto centroids = files.find(dir + postcode_files::CENTROIDS_FILE);
    const auto cells = files.find(dir + postcode_files::CELLS_FILE);
    REQUIRE((centroids == files.end()) == (cells == files.end()));
    std::vector<postcode_files::LogicalCentroid> out;
    if (centroids == files.end()) return out;
    postcode_files::decode(reinterpret_cast<const uint8_t*>(centroids->second.data()), centroids->second.size(),
                           reinterpret_cast<const uint8_t*>(cells->second.data()), cells->second.size(),
                           [&](const postcode_files::LogicalCentroid& c) { out.push_back(c); });
    return out;
}

constexpr uint16_t CH = pack_country_code('C', 'H'), DE = pack_country_code('D', 'E');

// A postcode's address: a node of a house number with the postcode tag.
struct Postcoded {
    int64_t id;
    int32_t lat, lon;
    const char* postcode;
};

// Addresses of postcodes: in Testland CH 8001 twice (its centroid halfway
// between them), 8004 beside that, 8002 in the far corner and 80A1, which no
// Swiss postcode is like (no centroid); in Testreich DE 78462 and 78464 side
// by side.
std::vector<Postcoded> postcoded_addresses() {
    const int32_t lat0 = 470000000, lng0 = 80000000;
    return {{7001, lat0 + 210000, lng0 + 230000, "8001"}, {7002, lat0 + 410000, lng0 + 430000, "8001"},
            {7003, lat0 + 313000, lng0 + 333000, "8004"}, {7004, lat0 + 1700000, lng0 + 1700000, "8002"},
            {7005, lat0 + 1700000, lng0 + 300000, "80A1"}, {7006, 468500000, 80500000, "78462"},
            {7007, 468500000, 80520000, "78464"}};
}

// The fixture with Testland's country code, CH, a second country, Testreich
// (DE), a square of 0.1 degrees 0.1 degrees south of it, and the addresses.
std::vector<TestObject> with_postcodes(const std::vector<Postcoded>& addresses) {
    std::vector<TestObject> objects = fixture();
    const int32_t s = 468000000, n = 469000000, w = 80000000, e = 81000000;
    objects.push_back(node(6001, s, w));
    objects.push_back(node(6002, s, e));
    objects.push_back(node(6003, n, e));
    objects.push_back(node(6004, n, w));
    objects.push_back(way(6000, {6001, 6002, 6003, 6004, 6001}));
    objects.push_back(boundary(50, 2, "Testreich", {6000}));
    // The countries' codes, which the builder reads from ISO3166-1:alpha2.
    for (TestObject& o : objects)
        if (o.type == OsmType::Relation && (o.id == 40 || o.id == 50)) o.tags.push_back({"ISO3166-1:alpha2", o.id == 40 ? "CH" : "DE"});
    for (const Postcoded& a : addresses)
        objects.push_back(node(a.id, a.lat, a.lon,
                               {{"addr:housenumber", std::to_string(a.id)}, {"addr:postcode", a.postcode},
                                {"addr:street", "Teststrasse"}}));
    std::stable_sort(objects.begin(), objects.end(), [](const TestObject& a, const TestObject& b) {
        return a.type != b.type ? a.type < b.type : a.id < b.id;
    });
    return objects;
}

// The dir a client of an admin preset holds: the mode dir's files and the
// exact tier's admin geometry beside them.
std::string admin_preset_dir(const std::string& out, const std::string& region, const std::string& into) {
    fs::create_directories(into);
    for (const auto& e : fs::directory_iterator(out + "/" + region + "/admin"))
        if (e.is_regular_file()) fs::copy_file(e.path(), into + "/" + e.path().filename().string());
    for (const char* f : {"admin_polygons.bin", "admin_rings.bin", "arc_index.bin", "arc_vertices.bin"})
        fs::copy_file(out + "/" + region + "/quality/exact/" + f, into + "/" + f, fs::copy_options::overwrite_existing);
    return into;
}

// The fixture with five more addresses on Teststrasse, a building and a
// POI relation with a house number, all but one named a town by addr:city
// (none with `cities` false): Testville, the place node's name; Gemeinde 3
// and Kanton, boundaries'; Teststrasse, only the street's; and Nirgendwo,
// nothing's. Each address is at a point of its own, which `cities_at`
// gives with the town ("" for none).
struct CityAt {
    double lat, lng;
    std::string city;
};

std::vector<CityAt> cities_at() {
    return {{47.141, 8.15, "Testville"},     {47.1412, 8.1502, "Gemeinde 3"}, {47.1414, 8.1504, ""},
            {47.1416, 8.1506, ""},           {47.1418, 8.1508, ""},           {47.1425, 8.1525, "Testville"},
            {47.1435, 8.1545, "Kanton"}};
}

std::vector<TestObject> with_cities(bool cities) {
    std::vector<TestObject> objects = fixture();
    const int32_t lat0 = 470000000, lng0 = 80000000;
    auto tags = [&](const std::string& number, const char* city) {
        std::vector<std::pair<std::string, std::string>> t = {{"addr:housenumber", number}, {"addr:street", "Teststrasse"}};
        if (cities && city) t.push_back({"addr:city", city});
        return t;
    };
    for (TestObject& o : objects)
        if (o.type == OsmType::Node && o.id == 952) o.tags = tags("1", "Testville");
    const char* towns[] = {"Gemeinde 3", "Teststrasse", "Nirgendwo", nullptr};
    for (int k = 0; k < 4; k++)
        objects.push_back(node(953 + k, lat0 + 1412000 + 2000 * k, lng0 + 1502000 + 2000 * k, tags(std::to_string(3 + 2 * k), towns[k])));
    // A building round (47.1425, 8.1525) and a castle round (47.1435, 8.1545),
    // squares of 0.0002 degrees.
    auto square = [&](int64_t first, int32_t lat, int32_t lng) {
        const int32_t corners[4][2] = {{lat - 1000, lng - 1000}, {lat - 1000, lng + 1000}, {lat + 1000, lng + 1000}, {lat + 1000, lng - 1000}};
        for (int k = 0; k < 4; k++) objects.push_back(node(first + k, corners[k][0], corners[k][1]));
        return std::vector<int64_t>{first, first + 1, first + 2, first + 3, first};
    };
    std::vector<std::pair<std::string, std::string>> building = tags("11", "Testville");
    building.push_back({"building", "yes"});
    objects.push_back(way(4020, square(980, lat0 + 1425000, lng0 + 1525000), building));
    objects.push_back(way(4021, square(990, lat0 + 1435000, lng0 + 1545000)));
    TestObject castle;
    castle.type = OsmType::Relation;
    castle.id = 60;
    castle.tags = tags("13", "Kanton");
    for (const auto& tag : std::vector<std::pair<std::string, std::string>>{{"name", "Schloss"}, {"tourism", "attraction"}, {"type", "multipolygon"}})
        castle.tags.push_back(tag);
    castle.members.emplace_back(OsmType::Way, 4021, "outer");
    objects.push_back(castle);
    std::stable_sort(objects.begin(), objects.end(), [](const TestObject& a, const TestObject& b) {
        return a.type != b.type ? a.type < b.type : a.id < b.id;
    });
    return objects;
}

// The core strings of a tree's dir, as they sit back to back.
std::string core_strings(const std::map<std::string, std::vector<char>>& files, const std::string& dir) {
    const std::vector<char>& file = files.at(dir + "strings_core.bin");
    std::string strings;
    string_files::decode(reinterpret_cast<const uint8_t*>(file.data()), file.size(),
                         [&](const char* p, size_t n) { strings.append(p, n); });
    return strings;
}

}  // namespace

TEST(build_index_writes_every_regions_admin_dirs_as_arcs_and_a_chained_build_writes_them_again) {
    const ScratchDir root("admin-dirs-build");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log";
    write_test_pbf(pbf, fixture(), 1000);
    auto build = [&](const std::string& out, std::vector<std::string> extra) {
        std::vector<std::string> words = {GEOCODER_BUILD_INDEX_BIN, out, pbf, "--continents", "--multi-output",
                                          "--multi-quality"};
        words.insert(words.end(), extra.begin(), extra.end());
        return gctest::run(words, log);
    };
    const std::string first = root.path() + "/first";
    REQUIRE(build(first, {}));
    const auto files = tree(first);

    // Admin geometry in the four admin dirs of every region and nowhere else:
    // no quality ladder, no keyhole vertices, no postal files.
    std::set<std::string> geometry;
    for (const std::string& name : names_of(files)) {
        if (admin_geometry(name)) geometry.insert(name);
        const std::string file = fs::path(name).filename().string();
        CHECK(file != "admin_vertices.bin" && file.rfind("postal_", 0) != 0);
        CHECK(name.find("/quality/q") == std::string::npos && name.find("/quality/uncapped") == std::string::npos);
    }
    CHECK(geometry == admin_dir_layout());
    // The full set's slot sidecars beside the exact tier, the minimal set's
    // in admin-minimal/.
    for (const std::string& region : kRegions) {
        const bool held = kRegionsHeld.count(region) > 0;
        CHECK_EQ(files.count(region + "/quality/exact/admin_polygons.osm_ids") > 0, held);
        CHECK(files.count(region + "/quality/exact/arc_index.osm_ids"));
        CHECK_EQ(files.count(region + "/admin-minimal/arc_index.osm_ids") > 0, held);
        CHECK(!files.count(region + "/quality/simplified/admin_polygons.osm_ids"));
        CHECK(!files.count(region + "/admin-minimal-poi/arc_index.osm_ids"));
    }
    auto file = [&](const std::string& region, const std::string& name) { return files.at(region + "/" + name); };
    // The tiers' records and rings are one; the minimal dirs' arcs are one.
    CHECK(file("planet", "quality/simplified/admin_polygons.bin") == file("planet", "quality/exact/admin_polygons.bin"));
    CHECK(file("planet", "quality/simplified/admin_rings.bin") == file("planet", "quality/exact/admin_rings.bin"));
    for (const char* f : {"arc_index.bin", "arc_vertices.bin"})
        CHECK(file("planet", std::string("admin-minimal/") + f) == file("planet", std::string("admin-minimal-poi/") + f));
    // In the shared slots, one 16 B record each: the seven boundaries.
    CHECK_EQ(file("planet", "quality/exact/admin_polygons.bin").size(), 7 * sizeof(AdminPolygonRecord));
    CHECK(file("planet", "quality/exact/arc_vertices.bin").size() > file("planet", "quality/simplified/arc_vertices.bin").size());
    CHECK(!file("planet", "admin-minimal/admin_rings.bin").empty());
    // Every polygon is in Europe, whose arcs are then the planet's in the
    // same slots; Asia has none.
    for (const char* dir : {"quality/simplified", "quality/exact", "admin-minimal", "admin-minimal-poi"})
        for (const char* f : {"admin_rings.bin", "arc_index.bin", "arc_vertices.bin"})
            CHECK(file("europe", std::string(dir) + "/" + f) == file("planet", std::string(dir) + "/" + f));
    CHECK(file("asia", "quality/exact/arc_vertices.bin").empty());
    CHECK(file("asia", "quality/exact/admin_polygons.bin").empty());

    // The same data again on top of that build: the same files.
    const std::string again = root.path() + "/again";
    REQUIRE(build(again, {"--prev-output", first}));
    CHECK(tree(again) == files);
}

TEST(multi_quality_takes_no_scale_list_and_multi_output_needs_it) {
    const ScratchDir root("admin-dirs-flags");
    const std::string pbf = root.path() + "/fixture.osm.pbf";
    write_test_pbf(pbf, fixture(), 1000);
    // Each refused before the build starts, saying why: no output dir.
    auto refusal = [&](const std::string& name, std::vector<std::string> flags) {
        const std::string out = root.path() + "/" + name, log = root.path() + "/" + name + ".log";
        std::vector<std::string> words = {GEOCODER_BUILD_INDEX_BIN, out, pbf};
        words.insert(words.end(), flags.begin(), flags.end());
        CHECK(!gctest::run(words, log));
        CHECK(!fs::exists(out));
        const std::vector<char> said = bytes_of(log);
        return std::string(said.begin(), said.end());
    };
    CHECK(refusal("scales", {"--multi-output", "--multi-quality", "0,0.5,2.5"}).find("--multi-quality takes no scale list") !=
          std::string::npos);
    // Its mode dirs hold no admin geometry, which a client reads from a
    // boundary tier.
    for (const std::vector<std::string>& flags :
         {std::vector<std::string>{"--multi-output"}, std::vector<std::string>{"--multi-output", "--continents"}})
        CHECK(refusal("no-tiers-" + std::to_string(flags.size()), flags)
                  .find("--multi-output needs --multi-quality") != std::string::npos);
}

TEST(a_build_without_the_multi_flags_writes_whole_dirs_the_server_answers_from) {
    const ScratchDir root("admin-dirs-single-output");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log";
    write_test_pbf(pbf, fixture(), 1000);
    const std::vector<std::string> geometry = {"admin_polygons.bin", "admin_rings.bin", "arc_index.bin",
                                               "arc_vertices.bin", kAdminOwnNamesFile};
    // The default invocation: the planet's dir, its mode's files and the
    // exact tier's admin geometry beside them; no boundary tier, no
    // admin-minimal.
    const std::string out = root.path() + "/single";
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, out, pbf}, log));
    const auto files = tree(out);
    for (const std::string& name : names_of(files))
        CHECK(name.find("quality/") == std::string::npos && name.find("admin-minimal") == std::string::npos);
    // The exact tier's bytes: what --multi-quality writes in quality/exact/,
    // and beside the mode's files then too.
    const std::string tiers = root.path() + "/tiers";
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, tiers, pbf, "--multi-quality"}, log));
    const auto with_tiers = tree(tiers);
    for (const std::string& f : geometry) {
        REQUIRE(files.count("planet/" + f) && with_tiers.count("planet/" + f) &&
                with_tiers.count("planet/quality/exact/" + f));
        CHECK(files.at("planet/" + f) == with_tiers.at("planet/quality/exact/" + f));
        CHECK(files.at("planet/" + f) == with_tiers.at("planet/" + f));
    }
    CHECK_EQ(files.at("planet/admin_polygons.bin").size(), 7 * sizeof(AdminPolygonRecord));
    // Its side files hold a value for each street and each address: the
    // street's parent, one of the seven polygons, and no postcode or city,
    // of which the fixture has none; no interpolations, so no file of their
    // postcodes.
    const auto side = side_files(files, "planet/");
    CHECK(names_of(side) ==
          std::set<std::string>({"addr_cities.bin", "addr_postcodes.bin", "way_parents.bin", "way_postcodes.bin"}));
    for (const auto& [name, values] : side) {
        REQUIRE(values.size() == 1);
        CHECK(name == "way_parents.bin" ? values[0] < 7 : values[0] == NO_DATA);
    }
    // Its street files hold the fixture's street, and its interpolation
    // files, of which the fixture has none, are the two footers.
    const auto streets = ways_of<WayHeader>(way_files::STREETS, files, "planet/");
    REQUIRE(streets.ways.size() == 1);
    CHECK(streets.ways[0].node_count == 2 && streets.nodes.size() == 2);
    REQUIRE(files.count("planet/interp_ways.bin") && files.count("planet/interp_nodes.bin"));
    CHECK(files.at("planet/interp_ways.bin") == std::vector<char>({0, 0, 0, 0, 'I', 'W', 'Y', '1'}));
    CHECK(files.at("planet/interp_nodes.bin") == std::vector<char>({0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'I', 'W', 'N', '1'}));
    CHECK(ways_of<InterpWay>(way_files::INTERPS, files, "planet/").ways.empty());
    // Its address files hold the fixture's address, a node.
    const Addresses addresses = addresses_of(files, "planet/");
    REQUIRE(addresses.points.size() == 1);
    CHECK(addresses.points[0].vertex_count == 0 && addresses.points[0].vertex_offset == NO_DATA && addresses.vertices.empty());

    // The server loads the dir and puts each point on its side of the border
    // of the first two municipalities, 0.0005 degrees off it (its ways stray
    // 0.0002 degrees): by their rings, as the cells there hold both.
    const std::vector<std::pair<double, double>> points = {{47.05, 8.0995}, {47.05, 8.1005}};
    auto check_answers = [&](const std::string& dir, const std::string& name) {
        const std::vector<std::string> answers = server_answers(dir, points, root.path() + "/" + name, log);
        REQUIRE(answers.size() == points.size());
        const int failures_before = gctest::failures();
        CHECK(says(answers[0], "Gemeinde 0") && !says(answers[0], "Gemeinde 1"));
        CHECK(says(answers[1], "Gemeinde 1") && !says(answers[1], "Gemeinde 0"));
        for (const std::string& answer : answers) CHECK(says(answer, "Kanton"));
        if (gctest::failures() != failures_before)
            for (const std::string& answer : answers) std::printf("    %s: %s\n", name.c_str(), answer.c_str());
    };
    check_answers(out + "/planet", "single");

    // With --continents, every region's dir alike (Europe's arcs the
    // planet's), which the server loads continent by continent.
    const std::string continents = root.path() + "/continents";
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, continents, pbf, "--continents"}, log));
    const auto regions = tree(continents);
    for (const std::string& region : kRegions)
        for (const std::string& f : geometry) CHECK(regions.count(region + "/" + f));
    for (const char* f : {"admin_rings.bin", "arc_index.bin", "arc_vertices.bin"})
        CHECK(regions.at(std::string("europe/") + f) == regions.at(std::string("planet/") + f));
    // Europe's street and address have their side values; a region without
    // streets has no side file, and its way files and address files are the
    // footers.
    for (const std::string& region : kRegions) {
        CHECK_EQ(addresses_of(regions, region + "/").points.size(), kRegionsHeld.count(region) ? size_t(1) : size_t(0));
        for (const char* f : {addr_files::POINTS_FILE, addr_files::VERTICES_FILE, addr_files::INDEX_FILE})
            CHECK(regions.count(region + "/" + f));
        if (!kRegionsHeld.count(region)) {
            CHECK(regions.at(region + "/addr_points.bin") == std::vector<char>({0, 0, 0, 0, 'A', 'D', 'B', '1'}));
            CHECK(regions.at(region + "/addr_vertices.bin") ==
                  std::vector<char>({0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'A', 'D', 'F', '1'}));
            CHECK(regions.at(region + "/addr_index.bin") == std::vector<char>({0, 0, 0, 0, 'A', 'D', 'X', '1'}));
        }
        CHECK_EQ(names_of(side_files(regions, region + "/")).size(), kRegionsHeld.count(region) ? size_t(4) : size_t(0));
        CHECK_EQ(ways_of<WayHeader>(way_files::STREETS, regions, region + "/").ways.size(),
                 kRegionsHeld.count(region) ? size_t(1) : size_t(0));
        CHECK(ways_of<InterpWay>(way_files::INTERPS, regions, region + "/").ways.empty());
        CHECK_EQ(regions.count(region + "/interp_ways.bin") + regions.count(region + "/interp_nodes.bin"), size_t(2));
    }
    check_answers(continents, "continents");
}

// Each address keeps its addr:city in addr_cities.bin beside the full dir's
// address points, where a place node or a boundary has that name: a core
// string's offset, else NO_DATA (only the street's, or nothing's), as for a
// node, a building and a relation, in the planet's dir and in Europe's of its
// own strings. The values write no string, so a build of the same data
// without them writes every other file as it does; a chained build of the
// same data writes the same files.
TEST(a_build_keeps_an_addresss_addr_city_where_a_place_has_its_name) {
    const ScratchDir root("addr-cities-build");
    const std::string with_pbf = root.path() + "/with.osm.pbf", without_pbf = root.path() + "/without.osm.pbf",
                      log = root.path() + "/build.log";
    write_test_pbf(with_pbf, with_cities(true), 1000);
    write_test_pbf(without_pbf, with_cities(false), 1000);
    auto build = [&](const std::string& pbf, const std::string& out, std::vector<std::string> extra) {
        std::vector<std::string> words = {GEOCODER_BUILD_INDEX_BIN, out, pbf, "--continents", "--multi-output",
                                          "--multi-quality"};
        words.insert(words.end(), extra.begin(), extra.end());
        return gctest::run(words, log);
    };
    const std::string with = root.path() + "/with", without = root.path() + "/without", again = root.path() + "/again";
    REQUIRE(build(with_pbf, with, {}));
    REQUIRE(build(without_pbf, without, {}));
    const auto files = tree(with), plain = tree(without);

    for (const std::string& name : names_of(files))
        if (fs::path(name).filename() == "addr_cities.bin") CHECK(fs::path(name).parent_path().filename() == "full");
    for (const char* region : {"planet", "europe"}) {
        const std::string dir = std::string(region) + "/full/";
        REQUIRE(files.count(dir + "addr_cities.bin"));
        const Addresses addresses = addresses_of(files, dir);
        const std::vector<uint32_t> cities = side_files(files, dir).at("addr_cities.bin");
        const std::string core = core_strings(files, dir);
        const std::vector<CityAt> want = cities_at();
        REQUIRE(addresses.points.size() == want.size());
        size_t found = 0;
        for (size_t i = 0; i < addresses.points.size(); i++) {
            const AddrPoint& a = addresses.points[i];
            for (const CityAt& w : want) {
                if (std::fabs(a.lat - w.lat) > 5e-5 || std::fabs(a.lng - w.lng) > 5e-5) continue;
                found++;
                const uint32_t v = cities[i];
                const int failures_before = gctest::failures();
                if (w.city.empty()) {
                    CHECK_EQ(v, NO_DATA);
                } else {
                    REQUIRE(v < core.size());
                    CHECK(std::string(core.c_str() + v) == w.city);
                }
                if (gctest::failures() > failures_before) std::printf("    %s at %.4f %.4f\n", dir.c_str(), w.lat, w.lng);
            }
        }
        CHECK_EQ(found, want.size());
        CHECK(core.find("Nirgendwo") == std::string::npos);
    }

    // Without the tags: the same files but for addr_cities.bin, which holds
    // only NO_DATA.
    CHECK(names_of(plain) == names_of(files));
    for (const auto& [name, bytes] : plain) {
        if (fs::path(name).filename() != "addr_cities.bin") {
            if (files.at(name) != bytes) std::printf("    %s differs\n", name.c_str());
            CHECK(files.at(name) == bytes);
            continue;
        }
        const std::string dir = name.substr(0, name.size() - std::string("addr_cities.bin").size());
        const std::vector<uint32_t> cities = side_files(plain, dir).at("addr_cities.bin");
        for (uint32_t v : cities) CHECK_EQ(v, NO_DATA);
    }

    // The same data again on top of the first build.
    REQUIRE(build(with_pbf, again, {"--prev-output", with}));
    CHECK(tree(again) == files);

    // Changed data on top of the first build: the house of Gemeinde 3 gone,
    // a new one of Testville further along. The houses kept keep their slots,
    // so the slots are not a fresh build's order, and each value follows its
    // house; a slot no house holds is NO_DATA.
    std::vector<TestObject> changed = with_cities(true);
    changed.erase(std::remove_if(changed.begin(), changed.end(),
                                 [](const TestObject& o) { return o.type == OsmType::Node && o.id == 953; }),
                  changed.end());
    changed.push_back(node(961, 471420000, 81510000,
                           {{"addr:housenumber", "15"}, {"addr:street", "Teststrasse"}, {"addr:city", "Testville"}}));
    std::stable_sort(changed.begin(), changed.end(), [](const TestObject& a, const TestObject& b) {
        return a.type != b.type ? a.type < b.type : a.id < b.id;
    });
    const std::string changed_pbf = root.path() + "/changed.osm.pbf", chained = root.path() + "/chained",
                      fresh = root.path() + "/fresh";
    write_test_pbf(changed_pbf, changed, 1000);
    REQUIRE(build(changed_pbf, chained, {"--prev-output", with}));
    REQUIRE(build(changed_pbf, fresh, {}));
    std::vector<CityAt> moved = cities_at();
    moved.erase(moved.begin() + 1);
    moved.push_back({47.142, 8.151, "Testville"});
    const auto chained_files = tree(chained), fresh_files = tree(fresh);
    auto points_of = [](const Addresses& addresses) {
        std::vector<std::pair<float, float>> out;
        for (const AddrPoint& a : addresses.points) out.push_back({a.lat, a.lng});
        return out;
    };
    for (const char* region : {"planet", "europe"}) {
        const std::string dir = std::string(region) + "/full/";
        const Addresses addresses = addresses_of(chained_files, dir);
        const std::vector<uint32_t> cities = side_files(chained_files, dir).at("addr_cities.bin");
        const std::string core = core_strings(chained_files, dir);
        REQUIRE(cities.size() == addresses.points.size());
        CHECK(points_of(addresses) != points_of(addresses_of(fresh_files, dir)));
        size_t found = 0;
        for (size_t i = 0; i < addresses.points.size(); i++) {
            const AddrPoint& a = addresses.points[i];
            if (a.lat == 0 && a.lng == 0) {
                CHECK_EQ(cities[i], NO_DATA);
                continue;
            }
            for (const CityAt& w : moved) {
                if (std::fabs(a.lat - w.lat) > 5e-5 || std::fabs(a.lng - w.lng) > 5e-5) continue;
                found++;
                const int failures_before = gctest::failures();
                if (w.city.empty()) {
                    CHECK_EQ(cities[i], NO_DATA);
                } else {
                    REQUIRE(cities[i] < core.size());
                    CHECK(std::string(core.c_str() + cities[i]) == w.city);
                }
                if (gctest::failures() > failures_before) std::printf("    chained %s at %.4f %.4f\n", dir.c_str(), w.lat, w.lng);
            }
        }
        CHECK_EQ(found, moved.size());
    }
}

TEST(an_admin_polygon_is_built_whatever_simplifying_would_leave_of_its_ring) {
    // A closed-way village whose six nodes sit at one point: Douglas-Peucker
    // keeps two of its seven vertices at any tolerance, which once dropped
    // the polygon. The exact tier keeps every polygon the ways make: the
    // village is a record of its own, and the build's arc checks pass.
    std::vector<TestObject> objects = fixture();
    std::vector<int64_t> refs;
    for (int64_t id = 600; id < 606; id++) {
        objects.push_back(node(id, 470800000, 80800000));
        refs.push_back(id);
    }
    refs.push_back(refs.front());
    objects.push_back(way(5000, refs, {{"admin_level", "9"}, {"boundary", "administrative"}, {"name", "Punkt"}}));
    std::stable_sort(objects.begin(), objects.end(), [](const TestObject& a, const TestObject& b) {
        return a.type != b.type ? a.type < b.type : a.id < b.id;
    });
    const ScratchDir root("admin-dirs-one-point");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log", out = root.path() + "/out";
    write_test_pbf(pbf, objects, 1000);
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, out, pbf, "--multi-output", "--multi-quality"}, log));
    const auto files = tree(out);
    for (const char* dir : {"quality/simplified", "quality/exact"})
        CHECK_EQ(files.at(std::string("planet/") + dir + "/admin_polygons.bin").size(), 8 * sizeof(AdminPolygonRecord));
    const std::vector<char>& file = files.at("planet/full/strings_core.bin");
    std::string strings;
    string_files::decode(reinterpret_cast<const uint8_t*>(file.data()), file.size(),
                         [&](const char* p, size_t n) { strings.append(p, n); });
    CHECK(strings.find(std::string("Punkt", 6)) != std::string::npos);
}

TEST(a_chained_build_of_edited_data_keeps_each_retained_arcs_slot_where_a_fresh_build_moves_some) {
    const ScratchDir root("admin-dirs-chained-edit");
    const std::string day_one = root.path() + "/day1.osm.pbf", day_two = root.path() + "/day2.osm.pbf",
                      log = root.path() + "/build.log";
    write_test_pbf(day_one, fixture(), 1000);
    write_test_pbf(day_two, fixture(true), 1000);
    auto build = [&](const std::string& pbf, const std::string& out, std::vector<std::string> extra) {
        std::vector<std::string> words = {GEOCODER_BUILD_INDEX_BIN, out, pbf, "--continents", "--multi-output",
                                          "--multi-quality"};
        words.insert(words.end(), extra.begin(), extra.end());
        return gctest::run(words, log);
    };
    const std::string out1 = root.path() + "/out1", chained = root.path() + "/chained",
                      fresh = root.path() + "/fresh";
    REQUIRE(build(day_one, out1, {}));
    REQUIRE(build(day_two, chained, {"--prev-output", out1}));
    REQUIRE(build(day_two, fresh, {}));
    // The chained arcs aren't the fresh build's: --prev-output's arc
    // sidecars were read.
    CHECK(arc_files(chained) != arc_files(fresh));

    // In each set of the planet and of Europe (which holds every polygon,
    // its own slots against its own previous sidecars).
    for (const char* region : {"planet", "europe"})
        for (const char* sidecar : {"quality/exact/arc_index.osm_ids", "admin-minimal/arc_index.osm_ids"}) {
            const std::string name = std::string(region) + "/" + sidecar;
            const int failures_before = gctest::failures();
            const std::vector<uint64_t> before = slot_keys(out1 + "/" + name);
            const std::vector<uint64_t> after = slot_keys(chained + "/" + name);
            const std::vector<uint64_t> anew = slot_keys(fresh + "/" + name);
            REQUIRE(!before.empty() && !after.empty() && !anew.empty());
            const auto after_slot = slot_of_key(after), anew_slot = slot_of_key(anew);
            // Every arc both days have keeps its slot chained; a fresh build
            // presents the moved hole elsewhere and shifts some of them.
            size_t retained = 0, kept = 0, kept_fresh = 0;
            for (size_t s = 0; s < before.size(); s++) {
                REQUIRE(before[s] != UINT64_MAX);  // a fresh build's sidecar has no tombstone
                const auto it = after_slot.find(before[s]);
                if (it == after_slot.end()) continue;
                retained++;
                kept += it->second == s;
                const auto f = anew_slot.find(before[s]);
                kept_fresh += f != anew_slot.end() && f->second == s;
            }
            CHECK_EQ(retained, before.size() - 1);  // all but the old hole's
            CHECK_EQ(kept, retained);
            CHECK(kept_fresh < retained);
            // The old hole's slot is a tombstone, not reused the day it is
            // freed; the new hole's comes after the first day's slots.
            CHECK_EQ(after.size(), before.size() + 1);
            CHECK_EQ(static_cast<size_t>(std::count(after.begin(), after.end(), UINT64_MAX)), size_t(1));
            CHECK(after.back() != UINT64_MAX && std::find(before.begin(), before.end(), after.back()) == before.end());
            CHECK_EQ(anew.size(), before.size());
            if (gctest::failures() != failures_before) std::printf("    in %s\n", name.c_str());
        }
}

TEST(a_chained_build_on_a_previous_build_without_arc_sidecars_allocates_every_arc_slot_cold) {
    const ScratchDir root("admin-dirs-cold-arcs");
    const std::string day_one = root.path() + "/day1.osm.pbf", day_two = root.path() + "/day2.osm.pbf",
                      log = root.path() + "/build.log";
    write_test_pbf(day_one, fixture(), 1000);
    write_test_pbf(day_two, fixture(true), 1000);
    auto build = [&](const std::string& pbf, const std::string& out, std::vector<std::string> extra) {
        std::vector<std::string> words = {GEOCODER_BUILD_INDEX_BIN, out, pbf, "--continents", "--multi-output",
                                          "--multi-quality"};
        words.insert(words.end(), extra.begin(), extra.end());
        return gctest::run(words, log);
    };
    const std::string out1 = root.path() + "/out1", prev = root.path() + "/prev", cold = root.path() + "/cold",
                      chained = root.path() + "/chained", fresh = root.path() + "/fresh";
    REQUIRE(build(day_one, out1, {}));
    // The previous build as it is but for its arc sidecars, every region's.
    fs::copy(out1, prev, fs::copy_options::recursive);
    size_t removed = 0;
    for (const auto& e : fs::recursive_directory_iterator(prev))
        if (e.path().filename() == "arc_index.osm_ids") removed++;
    for (const std::string& region : kRegions)
        for (const char* sidecar : {"/quality/exact/arc_index.osm_ids", "/admin-minimal/arc_index.osm_ids"})
            fs::remove(prev + "/" + region + sidecar);
    REQUIRE(removed > 0);
    REQUIRE(build(day_two, cold, {"--prev-output", prev}));
    const std::vector<char> text = bytes_of(log);
    const std::string said(text.begin(), text.end());
    for (const char* set : {"full", "minimal"}) {
        CHECK(said.find(std::string("ARC_CHECK slots=arcs set=") + set + " region=planet previous_sidecar_missing=1") !=
              std::string::npos);
        CHECK(said.find(std::string("planet arcs: no previous ") + set + " arc slots at " + prev + "/planet/") !=
              std::string::npos);
    }
    CHECK(said.find("region=europe previous_sidecar_missing=1") != std::string::npos);
    REQUIRE(build(day_two, chained, {"--prev-output", out1}));
    REQUIRE(build(day_two, fresh, {}));
    // Its arcs are a fresh build's, slots and all; its admin slots the chained build's.
    auto arcs_only = [](std::map<std::string, std::vector<char>> files) {
        for (auto it = files.begin(); it != files.end();) {
            const std::string file = fs::path(it->first).filename().string();
            it = file == "arc_index.bin" || file == "arc_vertices.bin" || file == "arc_index.osm_ids" ? std::next(it)
                                                                                                      : files.erase(it);
        }
        return files;
    };
    CHECK(arcs_only(arc_files(cold)) == arcs_only(arc_files(fresh)));
    CHECK(arcs_only(arc_files(cold)) != arcs_only(arc_files(chained)));
    for (const std::string& region : {std::string("planet"), std::string("europe")}) {
        const std::string sidecar = region + "/quality/exact/admin_polygons.osm_ids";
        CHECK(bytes_of(cold + "/" + sidecar) == bytes_of(chained + "/" + sidecar));
    }
}

TEST(a_chained_build_keeps_the_slot_of_an_arc_whose_key_changes_by_its_way_set_or_its_ends) {
    const ScratchDir root("admin-dirs-fallbacks");
    const std::string day_one = root.path() + "/day1.osm.pbf", log = root.path() + "/build.log";
    write_test_pbf(day_one, fixture(), 1000);
    auto build = [&](const std::string& pbf, const std::string& out, std::vector<std::string> extra) {
        std::vector<std::string> words = {GEOCODER_BUILD_INDEX_BIN, out, pbf, "--continents", "--multi-output",
                                          "--multi-quality"};
        words.insert(words.end(), extra.begin(), extra.end());
        return gctest::run(words, log);
    };
    const std::string out1 = root.path() + "/out1";
    REQUIRE(build(day_one, out1, {}));
    // Each set's arcs' fallback keys beside its slots.
    const auto files = tree(out1);
    for (const std::string& region : kRegions) {
        const bool held = kRegionsHeld.count(region) > 0;
        CHECK(files.count(region + "/quality/exact/arc_index.fallback.osm_ids"));
        CHECK_EQ(files.count(region + "/admin-minimal/arc_index.fallback.osm_ids") > 0, held);
        CHECK(!files.count(region + "/quality/simplified/arc_index.fallback.osm_ids"));
        CHECK(!files.count(region + "/admin-minimal-poi/arc_index.fallback.osm_ids"));
    }

    struct Case {
        Edit edit;
        size_t changed, by_way_set, by_ends;
        const char* name;
    };
    for (const Case& c : {Case{Edit::InnerNodesMoved, 1, 1, 0, "inner nodes moved"},
                          Case{Edit::MiddleMoved, 4, 4, 0, "middle moved"},
                          Case{Edit::WayRemapped, 1, 0, 1, "way remapped"}}) {
        const std::string pbf = root.path() + "/day2.osm.pbf", out = root.path() + "/out2";
        fs::remove_all(out);
        write_test_pbf(pbf, edited(c.edit), 1000);
        REQUIRE(build(pbf, out, {"--prev-output", out1}));
        const std::vector<char> text = bytes_of(log);
        const std::string said(text.begin(), text.end());
        const int failures_before = gctest::failures();
        // In each set of the planet and of Europe (every polygon, its own
        // slots against its own previous sidecars): every slot held both
        // days, the changed arcs' by their new keys, none appended.
        for (const char* region : {"planet", "europe"})
            for (const std::string set : {"full", "minimal"}) {
                const std::string sidecar =
                    std::string("/") + region + (set == "full" ? "/quality/exact" : "/admin-minimal") + "/arc_index.osm_ids";
                const std::vector<uint64_t> before = slot_keys(out1 + sidecar), after = slot_keys(out + sidecar);
                REQUIRE(!before.empty());
                CHECK_EQ(after.size(), before.size());
                CHECK_EQ(static_cast<size_t>(std::count(after.begin(), after.end(), UINT64_MAX)), size_t(0));
                size_t changed = 0;
                for (size_t s = 0; s < std::min(before.size(), after.size()); s++) changed += after[s] != before[s];
                CHECK_EQ(changed, c.changed);
                CHECK(said.find("ARC_CHECK fallbacks=arcs set=" + set + " region=" + region +
                                " previous_fallback_sidecar_missing=0 by_way_set=" + std::to_string(c.by_way_set) +
                                " by_ends=" + std::to_string(c.by_ends) + "\n") != std::string::npos);
                if (gctest::failures() != failures_before) std::printf("    in %s %s\n", region, sidecar.c_str());
            }
        if (gctest::failures() != failures_before) std::printf("    with the %s\n", c.name);
    }
}

TEST(a_chained_build_on_a_previous_build_without_arc_fallback_sidecars_takes_slots_by_arc_keys_alone) {
    const ScratchDir root("admin-dirs-first-fallback-night");
    const std::string day_one = root.path() + "/day1.osm.pbf", day_two = root.path() + "/day2.osm.pbf",
                      log = root.path() + "/build.log";
    write_test_pbf(day_one, fixture(), 1000);
    write_test_pbf(day_two, edited(Edit::InnerNodesMoved), 1000);
    auto build = [&](const std::string& pbf, const std::string& out, std::vector<std::string> extra) {
        std::vector<std::string> words = {GEOCODER_BUILD_INDEX_BIN, out, pbf, "--continents", "--multi-output",
                                          "--multi-quality"};
        words.insert(words.end(), extra.begin(), extra.end());
        return gctest::run(words, log);
    };
    const std::string out1 = root.path() + "/out1", prev = root.path() + "/prev", out2 = root.path() + "/out2";
    REQUIRE(build(day_one, out1, {}));
    // The previous build as it is but for its arcs' fallback sidecars, every region's.
    fs::copy(out1, prev, fs::copy_options::recursive);
    std::vector<fs::path> fallbacks;
    for (const auto& e : fs::recursive_directory_iterator(prev))
        if (e.path().filename() == "arc_index.fallback.osm_ids" &&
            fs::relative(e.path(), prev).string().find("/water/") == std::string::npos)
            fallbacks.push_back(e.path());
    REQUIRE(fallbacks.size() == kRegions.size() + kRegionsHeld.size());
    for (const fs::path& p : fallbacks) fs::remove(p);
    REQUIRE(build(day_two, out2, {"--prev-output", prev}));
    const std::vector<char> text = bytes_of(log);
    const std::string said(text.begin(), text.end());
    for (const char* region : {"planet", "europe"})
        for (const std::string set : {"full", "minimal"}) {
            const std::string dir = std::string("/") + region + (set == "full" ? "/quality/exact" : "/admin-minimal");
            CHECK(said.find("ARC_CHECK fallbacks=arcs set=" + set + " region=" + region +
                            " previous_fallback_sidecar_missing=1 by_way_set=0 by_ends=0\n") != std::string::npos);
            CHECK(said.find(std::string(region) + " arcs: no previous " + set + " arc fallback keys beside " + prev + dir +
                            "/arc_index.osm_ids, slots by arc keys alone") != std::string::npos);
            // The moved arc is a new one: its old slot a tombstone, its new one appended.
            const std::vector<uint64_t> before = slot_keys(out1 + dir + "/arc_index.osm_ids"),
                                        after = slot_keys(out2 + dir + "/arc_index.osm_ids");
            REQUIRE(!before.empty());
            CHECK_EQ(after.size(), before.size() + 1);
            CHECK_EQ(static_cast<size_t>(std::count(after.begin(), after.end(), UINT64_MAX)), size_t(1));
            // And it writes its own fallback keys for the next night.
            CHECK(fs::exists(out2 + dir + "/arc_index.fallback.osm_ids"));
        }
}

TEST(a_building_is_written_with_its_footprint_which_the_server_takes_its_distance_from) {
    const ScratchDir root("building-footprint-build");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log";
    // A building of house number 7, 22 m by 60 m, its west wall 8 m east of
    // the node of the fixture's house number 1.
    const int32_t lat0 = 470000000, lng0 = 80000000;
    const int32_t south = lat0 + 1409000, north = lat0 + 1411000, west = lng0 + 1501000, east = lng0 + 1509000;
    const int32_t corners[5][2] = {{south, west}, {south, east}, {north, east}, {north, west}, {south, west}};
    std::vector<TestObject> objects = fixture();
    for (int k = 0; k < 4; k++) objects.push_back(node(970 + k, corners[k][0], corners[k][1]));
    objects.push_back(way(4010, {970, 971, 972, 973, 970},
                          {{"addr:housenumber", "7"}, {"addr:street", "Teststrasse"}, {"building", "yes"}}));
    std::stable_sort(objects.begin(), objects.end(), [](const TestObject& a, const TestObject& b) {
        return a.type != b.type ? a.type < b.type : a.id < b.id;
    });
    write_test_pbf(pbf, objects, 1000);
    const std::string out = root.path() + "/single";
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, out, pbf}, log));

    // The node's record is a point; the building's has the way's five
    // vertices, the last the first again, on the grid a building's span
    // takes.
    const Addresses addresses = addresses_of(tree(out), "planet/");
    REQUIRE(addresses.points.size() == 2);
    const bool building_first = addresses.points[0].vertex_count != 0;
    const AddrPoint& building = addresses.points[building_first ? 0 : 1];
    const AddrPoint& house = addresses.points[building_first ? 1 : 0];
    CHECK(house.vertex_count == 0 && house.vertex_offset == NO_DATA);
    REQUIRE(building.vertex_count == 5 && building.vertex_offset == 0 && addresses.vertices.size() == POLY_HEADER_BYTES + 5 * 4);
    CHECK_EQ(addresses.vertices[0], static_cast<uint8_t>(VertexEncoding::U16_011M));
    const std::vector<NodeCoord> ring = unpack_polygon(addresses.vertices.data(), 5);
    for (int k = 0; k < 5; k++)
        CHECK(std::fabs(ring[k].lat - corners[k][0] / 1e7) < 1e-5 && std::fabs(ring[k].lng - corners[k][1] / 1e7) < 1e-5);
    CHECK(building.lat > south / 1e7 && building.lat < north / 1e7 && building.lng > west / 1e7 && building.lng < east / 1e7);

    // A point inside the building's west end is 11 m from the node and 20 m
    // from the building's own point: the server answers the building, by its
    // walls.
    const std::vector<std::string> answers = server_answers(out + "/planet", {{47.1410, 8.15015}}, root.path() + "/q", log);
    REQUIRE(answers.size() == 1);
    CHECK(says(answers[0], "7") && !says(answers[0], "1"));
    if (!says(answers[0], "7")) std::printf("    %s\n", answers[0].c_str());
}

namespace {

// A closed way `id` of four new nodes from `first_node` on, around the
// rectangle (south, west, north, east) in 1e-7 degrees.
void add_rectangle(std::vector<TestObject>& objects, int64_t id, int64_t first_node, int32_t south, int32_t west,
                   int32_t north, int32_t east, std::vector<std::pair<std::string, std::string>> tags = {}) {
    const int32_t corners[4][2] = {{south, west}, {south, east}, {north, east}, {north, west}};
    for (int k = 0; k < 4; k++) objects.push_back(node(first_node + k, corners[k][0], corners[k][1]));
    objects.push_back(way(id, {first_node, first_node + 1, first_node + 2, first_node + 3, first_node}, std::move(tags)));
}

// A multipolygon relation `id` of one outer way.
TestObject multipolygon(int64_t id, int64_t outer, std::vector<std::pair<std::string, std::string>> tags) {
    TestObject o;
    o.type = OsmType::Relation;
    o.id = id;
    o.tags = std::move(tags);
    o.tags.push_back({"type", "multipolygon"});
    o.members.emplace_back(OsmType::Way, outer, "outer");
    return o;
}

// The fixture with `extra` objects, written to `pbf` in PBF order.
void write_fixture_with(const std::string& pbf, std::vector<TestObject> extra) {
    std::vector<TestObject> objects = fixture();
    objects.insert(objects.end(), extra.begin(), extra.end());
    std::stable_sort(objects.begin(), objects.end(), [](const TestObject& a, const TestObject& b) {
        return a.type != b.type ? a.type < b.type : a.id < b.id;
    });
    write_test_pbf(pbf, objects, 1000);
}

// The POI records a POI tier dir of a tree holds, as its files decode.
std::vector<PoiRecord> poi_records_in(const std::map<std::string, std::vector<char>>& files, const std::string& dir) {
    const std::string records = dir + "/poi_records.bin", vertices = dir + "/poi_vertices.bin";
    REQUIRE(files.count(records) && files.count(vertices));
    const auto& r2 = files.at(records);
    const auto& v2 = files.at(vertices);
    const poi_files::Logical pois = poi_files::decode(reinterpret_cast<const uint8_t*>(r2.data()), r2.size(),
                                                      reinterpret_cast<const uint8_t*>(v2.data()), v2.size());
    static_assert(sizeof(PoiRecord) == poi_files::LOGICAL_STRIDE, "a logical POI record is a PoiRecord");
    REQUIRE(pois.records.size() % sizeof(PoiRecord) == 0);
    std::vector<PoiRecord> out(pois.records.size() / sizeof(PoiRecord));
    if (!out.empty()) std::memcpy(out.data(), pois.records.data(), pois.records.size());
    return out;
}

// The categories of `records`, in order.
std::vector<int> categories_of(const std::vector<PoiRecord>& records) {
    std::vector<int> out;
    for (const PoiRecord& pr : records) out.push_back(pr.category);
    std::sort(out.begin(), out.end());
    return out;
}

// The categories a poi_meta.json lists, its top-level keys, in order.
std::vector<int> meta_categories(const std::vector<char>& meta) {
    const std::string text(meta.begin(), meta.end());
    std::vector<int> out;
    // Each top-level member is a line `  "<id>": {...}`.
    for (size_t at = text.find("\n  \""); at != std::string::npos; at = text.find("\n  \"", at + 1)) {
        const size_t from = at + 4, to = text.find('"', from);
        REQUIRE(to != std::string::npos && text.compare(to, 4, "\": {") == 0);
        out.push_back(std::stoi(text.substr(from, to - from)));
    }
    std::sort(out.begin(), out.end());
    return out;
}

// The dataset of a client of POI tier `tier` of a build's whole dir `out`:
// the planet dir's files and poi/<tier>'s beside them, at `at`.
std::string poi_dataset(const std::string& out, const std::string& tier, const std::string& at) {
    fs::create_directories(at);
    for (const std::string dir : {std::string("planet"), "planet/poi/" + tier})
        for (const auto& e : fs::directory_iterator(out + "/" + dir))
            if (e.is_regular_file())
                fs::copy_file(e.path(), at + "/" + e.path().filename().string(), fs::copy_options::overwrite_existing);
    return at;
}

}  // namespace

// A named fire pond, emergency=fire_water_pond, alone or on a basin
// (landuse=basin), is a rank-30 POI of its emergency tag in Nominatim
// (poi_classify.h): the way pass resolves the nodes of both, though the
// basin's keys make no POI way alone, the build writes each polygon as
// FIRE_WATER_POND, and the server answers each at a point inside, on its
// parent street.
TEST(a_fire_pond_alone_or_on_a_basin_is_written_as_one_and_answered) {
    const ScratchDir root("fire-pond-build");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log";
    // 22 m by 60 m each, 33 m north of Teststrasse and 600 m and 450 m west
    // of its address.
    const int32_t lat0 = 470000000, lng0 = 80000000;
    std::vector<TestObject> ponds;
    add_rectangle(ponds, 4020, 990, lat0 + 1403000, lng0 + 1420000, lat0 + 1405000, lng0 + 1428000,
                  {{"emergency", "fire_water_pond"}, {"landuse", "basin"}, {"name", "Feuerweiher Testdorf"}});
    add_rectangle(ponds, 4021, 994, lat0 + 1403000, lng0 + 1435000, lat0 + 1405000, lng0 + 1443000,
                  {{"emergency", "fire_water_pond"}, {"name", "Loeschteich Testdorf"}});
    write_fixture_with(pbf, ponds);
    const std::string out = root.path() + "/single";
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, out, pbf}, log));

    const std::vector<PoiRecord> pois = poi_records_in(tree(out), "planet/poi/all");
    REQUIRE(pois.size() == 2);
    for (const PoiRecord& pond : pois) {
        CHECK_EQ(int(pond.category), int(PoiCategory::FIRE_WATER_POND));
        CHECK_EQ(pond.vertex_count, 5u);
        CHECK(pond.parent_street_id != NO_DATA);
    }

    const std::string dataset = poi_dataset(out, "all", root.path() + "/dataset");
    const std::vector<std::string> answers =
        server_answers(dataset, {{47.1404, 8.1424}, {47.1404, 8.1439}}, root.path() + "/q", log);
    REQUIRE(answers.size() == 2);
    const char* names[] = {"Feuerweiher Testdorf", "Loeschteich Testdorf"};
    for (size_t k = 0; k < 2; k++) {
        CHECK(answer_string(answers[k], {"display_name"}).value_or("").rfind(std::string(names[k]) + ", ", 0) == 0);
        CHECK(answer_string(answers[k], {"address", "road"}) == std::optional<std::string>("Teststrasse"));
        if (!says(answers[k], names[k])) std::printf("    %s\n", answers[k].c_str());
    }
}

// A named bay that is also a pier, natural=bay with man_made=pier, is a
// rank-30 POI of its man_made tag in Nominatim, a PIER here (tier 3), which
// the notable tier doesn't ship, while it ships bays (tier 2): poi/all holds
// the pier, poi/notable the bay, poi/major neither, each of its tier and of
// no flag but its wiki tag's, and the slot of the way remembers tier 2, the
// lowest that holds it, so a tombstone of it reaches poi/notable too. With a
// wiki tag the bay is of tier 1 and the pier of tier 2: poi/major holds the
// bay.
TEST(a_bay_that_is_a_pier_is_a_pier_where_piers_ship_and_a_bay_where_only_bays_do) {
    const ScratchDir root("bay-pier-build");
    const std::string log = root.path() + "/build.log";
    const int32_t lat0 = 470000000, lng0 = 80000000;
    for (const bool wiki : {false, true}) {
        const std::string pbf = root.path() + (wiki ? "/wiki.osm.pbf" : "/plain.osm.pbf");
        std::vector<std::pair<std::string, std::string>> tags = {
            {"man_made", "pier"}, {"name", "Hafenbucht Testdorf"}, {"natural", "bay"}};
        if (wiki) tags.push_back({"wikidata", "Q4242"});
        std::vector<TestObject> bay;
        add_rectangle(bay, 4030, 990, lat0 + 1403000, lng0 + 1420000, lat0 + 1405000, lng0 + 1428000, tags);
        write_fixture_with(pbf, bay);
        const std::string out = root.path() + (wiki ? "/wiki" : "/plain");
        REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, out, pbf}, log));
        const auto files = tree(out);

        const int pier = int(PoiCategory::PIER), bay_kind = int(PoiCategory::BAY);
        const std::map<std::string, std::vector<int>> want = {
            {"major", wiki ? std::vector<int>{bay_kind} : std::vector<int>{}},
            {"notable", {wiki ? pier : bay_kind}},
            {"all", {pier}}};
        for (const auto& [tier, categories] : want) {
            const std::vector<PoiRecord> pois = poi_records_in(files, "planet/poi/" + tier);
            if (categories_of(pois) != categories) std::printf("    %s, wiki %d\n", tier.c_str(), int(wiki));
            CHECK(categories_of(pois) == categories);
            for (const PoiRecord& pr : pois) {
                const int expected_tier = pr.category == pier ? (wiki ? 2 : 3) : (wiki ? 1 : 2);
                CHECK_EQ(int(pr.tier), expected_tier);
                CHECK_EQ(int(pr.flags), wiki ? int(POI_FLAG_WIKIDATA) : 0);
                CHECK_EQ(pr.vertex_count, 5u);
            }
            // poi_meta.json lists the category the tier holds the way as,
            // and no other; none for the tier that holds nothing.
            CHECK(meta_categories(files.at("planet/poi/" + tier + "/poi_meta.json")) == categories);
        }

        gc::id_alloc::IdAllocator alloc;
        REQUIRE(alloc.load_previous(out + "/planet/poi/all/poi_records.osm_ids"));
        REQUIRE(alloc.slots().size() == 1);
        CHECK_EQ(int(alloc.slots()[0].tier), wiki ? 1 : 2);
    }
}

// A glacier that is also an attraction, natural=glacier with
// tourism=attraction, is an ATTRACTION (tier 2) where attractions ship, of an
// attraction's importance (20), and a GLACIER (tier 1) in poi/major, of a
// glacier's (40), as the plain glacier of its size beside it is: in the
// planet's dir and Europe's, and in a chained build whose new museum, of a
// category sorting first, moves the slots of a fresh build's order.
TEST(a_water_object_written_as_its_kind_takes_the_importance_of_that_kind) {
    const ScratchDir root("water-importance-build");
    const std::string log = root.path() + "/build.log";
    const int32_t lat0 = 470000000, lng0 = 80000000, south = lat0 + 1403000, north = lat0 + 1405000;
    std::vector<TestObject> ice;
    add_rectangle(ice, 4040, 990, south, lng0 + 1420000, north, lng0 + 1428000,
                  {{"name", "Gletscher Testdorf"}, {"natural", "glacier"}});
    add_rectangle(ice, 4041, 994, south, lng0 + 1435000, north, lng0 + 1443000,
                  {{"name", "Eisgrotte Testdorf"}, {"natural", "glacier"}, {"tourism", "attraction"}});
    const std::string day_one = root.path() + "/one.osm.pbf", day_two = root.path() + "/two.osm.pbf";
    write_fixture_with(day_one, ice);
    ice.push_back(node(998, lat0 + 1404000, lng0 + 1450000, {{"name", "Museum Testdorf"}, {"tourism", "museum"}}));
    write_fixture_with(day_two, ice);
    const std::string out1 = root.path() + "/one", out2 = root.path() + "/two";
    const std::vector<std::string> multi = {"--continents", "--multi-output", "--multi-quality"};
    std::vector<std::string> words = {GEOCODER_BUILD_INDEX_BIN, out1, day_one};
    words.insert(words.end(), multi.begin(), multi.end());
    REQUIRE(gctest::run(words, log));
    words = {GEOCODER_BUILD_INDEX_BIN, out2, day_two, "--prev-output", out1};
    words.insert(words.end(), multi.begin(), multi.end());
    REQUIRE(gctest::run(words, log));

    const int glacier = int(PoiCategory::GLACIER), attraction = int(PoiCategory::ATTRACTION);
    for (const std::string& out : {out1, out2}) {
        const auto files = tree(out);
        for (const char* region : {"planet", "europe"}) {
            // The glaciers in poi/major, the attraction and the plain one in
            // poi/notable.
            const std::string major = std::string(region) + "/poi/major", notable = std::string(region) + "/poi/notable";
            const std::vector<PoiRecord> glaciers = poi_records_in(files, major);
            REQUIRE(categories_of(glaciers) == std::vector<int>({glacier, glacier}));
            for (const PoiRecord& pr : glaciers) {
                if (pr.importance != 40) std::printf("    %s %s: importance %d\n", out.c_str(), major.c_str(), int(pr.importance));
                CHECK_EQ(int(pr.importance), 40);
            }
            for (const PoiRecord& pr : poi_records_in(files, notable)) {
                if (pr.category == glacier) CHECK_EQ(int(pr.importance), 40);
                if (pr.category == attraction) CHECK_EQ(int(pr.importance), 20);
            }
        }
    }
}

// A named multipolygon of water that is also a rank-30 POI, natural=bay or
// natural=water with leisure=swimming_area, is that POI in Nominatim, which
// imports a multipolygon as a closed way: the build writes both as
// SWIMMING_AREA, and the server answers the bay at a point inside, on its
// parent street. A plain multipolygon bay stays a BAY, which never takes the
// primary slot nor gives the road: Seeweg, its parent street, runs nearer its
// middle than Teststrasse, and the answer at a point inside nearer
// Teststrasse takes Teststrasse. A main tag of a lower rank ranks the object
// whatever its others: a bay of place=locality (25) that is an attraction
// stays a BAY, out of the primary slot, and one of highway=footway (27), a
// street in Nominatim (the street lane takes address ranks 26 to 29), a BAY of
// the highway flag, whose own name the server answers inside it, as Nominatim
// answers a street. A multipolygon the scan takes by its own categories
// where the classifier makes it water of a kind the scan takes no POI of
// keeps its name in `places` but never the primary slot, of no parent street:
// a wetland nature reserve whose rank-30 tag (emergency=suction_point) no
// tier ships, and a reservoir that is a building and an attraction, whose
// class (building, the first of its fallback tags) Nominatim's POI lane
// takes none of. A lake that is a swimming area with an island, a hole of
// it, which a POI's polygon of its outer ring would fill, is no POI: a point
// on the island finds neither its name nor its road, nor the address it
// carries. A bay of a hole never takes the slot anyway, and keeps its parent
// street. A lake of highway=pedestrian (26) and no hole is a street, as the
// closed way is: a NATURAL_WATER of the highway flag, whose own name the
// server answers inside it, though Teststrasse is nearer than its outline.
TEST(a_water_multipolygon_that_is_a_rank_30_poi_is_written_as_that_poi_and_answered) {
    const ScratchDir root("water-relation-build");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log";
    // 22 m by 60 m each, 33 m north of Teststrasse; Seeweg 22 m north of the
    // last six.
    const int32_t lat0 = 470000000, lng0 = 80000000, south = lat0 + 1403000, north = lat0 + 1405000;
    std::vector<TestObject> water;
    add_rectangle(water, 4040, 990, south, lng0 + 1420000, north, lng0 + 1428000);
    add_rectangle(water, 4041, 994, south, lng0 + 1435000, north, lng0 + 1443000);
    add_rectangle(water, 4042, 998, south, lng0 + 1450000, north, lng0 + 1458000);
    add_rectangle(water, 4043, 1002, south, lng0 + 1465000, north, lng0 + 1473000);
    add_rectangle(water, 4044, 1006, south, lng0 + 1480000, north, lng0 + 1488000);
    add_rectangle(water, 4045, 1010, south, lng0 + 1520000, north, lng0 + 1528000);
    add_rectangle(water, 4046, 1014, south, lng0 + 1535000, north, lng0 + 1543000);
    add_rectangle(water, 4047, 1018, lat0 + 1403200, lng0 + 1537000, lat0 + 1403800, lng0 + 1541000);
    add_rectangle(water, 4048, 1022, south, lng0 + 1550000, north, lng0 + 1558000);
    add_rectangle(water, 4049, 1026, south, lng0 + 1565000, north, lng0 + 1573000);
    add_rectangle(water, 4050, 1030, lat0 + 1403200, lng0 + 1567000, lat0 + 1403800, lng0 + 1571000);
    add_rectangle(water, 4051, 1034, south, lng0 + 1580000, north, lng0 + 1588000);
    water.push_back(node(1100, lat0 + 1407000, lng0 + 1449000));
    water.push_back(node(1101, lat0 + 1407000, lng0 + 1560000));
    water.push_back(way(4060, {1100, 1101}, {{"highway", "residential"}, {"name", "Seeweg"}}));
    water.push_back(multipolygon(5040, 4040, {{"leisure", "swimming_area"}, {"name", "Badebucht Testdorf"}, {"natural", "bay"}}));
    water.push_back(multipolygon(5041, 4041, {{"leisure", "swimming_area"}, {"name", "Badesee Testdorf"}, {"natural", "water"}}));
    water.push_back(multipolygon(5042, 4042, {{"name", "Bucht Testdorf"}, {"natural", "bay"}}));
    water.push_back(multipolygon(5043, 4043, {{"name", "Ortsbucht Testdorf"}, {"natural", "bay"}, {"place", "locality"},
                                              {"tourism", "attraction"}}));
    water.push_back(multipolygon(5044, 4044, {{"highway", "footway"}, {"name", "Uferweg Testdorf"}, {"natural", "bay"},
                                              {"tourism", "attraction"}}));
    water.push_back(multipolygon(5045, 4045, {{"emergency", "suction_point"}, {"leisure", "nature_reserve"},
                                              {"name", "Ried Testdorf"}, {"natural", "wetland"}}));
    TestObject island_lake = multipolygon(5046, 4046, {{"addr:housenumber", "5"}, {"addr:postcode", "8899"},
                                                        {"addr:street", "Inselweg"}, {"leisure", "swimming_area"},
                                                        {"name", "Inselsee Testdorf"}, {"natural", "water"}});
    island_lake.members.emplace_back(OsmType::Way, 4047, "inner");
    water.push_back(island_lake);
    water.push_back(multipolygon(5048, 4048, {{"building", "yes"}, {"landuse", "reservoir"}, {"name", "Stausee Testdorf"},
                                              {"tourism", "attraction"}}));
    TestObject island_bay = multipolygon(5049, 4049, {{"name", "Inselbucht Testdorf"}, {"natural", "bay"}});
    island_bay.members.emplace_back(OsmType::Way, 4050, "inner");
    water.push_back(island_bay);
    water.push_back(multipolygon(5051, 4051, {{"highway", "pedestrian"}, {"name", "Seeplatz Testdorf"}, {"natural", "water"}}));
    write_fixture_with(pbf, water);
    const std::string out = root.path() + "/single";
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, out, pbf}, log));

    const std::vector<PoiRecord> pois = poi_records_in(tree(out), "planet/poi/all");
    const int bay = int(PoiCategory::BAY), swimming = int(PoiCategory::SWIMMING_AREA);
    const int lake = int(PoiCategory::NATURAL_WATER);
    const std::vector<int> want = {int(PoiCategory::ATTRACTION), int(PoiCategory::NATURE_RESERVE), bay, bay, bay, bay,
                                   lake, swimming, swimming};
    if (categories_of(pois) != want)
        for (const PoiRecord& pr : pois) std::printf("    category %d at %.4f\n", int(pr.category), pr.lng);
    CHECK(categories_of(pois) == want);
    // The record whose polygon holds longitude `lng`.
    auto at = [&](double lng) -> const PoiRecord* {
        for (const PoiRecord& pr : pois)
            if (std::abs(pr.lng - lng) < 0.0004) return &pr;
        return nullptr;
    };
    struct Want {
        double lng;
        int category;
        bool parent;
        uint8_t flags;
    };
    for (const Want& w : {Want{8.1424, swimming, true, 0}, Want{8.1439, swimming, true, 0}, Want{8.1454, bay, true, 0},
                          Want{8.1469, bay, true, 0}, Want{8.1484, bay, true, POI_FLAG_HIGHWAY},
                          Want{8.1524, int(PoiCategory::NATURE_RESERVE), false, 0},
                          Want{8.1554, int(PoiCategory::ATTRACTION), false, 0}, Want{8.1569, bay, true, 0},
                          Want{8.1584, lake, true, POI_FLAG_HIGHWAY}}) {
        const PoiRecord* pr = at(w.lng);
        REQUIRE(pr != nullptr);
        if (pr->category != w.category || (pr->parent_street_id != NO_DATA) != w.parent || pr->flags != w.flags)
            std::printf("    at %.4f: category %d, parent %u, flags %d\n", w.lng, int(pr->category), pr->parent_street_id,
                        int(pr->flags));
        CHECK_EQ(int(pr->category), w.category);
        CHECK_EQ(pr->parent_street_id != NO_DATA, w.parent);
        CHECK_EQ(int(pr->flags), int(w.flags));
    }

    const std::string dataset = poi_dataset(out, "all", root.path() + "/dataset");
    const std::vector<std::pair<double, double>> points = {{47.1404, 8.1424},  {47.14031, 8.1454}, {47.14031, 8.1469},
                                                           {47.14031, 8.1484}, {47.14031, 8.1524}, {47.14033, 8.1539},
                                                           {47.14031, 8.1554}, {47.14031, 8.1584}};
    const std::vector<std::string> answers = server_answers(dataset, points, root.path() + "/q", log);
    REQUIRE(answers.size() == points.size());
    CHECK(answer_string(answers[0], {"display_name"}).value_or("").rfind("Badebucht Testdorf, ", 0) == 0);
    CHECK(answer_string(answers[1], {"display_name"}).value_or("").rfind("Bucht Testdorf", 0) == std::string::npos);
    const char* roads[] = {"Teststrasse", "Teststrasse", "Teststrasse", "Uferweg Testdorf", "Teststrasse",
                           "Teststrasse", "Teststrasse", "Seeplatz Testdorf"};
    for (size_t k = 0; k < answers.size(); k++) {
        if (answer_string(answers[k], {"address", "road"}) != std::optional<std::string>(roads[k]))
            std::printf("    %s\n", answers[k].c_str());
        CHECK(answer_string(answers[k], {"address", "road"}) == std::optional<std::string>(roads[k]));
    }
    // The nature reserve is in `places`, and the island lake nowhere.
    CHECK(says(answers[4], "Ried Testdorf"));
    CHECK(at(8.1539) == nullptr);
    const bool island = says(answers[5], "Inselsee Testdorf") || says(answers[5], "Inselweg") ||
                        says(answers[5], "8899") || answer_string(answers[5], {"address", "house_number"});
    if (island) std::printf("    %s\n", answers[5].c_str());
    CHECK(!island);
    if (!says(answers[0], "Badebucht Testdorf")) std::printf("    %s\n", answers[0].c_str());
}

// A named water area of highway=footway is a street in Nominatim, and the
// server answers its own name inside it, but not one of footway=sidewalk,
// crossing, link or traffic_aisle, which Nominatim takes as no main tag
// (presets.lua filter_footways): that one is the water it is, out of the
// primary slot, though it links Teststrasse as its parent street. Each area
// is 60 m by 89 m across Teststrasse, which runs 6 m south of the point
// asked inside it, nearer than its outline. Nor is its way a street of the
// street index: at a point 1 m inside its outline, 33 m north of
// Teststrasse, the road is Teststrasse, where the plain footway's is its own
// name.
TEST(a_water_area_of_a_sidewalk_crossing_link_or_traffic_aisle_footway_answers_no_road_of_its_own) {
    const ScratchDir root("water-footway-build");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log";
    const int32_t lat0 = 470000000, lng0 = 80000000, south = lat0 + 1396000, north = lat0 + 1404000;
    const char* names[] = {"Uferweg Testdorf", "Seesteig Testdorf", "Seefurt Testdorf", "Seegang Testdorf",
                           "Seegasse Testdorf"};
    const char* footways[] = {nullptr, "sidewalk", "crossing", "link", "traffic_aisle"};
    std::vector<TestObject> water;
    std::vector<std::pair<double, double>> points;
    for (int k = 0; k < 5; k++) {
        std::vector<std::pair<std::string, std::string>> tags = {{"area", "yes"}};
        if (footways[k]) tags.push_back({"footway", footways[k]});
        tags.insert(tags.end(), {{"highway", "footway"}, {"name", names[k]}, {"natural", "water"}});
        const int32_t west = lng0 + 1420000 + 15000 * k;
        add_rectangle(water, 4040 + k, 990 + 4 * k, south, west, north, west + 8000, tags);
        points.push_back({47.14005, (west + 4000) / 1e7});
    }
    for (int k = 0; k < 5; k++) points.push_back({47.1403, (lng0 + 1420000 + 15000 * k + 130) / 1e7});
    write_fixture_with(pbf, water);
    const std::string out = root.path() + "/single";
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, out, pbf}, log));

    const std::vector<PoiRecord> pois = poi_records_in(tree(out), "planet/poi/all");
    REQUIRE(pois.size() == 5);
    for (const PoiRecord& pr : pois) {
        const bool street = std::abs(pr.lng - 8.1424) < 0.0004;
        CHECK_EQ(int(pr.category), int(PoiCategory::NATURAL_WATER));
        CHECK_EQ(int(pr.flags), street ? int(POI_FLAG_HIGHWAY) : 0);
        CHECK(pr.parent_street_id != NO_DATA);
    }

    const std::string dataset = poi_dataset(out, "all", root.path() + "/dataset");
    const std::vector<std::string> answers = server_answers(dataset, points, root.path() + "/q", log);
    REQUIRE(answers.size() == points.size());
    for (size_t k = 0; k < answers.size(); k++) {
        const std::string road = k % 5 == 0 ? names[0] : "Teststrasse";
        if (answer_string(answers[k], {"address", "road"}) != std::optional<std::string>(road))
            std::printf("    %s\n", answers[k].c_str());
        CHECK(answer_string(answers[k], {"address", "road"}) == std::optional<std::string>(road));
    }
}

// A named water area of highway=pedestrian (26) or highway=steps (27) that is
// an attraction, of no wiki tag, is a street in Nominatim, which the build
// writes as a lake of the highway flag in the tier an attraction ships in, 2:
// poi/notable holds each as poi/all does, and from either the server answers
// its own name inside it, though Teststrasse, 6 m south of the point, is
// nearer than its outline. poi/major holds none.
TEST(a_water_street_that_is_an_attraction_answers_its_own_name_from_poi_notable_too) {
    const ScratchDir root("water-street-tier-build");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log";
    const int32_t lat0 = 470000000, lng0 = 80000000;
    const char* names[] = {"Seeplatz Testdorf", "Seetreppe Testdorf"};
    std::vector<TestObject> water;
    add_rectangle(water, 4040, 990, lat0 + 1396000, lng0 + 1420000, lat0 + 1404000, lng0 + 1428000,
                  {{"area", "yes"}, {"highway", "pedestrian"}, {"name", names[0]}, {"natural", "water"},
                   {"tourism", "attraction"}});
    add_rectangle(water, 4041, 994, lat0 + 1396000, lng0 + 1435000, lat0 + 1404000, lng0 + 1443000,
                  {{"area", "yes"}, {"highway", "steps"}, {"name", names[1]}, {"natural", "water"},
                   {"tourism", "attraction"}});
    write_fixture_with(pbf, water);
    const std::string out = root.path() + "/single";
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, out, pbf}, log));
    const auto files = tree(out);

    CHECK(poi_records_in(files, "planet/poi/major").empty());
    for (const std::string tier : {"notable", "all"}) {
        const std::vector<PoiRecord> pois = poi_records_in(files, "planet/poi/" + tier);
        if (pois.size() != 2) std::printf("    poi/%s holds %zu\n", tier.c_str(), pois.size());
        CHECK_EQ(pois.size(), size_t(2));
        for (const PoiRecord& pr : pois) {
            if (pr.flags != POI_FLAG_HIGHWAY) std::printf("    poi/%s at %.4f: flags %d\n", tier.c_str(), pr.lng, int(pr.flags));
            CHECK_EQ(int(pr.category), int(PoiCategory::NATURAL_WATER));
            CHECK_EQ(int(pr.tier), 2);
            CHECK_EQ(int(pr.flags), int(POI_FLAG_HIGHWAY));
        }
        const std::string dataset = poi_dataset(out, tier, root.path() + "/" + tier);
        const std::vector<std::string> answers =
            server_answers(dataset, {{47.14005, 8.1424}, {47.14005, 8.1439}}, root.path() + "/q-" + tier, log);
        REQUIRE(answers.size() == 2);
        for (size_t k = 0; k < answers.size(); k++) {
            if (answer_string(answers[k], {"address", "road"}) != std::optional<std::string>(names[k]))
                std::printf("    poi/%s: %s\n", tier.c_str(), answers[k].c_str());
            CHECK(answer_string(answers[k], {"address", "road"}) == std::optional<std::string>(names[k]));
        }
    }
}

// A named water area of a street's highway kind is a street in Nominatim:
// one placex row of class highway and rank_address 26 or 27, which its
// street lane finds by the distance to its geometry and answers by its own
// name, of no parent street, and which is no address point, whatever addr:*
// tags it carries (IsAddressPoint takes rank_address 30 alone). The build
// writes no address of a multipolygon lake of highway=pedestrian, nor of a
// closed way of area=yes, each 60 m by 89 m across Teststrasse, which runs
// 6 m south of the point asked inside each: there the server answers each
// one's own name, not its addr:street, Inselweg, nor its house number. A
// closed way of a highway tag is a polygon of area=yes alone (init.lua
// process_way), a line else: inside a lake of highway=pedestrian of none,
// Teststrasse is nearer than its outline, and answers. A lake of
// highway=pedestrian 2 km north of every street links no parent street, and
// its own name answers inside it, as just outside it, 3 m west, where its
// outline is the nearest feature. A basin of area=yes and highway=pedestrian
// alone, of no POI main tag, is such a street too: the build writes its
// polygon, which answers inside it though Teststrasse is nearer than its
// outline. Nor is any water multipolygon of a hole an
// address point, whose polygon of the outer ring would fill it: a point on
// the island of a wetland nature reserve that is a rank-30 POI of no
// category of its own finds no house number, though the reserve of no
// island keeps its address. A bay of highway=pedestrian of a hole never
// takes the slot, and is no street either: at a point on its island its own
// name doesn't answer.
TEST(a_water_street_is_no_address_point_and_answers_its_own_name_as_a_street_does) {
    const ScratchDir root("water-street-address-build");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log";
    const int32_t lat0 = 470000000, lng0 = 80000000, south = lat0 + 1396000, north = lat0 + 1404000;
    const std::vector<std::pair<std::string, std::string>> address = {
        {"addr:housenumber", "5"}, {"addr:postcode", "8899"}, {"addr:street", "Inselweg"}};
    auto with_address = [&](std::vector<std::pair<std::string, std::string>> tags) {
        tags.insert(tags.begin(), address.begin(), address.end());
        return tags;
    };
    std::vector<TestObject> water;
    add_rectangle(water, 4040, 990, south, lng0 + 1420000, north, lng0 + 1428000);
    water.push_back(multipolygon(5040, 4040, with_address({{"highway", "pedestrian"}, {"name", "Seeplatz Testdorf"},
                                                           {"natural", "water"}})));
    add_rectangle(water, 4041, 994, south, lng0 + 1435000, north, lng0 + 1443000,
                  with_address({{"area", "yes"}, {"highway", "pedestrian"}, {"name", "Seesteg Testdorf"},
                                {"natural", "water"}}));
    add_rectangle(water, 4042, 998, south, lng0 + 1450000, north, lng0 + 1458000,
                  {{"highway", "pedestrian"}, {"name", "Seeufer Testdorf"}, {"natural", "water"}});
    add_rectangle(water, 4049, 1026, south, lng0 + 1535000, north, lng0 + 1543000,
                  with_address({{"area", "yes"}, {"highway", "pedestrian"}, {"landuse", "basin"},
                                {"name", "Beckenplatz Testdorf"}}));
    add_rectangle(water, 4043, 1002, lat0 + 1600000, lng0 + 1420000, lat0 + 1608000, lng0 + 1428000);
    water.push_back(multipolygon(5043, 4043, {{"highway", "pedestrian"}, {"name", "Bergsee Testdorf"}, {"natural", "water"}}));
    // The reserves and the bay 22 m by 60 m, 33 m north of Teststrasse, each
    // island 7 m by 30 m in its middle.
    const int32_t r_south = lat0 + 1403000, r_north = lat0 + 1405000;
    add_rectangle(water, 4044, 1006, r_south, lng0 + 1465000, r_north, lng0 + 1473000);
    add_rectangle(water, 4045, 1010, lat0 + 1403700, lng0 + 1467000, lat0 + 1404300, lng0 + 1471000);
    add_rectangle(water, 4046, 1014, r_south, lng0 + 1520000, r_north, lng0 + 1528000);
    add_rectangle(water, 4047, 1018, r_south, lng0 + 1395000, r_north, lng0 + 1403000);
    add_rectangle(water, 4048, 1022, lat0 + 1403700, lng0 + 1397000, lat0 + 1404300, lng0 + 1401000);
    TestObject island_reserve = multipolygon(5044, 4044, {{"addr:housenumber", "7"}, {"addr:street", "Riedweg"},
                                                           {"emergency", "suction_point"}, {"leisure", "nature_reserve"},
                                                           {"name", "Inselried Testdorf"}, {"natural", "wetland"}});
    island_reserve.members.emplace_back(OsmType::Way, 4045, "inner");
    water.push_back(island_reserve);
    water.push_back(multipolygon(5046, 4046, {{"addr:housenumber", "8"}, {"addr:street", "Riedweg"},
                                              {"emergency", "suction_point"}, {"leisure", "nature_reserve"},
                                              {"name", "Ried Testdorf"}, {"natural", "wetland"}}));
    TestObject island_bay = multipolygon(5047, 4047, {{"highway", "pedestrian"}, {"name", "Inselbucht Testdorf"},
                                                       {"natural", "bay"}});
    island_bay.members.emplace_back(OsmType::Way, 4048, "inner");
    water.push_back(island_bay);
    write_fixture_with(pbf, water);
    const std::string out = root.path() + "/single";
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, out, pbf}, log));
    const auto files = tree(out);

    // The addresses: the fixture's house 1, a node, and the reserve's of no
    // island, its polygon.
    const std::vector<AddrPoint> points = addresses_of(files, "planet/").points;
    if (points.size() != 2) std::printf("    %zu addresses\n", points.size());
    CHECK_EQ(points.size(), size_t(2));
    CHECK_EQ(std::count_if(points.begin(), points.end(), [](const AddrPoint& a) { return a.vertex_count != 0; }), 1);

    const std::vector<PoiRecord> pois = poi_records_in(files, "planet/poi/all");
    auto at = [&](double lat, double lng) -> const PoiRecord* {
        for (const PoiRecord& pr : pois)
            if (std::abs(pr.lng - lng) < 0.0004 && std::abs(pr.lat - lat) < 0.002) return &pr;
        return nullptr;
    };
    struct Want {
        double lat, lng;
        int category;
        uint8_t flags;
        bool parent;
    };
    const int lake = int(PoiCategory::NATURAL_WATER);
    for (const Want& w : {Want{47.14, 8.1424, lake, POI_FLAG_HIGHWAY, true}, Want{47.14, 8.1439, lake, POI_FLAG_HIGHWAY, true},
                          Want{47.14, 8.1454, lake, 0, true}, Want{47.1604, 8.1424, lake, POI_FLAG_HIGHWAY, false},
                          Want{47.1404, 8.1469, int(PoiCategory::NATURE_RESERVE), 0, false},
                          Want{47.1404, 8.1524, int(PoiCategory::NATURE_RESERVE), 0, false},
                          Want{47.1404, 8.1399, int(PoiCategory::BAY), 0, false},
                          Want{47.14, 8.1539, int(PoiCategory::RESERVOIR), POI_FLAG_HIGHWAY, true}}) {
        const PoiRecord* pr = at(w.lat, w.lng);
        REQUIRE(pr != nullptr);
        if (pr->category != w.category || pr->flags != w.flags || (pr->parent_street_id != NO_DATA) != w.parent)
            std::printf("    at %.4f, %.4f: category %d, flags %d, parent %u\n", w.lat, w.lng, int(pr->category),
                        int(pr->flags), pr->parent_street_id);
        CHECK_EQ(int(pr->category), w.category);
        CHECK_EQ(int(pr->flags), int(w.flags));
        CHECK_EQ(pr->parent_street_id != NO_DATA, w.parent);
    }

    const std::string dataset = poi_dataset(out, "all", root.path() + "/dataset");
    const std::vector<std::pair<double, double>> asked = {
        {47.14005, 8.1424}, {47.14005, 8.1439}, {47.14005, 8.1454}, {47.1604, 8.1424},
        {47.1604, 8.14196}, {47.1404, 8.1469},  {47.1404, 8.1399}, {47.14005, 8.1539}};
    const std::vector<std::string> answers = server_answers(dataset, asked, root.path() + "/q", log);
    REQUIRE(answers.size() == asked.size());
    const char* roads[] = {"Seeplatz Testdorf", "Seesteg Testdorf", "Teststrasse", "Bergsee Testdorf",
                           "Bergsee Testdorf", "Teststrasse", "Teststrasse", "Beckenplatz Testdorf"};
    for (size_t k = 0; k < answers.size(); k++) {
        const bool numbered = answer_string(answers[k], {"address", "house_number"}).has_value();
        if (answer_string(answers[k], {"address", "road"}) != std::optional<std::string>(roads[k]) || numbered ||
            says(answers[k], "Inselweg") || says(answers[k], "Riedweg"))
            std::printf("    %s\n", answers[k].c_str());
        CHECK(answer_string(answers[k], {"address", "road"}) == std::optional<std::string>(roads[k]));
        CHECK(!numbered);
        CHECK(!says(answers[k], "Inselweg") && !says(answers[k], "Riedweg"));
    }
}

// A named water way of a street's highway kind is a street in Nominatim, of
// its polygon where it is area=yes, of its line else (init.lua process_way),
// and no address point. The build writes no address of one where it holds
// that street: the polygon of its POI, or the line of its street where the
// street index takes its highway kind. It takes no highway=construction
// (is_included_highway), so a lake of that kind and of no area=yes, 60 m by
// 89 m across Teststrasse, which runs 6 m south of the point asked inside
// it, keeps its address, whose house number answers there, on its
// addr:street. One of area=yes is a street polygon, of no address: its own
// name answers inside it.
TEST(a_water_way_of_a_street_the_build_holds_nowhere_keeps_its_address) {
    const ScratchDir root("water-construction-build");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log";
    const int32_t lat0 = 470000000, lng0 = 80000000, south = lat0 + 1396000, north = lat0 + 1404000;
    std::vector<TestObject> water;
    add_rectangle(water, 4040, 990, south, lng0 + 1420000, north, lng0 + 1428000,
                  {{"addr:housenumber", "5"}, {"addr:street", "Inselweg"}, {"highway", "construction"},
                   {"name", "Bausee Testdorf"}, {"natural", "water"}});
    add_rectangle(water, 4041, 994, south, lng0 + 1435000, north, lng0 + 1443000,
                  {{"addr:housenumber", "6"}, {"addr:street", "Inselweg"}, {"area", "yes"},
                   {"highway", "construction"}, {"name", "Baustellensee Testdorf"}, {"natural", "water"}});
    write_fixture_with(pbf, water);
    const std::string out = root.path() + "/single";
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, out, pbf}, log));

    // The addresses: the fixture's house 1, a node, and the lake's of no
    // area=yes, its polygon.
    const std::vector<AddrPoint> points = addresses_of(tree(out), "planet/").points;
    if (points.size() != 2) std::printf("    %zu addresses\n", points.size());
    CHECK_EQ(points.size(), size_t(2));
    CHECK_EQ(std::count_if(points.begin(), points.end(), [](const AddrPoint& a) { return a.vertex_count != 0; }), 1);

    const std::string dataset = poi_dataset(out, "all", root.path() + "/dataset");
    const std::vector<std::string> answers =
        server_answers(dataset, {{47.14005, 8.1424}, {47.14005, 8.1439}}, root.path() + "/q", log);
    REQUIRE(answers.size() == 2);
    const std::optional<std::string> roads[] = {"Inselweg", "Baustellensee Testdorf"};
    const std::optional<std::string> numbers[] = {"5", std::nullopt};
    for (size_t k = 0; k < answers.size(); k++) {
        const auto road = answer_string(answers[k], {"address", "road"});
        const auto number = answer_string(answers[k], {"address", "house_number"});
        if (road != roads[k] || number != numbers[k]) std::printf("    %s\n", answers[k].c_str());
        CHECK(road == roads[k]);
        CHECK(number == numbers[k]);
    }
}

// A landuse node ranks 30 in Nominatim (ranking.sql compute_place_rank), so
// a named basin point is a rank-30 POI, in the POI lane: the build links it
// Beckenweg, 17 m north of it, and at a point 11 m south of it, nearer
// Teststrasse (22 m) than Beckenweg (28 m), it answers, on Beckenweg. One
// that is a locality (25) and a fountain too, or a reservoir that is a
// building and an attraction, or a building alone, whose class (building,
// its first fallback tag) the POI lane takes none of, is in neither lane
// there: the build writes each a RESERVOIR of no parent street, and
// Teststrasse answers by it.
TEST(a_reservoir_point_a_lower_rank_or_its_class_keeps_out_of_the_poi_lane_never_takes_the_primary_slot) {
    const ScratchDir root("reservoir-point-build");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log";
    const int32_t lat0 = 470000000, lng0 = 80000000;
    std::vector<TestObject> objects;
    objects.push_back(node(1200, lat0 + 1404500, lng0 + 1420000));
    objects.push_back(node(1201, lat0 + 1404500, lng0 + 1490000));
    objects.push_back(way(4070, {1200, 1201}, {{"highway", "residential"}, {"name", "Beckenweg"}}));
    objects.push_back(node(1202, lat0 + 1403000, lng0 + 1430000, {{"landuse", "basin"}, {"name", "Becken Testdorf"}}));
    objects.push_back(node(1203, lat0 + 1403000, lng0 + 1445000, {{"amenity", "fountain"}, {"landuse", "basin"},
                                                                  {"name", "Brunnenbecken Testdorf"}, {"place", "locality"}}));
    objects.push_back(node(1204, lat0 + 1403000, lng0 + 1460000, {{"building", "yes"}, {"landuse", "reservoir"},
                                                                  {"name", "Stauweiher Testdorf"}, {"tourism", "attraction"}}));
    objects.push_back(node(1205, lat0 + 1403000, lng0 + 1475000, {{"building", "yes"}, {"landuse", "basin"},
                                                                  {"name", "Beckenhaus Testdorf"}}));
    write_fixture_with(pbf, objects);
    const std::string out = root.path() + "/single";
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, out, pbf}, log));

    const std::vector<PoiRecord> pois = poi_records_in(tree(out), "planet/poi/all");
    REQUIRE(pois.size() == 4);
    for (const PoiRecord& pr : pois) {
        const bool plain = std::abs(pr.lng - 8.1430) < 0.0001;
        if ((pr.parent_street_id != NO_DATA) != plain) std::printf("    at %.4f: parent %u\n", pr.lng, pr.parent_street_id);
        CHECK_EQ(int(pr.category), int(PoiCategory::RESERVOIR));
        CHECK_EQ(int(pr.flags), 0);
        CHECK_EQ(pr.parent_street_id != NO_DATA, plain);
    }

    const std::string dataset = poi_dataset(out, "all", root.path() + "/dataset");
    const std::vector<std::string> answers = server_answers(
        dataset, {{47.1402, 8.1430}, {47.1402, 8.1445}, {47.1402, 8.1460}, {47.1402, 8.1475}}, root.path() + "/q", log);
    REQUIRE(answers.size() == 4);
    const char* roads[] = {"Beckenweg", "Teststrasse", "Teststrasse", "Teststrasse"};
    for (size_t k = 0; k < answers.size(); k++) {
        if (answer_string(answers[k], {"address", "road"}) != std::optional<std::string>(roads[k]))
            std::printf("    %s\n", answers[k].c_str());
        CHECK(answer_string(answers[k], {"address", "road"}) == std::optional<std::string>(roads[k]));
    }
}

// A basin point that is an emergency=suction_point too is a rank-30 POI in
// Nominatim, in the POI lane, a generic rank-30 feature here, which no POI
// dir ships without a wiki tag: the build writes it as the RESERVOIR it is
// in its place, of no parent street, and at a point 11 m south of it,
// nearer Teststrasse (22 m) than Beckenweg (28 m), Teststrasse answers, as
// where no dir ships the POI. Of a wikidata tag poi/all ships the POI,
// linked to Beckenweg, and answers it there, and poi/notable the RESERVOIR,
// of no parent street: Teststrasse answers from it. A plain basin point
// answers, on Beckenweg.
TEST(a_reservoir_point_in_place_of_a_poi_no_dir_ships_never_takes_the_primary_slot) {
    const ScratchDir root("reservoir-stand-in-build");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log";
    const int32_t lat0 = 470000000, lng0 = 80000000;
    std::vector<TestObject> objects;
    objects.push_back(node(1200, lat0 + 1404500, lng0 + 1420000));
    objects.push_back(node(1201, lat0 + 1404500, lng0 + 1490000));
    objects.push_back(way(4070, {1200, 1201}, {{"highway", "residential"}, {"name", "Beckenweg"}}));
    objects.push_back(node(1202, lat0 + 1403000, lng0 + 1430000, {{"landuse", "basin"}, {"name", "Becken Testdorf"}}));
    objects.push_back(node(1203, lat0 + 1403000, lng0 + 1445000, {{"emergency", "suction_point"}, {"landuse", "basin"},
                                                                  {"name", "Saugbecken Testdorf"}}));
    objects.push_back(node(1204, lat0 + 1403000, lng0 + 1460000, {{"emergency", "suction_point"}, {"landuse", "basin"},
                                                                  {"name", "Loeschbecken Testdorf"}, {"wikidata", "Q4242"}}));
    write_fixture_with(pbf, objects);
    const std::string out = root.path() + "/single";
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, out, pbf}, log));
    const auto files = tree(out);

    struct Want {
        double lng;
        PoiCategory category;
        bool parent;
        const char* road;
    };
    struct Tier {
        const char* name;
        std::vector<Want> wants;
    };
    for (const Tier& t : {Tier{"all", {{8.1430, PoiCategory::RESERVOIR, true, "Beckenweg"},
                                       {8.1445, PoiCategory::RESERVOIR, false, "Teststrasse"},
                                       {8.1460, PoiCategory::GENERIC_RANK30, true, "Beckenweg"}}},
                          Tier{"notable", {{8.1460, PoiCategory::RESERVOIR, false, "Teststrasse"}}}}) {
        const std::vector<PoiRecord> pois = poi_records_in(files, std::string("planet/poi/") + t.name);
        REQUIRE(pois.size() == t.wants.size());
        std::vector<std::pair<double, double>> points;
        for (const Want& w : t.wants) {
            const auto pr = std::find_if(pois.begin(), pois.end(), [&](const PoiRecord& p) { return std::abs(p.lng - w.lng) < 0.0001; });
            REQUIRE(pr != pois.end());
            if (pr->category != uint8_t(w.category) || (pr->parent_street_id != NO_DATA) != w.parent)
                std::printf("    poi/%s at %.4f: category %d, parent %u\n", t.name, w.lng, int(pr->category), pr->parent_street_id);
            CHECK_EQ(int(pr->category), int(w.category));
            CHECK_EQ(pr->parent_street_id != NO_DATA, w.parent);
            points.push_back({47.1402, w.lng});
        }
        const std::string dataset = poi_dataset(out, t.name, root.path() + "/dataset-" + t.name);
        const std::vector<std::string> answers = server_answers(dataset, points, root.path() + "/q-" + t.name, log);
        REQUIRE(answers.size() == points.size());
        for (size_t k = 0; k < answers.size(); k++) {
            if (answer_string(answers[k], {"address", "road"}) != std::optional<std::string>(t.wants[k].road))
                std::printf("    poi/%s: %s\n", t.name, answers[k].c_str());
            CHECK(answer_string(answers[k], {"address", "road"}) == std::optional<std::string>(t.wants[k].road));
        }
    }
}

// A build logs, for each dir it writes with addresses and for each POI tier,
// the bytes of the dir's logical vertex stream, the most a patch indexes of
// them and their share of that: a STREAM_BYTES line, which the nightly's
// report reads (tools/o11y_report.py). A building of five vertices and a
// hospital of five: each stream is one block of 30 bytes.
TEST(a_build_logs_each_vertex_streams_bytes_and_their_share_of_what_a_patch_indexes) {
    const ScratchDir root("stream-bytes-build");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log", out = root.path() + "/out";
    const int32_t south = 470000000 + 1409000, north = south + 2000, west = 80000000 + 1501000;
    std::vector<TestObject> objects = fixture();
    // Two rectangles 8 m wide, the hospital 150 m east of the building.
    auto rectangle = [&](int64_t first_node, int32_t w) {
        const int32_t corners[4][2] = {{south, w}, {south, w + 8000}, {north, w + 8000}, {north, w}};
        for (int k = 0; k < 4; k++) objects.push_back(node(first_node + k, corners[k][0], corners[k][1]));
        return std::vector<int64_t>{first_node, first_node + 1, first_node + 2, first_node + 3, first_node};
    };
    objects.push_back(way(4010, rectangle(970, west), {{"addr:housenumber", "7"}, {"addr:street", "Teststrasse"}, {"building", "yes"}}));
    objects.push_back(way(4011, rectangle(980, west + 20000), {{"amenity", "hospital"}, {"name", "Spital"}}));
    std::stable_sort(objects.begin(), objects.end(), [](const TestObject& a, const TestObject& b) {
        return a.type != b.type ? a.type < b.type : a.id < b.id;
    });
    write_test_pbf(pbf, objects, 1000);
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, out, pbf, "--multi-output", "--multi-quality"}, log));
    REQUIRE(addresses_of(tree(out), "planet/full/").vertices.size() == 30);

    const std::vector<char> text = bytes_of(log);
    const std::string said(text.begin(), text.end());
    auto count = [&](const std::string& part) {
        size_t n = 0;
        for (size_t at = said.find(part); at != std::string::npos; at = said.find(part, at + 1)) n++;
        return n;
    };
    const int failures_before = gctest::failures();
    CHECK_EQ(count("\nSTREAM_BYTES dir=planet/full stream=addr_vertices bytes=30 limit=8589934588 share=0.0%\n"), size_t(1));
    CHECK_EQ(count("\nSTREAM_BYTES dir=planet/poi/all stream=poi_vertices bytes=30 limit=4294967294 share=0.0%\n"), size_t(1));
    // The modes without address files have no line.
    CHECK_EQ(count("stream=addr_vertices"), size_t(1));
    if (gctest::failures() > failures_before) std::printf("%s\n", said.c_str());
}

TEST(a_fresh_build_writes_streets_and_addresses_in_cell_order_whatever_their_names) {
    const ScratchDir root("record-order-build");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log";
    // Six streets of 30 m across the fixture's country, kilometres apart,
    // each with an address 11 m north of its middle.
    const int32_t lat0 = 470000000, lng0 = 80000000;
    auto degrees = [](int32_t e7) { return e7 / 1e7; };
    struct Spot {
        int32_t lat, lon;
        uint64_t cell = 0;
        std::string name;
    };
    std::vector<Spot> spots = {{lat0 + 150000, lng0 + 700000},  {lat0 + 1800000, lng0 + 300000},
                               {lat0 + 700000, lng0 + 1700000}, {lat0 + 1900000, lng0 + 1850000},
                               {lat0 + 900000, lng0 + 950000},  {lat0 + 1200000, lng0 + 400000}};
    for (Spot& s : spots) {
        const NodeCoord ends[] = {{static_cast<float>(degrees(s.lat)), static_cast<float>(degrees(s.lon))},
                                  {static_cast<float>(degrees(s.lat)), static_cast<float>(degrees(s.lon + 4000))}};
        s.cell = least_way_cell(ends, 2);
    }
    // Named against their cells: the street of the least cell is "Weg F",
    // the last by name, and the fixture's Teststrasse is ahead of them all.
    std::sort(spots.begin(), spots.end(), [](const Spot& a, const Spot& b) { return a.cell < b.cell; });
    for (size_t i = 0; i < spots.size(); i++) {
        REQUIRE(i == 0 || spots[i - 1].cell < spots[i].cell);
        spots[i].name = std::string("Weg ") + static_cast<char>('F' - i);
    }
    std::vector<TestObject> objects = fixture();
    for (size_t i = 0; i < spots.size(); i++) {
        const Spot& s = spots[i];
        const int64_t id = 960 + 3 * static_cast<int64_t>(i);
        objects.push_back(node(id, s.lat, s.lon));
        objects.push_back(node(id + 1, s.lat, s.lon + 4000));
        objects.push_back(node(id + 2, s.lat + 1000, s.lon + 2000,
                               {{"addr:housenumber", std::to_string(i + 1)}, {"addr:street", s.name}}));
        objects.push_back(way(4001 + static_cast<int64_t>(i), {id, id + 1}, {{"highway", "residential"}, {"name", s.name}}));
    }
    std::stable_sort(objects.begin(), objects.end(), [](const TestObject& a, const TestObject& b) {
        return a.type != b.type ? a.type < b.type : a.id < b.id;
    });
    write_test_pbf(pbf, objects, 1000);

    // The addresses by the cell of each one's point, the fixture's (spot 6)
    // among them. By street name they would go 6, 5, 4, 3, 2, 1, 0.
    struct Address {
        uint64_t cell;
        double lat, lng;
        size_t spot;
    };
    std::vector<Address> addresses;
    for (size_t i = 0; i < spots.size(); i++) {
        const double lat = degrees(spots[i].lat + 1000), lng = degrees(spots[i].lon + 2000);
        addresses.push_back({uint64_t{point_to_cell(lat, lng).id()}, lat, lng, i});
    }
    addresses.push_back({uint64_t{point_to_cell(degrees(lat0 + 1410000), degrees(lng0 + 1500000)).id()},
                         degrees(lat0 + 1410000), degrees(lng0 + 1500000), spots.size()});
    std::sort(addresses.begin(), addresses.end(), [](const Address& a, const Address& b) { return a.cell < b.cell; });
    {
        std::vector<size_t> by_cell;
        for (const Address& a : addresses) by_cell.push_back(a.spot);
        REQUIRE(by_cell != std::vector<size_t>({6, 5, 4, 3, 2, 1, 0}));
    }

    // The dir's street ways ascend by least cell, the six streets among them
    // from "Weg F" to "Weg A", and its addresses by the cell of their point.
    auto check_order = [&](const std::map<std::string, std::vector<char>>& files, const std::string& dir) {
        REQUIRE(files.count(dir + "street_ways.bin") && files.count(dir + "street_nodes.bin") &&
                files.count(dir + "addr_points.bin") && files.count(dir + "addr_vertices.bin") &&
                files.count(dir + "addr_index.bin"));
        const auto [ways, nodes] = ways_of<WayHeader>(way_files::STREETS, files, dir);
        REQUIRE(ways.size() == spots.size() + 1);
        std::vector<size_t> streets;
        uint64_t last = 0;
        for (const WayHeader& w : ways) {
            REQUIRE(w.node_count >= 2 && size_t(w.node_offset) + w.node_count <= nodes.size());
            const NodeCoord* first = &nodes[w.node_offset];
            const uint64_t cell = least_way_cell(first, w.node_count);
            CHECK(cell > last);
            last = cell;
            for (size_t i = 0; i < spots.size(); i++)
                if (close_to(first->lat, degrees(spots[i].lat)) && close_to(first->lng, degrees(spots[i].lon))) streets.push_back(i);
        }
        CHECK(streets == std::vector<size_t>({0, 1, 2, 3, 4, 5}));
        const std::vector<AddrPoint> points = addresses_of(files, dir).points;
        REQUIRE(points.size() == addresses.size());
        for (size_t k = 0; k < points.size(); k++)
            CHECK(close_to(points[k].lat, addresses[k].lat) && close_to(points[k].lng, addresses[k].lng));
    };

    // A whole dir, which the server answers from: each address by its own
    // street.
    const std::string single = root.path() + "/single";
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, single, pbf}, log));
    check_order(tree(single), "planet/");
    std::vector<std::pair<double, double>> at;
    for (const Spot& s : spots) at.push_back({degrees(s.lat + 1000), degrees(s.lon + 2000)});
    const std::vector<std::string> answers = server_answers(single + "/planet", at, root.path() + "/single-q", log);
    REQUIRE(answers.size() == spots.size());
    for (size_t i = 0; i < spots.size(); i++)
        for (size_t j = 0; j < spots.size(); j++) CHECK_EQ(says(answers[i], spots[j].name), i == j);

    // Every region's dirs: Europe's records are the planet's, in its order.
    // The same data again on top of that build keeps every record's slot.
    auto build = [&](const std::string& out, std::vector<std::string> extra) {
        std::vector<std::string> words = {GEOCODER_BUILD_INDEX_BIN, out, pbf, "--continents", "--multi-output",
                                          "--multi-quality"};
        words.insert(words.end(), extra.begin(), extra.end());
        return gctest::run(words, log);
    };
    const std::string first = root.path() + "/first";
    REQUIRE(build(first, {}));
    const auto files = tree(first);
    check_order(files, "planet/full/");
    check_order(files, "europe/full/");
    const std::string again = root.path() + "/again";
    REQUIRE(build(again, {"--prev-output", first}));
    CHECK(tree(again) == files);
}

TEST(a_build_writes_its_postcode_centroids_in_cell_order_and_the_server_answers_from_them) {
    const ScratchDir root("postcode-centroids-build");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log", out = root.path() + "/out";
    write_test_pbf(pbf, with_postcodes(postcoded_addresses()), 1000);
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, out, pbf, "--continents", "--multi-output", "--multi-quality"}, log));
    const auto files = tree(out);

    // Each mode dir of the planet and of Europe holds the two files, the same
    // bytes, and nothing else of the centroids: no entries file, no slots.
    const auto planet = files.at("planet/admin/" + std::string(postcode_files::CENTROIDS_FILE));
    for (const std::string& name : names_of(files)) {
        const std::string file = fs::path(name).filename().string();
        CHECK(file != "postcode_centroid_entries.bin" && file != "postcode_centroids.osm_ids");
    }
    for (const char* region : {"planet", "europe"})
        for (const char* mode : {"admin", "no-addresses", "full"})
            for (const char* f : {postcode_files::CENTROIDS_FILE, postcode_files::CELLS_FILE}) {
                const std::string at = std::string(region) + "/" + mode + "/" + f;
                REQUIRE(files.count(at));
                CHECK(files.at(at) == files.at(std::string("planet/admin/") + f));
            }
    // A region without postcodes has neither file.
    CHECK(!files.count("asia/admin/" + std::string(postcode_files::CENTROIDS_FILE)));
    CHECK(!files.count("asia/admin/" + std::string(postcode_files::CELLS_FILE)));

    // The centroids of the five postcodes that pass their country's pattern,
    // each the mean of its addresses, in cell order with the admin cell of its
    // point; a cell holds two of them.
    const std::vector<postcode_files::LogicalCentroid> rows = centroids_of(files, "planet/admin/");
    struct Want {
        double lat, lng;
        uint16_t cc;
    };
    const std::vector<Want> want = {{47.031, 8.033, CH}, {47.0313, 8.0333, CH}, {47.17, 8.17, CH},
                                    {46.85, 8.05, DE},   {46.85, 8.052, DE}};
    REQUIRE(rows.size() == want.size());
    std::set<uint32_t> keys;
    for (const Want& w : want) {
        size_t found = 0;
        for (const auto& r : rows)
            found += std::fabs(r.lat - w.lat) < 1e-5 && std::fabs(r.lng - w.lng) < 1e-5 && r.country_code == w.cc;
        CHECK_EQ(found, size_t(1));
    }
    for (size_t i = 0; i < rows.size(); i++) {
        const auto& r = rows[i];
        const S2CellId cell = S2CellId(S2LatLng::FromDegrees(r.lat, r.lng)).parent(kAdminCellLevel);
        CHECK_EQ(uint64_t(r.cell_key) << 32, cell.id());
        CHECK(i == 0 || rows[i - 1].cell_key <= r.cell_key);
        keys.insert(r.cell_key);
    }
    CHECK(keys.size() < rows.size());

    // The server, from the dir of an admin preset, answers each point by the
    // nearest centroid of its country within 0.05 degrees: none for the two
    // points with none that near.
    const std::string dir = admin_preset_dir(out, "planet", root.path() + "/preset");
    const std::vector<std::pair<double, double>> points = {
        {47.0312, 8.0332}, {47.169, 8.169}, {46.851, 8.0505}, {46.8505, 8.0516}, {47.1, 8.1}, {47.19, 8.01}};
    const std::vector<const char*> postcodes = {"8004", "8002", "78462", "78464", nullptr, nullptr};
    const std::vector<std::string> answers = server_answers(dir, points, root.path() + "/q", log);
    REQUIRE(answers.size() == points.size());
    const int failures_before = gctest::failures();
    for (size_t i = 0; i < points.size(); i++) {
        CHECK(says(answers[i], "Testland") || says(answers[i], "Testreich"));
        const std::optional<std::string> postcode = answer_string(answers[i], {"address", "postcode"});
        if (postcodes[i]) CHECK(postcode == std::string(postcodes[i]));
        else CHECK(!postcode);
    }
    if (gctest::failures() != failures_before)
        for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_house_whose_postcode_its_country_rejects_answers_its_streets) {
    // Two houses of Teststrasse in Testland (CH) beside its middle, one tagged 8003 and one 80A1,
    // which no Swiss postcode is like: the clean-postcodes sanitizer makes that tag an address
    // term, so its house takes the street's postcode (placex_update), the centroid nearest the
    // street's middle, 8003. The other house answers its own.
    const ScratchDir root("rejected-house-postcode");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log", out = root.path() + "/out";
    const int32_t lat0 = 470000000, lng0 = 80000000;
    write_test_pbf(pbf, with_postcodes({{7010, lat0 + 1405000, lng0 + 1505000, "8003"}, {7011, lat0 + 1395000, lng0 + 1495000, "80A1"}}),
                   1000);
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, out, pbf}, log));
    const std::vector<std::pair<double, double>> points = {{47.1395, 8.1495}, {47.1405, 8.1505}};
    const std::vector<std::string> answers = server_answers(out + "/planet", points, root.path() + "/q", log);
    REQUIRE(answers.size() == points.size());
    const int failures_before = gctest::failures();
    for (const std::string& answer : answers) {
        CHECK(answer_string(answer, {"address", "postcode"}) == std::string("8003"));
        CHECK(answer_string(answer, {"address", "road"}) == std::string("Teststrasse"));
    }
    CHECK(answer_string(answers[0], {"address", "house_number"}) == std::string("7011"));
    if (gctest::failures() != failures_before)
        for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(postcode_centroids_keep_no_slots_and_a_region_of_no_valid_postcode_has_no_files) {
    // The next day 8002's address is gone and 8007's is new. A build of that
    // day on the day before writes the files a fresh build of it does: a
    // centroid that is gone leaves nothing, and one that is new takes no
    // slot of its own.
    const ScratchDir root("postcode-centroids-chained");
    const std::string log = root.path() + "/build.log";
    std::vector<Postcoded> next = postcoded_addresses();
    next.erase(std::remove_if(next.begin(), next.end(), [](const Postcoded& a) { return std::strcmp(a.postcode, "8002") == 0; }),
               next.end());
    next.push_back({7008, 470000000 + 900000, 80000000 + 1200000, "8007"});
    auto build = [&](const std::string& name, const std::vector<Postcoded>& addresses, std::vector<std::string> extra) {
        const std::string pbf = root.path() + "/" + name + ".osm.pbf", out = root.path() + "/" + name;
        write_test_pbf(pbf, with_postcodes(addresses), 1000);
        std::vector<std::string> words = {GEOCODER_BUILD_INDEX_BIN, out, pbf, "--multi-output", "--multi-quality"};
        words.insert(words.end(), extra.begin(), extra.end());
        REQUIRE(gctest::run(words, log));
        return tree(out);
    };
    const auto one = build("one", postcoded_addresses(), {});
    const auto fresh = build("fresh", next, {});
    const auto chained = build("chained", next, {"--prev-output", root.path() + "/one"});
    for (const char* mode : {"admin", "no-addresses", "full"})
        for (const char* f : {postcode_files::CENTROIDS_FILE, postcode_files::CELLS_FILE}) {
            const std::string at = std::string("planet/") + mode + "/" + f;
            REQUIRE(fresh.count(at) && chained.count(at) && one.count(at));
            CHECK(chained.at(at) == fresh.at(at));
        }
    // Day one holds 8002's centroid and not 8007's, the chained day 8007's and
    // not 8002's: each the one address of its postcode, at its point.
    const std::vector<postcode_files::LogicalCentroid> day_one = centroids_of(one, "planet/full/"),
                                                       day_two = centroids_of(chained, "planet/full/");
    CHECK_EQ(day_one.size(), size_t(5));
    CHECK_EQ(day_two.size(), size_t(5));
    auto at = [](const std::vector<postcode_files::LogicalCentroid>& rows, double lat, double lng) {
        size_t found = 0;
        for (const auto& r : rows) found += std::fabs(r.lat - lat) < 1e-5 && std::fabs(r.lng - lng) < 1e-5 && r.country_code == CH;
        return found;
    };
    const double p8002[2] = {47.17, 8.17}, p8007[2] = {47.09, 8.12};
    CHECK_EQ(at(day_one, p8002[0], p8002[1]), size_t(1));
    CHECK_EQ(at(day_one, p8007[0], p8007[1]), size_t(0));
    CHECK_EQ(at(day_two, p8007[0], p8007[1]), size_t(1));
    CHECK_EQ(at(day_two, p8002[0], p8002[1]), size_t(0));
    for (const auto& tree_files : {one, fresh, chained})
        for (const std::string& name : names_of(tree_files)) CHECK(fs::path(name).filename() != "postcode_centroids.osm_ids");

    // Addresses whose only postcode no Swiss postcode is like keep none, so
    // their region has no centroid files, as one without postcodes.
    const auto none = build("none", {{7005, 470000000 + 1700000, 80000000 + 300000, "80A1"}}, {});
    for (const char* mode : {"admin", "no-addresses", "full"}) {
        const std::string at = std::string("planet/") + mode + "/";
        CHECK(!none.count(at + postcode_files::CENTROIDS_FILE) && !none.count(at + postcode_files::CELLS_FILE));
    }
}

TEST(a_build_writes_the_postcode_grid_of_its_us_addresses_in_every_mode_dir_and_the_server_answers_from_it) {
    // Testland as the United States: 19026's houses along 47.05 N from 8.02
    // to 8.12 E, 19018's from 8.135 to 8.15. At 8.121 E the nearest centroid
    // is 19018's (8.1425, 1.6 km) rather than 19026's (8.07, 3.9 km); the
    // house 76 m west of it is 19026's. A house in Testreich (DE) names a US
    // ZIP no other house names, 19099, and casts no vote: no run names it.
    const ScratchDir root("postcode-grid-build");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log", out = root.path() + "/out";
    std::vector<Postcoded> houses;
    for (int i = 0; i <= 20; i++) houses.push_back({8100 + i, 470500000, 80200000 + 50000 * i, "19026"});
    for (int i = 0; i < 4; i++) houses.push_back({8200 + i, 470500000, 81350000 + 50000 * i, "19018-0002"});
    houses.push_back({8300, 468500000, 80500000, "19099"});
    std::vector<TestObject> objects = with_postcodes(houses);
    for (TestObject& o : objects)
        if (o.type == OsmType::Relation && o.id == 40)
            for (auto& [key, value] : o.tags)
                if (key == "ISO3166-1" || key == "ISO3166-1:alpha2") value = "US";
    write_test_pbf(pbf, objects, 1000);
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, out, pbf, "--continents", "--multi-output", "--multi-quality"}, log));
    const auto files = tree(out);

    // Every mode dir of the planet and of Europe holds it, the same bytes;
    // no other region does.
    const std::string name = postcode_grid::FILE_NAME;
    REQUIRE(files.count("planet/admin/" + name));
    const std::vector<char>& grid = files.at("planet/admin/" + name);
    for (const char* region : {"planet", "europe"})
        for (const char* mode : {"admin", "no-addresses", "full", "admin-minimal", "admin-minimal-poi"}) {
            const std::string at = std::string(region) + "/" + mode + "/" + name;
            REQUIRE(files.count(at));
            CHECK(files.at(at) == grid);
        }
    for (const std::string& file : names_of(files))
        if (fs::path(file).filename() == name) CHECK(file.rfind("planet/", 0) == 0 || file.rfind("europe/", 0) == 0);

    // Its runs: 19018 and 19026 of the US, at level 16; each house's cell
    // has its postcode.
    std::vector<postcode_grid::LogicalRow> rows;
    postcode_grid::decode(reinterpret_cast<const uint8_t*>(grid.data()), grid.size(),
                          [&](const postcode_grid::LogicalRow& r) { rows.push_back(r); });
    REQUIRE(!rows.empty());
    std::set<std::string> named;
    for (const auto& r : rows) {
        int level = 0;
        CHECK(postcode_grid::level_of(r.cell, level) && level == 16);
        CHECK(r.country_code == 0 || r.country_code == pack_country_code('U', 'S'));
        if (r.country_code) named.insert(std::string(postcode_grid::code_of(r)));
    }
    CHECK(named == std::set<std::string>({"19018", "19026"}));
    auto grid_at = [&](double lat, double lng) {
        const uint64_t cell = S2CellId(S2LatLng::FromDegrees(lat, lng)).parent(16).id();
        std::string code = "-";
        for (const auto& r : rows)
            if (r.cell <= cell) code = r.country_code ? std::string(postcode_grid::code_of(r)) : "-";
        return code;
    };
    for (const Postcoded& h : houses)
        if (h.lat == 470500000) CHECK_EQ(grid_at(h.lat * 1e-7, h.lon * 1e-7), std::string(h.postcode).substr(0, 5));

    // The server, from the dir of an admin preset and from admin-minimal,
    // answers each point from the grid: 19026 beside its last house, where
    // the nearest centroid is 19018's.
    const std::vector<std::pair<double, double>> points = {{47.0503, 8.121}, {47.0497, 8.14}, {47.0502, 8.03}};
    const std::vector<std::string> want = {"19026", "19018", "19026"};
    for (const std::string& dir : {admin_preset_dir(out, "planet", root.path() + "/preset"), out + "/planet/admin-minimal"}) {
        const std::vector<std::string> answers = server_answers(dir, points, root.path() + "/q", log);
        REQUIRE(answers.size() == points.size());
        for (size_t i = 0; i < points.size(); i++) {
            CHECK(says(answers[i], "Testland"));
            const std::optional<std::string> postcode = answer_string(answers[i], {"address", "postcode"});
            CHECK(postcode == want[i]);
            if (postcode != want[i]) std::printf("    %s: %s\n", dir.c_str(), answers[i].c_str());
        }
    }
}

namespace {

// The ids of the entries the attribution.json at `path` lists, in order;
// "none" for no such file.
std::vector<std::string> credited(const std::map<std::string, std::vector<char>>& files, const std::string& path) {
    const auto it = files.find(path);
    if (it == files.end()) return {"none"};
    const std::string text(it->second.begin(), it->second.end());
    const std::string key = "\"id\": \"";
    std::vector<std::string> ids;
    for (size_t at = text.find(key); at != std::string::npos; at = text.find(key, at + 1)) {
        const size_t from = at + key.size();
        ids.push_back(text.substr(from, text.find('"', from) - from));
    }
    return ids;
}

}  // namespace

TEST(each_mode_dir_credits_the_sources_of_its_data) {
    const ScratchDir root("attribution-build");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log";
    write_test_pbf(pbf, with_postcodes(postcoded_addresses()), 1000);
    auto write = [](const std::string& path, const std::string& text) {
        std::ofstream f(path, std::ios::binary);
        f << text;
        REQUIRE(f.good());
    };
    // GeoNames rows in London and of a Swiss postcode the fixture's addresses
    // have, whose centroid OSM's takes; a TIGER range in New York, with its
    // ZIP.
    const std::string header = "postcode,lat,lon,country_code\n", swiss = "8004,47.0313,8.0333,CH\n";
    const std::string london = root.path() + "/london.csv", zurich = root.path() + "/zurich.csv", tiger = root.path() + "/tiger";
    write(london, header + "SW1A 1AA,51.501,-0.142,GB\n" + swiss);
    write(zurich, header + swiss);
    fs::create_directories(tiger);
    write(tiger + "/36061.csv", "from;to;interpolation;street;city;state;postcode;geometry\n"
                                "100;198;even;West 31st Street;New York;NY;10001;LINESTRING(-73.995 40.749,-73.993 40.750)\n");
    auto build = [&](const std::string& name, std::vector<std::string> extra) {
        const std::string out = root.path() + "/" + name;
        std::vector<std::string> words = {GEOCODER_BUILD_INDEX_BIN, out, pbf};
        words.insert(words.end(), extra.begin(), extra.end());
        REQUIRE(gctest::run(words, log));
        return tree(out);
    };
    using Ids = std::vector<std::string>;
    const Ids osm = {"osm"};

    // London's centroid is in the planet and Europe, New York's range and
    // its ZIP's centroid in the planet and North America, of no country there
    // to vote in a postcode grid; the admin-minimal dirs hold no postcode
    // centroids, nor do boundary tiers or POI tiers credit.
    const auto files = build("multi", {"--continents", "--multi-output", "--multi-quality", "--external-postcodes", london,
                                       "--tiger-data", tiger});
    const std::map<std::string, Ids> want = {
        {"planet", {"osm", "geonames", "codepoint-open", "tiger"}},
        {"europe", {"osm", "geonames", "codepoint-open"}},
        {"north-america", {"osm", "tiger"}}};
    for (const std::string& region : kRegions) {
        const auto it = want.find(region);
        for (const char* mode : {"full", "no-addresses", "admin"}) {
            const std::string at = region + "/" + mode + "/attribution.json";
            const Ids got = credited(files, at);
            if (got != (it == want.end() ? osm : it->second)) std::printf("    %s\n", at.c_str());
            CHECK(got == (it == want.end() ? osm : it->second));
        }
        for (const char* dir : {"admin-minimal", "admin-minimal-poi"})
            if (kRegionsHeld.count(region)) CHECK(credited(files, region + "/" + dir + "/attribution.json") == osm);
    }
    // Each region's three water dirs credit the data of their own
    // (water_layer.h): OSM's alone in a build without --marine.
    size_t credits = 0, water_credits = 0;
    for (const std::string& name : names_of(files)) {
        if (fs::path(name).filename() != "attribution.json") continue;
        if (name.find("/water/") != std::string::npos) {
            water_credits++;
            if (credited(files, name) != osm) std::printf("    %s\n", name.c_str());
            CHECK(credited(files, name) == osm);
            continue;
        }
        credits++;
        CHECK(name.find("/quality/") == std::string::npos && name.find("/poi/") == std::string::npos);
    }
    CHECK_EQ(credits, kRegions.size() * 3 + kRegionsHeld.size() * 2);
    CHECK_EQ(water_credits, kRegions.size() * 3);

    // A GeoNames row whose pair OSM holds gives no centroid, so no credit.
    const auto taken = build("taken", {"--continents", "--multi-output", "--multi-quality", "--external-postcodes", zurich});
    for (const char* region : {"planet", "europe"})
        for (const char* mode : {"full", "no-addresses", "admin"})
            CHECK(credited(taken, std::string(region) + "/" + mode + "/attribution.json") == osm);

    // A Swiss row of a postcode OSM has none for is GeoNames's, of no GB
    // data: no Code-Point Open, nor GB_full's notice.
    const std::string bern = root.path() + "/bern.csv";
    write(bern, header + "8099,47.05,8.05,CH\n");
    const auto swiss_only = build("swiss", {"--continents", "--multi-output", "--multi-quality", "--external-postcodes", bern});
    for (const char* region : {"planet", "europe"})
        for (const char* mode : {"full", "no-addresses", "admin"}) {
            const std::string at = std::string(region) + "/" + mode + "/attribution.json";
            if (credited(swiss_only, at) != Ids({"osm", "geonames"})) std::printf("    %s\n", at.c_str());
            CHECK(credited(swiss_only, at) == Ids({"osm", "geonames"}));
            const auto& file = swiss_only.at(at);
            CHECK(std::string(file.begin(), file.end()).find("GB_full") == std::string::npos);
        }

    // A whole dir, without the multi flags, credits its sources too.
    const auto single = build("single", {"--tiger-data", tiger});
    CHECK(credited(single, "planet/attribution.json") == Ids({"osm", "tiger"}));

    // A TIGER range without a ZIP gives an interpolation and no centroid: the
    // full dirs that hold it credit TIGER, and the modes without addresses
    // don't.
    const std::string unzipped = root.path() + "/tiger-unzipped";
    fs::create_directories(unzipped);
    write(unzipped + "/36061.csv", "from;to;interpolation;street;city;state;postcode;geometry\n"
                                   "100;198;even;West 31st Street;New York;NY;;LINESTRING(-73.995 40.749,-73.993 40.750)\n");
    const auto ranges = build("unzipped", {"--continents", "--multi-output", "--multi-quality", "--tiger-data", unzipped});
    for (const std::string& region : kRegions) {
        const bool holds = region == "planet" || region == "north-america";
        CHECK_EQ(ways_of<InterpWay>(way_files::INTERPS, ranges, region + "/full/").ways.size(), size_t(holds ? 1 : 0));
        for (const char* mode : {"full", "no-addresses", "admin"}) {
            const std::string at = region + "/" + mode + "/attribution.json";
            const Ids want = holds && !std::strcmp(mode, "full") ? Ids({"osm", "tiger"}) : osm;
            if (credited(ranges, at) != want) std::printf("    %s\n", at.c_str());
            CHECK(credited(ranges, at) == want);
        }
    }
}

// The postcode grid holds the ZIPs of the TIGER ranges that vote in it, so
// every mode dir holding a grid of their votes credits TIGER, the
// admin-minimal dirs too, which hold no centroid; a grid of OSM's houses'
// votes alone is OpenStreetMap's data. Testland is the United States, with
// 19026's houses along 47.05 N, and a range of 19026 among them, whose ZIP
// gives no centroid: OSM's houses have one of it.
TEST(a_mode_dir_holding_a_postcode_grid_of_tiger_ranges_credits_tiger) {
    const ScratchDir root("postcode-grid-credit");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log", tiger = root.path() + "/tiger";
    std::vector<Postcoded> houses;
    for (int i = 0; i <= 20; i++) houses.push_back({8100 + i, 470500000, 80200000 + 50000 * i, "19026"});
    std::vector<TestObject> objects = with_postcodes(houses);
    for (TestObject& o : objects)
        if (o.type == OsmType::Relation && o.id == 40)
            for (auto& [key, value] : o.tags)
                if (key == "ISO3166-1" || key == "ISO3166-1:alpha2") value = "US";
    write_test_pbf(pbf, objects, 1000);
    fs::create_directories(tiger);
    {
        std::ofstream f(tiger + "/42101.csv", std::ios::binary);
        f << "from;to;interpolation;street;city;state;postcode;geometry\n"
             "100;198;even;Teststrasse;Testville;PA;19026;LINESTRING(8.05 47.051,8.06 47.051)\n";
        REQUIRE(f.good());
    }
    using Ids = std::vector<std::string>;
    for (const bool ranges : {false, true}) {
        const std::string out = root.path() + (ranges ? "/ranges" : "/houses");
        std::vector<std::string> words = {GEOCODER_BUILD_INDEX_BIN, out, pbf, "--continents", "--multi-output", "--multi-quality"};
        if (ranges) words.insert(words.end(), {"--tiger-data", tiger});
        REQUIRE(gctest::run(words, log));
        const auto files = tree(out);
        const Ids want = ranges ? Ids{"osm", "tiger"} : Ids{"osm"};
        for (const char* region : {"planet", "europe"})
            for (const char* mode : {"admin", "no-addresses", "full", "admin-minimal", "admin-minimal-poi"}) {
                const std::string dir = std::string(region) + "/" + mode + "/";
                CHECK(files.count(dir + postcode_grid::FILE_NAME));
                if (credited(files, dir + "attribution.json") != want) std::printf("    %s\n", dir.c_str());
                CHECK(credited(files, dir + "attribution.json") == want);
            }
    }
}

// A build that ranks its POIs by Wikidata's sitelinks (--wikidata-sitelinks)
// credits Wikidata in the poi_attribution.json of each POI tier dir holding
// a POI whose importance took its item's count, beside the mode dir's credit
// of the OpenStreetMap data; a tier of none of them, or a build without
// them, writes none there. A museum (20) of Wikidata's Q4242 and 9 sitelinks
// takes 20 x (1 + log2(10) / 3) = 42 for its importance, where without them
// its wikidata tag gives 20 x 1.5 = 30; an artwork (2) of Q4243, tier 2 with
// its tag, 2 x (1 + log2(10) / 3) = 4, else 3. Downloads land flat by name,
// so no POI tier dir holds an attribution.json, which would take the place
// of the mode dir's.
TEST(poi_tiers_ranked_by_wikidata_sitelinks_credit_wikidata) {
    const ScratchDir root("wikidata-credit-build");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log";
    const int32_t lat0 = 470000000, lng0 = 80000000;
    write_fixture_with(pbf, {node(990, lat0 + 1404000, lng0 + 1424000,
                                  {{"name", "Museum Testdorf"}, {"tourism", "museum"}, {"wikidata", "Q4242"}}),
                             node(991, lat0 + 1404000, lng0 + 1434000,
                                  {{"name", "Kunstwerk Testdorf"}, {"tourism", "artwork"}, {"wikidata", "Q4243"}})});
    // Items of 9 sitelinks each, packed as extract_wikidata_sitelinks.py
    // packs them ('<IH'), in QID order.
    auto sitelinks = [&](const std::string& name, std::vector<uint32_t> qids) {
        const std::string path = root.path() + "/" + name;
        std::ofstream f(path, std::ios::binary);
        for (const uint32_t qid : qids) {
            const uint8_t entry[6] = {uint8_t(qid), uint8_t(qid >> 8), uint8_t(qid >> 16), uint8_t(qid >> 24), 9, 0};
            f.write(reinterpret_cast<const char*>(entry), sizeof entry);
        }
        REQUIRE(f.good());
        return path;
    };
    using Ids = std::vector<std::string>;
    const Ids wikidata = {"wikidata"}, none = {"none"};
    struct Build {
        const char* name;
        std::string sitelinks;
        // The credit of poi/major, poi/notable and poi/all.
        std::vector<Ids> credits;
        int museum, artwork;
    };
    const Build builds[] = {
        {"ranked", sitelinks("both.bin", {4242, 4243}), {wikidata, wikidata, wikidata}, 42, 4},
        // Of the artwork's item alone, and of one no POI has: poi/major
        // holds the museum only, of no count.
        {"artwork", sitelinks("artwork.bin", {1, 4243}), {none, wikidata, wikidata}, 30, 4},
        {"unmatched", sitelinks("unmatched.bin", {1}), {none, none, none}, 30, 3},
        {"plain", "", {none, none, none}, 30, 3},
    };
    for (const Build& b : builds) {
        const std::string out = root.path() + "/" + b.name;
        std::vector<std::string> words = {GEOCODER_BUILD_INDEX_BIN, out, pbf};
        if (!b.sitelinks.empty()) words.insert(words.end(), {"--wikidata-sitelinks", b.sitelinks});
        REQUIRE(gctest::run(words, log));
        const auto files = tree(out);
        const char* tiers[] = {"major", "notable", "all"};
        for (size_t t = 0; t < 3; t++) {
            const std::string dir = std::string("planet/poi/") + tiers[t] + "/", at = dir + "poi_attribution.json";
            if (credited(files, at) != b.credits[t]) std::printf("    %s: %s\n", b.name, at.c_str());
            CHECK(credited(files, at) == b.credits[t]);
            CHECK(!files.count(dir + "attribution.json"));
            // The importance each POI of the tier took.
            const std::vector<PoiRecord> pois = poi_records_in(files, std::string("planet/poi/") + tiers[t]);
            REQUIRE(pois.size() == (t == 0 ? 1u : 2u));
            for (const PoiRecord& pr : pois) {
                const bool museum = pr.category == uint8_t(PoiCategory::MUSEUM);
                if (pr.importance != (museum ? b.museum : b.artwork))
                    std::printf("    %s %s: category %d of importance %d\n", b.name, tiers[t], int(pr.category),
                                int(pr.importance));
                CHECK_EQ(int(pr.importance), museum ? b.museum : b.artwork);
                CHECK_EQ(int(pr.flags), int(POI_FLAG_WIKIDATA));
            }
        }
        CHECK(credited(files, "planet/attribution.json") == Ids{"osm"});
    }
}
