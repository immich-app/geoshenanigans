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
// does. Every mode dir credits the sources of the data it holds in
// attribution.json: OpenStreetMap, GeoNames and Code-Point Open, which its
// GB data is derived from, where a postcode centroid there came from it,
// TIGER where an interpolation or a centroid did.
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
// has place nodes, its admin-minimal dirs.
std::set<std::string> admin_dir_layout() {
    const std::vector<std::string> client = {"admin_polygons.bin", "admin_rings.bin", "arc_index.bin", "arc_vertices.bin"};
    std::set<std::string> out;
    for (const std::string& region : kRegions)
        for (const char* dir : {"quality/simplified", "quality/exact", "admin-minimal", "admin-minimal-poi"})
            if (kRegionsHeld.count(region) || std::string(dir).rfind("quality/", 0) == 0)
                for (const std::string& f : client) out.insert(region + "/" + dir + "/" + f);
    return out;
}

bool admin_geometry(const std::string& name) {
    const std::string file = fs::path(name).filename().string();
    return file == "admin_polygons.bin" || file == "admin_rings.bin" || file == "arc_index.bin" ||
           file == "arc_vertices.bin";
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
        it = admin_geometry(it->first) || file == "arc_index.osm_ids" ? std::next(it) : files.erase(it);
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
        std::ofstream f(coords);
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
                                 SideFile{"addr_postcodes.bin", "addr_points.bin"},
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
                                               "arc_vertices.bin"};
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
    // street's parent, one of the seven polygons, and no postcode, of which
    // the fixture has none; no interpolations, so no file of their postcodes.
    const auto side = side_files(files, "planet/");
    CHECK(names_of(side) == std::set<std::string>({"addr_postcodes.bin", "way_parents.bin", "way_postcodes.bin"}));
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
        CHECK_EQ(names_of(side_files(regions, region + "/")).size(), kRegionsHeld.count(region) ? size_t(3) : size_t(0));
        CHECK_EQ(ways_of<WayHeader>(way_files::STREETS, regions, region + "/").ways.size(),
                 kRegionsHeld.count(region) ? size_t(1) : size_t(0));
        CHECK(ways_of<InterpWay>(way_files::INTERPS, regions, region + "/").ways.empty());
        CHECK_EQ(regions.count(region + "/interp_ways.bin") + regions.count(region + "/interp_nodes.bin"), size_t(2));
    }
    check_answers(continents, "continents");
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
        if (e.path().filename() == "arc_index.fallback.osm_ids") fallbacks.push_back(e.path());
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
            // poi_meta.json lists the category the tier holds the way as.
            const auto& meta = files.at("planet/poi/" + tier + "/poi_meta.json");
            const std::string text(meta.begin(), meta.end());
            for (const int category : categories) CHECK(text.find("\"" + std::to_string(category) + "\":") != std::string::npos);
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
// takes none of. Nor does a lake that is a swimming area, whose island, a
// hole of it, the POI's polygon of its outer ring leaves out.
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
    TestObject island_lake = multipolygon(5046, 4046, {{"leisure", "swimming_area"}, {"name", "Inselsee Testdorf"},
                                                        {"natural", "water"}});
    island_lake.members.emplace_back(OsmType::Way, 4047, "inner");
    water.push_back(island_lake);
    water.push_back(multipolygon(5048, 4048, {{"building", "yes"}, {"landuse", "reservoir"}, {"name", "Stausee Testdorf"},
                                              {"tourism", "attraction"}}));
    write_fixture_with(pbf, water);
    const std::string out = root.path() + "/single";
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, out, pbf}, log));

    const std::vector<PoiRecord> pois = poi_records_in(tree(out), "planet/poi/all");
    const int bay = int(PoiCategory::BAY), swimming = int(PoiCategory::SWIMMING_AREA);
    const std::vector<int> want = {int(PoiCategory::ATTRACTION), int(PoiCategory::NATURE_RESERVE), bay, bay, bay,
                                   swimming, swimming, swimming};
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
                          Want{8.1524, int(PoiCategory::NATURE_RESERVE), false, 0}, Want{8.1539, swimming, false, 0},
                          Want{8.1554, int(PoiCategory::ATTRACTION), false, 0}}) {
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
                                                           {47.14031, 8.1554}};
    const std::vector<std::string> answers = server_answers(dataset, points, root.path() + "/q", log);
    REQUIRE(answers.size() == points.size());
    CHECK(answer_string(answers[0], {"display_name"}).value_or("").rfind("Badebucht Testdorf, ", 0) == 0);
    CHECK(answer_string(answers[1], {"display_name"}).value_or("").rfind("Bucht Testdorf", 0) == std::string::npos);
    const char* roads[] = {"Teststrasse", "Teststrasse", "Teststrasse", "Uferweg Testdorf", "Teststrasse",
                           "Teststrasse", "Teststrasse"};
    for (size_t k = 0; k < answers.size(); k++) {
        if (answer_string(answers[k], {"address", "road"}) != std::optional<std::string>(roads[k]))
            std::printf("    %s\n", answers[k].c_str());
        CHECK(answer_string(answers[k], {"address", "road"}) == std::optional<std::string>(roads[k]));
    }
    // The nature reserve is in `places`.
    CHECK(says(answers[4], "Ried Testdorf"));
    if (!says(answers[0], "Badebucht Testdorf")) std::printf("    %s\n", answers[0].c_str());
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

TEST(postcode_centroids_keep_no_slots_and_a_region_of_no_valid_postcode_has_files_of_none) {
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

    // Addresses whose only postcode no Swiss postcode is like: the files of no
    // centroid, the footer alone and an empty cells file.
    const auto none = build("none", {{7005, 470000000 + 1700000, 80000000 + 300000, "80A1"}}, {});
    for (const char* mode : {"admin", "no-addresses", "full"}) {
        const std::string at = std::string("planet/") + mode + "/";
        REQUIRE(none.count(at + postcode_files::CENTROIDS_FILE) && none.count(at + postcode_files::CELLS_FILE));
        CHECK(none.at(at + postcode_files::CENTROIDS_FILE) == std::vector<char>({0, 0, 0, 0, 'P', 'C', 'C', '1'}));
        CHECK(none.at(at + postcode_files::CELLS_FILE).empty());
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
    // its ZIP's centroid in the planet and North America; the admin-minimal
    // dirs hold no postcodes, nor do boundary tiers or POI tiers credit.
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
    size_t credits = 0;
    for (const std::string& name : names_of(files)) {
        if (fs::path(name).filename() != "attribution.json") continue;
        credits++;
        CHECK(name.find("/quality/") == std::string::npos && name.find("/poi/") == std::string::npos);
    }
    CHECK_EQ(credits, kRegions.size() * 3 + kRegionsHeld.size() * 2);

    // A GeoNames row whose pair OSM holds gives no centroid, so no credit.
    const auto taken = build("taken", {"--continents", "--multi-output", "--multi-quality", "--external-postcodes", zurich});
    for (const char* region : {"planet", "europe"})
        for (const char* mode : {"full", "no-addresses", "admin"})
            CHECK(credited(taken, std::string(region) + "/" + mode + "/attribution.json") == osm);

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

// A build that ranks its POIs by Wikidata's sitelinks (--wikidata-sitelinks)
// credits Wikidata in each POI tier dir's poi_attribution.json, beside the
// mode dir's credit of the OpenStreetMap data; one without them writes none
// there. Downloads land flat by name, so no POI tier dir holds an
// attribution.json, which would take the place of the mode dir's.
TEST(poi_tiers_ranked_by_wikidata_sitelinks_credit_wikidata) {
    const ScratchDir root("wikidata-credit-build");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log";
    const int32_t lat0 = 470000000, lng0 = 80000000;
    write_fixture_with(pbf, {node(990, lat0 + 1404000, lng0 + 1424000,
                                  {{"name", "Museum Testdorf"}, {"tourism", "museum"}, {"wikidata", "Q4242"}})});
    // Q4242 of 9 sitelinks, packed as extract_wikidata_sitelinks.py packs
    // them ('<IH').
    const std::string sitelinks = root.path() + "/qid_sitelinks.bin";
    {
        const uint8_t entry[6] = {0x92, 0x10, 0, 0, 9, 0};
        std::ofstream f(sitelinks, std::ios::binary);
        f.write(reinterpret_cast<const char*>(entry), sizeof entry);
        REQUIRE(f.good());
    }
    using Ids = std::vector<std::string>;
    for (const bool ranked : {true, false}) {
        const std::string out = root.path() + (ranked ? "/ranked" : "/plain");
        std::vector<std::string> words = {GEOCODER_BUILD_INDEX_BIN, out, pbf};
        if (ranked) words.insert(words.end(), {"--wikidata-sitelinks", sitelinks});
        REQUIRE(gctest::run(words, log));
        const auto files = tree(out);
        for (const char* tier : {"major", "notable", "all"}) {
            const std::string dir = std::string("planet/poi/") + tier + "/", at = dir + "poi_attribution.json";
            REQUIRE(files.count(dir + "poi_records.bin"));
            if (credited(files, at) != (ranked ? Ids{"wikidata"} : Ids{"none"})) std::printf("    %s\n", at.c_str());
            CHECK(credited(files, at) == (ranked ? Ids{"wikidata"} : Ids{"none"}));
            CHECK(!files.count(dir + "attribution.json"));
        }
        CHECK(credited(files, "planet/attribution.json") == Ids{"osm"});
    }
}
