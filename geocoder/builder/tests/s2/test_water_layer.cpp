// water_layer.h: the layer's build from what the passes collect (assembly,
// the naming join, the arcs, cells and major tier), a region's share, and
// its dirs with their slots from one day to the next.
#include "water_layer.h"

#include <algorithm>
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

#include "arc_blocks.h"
#include "captured_stderr.h"
#include "cell_pairs.h"
#include "scratch_dir.h"
#include "string_files.h"
#include "test_framework.h"
#include "water_names_s2.h"
#include "water_split.h"

namespace fs = std::filesystem;
using namespace water;

namespace {

using Ring = std::vector<PackedLocation>;

PackedLocation pt(int32_t lat, int32_t lng) { return {lat, lng}; }

// A square ring of side `side` e7 units from (lat, lng), counterclockwise,
// closed, with `per_side` vertices along each side.
Ring square(int32_t lat, int32_t lng, int32_t side, int per_side = 1) {
    Ring r;
    const PackedLocation corners[] = {pt(lat, lng), pt(lat, lng + side), pt(lat + side, lng + side), pt(lat + side, lng)};
    for (int e = 0; e < 4; e++) {
        const PackedLocation a = corners[e], b = corners[(e + 1) % 4];
        for (int k = 0; k < per_side; k++)
            r.push_back(pt(a.lat_e7 + int32_t(int64_t(b.lat_e7 - a.lat_e7) * k / per_side),
                           a.lon_e7 + int32_t(int64_t(b.lon_e7 - a.lon_e7) * k / per_side)));
    }
    r.push_back(r.front());
    return r;
}

ObjectTags tags(uint16_t code, const std::string& name, bool river = false, uint32_t qid = 0) {
    ObjectTags t;
    t.code = code;
    t.river_area = river;
    t.name = t.local_name = name;
    t.qid = qid;
    return t;
}

void add_geometry(Collected& c, int64_t way, const Ring& coords) {
    const size_t at = c.member_geometries.coords.size();
    c.member_geometries.coords.insert(c.member_geometries.coords.end(), coords.begin(), coords.end());
    c.member_geometries.ways[way] = {at, static_cast<uint32_t>(coords.size()), way * 10, way * 10 + 1};
}

const int32_t kLat = 470000000, kLng = 80000000;

// A lake relation (two outer ways, an island way), a closed-way pond, a
// river area its waterway names, one no line names, and a lake of its own
// type, verbatim.
Collected fixture() {
    Collected c;
    const Ring lake = square(kLat, kLng, 100000, 4), isle = square(kLat + 40000, kLng + 40000, 20000);
    // The lake's ring split in two ways at its corners 0 and 8.
    add_geometry(c, 11, Ring(lake.begin(), lake.begin() + 9));
    add_geometry(c, 12, Ring(lake.begin() + 8, lake.end()));
    add_geometry(c, 13, isle);
    CollectedArea rel;
    rel.id = 500;
    rel.tags = tags(code::kLake, "Test Lake", false, 42);
    rel.members = {{11, "outer"}, {12, "outer"}, {13, "inner"}};
    c.relations.push_back(rel);
    // A relation with a member the passes didn't find.
    CollectedArea broken = rel;
    broken.id = 501;
    broken.members.push_back({99, "outer"});
    c.relations.push_back(broken);
    c.closed_ways.push_back({20, tags(4, "Pond"), square(kLat + 200000, kLng, 1000)});
    ClosedWayArea river{30, tags(code::kRiver, "", true), square(kLat + 300000, kLng, 10000)};
    river.tags.name.clear();
    river.tags.local_name.clear();
    c.closed_ways.push_back(river);
    ClosedWayArea dry{31, river.tags, square(kLat + 400000, kLng, 10000)};
    c.closed_ways.push_back(dry);
    ObjectTags ganga = tags(0, "Ganga");
    ganga.verbatim = "ganga_river";
    c.closed_ways.push_back({32, ganga, square(kLat + 500000, kLng, 1000)});
    // The waterway through the river area, and a tributary with less length
    // in it.
    LineIn main;
    main.way = 40;
    main.key = "r900";
    main.local = "Testfluss";
    main.display = "Test River";
    main.qid = 77;
    main.vertices = {pt(kLat + 302000, kLng + 2000), pt(kLat + 305000, kLng + 5000), pt(kLat + 308000, kLng + 25000)};
    c.lines.push_back(main);
    LineIn tributary;
    tributary.way = 41;
    tributary.key = "nBach";
    tributary.local = "Bach";
    tributary.display = "Brook";
    tributary.vertices = {pt(kLat + 309000, kLng + 1000), pt(kLat + 309000, kLng + 3000)};
    c.lines.push_back(tributary);
    return c;
}

Layer build(Collected c, const std::string& prev = "", unsigned threads = 2) {
    const gctest::CapturedStderr log;
    return build_layer(c, prev, [](uint32_t qid) { return qid == 42 ? 50u : 0u; }, threads);
}

std::string name_of(const Layer& layer, size_t p) { return layer.names.data() + layer.records[p].name_id; }

std::vector<char> bytes_of(const fs::path& p) {
    std::ifstream f(p, std::ios::binary);
    return std::vector<char>(std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>());
}

std::map<std::string, std::vector<char>> tree(const std::string& dir) {
    std::map<std::string, std::vector<char>> files;
    for (const auto& e : fs::recursive_directory_iterator(dir))
        if (e.is_regular_file()) files[fs::relative(e.path(), dir).string()] = bytes_of(e.path());
    return files;
}

// A file's bytes as cell_pairs::Reader reads them.
struct Bytes {
    std::vector<char> data;
    uint64_t size() const { return data.size(); }
    const char* at(uint64_t off, size_t) const { return data.data() + off; }
};

// The name of each slot of the dir `dir` of `files`, from its
// strings_core.bin: "" for a tombstone.
std::vector<std::string> slot_names(const std::map<std::string, std::vector<char>>& files, const std::string& dir) {
    const std::vector<char>& core = files.at(dir + "/strings_core.bin");
    const std::vector<char>& records = files.at(dir + "/admin_polygons.bin");
    string_files::Decoder d(reinterpret_cast<const uint8_t*>(core.data()), core.size());
    std::vector<char> pool;
    while (d.next()) pool.insert(pool.end(), d.data(), d.data() + d.size());
    std::vector<std::string> out;
    for (size_t at = 0; at + sizeof(AdminPolygonRecord) <= records.size(); at += sizeof(AdminPolygonRecord)) {
        AdminPolygonRecord r;
        std::memcpy(&r, records.data() + at, sizeof r);
        out.push_back(r.name_id == NO_DATA ? "" : r.name_id < pool.size() ? pool.data() + r.name_id : "(out of the pool)");
    }
    return out;
}

std::string write(const Layer& layer, const std::string& dir, const std::string& prev = "") {
    const gctest::CapturedStderr log;
    const std::string summary = write_water_dirs(layer, dir, prev, "test");
    return summary + log.str();
}

}  // namespace

TEST(each_outer_ring_is_a_record_with_its_objects_name_type_and_area_and_its_holes) {
    const Layer layer = build(fixture());
    // Ways first by id, then relations: the pond, the named river area, the
    // Ganga, then the lake; the river area no line names and the relation
    // missing a member are out.
    REQUIRE(layer.size() == 4u);
    CHECK(name_of(layer, 0) == "Pond");
    CHECK(name_of(layer, 1) == "Test River");
    CHECK(name_of(layer, 2) == "Ganga");
    CHECK(name_of(layer, 3) == "Test Lake");
    CHECK_EQ(layer.records[0].country_code, uint16_t(4));
    CHECK_EQ(layer.records[1].country_code, code::kRiver);
    CHECK_EQ(layer.records[2].country_code, layer.types->verbatim_code("ganga_river"));
    CHECK_EQ(layer.records[3].country_code, code::kLake);
    // The lake's area: its outer ring less its island, in square degrees.
    CHECK(std::abs(layer.records[3].area - float(0.0001 - 0.000004)) < 1e-9f);
    // Its record's ring block holds the outer ring and the hole.
    const AdminArcPolygon& lake = layer.arcs.polygons[3];
    CHECK_EQ(lake.ring_count, 2u);
    std::vector<std::vector<arc_blocks::SlotRef>> rings;
    REQUIRE(arc_blocks::decode_rings(layer.arcs.set(ArcSet::Full).ring_blocks.data() + lake.blocks[0].offset,
                                     lake.blocks[0].size, rings));
    REQUIRE(rings.size() == 2u);
    // Its outer ring is cut where its two ways meet.
    CHECK_EQ(rings[0].size(), size_t(2));
    CHECK_EQ(rings[1].size(), size_t(1));
    // Keyed by OSM identity: ways by id, the relation's ring by its smallest
    // vertex, with a fallback by the relation and the ring's place.
    CHECK_EQ(layer.identities[0], way_identity(20));
    CHECK_EQ(layer.fallbacks[0], gc::id_alloc::kNoFallbackKey);
    CHECK_EQ(layer.identities[3] >> 56, uint64_t(gc::id_alloc::ObjectType::SYNTHETIC));
    CHECK_EQ(layer.fallbacks[3], relation_ring_fallback(500, 0));
    // Every polygon is in the cell index.
    std::set<uint32_t> listed;
    for (const CellItemPair& c : layer.cells) listed.insert(c.item_id & ID_MASK);
    CHECK_EQ(listed.size(), size_t(4));
}

TEST(the_join_credits_each_segment_the_length_it_has_inside_an_area) {
    Collected c = fixture();
    Build b = assemble(c, 1);
    std::vector<uint32_t> rivers;
    for (uint32_t p = 0; p < b.rings.polygons.size(); p++)
        if (b.objects[b.object_of[p]].tags.river_area) rivers.push_back(p);
    REQUIRE(rivers.size() == 2u);
    NamingCounts counts;
    std::vector<Credit> credits = credit_naming_lines(b.rings, rivers, b.object_of, b.lines, counts, 2);
    // The main line's first segment and the tributary are inside the river
    // area; the main line's second segment ends far east of it, a quarter of
    // it inside.
    uint64_t main_mm = 0, tributary_mm = 0;
    for (const Credit& cr : credits) {
        CHECK_EQ(b.objects[cr.object].id, int64_t(30));
        (b.lines.keys[cr.key] == "r900" ? main_mm : tributary_mm) += cr.mm;
    }
    REQUIRE(b.lines.vertices.size() == 5u);
    const PackedLocation* v = b.lines.vertices.data();
    CHECK_EQ(main_mm, segment_mm(v[0], v[1]) + share_mm(v[1], v[2], 0.25L));
    CHECK_EQ(tributary_mm, segment_mm(v[3], v[4]));
    const std::vector<NameChoice> choices = pick_names(credits, b.objects.size(), 1);
    size_t named = 0;
    for (size_t o = 0; o < b.objects.size(); o++) {
        if (b.objects[o].id != 30) continue;
        REQUIRE(choices[o].named());
        named++;
        CHECK(b.lines.keys[choices[o].key] == "r900");
        CHECK(b.lines.displays[choices[o].display] == "Test River");
    }
    CHECK_EQ(named, size_t(1));
}

TEST(a_segment_across_an_area_with_both_ends_and_its_middle_outside_names_it) {
    // A line across the river area nothing names, from 0.005 west of it to
    // 0.001 east of it: a seventh of it inside.
    Collected c = fixture();
    LineIn across;
    across.way = 47;
    across.key = "nLang";
    across.local = across.display = "Lang";
    across.vertices = {pt(kLat + 405000, kLng - 50000), pt(kLat + 405000, kLng + 20000)};
    c.lines.push_back(across);
    Collected joined = c;
    Build b = assemble(joined, 1);
    std::vector<uint32_t> rivers;
    for (uint32_t p = 0; p < b.rings.polygons.size(); p++)
        if (b.objects[b.object_of[p]].tags.river_area) rivers.push_back(p);
    NamingCounts counts;
    std::vector<Credit> credits = credit_naming_lines(b.rings, rivers, b.object_of, b.lines, counts, 2);
    uint64_t mm = 0;
    for (const Credit& cr : credits)
        if (b.objects[cr.object].id == 31) mm += cr.mm;
    CHECK_EQ(mm, share_mm(across.vertices[0], across.vertices[1], 1.0L / 7));
    // So the area takes its name.
    const Layer layer = build(c);
    std::set<std::string> names;
    for (size_t p = 0; p < layer.size(); p++) names.insert(name_of(layer, p));
    CHECK(names.count("Lang"));
}

TEST(a_segment_of_no_length_lends_its_line_no_wikidata_item) {
    // Two lines of one key in the river area no line names: one along it,
    // of Q50, and one of two vertices at one point, of Q3.
    Collected c = fixture();
    LineIn mill;
    mill.way = 45;
    mill.key = "nMill";
    mill.local = mill.display = "Mill";
    mill.qid = 50;
    mill.vertices = {pt(kLat + 401000, kLng + 1000), pt(kLat + 401000, kLng + 9000)};
    c.lines.push_back(mill);
    LineIn point = mill;
    point.way = 46;
    point.qid = 3;
    point.vertices = {pt(kLat + 405000, kLng + 5000), pt(kLat + 405000, kLng + 5000)};
    c.lines.push_back(point);
    Build b = assemble(c, 1);
    std::vector<uint32_t> rivers;
    for (uint32_t p = 0; p < b.rings.polygons.size(); p++)
        if (b.objects[b.object_of[p]].tags.river_area) rivers.push_back(p);
    NamingCounts counts;
    std::vector<Credit> credits = credit_naming_lines(b.rings, rivers, b.object_of, b.lines, counts, 2);
    for (const Credit& cr : credits) CHECK(cr.mm > 0);
    const std::vector<NameChoice> choices = pick_names(credits, b.objects.size(), 1);
    size_t named = 0;
    for (size_t o = 0; o < b.objects.size(); o++) {
        if (b.objects[o].id != 31) continue;
        REQUIRE(choices[o].named());
        named++;
        CHECK(b.lines.keys[choices[o].key] == "nMill");
        CHECK_EQ(choices[o].qid, 50u);
    }
    CHECK_EQ(named, size_t(1));
}

TEST(a_segment_across_the_antimeridian_credits_the_river_areas_it_runs_through_on_either_side) {
    // Two unnamed river areas, 0.01 by 0.01 degrees, either side of the
    // antimeridian along it, and one segment of a line from the middle of
    // one to the middle of the other: each holds the half of it on its side.
    Collected c = fixture();
    const int32_t lat = 650000000, east = kAntimeridianE7 - 100000, west = -kAntimeridianE7;
    c.closed_ways.push_back({2201, tags(code::kRiver, "", true), square(lat, east, 100000)});
    c.closed_ways.push_back({2202, tags(code::kRiver, "", true), square(lat, west, 100000)});
    LineIn anadyr;
    anadyr.way = 2210;
    anadyr.key = "nAnadyr";
    anadyr.local = anadyr.display = "Anadyr";
    anadyr.vertices = {pt(lat + 40000, east + 50000), pt(lat + 60000, west + 50000)};
    c.lines.push_back(anadyr);
    Collected joined = c;
    Build b = assemble(joined, 1);
    std::vector<uint32_t> rivers;
    for (uint32_t p = 0; p < b.rings.polygons.size(); p++)
        if (b.objects[b.object_of[p]].tags.river_area) rivers.push_back(p);
    NamingCounts counts;
    std::vector<Credit> credits = credit_naming_lines(b.rings, rivers, b.object_of, b.lines, counts, 2);
    std::map<int64_t, uint64_t> mm;
    for (const Credit& cr : credits) mm[b.objects[cr.object].id] += cr.mm;
    const PackedLocation seam_east = pt(lat + 50000, kAntimeridianE7), seam_west = pt(lat + 50000, -kAntimeridianE7);
    CHECK_EQ(mm[2201], segment_mm(anadyr.vertices[0], seam_east));
    CHECK_EQ(mm[2202], segment_mm(seam_west, anadyr.vertices[1]));
    // So both take its name.
    const Layer layer = build(c);
    std::set<int64_t> named;
    for (size_t p = 0; p < layer.size(); p++)
        for (int64_t id : {2201, 2202})
            if (layer.identities[p] == way_identity(id) && name_of(layer, p) == "Anadyr") named.insert(id);
    CHECK(named == std::set<int64_t>({2201, 2202}));
}

TEST(a_reach_along_the_antimeridian_credits_the_river_area_west_of_it_whichever_way_its_ends_are_spelled) {
    // The two unnamed river areas either side of the antimeridian, as above,
    // and a line whose one segment runs along it between them, 0.002 to
    // 0.008 up their sides: run north, the area to its left is the one west
    // of it, at 180 east, which holds all of it, however its ends are spelled
    // and whichever way it runs; the one east of it holds none.
    const int32_t lat = 650000000, east = kAntimeridianE7 - 100000, west = -kAntimeridianE7;
    const int32_t E = kAntimeridianE7, W = -kAntimeridianE7, s = lat + 20000, n = lat + 80000;
    const std::vector<std::pair<PackedLocation, PackedLocation>> reaches = {
        {pt(s, E), pt(n, E)}, {pt(s, W), pt(n, W)}, {pt(n, W), pt(s, W)}, {pt(s, E), pt(n, W)},
        {pt(s, W), pt(n, E)}, {pt(n, E), pt(s, W)}, {pt(n, W), pt(s, E)}};
    const uint64_t reach_mm = segment_mm(pt(s, E), pt(n, E));
    REQUIRE(reach_mm > 0);
    for (const auto& [a, b] : reaches) {
        Collected c = fixture();
        c.closed_ways.push_back({2201, tags(code::kRiver, "", true), square(lat, east, 100000)});
        c.closed_ways.push_back({2202, tags(code::kRiver, "", true), square(lat, west, 100000)});
        LineIn anadyr;
        anadyr.way = 2210;
        anadyr.key = "nAnadyr";
        anadyr.local = anadyr.display = "Anadyr";
        anadyr.vertices = {a, b};
        c.lines.push_back(anadyr);
        Collected joined = c;
        Build built = assemble(joined, 1);
        std::vector<uint32_t> rivers;
        for (uint32_t p = 0; p < built.rings.polygons.size(); p++)
            if (built.objects[built.object_of[p]].tags.river_area) rivers.push_back(p);
        NamingCounts counts;
        std::vector<Credit> credits = credit_naming_lines(built.rings, rivers, built.object_of, built.lines, counts, 2);
        std::map<int64_t, uint64_t> mm;
        for (const Credit& cr : credits) mm[built.objects[cr.object].id] += cr.mm;
        CHECK_EQ(mm[2201], reach_mm);
        CHECK_EQ(mm[2202], uint64_t(0));
        // So the one west of it takes its name, and the other none.
        const Layer layer = build(c);
        std::set<int64_t> named;
        for (size_t p = 0; p < layer.size(); p++)
            for (int64_t id : {2201, 2202})
                if (layer.identities[p] == way_identity(id) && name_of(layer, p) == "Anadyr") named.insert(id);
        CHECK(named == std::set<int64_t>({2201}));
    }
}

TEST(the_join_of_no_lines_credits_nothing_and_covers_no_river_area) {
    Collected c = fixture();
    Build b = assemble(c, 1);
    std::vector<uint32_t> rivers;
    for (uint32_t p = 0; p < b.rings.polygons.size(); p++)
        if (b.objects[b.object_of[p]].tags.river_area) rivers.push_back(p);
    REQUIRE(rivers.size() == 2u);
    NamingCounts counts;
    CHECK(credit_naming_lines(b.rings, rivers, b.object_of, NamingLines(), counts, 2).empty());
    CHECK_EQ(counts.cover_entries, size_t(0));
    CHECK(credit_naming_lines(b.rings, {}, b.object_of, b.lines, counts, 2).empty());
    CHECK_EQ(counts.segments_in_cover, size_t(0));
}

TEST(a_lake_of_many_islands_is_cut_into_pieces_that_each_carry_the_lakes_name_type_and_area) {
    Collected c = fixture();
    // A lake of 0.4 x 0.4 degrees with 12 x 12 islands, one way round.
    const int32_t lat = kLat - 10000000, side = 4000000;
    add_geometry(c, 70, square(lat, kLng, side, 8));
    CollectedArea isles;
    isles.id = 600;
    isles.tags = tags(code::kLake, "Island Lake");
    isles.members = {{70, "outer"}};
    for (int i = 0; i < 12; i++)
        for (int j = 0; j < 12; j++) {
            const int64_t way = 1000 + i * 12 + j;
            add_geometry(c, way, square(lat + 100000 + i * 320000, kLng + 100000 + j * 320000, 20000));
            isles.members.push_back({way, "inner"});
        }
    c.relations.push_back(isles);
    const Layer layer = build(c);
    std::vector<size_t> pieces;
    for (size_t p = 0; p < layer.size(); p++)
        if (name_of(layer, p) == "Island Lake") pieces.push_back(p);
    REQUIRE(pieces.size() > 2);
    std::set<uint64_t> ids, fallbacks;
    size_t holes = 0;
    for (size_t p : pieces) {
        CHECK_EQ(layer.records[p].country_code, code::kLake);
        CHECK_EQ(layer.records[p].area, layer.records[pieces[0]].area);
        CHECK(layer.arcs.polygons[p].ring_count <= kSplitMaxHoles + 1);
        holes += layer.arcs.polygons[p].ring_count - 1;
        ids.insert(layer.identities[p]);
        fallbacks.insert(layer.fallbacks[p]);
    }
    CHECK(holes > 100u && holes <= 144u);
    CHECK_EQ(ids.size(), pieces.size());
    CHECK_EQ(fallbacks.size(), pieces.size());
    // The lake's area: its square less its islands, not a piece's.
    CHECK(std::abs(layer.records[pieces[0]].area - float(0.16 - 144 * 0.000004)) < 1e-5f);
}

TEST(each_piece_of_a_lake_of_many_islands_keeps_its_slot_once_a_pieces_corner_moves) {
    // The lake of 12 x 12 islands, as above; the next day its south-west
    // corner, the smallest vertex of the piece there, moves in, and the
    // lake's box, so every cut, stays as it was.
    const int32_t lat = kLat - 10000000, side = 4000000;
    auto with_lake = [&](int32_t corner_in) {
        Collected c = fixture();
        Ring shore = square(lat, kLng, side, 8);
        shore.front() = shore.back() = pt(lat + corner_in, kLng + corner_in);
        add_geometry(c, 70, shore);
        CollectedArea isles;
        isles.id = 600;
        isles.tags = tags(code::kLake, "Island Lake");
        isles.members = {{70, "outer"}};
        for (int i = 0; i < 12; i++)
            for (int j = 0; j < 12; j++) {
                const int64_t way = 1000 + i * 12 + j;
                add_geometry(c, way, square(lat + 100000 + i * 320000, kLng + 100000 + j * 320000, 20000));
                isles.members.push_back({way, "inner"});
            }
        c.relations.push_back(isles);
        return c;
    };
    const ScratchDir root("water-piece-corner");
    const std::string one = root.path() + "/one", two = root.path() + "/two";
    const Layer day1 = build(with_lake(0));
    write(day1, one);
    const Layer day2 = build(with_lake(5000), one);
    REQUIRE(day2.size() == day1.size());
    REQUIRE(day1.size() > 6u);
    // The piece at the corner, and those cut from a piece whose corner it
    // was, are of other identities, and keep their slots by their fallback
    // keys; every polygon keeps its own.
    std::set<uint64_t> ids1(day1.identities.begin(), day1.identities.end());
    size_t moved = 0;
    for (uint64_t id : day2.identities) moved += !ids1.count(id);
    CHECK(moved >= 1u);
    const std::string n = std::to_string(day2.size());
    const std::string summary = write(day2, two, one);
    CHECK(summary.find("water/all: " + n + " polygons in " + n + " slots (" + n + " kept, " + std::to_string(moved) +
                       " by fallback, 0 recycled)") != std::string::npos);
}

TEST(a_lake_of_many_islands_the_grid_cant_cut_keeps_its_store_as_it_was) {
    // A lake of 0.0019 x 0.0019 degrees with 9 x 8 islands, between the
    // lines of the finest grid a cut takes (2^16 e7 units: 7171 and 7172 of
    // them in latitude, 1220 and 1221 in longitude).
    Collected c;
    add_geometry(c, 70, square(kLat, kLng, 19000));
    CollectedArea isles;
    isles.id = 600;
    isles.tags = tags(code::kLake, "Tiny Isles Lake");
    isles.members = {{70, "outer"}};
    for (int i = 0; i < 9; i++)
        for (int j = 0; j < 8; j++) {
            const int64_t way = 1000 + i * 8 + j;
            add_geometry(c, way, square(kLat + 1000 + i * 2000, kLng + 1000 + j * 2000, 500));
            isles.members.push_back({way, "inner"});
        }
    c.relations.push_back(isles);
    Build b = assemble(c, 1);
    REQUIRE(b.rings.polygons.size() == 1u);
    REQUIRE(b.rings.polygons[0].ring_count == 73u);
    const PackedLocation* vertices = b.rings.vertices.data();
    const std::vector<RingSpan> rings = b.rings.rings;
    const gctest::CapturedStderr log;
    split_polygons(b, 1);
    CHECK(log.str().find("Tiny Isles Lake, 72 holes) kept whole: no cut leaves it in more than one piece") !=
          std::string::npos);
    CHECK(log.str().find("0 cut along 0 lines into 0 pieces, 1 kept whole; 1 polygons") != std::string::npos);
    // Not copied: the store's vertices where they were.
    CHECK(b.rings.vertices.data() == vertices);
    CHECK_EQ(b.rings.rings.size(), rings.size());
    CHECK(b.piece_key == std::vector<uint64_t>({0}));
    CHECK(b.object_of == std::vector<uint32_t>({0}));
}

TEST(the_layer_is_the_same_on_one_thread_and_many) {
    const Layer one = build(fixture(), "", 1), many = build(fixture(), "", 8);
    const ScratchDir a("water-one"), b("water-many");
    write(one, a.path());
    write(many, b.path());
    CHECK(tree(a.path()) == tree(b.path()));
}

TEST(the_major_tier_takes_areas_of_100_km2_and_wikidata_rivers_of_5) {
    Collected c = fixture();
    // A lake of 0.15 x 0.15 degrees at 47 north (about 190 km2).
    c.closed_ways.push_back({60, tags(code::kLake, "Big Lake"), square(kLat - 2000000, kLng, 1500000)});
    // A wikidata river of 0.03 x 0.03 degrees (about 7.6 km2), and one
    // without, the same size.
    c.closed_ways.push_back({61, tags(code::kRiver, "Wide River", true, 5), square(kLat - 4000000, kLng, 300000)});
    c.closed_ways.push_back({62, tags(code::kRiver, "Plain River", true), square(kLat - 5000000, kLng, 300000)});
    const Layer layer = build(c);
    std::set<std::string> major;
    for (size_t p = 0; p < layer.size(); p++)
        if (layer.major[p]) major.insert(name_of(layer, p));
    CHECK(major == std::set<std::string>({"Big Lake", "Wide River"}));
    // The major dir holds them and no other.
    const ScratchDir dir("water-major");
    write(layer, dir.path());
    const std::map<std::string, std::vector<char>> files = tree(dir.path());
    const std::vector<std::string> by_slot = slot_names(files, "water/major/simplified");
    std::vector<std::string> slots = by_slot;
    std::sort(slots.begin(), slots.end());
    CHECK(slots == std::vector<std::string>({"Big Lake", "Wide River"}));
    // Its cell index lists those slots alone, and the big lake holds whole
    // cells: interior entries.
    Bytes cells{files.at("water/major/simplified/admin_cells.bin")},
        entries{files.at("water/major/simplified/admin_entries.bin")};
    cell_pairs::Reader reader(cells, entries, cell_pairs::ADMIN);
    size_t listed = 0, interior = 0;
    uint64_t cell;
    std::vector<uint32_t> ids;
    while (reader.next(cell, ids))
        for (uint32_t id : ids) {
            REQUIRE((id & ID_MASK) < by_slot.size());
            const std::string& name = by_slot[id & ID_MASK];
            CHECK(name == "Big Lake" || name == "Wide River");
            listed++;
            interior += (id & INTERIOR_FLAG) && name == "Big Lake";
        }
    CHECK(listed > 0);
    CHECK(interior > 0);
}

TEST(a_regions_share_holds_the_polygons_its_cells_list_whole_with_their_names) {
    const Layer planet = build(fixture());
    // The cells of the lake alone.
    std::vector<uint8_t> kept(planet.cells.size(), 0);
    for (size_t i = 0; i < planet.cells.size(); i++) kept[i] = (planet.cells[i].item_id & ID_MASK) == 3;
    const Layer region = subset_layer(planet, kept, 1, 2);
    REQUIRE(region.size() == 1u);
    CHECK(name_of(region, 0) == "Test Lake");
    CHECK(region.names == std::vector<char>({'T', 'e', 's', 't', ' ', 'L', 'a', 'k', 'e', '\0'}));
    CHECK_EQ(region.identities[0], planet.identities[3]);
    CHECK_EQ(region.arcs.polygons.size(), size_t(1));
    CHECK_EQ(region.arcs.set(ArcSet::Full).arcs(), size_t(3));
    // Its cells are the planet's it keeps, in their order, with their
    // interior marks, each of the lake's new id.
    std::vector<CellItemPair> cells;
    for (size_t i = 0; i < planet.cells.size(); i++)
        if (kept[i]) cells.push_back({planet.cells[i].cell_id, planet.cells[i].item_id & INTERIOR_FLAG});
    REQUIRE(!cells.empty());
    REQUIRE(region.cells.size() == cells.size());
    for (size_t i = 0; i < cells.size(); i++) {
        CHECK_EQ(region.cells[i].cell_id, cells[i].cell_id);
        CHECK_EQ(region.cells[i].item_id, cells[i].item_id);
    }
    CHECK(region.arcs.stage == &kWaterArcStage);
}

TEST(the_water_dirs_hold_every_file_and_keep_each_polygons_slot_the_next_day) {
    const ScratchDir root("water-dirs");
    const Layer day1 = build(fixture());
    const std::string one = root.path() + "/one";
    write(day1, one);
    const auto files = tree(one);
    const std::vector<std::string> client = {"admin_polygons.bin", "admin_rings.bin", "arc_index.bin", "arc_vertices.bin",
                                             "admin_cells.bin", "admin_entries.bin", "strings_core.bin",
                                             "strings_layout.json", "water_meta.json", "attribution.json"};
    for (const char* dir : {"water/all/simplified", "water/all/exact", "water/major/simplified"})
        for (const std::string& f : client) CHECK(files.count(std::string(dir) + "/" + f));
    for (const char* dir : {"water/all/exact", "water/major/simplified"})
        for (const char* f : {"admin_polygons.osm_ids", "admin_polygons.fallback.osm_ids", "arc_index.osm_ids",
                              "arc_index.fallback.osm_ids"})
            CHECK(files.count(std::string(dir) + "/" + f));
    CHECK(!files.count("water/all/simplified/admin_polygons.osm_ids"));
    // The two tiers of the all set: one set of records, rings, cells and
    // names; their own arcs.
    for (const char* f : {"admin_polygons.bin", "admin_rings.bin", "admin_cells.bin", "admin_entries.bin",
                          "strings_core.bin", "strings_layout.json", "water_meta.json", "attribution.json"})
        CHECK(files.at(std::string("water/all/simplified/") + f) == files.at(std::string("water/all/exact/") + f));
    // The simplified tier drops the vertices along the lake's sides that the
    // exact tier keeps.
    CHECK(files.at("water/all/simplified/arc_vertices.bin").size() < files.at("water/all/exact/arc_vertices.bin").size());
    CHECK(files.at("water/all/exact/admin_polygons.bin").size() == 4 * sizeof(AdminPolygonRecord));
    const std::vector<std::string> day1_slots = slot_names(files, "water/all/exact");
    CHECK(day1_slots == std::vector<std::string>({"Pond", "Test River", "Ganga", "Test Lake"}));
    CHECK(files.at("water/major/simplified/admin_polygons.bin").empty());
    const std::vector<char>& meta = files.at("water/all/exact/water_meta.json");
    const std::optional<std::vector<MetaType>> parsed = parse_meta_json(std::string(meta.begin(), meta.end()));
    REQUIRE(parsed.has_value() && parsed->size() == 4u);
    const std::vector<MetaType>& kinds = *parsed;
    CHECK(kinds[0].cls == "water" && kinds[0].type == "lake" && kinds[0].rank == 20);
    CHECK(kinds[3].cls == "water" && kinds[3].type == "ganga_river" && kinds[3].code >= kVerbatimBase);

    // The next day on top: the same files.
    const std::string two = root.path() + "/two";
    write(build(fixture(), one), two, one);
    CHECK(tree(two) == files);

    // The pond goes and the lake's smallest vertex moves: the pond's slot is
    // a tombstone, the lake keeps its slot by its fallback key, the others
    // keep theirs.
    Collected edited = fixture();
    edited.closed_ways.erase(edited.closed_ways.begin());
    WayGeometries& g = edited.member_geometries;
    g.coords[g.ways.at(11).offset] = pt(kLat - 10, kLng - 10);
    g.coords[g.ways.at(12).offset + g.ways.at(12).count - 1] = pt(kLat - 10, kLng - 10);
    const Layer day3 = build(edited, one);
    REQUIRE(day3.size() == 3u);
    CHECK(day3.identities[2] != day1.identities[3]);
    const std::string three = root.path() + "/three";
    const std::string summary = write(day3, three, one);
    CHECK(summary.find("3 polygons in 4 slots (3 kept, 1 by fallback, 0 recycled)") != std::string::npos);
    const auto after = tree(three);
    const std::vector<char>& records = after.at("water/all/exact/admin_polygons.bin");
    REQUIRE(records.size() == 4 * sizeof(AdminPolygonRecord));
    AdminPolygonRecord pond;
    std::memcpy(&pond, records.data(), sizeof pond);
    CHECK_EQ(pond.name_id, NO_DATA);
    for (const char* dir : {"water/all/simplified", "water/all/exact"})
        CHECK(slot_names(after, dir) == std::vector<std::string>({"", day1_slots[1], day1_slots[2], day1_slots[3]}));

    // A pond of another way the day after takes the freed slot: recycled,
    // not kept.
    Collected again = edited;
    again.closed_ways.push_back({21, tags(4, "Pond"), square(kLat + 200000, kLng, 1000)});
    const std::string four = root.path() + "/four";
    CHECK(write(build(again, three), four, three).find("4 polygons in 4 slots (3 kept, 0 by fallback, 1 recycled)") !=
          std::string::npos);
}

TEST(the_previous_slots_of_a_regions_water_dirs_count_their_tombstones) {
    const ScratchDir root("water-previous-slots");
    const std::string one = root.path() + "/one", two = root.path() + "/two";
    CHECK_EQ(previous_slots(""), 0u);
    CHECK_EQ(previous_slots(one), 0u);
    write(build(fixture()), one);
    CHECK_EQ(previous_slots(one), 4u);
    // The pond gone, its slot a tombstone the next day's dirs keep.
    Collected without_pond = fixture();
    without_pond.closed_ways.erase(without_pond.closed_ways.begin());
    const Layer day2 = build(without_pond, one);
    REQUIRE(day2.size() == 3u);
    write(day2, two, one);
    CHECK_EQ(previous_slots(two), 4u);
    // One that is no sidecar counts none, as the write refuses it.
    fs::remove(two + "/water/major/simplified/admin_polygons.osm_ids");
    CHECK_EQ(previous_slots(two), 4u);
    std::ofstream(two + "/water/all/exact/admin_polygons.osm_ids", std::ios::binary | std::ios::trunc) << "no";
    CHECK_EQ(previous_slots(two), 0u);
}

TEST(the_previous_arc_slots_of_a_regions_water_dirs_stay_past_the_arcs_a_day_of_less_water_has) {
    const ScratchDir root("water-previous-arc-slots");
    const std::string one = root.path() + "/one", two = root.path() + "/two";
    CHECK(previous_arc_slots("").all == 0u && previous_arc_slots("").major == 0u);
    CHECK(previous_arc_slots(one).all == 0u && previous_arc_slots(one).major == 0u);
    // The fixture and a lake of about 190 km2, the major tier's.
    Collected c = fixture();
    c.closed_ways.push_back({60, tags(code::kLake, "Big Lake"), square(kLat - 2000000, kLng, 1500000, 6)});
    const Layer day1 = build(c);
    const ArcCounts arcs1 = arc_counts(day1);
    CHECK_EQ(arcs1.all, uint64_t(day1.arcs.set(ArcSet::Full).arcs()));
    CHECK(arcs1.major > 0u && arcs1.major < arcs1.all);
    write(day1, one);
    const ArcCounts slots1 = previous_arc_slots(one);
    CHECK_EQ(slots1.all, arcs1.all);
    CHECK_EQ(slots1.major, arcs1.major);
    // The next day holds the river areas alone: few arcs, none of the major
    // tier's, and every slot of the day before kept.
    Collected less = fixture();
    less.closed_ways.erase(less.closed_ways.begin());
    less.closed_ways.pop_back();
    less.relations.clear();
    const Layer day2 = build(less, one);
    const ArcCounts arcs2 = arc_counts(day2);
    CHECK(arcs2.all > 0u && arcs2.all < slots1.all);
    CHECK_EQ(arcs2.major, uint64_t(0));
    write(day2, two, one);
    const ArcCounts slots2 = previous_arc_slots(two);
    CHECK_EQ(slots2.all, slots1.all);
    CHECK_EQ(slots2.major, slots1.major);
    // One that is no sidecar counts none.
    std::ofstream(two + "/water/major/simplified/arc_index.osm_ids", std::ios::binary | std::ios::trunc) << "no";
    CHECK_EQ(previous_arc_slots(two).major, uint64_t(0));
    CHECK_EQ(previous_arc_slots(two).all, slots1.all);
}

namespace {

// A marine area as marine::read_areas gives it: each ring closed.
marine::Area marine_area(uint64_t id, uint8_t kind, const std::string& name,
                         std::vector<std::vector<Ring>> polygons) {
    marine::Area a;
    a.id = id;
    a.kind = kind;
    a.name = name;
    a.polygons = std::move(polygons);
    return a;
}

// The fixture and two marine areas south of it: an ocean of two squares, one
// with an island, and a sea of a square beside it, sharing its east side.
Collected with_marine() {
    Collected c = fixture();
    const int32_t lat = kLat - 20000000;
    c.marine.push_back(marine_area(1902, marine::kOcean, "North Atlantic Ocean",
                                   {{square(lat, kLng, 2000000, 40), square(lat + 500000, kLng + 500000, 100000, 3)},
                                    {square(lat + 5000000, kLng, 1000000, 5)}}));
    c.marine.push_back(marine_area(2350, marine::kSea, "North Sea", {{square(lat, kLng + 2000000, 2000000, 40)}}));
    return c;
}

std::string text_of(const std::vector<char>& bytes) { return std::string(bytes.begin(), bytes.end()); }

}  // namespace

TEST(marine_areas_are_objects_of_their_own_as_seas_and_oceans) {
    const Layer layer = build(with_marine());
    // After the OSM objects, by id: the ocean's two polygons, then the sea.
    REQUIRE(layer.size() == 7u);
    CHECK(name_of(layer, 4) == "North Atlantic Ocean");
    CHECK(name_of(layer, 5) == "North Atlantic Ocean");
    CHECK(name_of(layer, 6) == "North Sea");
    CHECK_EQ(layer.records[4].country_code, code::kPlaceOcean);
    CHECK_EQ(layer.records[6].country_code, code::kPlaceSea);
    for (size_t p = 0; p < layer.size(); p++) CHECK_EQ(int(layer.marine[p]), int(p >= 4));
    // The object's area: both its squares less the island.
    CHECK(std::abs(layer.records[4].area - float(0.04 - 0.0001 + 0.01)) < 1e-6f);
    CHECK_EQ(layer.records[5].area, layer.records[4].area);
    CHECK_EQ(layer.arcs.polygons[4].ring_count, 2u);
    // Keyed in an id space of their own: the area and the outer ring's
    // corner, with a fallback by the area and the polygon's place.
    const Ring first = square(kLat - 20000000, kLng, 2000000, 40);
    const uint64_t corner = ring_corner(first.data(), static_cast<uint32_t>(first.size()), 0);
    CHECK_EQ(layer.identities[4], marine_identity(1902, corner));
    CHECK_EQ(layer.fallbacks[4], marine_fallback(1902, 0));
    CHECK_EQ(layer.fallbacks[5], marine_fallback(1902, 1));
    CHECK(layer.identities[4] != relation_ring_identity(1902, corner));
    std::set<uint64_t> ids(layer.identities.begin(), layer.identities.end());
    CHECK_EQ(ids.size(), layer.size());
    // Every one is in the cell index and in the major tier.
    std::set<uint32_t> listed;
    for (const CellItemPair& cell : layer.cells) listed.insert(cell.item_id & ID_MASK);
    for (uint32_t p = 4; p < 7; p++) {
        CHECK(listed.count(p));
        CHECK(layer.major[p]);
    }
    // The ocean's square and the sea's share their side: an arc of both.
    auto slots_of = [&](size_t p) {
        const AdminArcPolygon& poly = layer.arcs.polygons[p];
        std::vector<std::vector<arc_blocks::SlotRef>> rings;
        REQUIRE(arc_blocks::decode_rings(layer.arcs.set(ArcSet::Full).ring_blocks.data() + poly.blocks[0].offset,
                                         poly.blocks[0].size, rings));
        std::set<uint32_t> slots;
        for (const auto& ring : rings)
            for (const arc_blocks::SlotRef& r : ring) slots.insert(r.slot);
        return slots;
    };
    const std::set<uint32_t> ocean = slots_of(4), sea = slots_of(6);
    size_t shared = 0;
    for (uint32_t slot : sea) shared += ocean.count(slot);
    CHECK(shared >= 1);
}

TEST(a_marine_area_under_100_km2_is_in_the_major_tier_as_every_sea_and_ocean_is) {
    // A sea of 0.002 x 0.002 degrees, as a marine file cut to a box may hold.
    Collected c = fixture();
    c.marine.push_back(marine_area(2351, marine::kSea, "Small Sea", {{square(kLat - 20000000, kLng, 20000, 4)}}));
    const Layer layer = build(c);
    size_t marine = 0;
    for (size_t p = 0; p < layer.size(); p++)
        if (layer.marine[p]) {
            marine++;
            CHECK(layer.major[p]);
        }
    CHECK_EQ(marine, size_t(1));
}

TEST(a_water_dir_holding_a_marine_area_credits_marine_regions) {
    const ScratchDir root("water-marine");
    write(build(with_marine()), root.path() + "/with");
    write(build(fixture()), root.path() + "/without");
    for (const char* dir : {"water/all/simplified", "water/all/exact", "water/major/simplified"}) {
        const std::string with = text_of(bytes_of(root.path() + "/with/" + dir + "/attribution.json"));
        const std::string without = text_of(bytes_of(root.path() + "/without/" + dir + "/attribution.json"));
        CHECK(with.find("\"id\": \"osm\"") != std::string::npos);
        CHECK(with.find("\"id\": \"marine-regions\"") != std::string::npos);
        CHECK(with.find("https://doi.org/10.14284/699") != std::string::npos);
        CHECK(with.find("\"licence\": \"CC BY 4.0\"") != std::string::npos);
        CHECK(without.find("\"id\": \"osm\"") != std::string::npos);
        CHECK(without.find("marine-regions") == std::string::npos);
        CHECK(with.rfind("[\n  {\n    \"id\": \"osm\",\n", 0) == 0);
        CHECK(with.size() > 6 && with.compare(with.size() - 6, 6, "  }\n]\n") == 0);
    }
    // A region whose share holds no marine area credits OSM alone.
    const Layer planet = build(with_marine());
    std::vector<uint8_t> kept(planet.cells.size(), 0);
    for (size_t i = 0; i < planet.cells.size(); i++) kept[i] = (planet.cells[i].item_id & ID_MASK) == 3;
    Layer lake = subset_layer(planet, kept, 1, 2);
    REQUIRE(lake.size() == 1u);
    CHECK_EQ(int(lake.marine[0]), 0);
    {
        const gctest::CapturedStderr log;
        assign_region_arc_slots(lake.arcs, "", "test", 0);
    }
    write(lake, root.path() + "/lake");
    const std::string credit = text_of(bytes_of(root.path() + "/lake/water/all/exact/attribution.json"));
    CHECK(credit.find("\"id\": \"osm\"") != std::string::npos);
    CHECK(credit.find("marine-regions") == std::string::npos);
}

TEST(a_regions_share_counts_the_cells_around_its_own_and_each_polygon_they_list_whole) {
    const Layer planet = build(with_marine());
    // Region 0 holds one cell of the ocean's big square alone, region 1
    // every cell, region 2 none.
    uint64_t one = 0;
    for (const CellItemPair& c : planet.cells)
        if ((c.item_id & ID_MASK) == 4) one = c.cell_id;
    REQUIRE(one != 0);
    const std::vector<uint8_t> masks = routed_cell_masks(planet.cells, 3, [&](size_t r, uint64_t cell) {
        return r == 1 || (r == 0 && cell == one);
    }, 2);
    // Region 0 keeps that cell and the cells around it, as its split routes
    // them.
    std::vector<S2CellId> around;
    S2CellId(one).AppendAllNeighbors(S2CellId(one).level(), &around);
    std::set<uint64_t> routed = {one};
    for (S2CellId c : around) routed.insert(c.id());
    size_t near = 0;
    for (size_t i = 0; i < planet.cells.size(); i++) {
        CHECK_EQ(int(masks[i] >> 0 & 1), int(routed.count(planet.cells[i].cell_id)));
        CHECK_EQ(int(masks[i] >> 1 & 1), 1);
        CHECK_EQ(int(masks[i] >> 2 & 1), 0);
        near += routed.count(planet.cells[i].cell_id);
    }
    REQUIRE(near > 0 && near < planet.cells.size());
    // Each share's bytes: at least what its subset holds once its arc slots
    // are made, the polygons its cells list whole, and none for a region of
    // no cells.
    const std::vector<uint64_t> shares = share_bytes(planet, masks, 3);
    // And its arcs, those of its share's polygons and its major tier's.
    std::vector<ArcCounts> arcs;
    CHECK(share_bytes(planet, masks, 3, &arcs) == shares);
    REQUIRE(arcs.size() == 3u);
    CHECK(arcs[1].all == arc_counts(planet).all && arcs[1].major == arc_counts(planet).major);
    CHECK(arcs[1].major > 0u && arcs[1].major < arcs[1].all);
    CHECK(arcs[0].all > 0u && arcs[0].all < arcs[1].all);
    CHECK(arcs[2].all == 0u && arcs[2].major == 0u);
    for (int r = 0; r < 2; r++) {
        const Layer share = subset_layer(planet, masks, uint8_t(1u << r), 2);
        CHECK_EQ(arcs[r].all, arc_counts(share).all);
        CHECK_EQ(arcs[r].major, arc_counts(share).major);
    }
    for (int r = 0; r < 2; r++) {
        // Its bit of the masks, as a continent's split reads them: the
        // share of the entries that bit marks.
        std::vector<uint8_t> kept(planet.cells.size());
        for (size_t i = 0; i < kept.size(); i++) kept[i] = masks[i] >> r & 1;
        Layer region = subset_layer(planet, masks, uint8_t(1u << r), 2);
        const Layer marked = subset_layer(planet, kept, 1, 2);
        CHECK(region.identities == marked.identities);
        CHECK(region.cells.size() == marked.cells.size());
        CHECK(region.names == marked.names);
        {
            const gctest::CapturedStderr log;
            assign_region_arc_slots(region.arcs, "", "test", 0);
        }
        CHECK(shares[r] >= region.bytes());
        CHECK(shares[r] <= 2 * region.bytes());
    }
    CHECK_EQ(shares[2], uint64_t(0));
}

TEST(marine_areas_keep_their_slots_from_one_day_to_the_next) {
    const ScratchDir root("water-marine-days");
    const std::string one = root.path() + "/one", two = root.path() + "/two";
    const Layer day1 = build(with_marine());
    write(day1, one);
    // The next day, the ocean's first square's smallest vertex moved: the
    // polygon keeps its slot by its fallback key.
    Collected c = with_marine();
    c.marine[0].polygons[0][0].front().lat_e7 -= 10;
    c.marine[0].polygons[0][0].back() = c.marine[0].polygons[0][0].front();
    const Layer day2 = build(c, one);
    CHECK(day2.identities[4] != day1.identities[4]);
    const std::string summary = write(day2, two, one);
    CHECK(summary.find("7 polygons in 7 slots (7 kept, 1 by fallback, 0 recycled)") != std::string::npos);
    const auto a = tree(one), b = tree(two);
    CHECK(a.at("water/all/exact/admin_polygons.bin") == b.at("water/all/exact/admin_polygons.bin"));
}

TEST(polygons_of_one_object_meeting_at_their_smallest_vertex_keep_slots_of_their_own) {
    // Two triangles of one marine area that touch at their south-west corner,
    // the smallest vertex of both, and two outer rings of a lake relation
    // that do the same.
    Collected c = fixture();
    const int32_t lat = kLat - 30000000;
    auto triangles = [](int32_t at_lat, int32_t at_lng) {
        const PackedLocation p = pt(at_lat, at_lng);
        return std::pair<Ring, Ring>{{p, pt(at_lat, at_lng + 1000), pt(at_lat + 500, at_lng + 1000), p},
                                     {p, pt(at_lat + 1000, at_lng + 500), pt(at_lat + 1000, at_lng), p}};
    };
    const auto [a, b] = triangles(lat, kLng);
    c.marine.push_back(marine_area(3000, marine::kSea, "Pinched Sea", {{a}, {b}}));
    // And an area of one polygon twice, alike at every vertex: told apart by
    // their fallback keys.
    const Ring twice = triangles(lat + 200000, kLng).first;
    c.marine.push_back(marine_area(3001, marine::kSea, "Twice Sea", {{twice}, {twice}}));
    const auto [x, y] = triangles(lat + 100000, kLng);
    add_geometry(c, 80, x);
    add_geometry(c, 81, y);
    CollectedArea pinched;
    pinched.id = 700;
    pinched.tags = tags(code::kLake, "Pinched Lake");
    pinched.members = {{80, "outer"}, {81, "outer"}};
    c.relations.push_back(pinched);
    const Layer day1 = build(c);
    // Each pair is two polygons of its own.
    std::map<std::string, size_t> polygons;
    for (size_t p = 0; p < day1.size(); p++) polygons[name_of(day1, p)]++;
    for (const char* name : {"Pinched Sea", "Twice Sea", "Pinched Lake"}) CHECK_EQ(polygons[name], size_t(2));
    std::set<uint64_t> ids(day1.identities.begin(), day1.identities.end());
    CHECK_EQ(ids.size(), day1.size());
    // The next day, the same data: every polygon in its slot, no slot more.
    const ScratchDir root("water-pinched");
    const std::string one = root.path() + "/one", two = root.path() + "/two";
    write(day1, one);
    const std::string summary = write(build(c, one), two, one);
    const std::string all = std::to_string(day1.size());
    CHECK(summary.find(all + " polygons in " + all + " slots (" + all + " kept, 0 by fallback, 0 recycled)") !=
          std::string::npos);
    CHECK(tree(one) == tree(two));
}

namespace {

// An object's tags as the layer reads them (object_tags), from key-value
// pairs.
ObjectTags tags_from(const std::map<std::string, std::string>& kv) {
    return object_tags([&](const char* key) -> const char* {
        auto it = kv.find(key);
        return it == kv.end() ? nullptr : it->second.c_str();
    });
}

// The ways of `ids` whose polygons the layer holds, and those of them in its
// major tier.
std::pair<std::set<int64_t>, std::set<int64_t>> ways_held(const Layer& layer, const std::vector<int64_t>& ids) {
    std::set<int64_t> held, major;
    for (size_t p = 0; p < layer.size(); p++)
        for (int64_t id : ids)
            if (layer.identities[p] == way_identity(id)) {
                held.insert(id);
                if (layer.major[p]) major.insert(id);
            }
    return {held, major};
}

}  // namespace

TEST(an_osm_sea_is_no_water_area_as_the_marine_areas_hold_the_seas) {
    CHECK(!in_scope(tags_from({{"place", "sea"}, {"name", "Ionian Sea"}})));
    // Another kind's tags decide as before: the Caspian is a lake, a bay a
    // bay.
    CHECK_EQ(tags_from({{"natural", "water"}, {"water", "lake"}, {"place", "sea"}, {"name", "Caspian Sea"}}).code,
             code::kLake);
    CHECK_EQ(tags_from({{"natural", "bay"}, {"place", "sea"}, {"name", "Bay of Biscay"}}).code, code::kNaturalBay);
}

TEST(a_wikidata_tag_names_an_item_only_as_one_whole_q_number) {
    auto qid = [](const char* wikidata) {
        return tags_from({{"natural", "water"}, {"name", "See"}, {"wikidata", wikidata}}).qid;
    };
    CHECK_EQ(qid("Q123"), 123u);
    CHECK_EQ(qid("Q4294967295"), 4294967295u);
    // Two items, a number past 32 bits (2^32 + 123), and what follows or
    // comes before the number: none.
    for (const char* other : {"Q123;Q456", "Q4294967419", "Q12x", "Q 12", "Q+12", "Q-12", " Q12", "Q", "q12", "12"})
        CHECK_EQ(qid(other), 0u);
}

TEST(an_osm_bay_or_strait_of_a_marine_areas_name_that_it_holds_all_of_is_left_out) {
    Collected c = with_marine();
    // The North Sea is the square at lat .. lat + 0.2, kLng + 0.2 .. 0.4.
    const int32_t lat = kLat - 20000000, lng = kLng + 2000000;
    auto add_way = [&](int64_t way, uint16_t code, const std::string& name, const std::string& local, const Ring& ring) {
        ObjectTags t = tags(code, name);
        t.local_name = local;
        c.closed_ways.push_back({way, t, ring});
    };
    // Its namesakes in it: by name:en, and by a local name of other case and
    // spacing.
    add_way(2001, code::kNaturalBay, "North Sea", "Nordsee", square(lat + 500000, lng + 500000, 300000));
    add_way(2002, code::kNaturalStrait, "Strait of Dover", "NORTH  SEA", square(lat + 1000000, lng + 500000, 300000));
    // Kept: a namesake far from it, a lake of its name in it, and a bay of
    // another name in it.
    add_way(2003, code::kNaturalBay, "North Sea", "North Sea", square(kLat + 800000, kLng, 1000));
    add_way(2004, code::kLake, "North Sea", "North Sea", square(lat + 1500000, lng + 500000, 100000));
    add_way(2005, code::kNaturalBay, "German Bight", "Deutsche Bucht", square(lat + 200000, lng + 1500000, 200000));
    // Kept as well, holding water the sea doesn't: a bay relation round the
    // whole of it, one of its name reaching out of it, and one of two
    // polygons, one in the sea and one far from it.
    add_geometry(c, 2500, square(lat - 100000, lng - 100000, 2200000));
    CollectedArea around;
    around.id = 2600;
    around.tags = tags(code::kNaturalBay, "North Sea");
    around.members = {{2500, "outer"}};
    c.relations.push_back(around);
    add_way(2006, code::kNaturalBay, "North Sea", "North Sea", square(lat + 1800000, lng + 1800000, 300000));
    add_geometry(c, 2501, square(lat + 500000, lng + 1000000, 100000));
    add_geometry(c, 2502, square(lat + 500000, lng + 5000000, 100000));
    CollectedArea apart;
    apart.id = 2601;
    apart.tags = tags(code::kNaturalBay, "North Sea");
    apart.members = {{2501, "outer"}, {2502, "outer"}};
    c.relations.push_back(apart);
    const Layer layer = build(c);
    CHECK(ways_held(layer, {2001, 2002, 2003, 2004, 2005, 2006}).first == std::set<int64_t>({2003, 2004, 2005, 2006}));
    std::map<uint64_t, size_t> rings;
    for (size_t p = 0; p < layer.size(); p++) rings[layer.fallbacks[p]]++;
    CHECK_EQ(rings[relation_ring_fallback(2600, 0)], size_t(1));
    CHECK_EQ(rings[relation_ring_fallback(2601, 0)] + rings[relation_ring_fallback(2601, 1)], size_t(2));
    // The marine areas stay whole.
    size_t marine = 0;
    for (size_t p = 0; p < layer.size(); p++) marine += layer.marine[p];
    CHECK_EQ(marine, size_t(3));
}

TEST(namesakes_apart_are_rivers_of_their_own_and_a_line_lends_its_wikidata_item_only_where_it_runs) {
    Collected c = fixture();
    // Four unnamed river areas far apart, each with a line of a local name
    // alone, Mill Creek, in it: two of about 61 km2 (123 together), one of
    // about 6.7 km2 whose line has a wikidata item, and one of that size
    // whose line has none.
    struct Reach {
        int64_t way;
        int32_t lat, side;
        uint32_t qid;
    };
    const std::vector<Reach> reaches = {{2101, kLat - 3000000, 850000, 0},
                                        {2102, kLat - 4000000, 850000, 0},
                                        {2103, kLat - 5000000, 280000, 5},
                                        {2104, kLat - 6000000, 280000, 0}};
    for (const Reach& r : reaches) {
        c.closed_ways.push_back({r.way, tags(code::kRiver, "", true), square(r.lat, kLng, r.side)});
        LineIn line;
        line.way = r.way + 1000;
        line.key = "nMill Creek";
        line.local = line.display = "Mill Creek";
        line.qid = r.qid;
        line.vertices = {pt(r.lat + r.side / 2, kLng + r.side / 10), pt(r.lat + r.side / 2, kLng + r.side * 9 / 10)};
        c.lines.push_back(line);
    }
    const Layer layer = build(c);
    const auto [held, major] = ways_held(layer, {2101, 2102, 2103, 2104});
    CHECK(held == std::set<int64_t>({2101, 2102, 2103, 2104}));
    CHECK(major == std::set<int64_t>({2103}));
}

TEST(namesakes_that_share_a_shore_are_one_river) {
    Collected c = fixture();
    // Two unnamed river areas of about 61 km2, one north of the other along
    // a shared side, each with a line of a local name alone, Mill Creek, in
    // it and no wikidata item: one river of about 123 km2.
    const int32_t lat = kLat - 3000000, side = 850000;
    for (const auto& [way, south] : std::vector<std::pair<int64_t, int32_t>>{{2111, lat}, {2112, lat + side}}) {
        c.closed_ways.push_back({way, tags(code::kRiver, "", true), square(south, kLng, side)});
        LineIn line;
        line.way = way + 1000;
        line.key = "nMill Creek";
        line.local = line.display = "Mill Creek";
        line.vertices = {pt(south + side / 2, kLng + side / 10), pt(south + side / 2, kLng + side * 9 / 10)};
        c.lines.push_back(line);
    }
    const auto [held, major] = ways_held(build(c), {2111, 2112});
    CHECK(held == std::set<int64_t>({2111, 2112}));
    CHECK(major == std::set<int64_t>({2111, 2112}));
}

TEST(a_named_river_area_takes_only_its_own_rivers_identity_from_the_line_of_most_length_in_it) {
    // Two river areas of about 3 km2 far apart, each with a line of the
    // Beta's relation (Q202) in it: one the Beta names, and the Alpha (Q101)
    // of its own name. Joined, the two would be a wikidata river of 6 km2
    // in the major tier; the Alpha is a river of its own, under 5 km2.
    Collected c = fixture();
    const int32_t side = 190000;
    auto add_line = [&](int64_t way, int32_t lat) {
        LineIn line;
        line.way = way;
        line.key = "r950";
        line.local = line.display = "Beta";
        line.qid = 202;
        line.vertices = {pt(lat + side / 2, kLng + side / 10), pt(lat + side / 2, kLng + side * 9 / 10)};
        c.lines.push_back(line);
    };
    c.closed_ways.push_back({2301, tags(code::kRiver, "Alpha", true, 101), square(kLat - 3000000, kLng, side)});
    add_line(2311, kLat - 3000000);
    c.closed_ways.push_back({2302, tags(code::kRiver, "", true), square(kLat - 4000000, kLng, side)});
    add_line(2312, kLat - 4000000);
    const auto [held, major] = ways_held(build(c), {2301, 2302});
    CHECK(held == std::set<int64_t>({2301, 2302}));
    CHECK(major.empty());
    // An area of the Beta's own name, and no item, is the Beta's: the two
    // Beta areas make 6 km2 together.
    c.closed_ways.push_back({2303, tags(code::kRiver, "Beta", true), square(kLat - 5000000, kLng, side)});
    add_line(2313, kLat - 5000000);
    CHECK(ways_held(build(c), {2301, 2302, 2303}).second == std::set<int64_t>({2302, 2303}));
}

TEST(an_unnamed_river_area_of_a_wikidata_item_of_its_own_takes_a_lines_name_but_not_another_rivers_identity) {
    // The Alpha (Q101), an area of its name of about 3 km2; an unnamed area
    // of about 1 km2 tagged Q101, with a line of the Beta's relation (Q202)
    // in it; and an unnamed area of about 3 km2 the Beta names. Joined
    // through the unnamed Alpha area, the three would be a wikidata river of
    // 7 km2 in the major tier; the Alpha is a river of 4, the Beta of 3.
    Collected c = fixture();
    const int32_t side = 190000, small = 110000;
    auto add_line = [&](int64_t way, int32_t lat, int32_t width) {
        LineIn line;
        line.way = way;
        line.key = "r950";
        line.local = line.display = "Beta";
        line.qid = 202;
        line.vertices = {pt(lat + width / 2, kLng + width / 10), pt(lat + width / 2, kLng + width * 9 / 10)};
        c.lines.push_back(line);
    };
    c.closed_ways.push_back({2401, tags(code::kRiver, "Alpha", true, 101), square(kLat - 3000000, kLng, side)});
    c.closed_ways.push_back({2402, tags(code::kRiver, "", true, 101), square(kLat - 4000000, kLng, small)});
    add_line(2412, kLat - 4000000, small);
    c.closed_ways.push_back({2403, tags(code::kRiver, "", true), square(kLat - 5000000, kLng, side)});
    add_line(2413, kLat - 5000000, side);
    const Layer layer = build(c);
    const auto [held, major] = ways_held(layer, {2401, 2402, 2403});
    CHECK(held == std::set<int64_t>({2401, 2402, 2403}));
    CHECK(major.empty());
    // It shows the line's name.
    for (size_t p = 0; p < layer.size(); p++)
        if (layer.identities[p] == way_identity(2402)) CHECK(name_of(layer, p) == "Beta");
}

TEST(a_lake_of_100_km2_far_from_its_boxs_middle_latitude_is_in_the_major_tier) {
    // A lobe of 0.13 x 0.13 degrees at 60 north (about 104.6 km2), and an arm
    // 0.00001 wide from it to 70 north (about 0.5 km2): weighted by the
    // cosine of its box's middle latitude, 65, the lake would be about 89.
    Collected c = fixture();
    const int32_t lat = 600000000, side = 1300000, arm = 100;
    const Ring lake = {pt(lat, kLng), pt(lat, kLng + side), pt(lat + side, kLng + side), pt(700000000, kLng + side),
                       pt(700000000, kLng + side - arm), pt(lat + side, kLng + side - arm), pt(lat + side, kLng),
                       pt(lat, kLng)};
    c.closed_ways.push_back({63, tags(code::kLake, "Long Arm Lake"), lake});
    const Layer layer = build(c);
    const auto [held, major] = ways_held(layer, {63});
    CHECK(held == std::set<int64_t>({63}));
    CHECK(major == std::set<int64_t>({63}));
}

TEST(a_lake_the_major_tier_held_stays_in_it_at_95_km2_once_its_smallest_vertex_moves) {
    // A lake relation of one way round a square of about 105 km2; the next
    // day its south-west corner, the smallest vertex, moves in, and it holds
    // about 95: under 100 and over the margin's 90.
    const int32_t lat = kLat - 6000000, side = 1110000;
    auto with_lake = [&](int32_t corner_in) {
        Collected c = fixture();
        Ring ring = square(lat, kLng, side);
        ring.front() = ring.back() = pt(lat + corner_in, kLng + corner_in);
        add_geometry(c, 2200, ring);
        CollectedArea lake;
        lake.id = 2300;
        lake.tags = tags(code::kLake, "Margin Lake");
        lake.members = {{2200, "outer"}};
        c.relations.push_back(lake);
        return c;
    };
    auto lake_major = [](const Layer& layer) {
        for (size_t p = 0; p < layer.size(); p++)
            if (layer.fallbacks[p] == relation_ring_fallback(2300, 0)) return layer.major[p] != 0;
        return false;
    };
    const ScratchDir root("water-margin");
    const std::string one = root.path() + "/one";
    const Layer day1 = build(with_lake(0));
    REQUIRE(lake_major(day1));
    write(day1, one);
    const Layer day2 = build(with_lake(105700), one);
    CHECK(lake_major(day2));
    CHECK(!lake_major(build(with_lake(105700))));
}

TEST(an_object_two_inputs_hold_is_one_object_as_the_first_input_gives_it) {
    // The fixture as two inputs give it: the second input's copies of the
    // lake relation, the pond and the main line come after the first
    // input's, with other names.
    Collected c = fixture();
    CollectedArea lake = c.relations[0];
    lake.tags.name = lake.tags.local_name = "Other Lake";
    c.relations.push_back(lake);
    ClosedWayArea pond = c.closed_ways[0];
    pond.tags.name = pond.tags.local_name = "Other Pond";
    c.closed_ways.push_back(pond);
    LineIn main = c.lines[0];
    main.display = "Other River";
    c.lines.push_back(main);
    const Layer twice = build(c), once = build(fixture());
    REQUIRE(twice.size() == once.size());
    for (size_t p = 0; p < once.size(); p++) {
        CHECK(name_of(twice, p) == name_of(once, p));
        CHECK_EQ(twice.identities[p], once.identities[p]);
        CHECK_EQ(twice.records[p].area, once.records[p].area);
    }
}

namespace {

// A member river way of a waterway relation: its name and name:en (nullptr
// for none) and its length, e7 units along a parallel.
struct Reach {
    int64_t way;
    const char* name;
    const char* name_en;
    int32_t length;
};

// What the passes collect of waterway relation 910, of the name `name` and
// the name:en `name_en` (nullptr for none), and its member river ways
// `reaches`, keyed by their tags (line_key), end to end along one parallel.
Collected river(const char* name, const char* name_en, const std::vector<Reach>& reaches) {
    Collected c;
    CollectedWaterway w;
    w.id = 910;
    w.local_name = name;
    w.name = name_en ? name_en : name;
    w.english = name_en != nullptr;
    for (const Reach& r : reaches) w.ways.push_back(r.way);
    c.waterways.push_back(w);
    index_collected(c);
    int32_t lng = kLng;
    for (const Reach& r : reaches) {
        LineIn line;
        REQUIRE(line_key(c, r.way, r.name, r.name_en, 0, line));
        line.vertices = {pt(kLat, lng), pt(kLat, lng + r.length)};
        lng += r.length;
        c.lines.push_back(std::move(line));
    }
    return c;
}

// The names the assembled lines of relation 910 show.
std::set<std::string> shown(Collected c) {
    const Build b = assemble(c, 1);
    std::set<std::string> out;
    for (const NamingLine& l : b.lines.lines)
        if (b.lines.keys[l.key] == "r910") out.insert(b.lines.displays[l.display]);
    return out;
}

using Names = std::set<std::string>;

}  // namespace

TEST(a_member_of_a_waterway_relation_of_a_local_name_alone_keeps_its_own_name_en) {
    Collected c = fixture();
    CollectedWaterway local, english;
    local.id = 901;
    local.name = local.local_name = "小渋川";
    local.ways = {51, 52, 53};
    english.id = 902;
    english.name = "Danube";
    english.local_name = "Donau";
    english.english = true;
    english.ways = {61};
    c.waterways = {local, english};
    index_collected(c);
    // A member of a relation with a name:en of its own, a member of none,
    // and a way of no name:en keep none.
    auto keyed = [&](int64_t way, const char* name, const char* name_en) {
        LineIn line;
        CHECK(line_key(c, way, name, name_en, 0, line));
        return line;
    };
    const LineIn en = keyed(51, "小渋川", "Koshibu River"), local_only = keyed(52, "小渋川", nullptr),
                 none = keyed(53, nullptr, nullptr), danube = keyed(61, "Donau", "Danube River"),
                 brook = keyed(70, "Bach", "Brook");
    CHECK(en.key == "r901" && en.display == "小渋川" && en.way_en == "Koshibu River");
    CHECK(local_only.key == "r901" && local_only.display == "小渋川" && local_only.way_en.empty());
    CHECK(none.key == "r901" && none.display == "小渋川" && none.way_en.empty());
    CHECK(danube.key == "r902" && danube.display == "Danube" && danube.way_en.empty());
    CHECK(brook.key == "nBach" && brook.display == "Brook" && brook.way_en.empty());
}

TEST(a_member_of_a_waterway_relation_without_a_wikidata_item_keeps_its_own) {
    Collected c;
    CollectedWaterway with, without;
    with.id = 4200;
    with.name = with.local_name = "Aare";
    with.english = true;
    with.qid = 72;
    with.ways = {4201};
    without.id = 4300;
    without.name = without.local_name = "Reuss";
    without.english = true;
    without.ways = {4301};
    c.waterways = {with, without};
    index_collected(c);
    LineIn aare, reuss;
    REQUIRE(line_key(c, 4201, "Aare", nullptr, 9, aare));
    REQUIRE(line_key(c, 4301, "Reuss", nullptr, 9, reuss));
    CHECK(aare.key == "r4200" && reuss.key == "r4300");
    CHECK_EQ(aare.qid, 72u);
    CHECK_EQ(reuss.qid, 9u);
}

TEST(a_waterway_relation_of_a_local_name_alone_shows_the_name_en_its_river_ways_carry_over_most_of_its_length) {
    // The Koshibu River: its relation is 小渋川 alone, and its ways of a
    // name:en hold 9,000 of its 10,000 units of length, more than half. Of
    // them the two of Koshibu River hold the most, 5,000 to the 4,000 of
    // Koshibu-gawa's one, though that is the longest way; the last way, of
    // 1,000, has none.
    CHECK(shown(river("小渋川", nullptr,
                      {{51, "小渋川", "Koshibu River", 2000},
                       {52, "小渋川", "Koshibu River", 3000},
                       {53, "小渋川", "Koshibu-gawa", 4000},
                       {54, "小渋川", nullptr, 1000}})) == Names({"Koshibu River"}));
    // A relation's own name:en is its name, whatever its ways carry.
    CHECK(shown(river("Donau", "Danube", {{61, "Donau", "Danube River", 3000}, {62, "Donau", nullptr, 2000}})) ==
          Names({"Danube"}));
    // Of names:en over equal length, the smaller.
    CHECK(shown(river("Χείμαρρος", nullptr,
                      {{80, "Χείμαρρος", "Zeta River", 2000}, {81, "Χείμαρρος", "Alpha River", 2000}})) ==
          Names({"Alpha River"}));
    // The ways of a name:en together, not the one of the most length alone,
    // hold more than half of it.
    CHECK(shown(river("Χείμαρρος", nullptr,
                      {{80, "Χείμαρρος", "Alpha River", 1500},
                       {81, "Χείμαρρος", "Beta River", 1000},
                       {82, "Χείμαρρος", nullptr, 2000}})) == Names({"Alpha River"}));
    // A relation none of whose ways is any length shows its own name.
    CHECK(shown(river("Χείμαρρος", nullptr, {{83, "Χείμαρρος", "Alpha River", 0}})) == Names({"Χείμαρρος"}));
    // Another key's lines keep their names beside a relation that shows its
    // ways' name:en.
    Collected c = river("小渋川", nullptr, {{51, "小渋川", "Koshibu River", 2000}});
    const Collected others = fixture();
    c.lines.insert(c.lines.end(), others.lines.begin(), others.lines.end());
    CHECK(shown(c) == Names({"Koshibu River"}));
    const Build b = assemble(c, 1);
    Names main, tributary;
    for (const NamingLine& l : b.lines.lines) {
        if (b.lines.keys[l.key] == "r900") main.insert(b.lines.displays[l.display]);
        if (b.lines.keys[l.key] == "nBach") tributary.insert(b.lines.displays[l.display]);
    }
    CHECK(main == Names({"Test River"}));
    CHECK(tributary == Names({"Brook"}));
}

TEST(a_name_en_shows_once_its_ways_hold_more_than_half_of_their_rivers_length) {
    // Half of it, to the millimetre (two ways of one length along one
    // parallel), is the relation's own name; a grid step more is the name:en.
    CHECK(shown(river("Χείμαρρος", nullptr, {{1, "Χείμαρρος", "Alpha River", 2000}, {2, "Χείμαρρος", nullptr, 2000}})) ==
          Names({"Χείμαρρος"}));
    CHECK(shown(river("Χείμαρρος", nullptr, {{1, "Χείμαρρος", "Alpha River", 2001}, {2, "Χείμαρρος", nullptr, 2000}})) ==
          Names({"Alpha River"}));
    CHECK(shown(river("Χείμαρρος", nullptr, {{1, "Χείμαρρος", nullptr, 2001}, {2, "Χείμαρρος", "Alpha River", 2000}})) ==
          Names({"Χείμαρρος"}));
}

TEST(a_ways_length_counts_none_across_a_node_the_input_lacks) {
    // A way of two runs far apart is its two runs' length: the name:en way
    // of two short runs holds less than half of its river, and the name:en
    // way beside a way of two short runs more.
    auto two_runs = [](Collected& c, size_t i) {
        LineIn& l = c.lines[i];
        l.vertices = {pt(kLat + 10000, kLng), pt(kLat + 10000, kLng + 1000), pt(kLat + 10000, kLng + 50000),
                      pt(kLat + 10000, kLng + 51000)};
        l.runs = {2, 2};
    };
    Collected short_en = river("Drava", nullptr, {{1, "Drava", "Drau", 2000}, {2, "Drava", nullptr, 3000}});
    two_runs(short_en, 0);
    CHECK(shown(short_en) == Names({"Drava"}));
    Collected long_en = river("Drava", nullptr, {{1, "Drava", "Drau", 3000}, {2, "Drava", nullptr, 2000}});
    two_runs(long_en, 1);
    CHECK(shown(long_en) == Names({"Drau"}));
}

TEST(one_river_way_of_a_name_en_of_its_own_doesnt_rename_its_river) {
    // The Spree: its relation and most of its river ways carry the name
    // Spree alone, a few none, a few the names of their reaches, and one
    // the name:en Sprewa (Sorbian).
    CHECK(shown(river("Spree", nullptr,
                      {{1, "Spree", nullptr, 3000},
                       {2, "Spree", "Sprewa", 4000},
                       {3, nullptr, nullptr, 1000},
                       {4, "Hauptspree", nullptr, 1500},
                       {5, "Spree", nullptr, 2500}})) == Names({"Spree"}));
}

TEST(a_branchs_name_en_doesnt_rename_its_river) {
    // The Hunter River: its relation and most of its river ways carry the
    // name Hunter River alone; its two arms carry their own, one of them
    // as its name:en too.
    CHECK(shown(river("Hunter River", nullptr,
                      {{1, "Hunter River", nullptr, 4000},
                       {2, "Hunter River South Arm", "Hunter River South Arm", 1500},
                       {3, "Hunter River North Arm", nullptr, 1700},
                       {4, "Hunter River", nullptr, 4000}})) == Names({"Hunter River"}));
}

TEST(the_name_en_of_a_bridge_on_a_canal_way_doesnt_rename_the_canal) {
    // The Birmingham and Fazeley Canal: one way of its name carries a
    // bridge's name as its name:en, and the tunnel's way the tunnel's name.
    CHECK(shown(river("Birmingham and Fazeley Canal", nullptr,
                      {{1, "Birmingham and Fazeley Canal", nullptr, 3000},
                       {2, "Birmingham and Fazeley Canal", "Erdington Hall Bridge", 500},
                       {3, "Curdworth Tunnel", nullptr, 400},
                       {4, "Birmingham and Fazeley Canal", nullptr, 3000}})) ==
          Names({"Birmingham and Fazeley Canal"}));
}

TEST(a_river_of_as_many_ways_of_one_name_en_as_another_shows_its_own_name) {
    // The Drava: its relation is Drava alone. Its upper reach is many short
    // ways of the name Drau, its lower reaches fewer and longer ways of the
    // name Drava; as many of its ways carry the name:en Drau as Drava, over
    // less than half its length.
    CHECK(shown(river("Drava", nullptr,
                      {{1, "Drau", nullptr, 1000},
                       {2, "Drau", nullptr, 1000},
                       {3, "Drau", nullptr, 1000},
                       {4, "Drau", nullptr, 1000},
                       {5, "Drau", nullptr, 1000},
                       {6, "Drava", "Drau", 1000},
                       {7, "Drava", "Drava", 1000},
                       {8, "Drava", nullptr, 3000},
                       {9, "Drava", nullptr, 3000}})) == Names({"Drava"}));
}

TEST(the_local_names_of_a_rivers_reaches_dont_rename_it) {
    // The Havel: its relation is Havel alone, and more of its river ways'
    // length carries the name of a reach, Schnelle Havel, than Havel.
    CHECK(shown(river("Havel", nullptr,
                      {{1, "Havel", nullptr, 2200},
                       {2, "Schnelle Havel", nullptr, 4200},
                       {3, "Gülper Havel", nullptr, 600},
                       {4, nullptr, nullptr, 300}})) == Names({"Havel"}));
    // A waterway relation of a lock: its ways carry the names of the lock's
    // reaches.
    CHECK(shown(river("Aller Schleuse", nullptr,
                      {{11, "Schleuse Marklendorf Oberwasser", nullptr, 300},
                       {12, "Schleuse Marklendorf Unterwasser", nullptr, 300},
                       {13, "Schleuse Marklendorf", nullptr, 200}})) == Names({"Aller Schleuse"}));
    // The Aller: the lock's ways on it, a minority of its length, carry the
    // lock's names, one of them as its name:en too.
    CHECK(shown(river("Aller", nullptr,
                      {{21, "Aller", nullptr, 5000},
                       {22, "Schleuse Marklendorf Oberwasser", "Marklendorf Lock", 300},
                       {23, "Schleuse Marklendorf", nullptr, 200},
                       {24, "Aller", nullptr, 4000}})) == Names({"Aller"}));
}

namespace {

// Swaps each day's staged dir, `<dir>/<day>.staged`, in for `<dir>/<day>`,
// keeping the old dirs until every day is in: a swap that fails puts back
// each day it swapped (or removes it, for a day that had no dir) and
// rethrows, so the dir holds the old days or the new ones, never some of
// each.
void swap_in_staged(const std::string& dir, const std::vector<std::string>& days) {
    std::vector<std::string> swapped;
    try {
        for (const std::string& day : days) {
            const std::string day_dir = dir + "/" + day, old = day_dir + ".old";
            fs::remove_all(old);
            if (fs::exists(day_dir)) fs::rename(day_dir, old);
            swapped.push_back(day);
            fs::rename(day_dir + ".staged", day_dir);
        }
    } catch (const fs::filesystem_error&) {
        for (const std::string& day : swapped) {
            const std::string day_dir = dir + "/" + day, old = day_dir + ".old";
            fs::remove_all(day_dir);
            if (fs::exists(old)) fs::rename(old, day_dir);
        }
        for (const std::string& day : days) fs::remove_all(dir + "/" + day + ".staged");
        throw;
    }
    for (const std::string& day : days) fs::remove_all(dir + "/" + day + ".old");
}

void write_file(const std::string& path, const std::string& text) {
    fs::create_directories(fs::path(path).parent_path());
    std::ofstream(path, std::ios::binary) << text;
}

}  // namespace

TEST(a_fixture_swap_that_fails_part_way_puts_back_the_days_it_swapped) {
    // Day 1 has a dir and a staged one, day 2 a staged one alone, day 3 a
    // dir and no staged one, so its swap fails.
    const ScratchDir root("water-fixture-swap");
    const std::string dir = root.path();
    write_file(dir + "/day1/a", "old 1");
    write_file(dir + "/day1.staged/a", "new 1");
    write_file(dir + "/day2.staged/a", "new 2");
    write_file(dir + "/day3/a", "old 3");
    bool threw = false;
    try {
        swap_in_staged(dir, {"day1", "day2", "day3"});
    } catch (const fs::filesystem_error&) {
        threw = true;
    }
    CHECK(threw);
    CHECK(tree(dir) == (std::map<std::string, std::vector<char>>{{"day1/a", {'o', 'l', 'd', ' ', '1'}},
                                                                 {"day3/a", {'o', 'l', 'd', ' ', '3'}}}));
    // With every staged dir there, every day is the new one and no old dir
    // stays.
    write_file(dir + "/day1.staged/a", "new 1");
    write_file(dir + "/day2.staged/a", "new 2");
    write_file(dir + "/day3.staged/a", "new 3");
    swap_in_staged(dir, {"day1", "day2", "day3"});
    CHECK(tree(dir) == (std::map<std::string, std::vector<char>>{{"day1/a", {'n', 'e', 'w', ' ', '1'}},
                                                                 {"day2/a", {'n', 'e', 'w', ' ', '2'}},
                                                                 {"day3/a", {'n', 'e', 'w', ' ', '3'}}}));
}

// geocoder/testdata/water_dir: the client files of a water dir on three
// days, which tools/test_generate_patches.py diffs, patches and verifies
// with the real tools. Day 2 is day 1 with the pond's second vertex moved,
// on day 1's slots. Day 3 is day 2 with a pond of a water=* value of its own
// and a sea, on day 2's slots: its water_meta.json lists one more kind and
// its attribution.json credits Marine Regions. Under
// GEOCODER_WRITE_TESTDATA=1 this writes them instead.
TEST(the_water_dir_fixture_of_the_patch_tests_is_what_the_layer_writes) {
    const ScratchDir root("water-fixture");
    const std::string one = root.path() + "/one", two = root.path() + "/two", three = root.path() + "/three";
    write(build(fixture()), one);
    Collected moved = fixture();
    REQUIRE(moved.closed_ways[0].way == 20);
    moved.closed_ways[0].vertices[1].lat_e7 += 300;
    write(build(moved, one), two, one);
    Collected more = moved;
    ObjectTags salt = tags(0, "Salzweiher");
    salt.verbatim = "salt_pond";
    more.closed_ways.push_back({33, salt, square(kLat + 600000, kLng, 1000)});
    more.marine.push_back(marine_area(2350, marine::kSea, "North Sea", {{square(kLat - 20000000, kLng, 20000, 4)}}));
    write(build(more, two), three, two);
    const std::string fixture_dir = std::string(GEOCODER_TESTDATA_DIR) + "/water_dir";
    const char* write_testdata = std::getenv("GEOCODER_WRITE_TESTDATA");
    const bool rewrite = write_testdata && std::string(write_testdata) == "1";
    // Every day's client files, each day all ten, before any fixture dir is
    // compared or written.
    const std::set<std::string> client = {"admin_polygons.bin", "admin_rings.bin", "arc_index.bin", "arc_vertices.bin",
                                          "admin_cells.bin", "admin_entries.bin", "strings_core.bin",
                                          "strings_layout.json", "water_meta.json", "attribution.json"};
    std::map<std::string, std::map<std::string, std::vector<char>>> days;
    for (const auto& [day, dir] : {std::pair<std::string, std::string>{"day1", one}, {"day2", two}, {"day3", three}}) {
        std::set<std::string> names;
        for (const auto& [name, bytes] : tree(dir + "/water/all/exact"))
            if (name.size() <= 8 || name.compare(name.size() - 8, 8, ".osm_ids") != 0) {
                days[day][name] = bytes;
                names.insert(name);
            }
        REQUIRE(names == client);
    }
    // The kinds and credits the patch tests rely on, before any fixture dir
    // is written.
    for (const char* f : {"water_meta.json", "attribution.json"}) {
        REQUIRE(days["day1"][f] == days["day2"][f]);
        REQUIRE(days["day2"][f] != days["day3"][f]);
    }
    REQUIRE(text_of(days["day3"]["water_meta.json"]).find("\"type\": \"salt_pond\"") != std::string::npos);
    REQUIRE(text_of(days["day3"]["attribution.json"]).find("marine-regions") != std::string::npos);
    if (rewrite) {
        // Every day written whole beside its dir first, then each swapped in:
        // a write or a swap that fails leaves the fixture as it was.
        std::vector<std::string> day_names;
        for (const auto& [day, files] : days) {
            const std::string staged = fixture_dir + "/" + day + ".staged";
            fs::remove_all(staged);
            for (const auto& [name, bytes] : files) {
                const std::string path = staged + "/" + name;
                fs::create_directories(fs::path(path).parent_path());
                std::ofstream out(path, std::ios::binary);
                out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
                out.close();
                REQUIRE(out.good());
            }
            day_names.push_back(day);
        }
        swap_in_staged(fixture_dir, day_names);
    }
    for (const auto& [day, files] : days) {
        // The day's fixture dir is those files and no other: the patch tests
        // copy it whole.
        const std::string day_dir = fixture_dir + "/" + day;
        REQUIRE(fs::is_directory(day_dir));
        CHECK(tree(day_dir) == files);
    }
}

TEST(an_unnamed_riverbank_tagged_natural_water_too_takes_its_waterways_name) {
    Collected c = fixture();
    const ObjectTags bank = tags_from({{"natural", "water"}, {"waterway", "riverbank"}});
    CHECK(bank.river_area);
    CHECK_EQ(bank.code, code::kRiver);
    c.closed_ways.push_back({2400, bank, square(kLat + 900000, kLng, 10000)});
    LineIn aare;
    aare.way = 2401;
    aare.key = "nAare";
    aare.local = aare.display = "Aare";
    aare.vertices = {pt(kLat + 902000, kLng + 2000), pt(kLat + 906000, kLng + 6000)};
    c.lines.push_back(aare);
    const Layer layer = build(c);
    std::vector<std::string> names;
    for (size_t p = 0; p < layer.size(); p++)
        if (layer.identities[p] == way_identity(2400)) {
            names.push_back(name_of(layer, p));
            CHECK_EQ(layer.records[p].country_code, code::kRiver);
        }
    CHECK(names == std::vector<std::string>({"Aare"}));
}

namespace {

// Two water=* values whose hashes give one code, the smaller first.
std::pair<std::string, std::string> colliding_values() {
    std::map<uint16_t, std::string> seen;
    for (int k = 0;; k++) {
        const std::string v = "kind" + std::to_string(k);
        auto [it, fresh] = seen.emplace(static_cast<uint16_t>(value_hash(v) & 0x7FFF), v);
        if (!fresh) return {std::min(it->second, v), std::max(it->second, v)};
    }
}

// The fixture with a pond of each of `values`, water=* values of their own.
Collected with_kinds(const std::vector<std::string>& values) {
    Collected c = fixture();
    int64_t way = 3100;
    for (const std::string& v : values) {
        ObjectTags t = tags(0, "Pond " + v);
        t.verbatim = v;
        c.closed_ways.push_back({way, t, square(kLat + 1000000 + int32_t(way - 3100) * 10000, kLng, 1000)});
        way++;
    }
    return c;
}

// Why the layer's build over the previous build at `prev` stops, empty for
// a build that doesn't.
std::string refusal(Collected c, const std::string& prev) {
    const gctest::CapturedStderr log;
    return gctest::refusal_of<std::runtime_error>([&] {
        build_layer(c, prev, [](uint32_t) { return 0u; }, 2);
    });
}

}  // namespace

TEST(a_previous_water_build_whose_water_meta_json_is_gone_or_cut_short_stops_the_build) {
    // Two values of one hash code: the larger came first and took it, the
    // smaller the next free one; started afresh they would swap.
    const auto [small, large] = colliding_values();
    const uint16_t hashed = static_cast<uint16_t>(kVerbatimBase | (value_hash(small) & 0x7FFF));
    const ScratchDir root("water-kinds");
    const std::string one = root.path() + "/one", two = root.path() + "/two";
    write(build(with_kinds({large})), one);
    const Layer day2 = build(with_kinds({large, small}), one);
    CHECK_EQ(day2.types->verbatim_code(large), hashed);
    CHECK_EQ(day2.types->verbatim_code(small), uint16_t(hashed + 1));
    write(day2, two, one);
    CHECK_EQ(build(with_kinds({large, small}), two).types->verbatim_code(small), uint16_t(hashed + 1));
    CHECK_EQ(build(with_kinds({large, small})).types->verbatim_code(small), hashed);

    // Its water_meta.json cut short, or gone.
    const std::string meta = two + "/water/all/exact/water_meta.json";
    const std::vector<char> bytes = bytes_of(meta);
    std::ofstream(meta, std::ios::binary | std::ios::trunc).write(bytes.data(), std::streamsize(bytes.size() / 2));
    CHECK(refusal(with_kinds({large, small}), two).find("water_meta.json") != std::string::npos);
    fs::remove(meta);
    CHECK(refusal(with_kinds({large, small}), two).find("water_meta.json") != std::string::npos);
    // One of no kinds is sound, and a previous build without water dirs
    // starts the codes afresh.
    std::ofstream(meta, std::ios::binary | std::ios::trunc) << "{\"types\": [\n\n]}\n";
    CHECK(refusal(with_kinds({large, small}), two).empty());
    fs::remove_all(two + "/water");
    CHECK(refusal(with_kinds({large, small}), two).empty());
}

TEST(a_previous_water_build_whose_polygon_sidecar_cant_be_read_stops_the_build) {
    const ScratchDir root("water-sidecars");
    const std::string one = root.path() + "/one", two = root.path() + "/two";
    write(build(fixture()), one);
    auto cut = [](const std::string& path) {
        const std::vector<char> bytes = bytes_of(path);
        REQUIRE(bytes.size() >= 12);
        std::ofstream(path, std::ios::binary | std::ios::trunc).write(bytes.data(), std::streamsize(bytes.size() - 6));
        return bytes;
    };
    auto put_back = [](const std::string& path, const std::vector<char>& bytes) {
        std::ofstream(path, std::ios::binary | std::ios::trunc).write(bytes.data(), std::streamsize(bytes.size()));
    };
    // The major tier's, cut short: the layer's build on it stops.
    const std::string major = one + "/water/major/simplified/admin_polygons.osm_ids";
    const std::vector<char> major_bytes = cut(major);
    CHECK(refusal(fixture(), one).find(major) != std::string::npos);
    put_back(major, major_bytes);
    // The all set's: its dirs' write on it stops.
    const std::string all = one + "/water/all/exact/admin_polygons.osm_ids";
    cut(all);
    const Layer day2 = build(fixture(), one);
    std::string why;
    {
        const gctest::CapturedStderr log;
        why = gctest::refusal_of<std::runtime_error>([&] { write_water_dirs(day2, two, one, "test"); });
    }
    CHECK(why.find(all) != std::string::npos);
    // Gone, they start the slots afresh.
    fs::remove(major);
    fs::remove(all);
    CHECK(refusal(fixture(), one).empty());
    CHECK(write(build(fixture(), one), two, one).find("4 polygons in 4 slots (0 kept") != std::string::npos);
}

TEST(a_previous_water_build_whose_fallback_sidecar_is_there_but_cant_be_opened_stops_the_build) {
    const ScratchDir root("water-fallbacks");
    const std::string one = root.path() + "/one", two = root.path() + "/two";
    write(build(fixture()), one);
    // A link to itself is there, and no file can be opened through it.
    auto unopenable = [](const std::string& path) {
        fs::remove(path);
        fs::create_symlink(fs::path(path).filename(), path);
    };
    const std::string major = one + "/water/major/simplified/admin_polygons.fallback.osm_ids";
    const std::string all = one + "/water/all/exact/admin_polygons.fallback.osm_ids";
    unopenable(major);
    CHECK(refusal(fixture(), one).find(major) != std::string::npos);
    fs::remove(major);
    CHECK(refusal(fixture(), one).empty());
    unopenable(all);
    const Layer day2 = build(fixture(), one);
    std::string why;
    {
        const gctest::CapturedStderr log;
        why = gctest::refusal_of<std::runtime_error>([&] { write_water_dirs(day2, two, one, "test"); });
    }
    CHECK(why.find(all) != std::string::npos);
    // Gone, the slots keep their identities without fallback keys.
    fs::remove(all);
    CHECK(write(build(fixture(), one), root.path() + "/three", one).find("4 polygons in 4 slots (4 kept") !=
          std::string::npos);
}

TEST(a_previous_water_build_whose_polygon_sidecar_is_there_but_cant_be_opened_stops_the_layers_build) {
    // Its type codes hang on that build's water_meta.json, which the sidecar
    // being there says it wrote: the layer's build stops before it numbers
    // the water=* values afresh.
    const ScratchDir root("water-unopenable");
    const std::string one = root.path() + "/one";
    write(build(with_kinds({"ganga_river"})), one);
    const std::string all = one + "/water/all/exact/admin_polygons.osm_ids";
    fs::remove(all);
    fs::create_symlink(fs::path(all).filename(), all);
    CHECK(refusal(with_kinds({"ganga_river"}), one).find(all + " is there but can't be opened") != std::string::npos);
    fs::remove(all);
    CHECK(refusal(with_kinds({"ganga_river"}), one).empty());
}

TEST(a_way_tagged_as_the_multipolygon_whose_outer_ring_it_is_leaves_the_island_out) {
    Collected c = fixture();
    const int32_t lat = kLat - 1000000;
    // A lake way tagged as the relation that takes it whole as its outer
    // ring round an island, and the island way, tagged on its own.
    const Ring shore = square(lat, kLng, 100000, 4), isle = square(lat + 40000, kLng + 40000, 20000);
    add_geometry(c, 2700, shore);
    add_geometry(c, 2701, isle);
    c.closed_ways.push_back({2700, tags(code::kLake, "Twin Lake"), shore});
    c.closed_ways.push_back({2701, tags(code::kWetland, "Isle Marsh"), isle});
    CollectedArea twin;
    twin.id = 2800;
    twin.tags = tags(code::kLake, "Twin Lake");
    twin.members = {{2700, "outer"}, {2701, "inner"}};
    c.relations.push_back(twin);
    // Kept: a way that is the outer ring of a relation of another name, and
    // one whose relation lacks a member way.
    const Ring other = square(lat + 200000, kLng, 100000), broken = square(lat + 400000, kLng, 100000);
    add_geometry(c, 2710, other);
    c.closed_ways.push_back({2710, tags(code::kLake, "Other Lake"), other});
    CollectedArea mere;
    mere.id = 2810;
    mere.tags = tags(code::kLake, "Mere");
    mere.members = {{2710, "outer"}};
    c.relations.push_back(mere);
    add_geometry(c, 2720, broken);
    c.closed_ways.push_back({2720, tags(code::kLake, "Broken Lake"), broken});
    CollectedArea lacking;
    lacking.id = 2820;
    lacking.tags = tags(code::kLake, "Broken Lake");
    lacking.members = {{2720, "outer"}, {99999, "inner"}};
    c.relations.push_back(lacking);
    // And ways that are the outer rings of relations of their names but of
    // another kind: a lake in a wetland of its name, and a pond of a water=*
    // value of its own in a lake of another such value.
    const Ring moor = square(lat + 600000, kLng, 100000), salt = square(lat + 800000, kLng, 100000);
    add_geometry(c, 2730, moor);
    c.closed_ways.push_back({2730, tags(code::kLake, "Moor Lake"), moor});
    CollectedArea wetland;
    wetland.id = 2830;
    wetland.tags = tags(code::kWetland, "Moor Lake");
    wetland.members = {{2730, "outer"}};
    c.relations.push_back(wetland);
    add_geometry(c, 2740, salt);
    ObjectTags pond = tags(0, "Salt Water"), lake = pond;
    pond.verbatim = "salt_pond";
    lake.verbatim = "salt_lake";
    c.closed_ways.push_back({2740, pond, salt});
    CollectedArea salt_lake;
    salt_lake.id = 2840;
    salt_lake.tags = lake;
    salt_lake.members = {{2740, "outer"}};
    c.relations.push_back(salt_lake);
    const Layer layer = build(c);
    CHECK(ways_held(layer, {2700, 2701, 2710, 2720, 2730, 2740}).first ==
          std::set<int64_t>({2701, 2710, 2720, 2730, 2740}));
    for (int64_t relation : {2830, 2840}) {
        size_t held = 0;
        for (size_t p = 0; p < layer.size(); p++) held += layer.fallbacks[p] == relation_ring_fallback(relation, 0);
        CHECK_EQ(held, size_t(1));
    }
    // The lake is the relation's polygon, the island a hole in it.
    size_t twins = 0;
    for (size_t p = 0; p < layer.size(); p++)
        if (layer.fallbacks[p] == relation_ring_fallback(2800, 0)) {
            twins++;
            CHECK(name_of(layer, p) == "Twin Lake");
            CHECK_EQ(layer.arcs.polygons[p].ring_count, 2u);
        }
    CHECK_EQ(twins, size_t(1));
}

TEST(a_way_its_relation_takes_only_one_lobe_of_stays_beside_the_relation) {
    Collected c = fixture();
    const int32_t lat = kLat - 7000000;
    // A lake way of two squares meeting at a corner, x, and a way round the
    // west one again: the relation of both, of the lake's tags, cancels the
    // west square and keeps the east one, all of it the lake way's.
    const PackedLocation x = pt(lat, kLng + 100000);
    const Ring west = {x, pt(lat + 100000, kLng + 100000), pt(lat + 100000, kLng), pt(lat, kLng), x};
    Ring eight = west;
    for (const PackedLocation p :
         {pt(lat, kLng + 200000), pt(lat - 100000, kLng + 200000), pt(lat - 100000, kLng + 100000), x})
        eight.push_back(p);
    add_geometry(c, 2900, eight);
    add_geometry(c, 2901, west);
    c.closed_ways.push_back({2900, tags(code::kLake, "Eight Lake"), eight});
    CollectedArea rel;
    rel.id = 2950;
    rel.tags = tags(code::kLake, "Eight Lake");
    rel.members = {{2900, "outer"}, {2901, "outer"}};
    c.relations.push_back(rel);
    const Layer layer = build(c);
    CHECK(ways_held(layer, {2900}).first == std::set<int64_t>({2900}));
    size_t east = 0;
    for (size_t p = 0; p < layer.size(); p++) east += layer.fallbacks[p] == relation_ring_fallback(2950, 0);
    CHECK_EQ(east, size_t(1));
}

TEST(a_way_left_to_its_relation_gives_it_its_wikidata_item_where_the_relation_has_none) {
    // A river area of about 7.6 km2 tagged on a relation without wikidata
    // and on its outer way with Q5: the relation takes Q5, so the major
    // tier's rule for wikidata rivers holds it.
    Collected c = fixture();
    const Ring river = square(kLat - 8000000, kLng, 300000);
    add_geometry(c, 2910, river);
    c.closed_ways.push_back({2910, tags(code::kRiver, "Twin River", true, 5), river});
    CollectedArea rel;
    rel.id = 2960;
    rel.tags = tags(code::kRiver, "Twin River", true);
    rel.members = {{2910, "outer"}};
    c.relations.push_back(rel);
    // The same without the way's item: out of the major tier.
    const Ring plain = square(kLat - 9000000, kLng, 300000);
    add_geometry(c, 2911, plain);
    c.closed_ways.push_back({2911, tags(code::kRiver, "Plain Twin", true), plain});
    CollectedArea bare;
    bare.id = 2961;
    bare.tags = tags(code::kRiver, "Plain Twin", true);
    bare.members = {{2911, "outer"}};
    c.relations.push_back(bare);
    const Layer layer = build(c);
    CHECK(ways_held(layer, {2910, 2911}).first.empty());
    std::map<std::string, bool> major;
    for (size_t p = 0; p < layer.size(); p++) {
        const std::string name = name_of(layer, p);
        if (name == "Twin River" || name == "Plain Twin") major[name] = layer.major[p];
    }
    const std::map<std::string, bool> want = {{"Twin River", true}, {"Plain Twin", false}};
    CHECK(major == want);
}

TEST(the_lines_are_keyed_by_the_waterway_relations_of_every_input_once_all_are_read) {
    // Two copies of a river way, the first read before the input that holds
    // its waterway relation: both are the relation's.
    Collected c;
    LineIn early;
    early.way = 4001;
    early.name = "Limmat";
    early.vertices = {pt(kLat, kLng), pt(kLat + 1000, kLng)};
    c.lines.push_back(early);
    CollectedWaterway river;
    river.id = 4100;
    river.name = "Limmat River";
    river.local_name = "Limmat";
    river.english = true;
    river.qid = 72;
    river.ways = {4001};
    c.waterways.push_back(river);
    c.lines.push_back(early);
    // A way of a name and a name:en of its own, and an unnamed one of no
    // relation, which names nothing.
    LineIn sihl = early;
    sihl.way = 4002;
    sihl.name = "Sihl";
    sihl.name_en = "Sihl River";
    sihl.way_qid = 9;
    c.lines.push_back(sihl);
    LineIn unnamed = early;
    unnamed.way = 4003;
    unnamed.name.clear();
    c.lines.push_back(unnamed);
    key_lines(c);
    REQUIRE(c.lines.size() == 3u);
    for (size_t k = 0; k < 2; k++) {
        CHECK(c.lines[k].key == "r4100");
        CHECK(c.lines[k].display == "Limmat River");
        CHECK(c.lines[k].local == "Limmat");
        CHECK_EQ(c.lines[k].qid, 72u);
    }
    CHECK(c.lines[2].key == "nSihl" && c.lines[2].display == "Sihl River" && c.lines[2].local == "Sihl");
    CHECK_EQ(c.lines[2].qid, 9u);
}

TEST(the_collected_relations_are_indexed_again_only_once_more_are_collected) {
    Collected c;
    CollectedWaterway river;
    river.id = 4100;
    river.name = river.local_name = "Limmat";
    river.ways = {4001};
    c.waterways.push_back(river);
    index_collected(c);
    REQUIRE(c.waterway_of_way.count(4001) == 1u);
    // An entry the index never makes shows whether it is made again: not by
    // another input's pass over the same relations, nor by key_lines.
    c.waterway_of_way[99] = 0;
    index_collected(c);
    CHECK(c.waterway_of_way.count(99) == 1u);
    key_lines(c);
    CHECK(c.waterway_of_way.count(99) == 1u);
    // A relation more, a waterway or an area: indexed again.
    river.id = 4101;
    river.ways = {4002};
    c.waterways.push_back(river);
    index_collected(c);
    CHECK(c.waterway_of_way.count(99) == 0u);
    CHECK(c.waterway_of_way.count(4002) == 1u);
    c.waterway_of_way[99] = 0;
    CollectedArea lake;
    lake.id = 500;
    lake.tags = tags(code::kLake, "See");
    lake.members = {{11, "outer"}};
    c.relations.push_back(lake);
    index_collected(c);
    CHECK(c.waterway_of_way.count(99) == 0u);
    CHECK_EQ(c.waterway_of_way.size(), size_t(2));
}

TEST(a_namesake_inside_a_marine_area_along_its_side_goes_and_one_touching_or_crossing_it_stays) {
    Collected c = with_marine();
    // The North Sea: lat .. lat + 0.2 by lng .. lng + 0.2, a vertex each
    // 0.005 along its sides.
    const int32_t lat = kLat - 20000000, lng = kLng + 2000000;
    auto rect = [](int32_t south, int32_t west, int32_t height, int32_t width) {
        return Ring{pt(south, west), pt(south, west + width), pt(south + height, west + width), pt(south + height, west),
                    pt(south, west)};
    };
    // A bay of its name south of it, its north side along the sea's south
    // side: none of its water is the sea's.
    c.closed_ways.push_back({3001, tags(code::kNaturalBay, "North Sea"), rect(lat - 300000, lng + 500000, 300000, 300000)});
    // A strait of its name across it from south to north between two of the
    // sea's vertices: much of it in the sea, its ends out.
    c.closed_ways.push_back({3002, tags(code::kNaturalStrait, "North Sea"), rect(lat - 300000, lng + 910000, 2600000, 30000)});
    // Inside it: a bay along its south side from a vertex of the sea's to a
    // point between two, and a strait from its south side to its north,
    // its corners between the sea's vertices.
    c.closed_ways.push_back({3007, tags(code::kNaturalBay, "North Sea"), rect(lat, lng + 500000, 300000, 320000)});
    c.closed_ways.push_back({3008, tags(code::kNaturalStrait, "North Sea"), rect(lat, lng + 1210000, 2000000, 30000)});
    const Layer layer = build(c);
    CHECK(ways_held(layer, {3001, 3002, 3007, 3008}).first == std::set<int64_t>({3001, 3002}));
}

TEST(a_namesake_the_marine_areas_of_its_name_hold_together_goes_and_one_over_their_island_stays) {
    Collected c = with_marine();
    // The Irish Sea: two squares sharing a side, one with a vertex each
    // 0.005 along it, the other each 0.04.
    const int32_t lat = kLat - 23000000;
    c.marine.push_back(marine_area(2360, marine::kSea, "Irish Sea",
                                   {{square(lat, kLng, 2000000, 40)}, {square(lat, kLng + 2000000, 2000000, 5)}}));
    // A bay over both, across the side they share, and one reaching past
    // them.
    c.closed_ways.push_back({3010, tags(code::kNaturalBay, "Irish Sea"), square(lat + 500000, kLng + 1500000, 1000000)});
    c.closed_ways.push_back({3011, tags(code::kNaturalBay, "Irish Sea"), square(lat + 1500000, kLng + 3500000, 1000000)});
    // Bays of the ocean's name round its island (lat .. + 0.2: the island
    // lat + 0.05 .. 0.06, west + 0.05 .. 0.06): one over it, one whose hole
    // is the island, another whose hole is larger.
    const int32_t ocean = kLat - 20000000;
    const Ring island = square(ocean + 500000, kLng + 500000, 100000, 3);
    c.closed_ways.push_back({3012, tags(code::kNaturalBay, "North Atlantic Ocean"), square(ocean + 400000, kLng + 400000, 300000)});
    add_geometry(c, 3013, square(ocean + 400000, kLng + 400000, 300000));
    add_geometry(c, 3014, island);
    add_geometry(c, 3015, square(ocean + 450000, kLng + 450000, 200000));
    CollectedArea holed;
    holed.id = 3020;
    holed.tags = tags(code::kNaturalBay, "North Atlantic Ocean");
    holed.members = {{3013, "outer"}, {3014, "inner"}};
    c.relations.push_back(holed);
    CollectedArea wider = holed;
    wider.id = 3021;
    wider.members = {{3013, "outer"}, {3015, "inner"}};
    c.relations.push_back(wider);
    const Layer layer = build(c);
    CHECK(ways_held(layer, {3010, 3011, 3012}).first == std::set<int64_t>({3011, 3012}));
    std::map<uint64_t, size_t> rings;
    for (size_t p = 0; p < layer.size(); p++) rings[layer.fallbacks[p]]++;
    CHECK_EQ(rings[relation_ring_fallback(3020, 0)], size_t(0));
    CHECK_EQ(rings[relation_ring_fallback(3021, 0)], size_t(0));
}

TEST(a_namesake_cutting_a_marine_areas_corner_or_meeting_it_along_a_side_or_at_a_point_stays) {
    Collected c = with_marine();
    // The North Sea, as above: its east side at east, a vertex each 0.005
    // along it.
    const int32_t lat = kLat - 20000000, east = kLng + 4000000;
    // A strait of its name across its south-east corner, a band 0.0002 wide
    // from south of it to east of it: neither holds a vertex of the other,
    // and the parallel halfway up the latitudes the two share passes east
    // of the sea, so only the strait's edges crossing the sea's east side
    // show the water of the strait the sea doesn't hold.
    c.closed_ways.push_back({3003, tags(code::kNaturalStrait, "North Sea"),
                             Ring{pt(lat - 30000, east - 40000), pt(lat + 40000, east + 30000), pt(lat + 38000, east + 30000),
                                  pt(lat - 30000, east - 38000), pt(lat - 30000, east - 40000)}});
    // Bays of its name east of it, holding no water with it: one along its
    // east side over two of the sea's edges, one whose vertex lies on that
    // side between two of the sea's, and one meeting a vertex of the sea at
    // a vertex of its own.
    c.closed_ways.push_back({3004, tags(code::kNaturalBay, "North Sea"),
                             Ring{pt(lat + 1000000, east), pt(lat + 1000000, east + 100000), pt(lat + 1100000, east + 100000),
                                  pt(lat + 1100000, east), pt(lat + 1000000, east)}});
    c.closed_ways.push_back({3005, tags(code::kNaturalBay, "North Sea"),
                             Ring{pt(lat + 1525000, east), pt(lat + 1500000, east + 100000), pt(lat + 1550000, east + 100000),
                                  pt(lat + 1525000, east)}});
    c.closed_ways.push_back({3006, tags(code::kNaturalBay, "North Sea"),
                             Ring{pt(lat + 1800000, east), pt(lat + 1750000, east + 50000), pt(lat + 1800000, east + 100000),
                                  pt(lat + 1850000, east + 50000), pt(lat + 1800000, east)}});
    const Layer layer = build(c);
    CHECK(ways_held(layer, {3003, 3004, 3005, 3006}).first == std::set<int64_t>({3003, 3004, 3005, 3006}));
}

TEST(a_namesake_inside_marine_areas_of_its_name_that_overlap_goes) {
    Collected c = with_marine();
    // Marine areas of one name and MRGIDs of their own: the Celtic Sea
    // twice over one square, and the Bristol Channel over two squares that
    // overlap by half.
    const int32_t lat = kLat - 26000000;
    auto rect = [](int32_t south, int32_t west, int32_t height, int32_t width) {
        return Ring{pt(south, west), pt(south, west + width), pt(south + height, west + width), pt(south + height, west),
                    pt(south, west)};
    };
    c.marine.push_back(marine_area(2370, marine::kSea, "Celtic Sea", {{square(lat, kLng, 2000000, 5)}}));
    c.marine.push_back(marine_area(2371, marine::kSea, "Celtic Sea", {{square(lat, kLng, 2000000, 5)}}));
    c.marine.push_back(marine_area(2372, marine::kSea, "Bristol Channel", {{square(lat, kLng + 3000000, 2000000, 5)}}));
    c.marine.push_back(marine_area(2373, marine::kSea, "Bristol Channel", {{square(lat, kLng + 4000000, 2000000, 5)}}));
    // Bays of their names: in the Celtic Sea's square, in the half both
    // squares of the Bristol Channel hold, and across that half from one
    // square's own half to the other's, all of which they hold. One
    // reaching east past them stays.
    c.closed_ways.push_back({3030, tags(code::kNaturalBay, "Celtic Sea"), rect(lat + 500000, kLng + 500000, 500000, 500000)});
    c.closed_ways.push_back(
        {3031, tags(code::kNaturalBay, "Bristol Channel"), rect(lat + 500000, kLng + 4200000, 500000, 500000)});
    c.closed_ways.push_back(
        {3032, tags(code::kNaturalBay, "Bristol Channel"), rect(lat + 500000, kLng + 3500000, 500000, 2000000)});
    c.closed_ways.push_back(
        {3033, tags(code::kNaturalBay, "Bristol Channel"), rect(lat + 500000, kLng + 4500000, 500000, 2000000)});
    const Layer layer = build(c);
    CHECK(ways_held(layer, {3030, 3031, 3032, 3033}).first == std::set<int64_t>({3033}));
}

TEST(a_namesake_inside_one_marine_area_of_its_name_goes_though_another_of_its_name_crosses_it) {
    Collected c = with_marine();
    // The North Sea, as above, and a second area of its name over part of
    // it, a triangle whose sloping side crosses the east side of a bay of
    // its name that the North Sea's square holds whole, at lat + 0.1025:
    // between two latitudes of a vertex of either.
    const int32_t lat = kLat - 20000000;
    c.marine.push_back(marine_area(2352, marine::kSea, "North Sea",
                                   {{Ring{pt(lat + 500000, kLng + 2675000), pt(lat + 500000, kLng + 3675000),
                                          pt(lat + 1500000, kLng + 3675000), pt(lat + 500000, kLng + 2675000)}}}));
    c.closed_ways.push_back({3040, tags(code::kNaturalBay, "North Sea"),
                             Ring{pt(lat + 800000, kLng + 2800000), pt(lat + 800000, kLng + 3200000),
                                  pt(lat + 1200000, kLng + 3200000), pt(lat + 1200000, kLng + 2800000),
                                  pt(lat + 800000, kLng + 2800000)}});
    const Layer layer = build(c);
    CHECK(ways_held(layer, {3040}).first.empty());
}
