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
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "arc_blocks.h"
#include "scratch_dir.h"
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
    std::ostringstream log;
    std::streambuf* was = std::cerr.rdbuf(log.rdbuf());
    Layer layer = build_layer(c, prev, [](uint32_t qid) { return qid == 42 ? 50u : 0u; }, threads);
    std::cerr.rdbuf(was);
    return layer;
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

std::string write(const Layer& layer, const std::string& dir, const std::string& prev = "") {
    std::ostringstream log;
    std::streambuf* was = std::cerr.rdbuf(log.rdbuf());
    const std::string summary = write_water_dirs(layer, dir, prev, "test");
    std::cerr.rdbuf(was);
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

TEST(the_join_credits_each_segment_to_the_area_holding_its_midpoint_and_none_outside) {
    Collected c = fixture();
    Build b = assemble(c, 1);
    std::vector<uint32_t> rivers;
    for (uint32_t p = 0; p < b.rings.polygons.size(); p++)
        if (b.objects[b.object_of[p]].tags.river_area) rivers.push_back(p);
    REQUIRE(rivers.size() == 2u);
    NamingCounts counts;
    std::vector<Credit> credits = credit_naming_lines(b.rings, rivers, b.object_of, b.lines, counts, 2);
    // The main line's first segment and the tributary are inside the river
    // area; the main line's second segment ends far east of it, its
    // midpoint outside.
    uint64_t main_mm = 0, tributary_mm = 0;
    for (const Credit& cr : credits) {
        CHECK_EQ(b.objects[cr.object].id, int64_t(30));
        (b.lines.keys[cr.key] == "r900" ? main_mm : tributary_mm) += cr.mm;
    }
    const PackedLocation* v = b.lines.vertices.data();
    CHECK_EQ(main_mm, segment_mm(v[0], v[1]));
    CHECK_EQ(tributary_mm, segment_mm(v[3], v[4]));
    const std::vector<NameChoice> choices = pick_names(credits, b.objects.size(), 1);
    size_t named = 0;
    for (size_t o = 0; o < b.objects.size(); o++) {
        if (b.objects[o].id != 30) continue;
        named++;
        CHECK(b.lines.keys[choices[o].key] == "r900");
        CHECK(b.lines.displays[choices[o].display] == "Test River");
    }
    CHECK_EQ(named, size_t(1));
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
    // The big lake holds whole cells: interior entries.
    size_t interior = 0;
    for (const CellItemPair& cell : layer.cells)
        interior += (cell.item_id & INTERIOR_FLAG) && name_of(layer, cell.item_id & ID_MASK) == "Big Lake";
    CHECK(interior > 0);
}

TEST(a_regions_share_holds_the_polygons_its_cells_list_whole_with_their_names) {
    const Layer planet = build(fixture());
    // The cells of the lake alone.
    std::vector<uint8_t> kept(planet.cells.size(), 0);
    for (size_t i = 0; i < planet.cells.size(); i++) kept[i] = (planet.cells[i].item_id & ID_MASK) == 3;
    const Layer region = subset_layer(planet, kept, 2);
    REQUIRE(region.size() == 1u);
    CHECK(name_of(region, 0) == "Test Lake");
    CHECK(region.names == std::vector<char>({'T', 'e', 's', 't', ' ', 'L', 'a', 'k', 'e', '\0'}));
    CHECK_EQ(region.identities[0], planet.identities[3]);
    CHECK_EQ(region.arcs.polygons.size(), size_t(1));
    CHECK_EQ(region.arcs.set(ArcSet::Full).arcs(), size_t(3));
    for (const CellItemPair& c : region.cells) CHECK_EQ(c.item_id & ID_MASK, 0u);
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
    for (const char* f : {"admin_polygons.bin", "admin_rings.bin", "admin_cells.bin", "strings_core.bin", "water_meta.json"})
        CHECK(files.at(std::string("water/all/simplified/") + f) == files.at(std::string("water/all/exact/") + f));
    CHECK(files.at("water/all/exact/admin_polygons.bin").size() == 4 * sizeof(AdminPolygonRecord));
    CHECK(files.at("water/major/simplified/admin_polygons.bin").empty());
    const std::vector<char>& meta = files.at("water/all/exact/water_meta.json");
    const std::vector<MetaType> kinds = parse_meta_json(std::string(meta.begin(), meta.end()));
    REQUIRE(kinds.size() == 4u);
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
    CHECK(summary.find("3 polygons in 4 slots (3 kept, 1 by fallback)") != std::string::npos);
    const auto after = tree(three);
    const std::vector<char>& records = after.at("water/all/exact/admin_polygons.bin");
    REQUIRE(records.size() == 4 * sizeof(AdminPolygonRecord));
    AdminPolygonRecord pond;
    std::memcpy(&pond, records.data(), sizeof pond);
    CHECK_EQ(pond.name_id, NO_DATA);
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
    Layer lake = subset_layer(planet, kept, 2);
    REQUIRE(lake.size() == 1u);
    CHECK_EQ(int(lake.marine[0]), 0);
    {
        std::ostringstream log;
        std::streambuf* was = std::cerr.rdbuf(log.rdbuf());
        assign_region_arc_slots(lake.arcs, "", "test", 0);
        std::cerr.rdbuf(was);
    }
    write(lake, root.path() + "/lake");
    CHECK(text_of(bytes_of(root.path() + "/lake/water/all/exact/attribution.json")).find("marine-regions") ==
          std::string::npos);
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
    CHECK(summary.find("7 polygons in 7 slots (7 kept, 1 by fallback)") != std::string::npos);
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
    std::set<uint64_t> ids(day1.identities.begin(), day1.identities.end());
    CHECK_EQ(ids.size(), day1.size());
    // The next day, the same data: every polygon in its slot, no slot more.
    const ScratchDir root("water-pinched");
    const std::string one = root.path() + "/one", two = root.path() + "/two";
    write(day1, one);
    const std::string summary = write(build(c, one), two, one);
    const std::string all = std::to_string(day1.size());
    CHECK(summary.find(all + " polygons in " + all + " slots (" + all + " kept, 0 by fallback)") != std::string::npos);
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

}  // namespace

TEST(an_osm_sea_is_no_water_area_as_the_marine_areas_hold_the_seas) {
    CHECK(!in_scope(tags_from({{"place", "sea"}, {"name", "Ionian Sea"}})));
    // Another kind's tags decide as before: the Caspian is a lake, a bay a
    // bay.
    CHECK_EQ(tags_from({{"natural", "water"}, {"water", "lake"}, {"place", "sea"}, {"name", "Caspian Sea"}}).code,
             code::kLake);
    CHECK_EQ(tags_from({{"natural", "bay"}, {"place", "sea"}, {"name", "Bay of Biscay"}}).code, code::kNaturalBay);
}

// geocoder/testdata/water_dir: the client files of a water dir on two days,
// which tools/test_generate_patches.py diffs, patches and verifies with the
// real tools. Day 2 is day 1 with the pond's second vertex moved, on day 1's
// slots. With GEOCODER_WRITE_TESTDATA set this writes them instead.
TEST(the_water_dir_fixture_of_the_patch_tests_is_what_the_layer_writes) {
    const ScratchDir root("water-fixture");
    const std::string one = root.path() + "/one", two = root.path() + "/two";
    write(build(fixture()), one);
    Collected moved = fixture();
    REQUIRE(moved.closed_ways[0].way == 20);
    moved.closed_ways[0].vertices[1].lat_e7 += 300;
    write(build(moved, one), two, one);
    const std::string fixture_dir = std::string(GEOCODER_TESTDATA_DIR) + "/water_dir";
    const bool rewrite = std::getenv("GEOCODER_WRITE_TESTDATA") != nullptr;
    for (const auto& [day, dir] : {std::pair<std::string, std::string>{"day1", one}, {"day2", two}}) {
        const auto written = tree(dir + "/water/all/exact");
        size_t client = 0;
        for (const auto& [name, bytes] : written) {
            if (name.size() > 8 && name.compare(name.size() - 8, 8, ".osm_ids") == 0) continue;
            client++;
            const std::string path = fixture_dir + "/" + day + "/" + name;
            if (rewrite) {
                fs::create_directories(fs::path(path).parent_path());
                std::ofstream(path, std::ios::binary).write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
            }
            CHECK(bytes_of(path) == bytes);
        }
        CHECK_EQ(client, size_t(10));
    }
}
