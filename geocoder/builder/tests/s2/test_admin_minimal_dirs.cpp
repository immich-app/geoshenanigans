// write_admin_minimal (cell_index.h) writes the smallest set twice, both on
// the minimal arc set: admin-minimal/, read without a POI tier, ships only
// the core strings its records name; admin-minimal-poi/, read beside one,
// keeps the shared polygon slots and strings that the POI tier's
// parent_poly_ids and names point into. write_admin_tiers writes the full
// set's boundary tiers, quality/simplified and quality/exact.
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <utility>
#include <vector>

#include <unistd.h>

#include "admin_arcs.h"
#include "admin_rank_config.h"
#include "cell_index.h"
#include "parsed_data.h"
#include "place_files.h"
#include "string_files.h"
#include "test_framework.h"
#include "types.h"

namespace {

// A directory removed with what's in it at the end of the test.
struct TempDir {
    std::filesystem::path path =
        std::filesystem::temp_directory_path() / ("admin-minimal-dirs-test-" + std::to_string(getpid()));
    TempDir() { std::filesystem::create_directories(path); }
    ~TempDir() { std::filesystem::remove_all(path); }
};

std::vector<char> file_bytes(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>()};
}

// A string tier's pool, decoded from its FCC2 file (string_files.h).
std::vector<char> pool_bytes(const std::string& path) {
    const std::vector<char> f = file_bytes(path);
    std::vector<char> pool;
    string_files::decode(reinterpret_cast<const uint8_t*>(f.data()), f.size(),
                         [&](const char* p, size_t n) { pool.insert(pool.end(), p, p + n); });
    return pool;
}

template <class Record>
std::vector<Record> records(const std::string& path) {
    const std::vector<char> bytes = file_bytes(path);
    REQUIRE(bytes.size() % sizeof(Record) == 0);
    std::vector<Record> out(bytes.size() / sizeof(Record));
    if (!bytes.empty()) std::memcpy(out.data(), bytes.data(), bytes.size());
    return out;
}

// A dir's place nodes in their files' order (place_files.h): the visible
// ones by cell, then the hidden ones.
std::vector<PlaceNode> places_of(const std::string& dir) {
    const std::vector<char> c = file_bytes(dir + "/place_cells.bin"), n = file_bytes(dir + "/place_nodes.bin"),
                            h = file_bytes(dir + "/place_hidden.bin");
    auto u8 = [](const std::vector<char>& v) { return reinterpret_cast<const uint8_t*>(v.data()); };
    std::vector<PlaceNode> out;
    place_files::decode(u8(c), c.size(), u8(n), n.size(), u8(h), h.size(),
                        [&](const place_files::LogicalPlace& p) { out.push_back(p.node); });
    return out;
}

bool same_bytes(const AdminPolygonRecord& a, const AdminPolygonRecord& b) { return std::memcmp(&a, &b, sizeof a) == 0; }

// A dir's polygon records.
std::vector<AdminPolygonRecord> polygons_of(const std::string& dir) {
    return records<AdminPolygonRecord>(dir + "/admin_polygons.bin");
}

// The bytes of a record's ring block: to the next record's offset, the last
// one's to the end of admin_rings.bin.
uint32_t block_bytes(const std::vector<AdminPolygonRecord>& polygons, size_t slot, size_t rings_len) {
    const size_t end = slot + 1 < polygons.size() ? polygons[slot + 1].ring_offset : rings_len;
    REQUIRE(end >= polygons[slot].ring_offset);
    return static_cast<uint32_t>(end - polygons[slot].ring_offset);
}

uint8_t level_of(const AdminPolygonRecord& p) { return p.level_override >> 4; }

// The builder's tombstone record, wherever its empty block lies.
AdminPolygonRecord tombstone_record() { return admin_polygon_record(admin_polygon_tombstone(), 0); }

// strings_layout.json's tiers as [start, end).
std::vector<std::pair<uint32_t, uint32_t>> layout_of(const std::string& dir) {
    const std::vector<char> bytes = file_bytes(dir + "/strings_layout.json");
    const std::string json(bytes.begin(), bytes.end());
    std::vector<std::pair<uint32_t, uint32_t>> tiers;
    for (size_t at = json.find("\"start\""); at != std::string::npos; at = json.find("\"start\"", at + 1)) {
        unsigned start = 0, end = 0;
        REQUIRE(std::sscanf(json.c_str() + at, "\"start\": %u, \"end\": %u", &start, &end) == 2);
        tiers.push_back({start, end});
    }
    return tiers;
}

// What the server reads at a name offset of a dir that holds only its core
// strings (StringPool::get): "" for NO_DATA or an offset no tier of the dir
// holds.
std::string name_in(const std::string& dir, uint32_t offset) {
    const std::vector<char> core = pool_bytes(dir + "/strings_core.bin");
    const auto tiers = layout_of(dir);
    REQUIRE(!tiers.empty());
    if (offset == NO_DATA || offset < tiers[0].first || offset >= tiers[0].second) return "";
    REQUIRE(offset - tiers[0].first < core.size());
    return std::string(core.data() + (offset - tiers[0].first));
}

// The strings of a pool, back to back.
std::vector<std::string> strings_of(const std::vector<char>& pool) {
    std::vector<std::string> out;
    for (size_t at = 0; at < pool.size(); at += out.back().size() + 1) out.emplace_back(pool.data() + at);
    return out;
}

// The level-10 cell of number n: the index holds cells of one level.
uint64_t c10(uint64_t n) { return cell_pairs::cell_of(n, 10); }

// A file's bytes behind the size() / at(off, n) interface cell_pairs::Reader reads through.
struct Bytes {
    std::vector<char> data;
    uint64_t size() const { return data.size(); }
    const char* at(uint64_t off, size_t) const { return data.data() + off; }
};

// The admin cell index of a dir as (cell, id) pairs in file order.
std::vector<std::pair<uint64_t, uint32_t>> admin_pairs_of(const std::string& dir) {
    Bytes cells{file_bytes(dir + "/admin_cells.bin")}, entries{file_bytes(dir + "/admin_entries.bin")};
    cell_pairs::Reader reader(cells, entries, cell_pairs::ADMIN);
    std::vector<std::pair<uint64_t, uint32_t>> out;
    uint64_t cell;
    std::vector<uint32_t> ids;
    while (reader.next(cell, ids))
        for (uint32_t id : ids) out.push_back({cell, id});
    return out;
}

// A polygon's ring block in a dir.
std::vector<char> ring_block(const std::string& dir, uint32_t slot) {
    const std::vector<AdminPolygonRecord> polygons = polygons_of(dir);
    const std::vector<char> rings = file_bytes(dir + "/admin_rings.bin");
    REQUIRE(slot < polygons.size());
    const uint32_t bytes = block_bytes(polygons, slot, rings.size());
    REQUIRE(bytes > 0 && size_t(polygons[slot].ring_offset) + bytes <= rings.size());
    return {rings.begin() + polygons[slot].ring_offset, rings.begin() + polygons[slot].ring_offset + bytes};
}

// A record but for where its ring block lies.
AdminPolygonRecord unplaced(AdminPolygonRecord p) {
    p.ring_offset = 0;
    return p;
}

uint8_t pt(PlaceType t) { return static_cast<uint8_t>(t); }

// A region: a country with a state and a city it keeps, a borough, a
// quarter and a postcode area it drops, and a polygon the previous build had
// (a strategy-2 tombstone); place nodes the city and the borough claim, a
// suburb, a hamlet and a town; and POIs in the city and
// the borough, one named like a street (a core string no admin-minimal
// record names).
struct Region {
    ParsedData data;
    TempDir tmp;
    std::string simplified_dir, exact_dir, own_dir, poi_dir;

    // The place nodes, in the order they are added.
    enum Places : size_t { HARBOUR_CITY, MILL_VILLAGE, DOCKS_SUBURB, FORD_HAMLET, ATLANTIS_TOWN };

    // A square 0.4 degrees wide at `lat`, its north edge bowing out.
    void add_polygon(const char* name, uint8_t level, uint64_t relation, double lat) {
        const int32_t south = static_cast<int32_t>(lat * 1e7), side = 4000000;
        data.admin_exact_rings.append({{south, 200000000}, {south, 204000000}, {south + side, 204000000},
                                       {south + side + 130567, 202000000}, {south + side, 200000000},
                                       {south, 200000000}},
                                      {});
        AdminPolygon ap{};
        ap.name_id = data.string_pool.intern(name);
        ap.admin_level = level;
        ap.area = data.admin_exact_rings.area(static_cast<uint32_t>(data.admin_polygons.size()));
        data.admin_polygons.push_back(ap);
        data.admin_osm_ids.push_back((static_cast<uint64_t>(gc::id_alloc::ObjectType::OSM_RELATION) << 56) | (relation << 16));
    }

    void add_place(const char* name, PlaceType type, uint8_t flags, uint32_t parent) {
        PlaceNode pn{};
        pn.lat = 10.1f;
        pn.lng = 20.1f;
        pn.name_id = data.string_pool.intern(name);
        pn.place_type = pt(type);
        pn._pad1 = flags;
        pn.parent_poly_id = parent;
        data.place_nodes.push_back(pn);
    }

    void add_poi(const char* name, const char* street, uint32_t parent) {
        PoiRecord pr{};
        pr.name_id = data.string_pool.intern(name);
        pr.parent_street_id = street ? data.string_pool.intern(street) : NO_DATA;
        pr.parent_postcode_id = NO_DATA;
        pr.parent_poly_id = parent;
        pr.vertex_offset = NO_DATA;
        pr.tier = 1;
        data.poi_records.push_back(pr);
    }

    Region() {
        // Their slots once strategy-2's tombstone is in at 3:
        add_polygon("Atlantis", 2, 11, 10.0);        // 0, kept
        add_polygon("Old Town", 9, 12, 11.0);        // 1, a borough: dropped
        add_polygon("Harbour", 8, 13, 12.0);         // 2, kept
        add_polygon("Quay", 10, 14, 13.0);           // 4, a quarter: dropped
        add_polygon("Northern State", 4, 15, 14.0);  // 5, kept
        add_polygon("1234", 11, 16, 15.0);           // 6, a postcode area: dropped
        std::vector<bool> in_minimal;
        for (const AdminPolygon& p : data.admin_polygons)
            in_minimal.push_back(admin_minimal_keeps(p.admin_level, p.place_type_override));
        data.admin_arcs = encode_admin_arcs(build_admin_arcs(data.admin_exact_rings, in_minimal, "", 1),
                                            data.admin_exact_rings, 1);
        // The tombstone strategy-2 leaves at slot 3.
        data.admin_polygons.insert(data.admin_polygons.begin() + 3, admin_polygon_tombstone());
        data.admin_osm_ids.insert(data.admin_osm_ids.begin() + 3, 0);
        data.admin_arcs.polygons.insert(data.admin_arcs.polygons.begin() + 3, AdminArcPolygon{});

        add_place("Harbour", PlaceType::CITY, PLACE_FLAG_HIDDEN | PLACE_FLAG_LABEL_OF, 2);
        add_place("Mill", PlaceType::VILLAGE, PLACE_FLAG_HIDDEN | PLACE_FLAG_LABEL_OF, 1);
        add_place("Docks", PlaceType::SUBURB, 0, NO_DATA);
        add_place("Ford", PlaceType::HAMLET, 0, NO_DATA);
        add_place("Atlantis", PlaceType::TOWN, 0, NO_DATA);

        add_poi("Lighthouse", nullptr, 2);
        add_poi("Market", "Market", 1);

        // As compute_admin_index_cells leaves them: the kept polygons'
        // cells, in canonical order.
        data.admin_minimal_index_cells = {{c10(100), 0}, {c10(100), 2 | INTERIOR_FLAG}, {c10(200), 5},
                                          {c10(200), 0 | INTERIOR_FLAG}, {c10(300), 5 | INTERIOR_FLAG}};

        partition_strings_into_tiers(data);

        simplified_dir = (tmp.path / "quality" / "simplified").string();
        exact_dir = (tmp.path / "quality" / "exact").string();
        own_dir = (tmp.path / "admin-minimal").string();
        poi_dir = (tmp.path / "admin-minimal-poi").string();
        write_admin_tiers(data, tmp.path.string());
        write_admin_minimal(data, tmp.path.string(), "");
    }

    std::string shared_name(uint32_t offset) const {
        const char* s = data.get_string(offset);
        return s ? s : "";
    }
};

}  // namespace

TEST(admin_minimal_ships_only_the_core_strings_its_records_name) {
    const Region r;

    // Its kept polygons' and place nodes' names, in the shared core's order;
    // not the dropped borough's, quarter's or suburb's, nor the POI's that
    // a street shares.
    CHECK(strings_of(pool_bytes(r.own_dir + "/strings_core.bin")) ==
          (std::vector<std::string>{"Atlantis", "Ford", "Harbour", "Mill", "Northern State"}));
    const uint32_t size = static_cast<uint32_t>(pool_bytes(r.own_dir + "/strings_core.bin").size());
    CHECK(layout_of(r.own_dir) == (std::vector<std::pair<uint32_t, uint32_t>>{
                                  {0, size}, {size, size}, {size, size}, {size, size}, {size, size}}));

    // Every name reads as the shared strings name it: the polygons in their
    // own slots (a fresh build numbers them in id order).
    const std::vector<AdminPolygonRecord> polygons = polygons_of(r.own_dir);
    REQUIRE(polygons.size() == 3);
    const uint32_t shared_ids[] = {0, 2, 5};
    for (size_t slot = 0; slot < 3; slot++) {
        const AdminPolygon& source = r.data.admin_polygons[shared_ids[slot]];
        CHECK(name_in(r.own_dir, polygons[slot].name_id) == r.shared_name(source.name_id));
        CHECK_EQ(level_of(polygons[slot]), source.admin_level);
    }
    // The places, in one cell: the visible ones in their order, then the
    // hidden city.
    const std::vector<PlaceNode> places = places_of(r.own_dir);
    const size_t kept_places[] = {Region::MILL_VILLAGE, Region::FORD_HAMLET, Region::ATLANTIS_TOWN, Region::HARBOUR_CITY};
    REQUIRE(places.size() == std::size(kept_places));
    for (size_t k = 0; k < places.size(); k++)
        CHECK(name_in(r.own_dir, places[k].name_id) == r.shared_name(r.data.place_nodes[kept_places[k]].name_id));
    // The city labels its polygon in admin-minimal's own slot.
    CHECK_EQ(places[3].parent_poly_id, 1u);
    CHECK_EQ(places[3]._pad1, uint8_t(PLACE_FLAG_HIDDEN | PLACE_FLAG_LABEL_OF));
}

TEST(admin_minimal_poi_keeps_the_shared_polygon_slots) {
    const Region r;
    const std::vector<AdminPolygonRecord> shared = polygons_of(r.exact_dir), polygons = polygons_of(r.poi_dir),
                                          own = polygons_of(r.own_dir);
    REQUIRE(shared.size() == r.data.admin_polygons.size());
    REQUIRE(polygons.size() == shared.size());
    REQUIRE(own.size() == 3);

    // A kept polygon is the boundary tiers' at the same slot, but for where
    // its ring block lies, which holds the minimal set's arcs as in
    // admin-minimal/; a dropped one is the builder's tombstone, which the
    // reader takes for no polygon.
    const uint32_t own_slot[] = {0, NO_DATA, 1, NO_DATA, NO_DATA, 2, NO_DATA};
    for (uint32_t slot : {0u, 2u, 5u}) {
        CHECK(same_bytes(unplaced(polygons[slot]), unplaced(shared[slot])));
        CHECK(ring_block(r.poi_dir, slot) == ring_block(r.own_dir, own_slot[slot]));
    }
    for (uint32_t slot : {1u, 3u, 4u, 6u}) {
        CHECK(same_bytes(unplaced(polygons[slot]), tombstone_record()));
        CHECK_EQ(block_bytes(polygons, slot, file_bytes(r.poi_dir + "/admin_rings.bin").size()), 0u);
    }

    // So a POI's parent_poly_id names its own polygon: the city, or nothing
    // where admin-minimal drops the borough.
    CHECK(r.shared_name(polygons[r.data.poi_records[0].parent_poly_id].name_id) == "Harbour");
    CHECK_EQ(polygons[r.data.poi_records[1].parent_poly_id].name_id, NO_DATA);

    // Its cell index lists the shared ids.
    CHECK(admin_pairs_of(r.poi_dir) == (std::vector<std::pair<uint64_t, uint32_t>>{
                                       {c10(100), 0}, {c10(100), 2 | INTERIOR_FLAG}, {c10(200), 5},
                                       {c10(200), 0 | INTERIOR_FLAG}, {c10(300), 5 | INTERIOR_FLAG}}));
}

TEST(admin_minimal_poi_places_name_the_shared_slots_and_strings) {
    const Region r;
    const std::vector<PlaceNode> places = places_of(r.poi_dir);
    REQUIRE(places.size() == 4);

    // The city stays the label of its polygon, in the shared slot; the village
    // the dropped borough claimed is a place again, as in admin-minimal/.
    const PlaceNode& city = places[3];
    CHECK_EQ(city.parent_poly_id, 2u);
    CHECK_EQ(city._pad1, uint8_t(PLACE_FLAG_HIDDEN | PLACE_FLAG_LABEL_OF));
    const PlaceNode& village = places[0];
    CHECK_EQ(village.parent_poly_id, NO_DATA);
    CHECK_EQ(village._pad1, uint8_t(0));

    // Names keep their shared offsets, read through the shared core and
    // layout that the POI tier's names extend.
    const size_t kept_places[] = {Region::MILL_VILLAGE, Region::FORD_HAMLET, Region::ATLANTIS_TOWN, Region::HARBOUR_CITY};
    for (size_t k = 0; k < places.size(); k++) CHECK_EQ(places[k].name_id, r.data.place_nodes[kept_places[k]].name_id);
    CHECK(pool_bytes(r.poi_dir + "/strings_core.bin") == r.data.strings_tiers[0]);
    std::vector<std::pair<uint32_t, uint32_t>> bases;
    for (size_t t = 0; t < STR_TIER_COUNT; t++)
        bases.push_back({r.data.strings_tier_bases[t], r.data.strings_tier_bases[t + 1]});
    CHECK(layout_of(r.poi_dir) == bases);
    CHECK(r.data.strings_tier_bases[STR_TIER_POI] < r.data.strings_tier_bases[STR_TIER_POI + 1]);
}

TEST(both_admin_minimal_dirs_hold_the_same_minimal_arcs) {
    const Region r;
    for (const char* file : {"/arc_index.bin", "/arc_vertices.bin"}) {
        CHECK(!file_bytes(r.own_dir + file).empty());
        CHECK(file_bytes(r.own_dir + file) == file_bytes(r.poi_dir + file));
        // Not the full set's: the minimal set's own arcs.
        CHECK(file_bytes(r.own_dir + file) != file_bytes(r.simplified_dir + file));
    }
    // The minimal set's slot sidecars, in admin-minimal/ alone.
    for (const char* sidecar : {"/arc_index.osm_ids", "/arc_index.fallback.osm_ids"}) {
        CHECK(std::filesystem::exists(r.own_dir + sidecar));
        CHECK(!std::filesystem::exists(r.poi_dir + sidecar));
    }
}

TEST(the_boundary_tiers_share_their_records_and_rings_and_postcode_areas_are_records_there) {
    const Region r;
    for (const char* file : {"/admin_polygons.bin", "/admin_rings.bin"})
        CHECK(file_bytes(r.simplified_dir + file) == file_bytes(r.exact_dir + file));
    // Their arcs differ: every raw vertex in exact, fewer in simplified.
    CHECK(file_bytes(r.exact_dir + "/arc_vertices.bin").size() > file_bytes(r.simplified_dir + "/arc_vertices.bin").size());
    // Every live polygon at its own slot, its area that of its exact rings.
    const std::vector<AdminPolygonRecord> shared = polygons_of(r.exact_dir);
    const size_t rings_len = file_bytes(r.exact_dir + "/admin_rings.bin").size();
    REQUIRE(shared.size() == 7);
    for (uint32_t slot = 0; slot < 7; slot++) {
        if (slot == 3) {
            CHECK(same_bytes(unplaced(shared[slot]), tombstone_record()));
            CHECK_EQ(block_bytes(shared, slot, rings_len), 0u);
            continue;
        }
        CHECK(block_bytes(shared, slot, rings_len) > 0);
        CHECK(shared[slot].area == r.data.admin_polygons[slot].area);
        CHECK(shared[slot].name_id == r.data.admin_polygons[slot].name_id);
    }
    // The postcode area is a level-11 record there; no dir has postal files.
    CHECK_EQ(level_of(shared[6]), uint8_t(11));
    for (const auto& entry : std::filesystem::recursive_directory_iterator(r.tmp.path))
        CHECK(entry.path().filename().string().rfind("postal_", 0) != 0);
    // The slots' sidecars beside the exact tier only.
    for (const char* sidecar : {"/admin_polygons.osm_ids", "/arc_index.osm_ids", "/arc_index.fallback.osm_ids"}) {
        CHECK(std::filesystem::exists(r.exact_dir + sidecar));
        CHECK(!std::filesystem::exists(r.simplified_dir + sidecar));
    }
}
