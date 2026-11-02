// write_admin_minimal (cell_index.h) writes the smallest set twice:
// admin-minimal/, read without a POI tier, ships only the core strings its
// records name; admin-minimal-poi/, read beside one, keeps the shared polygon
// slots and strings that the POI tier's parent_poly_ids and names point into.
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

#include "cell_index.h"
#include "parsed_data.h"
#include "polygon_rings.h"
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

template <class Record>
std::vector<Record> records(const std::string& path) {
    const std::vector<char> bytes = file_bytes(path);
    REQUIRE(bytes.size() % sizeof(Record) == 0);
    std::vector<Record> out(bytes.size() / sizeof(Record));
    if (!bytes.empty()) std::memcpy(out.data(), bytes.data(), bytes.size());
    return out;
}

bool same_bytes(const AdminPolygon& a, const AdminPolygon& b) { return std::memcmp(&a, &b, sizeof a) == 0; }

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
    const std::vector<char> core = file_bytes(dir + "/strings_core.bin");
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

// The admin cell index of a dir as (cell, id) pairs in file order.
std::vector<std::pair<uint64_t, uint32_t>> cell_pairs(const std::string& dir) {
    const std::vector<char> cells = file_bytes(dir + "/admin_cells.bin");
    const std::vector<char> entries = file_bytes(dir + "/admin_entries.bin");
    std::vector<std::pair<uint64_t, uint32_t>> out;
    for (size_t row = 0; row + 12 <= cells.size(); row += 12) {
        uint64_t cell;
        uint32_t at;
        uint16_t count;
        std::memcpy(&cell, cells.data() + row, 8);
        std::memcpy(&at, cells.data() + row + 8, 4);
        std::memcpy(&count, entries.data() + at, 2);
        for (uint16_t i = 0; i < count; i++) {
            uint32_t id;
            std::memcpy(&id, entries.data() + at + 2 + 4 * i, 4);
            out.push_back({cell, id});
        }
    }
    return out;
}

// A polygon's packed bytes in a dir: from its vertex_offset to the next
// polygon's, or the file's end.
std::vector<char> polygon_bytes(const std::string& dir, uint32_t slot) {
    const std::vector<AdminPolygon> polygons = records<AdminPolygon>(dir + "/admin_polygons.bin");
    const std::vector<char> vertices = file_bytes(dir + "/admin_vertices.bin");
    REQUIRE(slot < polygons.size() && polygons[slot].vertex_count > 0);
    size_t end = vertices.size();
    for (size_t s = slot + 1; s < polygons.size(); s++)
        if (polygons[s].vertex_count > 0) {
            end = polygons[s].vertex_offset;
            break;
        }
    return {vertices.begin() + polygons[slot].vertex_offset, vertices.begin() + end};
}

uint8_t pt(PlaceType t) { return static_cast<uint8_t>(t); }

// A region: a country with a state and a city it keeps, a borough and a
// quarter it drops, and a polygon the previous build had (a strategy-2
// tombstone); place nodes the city and the borough claim, a suburb, a hamlet,
// a town and a place tombstone; and POIs in the city and the borough, one
// named like a street (a core string no admin-minimal record names).
struct Region {
    ParsedData data;
    TempDir tmp;
    std::string quality_dir, own_dir, poi_dir;

    // The place nodes, in the order they are added.
    enum Places : size_t { HARBOUR_CITY, MILL_VILLAGE, DOCKS_SUBURB, FORD_HAMLET, ATLANTIS_TOWN, PLACE_TOMBSTONE };

    void add_polygon(const char* name, uint8_t level, uint64_t relation, double lat) {
        const std::vector<LatLng> outer = {{lat, 20.0}, {lat, 20.4}, {lat + 0.4, 20.4}, {lat + 0.4, 20.0}, {lat, 20.0}};
        const std::vector<LatLng> ring = encode_rings(outer, {});
        AdminPolygon ap{};
        ap.vertex_offset = static_cast<uint32_t>(data.admin_vertices.size());
        ap.vertex_count = static_cast<uint32_t>(ring.size());
        ap.name_id = data.string_pool.intern(name);
        ap.admin_level = level;
        ap.area = 1;
        for (const auto& [la, lo] : ring) data.admin_vertices.push_back({static_cast<float>(la), static_cast<float>(lo)});
        data.admin_polygons.push_back(ap);
        data.admin_osm_ids.push_back((static_cast<uint64_t>(gc::id_alloc::ObjectType::OSM_RELATION) << 56) | (relation << 16));
    }

    void add_place(const char* name, PlaceType type, uint8_t flags, uint32_t parent) {
        PlaceNode pn{};
        pn.lat = 10.1f;
        pn.lng = 20.1f;
        pn.name_id = name ? data.string_pool.intern(name) : NO_DATA;
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
        add_polygon("Atlantis", 2, 11, 10.0);        // 0, kept
        add_polygon("Old Town", 9, 12, 11.0);        // 1, a borough: dropped
        add_polygon("Harbour", 8, 13, 12.0);         // 2, kept
        data.admin_polygons.push_back(admin_polygon_tombstone());  // 3
        data.admin_osm_ids.push_back(0);
        add_polygon("Quay", 10, 14, 13.0);           // 4, a quarter: dropped
        add_polygon("Northern State", 4, 15, 14.0);  // 5, kept

        add_place("Harbour", PlaceType::CITY, PLACE_FLAG_HIDDEN | PLACE_FLAG_LABEL_OF, 2);
        add_place("Mill", PlaceType::VILLAGE, PLACE_FLAG_HIDDEN | PLACE_FLAG_LABEL_OF, 1);
        add_place("Docks", PlaceType::SUBURB, 0, NO_DATA);
        add_place("Ford", PlaceType::HAMLET, 0, NO_DATA);
        add_place("Atlantis", PlaceType::TOWN, 0, NO_DATA);
        add_place(nullptr, PlaceType::CITY, 0, NO_DATA);

        add_poi("Lighthouse", nullptr, 2);
        add_poi("Market", "Market", 1);

        // As compute_admin_index_cells leaves them: the kept polygons'
        // cells, in canonical order.
        data.admin_minimal_index_cells = {{100, 0}, {100, 2 | INTERIOR_FLAG}, {200, 5}, {200, 0 | INTERIOR_FLAG},
                                          {300, 5 | INTERIOR_FLAG}};

        partition_strings_into_tiers(data);

        quality_dir = (tmp.path / "quality" / "q2.5").string();
        own_dir = (tmp.path / "admin-minimal").string();
        poi_dir = (tmp.path / "admin-minimal-poi").string();
        write_quality_variant(data, tmp.path.string(), quality_dir, 2.5);
        write_admin_minimal(data, tmp.path.string(), "", 2.5);
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
    CHECK(strings_of(file_bytes(r.own_dir + "/strings_core.bin")) ==
          (std::vector<std::string>{"Atlantis", "Ford", "Harbour", "Mill", "Northern State"}));
    const uint32_t size = static_cast<uint32_t>(file_bytes(r.own_dir + "/strings_core.bin").size());
    CHECK(layout_of(r.own_dir) == (std::vector<std::pair<uint32_t, uint32_t>>{
                                  {0, size}, {size, size}, {size, size}, {size, size}, {size, size}}));

    // Every name reads as the shared strings name it: the polygons in their
    // own slots (a fresh build numbers them in id order).
    const std::vector<AdminPolygon> polygons = records<AdminPolygon>(r.own_dir + "/admin_polygons.bin");
    REQUIRE(polygons.size() == 3);
    const uint32_t shared_ids[] = {0, 2, 5};
    for (size_t slot = 0; slot < 3; slot++) {
        const AdminPolygon& source = r.data.admin_polygons[shared_ids[slot]];
        CHECK(name_in(r.own_dir, polygons[slot].name_id) == r.shared_name(source.name_id));
        CHECK_EQ(polygons[slot].admin_level, source.admin_level);
    }
    const std::vector<PlaceNode> places = records<PlaceNode>(r.own_dir + "/place_nodes.bin");
    const size_t kept_places[] = {Region::HARBOUR_CITY, Region::MILL_VILLAGE, Region::FORD_HAMLET,
                                  Region::ATLANTIS_TOWN, Region::PLACE_TOMBSTONE};
    REQUIRE(places.size() == std::size(kept_places));
    for (size_t k = 0; k < places.size(); k++)
        CHECK(name_in(r.own_dir, places[k].name_id) == r.shared_name(r.data.place_nodes[kept_places[k]].name_id));
    CHECK_EQ(places[4].name_id, NO_DATA);
    // The city labels its polygon in admin-minimal's own slot.
    CHECK_EQ(places[0].parent_poly_id, 1u);
    CHECK_EQ(places[0]._pad1, uint8_t(PLACE_FLAG_HIDDEN | PLACE_FLAG_LABEL_OF));
}

TEST(admin_minimal_poi_keeps_the_shared_polygon_slots) {
    const Region r;
    const std::vector<AdminPolygon> shared = records<AdminPolygon>(r.quality_dir + "/admin_polygons.bin");
    const std::vector<AdminPolygon> polygons = records<AdminPolygon>(r.poi_dir + "/admin_polygons.bin");
    REQUIRE(shared.size() == r.data.admin_polygons.size());
    REQUIRE(polygons.size() == shared.size());

    // A kept polygon is the q2.5 dir's at the same slot, but for where its
    // bytes start; a dropped one is the builder's tombstone, which the reader
    // takes for no polygon.
    for (uint32_t slot : {0u, 2u, 5u}) {
        AdminPolygon p = polygons[slot];
        p.vertex_offset = shared[slot].vertex_offset;
        CHECK(same_bytes(p, shared[slot]));
        CHECK(polygon_bytes(r.poi_dir, slot) == polygon_bytes(r.quality_dir, slot));
    }
    for (uint32_t slot : {1u, 3u, 4u}) CHECK(same_bytes(polygons[slot], admin_polygon_tombstone()));

    // So a POI's parent_poly_id names its own polygon: the city, or nothing
    // where admin-minimal drops the borough.
    CHECK(r.shared_name(polygons[r.data.poi_records[0].parent_poly_id].name_id) == "Harbour");
    CHECK_EQ(polygons[r.data.poi_records[1].parent_poly_id].name_id, NO_DATA);

    // Its cell index lists the shared ids.
    CHECK(cell_pairs(r.poi_dir) == (std::vector<std::pair<uint64_t, uint32_t>>{
                                   {100, 0}, {100, 2 | INTERIOR_FLAG}, {200, 5}, {200, 0 | INTERIOR_FLAG},
                                   {300, 5 | INTERIOR_FLAG}}));
}

TEST(admin_minimal_poi_places_name_the_shared_slots_and_strings) {
    const Region r;
    const std::vector<PlaceNode> places = records<PlaceNode>(r.poi_dir + "/place_nodes.bin");
    REQUIRE(places.size() == 5);

    // The city stays the label of its polygon, in the shared slot; the village
    // the dropped borough claimed is a place again, as in admin-minimal/.
    const PlaceNode& city = places[0];
    CHECK_EQ(city.parent_poly_id, 2u);
    CHECK_EQ(city._pad1, uint8_t(PLACE_FLAG_HIDDEN | PLACE_FLAG_LABEL_OF));
    const PlaceNode& village = places[1];
    CHECK_EQ(village.parent_poly_id, NO_DATA);
    CHECK_EQ(village._pad1, uint8_t(0));

    // Names keep their shared offsets, read through the shared core and
    // layout that the POI tier's names extend.
    const size_t kept_places[] = {Region::HARBOUR_CITY, Region::MILL_VILLAGE, Region::FORD_HAMLET,
                                  Region::ATLANTIS_TOWN, Region::PLACE_TOMBSTONE};
    for (size_t k = 0; k < places.size(); k++) CHECK_EQ(places[k].name_id, r.data.place_nodes[kept_places[k]].name_id);
    CHECK(file_bytes(r.poi_dir + "/strings_core.bin") == r.data.strings_tiers[0]);
    std::vector<std::pair<uint32_t, uint32_t>> bases;
    for (size_t t = 0; t < STR_TIER_COUNT; t++)
        bases.push_back({r.data.strings_tier_bases[t], r.data.strings_tier_bases[t + 1]});
    CHECK(layout_of(r.poi_dir) == bases);
    CHECK(r.data.strings_tier_bases[STR_TIER_POI] < r.data.strings_tier_bases[STR_TIER_POI + 1]);
}
