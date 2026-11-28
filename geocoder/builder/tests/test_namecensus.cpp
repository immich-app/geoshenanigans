#include <array>
#include <cstring>
#include <fstream>
#include <set>
#include <sstream>
#include <tuple>

#include "pbf_test_objects.h"
#include "scratch_dir.h"
#include "test_framework.h"
#include "tool_runs.h"
#include "types.h"

namespace {

// The census table on stdout, its rows keyed by class and POI tier, 0 for a
// place or an admin area ("place 0", "poi 3").
struct CensusTable {
    std::vector<std::string> columns;
    std::map<std::string, std::vector<std::string>> rows;

    std::string value(const std::string& row, const std::string& column) const {
        const auto it = std::find(columns.begin(), columns.end(), column);
        REQUIRE(it != columns.end());
        REQUIRE(rows.count(row) == 1);
        return rows.at(row).at(it - columns.begin());
    }
};

CensusTable read_census(const std::string& log) {
    CensusTable table;
    std::ifstream in(log);
    for (std::string line; std::getline(in, line);) {
        std::istringstream fields(line);
        std::vector<std::string> values;
        for (std::string value; std::getline(fields, value, '\t');) values.push_back(value);
        if (values.size() < 2) continue;
        if (values[0] == "class") table.columns = values;
        else if (values[0] == "place" || values[0] == "admin" || values[0] == "poi")
            table.rows[values[0] + " " + values[1]] = values;
    }
    return table;
}

struct CensusRecord {
    uint8_t cls = 0, sub = 0;
    char otype = 0;
    int64_t id = 0;
    std::string stored;
};

// records.bin in the layout namecensus.cpp's header gives.
std::vector<CensusRecord> read_records(const std::string& path) {
    const auto bytes = read_file(path);
    std::vector<CensusRecord> records;
    size_t at = 0;
    auto take = [&](void* to, size_t n) {
        REQUIRE(at + n <= bytes.size());
        std::memcpy(to, bytes.data() + at, n);
        at += n;
    };
    while (at < bytes.size()) {
        CensusRecord r;
        uint8_t flags = 0, otype = 0, loc_kind = 0;
        int64_t loc_ref = 0;
        double lat = 0, lon = 0;
        uint16_t stored_len = 0;
        uint32_t bundle_len = 0;
        take(&r.cls, 1);
        take(&r.sub, 1);
        take(&flags, 1);
        take(&otype, 1);
        r.otype = static_cast<char>(otype);
        take(&r.id, sizeof r.id);
        take(&loc_kind, 1);
        take(&loc_ref, sizeof loc_ref);
        take(&lat, sizeof lat);
        take(&lon, sizeof lon);
        take(&stored_len, sizeof stored_len);
        r.stored.resize(stored_len);
        take(r.stored.data(), stored_len);
        take(&bundle_len, sizeof bundle_len);
        REQUIRE(at + bundle_len <= bytes.size());
        at += bundle_len;
        records.push_back(r);
    }
    return records;
}

}  // namespace

TEST(namecensus_counts_name_families_by_record_class) {
    ScratchDir dir("namecensus");
    const auto pbf = dir.path() + "/names.osm.pbf";
    const auto log = dir.path() + "/census.log";

    TestObject city;
    city.id = 1;
    city.lat = city.lon = 0;
    city.tags = {{"place", "city"}, {"name:en", "Munich"}, {"name:de", "München"},
                 {"name", "München"}, {"alt_name", "Monaco"}, {"old_name", "Munichen"},
                 {"short_name", "MUC"}, {"official_name", "Landeshauptstadt München"},
                 {"ref", "M"}, {"name:etymology", "Monks"}, {"brand", "Excluded"}};
    TestObject cafe = city;
    cafe.id = 2;
    cafe.tags = {{"amenity", "cafe"}, {"name", "Coffee"}, {"brand", "Chain"}};
    TestObject way;
    way.type = OsmType::Way;
    way.id = 1;
    way.refs = {city.id, cafe.id, city.id, city.id};
    TestObject region;
    region.type = OsmType::Relation;
    region.id = 1;
    region.members = {{OsmType::Way, way.id, "outer"}};
    region.tags = {{"type", "boundary"}, {"boundary", "administrative"}, {"admin_level", "4"},
                   {"name", "Bayern"}, {"ISO3166-2", "DE-BY"}};
    write_test_pbf(pbf, {city, cafe, way, region}, 1);
    REQUIRE(gctest::run({GEOCODER_NAMECENSUS_BIN, pbf, dir.path(), "2"}, log));

    const auto table = read_census(log);
    CHECK_EQ(table.rows.size(), 3u);
    for (const auto* row : {"place 0", "admin 0", "poi 3"}) {
        CHECK_EQ(table.value(row, "records"), "1");
        CHECK_EQ(table.value(row, "only_stored_name"), "0");
    }
    for (const auto* family : {"name:<lang>", "alt_name*", "old_name*", "short_name*",
                               "official_name*", "ref family"}) {
        CHECK_EQ(table.value("place 0", std::string(family) + ":records"), "1");
    }
    CHECK_EQ(table.value("admin 0", "ISO3166-2:records"), "1");
    CHECK_EQ(table.value("poi 3", "brand/iata/icao/faa:records"), "1");
    CHECK_EQ(table.value("place 0", "brand/iata/icao/faa:records"), "0");
    const auto records = read_file(dir.path() + "/records.bin");
    const std::string payload(records.begin(), records.end());
    CHECK(payload.find("Monks") == std::string::npos);
    CHECK(payload.find("alt_name\x1f" "Monaco\x1e" "name\x1f" "München") != std::string::npos);
}

TEST(namecensus_classifies_closed_ways_area_relations_and_unnamed_places) {
    ScratchDir dir("namecensus");
    const auto pbf = dir.path() + "/areas.osm.pbf";
    const auto log = dir.path() + "/census.log";

    TestObject quarter;
    quarter.id = 1;
    quarter.lat = quarter.lon = 0;
    quarter.tags = {{"place", "quarter"}, {"ref", "18"}};
    TestObject village = quarter;
    village.id = 2;
    village.tags = {{"place", "village"}, {"name", "Village"}};
    auto ring = [&](int64_t id, std::vector<std::pair<std::string, std::string>> tags) {
        TestObject way;
        way.type = OsmType::Way;
        way.id = id;
        way.refs = {quarter.id, village.id, quarter.id, quarter.id};
        way.tags = std::move(tags);
        return way;
    };
    const TestObject county = ring(10, {{"boundary", "administrative"}, {"admin_level", "6"}, {"name", "County"}});
    const TestObject country = ring(11, {{"boundary", "administrative"}, {"admin_level", "2"},
                                         {"ISO3166-1:alpha2", "XX"}, {"name", "Country"}});
    const TestObject town = ring(12, {{"place", "city"}, {"name", "Town"}});
    const TestObject cafe = ring(13, {{"amenity", "cafe"}, {"name", "Cafe"}});
    auto area = [&](int64_t id, std::vector<std::pair<std::string, std::string>> tags) {
        TestObject relation;
        relation.type = OsmType::Relation;
        relation.id = id;
        relation.members = {{OsmType::Way, county.id, "outer"}};
        relation.tags = std::move(tags);
        return relation;
    };
    const TestObject postal = area(1, {{"type", "boundary"}, {"boundary", "postal_code"}, {"postal_code", "12345"}});
    const TestObject park = area(2, {{"type", "multipolygon"}, {"leisure", "park"}, {"name", "Park"},
                                     {"wikidata", "Q1"}});
    write_test_pbf(pbf, {quarter, village, county, country, town, cafe, postal, park}, 1);
    REQUIRE(gctest::run({GEOCODER_NAMECENSUS_BIN, pbf, dir.path(), "2"}, log));

    using Kind = std::tuple<int, int, char, int64_t>;  // class, sub, osm type, osm id
    std::set<Kind> kinds;
    std::string quarter_name;
    for (const auto& r : read_records(dir.path() + "/records.bin")) {
        kinds.insert({r.cls, r.sub, r.otype, r.id});
        if (r.otype == 'n' && r.id == quarter.id) quarter_name = r.stored;
    }
    const std::set<Kind> expected = {
        {0, int(PlaceType::QUARTER), 'n', quarter.id}, {0, int(PlaceType::VILLAGE), 'n', village.id},
        {1, 6, 'w', county.id}, {1, 2, 'w', country.id}, {1, 15, 'w', town.id},
        {1, POSTAL_AREA_LEVEL, 'r', postal.id},
        // A park is notable; its wikidata makes it major.
        {2, 3, 'w', cafe.id}, {2, 1, 'r', park.id}};
    CHECK(kinds == expected);
    CHECK_EQ(quarter_name, "18");

    const auto table = read_census(log);
    CHECK_EQ(table.rows.size(), 4u);
    CHECK_EQ(table.value("place 0", "records"), "2");
    CHECK_EQ(table.value("place 0", "only_stored_name"), "1");
    CHECK_EQ(table.value("place 0", "ref family:records"), "1");
    CHECK_EQ(table.value("admin 0", "records"), "4");
    CHECK_EQ(table.value("poi 1", "records"), "1");
    CHECK_EQ(table.value("poi 3", "records"), "1");

    const auto way_nodes = read_file(dir.path() + "/way_nodes.bin");
    std::array<int64_t, 2> refs;
    REQUIRE(way_nodes.size() == sizeof(refs));
    std::memcpy(refs.data(), way_nodes.data(), sizeof(refs));
    CHECK_EQ(refs[0], county.id);
    CHECK_EQ(refs[1], quarter.id);
}

TEST(namecensus_resolves_location_references_above_the_planet_snapshot_ids) {
    ScratchDir dir("namecensus");
    const auto pbf = dir.path() + "/high-ids.osm.pbf";
    const auto log = dir.path() + "/census.log";
    TestObject node;
    node.id = 16'000'000'001LL;
    node.lat = node.lon = 0;
    TestObject way;
    way.type = OsmType::Way;
    way.id = 2'000'000'001LL;
    way.refs = {node.id, node.id, node.id, node.id};
    TestObject boundary;
    boundary.type = OsmType::Relation;
    boundary.id = 1;
    boundary.tags = {{"type", "boundary"}, {"boundary", "administrative"}, {"admin_level", "4"},
                     {"name", "Test boundary"}};
    boundary.members = {{OsmType::Way, way.id, "outer"}};
    write_test_pbf(pbf, {node, way, boundary}, 1);
    REQUIRE(gctest::run({GEOCODER_NAMECENSUS_BIN, pbf, dir.path(), "2"}, log));

    const auto way_nodes = read_file(dir.path() + "/way_nodes.bin");
    std::array<int64_t, 2> refs;
    REQUIRE(way_nodes.size() == sizeof(refs));
    std::memcpy(refs.data(), way_nodes.data(), sizeof(refs));
    CHECK_EQ(refs[0], way.id);
    CHECK_EQ(refs[1], node.id);
    const auto locations = read_file(dir.path() + "/node_locs.bin");
    REQUIRE(locations.size() == sizeof(int64_t) + 2 * sizeof(double));
    int64_t node_ref = 0;
    std::memcpy(&node_ref, locations.data(), sizeof(node_ref));
    CHECK_EQ(node_ref, node.id);
}

TEST(namecensus_refuses_an_invalid_thread_count) {
    ScratchDir dir("namecensus");
    const auto log = dir.path() + "/census.log";
    CHECK(!gctest::run({GEOCODER_NAMECENSUS_BIN, "missing.pbf", dir.path(), "-1"}, log));
    const auto output = read_file(log);
    CHECK(std::string(output.begin(), output.end()).find("thread count must be a positive integer") !=
          std::string::npos);
}

#ifdef __linux__
TEST(namecensus_refuses_a_failed_output_write) {
    ScratchDir dir("namecensus");
    const auto pbf = dir.path() + "/empty.osm.pbf";
    const auto log = dir.path() + "/census.log";
    TestObject city;
    city.id = 1;
    city.lat = city.lon = 0;
    city.tags = {{"place", "city"}, {"name", "Town"}};
    write_test_pbf(pbf, {city}, 1);
    std::filesystem::create_symlink("/dev/full", dir.path() + "/records.bin");
    CHECK(!gctest::run({GEOCODER_NAMECENSUS_BIN, pbf, dir.path(), "1"}, log));
}
#endif
