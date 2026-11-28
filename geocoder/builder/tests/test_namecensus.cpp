#include <array>
#include <fstream>
#include <sstream>

#include "pbf_test_objects.h"
#include "scratch_dir.h"
#include "test_framework.h"
#include "tool_runs.h"

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

    std::ifstream in(log);
    std::string line;
    std::vector<std::string> columns;
    std::map<std::string, std::vector<std::string>> rows;
    while (std::getline(in, line)) {
        const auto sep = line.find('\t');
        const auto type = line.substr(0, sep);
        if (type != "class" && type != "place" && type != "admin" && type != "poi") continue;
        std::istringstream fields(line.substr(sep + 1));
        auto& target = type == "class" ? columns : rows[type];
        for (std::string value; std::getline(fields, value, '\t');) target.push_back(value);
    }
    auto value = [&](const std::string& row, const std::string& column) {
        const auto it = std::find(columns.begin(), columns.end(), column);
        REQUIRE(it != columns.end());
        return rows.at(row).at(it - columns.begin());
    };
    for (const auto* type : {"place", "admin", "poi"}) {
        CHECK_EQ(value(type, "records"), "1");
        CHECK_EQ(value(type, "only_stored_name"), "0");
    }
    for (const auto* family : {"name:<lang>", "alt_name*", "old_name*", "short_name*",
                               "official_name*", "ref family"}) {
        CHECK_EQ(value("place", std::string(family) + ":records"), "1");
    }
    CHECK_EQ(value("admin", "ISO3166-2:records"), "1");
    CHECK_EQ(value("poi", "brand/iata/icao/faa:records"), "1");
    CHECK_EQ(value("place", "brand/iata/icao/faa:records"), "0");
    const auto records = read_file(dir.path() + "/records.bin");
    const std::string payload(records.begin(), records.end());
    CHECK(payload.find("Monks") == std::string::npos);
    CHECK(payload.find("alt_name\x1f" "Monaco\x1e" "name\x1f" "München") != std::string::npos);
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
