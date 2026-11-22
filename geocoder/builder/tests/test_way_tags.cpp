// parse_way_tags / way_needs_nodes: the way pass and the node pre-scan must
// agree on exactly which ways get their node coordinates resolved.
#include "way_tags.h"

#include <initializer_list>
#include <string>
#include <utility>
#include <vector>

#include "test_framework.h"

namespace {

// A block string table plus key/value index arrays for one way.
struct TagBlock {
    std::vector<std::string> st{""};
    std::vector<uint32_t> keys, vals;

    explicit TagBlock(std::initializer_list<std::pair<const char*, const char*>> tags) {
        for (const auto& [k, v] : tags) {
            keys.push_back(add(k));
            vals.push_back(add(v));
        }
    }
    uint32_t add(const char* s) {
        st.push_back(s);
        return static_cast<uint32_t>(st.size() - 1);
    }
    WayTags parse() const { return parse_way_tags(keys.data(), vals.data(), keys.size(), st); }
};

bool needs(std::initializer_list<std::pair<const char*, const char*>> tags, bool member = false) {
    TagBlock b(tags);
    return way_needs_nodes(b.parse(), member);
}

}  // namespace

TEST(way_tags_parse_picks_known_keys) {
    TagBlock b({{"highway", "residential"}, {"name", "Main St"}, {"name:en", "Main Street"},
                {"addr:street", "High St"}, {"ISO3166-1:alpha2", "NZ"}, {"surface", "asphalt"}});
    WayTags t = b.parse();
    REQUIRE(t.highway && t.name && t.name_en && t.street && t.iso);
    CHECK_EQ(std::string(t.highway), "residential");
    CHECK_EQ(std::string(t.name), "Main St");
    CHECK_EQ(std::string(t.street), "High St");
    CHECK_EQ(std::string(t.iso), "NZ");
    CHECK(t.tourism == nullptr);
    CHECK(t.landuse == nullptr);
    CHECK(t.emergency == nullptr);

    const WayTags pond = TagBlock({{"natural", "water"}, {"emergency", "fire_water_pond"}, {"ele", "412"}}).parse();
    REQUIRE(pond.emergency && pond.natural && pond.ele);
    CHECK_EQ(std::string(pond.emergency), "fire_water_pond");
    CHECK_EQ(std::string(pond.ele), "412");
}

TEST(way_tags_parse_ignores_out_of_range_indices) {
    TagBlock b({{"name", "Kept"}});
    uint32_t bad = static_cast<uint32_t>(b.st.size() + 5);
    b.keys.push_back(bad);
    b.vals.push_back(1);
    b.keys.push_back(b.add("highway"));
    b.vals.push_back(bad);
    WayTags t = b.parse();
    REQUIRE(t.name != nullptr);
    CHECK_EQ(std::string(t.name), "Kept");
    // A key whose value index is out of range is present with a null value.
    CHECK(t.highway == nullptr);
}

TEST(way_tags_best_name_prefers_non_empty_name_en) {
    CHECK_EQ(std::string(TagBlock({{"name", "a"}, {"name:en", "b"}}).parse().best_name()), "b");
    CHECK_EQ(std::string(TagBlock({{"name", "a"}, {"name:en", ""}}).parse().best_name()), "a");
    CHECK(TagBlock({{"highway", "road"}}).parse().best_name() == nullptr);
}

TEST(way_tags_poi_tags_include_protected_boundaries_only) {
    CHECK(TagBlock({{"boundary", "national_park"}}).parse().has_poi_tags());
    CHECK(TagBlock({{"boundary", "protected_area"}}).parse().has_poi_tags());
    CHECK(!TagBlock({{"boundary", "administrative"}}).parse().has_poi_tags());
    CHECK(TagBlock({{"building", "yes"}}).parse().has_poi_tags());
    CHECK(!TagBlock({{"landuse", "forest"}}).parse().has_poi_tags());
    CHECK(!TagBlock({{"shop", "bakery"}}).parse().has_poi_tags());
    // emergency=* classifies a POI way, as landuse=* and shop=* do, but makes
    // none on its own.
    CHECK(!TagBlock({{"emergency", "fire_water_pond"}}).parse().has_poi_tags());
}

TEST(way_tags_poi_tags_include_a_reservoir_or_basin_beside_a_poi_main_tag) {
    // A reservoir or basin with a POI main tag is a rank-30 POI of that tag
    // (poi_classify.h has_poi_main_tag), whichever key it is: emergency=* and
    // shop=*, which make no POI way alone, make one beside it.
    for (const char* kind : {"reservoir", "basin"}) {
        CHECK(TagBlock({{"landuse", kind}, {"emergency", "fire_water_pond"}}).parse().has_poi_tags());
        CHECK(TagBlock({{"landuse", kind}, {"shop", "bakery"}}).parse().has_poi_tags());
        // A value Nominatim's import drops makes none.
        CHECK(!TagBlock({{"landuse", kind}, {"emergency", "fire_hydrant"}}).parse().has_poi_tags());
        CHECK(!TagBlock({{"landuse", kind}, {"shop", "no"}}).parse().has_poi_tags());
        CHECK(!TagBlock({{"landuse", kind}}).parse().has_poi_tags());
    }
    CHECK(!TagBlock({{"landuse", "forest"}, {"emergency", "fire_water_pond"}}).parse().has_poi_tags());
}

TEST(way_needs_nodes_matches_the_way_pass_rule) {
    CHECK(needs({{"addr:interpolation", "even"}}));
    CHECK(needs({{"addr:housenumber", "12"}}));
    CHECK(needs({{"highway", "residential"}, {"name", "Main St"}}));
    CHECK(needs({{"boundary", "administrative"}}));
    CHECK(needs({{"boundary", "forest_compartment"}}));
    CHECK(needs({{"amenity", "school"}, {"name", "X"}}));
    CHECK(needs({{"waterway", "river"}, {"name", "X"}}));
    CHECK(needs({{"landuse", "basin"}, {"emergency", "fire_water_pond"}, {"name", "X"}}));
    CHECK(needs({}, true));

    CHECK(!needs({}));
    CHECK(!needs({{"highway", "residential"}}));
    // Highways and POI ways qualify on `name`, never on name:en alone.
    CHECK(!needs({{"highway", "residential"}, {"name:en", "Main Street"}}));
    CHECK(!needs({{"amenity", "school"}, {"name:en", "X"}}));
    CHECK(!needs({{"highway", "platform"}, {"name", "Postplatz"}}));
    CHECK(!needs({{"highway", "footway"}, {"footway", "sidewalk"}, {"name", "X"}}));
    CHECK(!needs({{"landuse", "residential"}, {"name", "X"}}));
    CHECK(!needs({{"shop", "bakery"}, {"name", "X"}}));
    CHECK(!needs({{"emergency", "fire_water_pond"}, {"name", "X"}}));
    CHECK(!needs({{"landuse", "basin"}, {"name", "X"}}));
    CHECK(!needs({{"amenity", "school"}}));
}
