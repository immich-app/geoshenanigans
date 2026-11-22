// water_classify.h and water_types.h: the kinds the layer holds, their
// Nominatim class, type and rank, the codes of water=* values outside the
// fixed table, water_meta.json, and the major tier's rule.
#include "water_classify.h"

#include <set>
#include <string>
#include <vector>

#include "test_framework.h"

using namespace water;

namespace {

Class of(const char* natural, const char* water_tag = nullptr, const char* waterway = nullptr,
         const char* landuse = nullptr, const char* place = nullptr) {
    Tags t;
    t.natural = natural;
    t.water = water_tag;
    t.waterway = waterway;
    t.landuse = landuse;
    t.place = place;
    return classify(t);
}

std::string kind_of(const Class& c, const TypeTable& table) {
    const MetaType k = table.kind(c.code ? c.code : table.verbatim_code(c.verbatim));
    return k.cls + "/" + k.type + " " + std::to_string(k.rank);
}

}  // namespace

TEST(each_water_tag_is_the_kind_and_rank_nominatim_gives_it) {
    TypeTable table;
    table.assign({"ganga_river"}, {});
    CHECK(kind_of(of("water", "lake"), table) == "water/lake 20");
    CHECK(kind_of(of("water", "reservoir"), table) == "water/reservoir 20");
    CHECK(kind_of(of("water", "pond"), table) == "water/pond 24");
    CHECK(kind_of(of("water", "wastewater"), table) == "water/wastewater 24");
    CHECK(kind_of(of("water", "lagoon"), table) == "water/lagoon 22");
    CHECK(kind_of(of("water"), table) == "natural/water 22");
    CHECK(kind_of(of("water", "yes"), table) == "natural/water 22");
    CHECK(kind_of(of("water", "intermittent"), table) == "natural/water 22");
    CHECK(kind_of(of("water", "tidal"), table) == "natural/water 22");
    CHECK(kind_of(of("water", "ganga_river"), table) == "water/ganga_river 22");
    CHECK(kind_of(of(nullptr, nullptr, nullptr, "reservoir"), table) == "landuse/reservoir 24");
    CHECK(kind_of(of(nullptr, nullptr, nullptr, "basin"), table) == "landuse/basin 24");
    CHECK(kind_of(of("bay"), table) == "natural/bay 22");
    CHECK(kind_of(of("strait"), table) == "natural/strait 22");
    CHECK(kind_of(of("wetland"), table) == "natural/wetland 22");
    CHECK(kind_of(of("glacier"), table) == "natural/glacier 22");
    CHECK(kind_of(of(nullptr, nullptr, nullptr, nullptr, "sea"), table) == "place/sea 2");
}

TEST(river_and_canal_areas_are_one_class_and_streams_and_ditches_none) {
    TypeTable table;
    const Class river = of("water", "river"), canal = of("water", "canal"), bank = of(nullptr, nullptr, "riverbank");
    CHECK(river.river_area && canal.river_area && bank.river_area);
    CHECK(kind_of(river, table) == "waterway/river 19");
    CHECK(kind_of(bank, table) == "waterway/river 19");
    CHECK(kind_of(canal, table) == "waterway/canal 20");
    for (const char* w : {"stream", "ditch", "drain", "fish_pass"}) CHECK(!of("water", w).any());
    // water=* needs natural=water, as in Nominatim.
    CHECK(!of(nullptr, "lake").any());
    CHECK(!of("coastline").any());
    CHECK(!of(nullptr, nullptr, "river").any());
}

TEST(the_first_matching_row_decides_so_the_caspian_is_a_lake) {
    TypeTable table;
    CHECK(kind_of(of("water", "lake", nullptr, nullptr, "sea"), table) == "water/lake 20");
    CHECK(kind_of(of("water", nullptr, nullptr, "reservoir"), table) == "natural/water 22");
    CHECK(kind_of(of("bay", nullptr, nullptr, nullptr, "sea"), table) == "natural/bay 22");
    CHECK(kind_of(of("wetland", "lake"), table) == "natural/wetland 22");
}

TEST(only_river_and_canal_areas_stay_in_scope_without_a_name) {
    CHECK(in_scope(of("water", "lake"), true));
    CHECK(!in_scope(of("water", "lake"), false));
    CHECK(in_scope(of("water", "river"), false));
    CHECK(in_scope(of("water", "canal"), false));
    CHECK(!in_scope(of("water", "stream"), true));
    CHECK(is_naming_waterway("river") && is_naming_waterway("canal"));
    CHECK(!is_naming_waterway("stream") && !is_naming_waterway("riverbank") && !is_naming_waterway(nullptr));
}

TEST(a_verbatim_type_keeps_the_code_the_previous_build_gave_it_and_a_new_one_takes_a_free_one) {
    TypeTable first;
    first.assign({"ganga_river", "salt_lake"}, {});
    const uint16_t ganga = first.verbatim_code("ganga_river"), salt = first.verbatim_code("salt_lake");
    CHECK(ganga >= kVerbatimBase && salt >= kVerbatimBase && ganga != salt);
    CHECK_EQ(ganga, uint16_t(kVerbatimBase | (value_hash("ganga_river") & 0x7FFF)));
    const std::vector<MetaType> meta = parse_meta_json(meta_json(first, {code::kLake, ganga, salt}));
    REQUIRE(meta.size() == 3u);
    CHECK(meta[0].code == code::kLake && meta[0].cls == "water" && meta[0].type == "lake" && meta[0].rank == 20);

    // The next build: the previous kinds keep their codes even where a new
    // value hashes onto one of them.
    std::vector<MetaType> previous = meta;
    previous.push_back({static_cast<uint16_t>(kVerbatimBase | (value_hash("moor") & 0x7FFF)), "water", "taken", 22});
    TypeTable second;
    second.assign({"ganga_river", "salt_lake", "moor", "taken"}, previous);
    CHECK_EQ(second.verbatim_code("ganga_river"), ganga);
    CHECK_EQ(second.verbatim_code("salt_lake"), salt);
    CHECK_EQ(second.verbatim_code("taken"), previous.back().code);
    CHECK(second.verbatim_code("moor") != previous.back().code);
    CHECK_EQ(second.verbatim_code("moor"), uint16_t(previous.back().code + 1));
    // A previous code of a value gone is free again.
    TypeTable third;
    third.assign({"moor"}, previous);
    CHECK_EQ(third.verbatim_code("moor"), previous.back().code);
    CHECK(gctest::refusal_of<std::logic_error>([&] { third.verbatim_code("ganga_river"); }) != "");
}

TEST(water_meta_json_round_trips_quotes_and_controls_and_refuses_what_it_did_not_write) {
    TypeTable table;
    table.assign({"a \"quoted\" \\ value\x01"}, {});
    const uint16_t c = table.verbatim_code("a \"quoted\" \\ value\x01");
    const std::vector<MetaType> back = parse_meta_json(meta_json(table, {c}));
    REQUIRE(back.size() == 1u);
    CHECK(back[0].type == "a \"quoted\" \\ value\x01");
    CHECK(parse_meta_json(meta_json(table, {})).empty());
    CHECK(parse_meta_json("").empty());
    CHECK(parse_meta_json("{\"types\": [{\"code\": 2}]}").empty());
}

TEST(the_major_tier_takes_100_km2_and_wikidata_rivers_of_5_and_keeps_its_own_at_90_percent) {
    CHECK(water_major_keeps(100, false, false, false));
    CHECK(!water_major_keeps(99.9, false, false, false));
    CHECK(!water_major_keeps(50, false, true, false));
    CHECK(water_major_keeps(5, true, true, false));
    CHECK(!water_major_keeps(4.9, true, true, false));
    CHECK(!water_major_keeps(50, true, false, false));
    CHECK(water_major_keeps(90, false, false, true));
    CHECK(!water_major_keeps(89.9, false, false, true));
    CHECK(water_major_keeps(4.5, true, true, true));
}
