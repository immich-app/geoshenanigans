// water_classify.h and water_types.h: the kinds the layer holds, their
// Nominatim class, type and rank, the codes of water=* values outside the
// fixed table, water_meta.json, and the major tier's rule.
#include "water_classify.h"

#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "patch_format.h"
#include "test_framework.h"

using namespace water;

namespace {

Class of(const char* natural, const char* water_tag = nullptr, const char* waterway = nullptr,
         const char* landuse = nullptr) {
    Tags t;
    t.natural = natural;
    t.water = water_tag;
    t.waterway = waterway;
    t.landuse = landuse;
    return classify(t);
}

// A water_meta.json of `kinds`, laid out as meta_json lays it out.
std::string meta(const std::vector<MetaType>& kinds) {
    std::string out = "{\"types\": [\n";
    for (size_t k = 0; k < kinds.size(); k++) out += (k ? ",\n" : "") + meta_entry(kinds[k]);
    return out + "\n]}\n";
}

std::string kind_of(const Class& c, const TypeTable& table) {
    const MetaType k = table.kind(c.code ? c.code : table.verbatim_code(c.verbatim));
    return k.cls + "/" + k.type + " " + std::to_string(k.rank);
}

}  // namespace

TEST(each_water_tag_is_the_kind_and_rank_nominatim_gives_it) {
    TypeTable table;
    table.assign({{"ganga_river", 1}}, {});
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
    // The marine areas' kinds.
    CHECK(table.kind(code::kPlaceSea).cls == "place" && table.kind(code::kPlaceSea).type == "sea");
    CHECK(table.kind(code::kPlaceOcean).cls == "place" && table.kind(code::kPlaceOcean).type == "ocean");
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

TEST(the_first_matching_row_decides) {
    TypeTable table;
    CHECK(kind_of(of("water", nullptr, nullptr, "reservoir"), table) == "natural/water 22");
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
    first.assign({{"ganga_river", 1}, {"salt_lake", 1}}, {});
    const uint16_t ganga = first.verbatim_code("ganga_river"), salt = first.verbatim_code("salt_lake");
    CHECK(ganga >= kVerbatimBase && salt >= kVerbatimBase && ganga != salt);
    CHECK_EQ(ganga, uint16_t(kVerbatimBase | (value_hash("ganga_river") & 0x7FFF)));
    const std::optional<std::vector<MetaType>> parsed = parse_meta_json(meta_json(first, {code::kLake, ganga, salt}));
    REQUIRE(parsed.has_value() && parsed->size() == 3u);
    const std::vector<MetaType>& meta = *parsed;
    CHECK(meta[0].code == code::kLake && meta[0].cls == "water" && meta[0].type == "lake" && meta[0].rank == 20);

    // The next build: the previous kinds keep their codes even where a new
    // value hashes onto one of them.
    std::vector<MetaType> previous = meta;
    previous.push_back({static_cast<uint16_t>(kVerbatimBase | (value_hash("moor") & 0x7FFF)), "water", "taken", 22});
    TypeTable second;
    second.assign({{"ganga_river", 1}, {"salt_lake", 1}, {"moor", 1}, {"taken", 1}}, previous);
    CHECK_EQ(second.verbatim_code("ganga_river"), ganga);
    CHECK_EQ(second.verbatim_code("salt_lake"), salt);
    CHECK_EQ(second.verbatim_code("taken"), previous.back().code);
    CHECK(second.verbatim_code("moor") != previous.back().code);
    CHECK_EQ(second.verbatim_code("moor"), uint16_t(previous.back().code + 1));
    // A previous code of a value gone is free again.
    TypeTable third;
    third.assign({{"moor", 1}}, previous);
    CHECK_EQ(third.verbatim_code("moor"), previous.back().code);
    CHECK(gctest::refusal_of<std::logic_error>([&] { third.verbatim_code("ganga_river"); }) != "");
}

TEST(water_meta_json_round_trips_quotes_and_controls_and_refuses_what_it_did_not_write) {
    TypeTable table;
    table.assign({{"a \"quoted\" \\ value\x01", 1}}, {});
    const uint16_t c = table.verbatim_code("a \"quoted\" \\ value\x01");
    const std::optional<std::vector<MetaType>> back = parse_meta_json(meta_json(table, {c}));
    REQUIRE(back.has_value() && back->size() == 1u);
    CHECK((*back)[0].type == "a \"quoted\" \\ value\x01");
    CHECK(!parse_meta_json("").has_value());
    CHECK(!parse_meta_json("{\"types\": [{\"code\": 2}]}").has_value());
    // Of a string, the writer's escapes alone: \", \\ and \u00xx below 0x20,
    // and no control byte as it is.
    auto with_type = [](const std::string& type) {
        return "{\"types\": [\n  {\"code\": 32769, \"class\": \"water\", \"type\": \"" + type + "\", \"rank\": 22}\n]}\n";
    };
    CHECK(parse_meta_json(with_type("a\\\"b\\\\c\\u001f")).has_value());
    for (const char* other : {"a\\nb", "a\\/b", "a\\tb", "a\\xb", "a\nb", "a\x01" "b"})
        CHECK(!parse_meta_json(with_type(other)).has_value());
}

TEST(water_meta_json_of_a_number_with_a_leading_zero_is_no_table) {
    // Numbers as meta_json writes them, JSON's: no leading zero, but 0
    // itself.
    auto with = [](const std::string& code, const std::string& rank) {
        return "{\"types\": [\n  {\"code\": " + code + ", \"class\": \"water\", \"type\": \"a\", \"rank\": " + rank +
               "}\n]}\n";
    };
    CHECK(parse_meta_json(with("32769", "22")).has_value());
    CHECK(parse_meta_json(with("1", "0")).has_value());
    for (const auto& [code, rank] : std::vector<std::pair<std::string, std::string>>{
             {"032769", "22"}, {"02", "20"}, {"32769", "022"}, {"1", "00"}, {"1", "020"}})
        CHECK(!parse_meta_json(with(code, rank)).has_value());
}

TEST(water_meta_json_of_a_code_twice_or_out_of_order_or_a_value_twice_is_no_table) {
    // Kinds as meta_json writes them: each code once, ascending, each value
    // outside the fixed table once.
    const MetaType lake{code::kLake, "water", "lake", 20};
    const MetaType a{32769, "water", "a", 22}, b{32770, "water", "b", 22};
    CHECK(parse_meta_json(meta({lake, a, b})).has_value());
    CHECK(!parse_meta_json(meta({lake, a, {32769, "water", "b", 22}})).has_value());
    CHECK(!parse_meta_json(meta({lake, b, a})).has_value());
    CHECK(!parse_meta_json(meta({a, lake})).has_value());
    CHECK(!parse_meta_json(meta({lake, lake})).has_value());
    CHECK(!parse_meta_json(meta({lake, a, {32770, "water", "a", 22}})).has_value());
}

TEST(water_meta_json_of_a_verbatim_kind_not_class_water_of_its_rank_or_of_a_code_past_the_fixed_table_is_no_table) {
    // A verbatim code is the water class's, rank kVerbatimRank; no code
    // lies between the fixed table and kVerbatimBase.
    const MetaType last{kFixedKindCount, kFixedKinds[kFixedKindCount - 1].cls, kFixedKinds[kFixedKindCount - 1].type,
                        kFixedKinds[kFixedKindCount - 1].rank};
    const MetaType a{32769, "water", "a", kVerbatimRank};
    CHECK(parse_meta_json(meta({last, a})).has_value());
    CHECK(parse_meta_json(meta({{kVerbatimBase, "water", "a", kVerbatimRank}})).has_value());
    CHECK(!parse_meta_json(meta({last, {32769, "natural", "a", kVerbatimRank}})).has_value());
    CHECK(!parse_meta_json(meta({last, {32769, "water", "a", 20}})).has_value());
    CHECK(!parse_meta_json(meta({{uint16_t(kFixedKindCount + 1), "water", "a", kVerbatimRank}})).has_value());
    CHECK(!parse_meta_json(meta({{uint16_t(kVerbatimBase - 1), "water", "a", kVerbatimRank}})).has_value());
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
    CHECK(!water_major_keeps(4.49, true, true, true));
}

TEST(the_major_tier_takes_coastal_water_of_10_km2_and_keeps_its_own_at_9) {
    CHECK(water_major_keeps(10, false, false, false, true));
    CHECK(!water_major_keeps(9.9, false, false, false, true));
    CHECK(water_major_keeps(9, false, false, true, true));
    CHECK(!water_major_keeps(8.9, false, false, true, true));
    CHECK(!water_major_keeps(50, false, false, false, false));
    // Bays, straits, coves, fjords, sounds, seas, lagoons and harbours, by
    // their class and type; no other kind.
    std::set<std::string> coastal;
    for (uint16_t code = 1; code <= kFixedKindCount; code++)
        if (is_coastal(code)) coastal.insert(std::string(kFixedKinds[code - 1].cls) + "/" + kFixedKinds[code - 1].type);
    CHECK(coastal == std::set<std::string>({"natural/bay", "natural/strait", "water/bay", "water/cove", "water/fjord",
                                            "water/harbour", "water/lagoon", "water/sea", "water/sound", "water/strait"}));
    CHECK(!is_coastal(kVerbatimBase));
}

TEST(a_riverbank_tagged_natural_water_too_is_a_river_area) {
    // waterway=riverbank comes before a bare natural=water, so that an
    // unnamed one can take its waterway's name; a water=* of its own still
    // decides.
    TypeTable table;
    for (const char* water_tag : {static_cast<const char*>(nullptr), "yes", "intermittent", "tidal"}) {
        const Class bank = of("water", water_tag, "riverbank");
        CHECK(bank.river_area);
        CHECK(kind_of(bank, table) == "waterway/river 19");
        CHECK(in_scope(bank, false));
    }
    CHECK(kind_of(of("water", "lake", "riverbank"), table) == "water/lake 20");
    CHECK(!of("water", "stream", "riverbank").any());
}

TEST(water_meta_json_of_every_kind_stays_within_what_a_patch_carries_inline) {
    // 20,000 water=* values of 255 bytes, which no file a patch carries could
    // list, beside one of many objects and one the previous build had.
    std::map<std::string, size_t> uses;
    for (int k = 0; k < 20000; k++) {
        std::string v = "v" + std::to_string(k);
        v.resize(255, 'x');
        uses[v] = 2;
    }
    uses["zz_of_many"] = 50;
    TypeTable before;
    before.assign({{"zz_from_before", 1}}, {});
    const std::optional<std::vector<MetaType>> previous =
        parse_meta_json(meta_json(before, {before.verbatim_code("zz_from_before")}));
    REQUIRE(previous.has_value());
    uses["zz_from_before"] = 1;
    TypeTable table;
    table.assign(uses, *previous);
    std::set<uint16_t> codes;
    for (uint16_t c = 1; c <= kFixedKindCount; c++) codes.insert(c);
    size_t coded = 0;
    for (const auto& [value, n] : uses) {
        const uint16_t c = table.verbatim_code(value);
        if (c == code::kNaturalWater) continue;
        CHECK(c >= kVerbatimBase);
        codes.insert(c);
        coded++;
    }
    CHECK(meta_json(table, codes).size() <= MAX_INLINE_CLIENT_FILE_BYTES);
    CHECK(coded > 2000 && coded < uses.size());
    CHECK_EQ(coded, table.verbatim_count());
    // The previous build's value keeps its code, and the values of most
    // objects come first; the rest are natural/water.
    CHECK_EQ(table.verbatim_code("zz_from_before"), before.verbatim_code("zz_from_before"));
    CHECK(table.verbatim_code("zz_of_many") >= kVerbatimBase);
    CHECK(table.kind(code::kNaturalWater).cls == "natural" && table.kind(code::kNaturalWater).type == "water");
    // A value it was not given has no code.
    CHECK(gctest::refusal_of<std::logic_error>([&] { table.verbatim_code("never_given"); }) != "");
}

TEST(water_meta_json_of_values_its_escapes_double_stays_within_what_a_patch_carries_inline) {
    // 20,000 water=* values of 255 bytes, all but their first few quotes and
    // backslashes, each of which water_meta.json writes as two: a kind of
    // about 565 bytes written, of about 315 unescaped.
    std::map<std::string, size_t> uses;
    for (int k = 0; k < 20000; k++) {
        std::string v = "v" + std::to_string(k);
        while (v.size() < 255) v += v.size() % 2 ? '"' : '\\';
        uses[v] = 2;
    }
    TypeTable table;
    table.assign(uses, {});
    std::set<uint16_t> codes;
    for (uint16_t c = 1; c <= kFixedKindCount; c++) codes.insert(c);
    size_t generic = 0;
    for (const auto& [value, n] : uses) {
        const uint16_t c = table.verbatim_code(value);
        if (c == code::kNaturalWater) generic++;
        else codes.insert(c);
    }
    const std::string text = meta_json(table, codes);
    CHECK(text.size() <= MAX_INLINE_CLIENT_FILE_BYTES);
    // Read back, each value as it was; the values past the budget are
    // natural/water.
    const std::optional<std::vector<MetaType>> read = parse_meta_json(text);
    REQUIRE(read.has_value());
    CHECK_EQ(read->size(), codes.size());
    for (const MetaType& m : *read)
        if (m.code >= kVerbatimBase) CHECK(uses.count(m.type) && table.verbatim_code(m.type) == m.code);
    CHECK(generic > 0 && generic < uses.size());
    CHECK_EQ(generic, table.generic_count());
}

TEST(water_meta_json_cut_short_or_with_more_after_it_is_no_table_and_one_of_no_kinds_is_one) {
    TypeTable table;
    table.assign({{"salt_pond", 1}}, {});
    const std::string whole = meta_json(table, {code::kLake, table.verbatim_code("salt_pond")});
    REQUIRE(parse_meta_json(whole).has_value());
    CHECK_EQ(parse_meta_json(whole)->size(), size_t(2));
    CHECK(parse_meta_json(whole.substr(0, whole.size() - 1)).has_value());
    for (size_t n : {size_t(0), size_t(1), whole.size() / 2, whole.find(']'), whole.size() - 2})
        CHECK(!parse_meta_json(whole.substr(0, n)).has_value());
    CHECK(!parse_meta_json(whole + "{}").has_value());
    CHECK(!parse_meta_json("{\"types\": [{\"code\": 2}]}").has_value());
    CHECK(!parse_meta_json("{\"types\": [{\"code\": 32769, \"class\": \"water\", \"type\": \"a\\uzz00\", \"rank\": 22}]}")
               .has_value());
    // A table of no kinds is a table.
    const std::optional<std::vector<MetaType>> none = parse_meta_json(meta_json(table, {}));
    REQUIRE(none.has_value());
    CHECK(none->empty());
}
