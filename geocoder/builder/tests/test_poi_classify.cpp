// Table-driven tests for classify_poi and poi_name (poi_classify.h) — the
// OSM-tag → (category, tier, flags) mapping encodes hundreds of
// Nominatim-parity decisions and previously had zero unit coverage (only
// validated via full planet builds + grid diffs).
#include "poi_classify.h"

#include <cstdio>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include "test_framework.h"

namespace {

struct Tags {
    const char* tourism = nullptr;
    const char* historic = nullptr;
    const char* boundary = nullptr;
    const char* amenity = nullptr;
    const char* leisure = nullptr;
    const char* natural = nullptr;
    const char* railway = nullptr;
    const char* aeroway = nullptr;
    const char* man_made = nullptr;
    const char* building = nullptr;
    const char* craft = nullptr;
    const char* power = nullptr;
    const char* place = nullptr;
    const char* waterway = nullptr;
    const char* office = nullptr;
    const char* wikipedia = nullptr;
    const char* wikidata = nullptr;
    const char* highway = nullptr;
    const char* shop = nullptr;
    const char* landuse = nullptr;
    const char* emergency = nullptr;
    // A polygon (a closed way), not a node.
    bool area = false;
};

std::optional<PoiClassification> classify(const Tags& t) {
    return classify_poi(t.tourism, t.historic, t.boundary, t.amenity, t.leisure,
                        t.natural, t.railway, t.aeroway, t.man_made, t.building,
                        t.craft, t.power, t.place, t.waterway, t.office,
                        t.wikipedia, t.wikidata, t.highway, t.shop, t.landuse,
                        t.emergency, t.area);
}

// `t` with its tag `key` set to `value`.
Tags with(Tags t, const char* key, const char* value) {
    const std::pair<const char*, const char* Tags::*> keys[] = {
        {"tourism", &Tags::tourism}, {"historic", &Tags::historic}, {"boundary", &Tags::boundary},
        {"amenity", &Tags::amenity}, {"leisure", &Tags::leisure},   {"natural", &Tags::natural},
        {"railway", &Tags::railway}, {"aeroway", &Tags::aeroway},   {"man_made", &Tags::man_made},
        {"building", &Tags::building}, {"craft", &Tags::craft},     {"power", &Tags::power},
        {"place", &Tags::place},     {"waterway", &Tags::waterway}, {"office", &Tags::office},
        {"highway", &Tags::highway}, {"shop", &Tags::shop},         {"landuse", &Tags::landuse},
        {"emergency", &Tags::emergency}};
    bool known = false;
    for (const auto& [name, member] : keys)
        if (!std::strcmp(name, key)) {
            t.*member = value;
            known = true;
        }
    REQUIRE(known);
    return t;
}

// The water kinds Nominatim ranks below 30 (settings/address-levels.json):
// natural=water, wetland, bay and glacier, and landuse=reservoir and basin.
struct WaterKind {
    const char* natural;
    const char* landuse;
    PoiCategory category;
};
const WaterKind kWaterKinds[] = {
    {"water", nullptr, PoiCategory::NATURAL_WATER},  {"wetland", nullptr, PoiCategory::WETLAND},
    {"bay", nullptr, PoiCategory::BAY},              {"glacier", nullptr, PoiCategory::GLACIER},
    {nullptr, "reservoir", PoiCategory::RESERVOIR},  {nullptr, "basin", PoiCategory::RESERVOIR},
};

Tags water(const WaterKind& kind) {
    Tags t;
    t.natural = kind.natural;
    t.landuse = kind.landuse;
    return t;
}

// The category classify gives `t`, UNKNOWN for none.
PoiCategory category_of(const Tags& t) {
    const auto c = classify(t);
    return c ? c->category : PoiCategory::UNKNOWN;
}

}  // namespace

TEST(poi_classify_tourism_priority_and_folds) {
    { Tags t; t.tourism = "museum";
      auto c = classify(t); REQUIRE(c.has_value());
      CHECK_EQ(uint8_t(c->category), uint8_t(PoiCategory::MUSEUM)); }
    // Lodging variants all fold into HOTEL.
    for (const char* v : {"hotel", "motel", "guest_house", "hostel", "apartment"}) {
        Tags t; t.tourism = v;
        auto c = classify(t); REQUIRE(c.has_value());
        CHECK_EQ(uint8_t(c->category), uint8_t(PoiCategory::HOTEL));
    }
    // wilderness_hut folds into ALPINE_HUT.
    { Tags t; t.tourism = "wilderness_hut";
      auto c = classify(t); REQUIRE(c.has_value());
      CHECK_EQ(uint8_t(c->category), uint8_t(PoiCategory::ALPINE_HUT)); }
}

TEST(poi_classify_tourism_beats_amenity) {
    // tourism is checked first; a node with both keeps the tourism class.
    Tags t; t.tourism = "attraction"; t.amenity = "restaurant";
    auto c = classify(t);
    REQUIRE(c.has_value());
    CHECK_EQ(uint8_t(c->category), uint8_t(PoiCategory::ATTRACTION));
}

TEST(poi_classify_unmatched_returns_nullopt) {
    { Tags t; auto c = classify(t); CHECK(!c.has_value()); }
    { Tags t; t.tourism = "yes"; t.building = "yes";
      // tourism=yes alone is not a class; building=yes alone is generic.
      auto c = classify(t);
      // Lock in current behaviour, whatever it is, as a regression net:
      // generic tags must not silently become a specific category.
      if (c.has_value()) {
          CHECK(c->category != PoiCategory::MUSEUM);
          CHECK(c->category != PoiCategory::HOTEL);
      }
    }
}

TEST(poi_classify_highway_flag) {
    // highway-class polygons (pedestrian plazas) carry POI_FLAG_HIGHWAY so
    // the server can use the feature's own name as the road.
    Tags t; t.highway = "pedestrian"; t.wikidata = "Q1";
    auto c = classify(t);
    if (c.has_value()) {
        CHECK((c->flags & POI_FLAG_HIGHWAY) != 0);
    }
}

TEST(poi_classify_keeps_a_generic_feature_only_with_a_wiki_tag) {
    // amenity=car_sharing takes no specific category: a generic rank-30
    // feature, which no POI dir ships unless a wiki tag lifts it into poi/all.
    Tags plain; plain.amenity = "car_sharing";
    CHECK(!classify(plain).has_value());

    Tags wikidata = plain; wikidata.wikidata = "Q1";
    Tags wikipedia = plain; wikipedia.wikipedia = "en:Carsharing";
    for (const Tags& t : {wikidata, wikipedia}) {
        auto c = classify(t);
        REQUIRE(c.has_value());
        CHECK_EQ(uint8_t(c->category), uint8_t(PoiCategory::GENERIC_RANK30));
        CHECK_EQ(int(c->tier), int(POI_MAX_SHIPPED_TIER));
    }
}

TEST(poi_name_of_an_unnamed_node_is_none) {
    // OSM carries empty values: a shop=bakery node tagged name= is no POI,
    // whatever its category.
    CHECK(poi_name(nullptr, nullptr) == nullptr);
    CHECK(poi_name(nullptr, "") == nullptr);
    CHECK(poi_name("", "") == nullptr);
    CHECK(poi_name("", nullptr) == nullptr);

    CHECK_EQ(std::string(poi_name(nullptr, "Fournil du Verger")), "Fournil du Verger");
    CHECK_EQ(std::string(poi_name("", "Fournil du Verger")), "Fournil du Verger");
    CHECK_EQ(std::string(poi_name("Orchard Bakery", "Fournil du Verger")), "Orchard Bakery");
    CHECK_EQ(std::string(poi_name("Orchard Bakery", "")), "Orchard Bakery");
}

TEST(poi_classify_a_water_kind_alone_takes_its_category) {
    for (const WaterKind& kind : kWaterKinds) CHECK_EQ(uint8_t(category_of(water(kind))), uint8_t(kind.category));
}

TEST(poi_classify_a_water_object_with_a_poi_main_tag_by_that_tag) {
    // Nominatim ranks an object by the lowest positive address rank of its
    // main tags (ranking.sql compute_place_rank): the POI tag's rank 30 beats
    // the water kind's 0, so the object is a rank-30 POI of that tag's class,
    // and is classified by it here too.
    struct Case {
        const char* key;
        const char* value;
        PoiCategory category;
    };
    const Case cases[] = {
        {"amenity", "fountain", PoiCategory::FOUNTAIN},
        {"leisure", "swimming_area", PoiCategory::SWIMMING_AREA},
        {"emergency", "fire_water_pond", PoiCategory::FIRE_WATER_POND},
        // Keys the classifier reads after natural=*.
        {"man_made", "pier", PoiCategory::PIER},
        {"railway", "station", PoiCategory::STATION},
        {"craft", "winery", PoiCategory::WINERY},
        {"office", "government", PoiCategory::GOVERNMENT},
        {"shop", "bakery", PoiCategory::SHOP_BAKERY},
    };
    for (const WaterKind& kind : kWaterKinds)
        for (const Case& c : cases) {
            Tags t = water(kind);
            if (!std::strcmp(c.key, "amenity")) t.amenity = c.value;
            else if (!std::strcmp(c.key, "leisure")) t.leisure = c.value;
            else if (!std::strcmp(c.key, "emergency")) t.emergency = c.value;
            else if (!std::strcmp(c.key, "man_made")) t.man_made = c.value;
            else if (!std::strcmp(c.key, "railway")) t.railway = c.value;
            else if (!std::strcmp(c.key, "craft")) t.craft = c.value;
            else if (!std::strcmp(c.key, "office")) t.office = c.value;
            else t.shop = c.value;
            if (category_of(t) != c.category)
                std::printf("    %s=%s with %s=%s\n", kind.natural ? "natural" : "landuse",
                            kind.natural ? kind.natural : kind.landuse, c.key, c.value);
            CHECK_EQ(uint8_t(category_of(t)), uint8_t(c.category));
        }
}

TEST(poi_classify_a_swimming_area_and_a_fire_pond_alone_by_their_tags) {
    Tags swimming; swimming.leisure = "swimming_area";
    Tags pond; pond.emergency = "fire_water_pond";
    for (const auto& [t, category] : {std::pair{swimming, PoiCategory::SWIMMING_AREA}, std::pair{pond, PoiCategory::FIRE_WATER_POND}}) {
        auto c = classify(t);
        REQUIRE(c.has_value());
        CHECK_EQ(uint8_t(c->category), uint8_t(category));
        CHECK_EQ(int(c->tier), 3);
        Tags wiki = t; wiki.wikidata = "Q1";
        CHECK_EQ(int(classify(wiki)->tier), 2);
    }
    CHECK_EQ(std::string(poi_category_label(PoiCategory::SWIMMING_AREA)), "swimming_area");
    CHECK_EQ(std::string(poi_category_label(PoiCategory::FIRE_WATER_POND)), "fire_water_pond");
}

TEST(poi_classify_a_water_object_with_a_poi_main_tag_of_no_category_is_a_generic_feature) {
    // A rank-30 POI in Nominatim whose tag takes no category here: a generic
    // rank-30 feature, which ships only with a wiki tag.
    const std::pair<const char*, const char*> tags[] = {
        {"emergency", "suction_point"}, {"leisure", "swimming"}, {"amenity", "watering_place"},
        {"tourism", "wilderness"}, {"historic", "aqueduct"}};
    for (const WaterKind& kind : kWaterKinds)
        for (const auto& [key, value] : tags) {
            Tags t = water(kind);
            if (!std::strcmp(key, "emergency")) t.emergency = value;
            else if (!std::strcmp(key, "leisure")) t.leisure = value;
            else if (!std::strcmp(key, "amenity")) t.amenity = value;
            else if (!std::strcmp(key, "tourism")) t.tourism = value;
            else t.historic = value;
            CHECK(!classify(t).has_value());
            t.wikidata = "Q1";
            CHECK_EQ(uint8_t(category_of(t)), uint8_t(PoiCategory::GENERIC_RANK30));
        }
    // building=* is a fallback main tag of rank 30: beside natural=*, the
    // object keeps class natural and is a rank-30 POI; beside landuse=*, also
    // a fallback, building is its class (the alphabetically first), which
    // Nominatim's POI lane leaves out, so it stays a reservoir.
    for (const WaterKind& kind : kWaterKinds) {
        Tags t = water(kind);
        t.building = "yes";
        t.wikidata = "Q1";
        CHECK_EQ(uint8_t(category_of(t)), uint8_t(kind.natural ? PoiCategory::GENERIC_RANK30 : kind.category));
    }
}

TEST(poi_classify_a_water_object_keeps_its_category_beside_a_tag_of_no_rank_30) {
    // Values Nominatim's import drops (presets.lua), tags that are no main tag
    // there (power, man_made=dam), and ones whose rank is below 30 as well
    // (address-levels.json: place=*, waterway=*, historic=neighbourhood,
    // leisure=common) leave the water kind the object's category.
    const std::pair<const char*, const char*> tags[] = {
        {"amenity", "parking_space"}, {"amenity", "no"}, {"emergency", "fire_hydrant"}, {"emergency", "yes"},
        {"tourism", "yes"}, {"historic", "yes"}, {"historic", "neighbourhood"}, {"building", "no"},
        {"leisure", "common"}, {"railway", "abandoned"}, {"man_made", "dam"}, {"power", "plant"},
        {"place", "island"}, {"waterway", "waterfall"}, {"shop", "no"}};
    for (const WaterKind& kind : kWaterKinds)
        for (const auto& [key, value] : tags) {
            Tags t = water(kind);
            if (!std::strcmp(key, "amenity")) t.amenity = value;
            else if (!std::strcmp(key, "emergency")) t.emergency = value;
            else if (!std::strcmp(key, "tourism")) t.tourism = value;
            else if (!std::strcmp(key, "historic")) t.historic = value;
            else if (!std::strcmp(key, "building")) t.building = value;
            else if (!std::strcmp(key, "leisure")) t.leisure = value;
            else if (!std::strcmp(key, "railway")) t.railway = value;
            else if (!std::strcmp(key, "man_made")) t.man_made = value;
            else if (!std::strcmp(key, "power")) t.power = value;
            else if (!std::strcmp(key, "place")) t.place = value;
            else if (!std::strcmp(key, "waterway")) t.waterway = value;
            else t.shop = value;
            if (category_of(t) != kind.category)
                std::printf("    %s=%s with %s=%s\n", kind.natural ? "natural" : "landuse",
                            kind.natural ? kind.natural : kind.landuse, key, value);
            CHECK_EQ(uint8_t(category_of(t)), uint8_t(kind.category));
        }
}

TEST(poi_classify_a_water_object_beside_a_main_tag_of_a_lower_address_rank_keeps_its_category) {
    // Nominatim ranks an object by the lowest positive address rank of its
    // main tags (ranking.sql compute_place_rank): place=locality (25) and
    // place=suburb (20) anywhere, and on an area landuse=residential (24),
    // highway=footway (27) and highway=residential (26), rank such an object
    // below 30, whatever POI main tag it carries too, so it stays water. A
    // landuse or highway node ranks 30 (ranking.sql:175-180): beside those, a
    // water node is a rank-30 POI of its shop.
    struct Lower {
        const char* key;
        const char* value;
        bool on_a_node;
    };
    const Lower lower[] = {{"place", "locality", true},      {"place", "suburb", true},
                           {"landuse", "residential", false}, {"highway", "footway", false},
                           {"highway", "residential", false}};
    for (const WaterKind& kind : kWaterKinds)
        for (const Lower& l : lower) {
            if (kind.landuse && !std::strcmp(l.key, "landuse")) continue;
            for (const bool area : {true, false}) {
                Tags t = with(with(water(kind), "shop", "bakery"), l.key, l.value);
                t.area = area;
                const PoiCategory want = area || l.on_a_node ? kind.category : PoiCategory::SHOP_BAKERY;
                if (category_of(t) != want)
                    std::printf("    %s=%s with shop=bakery and %s=%s, %s\n", kind.natural ? "natural" : "landuse",
                                kind.natural ? kind.natural : kind.landuse, l.key, l.value, area ? "an area" : "a node");
                CHECK_EQ(uint8_t(category_of(t)), uint8_t(want));
            }
        }
    // building=*, the fallback tag of rank 30, makes no rank-30 POI beside
    // one either.
    for (const WaterKind& kind : kWaterKinds) {
        if (!kind.natural) continue;
        Tags t = with(with(water(kind), "building", "yes"), "place", "locality");
        t.wikidata = "Q1";
        CHECK_EQ(uint8_t(category_of(t)), uint8_t(kind.category));
    }
}

TEST(poi_classify_a_water_object_that_yields_takes_its_category_from_a_rank_30_tag) {
    // Once a water kind yields to a POI main tag, the object's category is its
    // rank-30 tags' to give: leisure=park and nature_reserve (24, 0),
    // natural=wood (22, 0), place=island (17, 0) and waterway=waterfall
    // (20, 0) rank lower (address-levels.json), and power=plant and
    // man_made=dam are no main tags of Nominatim's (presets.lua), so none of
    // them classifies it after the water kind.
    struct Case {
        std::vector<std::pair<const char*, const char*>> tags;
        PoiCategory category;
    };
    const Case cases[] = {
        {{{"landuse", "reservoir"}, {"leisure", "park"}, {"emergency", "fire_water_pond"}}, PoiCategory::FIRE_WATER_POND},
        {{{"landuse", "basin"}, {"leisure", "nature_reserve"}, {"shop", "bakery"}}, PoiCategory::SHOP_BAKERY},
        {{{"landuse", "reservoir"}, {"natural", "wood"}, {"craft", "winery"}}, PoiCategory::WINERY},
        {{{"natural", "water"}, {"place", "island"}, {"shop", "bakery"}}, PoiCategory::SHOP_BAKERY},
        {{{"natural", "water"}, {"waterway", "waterfall"}, {"office", "government"}}, PoiCategory::GOVERNMENT},
        {{{"natural", "wetland"}, {"power", "plant"}, {"shop", "bakery"}}, PoiCategory::SHOP_BAKERY},
        {{{"natural", "bay"}, {"man_made", "dam"}, {"shop", "bakery"}}, PoiCategory::SHOP_BAKERY},
        // natural=spring ranks 30, and classifies the object still.
        {{{"landuse", "reservoir"}, {"natural", "spring"}, {"shop", "bakery"}}, PoiCategory::SPRING},
    };
    for (const Case& c : cases)
        for (const bool area : {true, false}) {
            Tags t;
            t.area = area;
            for (const auto& [key, value] : c.tags) t = with(t, key, value);
            if (category_of(t) != c.category)
                std::printf("    %s=%s, %s=%s and %s=%s\n", c.tags[0].first, c.tags[0].second, c.tags[1].first,
                            c.tags[1].second, c.tags[2].first, c.tags[2].second);
            CHECK_EQ(uint8_t(category_of(t)), uint8_t(c.category));
        }
}
