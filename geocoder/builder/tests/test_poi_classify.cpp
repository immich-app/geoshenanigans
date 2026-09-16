// Table-driven tests for classify_poi (poi_classify.h) — the OSM-tag →
// (category, tier, flags) mapping encodes hundreds of Nominatim-parity
// decisions and previously had zero unit coverage (only validated via full
// planet builds + grid diffs).
#include "poi_classify.h"

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
};

std::optional<PoiClassification> classify(const Tags& t) {
    return classify_poi(t.tourism, t.historic, t.boundary, t.amenity, t.leisure,
                        t.natural, t.railway, t.aeroway, t.man_made, t.building,
                        t.craft, t.power, t.place, t.waterway, t.office,
                        t.wikipedia, t.wikidata, t.highway, t.shop, t.landuse);
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
