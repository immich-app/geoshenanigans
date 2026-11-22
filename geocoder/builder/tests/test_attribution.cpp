// attribution.h: the credit each mode dir's attribution.json gives, the
// template's entries for the sources its data came from.
#include "attribution.h"

#include <cstdio>
#include <fstream>
#include <iterator>
#include <string>
#include <utility>

#include "test_framework.h"

TEST(attribution_of_osm_alone_is_its_entry) {
    CHECK_EQ(attribution_json(ATTRIBUTION_OSM), std::string(R"([
  {
    "id": "osm",
    "text": "© OpenStreetMap contributors",
    "url": "https://www.openstreetmap.org/copyright",
    "licence": "ODbL 1.0",
    "licence_url": "https://opendatacommons.org/licenses/odbl/1-0/",
    "notices": [
      "This dataset is a derivative database of OpenStreetMap, available under the ODbL 1.0."
    ]
  }
]
)"));
}

TEST(attribution_lists_its_sources_in_the_templates_order) {
    const std::string all = attribution_json(ATTRIBUTION_TIGER | ATTRIBUTION_GEONAMES | ATTRIBUTION_OSM);
    const size_t osm = all.find("\"id\": \"osm\""), geonames = all.find("\"id\": \"geonames\""),
                 tiger = all.find("\"id\": \"tiger\"");
    REQUIRE(osm != std::string::npos && geonames != std::string::npos && tiger != std::string::npos);
    CHECK(osm < geonames && geonames < tiger);
    // GeoNames's notices are its readme's lines, quotes and all; TIGER's
    // public domain names no licence URL.
    CHECK(all.find(R"("The Data is provided \"as is\" without warranty or any representation of accuracy, timeliness or completeness.")") !=
          std::string::npos);
    CHECK(all.find(R"("UK (GB_full.csv.zip): Contains Royal Mail data Royal Mail copyright and database right 2022.")") !=
          std::string::npos);
    CHECK_EQ(all.find("\"licence_url\"", tiger), std::string::npos);

    const std::string some = attribution_json(ATTRIBUTION_OSM | ATTRIBUTION_TIGER);
    CHECK(some.find("\"id\": \"geonames\"") == std::string::npos);
    CHECK(some.find("\"id\": \"tiger\"") != std::string::npos);
}

TEST(attribution_entries_are_the_templates) {
    // Each field of each entry, as configurations.template.json's top-level
    // `attribution` list writes it.
    std::ifstream f(GEOCODER_TEMPLATE_PATH);
    REQUIRE(f.good());
    const std::string json((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    REQUIRE(json.find("\"attribution\": [") != std::string::npos);
    for (const AttributionEntry& e : kAttribution) {
        for (const auto& [key, value] : {std::pair{"id", e.id}, std::pair{"text", e.text}, std::pair{"url", e.url},
                                         std::pair{"licence", e.licence}, std::pair{"licence_url", e.licence_url}}) {
            if (!value) continue;
            const std::string field = "\"" + std::string(key) + "\": " + json_quote(value);
            if (json.find(field) == std::string::npos) std::printf("    not in the template: %s\n", field.c_str());
            CHECK(json.find(field) != std::string::npos);
        }
        for (const char* notice : e.notices)
            if (notice) CHECK(json.find(json_quote(notice)) != std::string::npos);
    }
}

TEST(attribution_json_quote_escapes_what_json_needs) {
    CHECK_EQ(json_quote("plain"), std::string("\"plain\""));
    CHECK_EQ(json_quote("a \"b\" \\ c"), std::string(R"("a \"b\" \\ c")"));
    CHECK_EQ(json_quote("tab\tline\n"), std::string(R"("tab\u0009line\u000a")"));
    CHECK_EQ(json_quote("©"), std::string("\"©\""));
}
