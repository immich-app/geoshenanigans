// attribution.h: the credit each mode dir's attribution.json gives, the
// template's entries for the sources its data came from.
#include "attribution.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <iterator>
#include <stdexcept>
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
    const std::string all =
        attribution_json(ATTRIBUTION_TIGER | ATTRIBUTION_CODEPOINT | ATTRIBUTION_GEONAMES | ATTRIBUTION_OSM);
    const size_t osm = all.find("\"id\": \"osm\""), geonames = all.find("\"id\": \"geonames\""),
                 codepoint = all.find("\"id\": \"codepoint-open\""), tiger = all.find("\"id\": \"tiger\"");
    REQUIRE(osm != std::string::npos && geonames != std::string::npos && codepoint != std::string::npos &&
            tiger != std::string::npos);
    CHECK(osm < geonames && geonames < codepoint && codepoint < tiger);
    // GeoNames's notices are its readme's lines, quotes and all; TIGER's
    // public domain names no licence URL.
    CHECK(all.find(R"("The Data is provided \"as is\" without warranty or any representation of accuracy, timeliness or completeness.")") !=
          std::string::npos);
    CHECK(all.find(R"("UK (GB_full.csv.zip): Contains Royal Mail data Royal Mail copyright and database right 2022.")") !=
          std::string::npos);
    CHECK_EQ(all.find("\"licence_url\"", tiger), std::string::npos);

    const std::string some = attribution_json(ATTRIBUTION_OSM | ATTRIBUTION_TIGER);
    CHECK(some.find("\"id\": \"geonames\"") == std::string::npos);
    CHECK(some.find("\"id\": \"codepoint-open\"") == std::string::npos);
    CHECK(some.find("\"id\": \"tiger\"") != std::string::npos);
}

TEST(attribution_of_geonames_says_how_the_dataset_changed_its_data) {
    // CC BY 4.0 asks a distributor to say how it modified the material
    // (section 3(a)(1)(B)): the GeoNames entry says what the dataset holds of
    // its postal code files, after the readme's notices.
    for (const uint8_t sources : {uint8_t(ATTRIBUTION_GEONAMES), uint8_t(ATTRIBUTION_GEONAMES | ATTRIBUTION_CODEPOINT)}) {
        const std::string geonames = attribution_json(ATTRIBUTION_OSM | sources);
        const size_t at = geonames.find("\"id\": \"geonames\"");
        REQUIRE(at != std::string::npos);
        const std::string entry = geonames.substr(at, geonames.find('}', at) - at);
        const size_t warranty = entry.find("The Data is provided");
        const size_t changes = entry.find(
            R"("Changes: this dataset holds the coordinates of the postcodes of GeoNames's postal code files that are )"
            R"(of a valid form and that OpenStreetMap gives no centroid for, as their centroids, in its own format; it )"
            R"(holds none of the files' other fields.")");
        REQUIRE(warranty != std::string::npos);
        CHECK(changes != std::string::npos && warranty < changes);
    }
}

TEST(attribution_of_geonames_carries_the_credit_of_code_point_open) {
    // GeoNames's GB_full is derived from Ordnance Survey's Code-Point Open,
    // whose entry, under the Open Government Licence, travels with it: the
    // attribution statements of its licence.txt, the year GeoNames's Royal
    // Mail notice gives.
    const std::string geonames = attribution_json(ATTRIBUTION_OSM | ATTRIBUTION_GEONAMES | ATTRIBUTION_CODEPOINT);
    const size_t at = geonames.find("\"id\": \"codepoint-open\"");
    REQUIRE(at != std::string::npos);
    const std::string entry = geonames.substr(at, geonames.find('}', at) - at);
    for (const char* line : {
             R"("text": "Ordnance Survey Code-Point Open")",
             R"("url": "https://www.ordnancesurvey.co.uk/products/code-point-open")",
             R"("licence": "OGL v3.0")",
             R"("licence_url": "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/")",
             R"("GeoNames's GB_full.csv.zip is derived from Code-Point Open.")",
             R"("Contains Ordnance Survey data © Crown copyright and database right 2022.")",
             R"("Contains Royal Mail data © Royal Mail copyright and database right 2022.")",
             R"("Contains National Statistics data © Crown copyright and database right 2022.")"}) {
        if (entry.find(line) == std::string::npos) std::printf("    %s\n", line);
        CHECK(entry.find(line) != std::string::npos);
    }
}

TEST(attribution_of_wikidata_is_its_cc0_entry_alone) {
    // A POI tier ranked by Wikidata's sitelinks credits them in its own
    // poi_attribution.json; its OpenStreetMap data is the mode dir's to
    // credit.
    const std::string wikidata = attribution_json(ATTRIBUTION_WIKIDATA);
    CHECK(wikidata.find("\"id\": \"osm\"") == std::string::npos);
    for (const char* line : {
             R"("id": "wikidata")", R"("text": "Wikidata")", R"("url": "https://www.wikidata.org/")",
             R"("licence": "CC0 1.0")", R"("licence_url": "https://creativecommons.org/publicdomain/zero/1.0/")",
             R"("POI importance from the sitelink counts of the POIs' Wikidata items; Wikidata's structured data is )"
             R"(available under CC0 1.0.")"}) {
        if (wikidata.find(line) == std::string::npos) std::printf("    %s\n", line);
        CHECK(wikidata.find(line) != std::string::npos);
    }
    const std::string every = attribution_json(ATTRIBUTION_OSM | ATTRIBUTION_GEONAMES | ATTRIBUTION_CODEPOINT |
                                               ATTRIBUTION_TIGER | ATTRIBUTION_WIKIDATA);
    CHECK(every.find("\"id\": \"tiger\"") < every.find("\"id\": \"wikidata\""));
}

namespace {

// The UTF-8 of code point `c`.
std::string utf8(uint32_t c) {
    std::string out;
    if (c < 0x80) {
        out += static_cast<char>(c);
    } else if (c < 0x800) {
        out += static_cast<char>(0xC0 | (c >> 6));
        out += static_cast<char>(0x80 | (c & 0x3F));
    } else {
        out += static_cast<char>(0xE0 | (c >> 12));
        out += static_cast<char>(0x80 | ((c >> 6) & 0x3F));
        out += static_cast<char>(0x80 | (c & 0x3F));
    }
    return out;
}

// The JSON value at s[i] in one form whatever its layout: no space outside
// its strings, each string as json_quote writes it, each object's members in
// their order but those named `drop`; i past it. Throws on what is no JSON.
std::string canonical_value(const std::string& s, size_t& i, const std::string& drop) {
    auto space = [&] {
        while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i]))) i++;
    };
    auto expect = [&](char c) {
        space();
        if (i >= s.size() || s[i] != c) throw std::runtime_error(std::string("expected ") + c + " at " + std::to_string(i));
        i++;
    };
    auto string = [&] {
        expect('"');
        std::string raw;
        while (i < s.size() && s[i] != '"') {
            if (static_cast<unsigned char>(s[i]) < 0x20)
                throw std::runtime_error("a control character unescaped at " + std::to_string(i));
            if (s[i] != '\\') {
                raw += s[i++];
                continue;
            }
            if (i + 1 >= s.size()) throw std::runtime_error("an escape at the end");
            const char e = s[i + 1];
            i += 2;
            if (e == 'u') {
                const bool hex = i + 4 <= s.size() &&
                    std::all_of(s.begin() + i, s.begin() + i + 4, [](char c) { return std::isxdigit(static_cast<unsigned char>(c)); });
                if (!hex) throw std::runtime_error("\\u without four hex digits at " + std::to_string(i));
                raw += utf8(static_cast<uint32_t>(std::stoul(s.substr(i, 4), nullptr, 16)));
                i += 4;
            } else if (e == '"' || e == '\\' || e == '/') {
                raw += e;
            } else if (e == 'n' || e == 't' || e == 'r' || e == 'b' || e == 'f') {
                raw += e == 'n' ? '\n' : e == 't' ? '\t' : e == 'r' ? '\r' : e == 'b' ? '\b' : '\f';
            } else {
                throw std::runtime_error(std::string("no JSON escape \\") + e + " at " + std::to_string(i - 2));
            }
        }
        expect('"');
        return json_quote(raw.c_str());
    };
    space();
    if (i >= s.size()) throw std::runtime_error("no value at the end");
    if (s[i] == '"') return string();
    const char open = s[i];
    if (open != '{' && open != '[') {
        const size_t from = i;
        while (i < s.size() && s[i] != ',' && s[i] != '}' && s[i] != ']' && !std::isspace(static_cast<unsigned char>(s[i]))) i++;
        return s.substr(from, i - from);
    }
    const char close = open == '{' ? '}' : ']';
    i++;
    std::string out(1, open);
    bool first = true;
    for (space(); i < s.size() && s[i] != close; space()) {
        if (!first) expect(',');
        std::string member;
        bool dropped = false;
        if (open == '{') {
            member = string();
            dropped = !drop.empty() && member == json_quote(drop.c_str());
            expect(':');
            member += ':';
        }
        member += canonical_value(s, i, drop);
        if (dropped) {
            first = false;
            continue;
        }
        out += (out.size() > 1 ? "," : "") + member;
        first = false;
    }
    expect(close);
    return out + close;
}

// The JSON value at s[at] in that form.
std::string canonical(const std::string& s, size_t at = 0, const std::string& drop = "") {
    return canonical_value(s, at, drop);
}

// The JSON document `s` in that form: one value, and nothing after it but
// space. Throws on anything else after it.
std::string canonical_document(const std::string& s) {
    size_t i = 0;
    const std::string out = canonical_value(s, i, "");
    while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i]))) i++;
    if (i != s.size()) throw std::runtime_error("more after the value at " + std::to_string(i));
    return out;
}

}  // namespace

TEST(attribution_canonical_form_drops_only_the_members_named) {
    CHECK_EQ(canonical("[ {\"a\": \"x \\\"y\\\"\", \"b\" : [1, 2],\n \"c\": \"\\u00a9\"} ]"),
             std::string(R"([{"a":"x \"y\"","b":[1,2],"c":"©"}])"));
    CHECK_EQ(canonical(R"({"a": 1, "b": {"a": [2]}, "c": 3})", 0, "a"), std::string(R"({"b":{},"c":3})"));
    CHECK_EQ(canonical(R"({"b": 1, "a": 2})", 0, "b"), std::string(R"({"a":2})"));
}

TEST(attribution_canonical_document_is_one_value_and_space) {
    CHECK_EQ(canonical_document(" [1, {\"a\": 2}]\n"), std::string(R"([1,{"a":2}])"));
    // JSON's escapes, every one of them, and nothing else: a string holds no
    // raw control character, and \u four hex digits.
    CHECK_EQ(canonical_document(R"(["\" \\ \/ \b \f \n \r \t \u00A9"])"),
             std::string(R"(["\" \\ / \u0008 \u000c \u000a \u000d \u0009 ©"])"));
    for (const char* bad : {"[1] [2]", "[1],", "[1]x", "[1]\n]", "[1", R"(["\G"])", R"(["\x41"])", R"(["\u12G4"])",
                            R"(["\u12"])", "[\"a\nb\"]", "[\"a\tb\"]", R"(["a\)"}) {
        bool threw = false;
        try {
            canonical_document(bad);
        } catch (const std::exception&) {
            threw = true;
        }
        if (!threw) std::printf("    %s\n", bad);
        CHECK(threw);
    }
}

TEST(attribution_entries_are_the_templates) {
    // configurations.template.json's top-level `attribution` list, each entry
    // without its `applies_to`, is what a dir of every source credits: the
    // same entries in the same order, each with the same fields and notices,
    // so neither can credit a licence or a notice to another source. The
    // written document is that list alone, whole.
    std::ifstream f(GEOCODER_TEMPLATE_PATH);
    REQUIRE(f.good());
    const std::string json((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    const std::string key = "\"attribution\": [";
    const size_t at = json.find(key);
    REQUIRE(at != std::string::npos && json.find(key, at + 1) == std::string::npos);
    const std::string all = attribution_json(ATTRIBUTION_OSM | ATTRIBUTION_GEONAMES | ATTRIBUTION_CODEPOINT |
                                             ATTRIBUTION_TIGER | ATTRIBUTION_WIKIDATA);
    const std::string templates = canonical(json, at + key.size() - 1, "applies_to"), written = canonical_document(all);
    if (templates != written) std::printf("    template: %s\n    written:  %s\n", templates.c_str(), written.c_str());
    CHECK(templates == written);
}

TEST(attribution_json_quote_escapes_what_json_needs) {
    CHECK_EQ(json_quote("plain"), std::string("\"plain\""));
    CHECK_EQ(json_quote("a \"b\" \\ c"), std::string(R"("a \"b\" \\ c")"));
    CHECK_EQ(json_quote("tab\tline\n"), std::string(R"("tab\u0009line\u000a")"));
    CHECK_EQ(json_quote("©"), std::string("\"©\""));
}

TEST(attribution_of_geonames_beyond_gb_claims_no_gb_data) {
    // GeoNames's centroids of other countries than GB (an operator's
    // --external-postcodes) are no GB_full's: its entry gives the readme's
    // notice of every file and the changes, without the UK notice, and no
    // Code-Point Open entry comes with it.
    const std::string swiss = attribution_json(ATTRIBUTION_OSM | ATTRIBUTION_GEONAMES);
    CHECK(swiss.find("\"id\": \"geonames\"") != std::string::npos);
    CHECK(swiss.find("The Data is provided") != std::string::npos);
    CHECK(swiss.find("UK (GB_full.csv.zip)") == std::string::npos);
    CHECK(swiss.find("Royal Mail") == std::string::npos);
    CHECK(swiss.find("\"id\": \"codepoint-open\"") == std::string::npos);
    // Its notices skip the UK one and stay one JSON document.
    CHECK(canonical_document(swiss).find(R"("notices":["The Data is provided)") != std::string::npos);
    // With a GB centroid, the UK notice opens GeoNames's notices.
    const std::string gb = attribution_json(ATTRIBUTION_OSM | ATTRIBUTION_GEONAMES | ATTRIBUTION_CODEPOINT);
    const size_t at = gb.find("\"id\": \"geonames\"");
    REQUIRE(at != std::string::npos);
    CHECK(gb.find(R"("notices": [
      "UK (GB_full.csv.zip): Contains Royal Mail data Royal Mail copyright and database right 2022.",
      "The Data is provided)", at) != std::string::npos);
}
