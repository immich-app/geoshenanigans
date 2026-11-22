// attribution.h: the credit each mode dir's attribution.json gives, the
// template's entries for the sources its data came from.
#include "attribution.h"

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
            if (s[i] != '\\') {
                raw += s[i++];
                continue;
            }
            const char e = s.at(i + 1);
            i += 2;
            if (e == 'u') {
                raw += utf8(static_cast<uint32_t>(std::stoul(s.substr(i, 4), nullptr, 16)));
                i += 4;
            } else {
                raw += e == 'n' ? '\n' : e == 't' ? '\t' : e == 'r' ? '\r' : e == 'b' ? '\b' : e == 'f' ? '\f' : e;
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

}  // namespace

TEST(attribution_canonical_form_drops_only_the_members_named) {
    CHECK_EQ(canonical("[ {\"a\": \"x \\\"y\\\"\", \"b\" : [1, 2],\n \"c\": \"\\u00a9\"} ]"),
             std::string(R"([{"a":"x \"y\"","b":[1,2],"c":"©"}])"));
    CHECK_EQ(canonical(R"({"a": 1, "b": {"a": [2]}, "c": 3})", 0, "a"), std::string(R"({"b":{},"c":3})"));
    CHECK_EQ(canonical(R"({"b": 1, "a": 2})", 0, "b"), std::string(R"({"a":2})"));
}

TEST(attribution_entries_are_the_templates) {
    // configurations.template.json's top-level `attribution` list, each entry
    // without its `applies_to`, is what a dir of every source credits: the
    // same entries in the same order, each with the same fields and notices,
    // so neither can credit a licence or a notice to another source.
    std::ifstream f(GEOCODER_TEMPLATE_PATH);
    REQUIRE(f.good());
    const std::string json((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    const std::string key = "\"attribution\": [";
    const size_t at = json.find(key);
    REQUIRE(at != std::string::npos && json.find(key, at + 1) == std::string::npos);
    const std::string all = attribution_json(ATTRIBUTION_OSM | ATTRIBUTION_GEONAMES | ATTRIBUTION_TIGER);
    const std::string templates = canonical(json, at + key.size() - 1, "applies_to"), written = canonical(all);
    if (templates != written) std::printf("    template: %s\n    written:  %s\n", templates.c_str(), written.c_str());
    CHECK(templates == written);
}

TEST(attribution_json_quote_escapes_what_json_needs) {
    CHECK_EQ(json_quote("plain"), std::string("\"plain\""));
    CHECK_EQ(json_quote("a \"b\" \\ c"), std::string(R"("a \"b\" \\ c")"));
    CHECK_EQ(json_quote("tab\tline\n"), std::string(R"("tab\u0009line\u000a")"));
    CHECK_EQ(json_quote("©"), std::string("\"©\""));
}
