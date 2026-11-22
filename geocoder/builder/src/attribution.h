#pragma once
// The credit a dataset's data is owed. Each mode dir the build writes holds
// attribution.json: the entries of the sources its data came from, as
// configurations.template.json's top-level `attribution` list gives them
// (without their `applies_to`), in its order. Every dataset takes one mode
// dir, so every dataset holds its credit.
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>

// The sources, as bits of a set.
enum AttributionSource : uint8_t {
    // Everything but the two below.
    ATTRIBUTION_OSM = 1 << 0,
    // Postcode centroids where OSM has none (--external-postcodes):
    // GeoNames's GB_full, derived from Ordnance Survey's Code-Point Open,
    // whose credit travels with it.
    ATTRIBUTION_GEONAMES = 1 << 1,
    // US address ranges, their ZIPs, and postcode centroids where OSM has
    // none (--tiger-data).
    ATTRIBUTION_TIGER = 1 << 2,
};

struct AttributionEntry {
    uint8_t source;
    const char* id;
    const char* text;
    const char* url;
    const char* licence;
    const char* licence_url;  // nullptr: none (public domain)
    const char* notices[4];   // nullptr where there are fewer
};

// GeoNames's notices are the lines of the readme of its postal code files
// (download.geonames.org/export/zip/readme.txt, also in GB.zip) that cover
// GB_full.csv.zip, verbatim, then the changes CC BY 4.0 asks a distributor to
// indicate (section 3(a)(1)(B)): the nightly keeps each GB row's postcode and
// coordinates (geocoder-build.yml), and the build those of a valid form
// (validate_postcode_for_country) whose pair OSM has no centroid for
// (load_external_postcodes, write_index). Code-Point Open's are the attribution
// statements of its licence.txt (Doc/licence.txt in its download), verbatim
// but for the year, 2022, the one of GeoNames's Royal Mail notice, of the
// release GB_full came from; its licence there,
// www.ordnancesurvey.co.uk/opendata/licence, is the Open Government Licence
// v3.0.
inline constexpr AttributionEntry kAttribution[] = {
    {ATTRIBUTION_OSM, "osm", "© OpenStreetMap contributors", "https://www.openstreetmap.org/copyright", "ODbL 1.0",
     "https://opendatacommons.org/licenses/odbl/1-0/",
     {"This dataset is a derivative database of OpenStreetMap, available under the ODbL 1.0."}},
    {ATTRIBUTION_GEONAMES, "geonames", "GeoNames", "https://www.geonames.org/", "CC BY 4.0",
     "https://creativecommons.org/licenses/by/4.0/",
     {"UK (GB_full.csv.zip): Contains Royal Mail data Royal Mail copyright and database right 2022.",
      "The Data is provided \"as is\" without warranty or any representation of accuracy, timeliness or completeness.",
      "Changes: this dataset holds the coordinates of the GB postcodes of GB_full.csv.zip that are of a valid form and "
      "that OpenStreetMap gives no centroid for, as their centroids, in its own format; it holds none of the file's "
      "other fields."}},
    {ATTRIBUTION_GEONAMES, "codepoint-open", "Ordnance Survey Code-Point Open",
     "https://www.ordnancesurvey.co.uk/products/code-point-open", "OGL v3.0",
     "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
     {"GeoNames's GB_full.csv.zip is derived from Code-Point Open.",
      "Contains Ordnance Survey data © Crown copyright and database right 2022.",
      "Contains Royal Mail data © Royal Mail copyright and database right 2022.",
      "Contains National Statistics data © Crown copyright and database right 2022."}},
    {ATTRIBUTION_TIGER, "tiger", "US Census Bureau TIGER/Line",
     "https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html", "Public domain", nullptr,
     {"Address ranges and ZIP codes in the United States from the US Census Bureau's TIGER/Line files, which are in "
      "the public domain."}},
};

// `s` as a JSON string: quotes and backslashes escaped, control characters
// as \u00XX, UTF-8 as it is.
inline std::string json_quote(const char* s) {
    std::string out = "\"";
    for (const char* p = s; *p; p++) {
        const unsigned char c = static_cast<unsigned char>(*p);
        if (c == '"' || c == '\\') {
            out += '\\';
            out += static_cast<char>(c);
        } else if (c < 0x20) {
            char hex[7];
            std::snprintf(hex, sizeof hex, "\\u%04x", c);
            out += hex;
        } else {
            out += static_cast<char>(c);
        }
    }
    return out + "\"";
}

// attribution.json of a dir whose data came from `sources`: a JSON array of
// their entries.
inline std::string attribution_json(uint8_t sources) {
    std::string out = "[";
    bool first = true;
    for (const AttributionEntry& e : kAttribution) {
        if (!(sources & e.source)) continue;
        out += first ? "\n" : ",\n";
        first = false;
        out += "  {\n";
        out += "    \"id\": " + json_quote(e.id) + ",\n";
        out += "    \"text\": " + json_quote(e.text) + ",\n";
        out += "    \"url\": " + json_quote(e.url) + ",\n";
        out += "    \"licence\": " + json_quote(e.licence) + ",\n";
        if (e.licence_url) out += "    \"licence_url\": " + json_quote(e.licence_url) + ",\n";
        out += "    \"notices\": [";
        for (size_t n = 0; n < std::size(e.notices) && e.notices[n]; n++)
            out += (n ? ",\n      " : "\n      ") + json_quote(e.notices[n]);
        out += "\n    ]\n  }";
    }
    return out + "\n]\n";
}

inline void write_attribution(const std::string& dir, uint8_t sources) {
    const std::string path = dir + "/attribution.json";
    std::ofstream f(path, std::ios::binary);
    f << attribution_json(sources);
    f.flush();
    if (!f) throw std::runtime_error("failed to write " + path);
}
