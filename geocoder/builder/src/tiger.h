// TIGER address ranges (Nominatim's tiger-data export): one CSV per county,
// rows `from;to;interpolation;street;city;state;postcode;geometry`.
#pragma once

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "country_code.h"
#include "geometry.h"
#include "parsed_data.h"
#include "types.h"

// TIGER covers the US and its territories; OSM (and so Nominatim) keeps the
// territories as countries of their own.
inline uint16_t tiger_country(std::string_view state) {
    for (const char* territory : {"PR", "VI", "GU", "AS", "MP"}) {
        if (state == territory) return pack_country_code(territory[0], territory[1]);
    }
    return pack_country_code('U', 'S');
}

// One CSV's address ranges, parsed apart from ParsedData so files can parse
// in parallel. `strings` holds the file's distinct streets and postcodes in
// the order the rows first use them: interning them in that order interns
// what interning row by row would, in the same order.
struct TigerCsv {
    static constexpr uint32_t kNoPostcode = UINT32_MAX;
    struct Range {
        uint32_t node_offset;  // into nodes
        uint16_t node_count;
        uint8_t interpolation;  // 0 all, 1 even, 2 odd
        uint32_t street;        // into strings
        uint32_t postcode;      // into strings, or kNoPostcode
        uint32_t start_number, end_number;
        uint64_t synthetic_id;  // 56-bit content hash
    };
    // Segment midpoints summed per (country, postcode), in first-use order.
    struct Postcode {
        uint16_t country;
        uint32_t postcode;  // into strings
        ParsedData::PostcodeAccum sum;
    };
    uint64_t rows = 0;  // data rows read, loaded or not
    std::vector<std::string> strings;
    std::vector<NodeCoord> nodes;
    std::vector<Range> ranges;
    std::vector<Postcode> postcodes;
};

inline TigerCsv parse_tiger_csv(std::string_view text) {
    TigerCsv csv;
    std::unordered_map<std::string_view, uint32_t> string_ids;
    auto string_id = [&](std::string_view s) {
        auto [it, inserted] = string_ids.try_emplace(s, static_cast<uint32_t>(csv.strings.size()));
        if (inserted) csv.strings.emplace_back(s);
        return it->second;
    };
    std::unordered_map<uint64_t, uint32_t> postcode_ids;  // (country << 32 | string) → postcodes
    std::string coords_str, postcode;
    std::vector<NodeCoord> nodes;

    // Lines as std::getline splits them; the first is the header.
    size_t line_start = text.find('\n');
    line_start = line_start == std::string_view::npos ? text.size() : line_start + 1;
    while (line_start < text.size()) {
        size_t line_end = text.find('\n', line_start);
        if (line_end == std::string_view::npos) line_end = text.size();
        std::string_view line = text.substr(line_start, line_end - line_start);
        line_start = line_end + 1;
        csv.rows++;

        // Parse semicolon-delimited: from;to;interpolation;street;city;state;postcode;geometry
        std::string_view fields[8];
        size_t field_count = 0;
        size_t pos = 0;
        while (pos < line.size() && field_count < 8) {
            size_t next = line.find(';', pos);
            if (next == std::string_view::npos) next = line.size();
            fields[field_count++] = line.substr(pos, next - pos);
            pos = next + 1;
        }

        if (field_count < 8) continue;

        // Fields 0 and 1 end at a ';', so atoi stops inside them.
        int from_num = std::atoi(fields[0].data());
        int to_num = std::atoi(fields[1].data());
        std::string_view interp_type = fields[2];
        std::string_view street = fields[3];
        // fields[4] = city, fields[5] = state, fields[6] = postcode
        std::string_view geometry = fields[7];

        if (from_num <= 0 || to_num <= 0 || street.empty()) continue;
        if (geometry.find("LINESTRING") == std::string_view::npos) continue;

        // Parse WKT LINESTRING(lng lat, lng lat, ...)
        // Note: WKT uses lng lat order — we swap to our lat/lng convention
        size_t paren_start = geometry.find('(');
        size_t paren_end = geometry.rfind(')');
        if (paren_start == std::string_view::npos || paren_end == std::string_view::npos) continue;

        coords_str.assign(geometry.substr(paren_start + 1, paren_end - paren_start - 1));
        nodes.clear();

        size_t cpos = 0;
        while (cpos < coords_str.size()) {
            // Skip whitespace and commas
            while (cpos < coords_str.size() && (coords_str[cpos] == ' ' || coords_str[cpos] == ','))
                cpos++;
            if (cpos >= coords_str.size()) break;

            char* end;
            double lng = std::strtod(coords_str.c_str() + cpos, &end);
            cpos = end - coords_str.c_str();
            while (cpos < coords_str.size() && coords_str[cpos] == ' ') cpos++;
            double lat = std::strtod(coords_str.c_str() + cpos, &end);
            cpos = end - coords_str.c_str();

            if (lat != 0 || lng != 0) {
                nodes.push_back({static_cast<float>(lat), static_cast<float>(lng)});
            }
        }

        if (nodes.size() < 2 || nodes.size() > MAX_NODE_COUNT) continue;

        // As Nominatim's tiger_line_import: store the range ascending
        // with the geometry running from the start number (TIGER gives
        // ranges in the edge's digitising direction, often descending),
        // and align the start with the odd/even parity.
        if (from_num > to_num) {
            std::swap(from_num, to_num);
            std::reverse(nodes.begin(), nodes.end());
        }
        if ((interp_type == "odd" && from_num % 2 == 0) || (interp_type == "even" && from_num % 2 == 1)) {
            from_num++;
        }

        TigerCsv::Range range{};
        range.node_offset = static_cast<uint32_t>(csv.nodes.size());
        range.node_count = static_cast<uint16_t>(nodes.size());
        range.interpolation = 0;  // all
        if (interp_type == "even") range.interpolation = 1;
        else if (interp_type == "odd") range.interpolation = 2;
        range.street = string_id(street);
        range.start_number = static_cast<uint32_t>(from_num);
        range.end_number = static_cast<uint32_t>(to_num);
        csv.nodes.insert(csv.nodes.end(), nodes.begin(), nodes.end());

        // Strategy-2: TIGER interpolation has no OSM origin; use a
        // synthetic content hash (street string + range + ALL node
        // coords) so the same TIGER record gets the same dense idx
        // across builds. Hashing only the FIRST node collided two
        // distinct interpolation ways that shared a street/range/start
        // node but differed downstream — ~97k collisions on the planet,
        // which the strategy-2 allocator could not disambiguate
        // (tombstone + non-deterministic reslot, churning interp_ways/
        // nodes/entries). Mixing every node coordinate makes distinct
        // geometries get distinct ids; truly identical interps still
        // collide but are genuine duplicates.
        uint64_t syn_h = FNV1A_OFFSET_BASIS;
        auto mix = [&](uint64_t v) { syn_h ^= v; syn_h *= FNV1A_PRIME; };
        for (char c : street) mix(static_cast<uint8_t>(c));
        mix(0);
        mix(static_cast<uint64_t>(range.start_number));
        mix(static_cast<uint64_t>(range.end_number));
        for (const auto& nd : nodes) {
            uint32_t lb, gb;
            std::memcpy(&lb, &nd.lat, 4);
            std::memcpy(&gb, &nd.lng, 4);
            mix(static_cast<uint64_t>(lb));
            mix(static_cast<uint64_t>(gb));
        }
        range.synthetic_id = syn_h & 0x00FFFFFFFFFFFFFFull;

        // Accumulate TIGER postcode centroids — TIGER has excellent
        // US zip code coverage that OSM addr:postcode mostly lacks.
        // Nominatim imports TIGER the same way and postcodes feed
        // into location_postcode.
        postcode.assign(fields[6]);
        // Keep the per-segment ZIP association (Nominatim's
        // location_property_tiger.postcode): reverse lookups use the
        // winning street's nearby TIGER segment as the postcode source.
        range.postcode = TigerCsv::kNoPostcode;
        if (!postcode.empty() && is_valid_postcode(postcode.c_str())) {
            range.postcode = string_id(fields[6]);
            // Use the midpoint of the interpolation segment as the
            // postcode location (centroid of the segment).
            double mid_lat = 0, mid_lng = 0;
            for (const auto& n : nodes) { mid_lat += n.lat; mid_lng += n.lng; }
            mid_lat /= nodes.size(); mid_lng /= nodes.size();
            uint16_t country = tiger_country(fields[5]);
            uint64_t key = (static_cast<uint64_t>(country) << 32) | range.postcode;
            auto [it, inserted] = postcode_ids.try_emplace(key, static_cast<uint32_t>(csv.postcodes.size()));
            if (inserted) csv.postcodes.push_back({country, range.postcode, {}});
            csv.postcodes[it->second].sum.add(mid_lat, mid_lng);
        }
        csv.ranges.push_back(range);
    }
    return csv;
}

// Interns one TIGER CSV's strings and adds its postcode midpoints to
// postcode_accum, returning each string's pool id: the part of loading a
// file whose order shows, so files go through it one by one in file order.
inline std::vector<uint32_t> intern_tiger_csv(ParsedData& data, const TigerCsv& csv) {
    std::vector<uint32_t> string_ids(csv.strings.size());
    for (size_t i = 0; i < csv.strings.size(); i++) string_ids[i] = data.string_pool.intern(csv.strings[i]);
    for (const auto& pc : csv.postcodes) {
        auto& acc = data.postcode_accum[postcode_key(pc.country, string_ids[pc.postcode])];
        acc.sum_lat_e7 += pc.sum.sum_lat_e7;
        acc.sum_lng_e7 += pc.sum.sum_lng_e7;
        acc.count += pc.sum.count;
    }
    return string_ids;
}

// Appends the files' nodes and ranges to the interpolation arrays where
// appending them file after file would put them, every file at once.
// string_ids[f] is intern_tiger_csv's answer for csvs[f].
inline void append_tiger_ranges(ParsedData& data, const std::vector<TigerCsv>& csvs,
                                const std::vector<std::vector<uint32_t>>& string_ids, unsigned threads = 0) {
    const size_t files = csvs.size();
    const auto node_at = parallel_offsets<size_t>(files, [&](size_t f) { return csvs[f].nodes.size(); }, threads);
    const auto range_at = parallel_offsets<size_t>(files, [&](size_t f) { return csvs[f].ranges.size(); }, threads);
    const size_t nodes_from = data.interp_nodes.size(), ways_from = data.interp_ways.size();
    const size_t osm_from = data.interp_osm_ids.size(), deferred_from = data.deferred_interps.size();
    const size_t postcodes_from = data.interp_postcode_ids.size();
    data.interp_nodes.resize(nodes_from + node_at[files]);
    data.interp_ways.resize(ways_from + range_at[files]);
    data.interp_osm_ids.resize(osm_from + range_at[files]);
    data.deferred_interps.resize(deferred_from + range_at[files]);
    data.interp_postcode_ids.resize(postcodes_from + range_at[files]);
    parallel_for_each(files, [&](size_t f, unsigned) {
        const TigerCsv& csv = csvs[f];
        const auto& ids = string_ids[f];
        const uint32_t node_base = static_cast<uint32_t>(nodes_from + node_at[f]);
        std::copy(csv.nodes.begin(), csv.nodes.end(), data.interp_nodes.begin() + node_base);
        for (size_t k = 0; k < csv.ranges.size(); k++) {
            const auto& range = csv.ranges[k];
            InterpWay iw{};
            iw.node_offset = node_base + range.node_offset;
            iw.node_count = range.node_count;
            iw.street_id = ids[range.street];
            iw.start_number = range.start_number;
            iw.end_number = range.end_number;
            iw.interpolation = range.interpolation;

            // Defer S2 computation
            const size_t r = range_at[f] + k;
            const uint32_t interp_id = static_cast<uint32_t>(ways_from + r);
            data.interp_ways[ways_from + r] = iw;
            data.interp_osm_ids[osm_from + r] =
                pack_osm_id(gc::id_alloc::ObjectType::SYNTHETIC, static_cast<int64_t>(range.synthetic_id));
            data.deferred_interps[deferred_from + r] = {interp_id, iw.node_offset, iw.node_count};
            data.interp_postcode_ids[postcodes_from + r] =
                range.postcode == TigerCsv::kNoPostcode ? NO_DATA : ids[range.postcode];
        }
    }, threads);
}
