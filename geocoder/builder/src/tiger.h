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

// Interns the files' strings and adds their postcode midpoints to
// postcode_accum as loading file after file would; string_ids[f][k] is the
// pool id of csvs[f].strings[k]. Strings the pool holds already are looked
// up on every core: only the others go through the pool, each once, in the
// order of their first use, where interning them use by use puts them.
inline std::vector<std::vector<uint32_t>> intern_tiger_csvs(ParsedData& data, const std::vector<TigerCsv>& csvs,
                                                            unsigned threads = 0) {
    std::vector<std::vector<uint32_t>> ids(csvs.size());
    parallel_for_each(csvs.size(), [&](size_t f, unsigned) {
        ids[f].resize(csvs[f].strings.size());
        for (size_t k = 0; k < ids[f].size(); k++) ids[f][k] = data.string_pool.find(csvs[f].strings[k]);
    }, threads);

    // The uses of new strings, by string and then by use.
    struct Use {
        const std::string* text;
        uint32_t file, index;
    };
    std::vector<Use> uses = parallel_collect<Use>(csvs.size(), [&](size_t f, std::vector<Use>& out) {
        for (size_t k = 0; k < ids[f].size(); k++)
            if (ids[f][k] == StringPool::kAbsent)
                out.push_back({&csvs[f].strings[k], static_cast<uint32_t>(f), static_cast<uint32_t>(k)});
    }, threads);
    auto earlier = [](const Use& a, const Use& b) { return a.file != b.file ? a.file < b.file : a.index < b.index; };
    parallel_sort(uses.begin(), uses.end(), [&](const Use& a, const Use& b) {
        if (int c = a.text->compare(*b.text)) return c < 0;
        return earlier(a, b);
    }, threads);
    std::vector<uint32_t> firsts = parallel_filter(uses.size(), [&](size_t u) {
        return u == 0 || *uses[u - 1].text != *uses[u].text;
    }, threads);
    std::vector<uint32_t> by_first_use = firsts;
    parallel_sort(by_first_use.begin(), by_first_use.end(),
                  [&](uint32_t a, uint32_t b) { return earlier(uses[a], uses[b]); }, threads);
    for (uint32_t u : by_first_use) ids[uses[u].file][uses[u].index] = data.string_pool.intern(*uses[u].text);
    parallel_for(firsts.size(), [&](size_t b, size_t e, unsigned) {
        for (size_t r = b; r < e; r++) {
            const uint32_t id = ids[uses[firsts[r]].file][uses[firsts[r]].index];
            const size_t end = r + 1 < firsts.size() ? firsts[r + 1] : uses.size();
            for (size_t u = firsts[r] + 1; u < end; u++) ids[uses[u].file][uses[u].index] = id;
        }
    }, threads);

    for (size_t f = 0; f < csvs.size(); f++) {
        for (const auto& pc : csvs[f].postcodes) {
            auto& acc = data.postcode_accum[postcode_key(pc.country, ids[f][pc.postcode])];
            acc.sum_lat_e7 += pc.sum.sum_lat_e7;
            acc.sum_lng_e7 += pc.sum.sum_lng_e7;
            acc.count += pc.sum.count;
        }
    }
    return ids;
}

// Where each file's nodes and ranges go in the interpolation arrays: past
// what they held, file after file.
struct TigerLayout {
    std::vector<size_t> node_at, range_at;  // per file, then the total
    size_t nodes_from, ways_from, osm_from, deferred_from, postcodes_from;
};

// Grows the interpolation arrays by the files' nodes and ranges and returns
// where each file's go. It touches nothing intern_tiger_csvs does, so the
// two can run side by side.
inline TigerLayout grow_tiger_arrays(ParsedData& data, const std::vector<TigerCsv>& csvs, unsigned threads = 0) {
    const size_t files = csvs.size();
    TigerLayout at{parallel_offsets<size_t>(files, [&](size_t f) { return csvs[f].nodes.size(); }, threads),
                   parallel_offsets<size_t>(files, [&](size_t f) { return csvs[f].ranges.size(); }, threads),
                   data.interp_nodes.size(), data.interp_ways.size(), data.interp_osm_ids.size(),
                   data.deferred_interps.size(), data.interp_postcode_ids.size()};
    data.interp_nodes.resize(at.nodes_from + at.node_at[files]);
    data.interp_ways.resize(at.ways_from + at.range_at[files]);
    data.interp_osm_ids.resize(at.osm_from + at.range_at[files]);
    data.deferred_interps.resize(at.deferred_from + at.range_at[files]);
    data.interp_postcode_ids.resize(at.postcodes_from + at.range_at[files]);
    return at;
}

// Fills the room grow_tiger_arrays made with the files' nodes and ranges,
// as appending them file after file would, every file at once. string_ids
// is intern_tiger_csvs's answer for csvs.
inline void fill_tiger_ranges(ParsedData& data, const std::vector<TigerCsv>& csvs,
                              const std::vector<std::vector<uint32_t>>& string_ids, const TigerLayout& at,
                              unsigned threads = 0) {
    parallel_for_each(csvs.size(), [&](size_t f, unsigned) {
        const TigerCsv& csv = csvs[f];
        const auto& ids = string_ids[f];
        const uint32_t node_base = static_cast<uint32_t>(at.nodes_from + at.node_at[f]);
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
            const size_t r = at.range_at[f] + k;
            const uint32_t interp_id = static_cast<uint32_t>(at.ways_from + r);
            data.interp_ways[at.ways_from + r] = iw;
            data.interp_osm_ids[at.osm_from + r] =
                pack_osm_id(gc::id_alloc::ObjectType::SYNTHETIC, static_cast<int64_t>(range.synthetic_id));
            data.deferred_interps[at.deferred_from + r] = {interp_id, iw.node_offset, iw.node_count};
            data.interp_postcode_ids[at.postcodes_from + r] =
                range.postcode == TigerCsv::kNoPostcode ? NO_DATA : ids[range.postcode];
        }
    }, threads);
}
