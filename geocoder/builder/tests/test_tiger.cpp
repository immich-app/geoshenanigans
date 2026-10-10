// parse_tiger_csv: rows, strings in first-use order, postcode sums.
#include "tiger.h"

#include <cstring>
#include <string>

#include "test_framework.h"

namespace {

const char* kHeader = "from;to;interpolation;street;city;state;postcode;geometry\n";

std::string csv(const std::string& rows) { return kHeader + rows; }

// Loading file after file, the way TIGER files used to be added.
void add_tiger_ranges_serially(ParsedData& data, const TigerCsv& csv) {
    std::vector<uint32_t> string_ids(csv.strings.size());
    for (size_t i = 0; i < csv.strings.size(); i++) string_ids[i] = data.string_pool.intern(csv.strings[i]);

    uint32_t node_base = static_cast<uint32_t>(data.interp_nodes.size());
    data.interp_nodes.insert(data.interp_nodes.end(), csv.nodes.begin(), csv.nodes.end());
    for (const auto& range : csv.ranges) {
        InterpWay iw{};
        iw.node_offset = node_base + range.node_offset;
        iw.node_count = range.node_count;
        iw.street_id = string_ids[range.street];
        iw.start_number = range.start_number;
        iw.end_number = range.end_number;
        iw.interpolation = range.interpolation;
        uint32_t interp_id = static_cast<uint32_t>(data.interp_ways.size());
        data.interp_ways.push_back(iw);
        data.interp_osm_ids.push_back(
            pack_osm_id(gc::id_alloc::ObjectType::SYNTHETIC, static_cast<int64_t>(range.synthetic_id)));
        data.deferred_interps.push_back({interp_id, iw.node_offset, iw.node_count});
        data.interp_postcode_ids.push_back(
            range.postcode == TigerCsv::kNoPostcode ? NO_DATA : string_ids[range.postcode]);
    }
    for (const auto& pc : csv.postcodes) {
        auto& acc = data.postcode_accum[postcode_key(pc.country, string_ids[pc.postcode])];
        acc.sum_lat_e7 += pc.sum.sum_lat_e7;
        acc.sum_lng_e7 += pc.sum.sum_lng_e7;
        acc.count += pc.sum.count;
    }
}

// An OSM interpolation already parsed, as TIGER finds the arrays.
ParsedData new_data_with_one_interp() {
    ParsedData d;
    d.string_pool.intern("Oak St");
    d.interp_nodes = {{1.0f, 2.0f}, {1.5f, 2.5f}};
    InterpWay iw{};
    iw.node_count = 2;
    d.interp_ways.push_back(iw);
    d.interp_osm_ids.push_back(42);
    d.deferred_interps.push_back({0, 0, 2});
    d.interp_postcode_ids.push_back(NO_DATA);
    return d;
}

}  // namespace

TEST(tiger_parse_reads_a_range) {
    auto parsed = parse_tiger_csv(csv("100;198;even;Main St;Waco;TX;76701;LINESTRING(-97.1 31.5,-97.2 31.6)\n"));
    CHECK_EQ(parsed.rows, uint64_t(1));
    REQUIRE(parsed.ranges.size() == 1);
    const auto& r = parsed.ranges[0];
    CHECK_EQ(r.start_number, 100u);
    CHECK_EQ(r.end_number, 198u);
    CHECK_EQ(r.interpolation, 1);
    CHECK_EQ(r.node_offset, 0u);
    CHECK_EQ(r.node_count, 2);
    REQUIRE(parsed.nodes.size() == 2);
    CHECK_EQ(parsed.nodes[0].lat, 31.5f);
    CHECK_EQ(parsed.nodes[0].lng, -97.1f);
    REQUIRE(parsed.strings.size() == 2);
    CHECK_EQ(parsed.strings[r.street], std::string("Main St"));
    CHECK_EQ(parsed.strings[r.postcode], std::string("76701"));
}

TEST(tiger_parse_orders_a_descending_range_and_aligns_parity) {
    auto parsed = parse_tiger_csv(csv("199;100;odd;Elm St;;TX;;LINESTRING(1 10,2 20,3 30)\n"));
    REQUIRE(parsed.ranges.size() == 1);
    CHECK_EQ(parsed.ranges[0].start_number, 101u);
    CHECK_EQ(parsed.ranges[0].end_number, 199u);
    CHECK_EQ(parsed.ranges[0].interpolation, 2);
    REQUIRE(parsed.nodes.size() == 3);
    CHECK_EQ(parsed.nodes[0].lng, 3.0f);
    CHECK_EQ(parsed.nodes[2].lng, 1.0f);
}

TEST(tiger_parse_skips_unusable_rows_but_counts_them) {
    auto parsed = parse_tiger_csv(csv(
        "1;2;all;Short Row\n"
        "0;10;all;Zero St;;TX;76701;LINESTRING(1 1,2 2)\n"
        "1;10;all;;;TX;76701;LINESTRING(1 1,2 2)\n"
        "1;10;all;Point St;;TX;76701;POINT(1 1)\n"
        "1;10;all;One Node St;;TX;76701;LINESTRING(1 1)\n"
        "1;10;all;Null Island St;;TX;76701;LINESTRING(0 0,1 1)\n"
        "\n"));
    CHECK_EQ(parsed.rows, uint64_t(7));
    CHECK(parsed.ranges.empty());
    CHECK(parsed.strings.empty());
    CHECK(parsed.postcodes.empty());
}

TEST(tiger_parse_keeps_strings_in_first_use_order) {
    auto parsed = parse_tiger_csv(csv(
        "1;9;all;B St;;TX;76701;LINESTRING(1 1,2 2)\n"
        "1;9;all;A St;;TX;76701;LINESTRING(1 1,2 2)\n"
        "1;9;all;B St;;TX;00000;LINESTRING(1 1,2 2)\n"
        "1;9;all;76702;;TX;76702;LINESTRING(1 1,2 2)"));
    REQUIRE(parsed.ranges.size() == 4);
    REQUIRE(parsed.strings.size() == 4);
    CHECK_EQ(parsed.strings[0], std::string("B St"));
    CHECK_EQ(parsed.strings[1], std::string("76701"));
    CHECK_EQ(parsed.strings[2], std::string("A St"));
    CHECK_EQ(parsed.strings[3], std::string("76702"));
    CHECK_EQ(parsed.ranges[2].street, 0u);
    CHECK_EQ(parsed.ranges[2].postcode, TigerCsv::kNoPostcode);  // all-zero placeholder
    CHECK_EQ(parsed.ranges[3].street, parsed.ranges[3].postcode);
}

TEST(tiger_parse_sums_postcode_midpoints_per_country) {
    auto parsed = parse_tiger_csv(csv(
        "1;9;all;A St;;TX;00601;LINESTRING(-97 30,-97 32)\n"
        "1;9;all;B St;;PR;00601;LINESTRING(-66 18,-66 18.2)\n"
        "1;9;all;C St;;TX;00601;LINESTRING(-97 34,-97 36)\n"));
    REQUIRE(parsed.postcodes.size() == 2);
    const auto& us = parsed.postcodes[0];
    CHECK_EQ(us.country, pack_country_code('U', 'S'));
    CHECK_EQ(parsed.strings[us.postcode], std::string("00601"));
    CHECK_EQ(us.sum.count, uint64_t(2));
    CHECK_EQ(us.sum.sum_lat_e7, int64_t(310000000) + int64_t(350000000));
    CHECK_EQ(parsed.postcodes[1].country, pack_country_code('P', 'R'));
    CHECK_EQ(parsed.postcodes[1].postcode, us.postcode);
    CHECK_EQ(parsed.postcodes[1].sum.count, uint64_t(1));
}

TEST(tiger_parse_ids_follow_the_geometry) {
    auto parsed = parse_tiger_csv(csv(
        "1;9;all;A St;;TX;;LINESTRING(1 1,2 2)\n"
        "1;9;all;A St;;TX;;LINESTRING(1 1,2 2)\n"
        "1;9;all;A St;;TX;;LINESTRING(1 1,2 3)\n"));
    REQUIRE(parsed.ranges.size() == 3);
    CHECK_EQ(parsed.ranges[0].synthetic_id, parsed.ranges[1].synthetic_id);
    CHECK(parsed.ranges[0].synthetic_id != parsed.ranges[2].synthetic_id);
    CHECK_EQ(parsed.ranges[0].synthetic_id >> 56, uint64_t(0));
}

TEST(tiger_parse_header_only_or_empty) {
    CHECK_EQ(parse_tiger_csv("").rows, uint64_t(0));
    CHECK_EQ(parse_tiger_csv("from;to").rows, uint64_t(0));
    CHECK_EQ(parse_tiger_csv(kHeader).rows, uint64_t(0));
}

TEST(tiger_append_lays_files_out_as_adding_them_one_by_one_would) {
    std::vector<TigerCsv> files = {
        parse_tiger_csv(csv("1;9;all;B St;;TX;76701;LINESTRING(1 1,2 2)\n"
                            "2;8;even;Oak St;;PR;00601;LINESTRING(3 3,4 4,5 5)\n")),
        TigerCsv{},  // a file that could not be read
        parse_tiger_csv(csv("5;1;odd;A St;;TX;76701;LINESTRING(6 6,7 7)\n"
                            "1;9;all;B St;;TX;;LINESTRING(8 8,9 9)\n"
                            "3;7;all;C St;;TX;76702;LINESTRING(1 2,2 3,3 4,4 5)\n")),
        parse_tiger_csv(csv("1;3;all;D St;;TX;76701;LINESTRING(9 1,9 2)\n")),
    };
    ParsedData want = new_data_with_one_interp();
    for (const auto& f : files) add_tiger_ranges_serially(want, f);

    for (unsigned threads : {1u, 3u, 8u}) {
        ParsedData got = new_data_with_one_interp();
        std::vector<std::vector<uint32_t>> ids;
        for (const auto& f : files) ids.push_back(intern_tiger_csv(got, f));
        append_tiger_ranges(got, files, ids, threads);

        CHECK(got.string_pool.data() == want.string_pool.data());
        CHECK(got.interp_nodes.size() == want.interp_nodes.size() &&
              std::memcmp(got.interp_nodes.data(), want.interp_nodes.data(),
                          want.interp_nodes.size() * sizeof(NodeCoord)) == 0);
        CHECK(got.interp_ways.size() == want.interp_ways.size() &&
              std::memcmp(got.interp_ways.data(), want.interp_ways.data(),
                          want.interp_ways.size() * sizeof(InterpWay)) == 0);
        CHECK(got.interp_osm_ids == want.interp_osm_ids);
        CHECK(got.interp_postcode_ids == want.interp_postcode_ids);
        bool same_deferred = got.deferred_interps.size() == want.deferred_interps.size();
        for (size_t i = 0; same_deferred && i < want.deferred_interps.size(); i++) {
            const auto& a = got.deferred_interps[i];
            const auto& b = want.deferred_interps[i];
            same_deferred = a.interp_id == b.interp_id && a.node_offset == b.node_offset &&
                            a.node_count == b.node_count;
        }
        CHECK(same_deferred);
        // Same insertion sequence, so the same iteration order too.
        std::vector<std::pair<uint64_t, uint64_t>> pa, pb;
        for (const auto& [k, acc] : got.postcode_accum) pa.push_back({k, acc.count});
        for (const auto& [k, acc] : want.postcode_accum) pb.push_back({k, acc.count});
        CHECK(pa == pb);
    }
}
