// parse_tiger_csv: rows, strings in first-use order, postcode sums.
#include "tiger.h"

#include <string>

#include "test_framework.h"

namespace {

const char* kHeader = "from;to;interpolation;street;city;state;postcode;geometry\n";

std::string csv(const std::string& rows) { return kHeader + rows; }

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
