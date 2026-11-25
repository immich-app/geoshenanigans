// The postcode grid (postcode_grid.h, postcode_grid_build.h): a tag is a
// postcode as Nominatim's pattern takes it, a cell holds the postcode most of
// its votes name, a gap takes the nearer cell within its country's reach and
// none past it, the same on one thread and many; the encoder's file is run
// coded as the format lays it out, decodes to the rows it took, one stream
// has one encoding, and the streams and files the format can't hold are
// refused.
#include <cstdint>
#include <cstring>
#include <fstream>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "country_code.h"
#include "postcode_grid.h"
#include "postcode_grid_build.h"
#include "test_framework.h"
#include "types.h"

using postcode_grid::GridCode;
using postcode_grid::LogicalRow;

namespace {

constexpr uint16_t US = pack_country_code('U', 'S'), CA = pack_country_code('C', 'A');
constexpr int L = postcode_grid::GRID_LEVEL;

uint64_t cell(uint64_t pos, int level = L) { return postcode_grid::cell_at(pos, level); }

LogicalRow row(uint64_t pos, uint16_t country, const std::string& code, int level = L) {
    return postcode_grid::row_of(cell(pos, level), country, code);
}
LogicalRow none(uint64_t pos, int level = L) { return postcode_grid::row_of(cell(pos, level), 0, {}); }

std::vector<LogicalRow> decode(const std::vector<uint8_t>& file) {
    std::vector<LogicalRow> out;
    postcode_grid::decode(file.data(), file.size(), [&](const LogicalRow& r) { out.push_back(r); });
    return out;
}

bool same(const std::vector<LogicalRow>& a, const std::vector<LogicalRow>& b) {
    return a.size() == b.size() && (a.empty() || std::memcmp(a.data(), b.data(), a.size() * sizeof(LogicalRow)) == 0);
}

std::string describe(const std::vector<LogicalRow>& rows) {
    std::string out;
    for (const LogicalRow& r : rows)
        out += std::to_string(r.cell >> postcode_grid::cell_shift(L)) + ":" + std::string(postcode_grid::code_of(r)) + " ";
    return out;
}

std::string refusal(const std::vector<LogicalRow>& rows) {
    return gctest::refusal_of([&] { postcode_grid::encode_file(rows); });
}
std::string refusal(const std::vector<uint8_t>& file) {
    return gctest::refusal_of([&] { decode(file); });
}

}  // namespace

TEST(a_tag_is_a_us_postcode_as_nominatims_pattern_takes_it) {
    auto zip = [](const char* tag) { return postcode_grid::grid_postcode(US, tag).value_or("-"); };
    CHECK_EQ(zip("19026"), std::string("19026"));
    CHECK_EQ(zip("19026-1234"), std::string("19026"));
    CHECK_EQ(zip(" 19026\t"), std::string("19026"));
    CHECK_EQ(zip("US 19026"), std::string("19026"));
    CHECK_EQ(zip("US-19026"), std::string("19026"));
    CHECK_EQ(zip("us19026"), std::string("19026"));
    CHECK_EQ(zip("\xc2\xa0" "19073\xe2\x80\xaf"), std::string("19073"));
    // What the pattern doesn't take whole.
    for (const char* bad : {"1902", "190260", "19026 1234", "19026-123", "PA 19026", "US  19026", "US\t19026", "00000",
                            "00000-0000", "19026;19073", "", " "})
        CHECK_EQ(zip(bad), std::string("-"));
    // A country the grid doesn't cover has none.
    CHECK(!postcode_grid::grid_postcode(CA, "19026"));
}

TEST(a_countrys_reach_is_its_extent_in_steps_of_the_longest_cell_edge) {
    // 25 km over S2's longest level-16 edge, 165.7 m; 331.5 m at level 15.
    CHECK_EQ(postcode_grid::reach_cells(25000.0, 16), uint64_t(150));
    CHECK_EQ(postcode_grid::reach_cells(25000.0, 15), uint64_t(75));
    REQUIRE(postcode_grid::grid_country(US) != nullptr);
    CHECK(postcode_grid::grid_country(US)->extent_m == 25000.0);
    CHECK(postcode_grid::grid_country(CA) == nullptr);
}

TEST(a_cell_takes_the_postcode_most_of_its_votes_name_the_least_of_a_tie) {
    using postcode_grid::vote;
    std::vector<uint64_t> votes = {vote(7, 2), vote(7, 1), vote(7, 2), vote(3, 5), vote(3, 4), vote(9, 0)};
    const std::vector<uint64_t> cells = postcode_grid::cell_postcodes(votes);
    CHECK(cells == std::vector<uint64_t>({vote(3, 4), vote(7, 2), vote(9, 0)}));
}

TEST(a_build_votes_with_its_grid_countries_addresses_and_tiger_lines) {
    // Strings: 0 "19026", 1 "19073-0001", 2 "PA 19026", 3 "K1A 0B1"; the
    // cells are a tenth of a degree, the US east of 0.
    const std::vector<std::string> strings = {"19026", "19073-0001", "PA 19026", "K1A 0B1"};
    auto string_of = [&](uint32_t id) { return std::string_view(strings[id]); };
    auto country_at = [](double, double lng) { return lng > 0 ? US : CA; };
    auto pos_at = [](double lat, double lng) { return uint64_t(lat * 10) * 1000 + uint64_t(lng * 10); };
    const std::vector<AddrPoint> addrs = {{1.01f, 1.01f, 0, 0, 0, NO_DATA, 0}, {1.02f, 1.02f, 0, 0, 0, NO_DATA, 0},
                                          {1.03f, 1.03f, 0, 0, 0, NO_DATA, 0}, {1.04f, 1.04f, 0, 0, 0, NO_DATA, 0},
                                          {1.05f, -1.05f, 0, 0, 0, NO_DATA, 0}, {2.05f, 2.05f, 0, 0, 0, NO_DATA, 0}};
    const std::vector<uint32_t> addr_postcodes = {1, 0, 1, 2, 0, NO_DATA};
    // A TIGER line through four cells, halfway along it in the second (its
    // middle node, the third, in the third); an OSM interpolation (no ZIP); a
    // line of a string no pattern takes; a line of two nodes across three
    // cells and the same line the other way round, halfway along both in the
    // middle cell.
    const std::vector<NodeCoord> nodes = {{3.01f, 3.01f}, {3.11f, 3.01f}, {3.21f, 3.01f}, {3.31f, 3.01f}, {4.0f, 4.0f},
                                          {4.1f, 4.0f},   {5.0f, 5.0f},   {6.01f, 6.01f}, {6.27f, 6.01f}, {6.27f, 6.01f},
                                          {6.01f, 6.01f}};
    std::vector<InterpWay> interps(5);
    interps[0].node_offset = 0, interps[0].node_count = 4;
    interps[1].node_offset = 4, interps[1].node_count = 2;
    interps[2].node_offset = 6, interps[2].node_count = 1;
    interps[3].node_offset = 7, interps[3].node_count = 2;
    interps[4].node_offset = 9, interps[4].node_count = 2;
    const std::vector<uint32_t> interp_postcodes = {0, NO_DATA, 3, 0, 0};
    const postcode_grid::GridCells got =
        postcode_grid::grid_cells(addrs, addr_postcodes, interps, nodes, interp_postcodes, string_of, country_at, pos_at);
    REQUIRE(got.codes.size() == 2);
    CHECK(got.codes[0].country == US && got.codes[0].code == "19026");
    CHECK(got.codes[1].country == US && got.codes[1].code == "19073");
    CHECK_EQ(got.address_votes, size_t(3));
    CHECK_EQ(got.tiger_votes, size_t(3));
    // Cell (1.0, 1.0): 19073 twice, 19026 once; the first line's middle at
    // 3.16, the two-node lines' at 6.14 either way round.
    CHECK(got.cells == std::vector<uint64_t>({postcode_grid::vote(10010, 1), postcode_grid::vote(31030, 0), postcode_grid::vote(61060, 0)}));
}

TEST(a_builds_cells_and_rows_are_the_same_on_one_thread_and_many) {
    std::mt19937 rng(7);
    std::vector<std::string> strings;
    for (int i = 0; i < 300; i++) {
        char zip[16];
        std::snprintf(zip, sizeof zip, i % 7 == 0 ? "X%04d" : i % 5 == 0 ? "%05d-%04d" : "%05d", 10000 + int(rng() % 400), int(rng() % 10000));
        strings.push_back(zip);
    }
    std::vector<AddrPoint> addrs(200000);
    std::vector<uint32_t> addr_postcodes(addrs.size());
    for (size_t i = 0; i < addrs.size(); i++) {
        addrs[i] = {float(rng() % 100000) * 1e-4f, float(int(rng() % 100000) - 10000) * 1e-4f, 0, 0, 0, NO_DATA, 0};
        addr_postcodes[i] = rng() % 10 == 0 ? NO_DATA : uint32_t(rng() % strings.size());
    }
    std::vector<NodeCoord> nodes;
    std::vector<InterpWay> interps(50000);
    std::vector<uint32_t> interp_postcodes(interps.size());
    for (size_t i = 0; i < interps.size(); i++) {
        interps[i].node_offset = uint32_t(nodes.size());
        interps[i].node_count = uint16_t(1 + rng() % 5);
        for (int k = 0; k < interps[i].node_count; k++) nodes.push_back({float(rng() % 100000) * 1e-4f, float(rng() % 100000) * 1e-4f});
        interp_postcodes[i] = rng() % 3 == 0 ? NO_DATA : uint32_t(rng() % strings.size());
    }
    auto string_of = [&](uint32_t id) { return std::string_view(strings[id]); };
    auto country_at = [](double lat, double lng) { return lng > 0 && lat < 9.5 ? US : CA; };
    auto pos_at = [](double lat, double lng) { return uint64_t(lat * 30) * 4000 + uint64_t((lng + 2) * 30); };
    const postcode_grid::GridCells one =
        postcode_grid::grid_cells(addrs, addr_postcodes, interps, nodes, interp_postcodes, string_of, country_at, pos_at, 1);
    REQUIRE(one.cells.size() > 10000);
    const std::vector<LogicalRow> rows = postcode_grid::grid_rows(one.cells, one.codes, L);
    for (unsigned threads : {2u, 3u, 8u, 64u}) {
        const postcode_grid::GridCells many =
            postcode_grid::grid_cells(addrs, addr_postcodes, interps, nodes, interp_postcodes, string_of, country_at, pos_at, threads);
        CHECK(many.cells == one.cells);
        CHECK_EQ(many.codes.size(), one.codes.size());
        for (size_t i = 0; i < std::min(many.codes.size(), one.codes.size()); i++)
            CHECK(many.codes[i].country == one.codes[i].country && many.codes[i].code == one.codes[i].code);
        CHECK(same(postcode_grid::grid_rows(many.cells, many.codes, L), rows));
    }
}

TEST(a_gap_takes_the_nearer_cell_within_reach_and_none_past_it) {
    using postcode_grid::vote;
    const std::vector<GridCode> codes = {{US, "19026"}, {US, "19073"}};
    auto rows = [&](const std::vector<uint64_t>& cells) { return postcode_grid::grid_rows(cells, codes, L); };
    const uint64_t R = 150;
    // Four cells between: two each. Three between: the middle one is as near
    // either, and takes the earlier.
    CHECK(same(rows({vote(1000, 0), vote(1005, 1)}), {row(1000 - R, US, "19026"), row(1003, US, "19073"), none(1005 + R + 1)}));
    CHECK(same(rows({vote(2000, 0), vote(2004, 1)}), {row(2000 - R, US, "19026"), row(2003, US, "19073"), none(2004 + R + 1)}));
    // Next to each other; one postcode either side of a gap within reach is
    // one run.
    CHECK(same(rows({vote(3000, 0), vote(3001, 1)}), {row(3000 - R, US, "19026"), row(3001, US, "19073"), none(3001 + R + 1)}));
    CHECK(same(rows({vote(4000, 0), vote(4000 + 2 * R, 0)}), {row(4000 - R, US, "19026"), none(4000 + 3 * R + 1)}));
    // A gap of twice the reach: each side's half; of more: each side's
    // reach, none between.
    CHECK(same(rows({vote(10000, 0), vote(10000 + 2 * R + 1, 1)}),
               {row(10000 - R, US, "19026"), row(10000 + R + 1, US, "19073"), none(10000 + 3 * R + 2)}));
    CHECK(same(rows({vote(10000, 0), vote(10000 + 2 * R + 2, 1)}),
               {row(10000 - R, US, "19026"), none(10000 + R + 1), row(10000 + R + 2, US, "19073"), none(10000 + 3 * R + 3)}));
    CHECK(same(rows({vote(20000, 0), vote(20400, 0)}),
               {row(20000 - R, US, "19026"), none(20000 + R + 1), row(20400 - R, US, "19026"), none(20400 + R + 1)}));
    // The first run starts no earlier than the curve, the last ends with it.
    const uint64_t end = postcode_grid::cells_at(L);
    CHECK(same(rows({vote(20, 1), vote(end - 10, 0)}),
               {row(0, US, "19073"), none(20 + R + 1), row(end - 10 - R, US, "19026")}));
    CHECK(rows({}).empty());
}

// A region's grid is the planet's runs a query routed to it reads: the
// region serves the level-10 cells 5 to 8. A house of 19026 10 cells before
// cell 5 names the first cells of cell 5, where the region's own votes (one
// house of 19073 in cell 7) would name none; the run of 19008 in cells 19 and
// 20, which no query routed to the region reads, goes, and every cell the
// region serves has the planet's postcode.
TEST(a_regions_grid_is_the_planets_runs_a_query_routed_there_reads) {
    using postcode_grid::vote;
    const std::vector<GridCode> codes = {{US, "19008"}, {US, "19026"}, {US, "19073"}};
    const uint64_t K = uint64_t(1) << 12;  // level-16 cells a level-10 cell
    const uint64_t a = 5 * K - 10, d = 7 * K + 50, c = 20 * K + 100;
    const std::vector<LogicalRow> planet = postcode_grid::grid_rows({vote(a, 1), vote(d, 2), vote(c, 0)}, codes, L);
    auto served = [&](uint64_t id) {
        int level = 0;
        REQUIRE(postcode_grid::level_of(id, level) && level == 10);
        const uint64_t at = id >> postcode_grid::cell_shift(10);
        return at >= 5 && at < 9;
    };
    const std::vector<LogicalRow> region = postcode_grid::region_rows(planet, L, 10, served);
    auto code_at = [](const std::vector<LogicalRow>& rows, uint64_t pos) {
        std::string code = "-";
        for (const LogicalRow& r : rows)
            if (r.cell <= cell(pos)) code = r.country_code ? std::string(postcode_grid::code_of(r)) : "-";
        return code;
    };
    CHECK_EQ(code_at(planet, 5 * K + 5), std::string("19026"));
    CHECK_EQ(code_at(postcode_grid::grid_rows({vote(d, 2)}, codes, L), 5 * K + 5), std::string("-"));
    CHECK_EQ(code_at(region, 5 * K + 5), std::string("19026"));
    CHECK(same(region, {row(a - 150, US, "19026"), none(a + 151), row(d - 150, US, "19073"), none(d + 151)}));
    size_t differ = 0;
    for (uint64_t pos = 5 * K; pos < 9 * K; pos++) differ += code_at(planet, pos) != code_at(region, pos);
    CHECK_EQ(differ, size_t(0));
    CHECK_EQ(code_at(planet, c), std::string("19008"));
    CHECK_EQ(code_at(region, c), std::string("-"));
    // A region that serves none of the runs has no rows; one that serves
    // every cell has the planet's.
    CHECK(postcode_grid::region_rows(planet, L, 10, [](uint64_t) { return false; }).empty());
    CHECK(same(postcode_grid::region_rows(planet, L, 10, [](uint64_t) { return true; }), planet));
    CHECK_EQ(refusal(region), std::string());
    // Served cells finer than the grid's: a level-17 cell in the first
    // cell of 19026's run keeps it; one past the run's last keeps none.
    auto served_17 = [&](uint64_t at) {
        return [at](uint64_t id) {
            int level = 0;
            REQUIRE(postcode_grid::level_of(id, level) && level == 17);
            return (id >> postcode_grid::cell_shift(17)) == at;
        };
    };
    CHECK(same(postcode_grid::region_rows(planet, L, 17, served_17(4 * (a - 150))), {row(a - 150, US, "19026"), none(a + 151)}));
    CHECK(postcode_grid::region_rows(planet, L, 17, served_17(4 * (a + 151))).empty());
    CHECK(postcode_grid::region_rows({}, L, 17, served_17(0)).empty());
}

TEST(the_grid_file_is_run_coded_as_the_format_lays_it_out) {
    // A run of 19026 from cell 1000 and none from 1003, at level 16.
    const std::vector<uint8_t> file = postcode_grid::encode_file({row(1000, US, "19026"), none(1003)});
    std::vector<uint8_t> want = {
        2,                                       // two entries,
        0, 0, 0,                                 // no postcode,
        'S', 'U', '1', '9', '0', '2', '6', 0,    // then US 19026 (0x5553 LE)
        0,                                       // parameter 0: a step of 3 is 2, "001"
        0x09,                                    // row 0's entry 1, then the step; no rank of one other entry
    };
    uint8_t entry[12];
    postcode_grid::put64(entry, cell(1000));
    postcode_grid::put32(entry + 8, 0);
    want.insert(want.end(), entry, entry + 12);
    for (uint8_t b : {2, 0, 0, 0, 16, 0, 0, 0}) want.push_back(b);
    for (char c : {'P', 'G', 'R', '1'}) want.push_back(uint8_t(c));
    CHECK(file == want);
    CHECK(same(decode(file), {row(1000, US, "19026"), none(1003)}));
    // No rows: the footer alone, of level 0.
    CHECK(postcode_grid::encode_file({}) == std::vector<uint8_t>({0, 0, 0, 0, 0, 0, 0, 0, 'P', 'G', 'R', '1'}));
    CHECK(decode(postcode_grid::encode_file({})).empty());
}

TEST(the_encoders_file_decodes_to_its_rows_and_a_stream_has_one_encoding) {
    std::mt19937_64 rng(11);
    for (int t = 0; t < 40; t++) {
        const int level = t % 4 == 0 ? 10 : L;
        std::vector<LogicalRow> rows;
        uint64_t pos = rng() % 1000;
        const size_t n = 1 + rng() % (3 * postcode_grid::BLOCK_ROWS);
        std::string last = "-";
        for (size_t i = 0; i < n; i++) {
            std::string code;
            do {
                code = i && rng() % 4 == 0 ? std::string() : std::to_string(10000 + rng() % (t % 3 ? 4 : 40));
            } while (code == last || (i == 0 && code.empty()));
            last = code;
            rows.push_back(code.empty() ? none(pos, level) : row(pos, US, code, level));
            // Mostly short steps, some across a face.
            pos += 1 + (rng() % 9 == 0 ? (uint64_t(1) << (2 * level)) : rng() % 300);
            if (pos >= postcode_grid::cells_at(level)) break;
        }
        const std::vector<uint8_t> file = postcode_grid::encode_file(rows);
        const std::vector<LogicalRow> back = decode(file);
        CHECK(same(back, rows));
        CHECK(postcode_grid::encode_file(back) == file);
    }
}

TEST(the_encoder_refuses_rows_the_grid_cant_hold) {
    CHECK(refusal({none(5)}).find("a first row of no postcode") != std::string::npos);
    CHECK(refusal({row(5, US, "19026"), row(5, US, "19073")}).find("out of cell order") != std::string::npos);
    CHECK(refusal({row(5, US, "19026"), row(4, US, "19073")}).find("out of cell order") != std::string::npos);
    CHECK(refusal({row(5, US, "19026"), row(6, US, "19026")}).find("the postcode of the row before it") != std::string::npos);
    CHECK(refusal({row(5, US, "19026"), row(6, US, "19073", 15)}).find("two levels") != std::string::npos);
    CHECK(refusal({row(5, US, "1902\x01")}).find("printable ASCII") != std::string::npos);
    CHECK(refusal({row(5, 0, "19026")}).find("a code without a country") != std::string::npos);
    CHECK(refusal({row(5, US, "")}).find("a country without a code") != std::string::npos);
    CHECK(refusal({row(5, pack_country_code('u', 's'), "19026")}).find("two upper-case letters") != std::string::npos);
    CHECK(gctest::refusal_of([] { postcode_grid::row_of(cell(5), US, "123456789012345"); }).find("longer than 14") !=
          std::string::npos);
    LogicalRow bad = row(5, US, "19026");
    bad.cell = 0;
    CHECK(refusal({bad}).find("no cell of level") != std::string::npos);
    bad.cell = cell(5) | 2;  // an odd lowest bit
    CHECK(refusal({bad}).find("no cell of level") != std::string::npos);
    bad = row(5, US, "19026");
    bad.code[9] = 'x';  // a byte after the code's end
    CHECK(refusal({bad}).find("bytes after its end") != std::string::npos);
    CHECK(refusal({row(5, US, "19026", 21)}).find("no cell of level 0 to 20") != std::string::npos);
}

TEST(the_decoder_refuses_a_file_the_encoder_would_not_write) {
    std::vector<LogicalRow> rows;
    for (uint64_t i = 0; i < 3 * postcode_grid::BLOCK_ROWS; i++)
        rows.push_back(i % 5 == 4 ? none(100 + 7 * i) : row(100 + 7 * i, US, std::to_string(19000 + i % 3)));
    const std::vector<uint8_t> file = postcode_grid::encode_file(rows);
    REQUIRE(same(decode(file), rows));
    // Every single byte changed, but the footer's magic, is refused or reads
    // as other rows, never as these.
    size_t refused = 0, other = 0;
    for (size_t at = 0; at + 4 < file.size(); at++) {
        std::vector<uint8_t> f = file;
        f[at] ^= uint8_t(1u << (at % 8));
        std::vector<LogicalRow> got;
        const std::string why = gctest::refusal_of([&] { got = decode(f); });
        if (!why.empty()) {
            CHECK(why.find("Malformed postcode grid") == 0);
            refused++;
        } else {
            CHECK(!same(got, rows));
            other++;
        }
    }
    CHECK(refused > 0);
    std::printf("    %zu refused, %zu read as other rows\n", refused, other);
    // Cut short, padded, or without its footer.
    CHECK(refusal(std::vector<uint8_t>(file.begin(), file.end() - 13)).find("Malformed postcode grid") == 0);
    std::vector<uint8_t> padded = file;
    padded.insert(padded.begin(), 0);
    CHECK(!refusal(padded).empty());
    CHECK(refusal(std::vector<uint8_t>(file.begin(), file.begin() + 6)).find("shorter than its footer") != std::string::npos);
    std::vector<uint8_t> magic = file;
    magic.back() = '2';
    CHECK(refusal(magic).find("no PGR1 footer") != std::string::npos);
}

TEST(postcode_grid_is_the_shared_vectors_byte_for_byte) {
    // geocoder/testdata/postcode_grid.tsv, which the server's reader and test encoder read too.
    std::ifstream in(std::string(GEOCODER_TESTDATA_DIR) + "/postcode_grid.tsv");
    REQUIRE(in.good());
    auto bytes = [](const std::string& hex) {
        std::vector<uint8_t> out;
        for (size_t i = 0; i + 1 < hex.size(); i += 2) out.push_back(uint8_t(std::stoi(hex.substr(i, 2), nullptr, 16)));
        return out;
    };
    size_t cases = 0, rows_in_all = 0;
    bool header = true, zero = false;
    for (std::string line; std::getline(in, line);) {
        if (line.empty() || line[0] == '#') continue;
        if (header) {
            header = false;
            continue;
        }
        std::vector<std::string> field;
        size_t a = 0;
        for (size_t b; (b = line.find('\t', a)) != std::string::npos; a = b + 1) field.push_back(line.substr(a, b - a));
        field.push_back(line.substr(a));
        REQUIRE(field.size() == 3);
        std::vector<LogicalRow> rows;
        std::stringstream rs(field[1] == "-" ? "" : field[1]);
        for (std::string r; std::getline(rs, r, ';');) {
            unsigned long long c;
            unsigned cc;
            char code[32] = {};
            REQUIRE(std::sscanf(r.c_str(), "%llx,%x,%31[^;]", &c, &cc, code) >= 2);
            rows.push_back(postcode_grid::row_of(c, uint16_t(cc), code));
        }
        const std::vector<uint8_t> want = bytes(field[2]);
        CHECK(postcode_grid::encode_file(rows) == want);
        CHECK(same(decode(want), rows));
        zero |= rows.empty();
        rows_in_all += rows.size();
        cases++;
    }
    CHECK(cases >= 8);
    CHECK(zero);
    CHECK(rows_in_all >= 300);
}
