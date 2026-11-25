// The postcode centroid files (postcode_files.h): the encoder's files decode
// to the logical stream it took, one stream has one encoding, and the streams
// and files the format can't hold are refused.
#include <cmath>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <limits>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "cell_pairs.h"
#include "coord_keys.h"
#include "postcode_files.h"
#include "test_framework.h"
#include "types.h"

using postcode_files::LogicalCentroid;

namespace {

struct Files {
    std::vector<uint8_t> centroids, cells;
    bool operator==(const Files& o) const { return centroids == o.centroids && cells == o.cells; }
};

Files encode(const std::vector<LogicalCentroid>& rows) {
    Files f;
    auto into = [](std::vector<uint8_t>& v) { return [&v](const uint8_t* p, size_t n) { v.insert(v.end(), p, p + n); }; };
    postcode_files::Encoder enc(into(f.centroids), into(f.cells));
    for (const LogicalCentroid& c : rows) enc.add(c);
    enc.finish();
    for (const auto& e : enc.table()) f.centroids.insert(f.centroids.end(), e.begin(), e.end());
    const auto footer = enc.footer();
    f.centroids.insert(f.centroids.end(), footer.begin(), footer.end());
    return f;
}

std::vector<LogicalCentroid> decode(const Files& f) {
    std::vector<LogicalCentroid> out;
    postcode_files::decode(f.centroids.data(), f.centroids.size(), f.cells.data(), f.cells.size(),
                           [&](const LogicalCentroid& c) { out.push_back(c); });
    return out;
}

bool same(const std::vector<LogicalCentroid>& a, const std::vector<LogicalCentroid>& b) {
    return a.size() == b.size() && (a.empty() || std::memcmp(a.data(), b.data(), a.size() * sizeof(LogicalCentroid)) == 0);
}

// The key of the level-10 cell of number n.
uint32_t key(uint64_t n) { return uint32_t(cell_pairs::cell_of(n, 10) >> 32); }

LogicalCentroid row(float lat, float lng, uint32_t pid, uint16_t cc, uint32_t cell_key) {
    LogicalCentroid c;
    c.lat = lat;
    c.lng = lng;
    c.postcode_id = pid;
    c.country_code = cc;
    c.cell_key = cell_key;
    return c;
}

constexpr uint16_t GB = 'G' << 8 | 'B', NL = 'N' << 8 | 'L', IE = 'I' << 8 | 'E';

// n centroids in cells of ascending keys (one to thousands a cell, some far
// apart), half of them at four decimals, a few with a second country or far
// from their cell's others, ids ascending in a cell with gaps of every size.
std::vector<LogicalCentroid> random_stream(std::mt19937_64& rng, size_t n) {
    std::vector<LogicalCentroid> rows;
    uint64_t cell = rng() % 1000;
    uint32_t pid = 0;
    double lat0 = 51.5, lng0 = -0.1;
    // One draw a statement: the same stream whatever order a compiler evaluates arguments in.
    for (size_t i = 0; i < n; i++) {
        if (i == 0 || rng() % 40 == 0) {
            const uint64_t reach = rng() % 2 ? 3 : 100000;
            cell += 1 + rng() % reach;
            pid = uint32_t(rng() % 1000);
            const double a = double(int64_t(rng() % 1700001) - 850000) / 10000.0;
            const double g = double(int64_t(rng() % 3600001) - 1800000) / 10000.0;
            lat0 = a;
            lng0 = g;
        }
        const uint64_t shift = rng() % 30;
        pid += 1 + uint32_t((rng() % 100000) >> shift);
        const bool decimal = rng() % 2;
        const bool far = rng() % 50 == 0;
        const double spread = far ? 4.0 : 0.05;
        const double dlat = (double(rng() % 20001) - 10000.0) / 10000.0 * spread;
        const double dlng = (double(rng() % 20001) - 10000.0) / 10000.0 * spread;
        float lat = float(lat0 + dlat), lng = float(lng0 + dlng);
        if (decimal) {
            lat = coord_keys::float_of(coord_keys::from_dec4_key(int32_t(std::nearbyint((lat0 + dlat) * 1e4))));
            lng = coord_keys::float_of(coord_keys::from_dec4_key(int32_t(std::nearbyint((lng0 + dlng) * 1e4))));
        }
        const uint16_t cc = rng() % 30 == 0 ? IE : GB;
        rows.push_back(row(lat, lng, pid, cc, key(cell)));
    }
    return rows;
}

std::string refusal(const std::vector<LogicalCentroid>& rows) {
    try {
        encode(rows);
    } catch (const std::runtime_error& e) {
        return e.what();
    }
    return "";
}

std::string refusal_decoding(const Files& f) {
    try {
        decode(f);
    } catch (const std::runtime_error& e) {
        return e.what();
    }
    return "";
}

uint32_t get32(const std::vector<uint8_t>& b, size_t at) {
    uint32_t v;
    std::memcpy(&v, b.data() + at, 4);
    return v;
}
void set32(std::vector<uint8_t>& b, size_t at, uint32_t v) { std::memcpy(b.data() + at, &v, 4); }

// Where block b's table entry starts in a centroids file.
size_t entry_at(const Files& f, size_t b) {
    const uint32_t rows = get32(f.centroids, f.centroids.size() - 8);
    const size_t blocks = (rows + postcode_files::BLOCK_ROWS - 1) / postcode_files::BLOCK_ROWS;
    return f.centroids.size() - 8 - (blocks - b) * postcode_files::TABLE_ENTRY_BYTES;
}

}  // namespace

TEST(postcode_files_decode_to_the_stream_the_encoder_took) {
    std::mt19937_64 rng(20261117);
    for (size_t n : {size_t(0), size_t(1), size_t(2), size_t(31), size_t(32), size_t(33), size_t(64), size_t(65), size_t(3000)}) {
        const std::vector<LogicalCentroid> rows = random_stream(rng, n);
        const Files f = encode(rows);
        CHECK(same(decode(f), rows));
        CHECK(encode(decode(f)) == f);
        // The footer counts the rows, the table an entry a block of 32.
        const size_t blocks = (n + 31) / 32;
        CHECK_EQ(get32(f.centroids, f.centroids.size() - 8), uint32_t(n));
        CHECK(std::memcmp(f.centroids.data() + f.centroids.size() - 4, "PCC1", 4) == 0);
        CHECK(f.centroids.size() >= blocks * postcode_files::TABLE_ENTRY_BYTES + postcode_files::FOOTER_BYTES);
        CHECK_EQ(f.cells.empty(), n == 0);
    }
}

// No centroid is the footer alone and an empty cells file, a dir of the
// family's that holds none.
TEST(postcode_files_of_no_centroids_are_the_footer_and_an_empty_cells_file) {
    const Files f = encode({});
    CHECK(f.centroids == (std::vector<uint8_t>{0, 0, 0, 0, 'P', 'C', 'C', '1'}));
    CHECK(f.cells.empty());
    CHECK(decode(f).empty());
    const postcode_files::Footer ft = postcode_files::read_footer(f.centroids.data(), f.centroids.size());
    CHECK_EQ(ft.rows, 0u);
    CHECK_EQ(ft.blocks, size_t(0));
    CHECK_EQ(postcode_files::logical_size(ft), uint64_t(0));
    // A cells file beside no rows.
    Files cells_without_rows = f;
    cells_without_rows.cells = encode({row(1, 1, 1, GB, key(5))}).cells;
    CHECK(refusal_decoding(cells_without_rows).find("cells past the last row") != std::string::npos);
    // A byte before the footer of no rows: block bytes that no block is, refused
    // by the footer alone.
    Files leading = f;
    leading.centroids.insert(leading.centroids.begin(), 0);
    const std::string no_rows = "Malformed postcode centroid files: postcode_centroids.bin has block bytes but no rows";
    CHECK_EQ(refusal_decoding(leading), no_rows);
    CHECK_EQ(gctest::refusal_of([&] { postcode_files::read_footer(leading.centroids.data() + 1, leading.centroids.size()); }), no_rows);
}

TEST(a_block_holds_32_rows_its_first_in_its_table_entry) {
    std::vector<LogicalCentroid> rows;
    for (uint32_t i = 0; i < 33; i++) rows.push_back(row(52.0f + float(i) * 0.001f, 4.9f, 100 + i, NL, key(7)));
    const Files f = encode(rows);
    // Two entries: block 1 is row 32 alone, its entry and no bits.
    CHECK_EQ(f.centroids.size(), get32(f.centroids, entry_at(f, 1) + 12) + 2 * postcode_files::TABLE_ENTRY_BYTES + 8);
    CHECK_EQ(get32(f.centroids, entry_at(f, 1)), coord_keys::bits_of(rows[32].lat));
    CHECK_EQ(get32(f.centroids, entry_at(f, 1) + 8), 132u);
    CHECK_EQ(get32(f.centroids, entry_at(f, 0) + 12), 0u);
    CHECK(same(decode(f), rows));
    const postcode_files::Footer ft = postcode_files::read_footer(f.centroids.data() + f.centroids.size() - 8, f.centroids.size());
    CHECK_EQ(ft.blocks, size_t(2));
    CHECK_EQ(ft.rows_of(0), size_t(32));
    CHECK_EQ(ft.rows_of(1), size_t(1));
    CHECK_EQ(postcode_files::logical_size(ft), uint64_t(33 * postcode_files::LOGICAL_STRIDE));
}

// A block's bits are at most max_block_bytes of its rows, which the decoders
// hold a block to: 32 rows each a far step from the one before in every
// field, of both kinds and two countries, and the blocks of random streams.
TEST(a_blocks_bits_are_within_max_block_bytes) {
    std::vector<LogicalCentroid> far;
    for (uint32_t i = 0; i < 32; i++) {
        const float sign = i % 2 ? 1.0f : -1.0f;
        const float lat = i % 4 < 2 ? sign * 3e38f : sign * 89.9999f, lng = i % 3 ? -sign * 1e-40f : sign * 179.9999f;
        far.push_back(row(lat, lng, i * 0x07FFFFFFu, i % 2 ? uint16_t(1) : uint16_t(0xFFFF), key(5)));
    }
    std::mt19937_64 rng(13);
    size_t blocks = 0, most = 0;
    for (int t = 0; t < 101; t++) {
        const Files f = encode(t ? random_stream(rng, 1 + rng() % 300) : far);
        if (t == 0) CHECK(same(decode(f), far));
        const postcode_files::Footer ft = postcode_files::read_footer(f.centroids.data() + f.centroids.size() - 8, f.centroids.size());
        for (size_t b = 0; b < ft.blocks; b++, blocks++) {
            const uint64_t end = b + 1 < ft.blocks ? get32(f.centroids, entry_at(f, b + 1) + 12) : ft.table_at;
            const uint64_t bytes = end - get32(f.centroids, entry_at(f, b) + 12);
            CHECK(bytes <= postcode_files::max_block_bytes(ft.rows_of(b)));
            if (t == 0) most = size_t(bytes);
        }
    }
    CHECK(blocks > 100);
    std::printf("    the far block: %zu bytes of at most %zu\n", most, postcode_files::max_block_bytes(32));
}

// A block's box holds its rows as an f64 reader computes it, each side the
// tightest that does, and its open sides are where none of -32766 to 32766
// units of 2^-14 degrees from row 0 reaches.
TEST(a_blocks_box_holds_its_rows_in_f64) {
    std::mt19937_64 rng(5);
    size_t open = 0, blocks = 0, tight[2] = {0, 0};
    for (int t = 0; t < 200; t++) {
        const std::vector<LogicalCentroid> rows = random_stream(rng, 1 + rng() % 200);
        const Files f = encode(rows);
        const postcode_files::Footer ft = postcode_files::read_footer(f.centroids.data() + f.centroids.size() - 8, f.centroids.size());
        for (size_t b = 0; b < ft.blocks; b++, blocks++) {
            const postcode_files::BlockEntry e = postcode_files::read_entry(f.centroids.data() + ft.table_at + b * 29);
            const double lat0 = double(coord_keys::float_of(e.lat_bits)), lng0 = double(coord_keys::float_of(e.lng_bits));
            auto side = [](double first, int16_t n) { return first + std::ldexp(double(n), -14); };
            for (size_t i = b * 32; i < b * 32 + ft.rows_of(b); i++) {
                const double lat = rows[i].lat, lng = rows[i].lng;
                CHECK(e.box[0] == -32767 || side(lat0, e.box[0]) <= lat);
                CHECK(e.box[1] == 32767 || side(lat0, e.box[1]) >= lat);
                CHECK(e.box[2] == -32767 || side(lng0, e.box[2]) <= lng);
                CHECK(e.box[3] == 32767 || side(lng0, e.box[3]) >= lng);
            }
            for (int s = 0; s < 4; s++) open += e.box[s] == -32767 || e.box[s] == 32767;
            // The tightest, lat and lng alike: one unit in from each closed side
            // leaves a row out, and an open side is one that no closed side
            // holds the rows in, so the farthest, 32766 units from row 0, leaves
            // a row out too.
            const double first[2] = {lat0, lng0};
            double least[2] = {1e9, 1e9}, most[2] = {-1e9, -1e9};
            for (size_t i = b * 32; i < b * 32 + ft.rows_of(b); i++) {
                const double v[2] = {rows[i].lat, rows[i].lng};
                for (int a = 0; a < 2; a++) least[a] = std::min(least[a], v[a]), most[a] = std::max(most[a], v[a]);
            }
            for (int a = 0; a < 2; a++) {
                const int16_t lo = e.box[2 * a], hi = e.box[2 * a + 1];
                if (lo == -32767) {
                    CHECK(side(first[a], -32766) > least[a]);
                } else if (lo < 32766) {
                    CHECK(side(first[a], int16_t(lo + 1)) > least[a]);
                    tight[a]++;
                }
                if (hi == 32767) {
                    CHECK(side(first[a], 32766) < most[a]);
                } else if (hi > -32766) {
                    CHECK(side(first[a], int16_t(hi - 1)) < most[a]);
                    tight[a]++;
                }
            }
        }
    }
    CHECK(open > 0);
    CHECK(blocks > 0);
    CHECK(tight[0] > 0 && tight[1] > 0);
    // Rows 3 degrees apart: past 32766 units (2.0 degrees) on both sides of row 0.
    const Files wide = encode({row(50, 10, 1, GB, key(3)), row(53, 7, 2, GB, key(3)), row(47, 13, 3, GB, key(3))});
    const postcode_files::BlockEntry e = postcode_files::read_entry(wide.centroids.data() + entry_at(wide, 0));
    CHECK_EQ(e.box[0], int16_t(-32767));
    CHECK_EQ(e.box[1], int16_t(32767));
    CHECK_EQ(e.box[2], int16_t(-32767));
    CHECK_EQ(e.box[3], int16_t(32767));
}

TEST(the_encoder_refuses_a_stream_the_files_cant_hold) {
    const LogicalCentroid c = row(51.5f, -0.12f, 10, GB, key(9));
    auto with = [](LogicalCentroid r, auto edit) {
        edit(r);
        return r;
    };
    struct Case {
        std::vector<LogicalCentroid> rows;
        std::string refusal;
    };
    const float nan = std::numeric_limits<float>::quiet_NaN(), inf = std::numeric_limits<float>::infinity();
    const Case cases[] = {
        {{c, with(c, [](LogicalCentroid& r) { r.cell_key = key(8); })}, "centroids out of cell order"},
        {{c, with(c, [](LogicalCentroid& r) { r.postcode_id = 9; })}, "centroids out of order in a cell"},
        {{c, with(c, [](LogicalCentroid& r) { r.country_code = GB - 1; })}, "centroids out of order in a cell"},
        {{c, c}, "a postcode of one country twice in a cell"},
        {{with(c, [](LogicalCentroid& r) { r.postcode_id = NO_DATA; })}, "a centroid without a postcode"},
        {{with(c, [](LogicalCentroid& r) { r.pad = 1; })}, "a centroid with its reserved bytes set"},
        {{with(c, [&](LogicalCentroid& r) { r.lat = nan; })}, "a coordinate that isn't finite"},
        {{with(c, [&](LogicalCentroid& r) { r.lng = -inf; })}, "a coordinate that isn't finite"},
        {{with(c, [](LogicalCentroid& r) { r.cell_key = 0; })}, "is not a cell of levels 5 to 13"},
    };
    for (const Case& k : cases) {
        const std::string got = refusal(k.rows);
        CHECK(got.find(k.refusal) != std::string::npos);
        if (got.find(k.refusal) == std::string::npos) std::printf("    want %s, got %s\n", k.refusal.c_str(), got.c_str());
    }
    // The same postcode in two cells, and two countries of one postcode in a cell, are two centroids.
    CHECK_EQ(refusal({c, with(c, [](LogicalCentroid& r) { r.cell_key = key(10); })}), std::string());
    CHECK_EQ(refusal({c, with(c, [](LogicalCentroid& r) { r.country_code = GB + 1; })}), std::string());
    // Any finite float, however far off the globe.
    CHECK_EQ(refusal({with(c, [](LogicalCentroid& r) { r.lat = 3e38f; r.lng = -1e-40f; })}), std::string());
}

TEST(the_decoder_refuses_files_the_encoder_doesnt_write) {
    std::mt19937_64 rng(31);
    const Files f = encode(random_stream(rng, 100));  // four blocks, the last of 4 rows
    const std::string m = "Malformed postcode centroid files: ";
    auto refused = [&](Files g, const std::string& why) {
        const std::string got = refusal_decoding(g);
        if (got.find(why) == std::string::npos) std::printf("    want %s, got %s\n", why.c_str(), got.c_str());
        CHECK(got.find(why) != std::string::npos);
    };
    CHECK_EQ(refusal_decoding(f), std::string());
    // The footer: short, without its magic, counting rows the table has no entries for.
    refused({std::vector<uint8_t>(f.centroids.begin(), f.centroids.begin() + 5), f.cells}, "shorter than its footer");
    {
        Files g = f;
        g.centroids.back() = '2';
        refused(g, "has no PCC1 footer");
    }
    {
        Files g = f;
        set32(g.centroids, g.centroids.size() - 8, 1000000);
        refused(g, "shorter than its block table");
    }
    {
        // The most rows a footer counts and no table: their blocks are
        // counted in 64 bits, which a 32-bit size_t would wrap to none.
        const std::vector<uint8_t> most = {0xFF, 0xFF, 0xFF, 0xFF, 'P', 'C', 'C', '1'};
        refused({most, {}}, "shorter than its block table");
    }
    {
        // 129 rows: a fifth block, the table read an entry early.
        Files g = f;
        set32(g.centroids, g.centroids.size() - 8, 129);
        CHECK(refusal_decoding(g) != std::string());
    }
    // An offset past the blocks, or not where the block before ends.
    {
        Files g = f;
        set32(g.centroids, entry_at(g, 2) + 12, uint32_t(entry_at(g, 0)) + 1);
        refused(g, "block 1 ends past the blocks or before it starts");
    }
    {
        Files g = f;
        set32(g.centroids, entry_at(g, 0) + 12, 1);
        refused(g, "block 0 is not where the blocks before it end");
    }
    // Codes past a block's end: block 0 cut to two bytes, the rest moved down with it.
    {
        Files g = f;
        const uint32_t end0 = get32(g.centroids, entry_at(g, 1) + 12);
        const uint32_t cut = end0 - 2;
        for (size_t b = 1; b < 4; b++) set32(g.centroids, entry_at(g, b) + 12, get32(g.centroids, entry_at(g, b) + 12) - cut);
        g.centroids.erase(g.centroids.begin() + 2, g.centroids.begin() + end0);
        refused(g, "block 0's codes run past its end");
    }
    // Bytes between the blocks and the table: the last block's bits one byte longer.
    {
        Files g = f;
        g.centroids.insert(g.centroids.begin() + long(entry_at(g, 0)), 0);
        refused(g, "block 3 is not the encoder's");
    }
    // More of them than the last block's 4 rows can have: refused for its
    // length, before its bits are read.
    {
        Files g = f;
        const size_t bits = entry_at(g, 0) - get32(g.centroids, entry_at(g, 3) + 12);
        g.centroids.insert(g.centroids.begin() + long(entry_at(g, 0)), postcode_files::max_block_bytes(4) + 1 - bits, 0);
        refused(g, "block 3 has more bytes than its rows can take");
        Files at_most = f;
        at_most.centroids.insert(at_most.centroids.begin() + long(entry_at(at_most, 0)), postcode_files::max_block_bytes(4) - bits, 0);
        refused(at_most, "block 3 is not the encoder's");
    }
    // Cells whose ranges don't tile the rows: one row short, one past, another file's.
    {
        Files g = f;
        std::vector<LogicalCentroid> rows = decode(f);
        rows.pop_back();
        g.cells = encode(rows).cells;
        refused(g, "rows past the last cell");
    }
    {
        Files g = f;
        std::vector<LogicalCentroid> rows = decode(f);
        rows.push_back(row(1, 1, rows.back().postcode_id + 1, GB, rows.back().cell_key));
        g.cells = encode(rows).cells;
        refused(g, "ranges don't end at the last row");
    }
    refused({f.centroids, {}}, "rows past the last cell");
    std::vector<uint8_t> lists;
    cell_pairs::Writer w(
        cell_pairs::POI, [&](const char* p, size_t n) { lists.insert(lists.end(), p, p + n); },
        [](const char*, size_t) {});
    const uint32_t id = 1;
    w.add(cell_pairs::cell_of(5, 10), &id, 1);
    w.finish();
    refused({f.centroids, lists}, "a cells file of another kind of index");
}

// The parameters, flags and box of a block's entry are the encoder's one
// choice for its rows: a changed one is refused, as is a row 0 the encoder
// refuses. (Row 0's fields are where the block's steps start, and a code's
// low bits its step's, so most other values of them are another block's
// rows: a_logical_stream_of_centroids_has_one_encoding.)
TEST(a_block_has_one_encoding) {
    std::vector<LogicalCentroid> rows;
    for (uint32_t i = 0; i < 32; i++)
        rows.push_back(row(51.5f + float(i % 7) * 0.003f, -0.12f + float(i) * 0.0021f, 1000 + 3 * i, i % 5 ? GB : IE, key(11)));
    const Files f = encode(rows);
    const size_t e = entry_at(f, 0);
    struct Edit {
        const char* what;
        size_t at;
        uint8_t xor_;
    };
    const Edit edits[] = {
        {"k_lat", e + 18, 0x01}, {"k_lng", e + 18, 0x20}, {"k_pid", e + 19, 0x04}, {"k_cc", e + 19, 0x80},
        {"mixed", e + 20, 0x10}, {"all kind 1", e + 20, 0x20}, {"one country", e + 20, 0x40}, {"the spare bit", e + 20, 0x80},
        {"lat_lo", e + 21, 0x01}, {"lat_hi", e + 23, 0x01}, {"lng_lo", e + 25, 0x01}, {"lng_hi", e + 27, 0x01},
        {"lat_hi's sign", e + 24, 0x80},
    };
    struct Set {
        const char* what;
        size_t at;
        uint32_t v;
    };
    const Set sets[] = {
        {"row 0 without a postcode", e + 8, NO_DATA},
        {"row 0's lat a NaN", e + 0, 0x7FC00000u},
        {"row 0's lng an infinity", e + 4, 0xFF800000u},
    };
    std::vector<Files> changed;
    for (const Edit& ed : edits) {
        changed.push_back(f);
        changed.back().centroids[ed.at] ^= ed.xor_;
    }
    for (const Set& st : sets) {
        changed.push_back(f);
        set32(changed.back().centroids, st.at, st.v);
    }
    for (size_t k = 0; k < changed.size(); k++) {
        const Files& g = changed[k];
        const char* what = k < std::size(edits) ? edits[k].what : sets[k - std::size(edits)].what;
        std::vector<LogicalCentroid> got;
        std::string why;
        try {
            got = decode(g);
        } catch (const std::runtime_error& ex) {
            why = ex.what();
        }
        if (why.empty()) std::printf("    %s: decoded\n", what);
        CHECK(!why.empty());
    }
}

// Every file a byte changed in, or cut short, either is refused or decodes
// to a stream whose encoding it is: no stream has two encodings. Among them
// a change that is another stream's encoding, the country of a block of one
// row, decodes to that stream.
TEST(a_logical_stream_of_centroids_has_one_encoding) {
    std::mt19937_64 rng(77);
    const Files f = encode(random_stream(rng, 289));  // ten blocks, the last of one row
    size_t refused = 0, decoded = 0;
    for (int t = -1; t < 6000; t++) {
        Files g = f;
        if (t < 0) {
            // Row 288's country, in the last block's table entry.
            g.centroids[g.centroids.size() - 8 - postcode_files::TABLE_ENTRY_BYTES + 16] ^= 0x5A;
        } else {
            std::vector<uint8_t>& file = t % 4 == 0 ? g.cells : g.centroids;
            if (t % 50 == 0) {
                file.resize(rng() % file.size());
            } else {
                const size_t at = rng() % file.size();
                file[at] ^= uint8_t(1u << (rng() % 8));
            }
        }
        std::vector<LogicalCentroid> rows;
        try {
            rows = decode(g);
        } catch (const std::runtime_error&) {
            CHECK(t >= 0);
            refused++;
            continue;
        }
        decoded++;
        CHECK(encode(rows) == g);
        if (t < 0) CHECK(rows.back().country_code == (GB ^ 0x5A) || rows.back().country_code == (IE ^ 0x5A));
    }
    CHECK(refused > 0);
    CHECK(decoded > 0);
    std::printf("    %zu refused, %zu decoded to their encoding\n", refused, decoded);
}

TEST(postcode_files_are_the_shared_vectors_byte_for_byte) {
    // geocoder/testdata/postcode_files.tsv, which the server's reader and test encoder read too.
    std::ifstream in(std::string(GEOCODER_TESTDATA_DIR) + "/postcode_files.tsv");
    REQUIRE(in.good());
    auto bytes = [](const std::string& hex) {
        std::vector<uint8_t> out;
        if (hex == "-") return out;
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
        REQUIRE(field.size() == 4);
        std::vector<LogicalCentroid> rows;
        std::stringstream rs(field[1] == "-" ? "" : field[1]);
        for (std::string r; std::getline(rs, r, ';');) {
            unsigned lat, lng, pid, cc, cell;
            REQUIRE(std::sscanf(r.c_str(), "%x,%x,%u,%x,%x", &lat, &lng, &pid, &cc, &cell) == 5);
            rows.push_back(row(coord_keys::float_of(lat), coord_keys::float_of(lng), pid, uint16_t(cc), cell));
        }
        const Files want{bytes(field[2]), bytes(field[3])};
        CHECK(encode(rows) == want);
        CHECK(same(decode(want), rows));
        zero |= rows.empty();
        rows_in_all += rows.size();
        cases++;
    }
    CHECK(cases >= 14);
    CHECK(zero);
    CHECK(rows_in_all >= 200);
}
