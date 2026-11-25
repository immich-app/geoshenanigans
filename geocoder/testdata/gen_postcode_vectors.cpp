// gen_postcode_vectors: writes geocoder/testdata/postcode_files.tsv, the postcode centroid files
// the builder's encoder (builder/src/postcode_files.h) writes for a fixed set of logical streams,
// which the builder's and the server's tests both check (the server's reader and its test
// encoder, postcode_files.rs, must agree with them byte for byte).
//   g++ -std=c++20 -O1 -I geocoder/builder/src geocoder/testdata/gen_postcode_vectors.cpp -o gen
//   ./gen > geocoder/testdata/postcode_files.tsv
#include <cmath>
#include <cstdio>
#include <random>
#include <string>
#include <vector>

#include "postcode_files.h"

using postcode_files::LogicalCentroid;

static std::string hex(const std::vector<uint8_t>& b) {
    if (b.empty()) return "-";
    std::string out;
    char c[3];
    for (uint8_t x : b) {
        snprintf(c, sizeof c, "%02x", x);
        out += c;
    }
    return out;
}

static void row(const char* what, const std::vector<LogicalCentroid>& rows) {
    std::vector<uint8_t> centroids, cells;
    auto into = [](std::vector<uint8_t>& v) { return [&v](const uint8_t* p, size_t n) { v.insert(v.end(), p, p + n); }; };
    postcode_files::Encoder enc(into(centroids), into(cells));
    for (const LogicalCentroid& c : rows) enc.add(c);
    enc.finish();
    for (const auto& e : enc.table()) centroids.insert(centroids.end(), e.begin(), e.end());
    const auto footer = enc.footer();
    centroids.insert(centroids.end(), footer.begin(), footer.end());
    std::string logical;
    for (const LogicalCentroid& c : rows) {
        char b[64];
        snprintf(b, sizeof b, "%s%08x,%08x,%u,%04x,%08x", logical.empty() ? "" : ";", coord_keys::bits_of(c.lat),
                 coord_keys::bits_of(c.lng), c.postcode_id, unsigned(c.country_code), c.cell_key);
        logical += b;
    }
    printf("%s\t%s\t%s\t%s\n", what, logical.empty() ? "-" : logical.c_str(), hex(centroids).c_str(), hex(cells).c_str());
}

// The key of the level-10 cell of number n.
static uint32_t key(uint64_t n) { return uint32_t(cell_pairs::cell_of(n, 10) >> 32); }

static LogicalCentroid at(float lat, float lng, uint32_t pid, uint16_t cc, uint32_t cell_key) {
    LogicalCentroid c;
    c.lat = lat;
    c.lng = lng;
    c.postcode_id = pid;
    c.country_code = cc;
    c.cell_key = cell_key;
    return c;
}

// The float nearest units x 1e-4: a coordinate of four decimals.
static float dec(int64_t units) { return coord_keys::float_of(coord_keys::from_dec4_key(int32_t(units))); }

static constexpr uint16_t GB = 'G' << 8 | 'B', IE = 'I' << 8 | 'E', NL = 'N' << 8 | 'L', CA = 'C' << 8 | 'A';

int main() {
    printf("# Postcode centroid files as the builder's encoder writes them (builder/src/postcode_files.h):\n");
    printf("# what the case covers, the logical rows (lat and lng f32 bits in hex, postcode_id, country_code\n");
    printf("# in hex, cell key in hex; ...; - for none), and postcode_centroids.bin and\n");
    printf("# postcode_centroid_cells.bin as hex (- for an empty file). The builder's tests encode the rows\n");
    printf("# to the files and decode them back; the server's read the rows from the files and its test\n");
    printf("# encoder (postcode_files.rs) must write the same files.\n");
    printf("# Regenerate with geocoder/testdata/gen_postcode_vectors.cpp.\n");
    printf("case\trows\tcentroids\tcells\n");
    std::mt19937_64 rng(20261117);
    row("no rows", {});
    row("one row", {at(51.50722f, -0.1275f, 7, GB, key(1000))});
    // One draw a statement, so the vectors don't depend on the order a compiler evaluates arguments in.
    auto run = [&](size_t n, uint32_t cell, uint32_t pid, bool decimals) {
        std::vector<LogicalCentroid> rows;
        for (size_t i = 0; i < n; i++) {
            pid += 1 + uint32_t(rng() % 300);
            const int64_t a = 515000 + int64_t(rng() % 2000), g = -1300 + int64_t(rng() % 2600);
            const bool d = decimals && rng() % 3 != 0;
            const float lat = d ? dec(a) : float(double(a) / 1e4 + 0.000013);
            const float lng = d ? dec(g) : float(double(g) / 1e4 - 0.000017);
            rows.push_back(at(lat, lng, pid, GB, cell));
        }
        return rows;
    };
    row("32 rows, one block", run(32, key(1001), 100, true));
    row("33 rows, the last block row 32 alone", run(33, key(1002), 100, true));
    row("64 rows, two blocks", run(64, key(1003), 5000, true));
    row("65 rows, three blocks", run(65, key(1004), 5000, true));
    {
        // Kinds alternating after row 0: a kind bit a row.
        std::vector<LogicalCentroid> rows;
        for (uint32_t i = 0; i < 20; i++) rows.push_back(at(i % 2 ? dec(523700 + i) : 52.37f + float(i) * 1e-5f, i % 2 ? dec(48900 + 3 * i) : 4.89f, 10 + i, NL, key(2000)));
        row("a block of both kinds", rows);
    }
    {
        std::vector<LogicalCentroid> rows;
        for (uint32_t i = 0; i < 25; i++) rows.push_back(at(dec(436512 + 7 * i), dec(-794012 + 11 * i), 200 + 2 * i, CA, key(2001)));
        row("a block all of kind 1", rows);
    }
    {
        std::vector<LogicalCentroid> rows;
        for (uint32_t i = 0; i < 20; i++) rows.push_back(at(dec(541000 + i), dec(-59000 - i), 300 + i, i % 3 ? GB : IE, key(2002)));
        row("a block of two countries", rows);
    }
    {
        // Small steps but one in each field past 12 x 2^k: escapes.
        std::vector<LogicalCentroid> rows;
        for (uint32_t i = 0; i < 32; i++) {
            const bool far = i == 17;
            rows.push_back(at(far ? -45.0f : 51.5f + float(i) * 1e-6f, far ? 170.0f : -0.12f - float(i) * 1e-6f,
                              far ? 4000000000u : 1000 + i, far ? uint16_t(0x7A7A) : GB, key(2003)));
        }
        rows[17].postcode_id = 1017;
        for (uint32_t i = 18; i < 32; i++) rows[i].postcode_id = 3000000000u + i;
        row("an escape in each field", rows);
    }
    row("open box sides (rows 3 degrees apart)",
        {at(50.0f, 10.0f, 1, GB, key(2004)), at(53.0f, 7.0f, 2, GB, key(2004)), at(47.0f, 13.0f, 3, GB, key(2004))});
    {
        // About Greenwich: lng of either sign below 0.01, where order keys crowd towards 0.
        std::vector<LogicalCentroid> rows;
        for (uint32_t i = 0; i < 30; i++) {
            const double g = (double(rng() % 2001) - 1000.0) * 1e-5;
            const bool d = rng() % 2;
            const float lat = 51.47f + float(i) * 0.0003f;
            rows.push_back(at(lat, d ? dec(int64_t(std::nearbyint(g * 1e4))) : float(g), 400 + 5 * i, GB, key(2005)));
        }
        row("London rows by the meridian", rows);
    }
    row("a cell over two blocks", run(40, key(2006), 600, true));
    {
        std::vector<LogicalCentroid> rows = run(10, key(2007), 700, true), b = run(10, key(2008), 100, false),
                                     c = run(10, key(2010), 900, true);
        rows.insert(rows.end(), b.begin(), b.end());
        rows.insert(rows.end(), c.begin(), c.end());
        row("a block over three cells", rows);
    }
    row("signed zeros",
        {at(-0.0f, 0.0f, 1, GB, key(2011)), at(0.0f, -0.0f, 2, GB, key(2011)), at(-0.0f, -0.0f, 3, GB, key(2011)),
         at(0.0f, 0.0f, 4, GB, key(2011)), at(1e-4f, -0.0f, 5, GB, key(2011))});
    row("one postcode in two countries",
        {at(54.9f, -7.4f, 77, GB, key(2012)), at(54.91f, -7.41f, 77, IE, key(2012)), at(54.92f, -7.42f, 78, GB, key(2012))});
    {
        // Cells far apart, of one row to a few dozen, over the bitmap's words.
        std::vector<LogicalCentroid> rows;
        uint64_t cell = 5;
        for (int c = 0; c < 12; c++) {
            const uint64_t jump = 1 + rng() % 400000;
            cell += jump;
            const size_t n = 1 + rng() % 30;
            const uint32_t pid = uint32_t(rng() % 100000);
            std::vector<LogicalCentroid> cr = run(n, key(cell), pid, c % 2 == 0);
            rows.insert(rows.end(), cr.begin(), cr.end());
        }
        row("cells far apart", rows);
    }
    return 0;
}
