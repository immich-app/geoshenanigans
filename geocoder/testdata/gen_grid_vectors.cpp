// gen_grid_vectors: writes geocoder/testdata/postcode_grid.tsv, the postcode grid files the
// builder's encoder (builder/src/postcode_grid.h) writes for a fixed set of logical streams,
// which the builder's and the server's tests both check (the server's reader and its test
// encoder, postcode_grid.rs, must agree with them byte for byte).
//   g++ -std=c++20 -O1 -I geocoder/builder/src geocoder/testdata/gen_grid_vectors.cpp -o gen
//   ./gen > geocoder/testdata/postcode_grid.tsv
#include <cstdio>
#include <random>
#include <string>
#include <vector>

#include "country_code.h"
#include "postcode_grid.h"

using postcode_grid::LogicalRow;

static const uint16_t US = pack_country_code('U', 'S'), GB = pack_country_code('G', 'B');

static std::string hex(const std::vector<uint8_t>& b) {
    std::string out;
    char c[3];
    for (uint8_t x : b) {
        snprintf(c, sizeof c, "%02x", x);
        out += c;
    }
    return out;
}

static void row(const char* what, const std::vector<LogicalRow>& rows) {
    const std::vector<uint8_t> file = postcode_grid::encode_file(rows);
    std::string logical;
    for (const LogicalRow& r : rows) {
        char b[64];
        snprintf(b, sizeof b, "%s%016llx,%04x,%s", logical.empty() ? "" : ";", (unsigned long long)r.cell, unsigned(r.country_code),
                 std::string(postcode_grid::code_of(r)).c_str());
        logical += b;
    }
    printf("%s\t%s\t%s\n", what, logical.empty() ? "-" : logical.c_str(), hex(file).c_str());
}

static LogicalRow at(uint64_t pos, uint16_t country, const std::string& code, int level = 16) {
    return postcode_grid::row_of(postcode_grid::cell_at(pos, level), country, code);
}

int main() {
    printf("# Postcode grid files as the builder's encoder writes them (builder/src/postcode_grid.h):\n");
    printf("# what the case covers, the logical rows (cell id in hex, country in hex, code; ...; - for\n");
    printf("# none), and postcode_grid.bin as hex. The builder's tests encode the rows to the file and\n");
    printf("# decode it back; the server's read the rows from the file and its test encoder\n");
    printf("# (postcode_grid.rs) must write the same file.\n");
    printf("# Regenerate with geocoder/testdata/gen_grid_vectors.cpp.\n");
    printf("case\trows\tfile\n");
    row("no rows: the footer alone", {});
    row("one run, to the end of the curve", {at(1000, US, "19026")});
    row("a run and the cells past it of no postcode", {at(1000, US, "19026"), at(1003, 0, "")});
    {
        // Two blocks and a row: runs of three postcodes with gaps of none.
        std::vector<LogicalRow> rows;
        const char* codes[] = {"19026", "19073", "19008"};
        for (size_t i = 0; i < 2 * postcode_grid::BLOCK_ROWS + 1; i++)
            rows.push_back(i % 4 == 3 ? at(5000 + 9 * i, 0, "") : at(5000 + 9 * i, US, codes[i % 3]));
        row("two blocks and a row of three postcodes and gaps", rows);
    }
    {
        // Steps of 1 to 2^33 cells, across faces: the block's parameter, 30,
        // takes each without the Rice code's escape.
        std::vector<LogicalRow> rows;
        uint64_t pos = 0;
        for (int i = 0; i < 35; i++) {
            rows.push_back(i % 2 ? at(pos, US, "10001") : at(pos, US, "99950"));
            pos += uint64_t(1) << (i % 34);
        }
        row("steps of every size, across faces", rows);
    }
    {
        // A block of 127 runs a cell apart and one 2^33 cells past them: the
        // parameter is 0, and the long step takes the escape, its 64 bits.
        std::vector<LogicalRow> rows;
        for (uint64_t i = 0; i < postcode_grid::BLOCK_ROWS - 1; i++) rows.push_back(at(1000 + i, US, i % 2 ? "19073" : "19026"));
        rows.push_back(at(1000 + postcode_grid::BLOCK_ROWS - 2 + (uint64_t(1) << 33) + 1, US, "19008"));
        row("a step past the Rice code's escape", rows);
    }
    {
        // A block's rows each of a postcode of its own: ranks of seven bits.
        std::vector<LogicalRow> rows;
        for (size_t i = 0; i < postcode_grid::BLOCK_ROWS; i++) rows.push_back(at(70000 + 3 * i, US, std::to_string(30000 + 7 * i)));
        row("a block of as many postcodes as rows", rows);
    }
    {
        // Codes of 1 to 14 bytes of two countries, spaces among them.
        std::vector<LogicalRow> rows;
        const std::string chars = "AB 9Z-0~ !x.Q7k";
        for (size_t len = 1; len <= postcode_grid::CODE_BYTES; len++) {
            std::string code = chars.substr(0, len);
            if (code.front() == ' ') code.front() = 'S';
            rows.push_back(at(90000 + 2 * len, len % 2 ? GB : US, code));
        }
        row("codes of every length of two countries", rows);
    }
    // Runs of one code in two countries, a cell apart: two entries, and the
    // country changes at the second.
    row("one code in two countries", {at(91000, GB, "19026"), at(91001, US, "19026"), at(91002, 0, "")});
    {
        // A grid at level 10 of random runs.
        std::mt19937_64 rng(3);
        std::vector<LogicalRow> rows;
        uint64_t pos = 77;
        std::string last;
        for (int i = 0; i < 100; i++) {
            std::string code = std::to_string(60000 + rng() % 5);
            if (i && rng() % 5 == 0) code.clear();
            if (code == last) continue;
            last = code;
            rows.push_back(code.empty() ? at(pos, 0, "", 10) : at(pos, US, code, 10));
            pos += 1 + rng() % 1000;
        }
        row("a grid at level 10", rows);
    }
    {
        // The first cell of the curve and its last.
        const uint64_t end = postcode_grid::cells_at(16);
        row("the first cell of the curve and its last", {at(0, US, "00501"), at(end - 1, 0, "")});
    }
    {
        // A random US grid of 1000 rows.
        std::mt19937_64 rng(5);
        std::vector<LogicalRow> rows;
        uint64_t pos = uint64_t(4) << 32;
        std::string last;
        while (rows.size() < 1000) {
            std::string code = rng() % 6 == 0 ? std::string() : std::to_string(19000 + rng() % 90);
            if (rows.empty() && code.empty()) continue;
            if (code == last) continue;
            last = code;
            rows.push_back(code.empty() ? at(pos, 0, "") : at(pos, US, code));
            // The step's draw, then its range's.
            const uint64_t step = rng();
            const uint64_t range = rng() % 8 == 0 ? 5000 : 60;
            pos += 1 + step % range;
        }
        row("a random grid of 1000 rows", rows);
    }
    return 0;
}
