// gen_postal_city_vectors: writes geocoder/testdata/postal_cities.tsv, the postal cities files the
// builder's encoder (builder/src/postal_cities.h) writes for a fixed set of logical streams, which
// the builder's and the server's tests both check (the server's reader and its test encoder,
// postal_cities.rs, must agree with them byte for byte).
//   g++ -std=c++20 -O1 -I geocoder/builder/src geocoder/testdata/gen_postal_city_vectors.cpp -o gen
//   ./gen > geocoder/testdata/postal_cities.tsv
#include <cstdio>
#include <random>
#include <string>
#include <vector>

#include "country_code.h"
#include "postal_cities.h"

using postal_cities::LogicalRow;

static const uint16_t US = pack_country_code('U', 'S'), PR = pack_country_code('P', 'R'), GU = pack_country_code('G', 'U');

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
    const std::vector<uint8_t> file = postal_cities::encode_file(rows);
    std::string logical;
    for (const LogicalRow& r : rows) {
        char b[16];
        snprintf(b, sizeof b, "%s%04x,", logical.empty() ? "" : ";", unsigned(r.country_code));
        logical += b + std::string(postal_cities::code_of(r)) + "," + std::string(postal_cities::name_of(r));
    }
    printf("%s\t%s\t%s\n", what, logical.empty() ? "-" : logical.c_str(), hex(file).c_str());
}

static LogicalRow at(uint16_t country, const std::string& code, const std::string& name) {
    return postal_cities::row_of(country, code, name);
}

int main() {
    printf("# Postal cities files as the builder's encoder writes them (builder/src/postal_cities.h):\n");
    printf("# what the case covers, the logical rows (country in hex, code, name; ...; - for none), and\n");
    printf("# postal_cities.bin as hex. The builder's tests encode the rows to the file and decode it\n");
    printf("# back; the server's read the rows from the file and its test encoder (postal_cities.rs)\n");
    printf("# must write the same file.\n");
    printf("# Regenerate with geocoder/testdata/gen_postal_city_vectors.cpp.\n");
    printf("case\trows\tfile\n");
    row("no rows: the footer alone", {});
    row("one row", {at(US, "19026", "Drexel Hill")});
    row("rows sharing a name and the bytes of their codes",
        {at(US, "19008", "Broomall"), at(US, "19018", "Clifton Heights"), at(US, "19026", "Drexel Hill"),
         at(US, "19073", "Newtown Square"), at(US, "19083", "Havertown"), at(US, "19087", "Wayne"), at(US, "19089", "Wayne")});
    {
        // Three countries, a block each, and the US over three blocks: a
        // block of BLOCK_ROWS, another, and the rest; the US's first code
        // is Puerto Rico's first too, of another name.
        std::vector<LogicalRow> rows = {at(GU, "96910", "Hagåtña"), at(GU, "96913", "Barrigada")};
        const char* towns[] = {"San Juan", "Bayamón", "Carolina", "Ponce"};
        for (int i = 0; i < 5; i++) rows.push_back(at(PR, "009" + std::to_string(10 + 3 * i), towns[i % 4]));
        rows.push_back(at(US, "00910", "Fixture Town"));
        for (int i = 0; i < 2 * int(postal_cities::BLOCK_ROWS) + 9; i++)
            rows.push_back(at(US, std::to_string(40200 + 2 * i), i % 5 ? "Louisville" : "Jeffersontown"));
        row("three countries, the last over three blocks", rows);
    }
    // The longest name of 48 bytes ends in a character of three bytes of
    // UTF-8 and one of four.
    row("the shortest and longest codes and names",
        {at(US, "1", "A"), at(US, "12", std::string(41, 'n') + "\xE5\xB1\xB1\xF0\xA0\x80\x80"), at(US, "12 ", "O'Fallon"),
         at(US, "12-34", "Lee's Summit"),
         at(US, "12345678901234", "Cañon City"), at(US, "2", "Wilkes-Barre"), at(US, "~~~~~~~~~~~~~~", "Mayagüez")});
    {
        // A random table: codes of 9 digits, names of a few dozen.
        std::mt19937_64 rng(5);
        std::vector<std::string> names;
        for (int i = 0; i < 40; i++) names.push_back("Town " + std::to_string(rng() % 100000));
        std::vector<LogicalRow> rows;
        uint64_t code = 100;
        for (int i = 0; i < 600; i++) {
            code += 1 + rng() % (i % 50 < 40 ? 7 : 90000);
            char digits[16];
            snprintf(digits, sizeof digits, "%09llu", (unsigned long long)code);
            rows.push_back(at(US, digits, names[rng() % names.size()]));
        }
        row("a random table of 600 rows", rows);
    }
    return 0;
}
