// gen_side_vectors: writes geocoder/testdata/side_blocks.tsv, the side files the builder's
// encoder (builder/src/side_blocks.h) writes for a fixed set of values, which the builder's and
// the server's tests both check (the server's reader and its test encoder, side_blocks.rs, must
// agree with them byte for byte).
//   g++ -std=c++20 -O1 -I geocoder/builder/src geocoder/testdata/gen_side_vectors.cpp -o gen
//   ./gen > geocoder/testdata/side_blocks.tsv
#include <algorithm>
#include <cstdio>
#include <random>
#include <string>
#include <vector>

#include "side_blocks.h"

static const uint32_t NONE = 0xFFFFFFFFu;  // NO_DATA

static void row(const std::string& what, const std::vector<uint32_t>& values) {
    const std::vector<uint8_t> file = side_blocks::encode("side_blocks.tsv", values.data(), values.size());
    std::string in, out;
    char c[16];
    for (uint32_t v : values) {
        snprintf(c, sizeof c, "%s%u", in.empty() ? "" : ",", v);
        in += c;
    }
    for (uint8_t x : file) {
        snprintf(c, sizeof c, "%02x", x);
        out += c;
    }
    printf("%s\t%s\t%s\n", what.c_str(), in.empty() ? "-" : in.c_str(), out.c_str());
}

// c values that take the distinct ones in turn.
static std::vector<uint32_t> cycle(size_t c, const std::vector<uint32_t>& distinct) {
    std::vector<uint32_t> out;
    for (size_t j = 0; j < c; j++) out.push_back(distinct[j % distinct.size()]);
    return out;
}

int main() {
    printf("# Side files as the builder's encoder writes them (builder/src/side_blocks.h): what the case\n");
    printf("# covers, the values (decimal; - for none) and the file as hex. The builder's tests encode the\n");
    printf("# values to the file and decode them back; the server's read each value from the file and its\n");
    printf("# test encoder (side_blocks.rs) must write the same file.\n");
    printf("# Regenerate with geocoder/testdata/gen_side_vectors.cpp.\n");
    printf("case\tvalues\tfile\n");

    row("no values", {});
    row("one value", {7});
    row("zero alone", {0});
    row("NO_DATA alone", {NONE});
    row("one value through a block", std::vector<uint32_t>(64, 12345));
    row("NO_DATA through a block and once more", std::vector<uint32_t>(65, NONE));
    row("two values 1 apart", cycle(64, {4, 5}));
    row("zero and NO_DATA", cycle(64, {0, NONE}));
    row("an id among NO_DATA", {NONE, NONE, 12, NONE, NONE, NONE, 12, 12, NONE, NONE, NONE});
    row("a dictionary in the order its values first come", {900, 5, 900, 70000, 5, 5, 4000000000u, 900, 70000, 1});
    // The cost rule's edge: 88 + c bits as a dictionary against 48 + 2c as FOR.
    row("two values 2 apart, 39 of them", cycle(39, {7, 9}));
    row("two values 2 apart, 40 of them", cycle(40, {7, 9}));
    row("two values 2 apart, 41 of them", cycle(41, {7, 9}));
    row("three values in 3 bits through a block", cycle(64, {0, 1, 7}));
    row("three values in 4 bits through a block", cycle(64, {0, 1, 8}));
    {
        // The most entries a dictionary wins with, and one more.
        std::vector<uint32_t> distinct;
        for (uint32_t k = 0; k < 53; k++) distinct.push_back(k == 52 ? NONE : k << 26);
        row("52 values 32 bits apart through a block", cycle(64, std::vector<uint32_t>(distinct.begin() + 1, distinct.end())));
        row("53 values 32 bits apart through a block", cycle(64, distinct));
    }

    std::mt19937_64 rng(20261114);
    // One draw a statement, so the vectors don't depend on the order a compiler evaluates
    // arguments in.
    for (uint32_t w = 1; w <= 32; w++) {
        // The least and the largest of a range of w bits, then values in it.
        const size_t c = 3 + (w * 5) % 62;
        const uint32_t span = w == 32 ? NONE : (1u << w) - 1;
        const uint32_t base = uint32_t(rng() % (uint64_t(NONE) - span + 1));
        std::vector<uint32_t> values = {base, base + span};
        while (values.size() < c) {
            const uint32_t d = uint32_t(rng()) & span;
            values.push_back(base + d);
        }
        row(std::to_string(c) + " values in a range of " + std::to_string(w) + " bits", values);
    }
    for (size_t k : {2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 48}) {
        // k values far apart, each once and then any of them.
        const size_t c = std::min<size_t>(64, 20 + k);
        std::vector<uint32_t> values;
        for (size_t i = 0; i < k; i++) values.push_back(uint32_t(rng()));
        while (values.size() < c) values.push_back(values[rng() % k]);
        row(std::to_string(c) + " values, " + std::to_string(k) + " of them far apart", values);
    }
    for (size_t n : {size_t(63), size_t(64), size_t(65), size_t(200)}) {
        // A postcode column: runs of one id, NO_DATA among them.
        std::vector<uint32_t> postcodes;
        uint32_t id = uint32_t(rng() % 100000);
        for (size_t i = 0; i < n; i++) {
            if (rng() % 9 == 0) id = uint32_t(rng() % 100000);
            const bool none = rng() % 6 == 0;
            postcodes.push_back(none ? NONE : id);
        }
        row(std::to_string(n) + " postcodes", postcodes);
        // Parents: ids that climb, a few of them NO_DATA.
        std::vector<uint32_t> parents;
        uint32_t parent = uint32_t(rng() % 1000000);
        for (size_t i = 0; i < n; i++) {
            parent += uint32_t(rng() % 5);
            const uint32_t back = uint32_t(rng() % 3);
            const bool none = rng() % 40 == 0;
            parents.push_back(none ? NONE : parent - back);
        }
        row(std::to_string(n) + " parents", parents);
        std::vector<uint32_t> any;
        for (size_t i = 0; i < n; i++) any.push_back(uint32_t(rng()));
        row(std::to_string(n) + " values of any 32 bits", any);
    }
    return 0;
}
