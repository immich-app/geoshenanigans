// Unit tests for popcount64 (src/bits.h).
#include "bits.h"

#include <random>

#include "test_framework.h"

namespace {

uint32_t count_by_bit(uint64_t x) {
    uint32_t n = 0;
    for (int b = 0; b < 64; b++) n += (x >> b) & 1;
    return n;
}

}  // namespace

TEST(popcount64_counts_edge_values) {
    CHECK_EQ(popcount64(0), 0u);
    CHECK_EQ(popcount64(~uint64_t(0)), 64u);
    CHECK_EQ(popcount64(uint64_t(1) << 63), 1u);
    CHECK_EQ(popcount64(0x5555555555555555ull), 32u);
}

TEST(popcount64_matches_counting_bit_by_bit) {
    std::mt19937_64 rng(5);
    for (int round = 0; round < 100000; round++) {
        uint64_t x = rng();
        if (round % 3 == 0) x &= rng();
        if (round % 5 == 0) x |= rng();
        CHECK_EQ(popcount64(x), count_by_bit(x));
    }
}
