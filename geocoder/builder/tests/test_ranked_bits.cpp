// RankedBits and mark_bit (ranked_bits.h): bits marked from many threads,
// then each set position's index among the set ones.
#include "ranked_bits.h"

#include "test_framework.h"

#include <random>

TEST(ranked_bits_index_set_positions_in_order_for_any_thread_count) {
    std::mt19937_64 rng(5);
    const size_t n = 300001;
    std::vector<uint64_t> words((n + 63) / 64, 0);
    std::vector<uint32_t> expect;
    for (size_t i = 0; i < n; i++)
        if (rng() % 7 == 0 || (i >= 640 && i < 1280)) {
            words[i / 64] |= uint64_t(1) << (i % 64);
            expect.push_back(static_cast<uint32_t>(i));
        }
    for (unsigned threads : {1u, 3u, 64u}) {
        const RankedBits bits(n, [&](size_t w) { return words[w]; }, threads);
        REQUIRE(bits.count() == expect.size());
        CHECK(bits.ones(threads) == expect);
        bool all = true;
        size_t k = 0;
        for (size_t i = 0; i < n; i++) {
            bool set = k < expect.size() && expect[k] == i;
            all = all && bits.index(i) == (set ? k : RankedBits::npos);
            if (set) k++;
        }
        CHECK(all);
        CHECK_EQ(bits.index(n), RankedBits::npos);
        CHECK_EQ(bits.index(SIZE_MAX - 1), RankedBits::npos);
    }
}

TEST(ranked_bits_of_nothing_is_empty) {
    const RankedBits bits(0, [](size_t) { return uint64_t(0); });
    CHECK_EQ(bits.count(), 0u);
    CHECK(bits.ones().empty());
    CHECK_EQ(bits.index(0), RankedBits::npos);
}

TEST(mark_bit_from_many_threads_sets_each_marked_position_once) {
    const size_t n = 1 << 20;
    std::vector<std::atomic<uint64_t>> words(n / 64);
    // Every position a multiple of 3 marked, each by several threads.
    parallel_for(3 * n, [&](size_t begin, size_t end, unsigned) {
        for (size_t k = begin; k < end; k++)
            if ((k % n) % 3 == 0) mark_bit(words, k % n);
    }, 16);
    const RankedBits bits(n, words);
    CHECK_EQ(bits.count(), (n + 2) / 3);
    CHECK_EQ(bits.index(3 * 1000), 1000u);
    CHECK_EQ(bits.index(3 * 1000 + 1), RankedBits::npos);
}
