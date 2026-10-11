// Unit tests for string_home_tier (parsed_data.h).
#include "parsed_data.h"

#include "test_framework.h"

#include <iterator>
#include <string>
#include <vector>

// For test_patch_format.cpp, which can't include parsed_data.h beside
// patch_format.h.
std::vector<std::string> builder_string_tier_files() {
    return {std::begin(STR_TIER_FILENAMES), std::end(STR_TIER_FILENAMES)};
}

// --- string_home_tier ---

TEST(string_home_tier_picks_the_most_widely_downloaded_consumer_tier) {
    // Tier indices: 0 core, 1 street, 2 addr, 3 postcode, 4 poi.
    struct Case { uint8_t mask; uint8_t tier; };
    const Case cases[] = {
        {0, 0},
        {STR_TIER_BIT_CORE, 0},
        {STR_TIER_BIT_STREET, 1},
        {STR_TIER_BIT_ADDR, 2},
        {STR_TIER_BIT_POSTCODE, 3},
        {STR_TIER_BIT_POI, 4},
        // Postcodes that are also house numbers or street names: admin mode
        // downloads the postcode tier but neither of the others.
        {STR_TIER_BIT_ADDR | STR_TIER_BIT_POSTCODE, 3},
        {STR_TIER_BIT_STREET | STR_TIER_BIT_POSTCODE, 3},
        {STR_TIER_BIT_STREET | STR_TIER_BIT_ADDR, 1},
        // POI names shared outside core: the poi tier pairs with any mode.
        {STR_TIER_BIT_POI | STR_TIER_BIT_STREET, 0},
        {STR_TIER_BIT_POI | STR_TIER_BIT_ADDR, 0},
        {STR_TIER_BIT_POI | STR_TIER_BIT_POSTCODE, 0},
        {STR_TIER_BIT_POI | STR_TIER_BIT_CORE, 0},
        {STR_TIER_BIT_CORE | STR_TIER_BIT_STREET | STR_TIER_BIT_ADDR, 0},
        // Strings only POI candidates no tier ships use aren't written;
        // shared with a shipped consumer, they live where that one needs.
        {STR_TIER_BIT_UNSHIPPED, STR_TIER_NONE},
        {STR_TIER_BIT_UNSHIPPED | STR_TIER_BIT_STREET, 1},
        {STR_TIER_BIT_UNSHIPPED | STR_TIER_BIT_POI, 4},
    };
    for (const auto& c : cases) CHECK_EQ(string_home_tier(c.mask), c.tier);
}

// --- StringStarts ---

TEST(string_starts_indexes_only_string_starts) {
    const std::string bytes("ab\0\0cde\0", 8);
    const std::vector<char> pool(bytes.begin(), bytes.end());
    const StringStarts starts(pool);
    CHECK_EQ(starts.count(), 3u);
    CHECK_EQ(starts.index(0), 0u);
    CHECK_EQ(starts.index(3), 1u);  // the empty string
    CHECK_EQ(starts.index(4), 2u);
    CHECK_EQ(starts.index(1), StringStarts::npos);
    CHECK_EQ(starts.index(7), StringStarts::npos);
    CHECK_EQ(starts.index(8), StringStarts::npos);
    CHECK_EQ(starts.index(NO_DATA), StringStarts::npos);
    CHECK(starts.offsets() == std::vector<uint32_t>({0, 3, 4}));
}

TEST(string_starts_ranks_across_blocks_for_any_thread_count) {
    std::vector<char> pool;
    std::vector<uint32_t> expect;
    for (size_t i = 0; pool.size() < 3000000; i++) {
        expect.push_back(static_cast<uint32_t>(pool.size()));
        pool.insert(pool.end(), i % 150, 'x');
        pool.push_back('\0');
    }
    for (unsigned threads : {1u, 5u, 64u}) {
        const StringStarts starts(pool, threads);
        REQUIRE(starts.count() == expect.size());
        CHECK(starts.offsets(threads) == expect);
        bool all = true;
        for (size_t s = 0; s < expect.size(); s++) {
            all = all && starts.index(expect[s]) == s;
            // The previous string's terminator, unless that string is empty.
            if (s > 0 && expect[s] - 1 != expect[s - 1])
                all = all && starts.index(expect[s] - 1) == StringStarts::npos;
        }
        CHECK(all);
    }
}
