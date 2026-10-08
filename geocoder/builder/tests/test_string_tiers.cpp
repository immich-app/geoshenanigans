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
