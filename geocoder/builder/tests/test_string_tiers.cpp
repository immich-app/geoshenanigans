// Unit tests for string_home_tier and string_starts (parsed_data.h).
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

// --- string_starts ---

TEST(string_starts_indexes_only_string_starts) {
    const std::string bytes("ab\0\0cde\0", 8);
    const std::vector<char> pool(bytes.begin(), bytes.end());
    const RankedBits starts = string_starts(pool);
    CHECK_EQ(starts.count(), 3u);
    CHECK_EQ(starts.index(0), 0u);
    CHECK_EQ(starts.index(3), 1u);  // the empty string
    CHECK_EQ(starts.index(4), 2u);
    CHECK_EQ(starts.index(1), RankedBits::npos);
    CHECK_EQ(starts.index(7), RankedBits::npos);
    CHECK_EQ(starts.index(8), RankedBits::npos);
    CHECK_EQ(starts.index(NO_DATA), RankedBits::npos);
    CHECK(starts.ones() == std::vector<uint32_t>({0, 3, 4}));
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
        const RankedBits starts = string_starts(pool, threads);
        REQUIRE(starts.count() == expect.size());
        CHECK(starts.ones(threads) == expect);
        bool all = true;
        for (size_t s = 0; s < expect.size(); s++) {
            all = all && starts.index(expect[s]) == s;
            // The previous string's terminator, unless that string is empty.
            if (s > 0 && expect[s] - 1 != expect[s - 1])
                all = all && starts.index(expect[s] - 1) == RankedBits::npos;
        }
        CHECK(all);
    }
}

// --- order_poi_strings_by_record ---

namespace {

// A build whose POI records name `names` (one record each, with its POI
// tier; "" for no name) and whose admin polygons name `core`, its strings
// partitioned as the canonical sort leaves them.
ParsedData poi_build(const std::vector<std::pair<std::string, uint8_t>>& names,
                     const std::vector<std::string>& core = {}) {
    ParsedData d;
    for (const auto& [name, tier] : names) {
        PoiRecord pr{};
        pr.name_id = name.empty() ? NO_DATA : d.string_pool.intern(name);
        pr.vertex_offset = NO_DATA;
        pr.tier = tier;
        d.poi_records.push_back(pr);
    }
    for (const auto& name : core) {
        AdminPolygon ap{};
        ap.name_id = d.string_pool.intern(name);
        d.admin_polygons.push_back(ap);
    }
    partition_strings_into_tiers(d);
    return d;
}

std::string tier_bytes(const ParsedData& d, size_t t) {
    return std::string(d.strings_tiers[t].begin(), d.strings_tiers[t].end());
}

// Each POI record's name as its offset reads in the pool ("-" for none).
std::vector<std::string> poi_names(const ParsedData& d) {
    std::vector<std::string> out;
    for (const auto& pr : d.poi_records)
        out.push_back(pr.name_id == NO_DATA ? "-" : std::string(d.string_pool.data().data() + pr.name_id));
    return out;
}

std::string tiers_back_to_back(const ParsedData& d) {
    std::string out;
    for (size_t t = 0; t < STR_TIER_COUNT; t++) out += tier_bytes(d, t);
    return out;
}

}  // namespace

TEST(order_poi_strings_lays_names_out_by_their_first_shipped_record) {
    const uint8_t unshipped = POI_MAX_SHIPPED_TIER + 1;
    ParsedData d = poi_build({{"zoo", 1}, {"cafe", 3}, {"zoo", 2}, {"bank", unshipped}, {"", 1},
                              {"bank", 3}, {"apple", unshipped}, {"Main", 1}, {"cafe", 1}},
                             {"Main"});
    REQUIRE(tier_bytes(d, STR_TIER_POI) == std::string("bank\0cafe\0zoo\0", 14));
    const std::vector<std::string> names = poi_names(d);
    const std::string core = tier_bytes(d, 0);
    const auto bases = d.strings_tier_bases;

    order_poi_strings_by_record(d);

    // The unshipped record names bank first but doesn't place it; Main is a
    // core name, apple no tier's.
    CHECK(tier_bytes(d, STR_TIER_POI) == std::string("zoo\0cafe\0bank\0", 14));
    CHECK(d.poi_tier_string_ends == (std::array<uint32_t, POI_MAX_SHIPPED_TIER + 1>{0, 9, 9, 14}));
    CHECK(poi_names(d) == names);
    CHECK_EQ(d.poi_records[0].name_id, bases[STR_TIER_POI]);
    CHECK_EQ(d.poi_records[6].name_id, NO_DATA);
    CHECK(tier_bytes(d, 0) == core);
    CHECK(d.strings_tier_bases == bases);
    const std::vector<char>& pool = d.string_pool.data();
    CHECK(std::string(pool.begin(), pool.end()) == tiers_back_to_back(d));
}

TEST(order_poi_strings_puts_each_poi_tiers_names_first) {
    // pub's home is tier 1 (record 3), inn's tier 2, bar's tier 3: tier 1's
    // names zoo and pub come first, in the order of their tier 1 records.
    ParsedData d = poi_build({{"pub", 3}, {"inn", 2}, {"zoo", 1}, {"pub", 1}, {"bar", 3}, {"inn", 3}});
    const std::vector<std::string> names = poi_names(d);
    order_poi_strings_by_record(d);
    CHECK(tier_bytes(d, STR_TIER_POI) == std::string("zoo\0pub\0inn\0bar\0", 16));
    CHECK(d.poi_tier_string_ends == (std::array<uint32_t, POI_MAX_SHIPPED_TIER + 1>{0, 8, 12, 16}));
    CHECK(poi_names(d) == names);
}

TEST(order_poi_strings_matches_a_serial_walk_for_any_thread_count) {
    uint64_t seed = 31;
    auto rnd = [&](uint32_t n) { seed = seed * 6364136223846793005ull + 1442695040888963407ull; return (uint32_t)(seed >> 33) % n; };
    std::vector<std::pair<std::string, uint8_t>> names;
    for (int i = 0; i < 300000; i++)
        names.push_back({rnd(50) == 0 ? "" : "poi " + std::to_string(rnd(60000)), static_cast<uint8_t>(1 + rnd(4))});
    const ParsedData before = poi_build(names, {"poi 7", "poi 8"});

    // By home tier (the lowest tier of a shipped record naming it), each name
    // at the first record of its home tier, then the ones no shipped record
    // names.
    const uint32_t base = before.strings_tier_bases[STR_TIER_POI];
    const std::vector<char>& old_tier = before.strings_tiers[STR_TIER_POI];
    std::string want;
    std::unordered_map<uint32_t, uint32_t> moved;  // old offset → new
    auto place = [&](uint32_t off) {
        if (moved.count(off)) return;
        moved[off] = base + static_cast<uint32_t>(want.size());
        want += old_tier.data() + (off - base);
        want.push_back('\0');
    };
    std::unordered_map<uint32_t, uint8_t> home;
    for (const auto& pr : before.poi_records)
        if (pr.tier <= POI_MAX_SHIPPED_TIER && pr.name_id != NO_DATA && pr.name_id >= base) {
            auto [it, fresh] = home.emplace(pr.name_id, pr.tier);
            if (!fresh) it->second = std::min(it->second, pr.tier);
        }
    std::array<uint32_t, POI_MAX_SHIPPED_TIER + 1> ends{};
    for (uint8_t t = 0; t <= POI_MAX_SHIPPED_TIER; t++) {
        for (const auto& pr : before.poi_records)
            if (pr.tier == t && pr.name_id != NO_DATA && pr.name_id >= base && home.at(pr.name_id) == t) place(pr.name_id);
        ends[t] = static_cast<uint32_t>(want.size());
    }
    for (size_t off = 0; off < old_tier.size(); off += strlen(old_tier.data() + off) + 1) place(base + static_cast<uint32_t>(off));
    REQUIRE(ends[POI_MAX_SHIPPED_TIER] == want.size());  // no unused name

    for (unsigned threads : {1u, 3u, 8u, 64u}) {
        ParsedData d = poi_build(names, {"poi 7", "poi 8"});
        order_poi_strings_by_record(d, threads);
        CHECK(tier_bytes(d, STR_TIER_POI) == want);
        CHECK(d.poi_tier_string_ends == ends);
        bool all = true;
        for (size_t i = 0; i < d.poi_records.size(); i++) {
            const uint32_t old = before.poi_records[i].name_id;
            const uint32_t expect = old != NO_DATA && old >= base ? moved.at(old) : old;
            all = all && d.poi_records[i].name_id == expect;
        }
        CHECK(all);
        const std::vector<char>& pool = d.string_pool.data();
        CHECK(std::string(pool.begin(), pool.end()) == tiers_back_to_back(d));
    }
}

TEST(order_poi_strings_puts_names_no_shipped_record_uses_last) {
    // Partitioning never leaves such a name; the order stays a permutation.
    ParsedData d;
    const std::string bytes("a\0b\0c\0", 6);
    d.strings_tiers[STR_TIER_POI].assign(bytes.begin(), bytes.end());
    d.strings_tier_bases = {0, 0, 0, 0, 0, 6};
    d.string_pool.mutable_data().assign(bytes.begin(), bytes.end());
    PoiRecord pr{};
    pr.name_id = 4;  // "c"
    pr.tier = 1;
    d.poi_records.push_back(pr);
    order_poi_strings_by_record(d);
    CHECK(tier_bytes(d, STR_TIER_POI) == std::string("c\0a\0b\0", 6));
    CHECK_EQ(d.poi_records[0].name_id, 0u);
}

TEST(order_poi_strings_leaves_a_build_without_poi_names_alone) {
    ParsedData d = poi_build({{"", 1}}, {"Main"});
    const std::string pool(d.string_pool.data().begin(), d.string_pool.data().end());
    order_poi_strings_by_record(d);
    CHECK(d.strings_tiers[STR_TIER_POI].empty());
    CHECK(std::string(d.string_pool.data().begin(), d.string_pool.data().end()) == pool);
}

TEST(order_poi_strings_refuses_a_pool_that_is_not_the_tiers) {
    ParsedData d = poi_build({{"zoo", 1}});
    d.string_pool.mutable_data().push_back('x');
    bool threw = false;
    try { order_poi_strings_by_record(d); } catch (const std::logic_error&) { threw = true; }
    CHECK(threw);
}
