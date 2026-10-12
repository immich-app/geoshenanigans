// string_offset_map.h: the diff's string offset remap must keep the exact
// mapping of the string-keyed hash map it replaced.
#include "string_offset_map.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <map>
#include <random>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "test_framework.h"

namespace {

using Pairs = std::vector<SortedU32Map::Pair>;

// The remap as geocoder-diff first built it: one walk of the concatenated
// pools, the last start of each new string winning.
Pairs legacy_remap(const std::string& old_pool, const std::string& new_pool) {
    std::unordered_map<std::string, uint32_t> new_idx;
    for (size_t pos = 0; pos < new_pool.size();) {
        size_t len = strlen(new_pool.c_str() + pos);
        new_idx[new_pool.substr(pos, len)] = static_cast<uint32_t>(pos);
        pos += len + 1;
    }
    Pairs out;
    for (size_t pos = 0; pos < old_pool.size();) {
        size_t len = strlen(old_pool.c_str() + pos);
        auto it = new_idx.find(old_pool.substr(pos, len));
        if (it != new_idx.end()) out.push_back({static_cast<uint32_t>(pos), it->second});
        pos += len + 1;
    }
    return out;
}

// Five tiers of strings from a small alphabet, so strings repeat within and
// across tiers; some tiers empty, and with `unterminated` some middle ones
// lacking their final NUL. The last non-empty tier always ends in one (the
// legacy walk read past the pool otherwise).
std::vector<std::string> random_tiers(std::mt19937& rng, bool unterminated) {
    static const char* words[] = {"", "a", "b", "ab", "ba", "abc", "Main St", "Main", "St", "x"};
    std::vector<std::string> tiers(5);
    for (auto& t : tiers) {
        if (rng() % 4 == 0) continue;
        int n = 1 + rng() % 30;
        for (int i = 0; i < n; i++) t += std::string(words[rng() % 10]) + '\0';
        if (unterminated && rng() % 3 == 0) t += words[1 + rng() % 9];
    }
    for (size_t t = tiers.size(); t-- > 0;) {
        if (tiers[t].empty()) continue;
        if (tiers[t].back() != '\0') tiers[t] += '\0';
        break;
    }
    return tiers;
}

Pairs remap_of(const std::vector<std::string>& old_tiers, const std::vector<std::string>& new_tiers, unsigned threads) {
    auto pools = [](const std::vector<std::string>& tiers) {
        std::vector<std::pair<const char*, size_t>> p;
        for (const auto& t : tiers) p.push_back({t.data(), t.size()});
        return p;
    };
    std::vector<char> old_concat, new_concat;
    return string_offset_pairs(string_pool_segments(pools(old_tiers), old_concat),
                               string_pool_segments(pools(new_tiers), new_concat), threads);
}

std::string joined(const std::vector<std::string>& tiers) {
    std::string out;
    for (const auto& t : tiers) out += t;
    return out;
}

}  // namespace

TEST(string_offset_pairs_match_the_legacy_concatenated_remap) {
    std::mt19937 rng(17);
    for (int round = 0; round < 400; round++) {
        bool unterminated = round % 2 == 1;
        auto old_tiers = random_tiers(rng, unterminated), new_tiers = random_tiers(rng, unterminated);
        Pairs expect = legacy_remap(joined(old_tiers), joined(new_tiers));
        for (unsigned threads : {1u, 3u}) CHECK(remap_of(old_tiers, new_tiers, threads) == expect);
    }
}

TEST(string_offset_pairs_maps_moved_and_kept_strings) {
    // "b" moved from tier 0 to tier 1, "a" stayed put, "c" was deleted.
    std::vector<std::string> old_tiers = {std::string("a\0b\0c\0", 6), "", "", "", ""};
    std::vector<std::string> new_tiers = {std::string("a\0", 2), std::string("b\0", 2), "", "", ""};
    Pairs expect = {{0, 0}, {2, 2}};
    CHECK(remap_of(old_tiers, new_tiers, 2) == expect);
}

TEST(sorted_u32_map_finds_exactly_its_keys) {
    std::mt19937 rng(4);
    for (uint32_t range : {1u, 10u, 1000u, 1u << 20, 0xFFFFFFFFu}) {
        for (size_t n : {size_t(0), size_t(1), size_t(7), size_t(5000)}) {
            std::map<uint32_t, uint32_t> ref;
            while (ref.size() < std::min<size_t>(n, range)) ref[rng() % range] = rng();
            SortedU32Map m(Pairs(ref.begin(), ref.end()));
            CHECK_EQ(m.size(), ref.size());
            CHECK(Pairs(m.begin(), m.end()) == Pairs(ref.begin(), ref.end()));
            for (int q = 0; q < 3000; q++) {
                uint32_t k = q < 3 ? std::vector<uint32_t>{0u, range - 1, 0xFFFFFFFFu}[q] : rng() % range;
                auto it = ref.find(k);
                const uint32_t* got = m.find(k);
                CHECK(it == ref.end() ? got == nullptr : got != nullptr && *got == it->second);
                CHECK_EQ(m.map(k), it == ref.end() ? k : it->second);
            }
        }
    }
}
