// record_match.h: pairing old and new records by key must give the pairing
// the diff's former FIFO queue per key gave, for any thread count.
#include "record_match.h"

#include <cstdint>
#include <deque>
#include <mutex>
#include <random>
#include <unordered_map>
#include <utility>
#include <vector>

#include "test_framework.h"

namespace {

using Pairs = std::vector<std::pair<uint32_t, uint32_t>>;

// The queue pairing: old records in index order take the oldest unused new
// record of their key.
Pairs fifo_pairs(const std::vector<uint64_t>& old_keys, const std::vector<uint64_t>& new_keys) {
    std::unordered_map<uint64_t, std::deque<uint32_t>> queues;
    for (uint32_t i = 0; i < new_keys.size(); i++) queues[new_keys[i]].push_back(i);
    Pairs out;
    for (uint32_t i = 0; i < old_keys.size(); i++) {
        auto it = queues.find(old_keys[i]);
        if (it == queues.end() || it->second.empty()) continue;
        out.push_back({i, it->second.front()});
        it->second.pop_front();
    }
    return out;
}

Pairs sorted_pairs(const std::vector<uint64_t>& old_keys, const std::vector<uint64_t>& new_keys, unsigned threads) {
    auto olds = keyed_records(old_keys.size(), [&](size_t i) { return old_keys[i]; }, threads);
    auto news = keyed_records(new_keys.size(), [&](size_t i) { return new_keys[i]; }, threads);
    sort_keyed_records(olds, threads);
    sort_keyed_records(news, threads);
    std::vector<uint32_t> match(old_keys.size(), UINT32_MAX);
    std::mutex m;
    size_t calls = 0;
    pair_records_by_key(olds, news, [&](uint32_t o, uint32_t n) {
        std::lock_guard<std::mutex> lock(m);
        CHECK(match[o] == UINT32_MAX);
        match[o] = n;
        calls++;
    }, threads);
    Pairs out;
    for (uint32_t i = 0; i < match.size(); i++)
        if (match[i] != UINT32_MAX) out.push_back({i, match[i]});
    CHECK_EQ(calls, out.size());
    return out;
}

std::vector<uint64_t> random_keys(size_t n, uint64_t range, std::mt19937_64& rng) {
    std::vector<uint64_t> v(n);
    for (auto& k : v) k = rng() % range;
    return v;
}

}  // namespace

TEST(pair_records_by_key_matches_fifo_queues) {
    std::mt19937_64 rng(11);
    // Few keys: long duplicate groups on both sides, uneven counts.
    for (uint64_t range : {uint64_t(1), uint64_t(3), uint64_t(50), uint64_t(100000)}) {
        auto old_keys = random_keys(200000, range, rng);
        auto new_keys = random_keys(190000, range + range / 10, rng);
        Pairs expect = fifo_pairs(old_keys, new_keys);
        for (unsigned threads : {1u, 2u, 7u, 64u}) CHECK(sorted_pairs(old_keys, new_keys, threads) == expect);
    }
}

TEST(pair_records_by_key_edge_sizes) {
    std::mt19937_64 rng(5);
    for (size_t n_old : {size_t(0), size_t(1), size_t(9)}) {
        for (size_t n_new : {size_t(0), size_t(1), size_t(9)}) {
            auto old_keys = random_keys(n_old, 4, rng);
            auto new_keys = random_keys(n_new, 4, rng);
            CHECK(sorted_pairs(old_keys, new_keys, 3) == fifo_pairs(old_keys, new_keys));
        }
    }
}

TEST(pair_records_by_key_skips_keys_one_side_lacks) {
    std::vector<uint64_t> old_keys = {5, 1, 9, 1, 7, 1};
    std::vector<uint64_t> new_keys = {1, 2, 9, 9, 3, 1};
    Pairs expect = {{1, 0}, {2, 2}, {3, 5}};
    CHECK(sorted_pairs(old_keys, new_keys, 1) == expect);
    CHECK(sorted_pairs(old_keys, new_keys, 4) == expect);
}
