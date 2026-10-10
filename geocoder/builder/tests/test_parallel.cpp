// parallel_for / parallel_sort: results must not depend on the thread count.
#include "parallel.h"

#include <atomic>
#include <cstdint>
#include <random>
#include <stdexcept>
#include <tuple>

#include "test_framework.h"

namespace {

struct Rec {
    uint32_t key;
    uint64_t id;  // unique: (key, id) is a strict total order
    float payload;
};

bool rec_less(const Rec& a, const Rec& b) {
    return std::tie(a.key, a.id) < std::tie(b.key, b.id);
}

bool same(const std::vector<Rec>& a, const std::vector<Rec>& b) {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); i++)
        if (a[i].key != b[i].key || a[i].id != b[i].id || a[i].payload != b[i].payload) return false;
    return true;
}

std::vector<Rec> new_recs(size_t n, uint32_t key_range, uint32_t seed) {
    std::mt19937_64 rng(seed);
    std::vector<Rec> v(n);
    for (size_t i = 0; i < n; i++) v[i] = {static_cast<uint32_t>(rng() % key_range), i, static_cast<float>(rng() % 1000)};
    std::shuffle(v.begin(), v.end(), rng);
    return v;
}

}  // namespace

TEST(parallel_for_visits_every_index_once) {
    for (unsigned threads : {1u, 2u, 3u, 8u, 64u}) {
        for (size_t n : {size_t(0), size_t(1), size_t(5), size_t(1000)}) {
            std::vector<std::atomic<int>> hits(n);
            parallel_for(n, [&](size_t b, size_t e, unsigned) {
                for (size_t i = b; i < e; i++) hits[i]++;
            }, threads);
            for (size_t i = 0; i < n; i++) CHECK_EQ(hits[i].load(), 1);
        }
    }
}

TEST(parallel_for_rethrows_a_worker_exception) {
    bool thrown = false;
    try {
        parallel_for(100, [](size_t b, size_t, unsigned) {
            if (b == 0) throw std::runtime_error("boom");
        }, 4);
    } catch (const std::runtime_error&) {
        thrown = true;
    }
    CHECK(thrown);
}

TEST(parallel_find_all_lists_matches_in_order_for_any_thread_count) {
    for (size_t n : {size_t(0), size_t(1), size_t(7), size_t(100000)}) {
        auto pred = [](size_t i) { return (i * 2654435761u) % 7 < 2; };
        std::vector<size_t> expect;
        for (size_t i = 0; i < n; i++)
            if (pred(i)) expect.push_back(i);
        for (unsigned threads : {1u, 3u, 64u}) CHECK(parallel_find_all(n, pred, threads) == expect);
    }
}

TEST(parallel_sort_matches_std_sort_for_any_thread_count) {
    auto input = new_recs(1 << 20, 5000, 7);
    auto expect = input;
    std::sort(expect.begin(), expect.end(), rec_less);
    for (unsigned threads : {1u, 2u, 3u, 7u, 16u, 64u}) {
        auto v = input;
        parallel_sort(v.begin(), v.end(), rec_less, threads);
        CHECK(same(v, expect));
    }
}

TEST(parallel_sort_handles_heavy_identical_duplicates) {
    // Few distinct values: splitters repeat and slices go empty or lopsided.
    std::mt19937 rng(3);
    std::vector<uint64_t> input(600000);
    for (auto& x : input) x = rng() % 3;
    auto expect = input;
    std::sort(expect.begin(), expect.end());
    for (unsigned threads : {2u, 5u, 32u}) {
        auto v = input;
        parallel_sort(v.begin(), v.end(), std::less<uint64_t>(), threads);
        CHECK(v == expect);
    }
}

TEST(parallel_sort_small_and_presorted_inputs) {
    for (size_t n : {size_t(0), size_t(1), size_t(2), size_t(70000), size_t(300000)}) {
        std::vector<uint64_t> asc(n), desc(n);
        for (size_t i = 0; i < n; i++) { asc[i] = i; desc[i] = n - i; }
        auto a = asc, d = desc;
        parallel_sort(a.begin(), a.end(), std::less<uint64_t>(), 8);
        parallel_sort(d.begin(), d.end(), std::less<uint64_t>(), 8);
        std::sort(desc.begin(), desc.end());
        CHECK(a == asc);
        CHECK(d == desc);
    }
}
