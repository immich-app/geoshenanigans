// parallel.h helpers: results must not depend on the thread count.
#include "parallel.h"

#include <atomic>
#include <mutex>
#include <cstdint>
#include <random>
#include <stdexcept>
#include <string>
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

// A chain of records: 0xAB 0xCD, a little-endian u16 payload length, the
// payload. Payloads are random and often hold a fake 0xAB 0xCD.
struct ChainRecord {
    size_t offset, length;
    bool operator==(const ChainRecord& o) const { return offset == o.offset && length == o.length; }
};

std::vector<uint8_t> new_chain(size_t records, uint32_t seed) {
    std::mt19937 rng(seed);
    std::vector<uint8_t> bytes;
    for (size_t r = 0; r < records; r++) {
        size_t length = rng() % 300;
        bytes.insert(bytes.end(), {0xAB, 0xCD, static_cast<uint8_t>(length), static_cast<uint8_t>(length >> 8)});
        for (size_t i = 0; i < length; i++) bytes.push_back(static_cast<uint8_t>(rng()));
        if (length >= 6 && rng() % 2) {
            size_t at = bytes.size() - length + rng() % (length - 5);
            bytes[at] = 0xAB;
            bytes[at + 1] = 0xCD;
        }
    }
    return bytes;
}

size_t walk_chain(const std::vector<uint8_t>& bytes, size_t offset, size_t stop, std::vector<ChainRecord>& out) {
    while (offset < stop) {
        if (offset + 4 > bytes.size() || bytes[offset] != 0xAB || bytes[offset + 1] != 0xCD)
            throw std::runtime_error("bad record at " + std::to_string(offset));
        size_t length = bytes[offset + 2] | (size_t(bytes[offset + 3]) << 8);
        out.push_back({offset, length});
        offset += 4 + length;
    }
    return offset;
}

size_t find_chain_start(const std::vector<uint8_t>& bytes, size_t from, size_t to) {
    for (size_t p = from; p < to && p + 1 < bytes.size(); p++)
        if (bytes[p] == 0xAB && bytes[p + 1] == 0xCD) return p;
    return to;
}

std::vector<ChainRecord> chain_walk(const std::vector<uint8_t>& bytes, size_t stripes, unsigned threads) {
    return parallel_chain_walk<ChainRecord>(
        bytes.size(), stripes,
        [&](size_t from, size_t to) { return find_chain_start(bytes, from, to); },
        [&](size_t offset, size_t stop, std::vector<ChainRecord>& out) { return walk_chain(bytes, offset, stop, out); },
        threads);
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

TEST(parallel_for_each_visits_every_index_once) {
    for (unsigned threads : {1u, 2u, 3u, 8u, 64u}) {
        for (size_t n : {size_t(0), size_t(1), size_t(5), size_t(1000)}) {
            std::vector<std::atomic<int>> hits(n);
            std::atomic<bool> worker_in_range{true};
            parallel_for_each(n, [&](size_t i, unsigned worker) {
                hits[i]++;
                if (worker >= threads) worker_in_range = false;
            }, threads);
            for (size_t i = 0; i < n; i++) CHECK_EQ(hits[i].load(), 1);
            CHECK(worker_in_range.load());
        }
    }
}

TEST(parallel_for_each_rethrows_a_worker_exception) {
    bool thrown = false;
    try {
        parallel_for_each(100, [](size_t i, unsigned) {
            if (i == 42) throw std::runtime_error("boom");
        }, 4);
    } catch (const std::runtime_error&) {
        thrown = true;
    }
    CHECK(thrown);
}

TEST(parallel_ordered_consumes_in_index_order) {
    for (unsigned threads : {1u, 2u, 3u, 8u, 64u}) {
        for (size_t window : {size_t(0), size_t(1), size_t(4)}) {
            for (size_t n : {size_t(0), size_t(1), size_t(7), size_t(500)}) {
                std::vector<size_t> seen;
                parallel_ordered(n, [](size_t i) { return std::vector<size_t>(i % 5, i); },
                                 [&](size_t i, std::vector<size_t>&& r) {
                                     CHECK_EQ(r.size(), i % 5);
                                     seen.push_back(i);
                                 }, threads, window);
                std::vector<size_t> expect(n);
                for (size_t i = 0; i < n; i++) expect[i] = i;
                CHECK(seen == expect);
            }
        }
    }
}

TEST(parallel_ordered_never_runs_more_than_window_ahead) {
    std::atomic<size_t> consumed{0};
    std::atomic<bool> too_far{false};
    parallel_ordered(300, [&](size_t i) {
        if (i >= consumed.load() + 3) too_far = true;
        return i;
    }, [&](size_t, size_t&&) { consumed++; }, 8, 3);
    CHECK(!too_far.load());
    CHECK_EQ(consumed.load(), size_t(300));
}

TEST(parallel_ordered_rethrows_from_either_side) {
    for (bool in_produce : {true, false}) {
        size_t consumed = 0;
        bool thrown = false;
        try {
            parallel_ordered(1000, [&](size_t i) {
                if (in_produce && i == 10) throw std::runtime_error("produce");
                return i;
            }, [&](size_t i, size_t&&) {
                if (!in_produce && i == 10) throw std::runtime_error("consume");
                consumed++;
            }, 4);
        } catch (const std::runtime_error&) {
            thrown = true;
        }
        CHECK(thrown);
        CHECK_EQ(consumed, size_t(10));
    }
}

TEST(parallel_chain_walk_matches_the_serial_walk) {
    for (uint32_t seed : {1u, 2u, 3u}) {
        auto bytes = new_chain(5000, seed);
        std::vector<ChainRecord> expect;
        walk_chain(bytes, 0, bytes.size(), expect);
        // Stripes far smaller than a record leave many with no start at all.
        for (size_t stripes : {size_t(1), size_t(2), size_t(7), size_t(64), size_t(5000), size_t(100000)}) {
            for (unsigned threads : {1u, 2u, 3u, 16u}) {
                CHECK(chain_walk(bytes, stripes, threads) == expect);
            }
        }
    }
}

TEST(parallel_chain_walk_throws_the_serial_walks_error) {
    auto bytes = new_chain(3000, 9);
    std::vector<ChainRecord> records;
    walk_chain(bytes, 0, bytes.size(), records);
    bytes[records[2000].offset] = 0;
    std::string expect;
    try {
        std::vector<ChainRecord> out;
        walk_chain(bytes, 0, bytes.size(), out);
    } catch (const std::runtime_error& e) {
        expect = e.what();
    }
    REQUIRE(!expect.empty());
    for (size_t stripes : {size_t(2), size_t(40), size_t(3000)}) {
        std::string got;
        try {
            chain_walk(bytes, stripes, 8);
        } catch (const std::runtime_error& e) {
            got = e.what();
        }
        CHECK_EQ(got, expect);
    }
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

TEST(parallel_for_dynamic_visits_every_index_once) {
    for (unsigned threads : {1u, 2u, 7u, 64u}) {
        for (size_t grain : {size_t(0), size_t(1), size_t(3), size_t(1000)}) {
            for (size_t n : {size_t(0), size_t(1), size_t(10), size_t(2500)}) {
                std::vector<std::atomic<int>> hits(n);
                std::atomic<bool> chunks_in_bounds{true};
                parallel_for_dynamic(n, grain, [&](size_t b, size_t e, unsigned worker) {
                    if (worker >= threads || e - b > std::max<size_t>(grain, 1)) chunks_in_bounds = false;
                    for (size_t i = b; i < e; i++) hits[i]++;
                }, threads);
                for (size_t i = 0; i < n; i++) CHECK_EQ(hits[i].load(), 1);
                CHECK(chunks_in_bounds.load());
            }
        }
    }
}

TEST(parallel_for_dynamic_rethrows_a_worker_exception) {
    bool thrown = false;
    try {
        parallel_for_dynamic(100, 10, [](size_t b, size_t, unsigned) {
            if (b == 50) throw std::runtime_error("boom");
        }, 4);
    } catch (const std::runtime_error&) {
        thrown = true;
    }
    CHECK(thrown);
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

TEST(parallel_offsets_are_exclusive_prefix_sums_for_any_thread_count) {
    for (size_t n : {size_t(0), size_t(1), size_t(1000), size_t(200000)}) {
        std::vector<uint64_t> expect(n + 1, 0);
        for (size_t i = 0; i < n; i++) expect[i + 1] = expect[i] + i % 7;
        for (unsigned threads : {1u, 3u, 64u}) {
            auto got = parallel_offsets<uint64_t>(n, [](size_t i) { return i % 7; }, threads);
            CHECK(got == expect);
        }
    }
}

TEST(parallel_filter_keeps_matching_indices_in_order) {
    for (size_t n : {size_t(0), size_t(5), size_t(300000)}) {
        std::vector<uint32_t> expect;
        for (size_t i = 0; i < n; i++)
            if (i % 3 == 1) expect.push_back(static_cast<uint32_t>(i));
        for (unsigned threads : {1u, 4u, 64u}) {
            std::atomic<size_t> calls{0};
            auto got = parallel_filter(n, [&](size_t i) { calls++; return i % 3 == 1; }, threads);
            CHECK(got == expect);
            CHECK_EQ(calls.load(), n);
        }
    }
}

TEST(parallel_any_finds_a_single_match) {
    for (unsigned threads : {1u, 3u, 64u}) {
        CHECK(parallel_any(100000, [](size_t i) { return i == 99999; }, threads));
        CHECK(!parallel_any(100000, [](size_t) { return false; }, threads));
        CHECK(!parallel_any(0, [](size_t) { return true; }, threads));
    }
}

TEST(parallel_sort_indices_matches_std_sort_without_ties) {
    auto recs = new_recs(400000, 3000, 9);
    auto less = [&](uint32_t a, uint32_t b) { return rec_less(recs[a], recs[b]); };
    auto expect = std_sort_indices(recs.size(), less);
    for (unsigned threads : {1u, 3u, 16u}) {
        auto got = parallel_sort_indices(recs.size(), less, [](uint32_t, uint32_t) { return true; }, threads);
        CHECK(!got.serial);
        CHECK(got.order == expect);
    }
}

TEST(parallel_sort_indices_keeps_std_sort_order_when_ties_matter) {
    auto recs = new_recs(400000, 3000, 10);
    auto by_key = [&](uint32_t a, uint32_t b) { return recs[a].key < recs[b].key; };
    auto expect = std_sort_indices(recs.size(), by_key);
    auto payload_differs = [&](uint32_t a, uint32_t b) { return recs[a].payload != recs[b].payload; };
    auto got = parallel_sort_indices(recs.size(), by_key, payload_differs, 8);
    CHECK(got.serial);
    CHECK(got.order == expect);

    // Ties whose order can't show stay parallel: any order sorted by key.
    auto never = parallel_sort_indices(recs.size(), by_key, [](uint32_t, uint32_t) { return false; }, 8);
    CHECK(!never.serial);
    CHECK(std::is_sorted(never.order.begin(), never.order.end(), by_key));
    auto perm = never.order;
    std::sort(perm.begin(), perm.end());
    bool is_perm = true;
    for (size_t i = 0; i < perm.size(); i++) is_perm = is_perm && perm[i] == i;
    CHECK(is_perm);
}

TEST(parallel_sort_keys_orders_like_std_sort_indices) {
    auto recs = new_recs(400000, 3000, 11);
    struct Key {
        uint32_t key;
        uint32_t index;
    };
    auto key_of = [&](size_t i) { return Key{recs[i].key, static_cast<uint32_t>(i)}; };
    auto key_less = [](const Key& a, const Key& b) { return a.key < b.key; };
    auto by_key = [&](uint32_t a, uint32_t b) { return recs[a].key < recs[b].key; };
    auto payload_differs = [&](uint32_t a, uint32_t b) { return recs[a].payload != recs[b].payload; };
    auto expect = std_sort_indices(recs.size(), by_key);
    for (unsigned threads : {1u, 3u, 16u}) {
        // Ties that show take the serial sort, which must tie-break as
        // std::sort over bare indices does.
        auto got = parallel_sort_keys(recs.size(), key_of, key_less, payload_differs, threads);
        CHECK(got.order == expect);
        CHECK(got.serial);
    }
}

TEST(parallel_sort_runs_sorts_each_run_alone) {
    std::mt19937 rng(4);
    std::vector<std::pair<uint32_t, uint32_t>> input;  // (run, value), runs adjacent
    for (uint32_t run = 0; input.size() < 300000; run++)
        for (uint32_t k = rng() % (run % 50 == 0 ? 40000 : 30); k > 0; k--) input.push_back({run, rng() % 100});
    auto same_run = [](const auto& a, const auto& b) { return a.first == b.first; };
    auto expect = input;
    std::sort(expect.begin(), expect.end());
    for (unsigned threads : {1u, 3u, 64u}) {
        auto v = input;
        parallel_sort_runs(v.begin(), v.end(), same_run, std::less<std::pair<uint32_t, uint32_t>>(), threads);
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

TEST(parallel_for_dynamic_hands_out_grain_sized_ranges) {
    for (unsigned threads : {1u, 3u, 16u}) {
        for (size_t grain : {size_t(1), size_t(7), size_t(5000)}) {
            for (size_t n : {size_t(0), size_t(1), size_t(1000)}) {
                std::vector<std::atomic<int>> hits(n);
                std::atomic<bool> worker_in_range{true};
                parallel_for_dynamic(n, grain, [&](size_t b, size_t e, unsigned w) {
                    if (w >= threads || e - b > grain) worker_in_range = false;
                    for (size_t i = b; i < e; i++) hits[i]++;
                }, threads);
                CHECK(worker_in_range.load());
                for (size_t i = 0; i < n; i++) CHECK_EQ(hits[i].load(), 1);
            }
        }
    }
}

TEST(parallel_prefix_fill_places_items_back_to_back_in_order) {
    std::mt19937 rng(21);
    for (size_t n : {size_t(0), size_t(1), size_t(9), size_t(5000)}) {
        std::vector<size_t> sizes(n);
        for (auto& s : sizes) s = rng() % 4 == 0 ? 0 : rng() % 50;
        std::vector<size_t> want(n);
        size_t total = 0;
        for (size_t i = 0; i < n; i++) { want[i] = total; total += sizes[i]; }
        for (unsigned threads : {1u, 3u, 16u}) {
            std::vector<size_t> got(n, SIZE_MAX);
            int allocs = 0;
            size_t allocated = SIZE_MAX;
            size_t returned = parallel_prefix_fill(n, [&](size_t i) { return sizes[i]; },
                [&](size_t t) { allocs++; allocated = t; },
                [&](size_t i, size_t offset) { got[i] = offset; return sizes[i]; }, threads);
            CHECK_EQ(allocs, 1);
            CHECK_EQ(allocated, total);
            CHECK_EQ(returned, total);
            CHECK(got == want);
        }
    }
}

TEST(parallel_for_runs_covers_every_index_in_whole_runs) {
    std::mt19937 rng(11);
    std::vector<uint32_t> keys(5000);
    uint32_t key = 0;
    for (auto& k : keys) { if (rng() % 4 == 0) key++; k = key; }
    for (size_t i = 100; i < 2600; i++) keys[i] = keys[99];  // longer than any worker's share
    auto same_run = [&](size_t i) { return keys[i] == keys[i - 1]; };
    for (unsigned threads : {1u, 2u, 3u, 8u, 64u}) {
        std::vector<int> hits(keys.size(), 0);
        std::vector<std::pair<size_t, size_t>> ranges(threads, {0, 0});
        std::atomic<bool> worker_in_range{true};
        parallel_for_runs(keys.size(), same_run, [&](size_t b, size_t e, unsigned w) {
            if (w >= threads) { worker_in_range = false; return; }
            ranges[w] = {b, e};
            for (size_t i = b; i < e; i++) hits[i]++;
        }, threads);
        CHECK(worker_in_range.load());
        for (int h : hits) CHECK_EQ(h, 1);
        size_t next = 0;
        for (const auto& [b, e] : ranges) {
            if (b == e) continue;
            CHECK_EQ(b, next);  // ranges numbered in order
            if (b > 0) CHECK(!same_run(b));
            next = e;
        }
        CHECK_EQ(next, keys.size());
    }
}

TEST(parallel_for_runs_keeps_a_single_run_whole) {
    std::vector<size_t> calls;
    std::mutex m;
    parallel_for_runs(1000, [](size_t) { return true; }, [&](size_t b, size_t e, unsigned) {
        std::lock_guard<std::mutex> lk(m);
        calls.push_back(e - b);
    }, 8);
    CHECK(calls == std::vector<size_t>({1000}));
    calls.clear();
    parallel_for_runs(0, [](size_t) { return false; }, [&](size_t, size_t, unsigned) { calls.push_back(1); }, 8);
    CHECK(calls.empty());
}
