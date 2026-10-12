// Matching records of an old and a new build by a content key, with sorted
// (key, index) arrays: 12 bytes a record where a hash map of per-key queues
// cost ~720 (an empty std::deque allocates 576 bytes), 39 GB for planet ways.
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "parallel.h"

#pragma pack(push, 4)
struct KeyedRecord {
    uint64_t key;
    uint32_t idx;
};
#pragma pack(pop)
static_assert(sizeof(KeyedRecord) == 12, "KeyedRecord must pack to 12 bytes");

// keys[i] = {key_of(i), i} for i < n, computed on every core.
template <class KeyOf>
std::vector<KeyedRecord> keyed_records(size_t n, KeyOf key_of, unsigned threads = 0) {
    std::vector<KeyedRecord> keys(n);
    parallel_for(n, [&](size_t b, size_t e, unsigned) {
        for (size_t i = b; i < e; i++) keys[i] = {key_of(i), static_cast<uint32_t>(i)};
    }, threads);
    return keys;
}

// Sorts by (key, idx). Indices are unique, so the order is total and the
// result the same for any thread count.
inline void sort_keyed_records(std::vector<KeyedRecord>& keys, unsigned threads = 0) {
    parallel_sort(keys.begin(), keys.end(), [](const KeyedRecord& a, const KeyedRecord& b) {
        return a.key != b.key ? a.key < b.key : a.idx < b.idx;
    }, threads);
}

// Calls on_pair(old_idx, new_idx) for the k-th old and the k-th new record
// of every key both sides hold, k below the smaller count: what a FIFO queue
// of new indices per key gives old records visited in index order. Both
// inputs sorted by sort_keyed_records. Key groups are split across threads,
// so on_pair runs concurrently, but never twice for one record.
template <class OnPair>
void pair_records_by_key(const std::vector<KeyedRecord>& olds, const std::vector<KeyedRecord>& news,
                         OnPair on_pair, unsigned threads = 0) {
    auto group_start = [&](size_t i) {
        while (i > 0 && i < olds.size() && olds[i].key == olds[i - 1].key) i++;
        return i;
    };
    auto key_less = [](const KeyedRecord& r, uint64_t k) { return r.key < k; };
    parallel_for(olds.size(), [&](size_t begin, size_t end, unsigned) {
        size_t o = group_start(begin), o_end = group_start(end);
        if (o >= o_end) return;
        // By value: the packed key is only 4-byte aligned, so lower_bound's
        // const& must not bind to it.
        uint64_t first_key = olds[o].key;
        size_t n = static_cast<size_t>(std::lower_bound(news.begin(), news.end(), first_key, key_less) - news.begin());
        while (o < o_end && n < news.size()) {
            uint64_t key = olds[o].key;
            while (n < news.size() && news[n].key < key) n++;
            for (; o < o_end && olds[o].key == key; o++)
                if (n < news.size() && news[n].key == key) on_pair(olds[o].idx, news[n++].idx);
        }
    }, threads);
}
