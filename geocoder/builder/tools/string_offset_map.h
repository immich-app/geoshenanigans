// geocoder-diff's string offset remap: each old string start whose string the
// new pools still hold → that string's new start. Held as sorted pairs, 8
// bytes a string, where the unordered_map it was (and the string-keyed map
// that built it) cost ~125 bytes a string in every variant's diff.
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <utility>
#include <vector>

#include "parallel.h"
#include "record_match.h"

// Sorted, unique u32 keys → u32 values. A directory over the keys' high bits
// sends a lookup straight to a bucket of a few keys.
class SortedU32Map {
public:
    using Pair = std::pair<uint32_t, uint32_t>;

    SortedU32Map() = default;
    // pairs: sorted by key, keys unique.
    explicit SortedU32Map(std::vector<Pair> pairs) : pairs_(std::move(pairs)) {
        if (pairs_.empty()) return;
        uint64_t span = (uint64_t)pairs_.back().first + 1;
        uint64_t width = std::max<uint64_t>(1, 8 * span / pairs_.size());  // ~8 keys a bucket
        while ((uint64_t(2) << shift_) <= width) shift_++;
        size_t buckets = bucket(pairs_.back().first) + 1;
        dir_.resize(buckets + 1);
        size_t at = 0;
        for (size_t b = 0; b <= buckets; b++) {
            while (at < pairs_.size() && bucket(pairs_[at].first) < b) at++;
            dir_[b] = static_cast<uint32_t>(at);
        }
    }

    // The value of key, or nullptr.
    const uint32_t* find(uint32_t key) const {
        size_t b = bucket(key);
        if (b + 1 >= dir_.size()) return nullptr;
        for (uint32_t i = dir_[b], end = dir_[b + 1]; i < end; i++) {
            if (pairs_[i].first == key) return &pairs_[i].second;
            if (pairs_[i].first > key) break;
        }
        return nullptr;
    }
    // key's value, or key itself when absent.
    uint32_t map(uint32_t key) const {
        const uint32_t* v = find(key);
        return v ? *v : key;
    }

    size_t size() const { return pairs_.size(); }
    std::vector<Pair>::const_iterator begin() const { return pairs_.begin(); }
    std::vector<Pair>::const_iterator end() const { return pairs_.end(); }

private:
    // 64-bit: a sparse map can need a shift of 32 or more.
    size_t bucket(uint32_t key) const { return static_cast<size_t>(uint64_t(key) >> shift_); }

    std::vector<Pair> pairs_;
    std::vector<uint32_t> dir_;
    unsigned shift_ = 0;
};

// A string pool's bytes at global offsets [base, base + size).
struct StringPoolSegment {
    const char* data;
    size_t size;
    uint32_t base;
};

// Pools laid end to end from offset 0 (the string tiers' global offsets), as
// segments. A pool without a final NUL ran its last string on into the next
// pool when the remap walked one concatenated pool, so such a side is still
// walked concatenated, in `concat`, which must outlive the segments.
inline std::vector<StringPoolSegment> string_pool_segments(const std::vector<std::pair<const char*, size_t>>& pools,
                                                           std::vector<char>& concat) {
    std::vector<StringPoolSegment> segs;
    uint32_t base = 0;
    bool terminated = true;
    for (const auto& [data, size] : pools) {
        if (size > 0) {
            segs.push_back({data, size, base});
            terminated = terminated && data[size - 1] == '\0';
        }
        base += static_cast<uint32_t>(size);
    }
    if (terminated) return segs;
    for (const auto& seg : segs) concat.insert(concat.end(), seg.data, seg.data + seg.size);
    return {{concat.data(), concat.size(), 0}};
}

// The (old start, new start) pairs of every old string the new pools hold,
// in old order, each to the string's last new start: what a map from string
// to start filled in pool order and looked up by each old string gives.
// Segments lie in ascending global order; a string runs to its NUL or its
// segment's end.
inline std::vector<SortedU32Map::Pair> string_offset_pairs(const std::vector<StringPoolSegment>& olds,
                                                           const std::vector<StringPoolSegment>& news,
                                                           unsigned threads = 0) {
    struct Str { const char* p; size_t len; };
    auto starts_of = [](const std::vector<StringPoolSegment>& segs) {
        std::vector<uint32_t> starts;
        for (const auto& s : segs)
            for (size_t pos = 0; pos < s.size; pos += strnlen(s.data + pos, s.size - pos) + 1)
                starts.push_back(s.base + static_cast<uint32_t>(pos));
        return starts;
    };
    auto str_at = [](const std::vector<StringPoolSegment>& segs, uint32_t start) {
        size_t k = segs.size() - 1;
        while (start < segs[k].base) k--;  // segments are few
        size_t pos = start - segs[k].base;
        return Str{segs[k].data + pos, strnlen(segs[k].data + pos, segs[k].size - pos)};
    };
    auto hash = [](Str s) {
        uint64_t h = 14695981039346656037ULL;
        for (size_t i = 0; i < s.len; i++) { h ^= (uint8_t)s.p[i]; h *= 1099511628211ULL; }
        return h;
    };

    std::vector<uint32_t> new_starts = starts_of(news);
    auto by_hash = keyed_records(new_starts.size(), [&](size_t i) { return hash(str_at(news, new_starts[i])); }, threads);
    for (auto& r : by_hash) r.idx = new_starts[r.idx];
    sort_keyed_records(by_hash, threads);
    std::vector<uint32_t>().swap(new_starts);

    constexpr uint32_t ABSENT = UINT32_MAX;  // no start: offsets stay below the 4 GiB pool limit
    std::vector<uint32_t> old_starts = starts_of(olds);
    std::vector<uint32_t> mapped(old_starts.size(), ABSENT);
    parallel_for(old_starts.size(), [&](size_t b, size_t e, unsigned) {
        for (size_t i = b; i < e; i++) {
            Str s = str_at(olds, old_starts[i]);
            uint64_t h = hash(s);
            auto lo = std::lower_bound(by_hash.begin(), by_hash.end(), h,
                                       [](const KeyedRecord& r, uint64_t k) { return r.key < k; });
            auto hi = lo;
            while (hi != by_hash.end() && hi->key == h) ++hi;
            for (auto it = hi; it != lo;) {  // the last start first
                --it;
                Str n = str_at(news, it->idx);
                if (n.len == s.len && memcmp(n.p, s.p, s.len) == 0) { mapped[i] = it->idx; break; }
            }
        }
    }, threads);

    std::vector<SortedU32Map::Pair> pairs;
    for (size_t i = 0; i < old_starts.size(); i++)
        if (mapped[i] != ABSENT) pairs.push_back({old_starts[i], mapped[i]});
    return pairs;
}
