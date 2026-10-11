#pragma once
// Dictionary encoding for per-thread parse buffers. Each distinct string is
// stored once (NUL-terminated, in blocks that never move) and named by a
// dense code in order of first add, so a parse thread keeps a 4-byte code
// per value instead of a std::string, and the merge interns each distinct
// string once rather than once per value (intern_dicts).
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "parallel.h"
#include "string_pool.h"
#include "types.h"

class StringDict {
public:
    // A code callers store for "no string"; DictIds maps it to NO_DATA.
    static constexpr uint32_t kNone = UINT32_MAX;

    uint32_t add(const char* s) { return add(s, std::strlen(s)); }

    // `s` holds no NUL in its first `len` bytes.
    uint32_t add(const char* s, size_t len) {
        if ((strs_.size() + 1) * 2 > table_.size()) grow();
        uint32_t h = static_cast<uint32_t>(std::hash<std::string_view>()(std::string_view(s, len)));
        size_t mask = table_.size() - 1;
        for (size_t i = h & mask;; i = (i + 1) & mask) {
            Slot& slot = table_[i];
            if (slot.code == kEmpty) {
                slot = {h, static_cast<uint32_t>(strs_.size())};
                strs_.push_back(store(s, len));
                return slot.code;
            }
            const char* stored = strs_[slot.code];
            if (slot.hash == h && std::strncmp(stored, s, len) == 0 && stored[len] == '\0')
                return slot.code;
        }
    }

    const char* get(uint32_t code) const { return strs_[code]; }
    size_t size() const { return strs_.size(); }

private:
    struct Slot { uint32_t hash; uint32_t code; };
    static constexpr uint32_t kEmpty = UINT32_MAX;
    static constexpr size_t kBlockBytes = size_t(1) << 20;

    void grow() {
        std::vector<Slot> bigger(std::max<size_t>(1024, table_.size() * 2), Slot{0, kEmpty});
        size_t mask = bigger.size() - 1;
        for (const Slot& slot : table_) {
            if (slot.code == kEmpty) continue;
            size_t i = slot.hash & mask;
            while (bigger[i].code != kEmpty) i = (i + 1) & mask;
            bigger[i] = slot;
        }
        table_.swap(bigger);
    }

    const char* store(const char* s, size_t len) {
        size_t need = len + 1;
        if (blocks_.empty() || used_ + need > block_size_) {
            block_size_ = std::max(kBlockBytes, need);
            blocks_.emplace_back(new char[block_size_]);
            used_ = 0;
        }
        char* dst = blocks_.back().get() + used_;
        std::memcpy(dst, s, len);
        dst[len] = '\0';
        used_ += need;
        return dst;
    }

    std::vector<Slot> table_;          // open addressing, at most half full
    std::vector<const char*> strs_;    // by code
    std::vector<std::unique_ptr<char[]>> blocks_;
    size_t block_size_ = 0;  // of blocks_.back()
    size_t used_ = 0;        // bytes used in blocks_.back()
};

// Pool ids for one StringDict's codes, as intern_dicts left them.
class DictIds {
public:
    explicit DictIds(std::vector<uint32_t> ids) : ids_(std::move(ids)) {}

    uint32_t operator()(uint32_t code) const {
        return code == StringDict::kNone ? NO_DATA : ids_[code];
    }

private:
    std::vector<uint32_t> ids_;
};

// Interns the strings a merge meets, on every core, leaving the pool and
// the ids exactly as interning each value in merge order would: strings not
// yet pooled are appended in the order the merge first meets them. The merge
// is a list of runs in order; run r reads dicts[run_dict[r]], and walk(r,
// emit) calls emit(code) for each code it reads, in order (kNone allowed).
// walk runs on many threads at once, one run each.
template <class Walk>
std::vector<DictIds> intern_dicts(const std::vector<const StringDict*>& dicts,
                                  const std::vector<uint32_t>& run_dict, Walk walk,
                                  StringPool& pool, unsigned threads = 0) {
    // A code's first meet: run << 32 | position in the run, which orders
    // meets across dicts as the serial merge does.
    constexpr uint64_t kNever = UINT64_MAX;
    // Equal strings share a shard, so each shard dedupes on its own.
    constexpr size_t kShards = 256;
    struct Meet {
        uint64_t at;  // first meet; after dedup, the string's index in its shard
        uint32_t code;
        uint32_t hash;
    };
    const size_t n_dicts = dicts.size();
    std::vector<std::vector<uint32_t>> runs_of(n_dicts);
    for (uint32_t r = 0; r < run_dict.size(); r++) runs_of[run_dict[r]].push_back(r);

    // meets[d * kShards + shard]: the codes of dict d the merge reads.
    std::vector<std::vector<Meet>> meets(n_dicts * kShards);
    parallel_for_each(n_dicts, [&](size_t d, unsigned) {
        std::vector<uint64_t> first(dicts[d]->size(), kNever);
        for (uint32_t r : runs_of[d]) {
            uint64_t pos = 0;
            walk(r, [&](uint32_t code) {
                if (pos > UINT32_MAX) throw std::length_error("intern_dicts: run too long");
                if (code != StringDict::kNone && first[code] == kNever)
                    first[code] = (uint64_t(r) << 32) | pos;
                pos++;
            });
        }
        for (uint32_t code = 0; code < first.size(); code++) {
            if (first[code] == kNever) continue;

            size_t h = std::hash<std::string_view>()(dicts[d]->get(code));
            meets[d * kShards + h % kShards].push_back(
                {first[code], code, static_cast<uint32_t>(h / kShards)});
        }
    }, threads);

    // Each shard's distinct strings with their first meet over all dicts
    // and their pool id when already pooled.
    struct Distinct {
        const char* str;
        uint64_t first;
        uint32_t id;
    };
    std::vector<std::vector<Distinct>> distinct(kShards);
    std::vector<size_t> fresh_count(kShards);
    parallel_for_each(kShards, [&](size_t shard, unsigned) {
        size_t n = 0;
        for (size_t d = 0; d < n_dicts; d++) n += meets[d * kShards + shard].size();
        size_t cap = 16;
        while (cap < 2 * n) cap *= 2;
        std::vector<uint32_t> table(cap, UINT32_MAX);  // open addressing into strs
        std::vector<uint32_t> hashes;                  // parallel to strs
        auto& strs = distinct[shard];
        for (size_t d = 0; d < n_dicts; d++) {
            for (Meet& m : meets[d * kShards + shard]) {
                const char* str = dicts[d]->get(m.code);
                size_t i = m.hash & (cap - 1);
                while (table[i] != UINT32_MAX &&
                       (hashes[table[i]] != m.hash || std::strcmp(strs[table[i]].str, str) != 0))
                    i = (i + 1) & (cap - 1);
                if (table[i] == UINT32_MAX) {
                    table[i] = static_cast<uint32_t>(strs.size());
                    strs.push_back({str, m.at, StringPool::kAbsent});
                    hashes.push_back(m.hash);
                } else {
                    strs[table[i]].first = std::min(strs[table[i]].first, m.at);
                }
                m.at = table[i];
            }
        }
        std::string buf;
        for (auto& s : strs) {
            buf.assign(s.str);
            s.id = pool.find(buf);
            fresh_count[shard] += s.id == StringPool::kAbsent;
        }
    }, threads);

    // First meets are unique (each names one code of one dict), so the new
    // strings have one order.
    struct Fresh {
        uint64_t first;
        uint32_t shard;
        uint32_t index;
    };
    std::vector<size_t> fresh_at(kShards + 1, 0);
    for (size_t shard = 0; shard < kShards; shard++) fresh_at[shard + 1] = fresh_at[shard] + fresh_count[shard];
    std::vector<Fresh> fresh(fresh_at[kShards]);
    parallel_for_each(kShards, [&](size_t shard, unsigned) {
        size_t at = fresh_at[shard];
        for (uint32_t i = 0; i < distinct[shard].size(); i++)
            if (distinct[shard][i].id == StringPool::kAbsent)
                fresh[at++] = {distinct[shard][i].first, static_cast<uint32_t>(shard), i};
    }, threads);
    parallel_sort(fresh.begin(), fresh.end(),
                  [](const Fresh& a, const Fresh& b) { return a.first < b.first; }, threads);
    std::vector<std::string_view> fresh_strs(fresh.size());
    parallel_for(fresh.size(), [&](size_t b, size_t e, unsigned) {
        for (size_t i = b; i < e; i++) fresh_strs[i] = distinct[fresh[i].shard][fresh[i].index].str;
    }, threads);
    std::vector<uint32_t> fresh_ids = pool.append_new(fresh_strs, threads);
    parallel_for(fresh.size(), [&](size_t b, size_t e, unsigned) {
        for (size_t i = b; i < e; i++) distinct[fresh[i].shard][fresh[i].index].id = fresh_ids[i];
    }, threads);

    std::vector<std::vector<uint32_t>> ids(n_dicts);
    for (size_t d = 0; d < n_dicts; d++) ids[d].assign(dicts[d]->size(), NO_DATA);
    parallel_for_each(kShards, [&](size_t shard, unsigned) {
        for (size_t d = 0; d < n_dicts; d++)
            for (const Meet& m : meets[d * kShards + shard]) ids[d][m.code] = distinct[shard][m.at].id;
    }, threads);

    std::vector<DictIds> out;
    out.reserve(n_dicts);
    for (auto& v : ids) out.emplace_back(std::move(v));
    return out;
}
