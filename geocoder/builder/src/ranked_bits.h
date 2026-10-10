// Bit sets over planet-sized position ranges: marked from many threads at
// once, then ranked so a set position maps to its index among the set ones.
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "bits.h"
#include "parallel.h"

// Sets bit i of words, from many threads at once. A read first: a word many
// marks share (a hot string, a cell's records) would otherwise bounce its
// cache line between the cores.
inline void mark_bit(std::vector<std::atomic<uint64_t>>& words, size_t i) {
    std::atomic<uint64_t>& word = words[i / 64];
    const uint64_t bit = uint64_t(1) << (i % 64);
    if (!(word.load(std::memory_order_relaxed) & bit)) word.fetch_or(bit, std::memory_order_relaxed);
}

// Positions [0, n) as bits, each 64-bit word ranked by the ones before it:
// a lookup is one word load, where a hash map probe would miss the cache
// several times. Fewer than 2^32 ones.
class RankedBits {
public:
    static constexpr size_t npos = SIZE_MAX;

    // word(w) gives positions [64w, 64w + 64); none at or past n is set.
    template <class Word>
    RankedBits(size_t n, Word word, unsigned threads = 0) : size_(n) {
        parallel_resize(blocks_, (n + 63) / 64);
        parallel_for(blocks_.size(), [&](size_t b0, size_t b1, unsigned) {
            for (size_t b = b0; b < b1; b++) blocks_[b].bits = word(b);
        }, threads);
        auto ranks = parallel_offsets<uint32_t>(blocks_.size(), [&](size_t b) {
            return popcount64(blocks_[b].bits);
        }, threads);
        parallel_for(blocks_.size(), [&](size_t b0, size_t b1, unsigned) {
            for (size_t b = b0; b < b1; b++) blocks_[b].rank = ranks[b];
        }, threads);
        count_ = ranks.back();
    }

    // Marked from many threads (mark_bit).
    RankedBits(size_t n, const std::vector<std::atomic<uint64_t>>& words, unsigned threads = 0)
        : RankedBits(n, [&](size_t w) { return words[w].load(std::memory_order_relaxed); }, threads) {}

    size_t count() const { return count_; }

    // Index of position i among the set ones, or npos when it is clear.
    size_t index(size_t i) const {
        if (i >= size_) return npos;
        const Block& b = blocks_[i >> 6];
        uint64_t bit = uint64_t(1) << (i & 63);
        if (!(b.bits & bit)) return npos;
        return b.rank + static_cast<size_t>(popcount64(b.bits & (bit - 1)));
    }

    // The set positions, ascending.
    std::vector<uint32_t> ones(unsigned threads = 0) const {
        std::vector<uint32_t> out;
        parallel_resize(out, count_);
        parallel_for(blocks_.size(), [&](size_t b0, size_t b1, unsigned) {
            for (size_t b = b0; b < b1; b++) {
                size_t s = blocks_[b].rank;
                for (uint64_t bits = blocks_[b].bits; bits; bits &= bits - 1)
                    out[s++] = static_cast<uint32_t>(b * 64 + __builtin_ctzll(bits));
            }
        }, threads);
        return out;
    }

private:
    struct Block {
        uint64_t bits;
        uint32_t rank;  // ones before this block
    };
    size_t size_;
    std::vector<Block> blocks_;
    size_t count_ = 0;
};
