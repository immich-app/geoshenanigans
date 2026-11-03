// The strings of a pool that some records use, back to back in the pool's
// order, and where each one lands: the continent split's pool
// (continent_filter.cpp) and admin-minimal's own core strings
// (cell_index.cpp).
#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>
#include <vector>

#include "parallel.h"
#include "ranked_bits.h"
#include "types.h"

class StringSubset {
public:
    // The strings at the pool offsets set in `used` (mark_bit, one bit per
    // pool byte), each a string's start.
    StringSubset(const std::vector<char>& pool, const std::vector<std::atomic<uint64_t>>& used)
        : kept_(pool.size(), used) {
        const std::vector<uint32_t> starts = kept_.ones();
        offsets_ = parallel_offsets<uint32_t>(starts.size(), [&](size_t k) {
            return static_cast<uint32_t>(std::strlen(pool.data() + starts[k]) + 1);
        });
        parallel_resize(bytes_, offsets_.back());
        parallel_for(starts.size(), [&](size_t b, size_t e, unsigned) {
            for (size_t k = b; k < e; k++)
                std::memcpy(bytes_.data() + offsets_[k], pool.data() + starts[k], offsets_[k + 1] - offsets_[k]);
        });
    }

    // The kept strings, back to back.
    const std::vector<char>& bytes() const { return bytes_; }
    std::vector<char> take_bytes() { return std::move(bytes_); }

    // Where the string at pool offset `off` lands; NO_DATA for one not kept.
    uint32_t offset_of(uint32_t off) const {
        const size_t k = kept_.index(off);
        return k == RankedBits::npos ? NO_DATA : offsets_[k];
    }

private:
    RankedBits kept_;
    std::vector<uint32_t> offsets_;
    std::vector<char> bytes_;
};
