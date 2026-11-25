// A set of ids as bits, in pages allocated as ids in them are set: a range
// of billions of ids costs a pointer a page until one is set, then 64 KiB a
// page it uses.
#pragma once

#include <cstdint>
#include <memory>
#include <vector>

class PagedBits {
public:
    // Ids 0 to limit - 1.
    explicit PagedBits(uint64_t limit) : limit_(limit), pages_((limit + kPageBits - 1) / kPageBits) {}

    // Sets `id`, below the limit. Not safe beside another call of set.
    void set(uint64_t id) {
        std::unique_ptr<uint64_t[]>& page = pages_[id >> kPageShift];
        if (!page) page = std::make_unique<uint64_t[]>(kPageWords);
        page[(id & kPageMask) >> 6] |= uint64_t(1) << (id & 63);
    }

    // Whether `id` is set; false for one at or past the limit. Safe on
    // every thread while nothing sets.
    bool test(uint64_t id) const {
        if (id >= limit_) return false;
        const std::unique_ptr<uint64_t[]>& page = pages_[id >> kPageShift];
        return page && ((page[(id & kPageMask) >> 6] >> (id & 63)) & 1);
    }

    // The pages allocated.
    size_t pages_used() const {
        size_t n = 0;
        for (const auto& page : pages_) n += page != nullptr;
        return n;
    }

    static constexpr unsigned kPageShift = 19;
    static constexpr uint64_t kPageBits = uint64_t(1) << kPageShift;

private:
    static constexpr uint64_t kPageMask = kPageBits - 1;
    static constexpr uint64_t kPageWords = kPageBits / 64;

    uint64_t limit_;
    std::vector<std::unique_ptr<uint64_t[]>> pages_;
};
