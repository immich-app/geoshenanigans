#pragma once
// Node coordinates for the node ids the way pass looks up. A pre-scan of the
// way blobs marks those ids in a bitmap (one bit per possible id); set() then
// keeps only marked nodes, each at its rank among the marked ids. On planet
// that is a fraction of the ~13 G node ids, so the index holds tens of GiB
// less than an array indexed by node id, which is resident end to end.
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include <sys/mman.h>

#include "parallel.h"

struct PackedLocation {
    int32_t lat_e7;  // latitude * 10^7
    int32_t lon_e7;  // longitude * 10^7
    bool valid() const { return lat_e7 != 0 || lon_e7 != 0; }
    double lat() const { return lat_e7 / 10000000.0; }
    double lon() const { return lon_e7 / 10000000.0; }
};

inline PackedLocation pack_location(double lat, double lng) {
    return {static_cast<int32_t>(lat * 10000000.0 + (lat >= 0 ? 0.5 : -0.5)),
            static_cast<int32_t>(lng * 10000000.0 + (lng >= 0 ? 0.5 : -0.5))};
}

class SparseNodeIndex {
public:
    explicit SparseNodeIndex(size_t capacity)
        : capacity_(capacity),
          words_(static_cast<uint64_t*>(map_zeroed(word_count() * sizeof(uint64_t)))) {}
    ~SparseNodeIndex() { release(); }
    SparseNodeIndex(const SparseNodeIndex&) = delete;
    SparseNodeIndex& operator=(const SparseNodeIndex&) = delete;

    // Pre-scan, any thread: the way pass will look `id` up. Ids past
    // capacity are never stored, so they need no mark.
    void mark(uint64_t id) {
        if (id >= capacity_) return;
        uint64_t* word = &words_[id >> 6];
        uint64_t bit = uint64_t(1) << (id & 63);
        if (!(__atomic_load_n(word, __ATOMIC_RELAXED) & bit))
            __atomic_fetch_or(word, bit, __ATOMIC_RELAXED);
    }

    // Once, between the pre-scan and the first set(): ranks the marked ids
    // and maps one zeroed slot per marked id.
    void finalize(unsigned threads = 0) {
        size_t blocks = word_count() / kWordsPerBlock;
        block_rank_.assign(blocks + 1, 0);
        parallel_for(blocks, [&](size_t begin, size_t end, unsigned) {
            for (size_t b = begin; b < end; b++) {
                uint64_t n = 0;
                for (size_t w = b * kWordsPerBlock; w < (b + 1) * kWordsPerBlock; w++)
                    n += static_cast<uint64_t>(__builtin_popcountll(words_[w]));
                block_rank_[b + 1] = n;
            }
        }, threads);
        for (size_t b = 0; b < blocks; b++) block_rank_[b + 1] += block_rank_[b];
        marked_ = block_rank_[blocks];
        if (marked_) locs_ = static_cast<PackedLocation*>(map_zeroed(marked_ * sizeof(PackedLocation)));
    }

    // Any thread, one writer per id: stores a marked node's location and
    // drops the rest. Ids past capacity are counted (see over_capacity()).
    void set(uint64_t id, double lat, double lng) {
        if (id >= capacity_) { over_capacity_.fetch_add(1, std::memory_order_relaxed); return; }
        if (!is_marked(id)) return;
        locs_[rank(id)] = pack_location(lat, lng);
    }

    // A node's location, or {0, 0} (invalid) for ids never set, past
    // capacity or never marked. Lookups of unmarked ids are counted (see
    // unmarked_lookups()): the pre-scan should have marked every id looked up.
    PackedLocation get(uint64_t id) const {
        if (id >= capacity_) return {0, 0};
        if (!is_marked(id)) {
            unmarked_lookups_.fetch_add(1, std::memory_order_relaxed);
            return {0, 0};
        }
        return locs_[rank(id)];
    }

    size_t marked() const { return marked_; }
    // set() calls dropped for ids past capacity: DATA LOSS, ways referencing
    // those nodes resolve them as missing.
    uint64_t over_capacity() const { return over_capacity_.load(); }
    uint64_t unmarked_lookups() const { return unmarked_lookups_.load(); }
    size_t bytes() const {
        return word_count() * sizeof(uint64_t) + block_rank_.capacity() * sizeof(uint64_t) +
               marked_ * sizeof(PackedLocation);
    }

    // Idempotent; set() afterwards counts every id as over capacity.
    void release() {
        if (words_) munmap(words_, word_count() * sizeof(uint64_t));
        if (locs_) munmap(locs_, marked_ * sizeof(PackedLocation));
        words_ = nullptr;
        locs_ = nullptr;
        std::vector<uint64_t>().swap(block_rank_);
        capacity_ = 0;
        marked_ = 0;
    }

private:
    // One rank entry per 512 ids: a block's 8 words share a cache line.
    static constexpr size_t kWordsPerBlock = 8;

    // Anonymous zero-filled mapping: a page costs memory once written.
    static void* map_zeroed(size_t bytes) {
        if (bytes == 0) return nullptr;
        void* p = mmap(nullptr, bytes, PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
        if (p == MAP_FAILED)
            throw std::runtime_error("cannot map " + std::to_string(bytes >> 20) + " MiB for the node index");
        madvise(p, bytes, MADV_HUGEPAGE);
        return p;
    }

    size_t word_count() const {
        size_t ids_per_block = 64 * kWordsPerBlock;
        return (capacity_ + ids_per_block - 1) / ids_per_block * kWordsPerBlock;
    }
    bool is_marked(uint64_t id) const { return (words_[id >> 6] >> (id & 63)) & 1; }
    // Marked ids below `id`.
    size_t rank(uint64_t id) const {
        size_t w = id >> 6;
        uint64_t r = block_rank_[w / kWordsPerBlock];
        for (size_t i = w & ~(kWordsPerBlock - 1); i < w; i++)
            r += static_cast<uint64_t>(__builtin_popcountll(words_[i]));
        return r + static_cast<uint64_t>(__builtin_popcountll(words_[w] & ((uint64_t(1) << (id & 63)) - 1)));
    }

    size_t capacity_;
    uint64_t* words_;
    std::vector<uint64_t> block_rank_;  // marked ids before each block
    size_t marked_ = 0;
    PackedLocation* locs_ = nullptr;
    std::atomic<uint64_t> over_capacity_{0};
    mutable std::atomic<uint64_t> unmarked_lookups_{0};
};
