#pragma once
// Dictionary encoding for per-thread parse buffers. Each distinct string is
// stored once (NUL-terminated, in blocks that never move) and named by a
// dense code in order of first add, so a parse thread keeps a 4-byte code
// per value instead of a std::string, and its merge interns each distinct
// string once rather than once per value (DictIds).
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

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

// Pool ids for one StringDict's codes, interned on first use. Asked in the
// order a merge would intern each value, it leaves the pool and the ids
// exactly as interning every value would: intern() of a string already in
// the pool returns its id and changes nothing.
class DictIds {
public:
    DictIds(const StringDict& dict, StringPool& pool)
        : dict_(&dict), pool_(&pool), ids_(dict.size(), kUnset) {}

    uint32_t operator()(uint32_t code) {
        if (code == StringDict::kNone) return NO_DATA;
        uint32_t& id = ids_[code];
        if (id == kUnset) {
            buf_.assign(dict_->get(code));
            id = pool_->intern(buf_);
        }
        return id;
    }

private:
    // Pool offsets stay below 0xFFFFFFFF (StringPool's u32 guard).
    static constexpr uint32_t kUnset = UINT32_MAX;
    const StringDict* dict_;
    StringPool* pool_;
    std::vector<uint32_t> ids_;
    std::string buf_;
};
