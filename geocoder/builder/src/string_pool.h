#pragma once

#include <array>
#include <cstdint>
#include <cstring>
#include <functional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "parallel.h"

class StringPool {
public:
    // The lookup table is split so append_new can index on every core; a
    // few shards per core keep the cores evenly loaded.
    static constexpr size_t kShards = 256;
    using Index = std::array<std::unordered_map<std::string, uint32_t>, kShards>;

    uint32_t intern(const std::string& s) {
        if (released_) throw std::logic_error("StringPool::intern after release_index");
        auto& index = index_[shard_of(s)];
        auto it = index.find(s);
        if (it != index.end()) {
            return it->second;
        }
        uint32_t offset = static_cast<uint32_t>(data_.size());
        require_room(offset, s.size());
        index[s] = offset;
        data_.insert(data_.end(), s.begin(), s.end());
        data_.push_back('\0');
        return offset;
    }

    // The offset of s if it is pooled, else kAbsent. Many threads may look
    // up at once while nothing interns.
    static constexpr uint32_t kAbsent = UINT32_MAX;
    uint32_t find(const std::string& s) const {
        if (released_) throw std::logic_error("StringPool::find after release_index");
        const auto& index = index_[shard_of(s)];
        auto it = index.find(s);
        return it == index.end() ? kAbsent : it->second;
    }

    // Interns strings that are neither pooled nor equal to each other, in
    // order, on every core: the pool ends as interning each would leave it.
    // Returns their offsets. strs must not point into the pool.
    std::vector<uint32_t> append_new(const std::vector<std::string_view>& strs, unsigned threads = 0) {
        if (released_) throw std::logic_error("StringPool::append_new after release_index");
        std::vector<uint32_t> offsets(strs.size());
        uint64_t end = data_.size();
        for (size_t i = 0; i < strs.size(); i++) {
            require_room(end, strs[i].size());
            offsets[i] = static_cast<uint32_t>(end);
            end += strs[i].size() + 1;
        }
        data_.resize(end);
        std::vector<uint8_t> shard(strs.size());
        parallel_for(strs.size(), [&](size_t b, size_t e, unsigned) {
            for (size_t i = b; i < e; i++) {
                std::memcpy(data_.data() + offsets[i], strs[i].data(), strs[i].size());
                data_[offsets[i] + strs[i].size()] = '\0';
                shard[i] = static_cast<uint8_t>(shard_of(strs[i]));
            }
        }, threads);
        parallel_for_each(kShards, [&](size_t s, unsigned) {
            auto& index = index_[s];
            size_t n = 0;
            for (uint8_t sh : shard) n += sh == s;
            index.reserve(index.size() + n);
            for (size_t i = 0; i < strs.size(); i++)
                if (shard[i] == s) index.emplace(std::string(strs[i]), offsets[i]);
        }, threads);
        return offsets;
    }

    // Ends interning and hands back the lookup table (GiBs on planet), for
    // the caller to free, off the critical path if it likes.
    Index release_index() {
        released_ = true;
        return std::exchange(index_, {});
    }

    const std::vector<char>& data() const { return data_; }
    std::vector<char>& mutable_data() { return data_; }

private:
    static_assert(kShards <= 256, "append_new keeps a shard in a byte");
    static size_t shard_of(std::string_view s) { return std::hash<std::string_view>()(s) % kShards; }

    // String offsets are u32 across the entire index format; a pool past
    // 4 GiB would silently wrap every subsequent offset.
    static void require_room(uint64_t size, size_t len) {
        if (size > 0xFFFFFFFFull - len - 1) throw std::runtime_error("string pool exceeds u32 offset space");
    }

    Index index_;
    std::vector<char> data_;
    bool released_ = false;
};
