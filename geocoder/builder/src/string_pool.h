#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

class StringPool {
public:
    uint32_t intern(const std::string& s) {
        if (released_) throw std::logic_error("StringPool::intern after release_index");
        auto it = index_.find(s);
        if (it != index_.end()) {
            return it->second;
        }
        // String offsets are u32 across the entire index format; a pool
        // past 4 GiB would silently wrap every subsequent offset.
        if (data_.size() > 0xFFFFFFFFull - s.size() - 1)
            throw std::runtime_error("string pool exceeds u32 offset space");
        uint32_t offset = static_cast<uint32_t>(data_.size());
        index_[s] = offset;
        data_.insert(data_.end(), s.begin(), s.end());
        data_.push_back('\0');
        return offset;
    }

    // The offset of s if it is pooled, else kAbsent. Many threads may look
    // up at once while nothing interns.
    static constexpr uint32_t kAbsent = UINT32_MAX;
    uint32_t find(const std::string& s) const {
        if (released_) throw std::logic_error("StringPool::find after release_index");
        auto it = index_.find(s);
        return it == index_.end() ? kAbsent : it->second;
    }

    // Frees the lookup table once nothing interns any more (GiBs on planet).
    void release_index() {
        std::unordered_map<std::string, uint32_t>().swap(index_);
        released_ = true;
    }

    const std::vector<char>& data() const { return data_; }
    std::vector<char>& mutable_data() { return data_; }

private:
    std::unordered_map<std::string, uint32_t> index_;
    std::vector<char> data_;
    bool released_ = false;
};
