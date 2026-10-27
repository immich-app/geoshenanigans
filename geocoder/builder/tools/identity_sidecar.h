// Identity sidecars (GCPATCH v8): geocoder-diff --identity matches street /
// interp / addr records across days by one stable entity key (u32) per
// record, read from <META>/{street,interp,addr}.slot.u32
// (tools/gen_identity_sidecar.py writes them).
#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <utility>
#include <vector>

#include "parallel.h"

// A family's keys from its sidecar's bytes: one per record, or empty when
// the sidecar is not exactly one u32 per record (the family then merges by
// content).
inline std::vector<uint32_t> identity_keys(const std::vector<char>& bytes, size_t n_records) {
    if (n_records == 0 || bytes.size() != n_records * sizeof(uint32_t)) return {};
    std::vector<uint32_t> v(n_records);
    memcpy(v.data(), bytes.data(), bytes.size());
    return v;
}

// old_of_new[j]: the old record holding new record j's entity, or NO_DATA.
// Key NO_DATA marks a record without an entity (a tombstone) and never matches.
// A key below the old record count (a stable-slot day's keys are its slots)
// indexes a table; a larger one is found in a sorted list, so the lookup
// takes memory by the record count, whatever the keys.
inline std::vector<uint32_t> old_index_of_new(const std::vector<uint32_t>& old_key, const std::vector<uint32_t>& new_key) {
    constexpr uint32_t NO_DATA = 0xFFFFFFFFu;
    const size_t n = old_key.size();
    std::vector<uint32_t> by_key(n, NO_DATA);
    std::vector<std::pair<uint32_t, uint32_t>> sparse;  // (key, old record), key >= n
    for (size_t i = 0; i < n; i++) {
        const uint32_t k = old_key[i];
        if (k == NO_DATA) continue;
        if (k >= n) {
            sparse.emplace_back(k, uint32_t(i));
        } else {
            if (by_key[k] != NO_DATA) throw std::runtime_error("Identity sidecar repeats a key");
            by_key[k] = uint32_t(i);
        }
    }
    std::sort(sparse.begin(), sparse.end());
    for (size_t i = 1; i < sparse.size(); i++)
        if (sparse[i - 1].first == sparse[i].first) throw std::runtime_error("Identity sidecar repeats a key");
    std::vector<uint32_t> o(new_key.size());
    parallel_for(new_key.size(), [&](size_t b, size_t e, unsigned) {
        for (size_t j = b; j < e; j++) {
            const uint32_t k = new_key[j];
            if (k < n) {
                o[j] = by_key[k];
                continue;
            }
            auto it = std::lower_bound(sparse.begin(), sparse.end(), std::make_pair(k, uint32_t(0)));
            o[j] = it != sparse.end() && it->first == k ? it->second : NO_DATA;
        }
    });
    return o;
}
