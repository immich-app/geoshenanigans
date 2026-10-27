// Identity sidecars (GCPATCH v8): geocoder-diff --identity matches street /
// interp / addr records across days by one stable entity key (u32) per
// record, read from <META>/{street,interp,addr}.slot.u32
// (tools/gen_identity_sidecar.py writes them).
#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <vector>

#include "parallel.h"

// A family's keys from its sidecar's bytes: one per record, or empty when
// the sidecar holds another number of keys (the family then merges by
// content).
inline std::vector<uint32_t> identity_keys(const std::vector<char>& bytes, size_t n_records) {
    std::vector<uint32_t> v(bytes.size() / 4);
    if (!v.empty()) memcpy(v.data(), bytes.data(), v.size() * 4);
    if (n_records == 0 || v.size() != n_records) return {};
    return v;
}

// old_of_new[j]: the old record holding new record j's entity, or NO_DATA.
// Key NO_DATA marks a record without an entity (a tombstone) and never matches.
inline std::vector<uint32_t> old_index_of_new(const std::vector<uint32_t>& old_key, const std::vector<uint32_t>& new_key) {
    constexpr uint32_t NO_DATA = 0xFFFFFFFFu;
    uint32_t max_key = 0;
    for (uint32_t k : old_key) if (k != NO_DATA) max_key = std::max(max_key, k);
    std::vector<uint32_t> by_key((size_t)max_key + 1, NO_DATA);
    for (uint32_t i = 0; i < old_key.size(); i++) {
        if (old_key[i] == NO_DATA) continue;
        if (by_key[old_key[i]] != NO_DATA) throw std::runtime_error("Identity sidecar repeats a key");
        by_key[old_key[i]] = i;
    }
    std::vector<uint32_t> o(new_key.size());
    parallel_for(new_key.size(), [&](size_t b, size_t e, unsigned) {
        for (size_t j = b; j < e; j++) o[j] = new_key[j] <= max_key ? by_key[new_key[j]] : NO_DATA;
    });
    return o;
}
