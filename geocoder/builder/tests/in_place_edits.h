// Which records a day edits in place, for the merge tests of a file whose
// records keep their slots while a field of many of them changes.
#pragma once

#include <cstdint>
#include <random>
#include <vector>

namespace gctest {

// Of n records: 1 in 23 scattered, and in [dense, dense + dense_n) 7 in 10
// with never 16 unchanged in a row. The 2026-11-03 nightly changed the
// importance of ~1 planet poi/all record in 23 (wikidata sitelinks were
// back), in stretches like that one of over 10,000 records.
inline std::vector<bool> edited_in_place(std::mt19937& rng, uint32_t n, uint32_t dense, uint32_t dense_n) {
    std::vector<bool> edited(n);
    uint32_t kept = 0;
    for (uint32_t i = 0; i < n; i++) {
        edited[i] = i >= dense && i - dense < dense_n ? rng() % 10 < 7 || kept == 15 : rng() % 23 == 0;
        kept = edited[i] ? 0 : kept + 1;
    }
    return edited;
}

}  // namespace gctest
