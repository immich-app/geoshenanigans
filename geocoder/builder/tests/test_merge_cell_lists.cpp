// merge_cell_lists (admin_cover.h): a polygon's boundary cells are the k-way
// merge of its arcs' ascending lists, each cell once, however many of them
// list it and however many arcs (or the same arc twice) it has.
#include "admin_cover.h"

#include <cstdint>
#include <random>
#include <set>
#include <vector>

#include "test_framework.h"

TEST(merging_cell_lists_gives_their_ascending_union_each_cell_once) {
    std::mt19937_64 rng(1108);
    size_t merged = 0, repeats = 0;
    for (int round = 0; round < 3000; round++) {
        // Lists drawn from a narrow range share many cells; from a wide one,
        // few. Some are empty, some repeat another list whole.
        const size_t k = rng() % 14;
        const uint64_t range = round % 3 == 0 ? 40 : round % 3 == 1 ? 4000 : UINT64_MAX;
        std::vector<std::vector<uint64_t>> lists(k);
        std::set<uint64_t> want;
        for (size_t i = 0; i < k; i++) {
            if (i > 0 && rng() % 6 == 0) {
                lists[i] = lists[rng() % i];
            } else {
                std::set<uint64_t> cells;
                const size_t n = rng() % 5 == 0 ? 0 : rng() % 40;
                while (cells.size() < n) cells.insert(range == UINT64_MAX ? rng() : rng() % range);
                lists[i].assign(cells.begin(), cells.end());
            }
            for (uint64_t c : lists[i]) repeats += !want.insert(c).second;
        }
        std::vector<CellList> spans;
        for (const auto& l : lists) spans.push_back({l.data(), l.data() + l.size()});
        std::vector<uint64_t> out{99, 1};  // replaced
        merge_cell_lists(spans, out);
        CHECK(out == std::vector<uint64_t>(want.begin(), want.end()));
        CHECK(spans.empty());
        merged += out.size();
    }
    CHECK(merged > 50000);
    CHECK(repeats > 50000);
}
