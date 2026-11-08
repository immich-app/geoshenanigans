// merge_cell_lists (admin_cover.h): a polygon's boundary cells are the k-way
// merge of its arcs' ascending lists, each cell once, however many of them
// list it, however many times one list names it, and however many arcs (or
// the same arc twice) it has.
#include "admin_cover.h"

#include <algorithm>
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
            } else if (rng() % 4 == 0) {
                // Ascending, with some cells named more than once.
                const size_t n = rng() % 40;
                for (size_t j = 0; j < n; j++) lists[i].push_back(range == UINT64_MAX ? rng() : rng() % range);
                for (size_t j = 0; j < n / 3; j++) lists[i].push_back(lists[i][rng() % n]);
                std::sort(lists[i].begin(), lists[i].end());
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

// A cell one list names more than once is in the union once, whether that
// list is the only one with cells or one of several.
TEST(a_cell_a_list_repeats_is_in_the_union_once) {
    const std::vector<uint64_t> repeating{1, 1, 2, 5, 5, 5}, other{2, 3, 3}, none;
    const auto merge = [](const std::vector<const std::vector<uint64_t>*>& given) {
        std::vector<CellList> spans;
        for (const std::vector<uint64_t>* l : given) spans.push_back({l->data(), l->data() + l->size()});
        std::vector<uint64_t> out{99};  // replaced
        merge_cell_lists(spans, out);
        CHECK(spans.empty());
        return out;
    };
    CHECK(merge({&repeating}) == (std::vector<uint64_t>{1, 2, 5}));
    CHECK(merge({&none, &repeating, &none}) == (std::vector<uint64_t>{1, 2, 5}));
    CHECK(merge({&repeating, &other}) == (std::vector<uint64_t>{1, 2, 3, 5}));
    CHECK(merge({&other, &none}) == (std::vector<uint64_t>{2, 3}));
}
