// cell_id_diff.h: the merge walk over sorted cell files must give what
// set_difference over sorted copies gives, and unsorted files must still work.
#include "cell_id_diff.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <random>
#include <string>
#include <vector>

#include "test_framework.h"

namespace {

// Records of `stride` bytes led by the given ids, the rest filled with junk.
std::string cell_file(const std::vector<uint64_t>& ids, size_t stride) {
    std::string out(ids.size() * stride, '\x5a');
    for (size_t i = 0; i < ids.size(); i++) std::memcpy(&out[i * stride], &ids[i], 8);
    return out;
}

CellIdDiff reference(std::vector<uint64_t> old_ids, std::vector<uint64_t> new_ids) {
    std::sort(old_ids.begin(), old_ids.end());
    std::sort(new_ids.begin(), new_ids.end());
    CellIdDiff d;
    std::set_difference(new_ids.begin(), new_ids.end(), old_ids.begin(), old_ids.end(), std::back_inserter(d.added));
    std::set_difference(old_ids.begin(), old_ids.end(), new_ids.begin(), new_ids.end(), std::back_inserter(d.removed));
    return d;
}

bool same(const CellIdDiff& a, const CellIdDiff& b) { return a.added == b.added && a.removed == b.removed; }

std::vector<uint64_t> random_ids(size_t n, uint64_t range, bool sorted, std::mt19937_64& rng) {
    std::vector<uint64_t> v(n);
    for (auto& x : v) x = rng() % range;
    if (sorted) std::sort(v.begin(), v.end());
    return v;
}

}  // namespace

TEST(diff_cell_ids_matches_set_difference_of_sorted_copies) {
    std::mt19937_64 rng(21);
    for (size_t stride : {size_t(12), size_t(20)}) {
        for (uint64_t range : {uint64_t(5), uint64_t(1000), uint64_t(1) << 62}) {
            for (bool old_sorted : {true, false}) {
                for (bool new_sorted : {true, false}) {
                    auto old_ids = random_ids(3000, range, old_sorted, rng);
                    auto new_ids = random_ids(2500, range, new_sorted, rng);
                    std::string o = cell_file(old_ids, stride), n = cell_file(new_ids, stride);
                    for (unsigned threads : {1u, 4u}) {
                        auto got = diff_cell_ids(o.data(), old_ids.size(), n.data(), new_ids.size(), stride, threads);
                        CHECK(same(got, reference(old_ids, new_ids)));
                    }
                }
            }
        }
    }
}

TEST(diff_cell_ids_empty_sides) {
    std::vector<uint64_t> ids = {3, 9, 9, 12};
    std::string f = cell_file(ids, 12);
    auto added = diff_cell_ids(nullptr, 0, f.data(), ids.size(), 12);
    CHECK(added.added == ids);
    CHECK(added.removed.empty());
    auto removed = diff_cell_ids(f.data(), ids.size(), nullptr, 0, 12);
    CHECK(removed.removed == ids);
    CHECK(removed.added.empty());
}

TEST(cell_ids_sorted_sees_one_step_down) {
    std::vector<uint64_t> ids(1000);
    for (size_t i = 0; i < ids.size(); i++) ids[i] = i / 3;
    CHECK(cell_ids_sorted(cell_file(ids, 20).data(), ids.size(), 20, 7));
    for (size_t at : {size_t(4), size_t(500), size_t(999)}) {
        auto bad = ids;
        bad[at] = bad[at - 1] - 1;
        CHECK(!cell_ids_sorted(cell_file(bad, 20).data(), bad.size(), 20, 7));
    }
}
