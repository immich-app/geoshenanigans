// geo_corrections.h: the diff's walks over both days' geo indexes, split
// across cores at the new index's blocks, give byte for byte what one
// sequential walk of their rows gives: the cells gained and lost, and the
// street / addr / interp list corrections; and a new index the patcher's
// Writer wouldn't write is refused.
#include "geo_corrections.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "test_framework.h"

using gctest::refusal_of;

namespace {

constexpr size_t T = geo_index::TABLES;
constexpr uint32_t NO_DATA = 0xFFFFFFFFu;

struct Row {
    uint64_t cell;
    geo_index::RowLists lists;
};

// An index's files and its view.
struct Side {
    std::vector<Row> rows;
    std::vector<char> top, blocks;
    geo_index::View view() const { return {top.data(), top.size(), blocks.data(), blocks.size()}; }
};

Side side_of(std::vector<Row> rows) {
    Side s;
    s.rows = std::move(rows);
    geo_index::Writer w([&](const char* p, size_t n) { s.top.insert(s.top.end(), p, p + n); },
                        [&](const char* p, size_t n) { s.blocks.insert(s.blocks.end(), p, p + n); });
    for (const Row& r : s.rows) w.add(r.cell, geo_index::ids_of(r.lists));
    w.finish();
    return s;
}

uint64_t cell(uint64_t n) { return geo_index::cell_of(n, 17); }

// Rows at cells drawn from [0, range), each with a few ids below max_id in
// one to three tables.
Side random_side(std::mt19937_64& rng, size_t n, uint64_t range, uint32_t max_id) {
    std::set<uint64_t> numbers;
    while (numbers.size() < std::min<uint64_t>(n, range)) numbers.insert(rng() % range);
    std::vector<Row> rows;
    for (uint64_t k : numbers) {
        Row r{cell(k), {}};
        const unsigned mask = 1 + static_cast<unsigned>(rng() % 7);
        for (size_t t = 0; t < T; t++) {
            if (!(mask >> t & 1)) continue;
            const size_t c = 1 + rng() % 5;
            for (size_t j = 0; j < c; j++) r.lists[t].push_back(static_cast<uint32_t>(rng() % max_id));
            std::sort(r.lists[t].begin(), r.lists[t].end());
        }
        rows.push_back(r);
    }
    return side_of(rows);
}

// The walk as geocoder-diff first wrote it, a table at a time over the rows.
std::vector<char> sequential(size_t t, const Side& o, const Side& n, const std::vector<uint64_t>& added,
                             const std::vector<uint64_t>& removed, const std::vector<uint32_t>& rm, bool drop) {
    const std::set<uint64_t> removed_set(removed.begin(), removed.end());
    std::vector<char> buf(12, 0);
    uint32_t dc = 0;
    size_t oi = 0, ni = 0, ai = 0;
    while (oi < o.rows.size() || ni < n.rows.size() || ai < added.size()) {
        uint64_t eff_old = UINT64_MAX;
        while (oi < o.rows.size() && removed_set.count(o.rows[oi].cell)) oi++;
        if (oi < o.rows.size()) eff_old = o.rows[oi].cell;
        const bool is_added = ai < added.size() && added[ai] < eff_old;
        if (is_added) eff_old = added[ai];
        const uint64_t n_cid = ni < n.rows.size() ? n.rows[ni].cell : UINT64_MAX;
        if (eff_old == UINT64_MAX && n_cid == UINT64_MAX) break;
        if (eff_old <= n_cid) {
            std::vector<uint32_t> d;
            if (!is_added) {
                for (uint32_t id : o.rows[oi].lists[t]) {
                    if (id < rm.size() && rm[id] != NO_DATA) d.push_back(rm[id]);
                    else if (!drop) d.push_back(id);
                }
                std::sort(d.begin(), d.end());
                oi++;
            } else {
                ai++;
            }
            const std::vector<uint32_t> nl = eff_old == n_cid ? n.rows[ni].lists[t] : std::vector<uint32_t>();
            if (eff_old == n_cid) ni++;
            if (d != nl) {
                append_geo_list_delta(buf, eff_old, d, nl);
                dc++;
            }
        } else {
            if (!n.rows[ni].lists[t].empty()) {
                append_geo_list_delta(buf, n_cid, {}, n.rows[ni].lists[t]);
                dc++;
            }
            ni++;
        }
    }
    const uint32_t marker = GEO_ENTRY_DELTA_MARKER, fid = static_cast<uint32_t>(GEO_ENTRIES_FILES[t]);
    memcpy(buf.data(), &marker, 4);
    memcpy(buf.data() + 4, &fid, 4);
    memcpy(buf.data() + 8, &dc, 4);
    return buf;
}

std::vector<uint64_t> cells_of(const Side& s) {
    std::vector<uint64_t> out;
    for (const Row& r : s.rows) out.push_back(r.cell);
    return out;
}

}  // namespace

TEST(geo_cell_changes_are_the_cells_each_side_alone_holds) {
    std::mt19937_64 rng(5);
    for (int round = 0; round < 30; round++) {
        const uint64_t range = round % 2 ? 9000 : 400000;
        const Side o = random_side(rng, round == 0 ? 0 : 5000, range, 50), n = random_side(rng, round == 1 ? 0 : 5200, range, 50);
        const std::vector<uint64_t> oc = cells_of(o), nc = cells_of(n);
        std::vector<uint64_t> added, removed;
        std::set_difference(nc.begin(), nc.end(), oc.begin(), oc.end(), std::back_inserter(added));
        std::set_difference(oc.begin(), oc.end(), nc.begin(), nc.end(), std::back_inserter(removed));
        for (unsigned threads : {1u, 3u, 16u}) {
            const CellIdDiff d = diff_geo_cells(o.view(), n.view(), threads);
            CHECK(d.added == added);
            CHECK(d.removed == removed);
        }
    }
}

TEST(geo_entry_corrections_match_the_sequential_walk) {
    std::mt19937_64 rng(31);
    for (int round = 0; round < 40; round++) {
        const uint64_t range = round % 2 ? 9000 : 400000;
        const Side o = random_side(rng, 5000, range, 50), n = random_side(rng, 5200, range, 50);
        const CellIdDiff d = diff_geo_cells(o.view(), n.view(), 1);
        std::array<std::vector<uint32_t>, T> rm;
        for (auto& r : rm) {
            r.resize(40);
            for (auto& v : r) v = rng() % 8 == 0 ? NO_DATA : static_cast<uint32_t>(rng() % 60);
        }
        if (round % 4 == 0) rm[1].clear();  // a file sent unchanged: no remap
        const std::array<bool, T> drop = {round % 3 != 0, !rm[1].empty(), round % 2 == 0};
        for (unsigned threads : {1u, 3u, 16u}) {
            const GeoEntryCorrections got = geo_entry_corrections(o.view(), n.view(), d.added, d.removed,
                                                                  {&rm[0], &rm[1], &rm[2]}, drop, threads);
            for (size_t t = 0; t < T; t++) {
                const std::vector<char> want = sequential(t, o, n, d.added, d.removed, rm[t], drop[t]);
                CHECK(got.sections[t] == want);
                uint32_t cells;
                memcpy(&cells, want.data() + 8, 4);
                CHECK_EQ(got.cells[t], cells);
            }
        }
    }
}

TEST(geo_entry_corrections_ranges_tolerate_inconsistent_cell_changes) {
    // Added and removed lists that don't follow from the indexes: cells
    // added that exist on either side or neither, removed ones the old side
    // lacks.
    std::mt19937_64 rng(8);
    for (int round = 0; round < 30; round++) {
        const Side o = random_side(rng, 1500, 3000, 30), n = random_side(rng, 1500, 3000, 30);
        std::vector<uint64_t> added, removed;
        for (int i = 0; i < 200; i++) added.push_back(cell(rng() % 3000));
        for (int i = 0; i < 200; i++) removed.push_back(cell(rng() % 3000));
        std::sort(added.begin(), added.end());
        const std::vector<uint32_t> rm = {3, 1, NO_DATA, 7};
        const GeoEntryCorrections got = geo_entry_corrections(o.view(), n.view(), added, removed, {&rm, &rm, &rm},
                                                              {true, false, true}, 5);
        for (size_t t = 0; t < T; t++) CHECK(got.sections[t] == sequential(t, o, n, added, removed, rm, t != 1));
    }
}

TEST(geo_entry_corrections_drop_unmapped_ids_when_asked) {
    // Old cell 5 lists streets 1 and 2; the new one lists 7. Street 1 was
    // deleted (unmapped) and 2 became 7: dropping unmapped ids derives the
    // new list exactly, keeping them (v7) leaves a stale 1 to correct.
    const Side o = side_of({{cell(5), {{{1, 2}, {}, {}}}}}), n = side_of({{cell(5), {{{7}, {}, {}}}}});
    const std::vector<uint32_t> rm = {NO_DATA, NO_DATA, 7};
    CHECK_EQ(geo_entry_corrections(o.view(), n.view(), {}, {}, {&rm, &rm, &rm}, {true, true, true}, 1).cells[0], 0u);
    CHECK_EQ(geo_entry_corrections(o.view(), n.view(), {}, {}, {&rm, &rm, &rm}, {false, true, true}, 1).cells[0], 1u);
}

TEST(geo_entry_corrections_empty_sides) {
    std::mt19937_64 rng(2);
    const Side n = random_side(rng, 300, 1000, 20), none = side_of({});
    const std::vector<uint32_t> rm;
    const std::vector<uint64_t> nothing, cells = cells_of(n);
    const GeoEntryCorrections fresh = geo_entry_corrections(none.view(), n.view(), cells, nothing, {&rm, &rm, &rm}, {});
    const GeoEntryCorrections gone = geo_entry_corrections(n.view(), none.view(), nothing, cells, {&rm, &rm, &rm}, {});
    for (size_t t = 0; t < T; t++) {
        CHECK(fresh.sections[t] == sequential(t, none, n, cells, nothing, rm, false));
        CHECK(gone.sections[t] == sequential(t, n, none, nothing, cells, rm, false));
    }
}

// A new index whose bytes decode but aren't the Writer's for its rows (a
// list without repeats flagged as having some), in a block of one range or
// another, is refused: the patcher couldn't rebuild it.
TEST(geo_entry_corrections_refuse_a_new_index_the_patchers_writer_wouldnt_write) {
    std::vector<Row> rows;
    for (uint64_t k = 0; k < 1000; k++) rows.push_back({cell(2 * k), {{{7}, {}, {}}}});
    const Side o = side_of(rows), n = side_of(rows);
    const std::vector<uint32_t> rm;
    auto refusal = [&](const Side& to) {
        return refusal_of([&] { geo_entry_corrections(o.view(), to.view(), {}, {}, {&rm, &rm, &rm}, {}, 4); });
    };
    CHECK_EQ(refusal(n), std::string());
    // 16 blocks, a range each: block b's first row is streets {7}, PLAIN (1, then 2 << 1 | 0, zigzag(7)).
    for (size_t b : {size_t(0), size_t(7), size_t(15)}) {
        Side bad = n;
        const size_t at = n.view().block(b).begin + 1;
        CHECK_EQ(int(bad.blocks[at]), 2);
        bad.blocks[at] = 3;  // the same list, with repeats flagged
        CHECK_EQ(refusal(bad), std::string("Geo index has a layout the patcher's geo_index::Writer can't reproduce"));
    }
}
