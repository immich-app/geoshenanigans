// geo_corrections.h: the street / addr / interp entry corrections must stay
// byte for byte what the diff's original sequential walk emitted.
#include "geo_corrections.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <random>
#include <string>
#include <unordered_set>
#include <vector>

#include "cell_id_diff.h"
#include "test_framework.h"

namespace {

// The walk as geocoder-diff first wrote it.
std::vector<char> legacy_corrections(uint32_t file_id, GeoCells old_geo, GeoCells new_geo,
                                     const std::vector<uint64_t>& g_added, const std::vector<uint64_t>& g_removed,
                                     ByteSpan old_entries, ByteSpan new_entries, size_t geo_off_pos,
                                     const std::vector<uint32_t>& id_rm) {
    auto parse_ids = [](ByteSpan entries, uint32_t off) {
        std::vector<uint32_t> ids;
        read_entry_list(entries, off, ids);
        return ids;
    };
    auto remap_ids_vec = [](std::vector<uint32_t>& ids, const std::vector<uint32_t>& rm) {
        for (auto& id : ids)
            if (id < rm.size() && rm[id] != 0xFFFFFFFFu) id = rm[id];
        std::sort(ids.begin(), ids.end());
    };
    std::unordered_set<uint64_t> removed_set(g_removed.begin(), g_removed.end());
    std::vector<char> buf(12, 0);
    uint32_t dc = 0;
    size_t oi = 0, ni = 0, added_i = 0;
    while (oi < old_geo.n || ni < new_geo.n || added_i < g_added.size()) {
        uint64_t eff_old = UINT64_MAX;
        bool is_added = false;
        while (oi < old_geo.n) {
            memcpy(&eff_old, old_geo.data + oi * 20, 8);
            if (!removed_set.count(eff_old)) break;
            oi++;
            eff_old = UINT64_MAX;
        }
        if (added_i < g_added.size() && g_added[added_i] < eff_old) {
            eff_old = g_added[added_i];
            is_added = true;
        }
        uint64_t n_cid = UINT64_MAX;
        if (ni < new_geo.n) memcpy(&n_cid, new_geo.data + ni * 20, 8);
        if (eff_old == UINT64_MAX && n_cid == UINT64_MAX) break;
        if (eff_old <= n_cid) {
            std::vector<uint32_t> d_ids;
            if (!is_added) {
                uint32_t off; memcpy(&off, old_geo.data + oi * 20 + geo_off_pos, 4);
                d_ids = parse_ids(old_entries, off);
                remap_ids_vec(d_ids, id_rm);
                oi++;
            } else {
                added_i++;
            }
            if (eff_old == n_cid) {
                uint32_t n_off; memcpy(&n_off, new_geo.data + ni * 20 + geo_off_pos, 4);
                auto n_ids = parse_ids(new_entries, n_off);
                if (d_ids != n_ids) { append_geo_list_delta(buf, eff_old, d_ids, n_ids); dc++; }
                ni++;
            } else if (!d_ids.empty()) {
                append_geo_list_delta(buf, eff_old, d_ids, {});
                dc++;
            }
        } else {
            uint32_t n_off; memcpy(&n_off, new_geo.data + ni * 20 + geo_off_pos, 4);
            auto n_ids = parse_ids(new_entries, n_off);
            if (!n_ids.empty()) { append_geo_list_delta(buf, n_cid, {}, n_ids); dc++; }
            ni++;
        }
    }
    uint32_t marker = GEO_ENTRY_DELTA_MARKER;
    memcpy(buf.data(), &marker, 4); memcpy(buf.data() + 4, &file_id, 4); memcpy(buf.data() + 8, &dc, 4);
    return buf;
}

// A geo_cells.bin and the entries file its offsets at byte 8 point into.
struct Side {
    std::string cells, entries;
    std::vector<uint64_t> ids;
};

// Cells drawn from [0, range): sorted and unique unless `messy` (then some
// repeat or run backwards). Lists hold a few ids below `max_id`, sometimes
// unsorted; some cells have none (NO_DATA) and a few point past the file.
Side random_side(std::mt19937_64& rng, size_t n, uint64_t range, uint32_t max_id, bool messy) {
    Side s;
    for (size_t i = 0; i < n; i++) s.ids.push_back(rng() % range);
    std::sort(s.ids.begin(), s.ids.end());
    if (!messy) s.ids.erase(std::unique(s.ids.begin(), s.ids.end()), s.ids.end());
    else if (s.ids.size() > 4) std::swap(s.ids[1], s.ids[s.ids.size() / 2]);
    for (uint64_t id : s.ids) {
        uint32_t off = 0xFFFFFFFFu;
        int kind = static_cast<int>(rng() % 10);
        if (kind == 9) {
            off = 0x7FFFFFF0u;
        } else if (kind > 1) {
            off = static_cast<uint32_t>(s.entries.size());
            uint16_t cnt = static_cast<uint16_t>(rng() % 6);
            s.entries.append(reinterpret_cast<const char*>(&cnt), 2);
            for (uint16_t k = 0; k < cnt; k++) {
                uint32_t v = static_cast<uint32_t>(rng() % max_id);
                s.entries.append(reinterpret_cast<const char*>(&v), 4);
            }
        }
        char rec[20] = {};
        memcpy(rec, &id, 8);
        memcpy(rec + 8, &off, 4);
        s.cells.append(rec, 20);
    }
    return s;
}

}  // namespace

TEST(geo_entry_corrections_match_the_sequential_walk) {
    std::mt19937_64 rng(31);
    for (int round = 0; round < 60; round++) {
        bool messy = round % 3 == 2;
        uint64_t range = round % 2 ? 400 : 100000;
        Side o = random_side(rng, 2000, range, 50, messy), n = random_side(rng, 2100, range, 50, messy);
        std::vector<uint32_t> id_rm(40);
        for (auto& v : id_rm) v = rng() % 8 == 0 ? 0xFFFFFFFFu : static_cast<uint32_t>(rng() % 60);
        GeoCells og{o.cells.data(), o.ids.size()}, ng{n.cells.data(), n.ids.size()};
        CellIdDiff d = diff_cell_ids(o.cells.data(), o.ids.size(), n.cells.data(), n.ids.size(), 20);
        ByteSpan oe{o.entries.data(), o.entries.size()}, ne{n.entries.data(), n.entries.size()};
        auto expect = legacy_corrections(9, og, ng, d.added, d.removed, oe, ne, 8, id_rm);
        uint32_t cells;
        memcpy(&cells, expect.data() + 8, 4);
        for (unsigned threads : {1u, 3u, 16u}) {
            auto got = geo_entry_corrections(9, og, ng, d.added, d.removed, oe, ne, 8, id_rm, threads);
            CHECK(got.section == expect);
            CHECK_EQ(got.cells, cells);
        }
    }
}

TEST(geo_entry_corrections_ranges_tolerate_inconsistent_cell_changes) {
    // Added and removed lists that don't follow from the cell files: cells
    // added that exist on neither side, removed ones the old side lacks.
    std::mt19937_64 rng(8);
    for (int round = 0; round < 30; round++) {
        Side o = random_side(rng, 1500, 3000, 30, false), n = random_side(rng, 1500, 3000, 30, false);
        std::vector<uint64_t> added, removed;
        for (int i = 0; i < 200; i++) added.push_back(rng() % 3000);
        for (int i = 0; i < 200; i++) removed.push_back(rng() % 3000);
        std::sort(added.begin(), added.end());
        std::vector<uint32_t> id_rm = {3, 1, 0xFFFFFFFFu, 7};
        GeoCells og{o.cells.data(), o.ids.size()}, ng{n.cells.data(), n.ids.size()};
        ByteSpan oe{o.entries.data(), o.entries.size()}, ne{n.entries.data(), n.entries.size()};
        auto expect = legacy_corrections(11, og, ng, added, removed, oe, ne, 8, id_rm);
        CHECK(geo_entry_corrections(11, og, ng, added, removed, oe, ne, 8, id_rm, 5).section == expect);
    }
}

TEST(geo_entry_corrections_empty_sides) {
    std::mt19937_64 rng(2);
    Side n = random_side(rng, 300, 1000, 20, false);
    std::vector<uint64_t> none;
    ByteSpan ne{n.entries.data(), n.entries.size()}, empty{nullptr, 0};
    GeoCells ng{n.cells.data(), n.ids.size()}, nothing{nullptr, 0};
    CHECK(geo_entry_corrections(10, nothing, ng, n.ids, none, empty, ne, 8, {}).section ==
          legacy_corrections(10, nothing, ng, n.ids, none, empty, ne, 8, {}));
    CHECK(geo_entry_corrections(10, ng, nothing, none, n.ids, ne, empty, 8, {}).section ==
          legacy_corrections(10, ng, nothing, none, n.ids, ne, empty, 8, {}));
}
