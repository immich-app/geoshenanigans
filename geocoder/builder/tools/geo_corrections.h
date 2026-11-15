// The diff's side of the geo index (geo_index.h): the cells it gained and
// lost (CELL_CHANGES_GEO_MARKER), and the street / addr / interp list
// corrections (GEO_ENTRY_DELTA_MARKER): the patcher derives each geo cell's
// lists from its old ones through the files' id remaps, and every cell where
// that differs from the new list travels as a delta.
#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include "cell_id_diff.h"
#include "geo_index.h"
#include "parallel.h"
#include "patch_format.h"

// A dir's geo index, mapped: none when the dir has no geo_cells.bin.
struct GeoIndexMap {
    explicit GeoIndexMap(const std::string& dir)
        : top(mmap_file(dir + "/geo_cells.bin")), blocks(mmap_file(dir + "/geo_entries.bin")),
          view(top.data, top.size, blocks.data, blocks.size) {}
    ~GeoIndexMap() {
        unmap_file(top);
        unmap_file(blocks);
    }
    GeoIndexMap(const GeoIndexMap&) = delete;
    GeoIndexMap& operator=(const GeoIndexMap&) = delete;
    MappedFile top, blocks;
    geo_index::View view;
};

// The rows of a geo index from its first cell at or past `from`, before
// `to`, a block at a time: one block's rows in memory.
class GeoRows {
public:
    GeoRows(const geo_index::View& view, uint64_t from, uint64_t to)
        : view_(view), scratch_(view.scratch()), to_(to) {
        if (view.blocks() == 0) return;
        const size_t b = view.block_of(from);
        block_ = b == view.blocks() ? 0 : b;
        load();
        while (!done() && cell() < from) next();
    }

    bool done() const { return at_ == cells_.size(); }
    uint64_t cell() const { return cells_[at_]; }
    // Table t's list of the row (empty: none).
    const std::vector<uint32_t>& list(size_t t) const { return lists_[at_][t]; }
    void next() {
        if (++at_ < cells_.size()) return;
        block_++;
        load();
    }

private:
    // The rows of block_ before to_, none past the last block.
    void load() {
        cells_.clear();
        at_ = 0;
        if (block_ >= view_.blocks() || view_.block(block_).first_cell >= to_) return;
        size_t n = 0;
        view_.rows_of(block_, scratch_, [&](uint64_t c, const geo_index::RowLists& l) {
            if (c >= to_) return;
            cells_.push_back(c);
            if (lists_.size() <= n) lists_.emplace_back();
            lists_[n++] = l;
        });
    }

    const geo_index::View& view_;
    geo_index::View::Scratch scratch_;
    uint64_t to_;
    size_t block_ = 0, at_ = 0;
    std::vector<uint64_t> cells_;
    std::vector<geo_index::RowLists> lists_;
};

// The ranges a walk over the new index splits into across cores: range k
// holds the cells from the first of the new index's block blocks*k/n up to
// that of range k + 1 (from 0, and up to UINT64_MAX, at the ends). A walk
// of each range alone gives the bytes the walk of all of them gives.
inline std::vector<uint64_t> geo_ranges(const geo_index::View& new_view, unsigned threads) {
    const size_t blocks = new_view.blocks();
    const size_t n = std::max<size_t>(1, std::min<size_t>(blocks, 8 * size_t(threads ? threads : parallel_threads())));
    std::vector<uint64_t> bounds(n + 1);
    bounds[0] = 0;
    bounds[n] = UINT64_MAX;
    for (size_t k = 1; k < n; k++) bounds[k] = new_view.block(blocks * k / n).first_cell;
    return bounds;
}

// The cells new holds and old doesn't (added) and the reverse (removed),
// ascending: each walked range's in order.
inline CellIdDiff diff_geo_cells(const geo_index::View& old_view, const geo_index::View& new_view, unsigned threads = 0) {
    const std::vector<uint64_t> bounds = geo_ranges(new_view, threads);
    std::vector<CellIdDiff> parts(bounds.size() - 1);
    parallel_for(parts.size(), [&](size_t b, size_t e, unsigned) {
        for (size_t k = b; k < e; k++) {
            GeoRows o(old_view, bounds[k], bounds[k + 1]), n(new_view, bounds[k], bounds[k + 1]);
            while (!o.done() || !n.done()) {
                if (n.done() || (!o.done() && o.cell() < n.cell())) {
                    parts[k].removed.push_back(o.cell());
                    o.next();
                } else if (o.done() || n.cell() < o.cell()) {
                    parts[k].added.push_back(n.cell());
                    n.next();
                } else {
                    o.next();
                    n.next();
                }
            }
        }
    }, threads);
    CellIdDiff out;
    for (const CellIdDiff& p : parts) {
        out.added.insert(out.added.end(), p.added.begin(), p.added.end());
        out.removed.insert(out.removed.end(), p.removed.begin(), p.removed.end());
    }
    return out;
}

struct GeoEntryCorrections {
    // Per table (GEO_ENTRIES_FILES order): the section (marker, file id,
    // cell count, deltas) and its cell count.
    std::array<std::vector<char>, geo_index::TABLES> sections;
    std::array<uint32_t, geo_index::TABLES> cells{};
};

// Merge-walks the effective old cells (old - removed + added, each added
// cell with empty lists) against the new ones, both in cell order, and
// corrects each table's list where the old one through the table's id remap
// (an id it doesn't map stays as it is, or drops out with drop_unmapped)
// isn't the new one. `added` ascends. The walk splits into ranges of cell
// ids at the new index's blocks, walked on every core, whose deltas
// concatenate to the one walk's bytes. Every block of the new index must be
// the bytes the patcher's geo_index::Writer writes for its rows (it writes
// them so), or the diff fails.
inline GeoEntryCorrections geo_entry_corrections(const geo_index::View& old_view, const geo_index::View& new_view,
                                                 const std::vector<uint64_t>& added,
                                                 const std::vector<uint64_t>& removed,
                                                 const std::array<const std::vector<uint32_t>*, geo_index::TABLES>& remaps,
                                                 const std::array<bool, geo_index::TABLES>& drop_unmapped,
                                                 unsigned threads = 0) {
    constexpr uint32_t NO_DATA = 0xFFFFFFFFu;
    constexpr size_t T = geo_index::TABLES;
    if (!std::is_sorted(added.begin(), added.end())) throw std::logic_error("geo_entry_corrections: added out of order");
    std::vector<uint64_t> removed_sorted(removed);
    std::sort(removed_sorted.begin(), removed_sorted.end());
    auto is_removed = [&](uint64_t id) { return std::binary_search(removed_sorted.begin(), removed_sorted.end(), id); };
    // The old list through table t's remap, sorted.
    auto derive = [&](size_t t, const std::vector<uint32_t>& old_ids, std::vector<uint32_t>& ids) {
        const std::vector<uint32_t>& rm = *remaps[t];
        ids.clear();
        for (uint32_t id : old_ids) {
            const bool mapped = id < rm.size() && rm[id] != NO_DATA;
            if (mapped) ids.push_back(rm[id]);
            else if (!drop_unmapped[t]) ids.push_back(id);
        }
        std::sort(ids.begin(), ids.end());
    };
    static const std::vector<uint32_t> none;

    const std::vector<uint64_t> bounds = geo_ranges(new_view, threads);
    const size_t ranges = bounds.size() - 1;
    std::vector<std::array<std::vector<char>, T>> bufs(ranges);
    std::vector<std::array<uint32_t, T>> counts(ranges);
    std::vector<char> canonical(ranges, 1);
    parallel_for(ranges, [&](size_t b, size_t e, unsigned) {
        std::vector<uint32_t> d_ids;
        geo_index::View::Scratch check = new_view.scratch();
        for (size_t k = b; k < e; k++) {
            const uint64_t lo = bounds[k], hi = bounds[k + 1];
            counts[k] = {};
            // The new index's blocks of the range (it starts at one), each as the Writer writes it.
            const size_t first = k == 0 ? 0 : new_view.block_of(lo), last = k + 1 == ranges ? new_view.blocks() : new_view.block_of(hi);
            for (size_t blk = first; blk < last; blk++) canonical[k] &= new_view.canonical(blk, check);
            GeoRows o(old_view, lo, hi), n(new_view, lo, hi);
            size_t ai = static_cast<size_t>(std::lower_bound(added.begin(), added.end(), lo) - added.begin());
            const size_t a_end = static_cast<size_t>(std::lower_bound(added.begin(), added.end(), hi) - added.begin());
            auto emit = [&](size_t t, uint64_t cell, const std::vector<uint32_t>& from, const std::vector<uint32_t>& to) {
                append_geo_list_delta(bufs[k][t], cell, from, to);
                counts[k][t]++;
            };
            while (!o.done() || !n.done() || ai < a_end) {
                while (!o.done() && is_removed(o.cell())) o.next();
                uint64_t eff_old = o.done() ? UINT64_MAX : o.cell();
                const bool is_added = ai < a_end && added[ai] < eff_old;
                if (is_added) eff_old = added[ai];
                const uint64_t n_cid = n.done() ? UINT64_MAX : n.cell();
                if (eff_old == UINT64_MAX && n_cid == UINT64_MAX) break;
                if (eff_old <= n_cid) {
                    for (size_t t = 0; t < T; t++) {
                        if (is_added) d_ids.clear();
                        else derive(t, o.list(t), d_ids);
                        const std::vector<uint32_t>& n_ids = eff_old == n_cid ? n.list(t) : none;
                        if (d_ids != n_ids) emit(t, eff_old, d_ids, n_ids);
                    }
                    if (is_added) ai++;
                    else o.next();
                    if (eff_old == n_cid) n.next();
                } else {
                    for (size_t t = 0; t < T; t++)
                        if (!n.list(t).empty()) emit(t, n_cid, none, n.list(t));
                    n.next();
                }
            }
        }
    }, threads);
    if (std::find(canonical.begin(), canonical.end(), 0) != canonical.end())
        throw std::runtime_error("Geo index has a layout the patcher's geo_index::Writer can't reproduce");

    GeoEntryCorrections out;
    for (size_t t = 0; t < T; t++) {
        std::vector<char>& s = out.sections[t];
        s.assign(12, 0);
        for (size_t k = 0; k < ranges; k++) {
            s.insert(s.end(), bufs[k][t].begin(), bufs[k][t].end());
            out.cells[t] += counts[k][t];
        }
        const uint32_t marker = GEO_ENTRY_DELTA_MARKER, file_id = static_cast<uint32_t>(GEO_ENTRIES_FILES[t]);
        memcpy(s.data(), &marker, 4);
        memcpy(s.data() + 4, &file_id, 4);
        memcpy(s.data() + 8, &out.cells[t], 4);
    }
    return out;
}
