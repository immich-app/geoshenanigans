// Boxes filed in a uniform grid of e7 cells over a bounding box, for finding
// those near another box: a polygon's edges near the edges of another, which
// polygon_relations.h's containment test pairs. A 2-D sibling of
// ring_edge_buckets.h.
#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <vector>

#include "exact_pip.h"

class EdgeGrid {
public:
    EdgeGrid() = default;

    // `count` items, box_of(k) the ExactRingBox of item k, over `bounds`,
    // with about `per_cell` items a cell. An item outside `bounds` is in no
    // cell, so a query never hands it.
    template <class BoxOf>
    EdgeGrid(const ExactRingBox& bounds, uint32_t count, BoxOf box_of, uint32_t per_cell = 4) : bounds_(bounds) {
        if (count == 0 || bounds.lat_lo > bounds.lat_hi || bounds.lng_lo > bounds.lng_hi) return;
        const double height = double(bounds.lat_hi) - bounds.lat_lo + 1, width = double(bounds.lng_hi) - bounds.lng_lo + 1;
        const double cells = std::min<double>(kMaxCells, std::max<double>(1, double(count) / std::max(1u, per_cell)));
        rows_ = static_cast<uint32_t>(std::clamp(std::round(std::sqrt(cells * height / width)), 1.0, cells));
        cols_ = static_cast<uint32_t>(std::clamp(std::round(cells / rows_), 1.0, cells));
        cell_lat_ = static_cast<int64_t>(std::ceil(height / rows_));
        cell_lng_ = static_cast<int64_t>(std::ceil(width / cols_));
        start_.assign(size_t(rows_) * cols_ + 1, 0);
        for_each_filing(count, box_of, [&](uint32_t, size_t cell) { start_[cell + 1]++; });
        for (size_t c = 0; c + 1 < start_.size(); c++) start_[c + 1] += start_[c];
        items_.resize(start_.back());
        std::vector<uint32_t> cursor(start_.begin(), start_.end() - 1);
        for_each_filing(count, box_of, [&](uint32_t k, size_t cell) { items_[cursor[cell]++] = k; });
    }

    // Calls visit(k) once for each item filed in a cell that `box` meets:
    // every item whose box meets it, and some whose box only comes near.
    // `scratch` holds the items between calls.
    template <class Visit>
    void for_each_meeting(const ExactRingBox& box, std::vector<uint32_t>& scratch, Visit visit) const {
        if (items_.empty() || !meets(box, bounds_)) return;
        const uint32_t r0 = row_of(box.lat_lo), r1 = row_of(box.lat_hi), c0 = col_of(box.lng_lo), c1 = col_of(box.lng_hi);
        if (r0 == r1 && c0 == c1) {
            const size_t cell = size_t(r0) * cols_ + c0;
            for (uint32_t i = start_[cell]; i < start_[cell + 1]; i++) visit(items_[i]);
            return;
        }
        scratch.clear();
        for (uint32_t r = r0; r <= r1; r++)
            for (uint32_t c = c0; c <= c1; c++) {
                const size_t cell = size_t(r) * cols_ + c;
                scratch.insert(scratch.end(), items_.begin() + start_[cell], items_.begin() + start_[cell + 1]);
            }
        std::sort(scratch.begin(), scratch.end());
        scratch.erase(std::unique(scratch.begin(), scratch.end()), scratch.end());
        for (uint32_t k : scratch) visit(k);
    }

    static bool meets(const ExactRingBox& a, const ExactRingBox& b) {
        return a.lat_lo <= b.lat_hi && b.lat_lo <= a.lat_hi && a.lng_lo <= b.lng_hi && b.lng_lo <= a.lng_hi;
    }

private:
    // A grid of millions of cells has its edges spread too thin to pay for its table.
    static constexpr double kMaxCells = 1 << 22;

    uint32_t row_of(int64_t lat) const {
        return static_cast<uint32_t>(std::clamp<int64_t>((lat - bounds_.lat_lo) / cell_lat_, 0, rows_ - 1));
    }
    uint32_t col_of(int64_t lng) const {
        return static_cast<uint32_t>(std::clamp<int64_t>((lng - bounds_.lng_lo) / cell_lng_, 0, cols_ - 1));
    }

    // fn(k, cell) for every cell item k's box meets.
    template <class BoxOf, class Fn>
    void for_each_filing(uint32_t count, BoxOf& box_of, Fn fn) const {
        for (uint32_t k = 0; k < count; k++) {
            const ExactRingBox b = box_of(k);
            if (!meets(b, bounds_)) continue;
            const uint32_t r0 = row_of(b.lat_lo), r1 = row_of(b.lat_hi), c0 = col_of(b.lng_lo), c1 = col_of(b.lng_hi);
            for (uint32_t r = r0; r <= r1; r++)
                for (uint32_t c = c0; c <= c1; c++) fn(k, size_t(r) * cols_ + c);
        }
    }

    ExactRingBox bounds_{};
    uint32_t rows_ = 1, cols_ = 1;
    int64_t cell_lat_ = 1, cell_lng_ = 1;
    std::vector<uint32_t> start_;  // cell c's items: items_[start_[c], start_[c + 1])
    std::vector<uint32_t> items_;
};
