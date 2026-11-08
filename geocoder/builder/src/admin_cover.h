// The admin cells of one polygon: the admin cell index covers the rings of
// every tier the reader reads, the exact ones the builder's links test among
// them. Either looks a point up in its own cell and tests each ring listed
// there with a ray cast over straight lat/lng edges.
// S2 joins two vertices by a great circle instead, which bows towards the
// pole off any edge not along a meridian or the equator (660 km of Egypt's
// border on 22°N bows 3.3 km north), so an S2 covering of the vertices
// misses cells the ray cast reaches. Here each edge is followed in pieces
// short enough that their great-circle chords stay close to it, and every
// cell near a piece is listed.
// A polygon's rings are chains of arcs (admin_arcs.h), each arc a stretch of
// border many rings run along, so the cells near its edges are its arcs':
// each arc's are found once per tier (admin_arc_cells), and a polygon's are
// the merge of its arcs' (merge_cell_lists), from which admin_polygon_cells
// classifies the rest.
#pragma once

#include <algorithm>
#include <cstdint>
#include <functional>
#include <utility>
#include <vector>

#include "packed_location.h"
#include "polygon_rings.h"
#include "types.h"

// How many equal lat/lng pieces the straight edge from (lat1, lng1) to
// (lat2, lng2), in degrees, is followed in: each piece's great-circle chord
// lies within ADMIN_COVER_CHORD_TOLERANCE_RAD of the piece, and no piece
// spans more than ADMIN_COVER_MAX_PIECE_RAD of latitude or longitude. The
// edge runs the way the ray cast takes it, the long way round for a
// longitude step over 180 degrees.
uint32_t straight_edge_pieces(double lat1, double lng1, double lat2, double lng2);

// (cell id, interior) pairs, ascending by id.
using AdminRingCells = std::vector<std::pair<uint64_t, bool>>;

// Appends to `out`, ascending and each once, the level-`level` cells within
// reach_rad of the straight edges between consecutive vertices of the
// polyline verts[0, count): an arc as a tier stores it, or a closed ring
// (its first vertex again at its end). An edge between two equal vertices
// has none. Each edge is followed from its end of smaller coord_key, so a
// polyline has the same cells in either direction, and a ring the same as
// the union of its arcs' or of its edges' alone.
void admin_arc_cells(const PackedLocation* verts, uint32_t count, int level, double reach_rad,
                     std::vector<uint64_t>& out);

// An ascending list of cells, [first, second).
using CellList = std::pair<const uint64_t*, const uint64_t*>;

// The ascending union of the ascending lists `lists`, each cell once, into
// `out`: a k-way merge. Uses up `lists` (left empty).
inline void merge_cell_lists(std::vector<CellList>& lists, std::vector<uint64_t>& out) {
    out.clear();
    lists.erase(std::remove_if(lists.begin(), lists.end(), [](const CellList& l) { return l.first == l.second; }),
                lists.end());
    if (lists.size() == 1) {
        out.assign(lists[0].first, lists[0].second);
        lists.clear();
        return;
    }
    size_t total = 0;
    for (const CellList& l : lists) total += static_cast<size_t>(l.second - l.first);
    out.reserve(total);
    // A heap of the lists by their next cell, smallest on top.
    const auto later = [](const CellList& a, const CellList& b) { return *a.first > *b.first; };
    std::make_heap(lists.begin(), lists.end(), later);
    while (!lists.empty()) {
        std::pop_heap(lists.begin(), lists.end(), later);
        CellList& next = lists.back();
        if (out.empty() || out.back() != *next.first) out.push_back(*next.first);
        if (++next.first == next.second)
            lists.pop_back();
        else
            std::push_heap(lists.begin(), lists.end(), later);
    }
}

// The level-`level` cells of a polygon's rings as exact_ring_contains tests
// them (the outer ring, then its holes, each its own ring in verts, all of
// them counted even-odd), ascending by id: the cells of `boundary` (flagged
// false), those within reach of the rings' edges, ascending and each once
// (their arcs' admin_arc_cells merged), and every other cell the rings hold
// (flagged true: the ray cast puts every point of it inside). The admin cell
// index takes the boundary at ADMIN_COVER_REACH_RAD. None for no rings or an
// empty outer ring; a boundary out of order is refused (std::logic_error).
AdminRingCells admin_polygon_cells(const std::vector<uint64_t>& boundary, const PackedLocation* verts,
                                   const RingSpan* rings, uint32_t ring_count, int level);

// Whether a region whose cells are those `holds` takes (by cell id) must keep
// `cell` in its admin cell index: the server routes a query to every region
// holding its cell or one of the cell's neighbours (Continent::covers_query,
// S2CellId::AppendAllNeighbors at the cell's level), and its admin scan then
// reads the query's own cell there.
bool admin_cell_routed(uint64_t cell, const std::function<bool(uint64_t)>& holds);

// Cover either geometry variant, marking a cell interior only if both do.
AdminRingCells merge_admin_ring_cells(const AdminRingCells& x, const AdminRingCells& y);
