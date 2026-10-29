// The admin cell index's covering of one ring. A reader looks a point up in
// its own cell and tests each ring listed there with a ray cast over straight
// lat/lng edges between the ring's stored f32 vertices. S2 joins two vertices
// by a great circle instead, which bows towards the pole off any edge not
// along a meridian or the equator (660 km of Egypt's border on 22°N bows
// 3.3 km north), so an S2 covering of the vertices misses cells the ray cast
// reaches. Here each edge is followed in pieces short enough that their
// great-circle chords stay close to it, and every cell near a piece is listed.
#pragma once

#include <cstdint>
#include <functional>
#include <utility>
#include <vector>

#include "types.h"

class S2LatLngRect;

// How many equal lat/lng pieces the straight edge from (lat1, lng1) to
// (lat2, lng2), in degrees, is followed in: each piece's great-circle chord
// lies within ADMIN_COVER_CHORD_TOLERANCE_RAD of the piece, and no piece
// spans more than ADMIN_COVER_MAX_PIECE_RAD of latitude or longitude. The
// edge runs the way the ray cast takes it, the long way round for a
// longitude step over 180 degrees.
uint32_t straight_edge_pieces(double lat1, double lng1, double lat2, double lng2);

// The bounds of the ring verts[0, n), n >= 1, within the poles and the
// antimeridian: a vertex decoded from its grid can round a little past them
// (180 + 4.6e-5 on the 1e-4 grid), which S2 rejects. The ray cast puts no
// query point outside these inside.
S2LatLngRect admin_ring_bounds(const NodeCoord* verts, uint32_t n);

// (cell id, interior) pairs, ascending by id.
using AdminRingCells = std::vector<std::pair<uint64_t, bool>>;

// The level-`level` cells of the ring verts[0, n) as the reader's ray cast
// tests it, ascending by id: every cell within ADMIN_COVER_REACH_RAD of an
// edge (flagged false), and every other cell the ring holds (flagged true:
// the ray cast puts every point of it inside).
AdminRingCells admin_ring_cells(const NodeCoord* verts, uint32_t n, int level);

// Whether a region whose cells are those `holds` takes (by cell id) must keep
// `cell` in its admin cell index: the server routes a query to every region
// holding its cell or one of the cell's neighbours (Continent::covers_query,
// S2CellId::AppendAllNeighbors at the cell's level), and its admin scan then
// reads the query's own cell there.
bool admin_cell_routed(uint64_t cell, const std::function<bool(uint64_t)>& holds);

// Cover either geometry variant, marking a cell interior only if both do.
AdminRingCells merge_admin_ring_cells(const AdminRingCells& x, const AdminRingCells& y);
