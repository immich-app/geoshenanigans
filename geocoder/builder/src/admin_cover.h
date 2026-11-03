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
#pragma once

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

// The level-`level` cells of a polygon's exact rings as exact_ring_contains
// tests them (the outer ring, then its holes, each its own ring in verts, all
// of them counted even-odd), ascending by id: every cell within reach_rad of
// an edge (flagged false), and every other cell the rings hold (flagged true:
// the ray cast puts every point of it inside). The admin cell index takes
// them at ADMIN_COVER_REACH_RAD. Each edge is followed from its end of
// smaller coord_key, so the cells are the same whatever the rings' rotation
// or direction.
AdminRingCells admin_ring_cells(const PackedLocation* verts, const RingSpan* rings, uint32_t ring_count, int level,
                                double reach_rad);

// Whether a region whose cells are those `holds` takes (by cell id) must keep
// `cell` in its admin cell index: the server routes a query to every region
// holding its cell or one of the cell's neighbours (Continent::covers_query,
// S2CellId::AppendAllNeighbors at the cell's level), and its admin scan then
// reads the query's own cell there.
bool admin_cell_routed(uint64_t cell, const std::function<bool(uint64_t)>& holds);

// Cover either geometry variant, marking a cell interior only if both do.
AdminRingCells merge_admin_ring_cells(const AdminRingCells& x, const AdminRingCells& y);
