#pragma once

#include <cstdint>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <s2/s2cell_id.h>

#include "packed_location.h"
#include "types.h"
#include "geometry.h"
#include "parsed_data.h"

// --- S2 cell level globals (set by main arg parsing) ---

extern int kStreetCellLevel;
extern int kAdminCellLevel;
extern int kMaxAdminLevel;

// --- S2 helpers ---

// The street-level cells the edge from (lat1, lng1) to (lat2, lng2) crosses.
// c1 and c2 are its ends' cells, point_to_cell of each: a polyline's inner
// nodes end two edges, so callers compute each once.
void cover_edge(S2CellId c1, S2CellId c2, double lat1, double lng1, double lat2, double lng2,
                std::vector<S2CellId>& out);


S2CellId point_to_cell(double lat, double lng);

// The kAdminCellLevel cells covering the closed ring `vertices`, each
// flagged when it lies wholly inside.
std::vector<std::pair<S2CellId, bool>> cover_polygon(const std::vector<std::pair<double,double>>& vertices);

// --- Helper functions that depend on S2 ---

// Interns the address strings (street, housenumber, postcode, in that order)
// and appends the point via append_addr_point.
void add_addr_point(ParsedData& data, double lat, double lng,
                    const char* housenumber, const char* street,
                    const char* postcode,
                    uint64_t& addr_count_total,
                    uint64_t osm_id_packed,
                    const NodeCoord* polygon_vertices = nullptr,
                    uint32_t polygon_vertex_count = 0);

// Appends one address point whose strings are already interned (NO_DATA
// for a missing street or postcode); `cell` is point_to_cell() of its
// location (lat/lng are stored as float).
void append_addr_point(ParsedData& data, double lat, double lng,
                       uint32_t housenumber_id, uint32_t street_id, uint32_t postcode_id,
                       S2CellId cell, uint64_t osm_id_packed,
                       const NodeCoord* polygon_vertices = nullptr,
                       uint32_t polygon_vertex_count = 0);

// Appends the admin polygon of a closed way's ring, with the ring itself to
// the exact rings and their area as the polygon's.
void add_admin_polygon(ParsedData& data,
                       const std::vector<PackedLocation>& ring,
                       const char* name, uint8_t admin_level,
                       const char* country_code,
                       uint8_t place_type_override = 0,
                       int64_t osm_way_id = 0);
