// The passes that link records to the admin polygons containing them, over
// the builder's admin candidates per cell (ParsedData::cell_to_admin): each
// admin polygon's parent, each street way's parent and postcode, each POI's
// parent and postcode.
#pragma once

#include <chrono>
#include <cstdint>
#include <vector>

#include "parsed_data.h"
#include "types.h"

// Moves each candidate to its polygon's new id (old_to_new) once the
// polygons are reordered, keeping its INTERIOR_FLAG.
void remap_admin_candidates(ParsedData& data, const std::vector<uint32_t>& old_to_new, unsigned int threads);

// Orders each cell's candidates by admin osm id, the order the passes break
// their ties in.
void order_admin_candidates(ParsedData& data, unsigned int threads);

void compute_admin_parent_chain(ParsedData& data, unsigned int num_threads);

void compute_way_parent_polygons(ParsedData& data, unsigned int num_threads);

void compute_poi_parent_polygons(ParsedData& data, unsigned int num_threads, std::vector<CellItemPair> order,
                std::chrono::steady_clock::time_point& _s2t, CpuTicks& _s2cpu);
