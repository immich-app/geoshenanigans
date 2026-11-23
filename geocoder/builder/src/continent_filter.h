#pragma once

#include "parsed_data.h"

struct ContinentBBox {
    const char* name;
    double min_lat, max_lat, min_lng, max_lng;
};

extern const ContinentBBox kContinents[];
extern const size_t kContinentCount;

class PolygonIndex;

// Whether a continent holds a cell of the admin and water cell indexes, as
// its split takes them: the cell's centre in its boundary polygon (`polygon`)
// where it has one, else in its box. The split keeps a cell it or one around
// it holds (admin_cell_routed).
bool cell_in_continent(uint64_t cell_id, const ContinentBBox& bbox, const PolygonIndex* polygon);

// What a split leaves of the planet records it copies from: all of them, or
// (for the last region to read them) none: each kind is freed once copied, so
// the planet's records and the continent's never stand whole at once.
enum class PlanetRecords { Keep, Consume };

// Fast version using precomputed continent bitmasks (avoids cell_in_bbox per cell).
// If polygon is provided, items are additionally tested at the coordinate level.
// way_masks/addr_masks/interp_masks/poi_masks/place_masks are parallel to the
// corresponding sorted_*_cells arrays on `full`, which only Consume changes.
ParsedData filter_by_bbox_masked(ParsedData& full, const ContinentBBox& bbox,
    uint8_t continent_bit,
    const std::vector<uint8_t>& way_masks,
    const std::vector<uint8_t>& addr_masks,
    const std::vector<uint8_t>& interp_masks,
    const std::vector<uint8_t>& poi_masks,
    const std::vector<uint8_t>& place_masks,
    const std::vector<std::pair<double,double>>* polygon = nullptr,
    PlanetRecords records = PlanetRecords::Keep);
