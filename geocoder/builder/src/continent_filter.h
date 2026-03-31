#pragma once

#include "parsed_data.h"

struct ContinentBBox {
    const char* name;
    double min_lat, max_lat, min_lng, max_lng;
};

extern const ContinentBBox kContinents[];
extern const size_t kContinentCount;

ParsedData filter_by_bbox(const ParsedData& full, const ContinentBBox& bbox);

// Fast version using precomputed continent bitmasks (avoids cell_in_bbox per cell)
// If polygon is provided, items are additionally tested at the coordinate level.
ParsedData filter_by_bbox_masked(const ParsedData& full, const ContinentBBox& bbox,
    uint8_t continent_bit,
    const std::vector<uint8_t>& way_masks,
    const std::vector<uint8_t>& addr_masks,
    const std::vector<uint8_t>& interp_masks,
    const std::vector<std::pair<double,double>>* polygon = nullptr);
