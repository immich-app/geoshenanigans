#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "parsed_data.h"

std::vector<std::vector<std::pair<double,double>>> assemble_outer_rings(
    const std::vector<std::pair<int64_t, std::string>>& members,
    const WayGeometries& way_geoms,
    bool include_all_roles = false);

// The closed rings the relation's role=inner ways form (its holes, once
// assign_holes places them in outer rings), stitched as assemble_outer_rings
// stitches the outer ways.
std::vector<std::vector<std::pair<double,double>>> assemble_inner_rings(
    const std::vector<std::pair<int64_t, std::string>>& members,
    const WayGeometries& way_geoms);
