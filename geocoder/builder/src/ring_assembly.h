#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "parsed_data.h"

std::vector<std::vector<std::pair<double,double>>> assemble_outer_rings(
    const std::vector<std::pair<int64_t, std::string>>& members,
    const std::unordered_map<int64_t, ParsedData::WayGeometry>& way_geoms,
    bool include_all_roles = false);
