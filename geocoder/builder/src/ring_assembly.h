#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "packed_location.h"
#include "parsed_data.h"

std::vector<std::vector<PackedLocation>> assemble_outer_rings(
    const std::vector<std::pair<int64_t, std::string>>& members,
    const WayGeometries& way_geoms,
    bool include_all_roles = false);

// Member roles a ring runs along (AreaRings::roles bits).
enum RingRole : uint8_t { kRoleOuter = 1, kRoleInner = 2, kRoleOther = 4 };

// Why osmium's assembler would build no area from the member ways.
enum class AreaFailure : uint8_t { None, OpenRing, TooManyTouches, Crossing };

struct AreaRings {
    std::vector<std::vector<PackedLocation>> rings;  // closed
    std::vector<uint8_t> roles;      // per ring: RingRole bits of the ways it runs along
    uint32_t cancelled_sub_ways = 0; // duplicates dropped in pairs
    uint32_t crossing_coordinates = 0;  // where rings walked in member order crossed (re-paired)
    AreaFailure failure = AreaFailure::None;  // set: no rings
};

// Every closed ring a relation's member ways form, whatever their roles, or
// none where osmium's area assembler (which osm2pgsql, and so Nominatim,
// builds boundary geometry with) gives up. nest_rings (polygon_rings.h) then
// tells the outer rings from the holes by geometry.
AreaRings assemble_area_rings(const std::vector<std::pair<int64_t, std::string>>& members,
                              const WayGeometries& way_geoms);
