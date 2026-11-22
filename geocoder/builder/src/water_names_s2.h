// The naming join of water_names.h over S2 cells.
#pragma once

#include <cstdint>
#include <vector>

#include "parsed_data.h"
#include "water_names.h"

namespace water {

struct NamingCounts {
    size_t cover_entries = 0;      // the river areas' L10 entries
    size_t segments_in_cover = 0;  // candidate segments whose midpoint's cell they list
    size_t probes = 0;             // (polygon, segment) pairs a boundary cell left to the ray cast
    size_t probes_inside = 0;
    size_t credits = 0;
};

// Every credit of the candidate lines to the objects `object_of` (by
// polygon of `rings`) gives the polygons `polygons` of the river and canal
// areas: each segment whose midpoint (segment_midpoint) such a polygon's
// rings hold (even-odd, holes honoured) credits its length (segment_mm) to
// the polygon's object, under its line's key and display; consecutive ones
// of one line summed. The midpoint's L10 cell gives the polygons: an
// interior entry of the covering holds it, any other is the exact ray cast.
std::vector<Credit> credit_naming_lines(const AdminExactRings& rings, const std::vector<uint32_t>& polygons,
                                        const std::vector<uint32_t>& object_of, const NamingLines& lines,
                                        NamingCounts& counts, unsigned threads = 0);

}  // namespace water
