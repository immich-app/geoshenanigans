// The naming join of water_names.h over S2 cells.
#pragma once

#include <cstdint>
#include <vector>

#include "parsed_data.h"
#include "water_names.h"

namespace water {

struct NamingCounts {
    size_t cover_entries = 0;      // the river areas' entries at the admin cell level
    size_t segments_in_cover = 0;  // (candidate segment, cell they list that it passes near) pairs
    size_t probes = 0;             // (polygon, segment) pairs a boundary cell left to the clip
    size_t probes_inside = 0;      // of those, the pairs of some length inside
    size_t credits = 0;
};

// Every credit of the candidate lines to the objects `object_of` (by
// polygon of `rings`) gives the polygons `polygons` of the river and canal
// areas: each segment credits each such polygon the length of its
// stretches inside the polygon's rings (SegmentClip: even-odd, holes
// honoured, in whole millimetres, share_mm) to the polygon's object, under
// its line's key and display, once; consecutive ones of one line summed. A
// segment across the antimeridian (segment_wraps) credits nothing. The
// cells at the admin cell level (kAdminCellLevel) the segment passes near,
// as the covering finds a ring's (admin_arc_cells), give the polygons: one
// whose interior entries are all those cells holds the whole segment, any
// other is clipped exactly.
std::vector<Credit> credit_naming_lines(const AdminExactRings& rings, const std::vector<uint32_t>& polygons,
                                        const std::vector<uint32_t>& object_of, const NamingLines& lines,
                                        NamingCounts& counts, unsigned threads = 0);

}  // namespace water
