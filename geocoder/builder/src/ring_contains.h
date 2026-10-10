// Crossing-number point-in-ring test over packed admin vertices, in the
// float arithmetic the country lookup has always used (its answers are
// build output, so the arithmetic must not change).
#pragma once

#include <cstdint>

#include "types.h"

// Whether the edge between va and vb crosses the ray from (plat, plng)
// towards +lat.
inline bool ring_edge_crosses(const NodeCoord& va, const NodeCoord& vb, float plat, float plng) {
    return ((va.lng > plng) != (vb.lng > plng)) &&
           (plat < (vb.lat - va.lat) * (plng - va.lng) / (vb.lng - va.lng) + va.lat);
}

inline bool ring_contains(const NodeCoord* verts, uint32_t cnt, float plat, float plng) {
    bool inside = false;
    for (uint32_t a = 0, b = cnt - 1; a < cnt; b = a++)
        if (ring_edge_crosses(verts[a], verts[b], plat, plng)) inside = !inside;
    return inside;
}
