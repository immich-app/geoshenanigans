// Pure helpers (no S2) for the passes that link records to their parents:
// the admin polygons containing them and the streets nearest to them.
#pragma once

#include <cstdint>

#include "types.h"

// Even-odd ray cast along the latitude axis: edge (a, b) toggles when it
// straddles lng (one end > lng, the other <= lng) and lat lies below the
// latitude the edge reaches at lng. Every linking pass must share exactly this
// float arithmetic: another rounding moves boundary points between polygons
// and changes parent ids.
inline bool ring_contains(const NodeCoord* verts, uint32_t n, float lat, float lng) {
    bool inside = false;
    for (uint32_t a = 0, b = n - 1; a < n; b = a++) {
        if (((verts[a].lng > lng) != (verts[b].lng > lng)) &&
            (lat < (verts[b].lat - verts[a].lat) * (lng - verts[a].lng) / (verts[b].lng - verts[a].lng) + verts[a].lat))
            inside = !inside;
    }
    return inside;
}
