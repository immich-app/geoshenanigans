// The point halfway along a line's nodes, Nominatim's centroid of a line
// (get_center_point: ST_LineInterpolatePoint(geom, 0.5)), as the server's
// line_middle finds it on the same f32 nodes: along their lengths in plain
// degrees, rounded to 1e-7. The same point either way round the line, but
// for the rounding of the lengths' sums.
#pragma once

#include <cmath>
#include <cstdint>

#include "exact_pip.h"
#include "packed_location.h"
#include "types.h"

// count is at least 1.
inline PackedLocation line_middle(const NodeCoord* nodes, uint16_t count) {
    auto length = [&](uint16_t k) {
        return std::hypot(double(nodes[k].lat) - nodes[k - 1].lat, double(nodes[k].lng) - nodes[k - 1].lng);
    };
    double total = 0;
    for (uint16_t k = 1; k < count; k++) total += length(k);
    double left = total / 2, lat = nodes[0].lat, lng = nodes[0].lng;
    for (uint16_t k = 1; k < count; k++) {
        const double step = length(k);
        if (step > 0 && left <= step) {
            const double t = left / step;
            lat = nodes[k - 1].lat + t * (double(nodes[k].lat) - nodes[k - 1].lat);
            lng = nodes[k - 1].lng + t * (double(nodes[k].lng) - nodes[k - 1].lng);
            break;
        }
        left -= step;
        lat = nodes[k].lat;
        lng = nodes[k].lng;
    }
    auto reduce = [](double v) { return std::round(v * 1e7) / 1e7; };
    return point_e7(reduce(lat), reduce(lng));
}
