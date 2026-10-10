// Whether a segment meets an axis-aligned rectangle, answered only where
// moving any input by up to kSegmentRectMargin cannot change the answer. The
// street edge covering (s2_helpers.cpp) asks it of an edge's ends in a cube
// face's (u,v) plane, where the geodesic edge is a straight segment, and an
// S2 cell's (u,v) bounds: a sure answer is S2Polyline::MayIntersect's for the
// cell, as the rounding of either side is far below the margin.
#pragma once

#include <algorithm>
#include <cmath>

// Far above the ~1e-15 rounding of a (u,v) coordinate, far below the ~1e-9
// (u,v) width of the smallest S2 cell.
static constexpr double kSegmentRectMargin = 1e-11;

enum class SegmentRect { Disjoint, Meets, Unsure };

// Segment (au, av)-(bu, bv) against [u0, u1] x [v0, v1].
inline SegmentRect segment_meets_rect(double au, double av, double bu, double bv,
                                      double u0, double u1, double v0, double v1) {
    const double m = kSegmentRectMargin;
    auto deep_inside = [&](double u, double v) { return u > u0 + m && u < u1 - m && v > v0 + m && v < v1 - m; };
    if (deep_inside(au, av) || deep_inside(bu, bv)) return SegmentRect::Meets;

    // Separating axes: u, v and the segment's normal. Every axis overlapping
    // by the margin leaves the segment through the rectangle.
    const double su0 = std::min(au, bu), su1 = std::max(au, bu);
    const double sv0 = std::min(av, bv), sv1 = std::max(av, bv);
    if (su1 < u0 - m || su0 > u1 + m || sv1 < v0 - m || sv0 > v1 + m) return SegmentRect::Disjoint;
    const double du = bu - au, dv = bv - av;
    // A corner's distance from the segment's line, times the segment length.
    auto side = [&](double u, double v) { return du * (v - av) - dv * (u - au); };
    const double s00 = side(u0, v0), s10 = side(u1, v0), s11 = side(u1, v1), s01 = side(u0, v1);
    const double lo = std::min(std::min(s00, s10), std::min(s11, s01));
    const double hi = std::max(std::max(s00, s10), std::max(s11, s01));
    // |du| + |dv| bounds the length: a margin of distance at least.
    const double ms = m * (std::abs(du) + std::abs(dv));
    if (lo > ms || hi < -ms) return SegmentRect::Disjoint;
    if (lo < -ms && hi > ms && su1 > u0 + m && su0 < u1 - m && sv1 > v0 + m && sv0 < v1 - m)
        return SegmentRect::Meets;
    return SegmentRect::Unsure;
}
