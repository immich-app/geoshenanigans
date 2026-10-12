// Unit tests for segment_meets_rect (src/segment_rect.h): sure answers away
// from the boundary, Unsure within the margin of it, and sure answers that
// agree with an exact clip of the segment against the rectangle.
#include "segment_rect.h"

#include <random>

#include "test_framework.h"

namespace {

// Rectangle [0, 1] x [0, 1].
SegmentRect unit(double au, double av, double bu, double bv) {
    return segment_meets_rect(au, av, bu, bv, 0, 1, 0, 1);
}

// Whether the segment meets the closed rectangle, by Liang-Barsky clipping
// in long double.
bool clip_meets(long double au, long double av, long double bu, long double bv,
                long double u0, long double u1, long double v0, long double v1) {
    long double t0 = 0, t1 = 1;
    const long double du = bu - au, dv = bv - av;
    const long double p[4] = {-du, du, -dv, dv};
    const long double q[4] = {au - u0, u1 - au, av - v0, v1 - av};
    for (int k = 0; k < 4; k++) {
        if (p[k] == 0) {
            if (q[k] < 0) return false;
            continue;
        }
        const long double t = q[k] / p[k];
        if (p[k] < 0) t0 = std::max(t0, t);
        else t1 = std::min(t1, t);
        if (t0 > t1) return false;
    }
    return true;
}

}  // namespace

TEST(segment_rect_meets_when_an_end_is_inside) {
    CHECK(unit(0.5, 0.5, 3, 3) == SegmentRect::Meets);
    CHECK(unit(-3, 0.2, 0.9, 0.9) == SegmentRect::Meets);
}

TEST(segment_rect_meets_when_crossing_with_both_ends_outside) {
    CHECK(unit(-1, 0.5, 2, 0.5) == SegmentRect::Meets);
    CHECK(unit(0.5, -1, 0.5, 2) == SegmentRect::Meets);
    CHECK(unit(-0.2, 0.5, 0.5, -0.2) == SegmentRect::Meets);  // cuts the corner at (0, 0)
    CHECK(unit(-0.5, 0.2, 0.2, -0.5) == SegmentRect::Disjoint);  // passes beside it
}

TEST(segment_rect_is_disjoint_when_an_axis_separates) {
    CHECK(unit(1.5, -1, 1.5, 2) == SegmentRect::Disjoint);
    CHECK(unit(-1, -0.5, 2, -0.5) == SegmentRect::Disjoint);
    CHECK(unit(-2, 3, 3, 2) == SegmentRect::Disjoint);
    // The boxes overlap, the line passes beside the corner at (1, 1).
    CHECK(unit(1.5, 0.6, 0.6, 1.5) == SegmentRect::Disjoint);
}

TEST(segment_rect_is_unsure_within_the_margin_of_the_boundary) {
    CHECK(unit(1, 0.5, 2, 0.5) == SegmentRect::Unsure);              // ends on the edge
    CHECK(unit(1 + 1e-13, 0.5, 2, 0.5) == SegmentRect::Unsure);      // ends a hair outside
    CHECK(unit(-1, 1, 2, 1) == SegmentRect::Unsure);                 // runs along the edge
    CHECK(unit(0.5, 1.5, 1.5, 0.5) == SegmentRect::Unsure);          // touches the corner
    CHECK(unit(0.5, 1.5 + 1e-13, 1.5 + 1e-13, 0.5) == SegmentRect::Unsure);
}

TEST(segment_rect_answers_for_cells_far_smaller_than_the_unit_square) {
    const double w = 1e-8;  // about a level-27 cell in (u,v)
    CHECK(segment_meets_rect(-1, w / 2, 1, w / 2, 0, w, 0, w) == SegmentRect::Meets);
    CHECK(segment_meets_rect(-1, 2 * w, 1, 2 * w, 0, w, 0, w) == SegmentRect::Disjoint);
}

TEST(segment_rect_sure_answers_agree_with_an_exact_clip) {
    std::mt19937_64 rng(11);
    std::uniform_real_distribution<double> u01(0, 1);
    size_t sure = 0, wrong = 0;
    for (int k = 0; k < 200000; k++) {
        const double scale = std::pow(10.0, -1 - 8 * u01(rng));
        const double u0 = u01(rng) - 0.5, v0 = u01(rng) - 0.5;
        const double u1 = u0 + scale, v1 = v0 + scale;
        // Ends around the rectangle, some on its edges and corners exactly.
        auto coord = [&](double lo, double hi) {
            double r = u01(rng);
            if (r < 0.1) return lo;
            if (r < 0.2) return hi;
            return lo + (hi - lo) * (3 * u01(rng) - 1);
        };
        const double au = coord(u0, u1), av = coord(v0, v1), bu = coord(u0, u1), bv = coord(v0, v1);
        const SegmentRect got = segment_meets_rect(au, av, bu, bv, u0, u1, v0, v1);
        if (got == SegmentRect::Unsure) continue;
        sure++;
        if ((got == SegmentRect::Meets) != clip_meets(au, av, bu, bv, u0, u1, v0, v1)) wrong++;
    }
    CHECK(sure > 150000);
    CHECK(wrong == 0);
}
